import json
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import httpx
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_executor_plugins.settings import (
    CommonSettings,
    ExecutorSettingsBase,
)

from thoa.config import settings as thoa_settings
from thoa.core.job_status import JobStatus


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    try:
        return json.loads(json.dumps(value))
    except Exception:
        return str(value)


def _contains_glob(path: str) -> bool:
    return any(ch in path for ch in "*?[]")


def _classify_artifact(io_file) -> tuple[str, str]:
    path = str(io_file)
    if _contains_glob(path):
        return "glob", path
    if getattr(io_file, "is_directory", False):
        return "directory", path
    return "file", path


@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    api_url: Optional[str] = field(
        default=None,
        metadata={"help": "THOA API base URL (defaults to THOA_API_URL)."},
    )
    api_key: Optional[str] = field(
        default=None,
        metadata={
            "help": "THOA API key (defaults to THOA_API_KEY).",
            "env_var": True,
        },
    )
    client_home: Optional[str] = field(
        default=None,
        metadata={"help": "Client home path to store on THOA jobs (defaults to local HOME)."},
    )
    default_ram_gb: Optional[int] = field(
        default=8,
        metadata={"help": "Fallback RAM (GB) when Snakemake resources do not define mem_mb."},
    )
    default_disk_gb: Optional[int] = field(
        default=20,
        metadata={"help": "Fallback disk (GB) when Snakemake resources do not define disk_mb."},
    )
    poll_seconds: Optional[int] = field(
        default=5,
        metadata={"help": "Seconds between THOA status polls."},
    )
    environment_public_id: Optional[str] = field(
        default=None,
        metadata={
            "help": "Existing THOA environment public_id to attach to submitted jobs (optional for MVP).",
        },
    )
    workflow_name: Optional[str] = field(
        default=None,
        metadata={"help": "Optional THOA workflow run name override."},
    )
    external_run_id: Optional[str] = field(
        default=None,
        metadata={"help": "Optional external run id stored on the THOA workflow run."},
    )
    sync_graph_on_each_submit: Optional[bool] = field(
        default=False,
        metadata={"help": "Re-sync the Snakemake DAG snapshot before every job submission."},
    )


common_settings = CommonSettings(
    non_local_exec=True,
    implies_no_shared_fs=True,
    job_deploy_sources=True,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=True,
    auto_deploy_default_storage_provider=True,
    init_seconds_before_status_checks=5,
)


class Executor(RemoteExecutor):
    def __post_init__(self):
        super().__post_init__()
        self._workflow_run_public_id: Optional[str] = None
        self._workflow_graph_synced = False
        self._workflow_node_public_ids: Dict[str, str] = {}
        self._submitted_any_jobs = False
        self._saw_job_error = False
        self._cancel_requested = False

        self._sync_client: Optional[httpx.Client] = None
        self._api_base = self._resolve_api_base()
        self._api_key = self._resolve_api_key()
        self._timeout = httpx.Timeout(getattr(thoa_settings, "THOA_API_TIMEOUT", 30))

    # -------------------- THOA API helpers --------------------
    def _resolve_api_base(self) -> str:
        executor_settings = self.workflow.executor_settings
        api_url = getattr(executor_settings, "api_url", None) or thoa_settings.THOA_API_URL
        return api_url.rstrip("/")

    def _resolve_api_key(self) -> str:
        executor_settings = self.workflow.executor_settings
        api_key = getattr(executor_settings, "api_key", None) or thoa_settings.THOA_API_KEY
        if not api_key:
            raise WorkflowError(
                "THOA executor plugin requires a THOA API key. "
                "Set THOA_API_KEY or --thoa-api-key."
            )
        return api_key

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "X-API-Key": self._api_key,
            "Accept": "application/json",
        }

    @property
    def _client(self) -> httpx.Client:
        if self._sync_client is None:
            self._sync_client = httpx.Client(
                base_url=self._api_base,
                headers=self._headers,
                timeout=self._timeout,
            )
        return self._sync_client

    def _request(self, method: str, path: str, **kwargs) -> Any:
        response = self._client.request(method, self._api_path(path), **kwargs)
        self._raise_for_status(response, method, path)
        if not response.content:
            return None
        return response.json()

    async def _arequest(self, method: str, path: str, **kwargs) -> Any:
        async with httpx.AsyncClient(
            base_url=self._api_base,
            headers=self._headers,
            timeout=self._timeout,
        ) as client:
            response = await client.request(method, self._api_path(path), **kwargs)
        self._raise_for_status(response, method, path)
        if not response.content:
            return None
        return response.json()

    def _api_path(self, path: str) -> str:
        path = path if path.startswith("/") else f"/{path}"
        if path.startswith("/api/") or path == "/api":
            return path
        return f"/api{path}"

    def _raise_for_status(self, response: httpx.Response, method: str, path: str) -> None:
        if response.status_code < 400:
            return
        detail = None
        try:
            payload = response.json()
            detail = payload.get("detail")
        except Exception:
            detail = response.text
        raise WorkflowError(
            f"THOA API {method} {path} failed with {response.status_code}: {detail}"
        )

    # -------------------- DAG / workflow graph helpers --------------------
    def _job_node_key(self, job) -> str:
        return str(job.jobid)

    def _artifact_key(self, kind: str, path: str) -> str:
        return f"{kind}:{os.path.normpath(path)}"

    def _safe_rule_name(self, job) -> str:
        try:
            return job.rule.name
        except Exception:
            return getattr(job, "name", f"job-{self._job_node_key(job)}")

    def _safe_resources(self, job) -> Dict[str, Any]:
        try:
            return _jsonable(dict(job.resources.items()))
        except Exception:
            try:
                return _jsonable(dict(job.resources))
            except Exception:
                return {}

    def _safe_io_list(self, files) -> List[str]:
        try:
            return [str(f) for f in files]
        except Exception:
            return []

    def _build_graph_payload(self) -> Dict[str, Any]:
        dag = self.workflow.dag
        dag_jobs = list(dag.jobs)
        node_key_by_job = {job: self._job_node_key(job) for job in dag_jobs}

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []
        artifacts_by_key: Dict[str, Dict[str, Any]] = {}
        bindings: List[Dict[str, Any]] = []
        binding_seen: set[tuple[str, str, str]] = set()

        for job in dag_jobs:
            node_key = node_key_by_job[job]
            nodes.append(
                {
                    "node_key": node_key,
                    "rule_name": self._safe_rule_name(job),
                    "wildcards_json": _jsonable(getattr(job, "wildcards_dict", {})),
                    "metadata_json": {
                        "jobid": getattr(job, "jobid", None),
                        "threads": getattr(job, "threads", None),
                        "resources": self._safe_resources(job),
                        "inputs": self._safe_io_list(getattr(job, "input", [])),
                        "outputs": self._safe_io_list(getattr(job, "output", [])),
                        "is_group": bool(getattr(job, "is_group", lambda: False)()),
                    },
                }
            )

        # Control edges from Snakemake's resolved DAG dependencies.
        try:
            dependencies = dag.dependencies
        except Exception:
            dependencies = {}

        for consumer_job, producer_map in dependencies.items():
            consumer_key = node_key_by_job.get(consumer_job)
            if not consumer_key:
                continue
            for producer_job, dep_files in (producer_map or {}).items():
                producer_key = node_key_by_job.get(producer_job)
                if not producer_key:
                    continue
                edges.append(
                    {
                        "from_node_key": producer_key,
                        "to_node_key": consumer_key,
                        "edge_type": "data",
                        "metadata_json": {
                            "files": [str(f) for f in dep_files] if dep_files else [],
                        },
                    }
                )

        for job in dag_jobs:
            node_key = node_key_by_job[job]

            for io_file in getattr(job, "input", []):
                kind, path = _classify_artifact(io_file)
                artifact_key = self._artifact_key(kind, path)
                artifacts_by_key.setdefault(
                    artifact_key,
                    {
                        "artifact_key": artifact_key,
                        "kind": kind,
                        "declared_path": None if kind == "glob" else path,
                        "glob_pattern": path if kind == "glob" else None,
                        "metadata_json": {},
                    },
                )
                binding_key = (node_key, artifact_key, "input")
                if binding_key not in binding_seen:
                    binding_seen.add(binding_key)
                    bindings.append(
                        {
                            "node_key": node_key,
                            "artifact_key": artifact_key,
                            "io_role": "input",
                            "declared_path": path,
                            "is_optional": False,
                            "metadata_json": {},
                        }
                    )

            for io_file in getattr(job, "output", []):
                kind, path = _classify_artifact(io_file)
                artifact_key = self._artifact_key(kind, path)
                artifact = artifacts_by_key.setdefault(
                    artifact_key,
                    {
                        "artifact_key": artifact_key,
                        "kind": kind,
                        "declared_path": None if kind == "glob" else path,
                        "glob_pattern": path if kind == "glob" else None,
                        "metadata_json": {},
                    },
                )
                if not artifact.get("producer_node_key"):
                    artifact["producer_node_key"] = node_key

                binding_key = (node_key, artifact_key, "output")
                if binding_key not in binding_seen:
                    binding_seen.add(binding_key)
                    bindings.append(
                        {
                            "node_key": node_key,
                            "artifact_key": artifact_key,
                            "io_role": "output",
                            "declared_path": path,
                            "is_optional": False,
                            "metadata_json": {},
                        }
                    )

        return {
            "nodes": nodes,
            "edges": edges,
            "artifacts": list(artifacts_by_key.values()),
            "bindings": bindings,
            "replace_edges_and_bindings": True,
        }

    def _refresh_workflow_node_map(self) -> None:
        if not self._workflow_run_public_id:
            return
        graph = self._request("GET", f"/workflow_runs/{self._workflow_run_public_id}/graph")
        self._workflow_node_public_ids = {
            node["node_key"]: node["public_id"] for node in graph.get("nodes", [])
        }

    def _ensure_workflow_run(self) -> None:
        if self._workflow_run_public_id:
            return

        workdir = str(getattr(self.workflow, "workdir", Path.cwd()))
        workflow_name = (
            getattr(self.workflow.executor_settings, "workflow_name", None)
            or f"snakemake:{Path(workdir).name}"
        )
        external_run_id = getattr(self.workflow.executor_settings, "external_run_id", None)

        workflow_run = self._request(
            "POST",
            "/workflow_runs",
            json={
                "engine": "snakemake",
                "name": workflow_name,
                "external_run_id": external_run_id,
                "status": "syncing",
                "metadata_json": {
                    "workdir": workdir,
                    "executor_plugin": "thoa",
                },
            },
        )
        self._workflow_run_public_id = workflow_run["public_id"]

    def _sync_workflow_graph(self) -> None:
        self._ensure_workflow_run()
        payload = self._build_graph_payload()
        self._request(
            "POST",
            f"/workflow_runs/{self._workflow_run_public_id}/graph",
            json=payload,
        )
        self._refresh_workflow_node_map()
        self._workflow_graph_synced = True

    def _ensure_workflow_graph_for_job(self, job: JobExecutorInterface) -> str:
        node_key = self._job_node_key(job)

        if not self._workflow_graph_synced:
            self._sync_workflow_graph()
        elif getattr(self.workflow.executor_settings, "sync_graph_on_each_submit", False):
            self._sync_workflow_graph()
        elif node_key not in self._workflow_node_public_ids:
            # DAG can change with checkpoints; refresh when a job appears later.
            self._sync_workflow_graph()

        workflow_node_public_id = self._workflow_node_public_ids.get(node_key)
        if not workflow_node_public_id:
            raise WorkflowError(
                f"Failed to map Snakemake job {node_key} to a THOA workflow node after graph sync."
            )
        return workflow_node_public_id

    def _update_workflow_run_status(self, status: str) -> None:
        if not self._workflow_run_public_id:
            return
        try:
            self._request(
                "PUT",
                f"/workflow_runs/{self._workflow_run_public_id}",
                json={"status": status},
            )
        except Exception as e:
            self.logger.warning(f"Failed to update THOA workflow run status to {status}: {e}")

    # -------------------- THOA job submission helpers --------------------
    def _job_ram_gb(self, job: JobExecutorInterface) -> int:
        try:
            mem_mb = job.resources.get("mem_mb")
        except Exception:
            mem_mb = None
        if mem_mb is None:
            return int(getattr(self.workflow.executor_settings, "default_ram_gb", 8) or 8)
        try:
            return max(1, int(math.ceil(float(mem_mb) / 1024.0)))
        except Exception:
            return int(getattr(self.workflow.executor_settings, "default_ram_gb", 8) or 8)

    def _job_disk_gb(self, job: JobExecutorInterface) -> int:
        disk_mb = None
        try:
            disk_mb = job.resources.get("disk_mb")
        except Exception:
            disk_mb = None
        if disk_mb is None:
            try:
                disk_mb = job.resources.get("disk")
            except Exception:
                disk_mb = None

        if disk_mb is None:
            return int(getattr(self.workflow.executor_settings, "default_disk_gb", 20) or 20)
        try:
            return max(1, int(math.ceil(float(disk_mb) / 1024.0)))
        except Exception:
            return int(getattr(self.workflow.executor_settings, "default_disk_gb", 20) or 20)

    def _client_home(self) -> str:
        client_home = getattr(self.workflow.executor_settings, "client_home", None)
        return client_home or str(Path.home())

    def _build_jobscript_content(self, job: JobExecutorInterface) -> str:
        # Generate the standard Snakemake remote jobscript, then submit its contents to THOA.
        exec_job = self.format_job_exec(job)
        jobscript = Path(self.get_jobscript(job))
        self.write_jobscript(job, exec_job)
        return jobscript.read_text(encoding="utf-8")

    def _submit_to_thoa(
        self,
        job: JobExecutorInterface,
        workflow_node_public_id: str,
    ) -> str:
        script_content = self._build_jobscript_content(job)

        script_response = self._request(
            "POST",
            "/scripts",
            json={
                "name": f"snakemake-jobscript-{job.jobid}",
                "script_content": script_content,
                "description": f"Snakemake jobscript for rule {self._safe_rule_name(job)}",
                "security_status": "pending",
            },
        )

        try:
            attempt_index = int(getattr(job, "attempt", 1))
        except Exception:
            attempt_index = 1

        job_response = self._request(
            "POST",
            "/jobs",
            json={
                "requested_ram": self._job_ram_gb(job),
                "requested_cpu": max(1, int(getattr(job, "threads", 1) or 1)),
                "requested_disk_space": self._job_disk_gb(job),
                "has_input_data": False,
                "client_home": self._client_home(),
                "workflow_node_public_id": workflow_node_public_id,
                "workflow_attempt_index": attempt_index,
                "engine_task_id": str(job.jobid),
            },
        )

        update_payload = {
            "script_public_id": script_response["public_id"],
            "current_working_directory": str(getattr(self.workflow, "workdir", Path.cwd())),
            "output_directory": str(getattr(self.workflow, "workdir", Path.cwd())),
        }
        environment_public_id = getattr(self.workflow.executor_settings, "environment_public_id", None)
        if environment_public_id:
            update_payload["environment_public_id"] = environment_public_id

        self._request("PUT", f"/jobs/{job_response['public_id']}", json=update_payload)
        return str(job_response["public_id"])

    # -------------------- Snakemake executor interface --------------------
    def run_job(self, job: JobExecutorInterface):
        try:
            workflow_node_public_id = self._ensure_workflow_graph_for_job(job)
            thoa_job_public_id = self._submit_to_thoa(job, workflow_node_public_id)
            self._submitted_any_jobs = True
            self._update_workflow_run_status("running")
        except Exception as e:
            raise WorkflowError(f"Failed to submit Snakemake job {job.jobid} to THOA: {e}") from e

        job_info = SubmittedJobInfo(
            job=job,
            external_jobid=thoa_job_public_id,
            aux={
                "thoa_job_public_id": thoa_job_public_id,
                "workflow_run_public_id": self._workflow_run_public_id,
                "workflow_node_public_id": workflow_node_public_id,
            },
        )
        self.report_job_submission(job_info)
        self.logger.info(
            f"Submitted Snakemake job {job.jobid} (rule={self._safe_rule_name(job)}) "
            f"to THOA as {thoa_job_public_id}"
        )

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
        poll_seconds = int(getattr(self.workflow.executor_settings, "poll_seconds", 5) or 5)
        self.next_sleep_seconds = max(1, poll_seconds)

        for active_job in active_jobs:
            thoa_job_id = str(active_job.external_jobid)
            try:
                async with self.status_rate_limiter:
                    job_rows = await self._arequest("GET", "/jobs", params={"public_id": thoa_job_id})
            except Exception as e:
                self.logger.warning(
                    f"Failed to poll THOA status for job {thoa_job_id}: {e}. Retrying on next poll."
                )
                yield active_job
                continue

            if not job_rows:
                self._saw_job_error = True
                self.report_job_error(
                    active_job,
                    msg=f"THOA job {thoa_job_id} not found while polling status.",
                )
                continue

            thoa_status = str(job_rows[0].get("status"))

            if thoa_status in {JobStatus.COMPLETED, JobStatus.ARCHIVED}:
                self.report_job_success(active_job)
            elif thoa_status in {
                JobStatus.FAILED_UPLOAD,
                JobStatus.FAILED_VALIDATION,
                JobStatus.FAILED_PROVISIONING,
                JobStatus.FAILED_EXECUTION,
                JobStatus.CANCELLED,
                "submission_failed",
            }:
                self._saw_job_error = True
                self.report_job_error(
                    active_job,
                    msg=f"THOA job {thoa_job_id} ended in status '{thoa_status}'.",
                )
            else:
                yield active_job

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        self._cancel_requested = True
        for active_job in active_jobs:
            thoa_job_id = str(active_job.external_jobid)
            try:
                self._request("PUT", f"/jobs/{thoa_job_id}", json={"status": JobStatus.CANCELLED})
            except Exception as e:
                self.logger.warning(f"Failed to mark THOA job {thoa_job_id} as cancelled: {e}")

    def shutdown(self):
        try:
            if self._workflow_run_public_id:
                if self._cancel_requested:
                    self._update_workflow_run_status("cancelled")
                elif self._saw_job_error:
                    self._update_workflow_run_status("failed")
                elif self._submitted_any_jobs:
                    self._update_workflow_run_status("completed")
        finally:
            if self._sync_client is not None:
                self._sync_client.close()
                self._sync_client = None
            super().shutdown()
