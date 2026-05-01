import typer
from typing import Optional, List
import pathlib
from thoa.core.api_utils import api_client

from rich.table import Table
from rich.panel import Panel
from rich.console import Console
from rich.theme import Theme
from rich import print as rprint
from rich.spinner import Spinner
from rich.text import Text
from rich.live import Live
from rich.progress import Progress, ProgressBar, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn
from rich import box
from datetime import datetime, timezone
from thoa.core import resolve_environment_spec
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from thoa.config import settings

import concurrent.futures
from azure.storage.blob import BlobClient

import time
import hashlib
import mmap
from pathlib import Path
import os

from thoa.core.job_utils import (
    print_config,
    validate_user_command,
    collect_files,
    compute_md5_buffered,
    hash_all,
    file_sizes_in_bytes,
    current_job_status,
    current_job_detail,
    all_files_have_upload_links,
    upload_all,
    max_threads,
    console,
    _fmt_duration,
    _parse_job_timestamp,
)
from thoa.core.input_specs import parse_input_spec
from thoa.core.remote_inputs import (
    detect_input_source_kind,
    import_google_drive_input,
    project_input_context,
)
from thoa.core.job_status import JobStatus

max_threads = min(32, os.cpu_count() * 2)


def _fmt_bytes(n: int) -> str:
    if n < 1_024:
        return f"{n} B"
    if n < 1_048_576:
        return f"{n / 1_024:.1f} KB"
    if n < 1_073_741_824:
        return f"{n / 1_048_576:.1f} MB"
    return f"{n / 1_073_741_824:.2f} GB"


def _wait_queue(job_id: str) -> None:
    QUEUED = {JobStatus.CREATED, JobStatus.QUEUED}
    with Progress(SpinnerColumn(), TextColumn("[cyan]{task.description}[/cyan]"), console=console) as prog:
        task = prog.add_task("Waiting in queue...", total=None)
        while True:
            status = current_job_status(job_id)
            if status not in QUEUED:
                break
            detail = current_job_detail(job_id)
            pos = detail.get("queue_position")
            if pos is not None:
                ahead = pos - 1
                if ahead == 0:
                    label = "Next in queue"
                else:
                    label = f"{ahead} job{'s' if ahead != 1 else ''} ahead in queue"
                prog.update(task, description=label)
            time.sleep(4)


_LIVE_STEPS = [
    ("queued",            "Waiting in queue",   None),
    ("uploading",         "Uploading files",    None),
    ("provisioning",      "Provisioning VM",    None),
    ("staging",           "Staging files",      None),
    ("running",           "Running",            None),
    ("uploading_outputs", "Uploading outputs",  None),
    ("completed",         "Completed",          None),
]

_STAGING_BASE_SEC = 56
_STAGING_BPS      = 85 * 1024 * 1024


def _estimate_secs(key: str, total_bytes: int, queue_pos: int | None = None) -> int | None:
    if key == "staging": return int(_STAGING_BASE_SEC + total_bytes / _STAGING_BPS)
    if key == "queued":  return 30 if (queue_pos is None or queue_pos == 1) else None
    return {"provisioning": 180}.get(key)


def _fmt_estimate(secs: int | None) -> str | None:
    if secs is None: return None
    if secs < 60:    return "< 1 min"
    mins = round(secs / 60)
    if mins < 60:    return f"~{mins} min"
    h, m = divmod(mins, 60)
    return f"~{h}h {m}m" if m else f"~{h}h"

# If a step exceeds this many seconds, show a "taking longer than expected" warning.
# Only steps whose duration is independent of job/data size are included.
# staging, uploading, uploading_outputs, and running are all excluded because
# they scale with data volume and can legitimately run for hours on large jobs.
_STUCK_THRESHOLDS_SEC = {
    "queued":       5 * 60,   # Prefect pickup; >5 min likely means scheduler is down
    "provisioning": 10 * 60,  # VM spin-up; independent of data size
}

_LIVE_STEP_KEYS = [s[0] for s in _LIVE_STEPS]

_LIVE_FAILURE_STEP: dict[str, str] = {
    "failed_upload":       "uploading",
    "failed_validation":   "provisioning",
    "failed_provisioning": "provisioning",
    "failed_execution":    "running",
    "failed_startup":      "queued",
    # "cancelled" is handled dynamically in _build_table — the failed step is
    # whichever step was active when the user cancelled, not always "queued".
}

_LIVE_TERMINAL = {
    "completed", "archived",
    "failed_upload", "failed_validation", "failed_provisioning",
    "failed_execution", "failed_startup", "cancelled",
}


def _live_job_progress(job_id: str, upload_state: dict | None = None) -> None:
    """Show all canonical job steps in a single Rich Live display.

    Polls status_timestamps and updates spinner → checkmark per step.
    Exits when the job reaches any terminal or failure status.
    upload_state, if provided, is a dict with n_done/n_total/size_str updated
    by a concurrent upload thread so the 'uploading' row shows live file counts.
    """
    def _build_table(detail: dict) -> Table:
        ts = detail.get("status_timestamps") or {}
        status = detail.get("status", "")
        files_staged = detail.get("files_staged") or 0
        files_total = detail.get("files_total")
        finished_at = detail.get("finished_at")
        queue_pos = detail.get("queue_position")
        input_ds = detail.get("input_dataset") or {}
        total_bytes = input_ds.get("total_size") or 0

        is_cancelled = status == "cancelled"
        if is_cancelled:
            interrupted_step = next((k for k in reversed(_LIVE_STEP_KEYS) if ts.get(k)), "queued")
            failed_step = None
        else:
            interrupted_step = None
            failed_step = _LIVE_FAILURE_STEP.get(status)
        is_complete = status in ("completed", "archived")

        table = Table(box=None, padding=(0, 1), show_header=False, expand=False)
        table.add_column("icon", no_wrap=True, width=3)
        table.add_column("label", min_width=20)
        table.add_column("bar", no_wrap=True, width=22)
        table.add_column("detail")

        for (key, label, _) in _LIVE_STEPS:
            est_sec  = _estimate_secs(key, total_bytes, queue_pos)
            estimate = _fmt_estimate(est_sec)
            start_ts = ts.get(key)

            if is_complete:
                state = "done" if start_ts else "skip"
            elif interrupted_step:
                interrupted_idx = _LIVE_STEP_KEYS.index(interrupted_step)
                this_idx = _LIVE_STEP_KEYS.index(key)
                if key == interrupted_step:
                    state = "cancelled"
                elif this_idx < interrupted_idx:
                    state = "done" if start_ts else "skip"
                else:
                    state = "skip"
            elif failed_step:
                failed_idx = _LIVE_STEP_KEYS.index(failed_step)
                this_idx = _LIVE_STEP_KEYS.index(key)
                if key == failed_step:
                    state = "failed"
                elif this_idx < failed_idx:
                    state = "done" if start_ts else "skip"
                else:
                    state = "skip"
            elif start_ts and key == status:
                state = "active"
            elif start_ts:
                state = "done"
            elif key == status:
                state = "active"
            else:
                state = "pending"

            if state == "skip":
                continue

            if state == "done":
                icon = Text("✓", style="bold green")
            elif state == "active":
                icon = Spinner("dots", style="cyan")
            elif state == "failed":
                icon = Text("✗", style="bold red")
            elif state == "cancelled":
                icon = Text("⊘", style="bold yellow")
            else:
                icon = Text("·", style="dim")

            style_map = {"done": "green", "active": "bold cyan", "failed": "red", "cancelled": "yellow", "pending": "dim"}
            label_text = Text(label, style=style_map[state])

            # Progress bar — "running" is a black box so no bar is shown.
            if key == "running":
                bar: ProgressBar | Text = Text("")
            elif state == "done":
                bar = ProgressBar(total=100, completed=100, width=20,
                                  complete_style="green", finished_style="green")
            elif state == "active":
                if key == "uploading" and upload_state and upload_state.get("n_total"):
                    pct = int((upload_state["n_done"] / upload_state["n_total"]) * 100)
                elif key == "staging" and files_total:
                    pct = int((files_staged / files_total) * 100)
                elif start_ts:
                    start_dt = _parse_job_timestamp(start_ts)
                    elapsed = (datetime.now(timezone.utc) - start_dt).total_seconds()
                    est_sec = est_sec or 60
                    pct = int(min(elapsed / est_sec, 0.95) * 100)
                else:
                    pct = 0
                bar = ProgressBar(total=100, completed=pct, width=20, complete_style="cyan")
            elif state == "failed":
                bar = ProgressBar(total=100, completed=100, width=20,
                                  complete_style="red", finished_style="red")
            elif state == "cancelled":
                bar = ProgressBar(total=100, completed=100, width=20,
                                  complete_style="yellow", finished_style="yellow")
            else:  # pending
                bar = ProgressBar(total=100, completed=0, width=20, style="bar.back")

            detail_parts: list[str] = []
            if state == "done" and start_ts:
                end_ts = next((ts[k] for k in _LIVE_STEP_KEYS[_LIVE_STEP_KEYS.index(key)+1:] if ts.get(k)), None)
                if not end_ts:
                    end_ts = finished_at
                if end_ts:
                    detail_parts.append(_fmt_duration(start_ts, end_ts))
                if key == "uploading":
                    if upload_state and upload_state.get("n_total"):
                        n = upload_state["n_total"]
                        sz = upload_state.get("size_str", "")
                        detail_parts.append(f"{n} {'file' if n == 1 else 'files'}{' · ' + sz if sz else ''}")
                    elif input_ds.get("number_of_files"):
                        n = input_ds["number_of_files"]
                        detail_parts.append(f"{n} {'file' if n == 1 else 'files'} · {_fmt_bytes(input_ds.get('total_size') or 0)}")
                elif key == "staging":
                    n = files_total or input_ds.get("number_of_files")
                    if n:
                        sz = _fmt_bytes(input_ds.get("total_size") or 0)
                        detail_parts.append(f"{n} {'file' if n == 1 else 'files'}{' · ' + sz if sz else ''}")
            elif state == "active":
                if start_ts:
                    now_iso = datetime.now(timezone.utc).isoformat()
                    verb = "waiting for" if key == "queued" else "running for"
                    detail_parts.append(f"{verb} {_fmt_duration(start_ts, now_iso)}")
                if key == "queued" and queue_pos is not None:
                    ahead = queue_pos - 1
                    if ahead == 0:
                        detail_parts.append("next in queue")
                    elif ahead > 0:
                        detail_parts.append(f"{ahead} job{'s' if ahead != 1 else ''} ahead")
                elif key == "uploading" and upload_state:
                    n_done = upload_state["n_done"]
                    n_total = upload_state["n_total"]
                    size_str = upload_state.get("size_str", "")
                    count_str = f"{n_done}/{n_total} files"
                    detail_parts.append(f"{count_str}  {size_str}" if size_str else count_str)
                elif key == "staging" and files_total:
                    detail_parts.append(f"{files_staged}/{files_total} files")
                if estimate:
                    detail_parts.append(f"est. {estimate}")
            elif state == "failed":
                detail_parts.append("failed")
            elif state == "cancelled":
                detail_parts.append("cancelled")

            # Stuck-step warning: append after other detail parts so it's visible
            is_stuck = False
            if state == "active" and start_ts:
                stuck_threshold = _STUCK_THRESHOLDS_SEC.get(key)
                if stuck_threshold:
                    start_dt = _parse_job_timestamp(start_ts)
                    elapsed_sec = (datetime.now(timezone.utc) - start_dt).total_seconds()
                    is_stuck = elapsed_sec > stuck_threshold

            plain = Text("  ".join(detail_parts), style="dim")
            if is_stuck:
                detail_text = Text.assemble(plain, ("  ⚠ taking longer than expected", "yellow"))
            else:
                detail_text = plain
            table.add_row(icon, label_text, bar, detail_text)

        return table

    with Live(console=console, refresh_per_second=4) as live:
        while True:
            detail = current_job_detail(job_id)
            status = detail.get("status", "")
            live.update(_build_table(detail))
            if status in _LIVE_TERMINAL:
                break
            time.sleep(2)


def _print_timeline_summary(job_id: str) -> None:
    detail = current_job_detail(job_id)
    ts = detail.get("status_timestamps") or {}
    if not ts:
        return

    STEP_LABELS = {
        "queued":            "Waiting in queue",
        "uploading":         "Uploading files",
        "provisioning":      "Provisioning VM",
        "staging":           "Staging files",
        "running":           "Running",
        "uploading_outputs": "Uploading outputs",
        "completed":         "Completed",
    }
    STEP_ORDER = [
        "queued", "uploading", "provisioning",
        "staging", "running", "uploading_outputs", "completed",
    ]

    input_ds = detail.get("input_dataset") or {}

    table = Table(title="Job Timeline", box=box.SIMPLE_HEAD)
    table.add_column("Step", style="cyan")
    table.add_column("Duration", style="yellow")
    table.add_column("Details", style="dim")

    for i, key in enumerate(STEP_ORDER):
        if key not in ts:
            continue
        label = STEP_LABELS.get(key, key)
        end_ts = next((ts[k] for k in STEP_ORDER[i + 1:] if k in ts), None)
        duration = _fmt_duration(ts[key], end_ts) if end_ts else "—"
        file_info = ""
        if key in ("uploading", "staging") and input_ds.get("number_of_files"):
            n = input_ds["number_of_files"]
            sz = _fmt_bytes(input_ds.get("total_size") or 0)
            file_info = f"{n} {'file' if n == 1 else 'files'} · {sz}"
        table.add_row(label, duration, file_info)

    console.print(table)


def _print_env_build_failure(job_id: str) -> None:
    console.print("\n[bold red]Environment Build Failed[/bold red]")
    console.print("[red]The environment could not be validated.[/red]\n")
    try:
        detail = api_client.get(f"/jobs/{job_id}/detail")
        build_logs = (detail or {}).get("environment", {}).get("build_logs")
        if build_logs:
            console.print("[bold yellow]Environment Build Logs:[/bold yellow]")
            console.print(build_logs)
    except Exception:
        pass


def _print_dry_run_summary(
    n_files: int,
    total_size_bytes: int,
    dataset_source: str,
    estimate,
    validation_passed: bool,
):
    if total_size_bytes < 1024:
        size_str = f"{total_size_bytes} B"
    elif total_size_bytes < 1024 ** 2:
        size_str = f"{total_size_bytes / 1024:.1f} KB"
    elif total_size_bytes < 1024 ** 3:
        size_str = f"{total_size_bytes / 1024 ** 2:.1f} MB"
    else:
        size_str = f"{total_size_bytes / 1024 ** 3:.2f} GB"

    table = Table(show_header=False, box=None, expand=False, padding=(0, 1))

    if dataset_source == "upload":
        table.add_row("[label]Input Files[/label]", f"[value]{n_files} file(s) ({size_str})[/value]")
        table.add_row("[label]Dataset Source[/label]", "[value]New upload[/value]")
    elif dataset_source == "existing":
        table.add_row("[label]Input Dataset[/label]", f"[value]{n_files} file(s) ({size_str})[/value]")
        table.add_row("[label]Dataset Source[/label]", "[value]Existing dataset (no upload)[/value]")
    else:
        table.add_row("[label]Input Files[/label]", "[value]None[/value]")

    if estimate:
        table.add_row("", "")
        table.add_row("[label]Estimated cost[/label]", f"[value]{estimate['min_credits_per_hour']:.1f} credits/hr[/value]")
        table.add_row("[label]Price range[/label]",    f"[value]{estimate['min_credits_per_hour']:.1f} – {estimate['max_credits_per_hour']:.1f} credits/hr[/value]")
    else:
        table.add_row("[label]Cost Estimate[/label]", "[warning]Unavailable — no matching VMs found for requested specs[/warning]")

    table.add_row("", "")
    if validation_passed:
        table.add_row("[label]Validation[/label]", "[bold green]✓ Passed — job would be accepted[/bold green]")
    else:
        table.add_row("[label]Validation[/label]", "[bold red]✗ Failed — see errors above[/bold red]")

    console.print(Panel(table, title="[bold yellow]Dry Run Summary[/bold yellow]", expand=False, border_style="yellow"))
    console.print("[yellow]Dry run complete. No job was submitted.[/yellow]")


def run_cmd(
    inputs: Optional[List[str]] = None,
    input_dataset: Optional[str] = None,
    output: Optional[List[str]] = None,
    n_cores: Optional[int] = None,
    ram: Optional[int] = None,
    storage: Optional[int] = None,
    tools: Optional[List[str]] = None,
    env_source: Optional[str] = None,
    env_id: Optional[str] = None,
    cmd: str = "",
    download_path: Optional[str] = None,
    run_async: bool = False,
    job_name: Optional[str] = None,
    job_description: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False,
    has_input_data: bool = True,
    use_existing_input_dataset: bool = False,
):
    
    """Run the job with the given configuration using the Bioconda-based execution environment."""

    # Transitional CLI behavior:
    # - local --input remains unchanged
    # - Google Drive input moves from --input-source to --input <url>::<mount_path>
    # - dataset ids intentionally stay on --input-dataset
    parsed_inputs = [parse_input_spec(raw) for raw in (inputs or [])]
    local_specs = [spec for spec in parsed_inputs if spec.kind == "local"]
    remote_specs = [spec for spec in parsed_inputs if spec.kind == "google_drive"]
    unknown_specs = [spec for spec in parsed_inputs if spec.kind == "unknown"]

    if unknown_specs:
        console.print(
            "[bold red]Error:[/bold red] Unsupported --input value(s): "
            + ", ".join(spec.raw for spec in unknown_specs)
        )
        raise typer.Exit(code=1)
    if len(remote_specs) > 1:
        console.print("[bold red]Error:[/bold red] Multiple remote inputs are not supported yet.")
        raise typer.Exit(code=1)
    if remote_specs and local_specs:
        console.print("[bold red]Error:[/bold red] Cannot combine local and remote --input values yet.")
        raise typer.Exit(code=1)

    remote_input_context = None

    if remote_specs:
        remote_spec = remote_specs[0]
        source_kind = detect_input_source_kind(remote_spec.source)
        if source_kind == "s3":
            console.print("[bold red]Error:[/bold red] S3 inputs are not implemented yet.")
            raise typer.Exit(code=1)
        if source_kind != "google_drive":
            console.print("[bold red]Error:[/bold red] Unsupported remote --input value.")
            raise typer.Exit(code=1)
        if input_dataset:
            console.print(
                "[bold red]Error:[/bold red] Cannot combine --input-dataset with remote --input."
            )
            raise typer.Exit(code=1)
        if not remote_spec.mount_path:
            console.print(
                "[bold red]Error:[/bold red] Google Drive input requires "
                "<gdrive_url>::<mount_path>."
            )
            raise typer.Exit(code=1)

        imported_input = import_google_drive_input(remote_spec.source)
        input_root = os.path.abspath(str(remote_spec.mount_path))
        input_dataset = str(imported_input["dataset_public_id"])
        remote_input_context = project_input_context(
            input_root,
            imported_input.get("input_context") or {},
        )
        inputs = []
        use_existing_input_dataset = True

    # Local inputs intentionally keep the old behavior in this PR.
    inputs = [spec.source for spec in local_specs]

    if input_dataset and inputs:
        console.print(
            "[bold red]Error:[/bold red] Cannot specify both --input and --input-dataset options at the same time. Please choose one or the other."
        )
        exit(1)

    if input_dataset:
        all_files = []
        input_dataset = input_dataset.strip()
        input_dataset_response = api_client.get(f"/datasets?public_id={input_dataset}&include_adjusted_context=True&include_jobs_as_input=False&include_jobs_as_output=False")[0]
        if input_dataset_response.get("deletion_pending"):
            console.print("[bold red]Error:[/bold red] Dataset is pending deletion and cannot be used as input.")
            raise typer.Exit(code=1)

        dataset_size_bytes = input_dataset_response.get("total_size") or 0
        disk_size_bytes = storage * (1024 ** 3)
        if dataset_size_bytes > disk_size_bytes:
            size_gb = dataset_size_bytes / (1024 ** 3)
            console.print(
                f"[bold red]Error:[/bold red] Dataset size ({size_gb:.1f} GB) exceeds "
                f"requested disk space ({storage} GB). Re-run with --storage {int(size_gb) + 1} or larger."
            )
            raise typer.Exit(code=1)

    elif inputs:
        all_files = collect_files(inputs)
        if len(all_files) > 1000:
            console.print(
                "[bold red]Error:[/bold red] More than 1000 input files detected. "
                "This amount is currently not supported. "
                "Please consider compressing your files into an archive and try again."
            )
            raise typer.Exit(code=1)

    else: 
        pass

    print_config(
        inputs=inputs,
        input_dataset=input_dataset,
        output=output,
        n_cores=n_cores,
        ram=ram,
        storage=storage,
        tools=tools,
        env_source=env_source,
        cmd=cmd,
        download_path=download_path,
        run_async=run_async,
        job_name=job_name,
        job_description=job_description,
        dry_run=dry_run,
        verbose=verbose,
    )
    
    # STEP 0: Validate that the user has sufficient resources to run the job
    valid = validate_user_command(n_cores=n_cores, ram=ram, storage=storage)

    if not valid:
        if dry_run:
            _print_dry_run_summary(
                n_files=0,
                total_size_bytes=0,
                dataset_source="none",
                estimate=None,
                validation_passed=False,
            )
        return

    if dry_run:
        if input_dataset:
            total_size_bytes = input_dataset_response.get("total_size") or 0
            n_files = len(input_dataset_response.get("adjusted_context") or {})
            dataset_source = "existing"
        elif inputs:
            file_sizes = file_sizes_in_bytes(all_files)
            total_size_bytes = sum(file_sizes.values())
            n_files = len(all_files)
            dataset_source = "upload"
        else:
            total_size_bytes = 0
            n_files = 0
            dataset_source = "none"

        estimate = api_client.get(
            "/azure_prices/estimate",
            params={"n_cores": n_cores, "ram": ram, "limit": 10}
        )

        _print_dry_run_summary(
            n_files=n_files,
            total_size_bytes=total_size_bytes,
            dataset_source=dataset_source,
            estimate=estimate,
            validation_passed=True,
        )
        raise typer.Exit(code=0)


    # STEP 1: Validate the user inputs
    with console.status(f"Starting Job Submission Workflow", spinner="dots12"):

        script_response = api_client.post("/scripts", json={
            "name": f"{job_name} script" or "Untitled Script",
            "script_content": cmd,
            "description": job_description or "No description provided",
            "security_status": "pending"
        })

        current_working_directory = str(Path.cwd())
        client_home = str(Path.home())

        job_response = api_client.post("/jobs", json={
            "requested_ram": ram,
            "requested_cpu": n_cores,
            "requested_disk_space": storage,
            "has_input_data": has_input_data,
            "client_home": client_home,
            "use_existing_input_dataset": use_existing_input_dataset,
        })

        # Always set script/cwd/output metadata so backend can build run_command
        # and mount flags even when no input files are provided.
        job_update_payload = {
            "script_public_id": script_response["public_id"],
            "current_working_directory": str(current_working_directory),
            "download_directory": str(download_path),
            "output_directory": str(output),
        }

        if input_dataset:
            job_update_payload["input_dataset_public_id"] = input_dataset
            job_update_payload["input_context"] = (
                remote_input_context
                if remote_input_context is not None
                else input_dataset_response.get("adjusted_context", {})
            )

        updated_job_response = api_client.put(
            f"/jobs/{job_response['public_id']}",
            json=job_update_payload,
        )

        # print(f"Job started successfully. View at: {job_response.get("public_id")}")
        console.print(
            f"[bold green]Job started successfully. View at:[/bold green][bold cyan] {settings.THOA_UI_URL}/workbench/jobs/{job_response.get('public_id')}[/bold cyan]")

    # STEP 2: Resolve the environment and attach it to the job
    if env_id:
        with console.status("Looking up environment", spinner="dots12"):
            env_results = api_client.get(f"/environments?public_id={env_id}")
            if not env_results:
                console.print(f"[bold red]Error:[/bold red] No environment found with ID [cyan]{env_id}[/cyan]. Use [bold]thoa envs list[/bold] to see your environments.")
                return
            environment_details = env_results[0] if isinstance(env_results, list) else env_results
        console.print(f"[green]Using existing environment[/green] [cyan]{env_id}[/cyan] (status: {environment_details.get('env_status', '?')})")
        api_client.put(
            f"/jobs/{job_response['public_id']}",
            json={"environment_public_id": environment_details["public_id"]},
        )
    else:
        with console.status("Packaging Environment", spinner="dots12"):
            tool_list = tools.split(",") if tools else []
            env_spec = resolve_environment_spec(env_source=env_source)

            environment_details = api_client.post("/environments",
                json={
                    "tools": tool_list,
                    "env_string": env_spec
                }
            )
            if not environment_details:
                console.print("[bold red]Failed to create environment. Please check your configuration.[/bold red]")
                return

            api_client.put(
                f"/jobs/{job_response['public_id']}",
                json={"environment_public_id": environment_details["public_id"]},
            )


    # STEP 3: Trigger validation of the environment ASYNC 
    def validate_env_background():

        """Background thread to validate the environment."""

        env_validation_result = {"env_status": "pending"}

        while env_validation_result.get("env_status") not in ("validated", "validation_failed"):
            try:
                env_validation_result = api_client.get(
                    f"/environments/{environment_details['public_id']}/validate"
                )
                time.sleep(4)
            except:
                time.sleep(1)

    validation_thread = Thread(target=validate_env_background)
    validation_thread.start()


    # STEP 4: Hash the file objects and create them on the server, as well as the input dataset object
    with console.status(f"Hashing File Objects", spinner="dots12"):

        # No inputs provided at all
        if not input_dataset and not inputs:
            console.print("[yellow]No input files specified. Skipping input upload.[/yellow]")
            new_input_dataset = None
            names_to_public_ids = {}

        elif input_dataset:
            console.print(f"[green]Using dataset {input_dataset} as job input.[/green]")
            console.print("[yellow]Using existing input dataset. Files will be staged under:[/yellow]")
            rel_paths = list(input_dataset_response.get("adjusted_context", {}).keys())
            if len(rel_paths) <= 5:
                for rel_path in rel_paths:
                    console.print(f"  ./{rel_path}")
            else:
                for rel_path in rel_paths[:4]:
                    console.print(f"  ./{rel_path}")
                console.print(f"  ...")
                console.print(f"  ./{rel_paths[-1]}")
            new_input_dataset = None
            names_to_public_ids = {}

        elif inputs:

            all_files = collect_files(inputs)
            file_sizes = file_sizes_in_bytes(all_files)
            all_hashes = hash_all(all_files)
            file_responses = []
            local_path_by_public_id = {}

            for path, size in file_sizes.items():
                response = api_client.post("/files", json={
                    "filename": str(path),
                    "md5sum": all_hashes[path],
                    "size": size,
                })
                file_responses.append(response)
                local_path_by_public_id[response["public_id"]] = str(path)

            names_to_public_ids = {f['filename']: f['public_id'] for f in file_responses}

            new_input_dataset = api_client.post("/datasets", json={
                "files": [f['public_id'] for f in file_responses],
            })

        # Only update if we have an input dataset
        if new_input_dataset:
            updated_job_response = api_client.put(
                f"/jobs/{job_response['public_id']}",
                json={
                    "input_dataset_public_id": new_input_dataset["public_id"],
                    "input_context": names_to_public_ids 
                }
            )
            
    upload_state: dict | None = None
    upload_thread: Thread | None = None

    if new_input_dataset:
        # Use the actual scanned local path, not FileModel.filename from the API,
        # because dedup may reuse an existing file row with an old filename.
        file_map = dict(local_path_by_public_id)
        md5_map = {
            public_id: all_hashes[Path(local_path)]
            for public_id, local_path in local_path_by_public_id.items()
        }
        n_upload_files = len(file_responses)
        total_upload_bytes = sum(file_sizes.values())

        if run_async:
            # Async path: upload synchronously so the job is ready before we return.
            _wait_queue(updated_job_response['public_id'])

            with console.status("Creating upload URLs for your files", spinner="dots12"):
                while not all_files_have_upload_links(
                    updated_job_response['public_id'],
                    new_input_dataset['public_id'],
                    [f.get("public_id") for f in file_responses]
                ):
                    time.sleep(4)
                upload_links = api_client.get("/temporary_links", params={
                    "dataset_public_id": new_input_dataset['public_id'],
                    "job_public_id": updated_job_response['public_id'],
                    "link_type": "upload",
                })
                file_link_map = {link["file_public_id"]: link for link in upload_links}

            for file_public_id, link in file_link_map.items():
                api_client.put(
                    f"/temporary_links/{link['public_id']}",
                    json={"client_path": file_map.get(file_public_id)},
                )

            _size_str = _fmt_bytes(total_upload_bytes)
            with Progress(
                SpinnerColumn(),
                TextColumn("[cyan]{task.description}[/cyan]"),
                BarColumn(),
                TimeElapsedColumn(),
                console=console,
            ) as up_progress:
                up_task = up_progress.add_task(
                    f"Uploading 0/{n_upload_files} files · {_size_str}",
                    total=n_upload_files,
                )
                missing = upload_all(
                    upload_links, file_map, md5_map,
                    max_workers=max_threads, progress=up_progress, task_id=up_task,
                    n_total=n_upload_files, size_str=_size_str,
                )

            for p in missing:
                console.print(f"[yellow]⚠ File not found, skipped: {p}[/yellow]")

            console.print(Panel(
                f"[bold green]Job submitted successfully![/bold green]\n\n"
                f"[label]Job ID:[/label]    [value]{job_response['public_id']}[/value]\n"
                f"[label]Status:[/label]    [value]{current_job_status(updated_job_response['public_id'])}[/value]\n"
                f"[label]View:[/label]      [value]{settings.THOA_UI_URL}/workbench/jobs/{job_response['public_id']}[/value]\n"
                f"[label]Attach:[/label]    [value]thoa jobs attach {job_response['public_id']}[/value]",
                title="[title]Job Submitted (async)[/title]",
                expand=False,
                border_style="green"
            ))
            return

        else:
            # Sync path: start upload in background so the live display covers all
            # steps — queue waiting, uploading, provisioning — in one unified table.
            upload_state = {
                "n_done": 0,
                "n_total": n_upload_files,
                "size_str": _fmt_bytes(total_upload_bytes),
            }
            _job_id = updated_job_response['public_id']
            _dataset_id = new_input_dataset['public_id']
            _file_ids = [f.get("public_id") for f in file_responses]

            def _run_upload() -> None:
                while not all_files_have_upload_links(_job_id, _dataset_id, _file_ids):
                    time.sleep(2)
                links = api_client.get("/temporary_links", params={
                    "dataset_public_id": _dataset_id,
                    "job_public_id": _job_id,
                    "link_type": "upload",
                })
                # The server may return fewer links than file_responses when some
                # blobs already exist in storage (dedup). Correct the total now so
                # the progress bar reflects the actual upload count.
                actual_bytes = sum(
                    file_sizes.get(Path(file_map.get(lnk["file_public_id"], "")), 0)
                    for lnk in links
                )
                upload_state["n_total"] = len(links)
                upload_state["size_str"] = _fmt_bytes(actual_bytes)
                for link in links:
                    api_client.put(
                        f"/temporary_links/{link['public_id']}",
                        json={"client_path": file_map.get(link["file_public_id"])},
                    )
                upload_state["missing_files"] = upload_all(links, file_map, md5_map, max_workers=max_threads, upload_state=upload_state)

            upload_thread = Thread(target=_run_upload, daemon=True)
            upload_thread.start()

    if run_async:
        console.print(Panel(
            f"[bold green]Job submitted successfully![/bold green]\n\n"
            f"[label]Job ID:[/label]   [value]{job_response['public_id']}[/value]\n"
            f"[label]Status:[/label]   [value]{current_job_status(updated_job_response['public_id'])}[/value]\n"
            f"[label]View:[/label]     [value]{settings.THOA_UI_URL}/workbench/jobs/{job_response['public_id']}[/value]",
            title="[title]Job Submitted (async)[/title]",
            expand=False,
            border_style="green"
        ))
        return

    # Single unified live display covering all steps: queue → upload → provision → run
    _live_job_progress(updated_job_response['public_id'], upload_state=upload_state)

    if upload_thread is not None:
        upload_thread.join()

    if upload_state:
        for p in upload_state.get("missing_files", []):
            console.print(f"[yellow]⚠ File not found, skipped: {p}[/yellow]")

    # Stream logs after the live display exits — job is already complete so all
    # buffered log events replay instantly without interfering with the live table.
    api_client.stream_logs_blocking(updated_job_response['public_id'], from_id="0-0")

    final_status = current_job_status(updated_job_response['public_id'])
    if final_status == JobStatus.FAILED_VALIDATION:
        _print_env_build_failure(updated_job_response['public_id'])
        raise typer.Exit(code=1)
    if final_status == JobStatus.CANCELLED:
        console.print("[bold yellow]Job was cancelled.[/bold yellow]")
        raise typer.Exit(code=1)
    if final_status in {
        JobStatus.FAILED_PROVISIONING, JobStatus.FAILED_STARTUP,
        JobStatus.FAILED_EXECUTION,
    }:
        console.print(f"[bold red]Job failed with status: {final_status}[/bold red]")
        raise typer.Exit(code=1)



    _print_timeline_summary(updated_job_response['public_id'])

    if download_path:
        job_with_output = api_client.get(f"/jobs?public_id={updated_job_response['public_id']}")[0]
        output_dataset_id = job_with_output.get("output_dataset_public_id")

        if not output_dataset_id:
            console.print("[yellow]No output dataset found for this job; skipping download.[/yellow]")
            return

        output_links = api_client.get(
            "/temporary_links",
            params={
                "dataset_public_id": output_dataset_id,
                "job_public_id": updated_job_response['public_id'],
                "link_type": "download_outputs"
            }
        )

        if not output_links:
            console.print("[yellow]No output files available to download.[/yellow]")
            return

        for link in output_links:
            remote_output_path_parent = Path(output)
            local_output_path = Path(download_path)
            remote_link_path = Path(link.get("client_path"))
            local_link_path = Path(str(remote_link_path).replace(str(remote_output_path_parent), str(local_output_path)))

            if not local_link_path.parent.exists():
                local_link_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                sas_url = link["url"]
                blob = BlobClient.from_blob_url(sas_url)
                stream = blob.download_blob(max_concurrency=4)
                with open(local_link_path, "wb") as fh:
                    for chunk in stream.chunks():
                        fh.write(chunk)
                try:
                    remote_md5 = (blob.get_blob_properties().metadata or {}).get("md5")
                    if remote_md5:
                        local_md5 = compute_md5_buffered(local_link_path)
                        if local_md5 != remote_md5:
                            console.print(f"[yellow]MD5 mismatch for {local_link_path.name}[/yellow]")
                except Exception:
                    pass
            except Exception as e:
                console.print(f"[red]Failed to download: {e}[/red]")
                
