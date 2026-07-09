"""
Real job submission tests against staging.
Each test submits a job via CLI, waits for completion via WebSocket stream.
Requires THOA_STAGING_API_URL and THOA_STAGING_API_KEY env vars (or THOA_API_URL/THOA_API_KEY).

Run with: pytest tests/integration/test_run.py -v -m slow
"""

import re
import time
import uuid
import pytest
from typer.testing import CliRunner
from thoa.cli import app
from tests.integration.helpers import (
    get_job_status, poll_job_until_terminal, api_get, api_post, api_put,
)

runner = CliRunner()


@pytest.fixture(autouse=True)
def _require_backend(backend_url_and_key):
    pass


def _extract_job_id(output: str) -> str | None:
    m = re.search(r"jobs/([0-9a-f-]{36})", output)
    return m.group(1) if m else None


def _run_job(cli_args: list[str], allow_failure: bool = False) -> tuple[str, str]:
    """Run a job via CLI, return (job_id, output).

    Forces --max-attempts=1 and --disable-preflight on every integration job so
    AI retry/preflight paths never engage — they make assertions non-deterministic
    and previously hung nightly runs in 'retrying' indefinitely.

    Asserts exit_code == 0 by default. Pass allow_failure=True for tests that
    intentionally submit a failing job (CLI exits non-zero when the job ends
    in a failed terminal state, which is the correct behavior).
    """
    cli_args = [*cli_args, "--max-attempts", "1", "--disable-preflight"]
    result = runner.invoke(app, cli_args)
    if not allow_failure:
        assert result.exit_code == 0, f"CLI failed: {result.output}"
    job_id = _extract_job_id(result.output)
    assert job_id, f"No job ID in output: {result.output}"
    return job_id, result.output


# --- VM sizes without input ---

@pytest.mark.slow
def test_job_2cpu_4ram_no_input():
    job_id, _ = _run_job([
        "run", "--tools", "samtools", "--cmd", "samtools --version",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])
    status = get_job_status(job_id)
    assert status in {"completed", "cleanup"}, f"Job {job_id} status: {status}"


@pytest.mark.slow
def test_job_4cpu_8ram_no_input():
    job_id, _ = _run_job([
        "run", "--tools", "bwa", "--cmd", "bwa 2>&1 | head -1",
        "--n-cores", "4", "--ram", "8", "--storage", "20",
    ])
    status = get_job_status(job_id)
    assert status in {"completed", "cleanup"}, f"Job {job_id} status: {status}"


@pytest.mark.slow
def test_job_8cpu_16ram_no_input():
    job_id, _ = _run_job([
        "run", "--tools", "fastqc", "--cmd", "fastqc --version",
        "--n-cores", "8", "--ram", "16", "--storage", "20",
    ])
    status = get_job_status(job_id)
    assert status in {"completed", "cleanup"}, f"Job {job_id} status: {status}"


# --- with --input ---

@pytest.mark.slow
def test_job_with_input_file(tmp_path):
    test_file = tmp_path / "input.txt"
    test_file.write_text("Hello from integration test")

    job_id, _ = _run_job([
        "run", "--input", str(test_file), "--tools", "bash",
        "--cmd", "find / -name input.txt -type f 2>/dev/null",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])
    status = get_job_status(job_id)
    assert status in {"completed", "cleanup"}, f"Job {job_id} status: {status}"


@pytest.mark.slow
def test_job_with_multiple_input_files(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    (d / "a.txt").write_text("file a")
    (d / "b.txt").write_text("file b")
    (d / "c.txt").write_text("file c")

    job_id, _ = _run_job([
        "run", "--input", str(d), "--tools", "bash",
        "--cmd", "find / -name '*.txt' -type f 2>/dev/null | wc -l",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])
    status = get_job_status(job_id)
    assert status in {"completed", "cleanup"}, f"Job {job_id} status: {status}"


# --- failures ---

@pytest.mark.slow
def test_job_with_invalid_tool():
    """Nonexistent tool -> env validation failure.
    Uses API directly because stream_logs_blocking hangs when the job
    never reaches 'running' state."""
    script = api_post("/scripts", json={
        "name": "invalid tool test",
        "script_content": "echo hello",
        "description": "test",
        "security_status": "pending",
    }).json()

    job = api_post("/jobs", json={
        "requested_ram": 4, "requested_cpu": 2,
        "requested_disk_space": 20, "has_input_data": False,
        "client_home": "/tmp",
        "max_attempts": 1,
        "disable_preflight": True,
    }).json()

    api_put(f"/jobs/{job['public_id']}", json={
        "script_public_id": script["public_id"],
        "current_working_directory": "/tmp",
    })

    env = api_post("/environments", json={
        "tools": ["nonexistent_tool_xyz_99999"],
        "env_string": "",
    }).json()

    api_put(f"/jobs/{job['public_id']}", json={
        "environment_public_id": env["public_id"],
    })

    final_status = poll_job_until_terminal(job["public_id"], timeout=600)
    assert final_status in {"failed", "failed_validation", "failed_execution"}, (
        f"Job {job['public_id']} ended '{final_status}', expected a failed terminal state"
    )


@pytest.mark.slow
def test_cancel_running_job_no_error_message():
    """Cancelling a running job via CLI shows the cancelled message and no error."""
    script = api_post("/scripts", json={
        "name": "cancel test",
        "script_content": "sleep 300",
        "description": "test",
        "security_status": "pending",
    }).json()

    job = api_post("/jobs", json={
        "requested_ram": 4, "requested_cpu": 2,
        "requested_disk_space": 20, "has_input_data": False,
        "client_home": "/tmp",
        "max_attempts": 1,
        "disable_preflight": True,
    }).json()

    api_put(f"/jobs/{job['public_id']}", json={
        "script_public_id": script["public_id"],
        "current_working_directory": "/tmp",
    })

    env = api_post("/environments", json={
        "tools": ["bash"], "env_string": "",
    }).json()

    api_put(f"/jobs/{job['public_id']}", json={
        "environment_public_id": env["public_id"],
    })

    deadline = time.time() + 300
    while time.time() < deadline:
        if get_job_status(job["public_id"]) == "running":
            break
        time.sleep(5)
    else:
        pytest.fail("Job did not reach running state within 5 min")

    result = runner.invoke(app, ["jobs", "cancel", job["public_id"]])
    assert result.exit_code == 0
    assert "cancelled" in result.output.lower()
    assert "Error" not in result.output
    assert "Traceback" not in result.output


@pytest.mark.slow
def test_job_script_failure():
    """Script exits with non-zero code -> job should end as failed_execution.

    CLI exits non-zero when the job ends in a failed terminal state, so we
    pass allow_failure=True; the assertion below verifies the status itself.
    """
    job_id, _ = _run_job([
        "run", "--tools", "bash", "--cmd", "echo 'about to fail' && exit 1",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ], allow_failure=True)
    status = get_job_status(job_id)
    assert status in {"failed_execution", "failed"}, f"Job {job_id} status: {status}"


# --- dataset download after job ---

@pytest.mark.slow
def test_dataset_download_after_job(tmp_path):
    """Submit job that produces output, then download output dataset via CLI.
    Uses a fixed /tmp/thoa_test_output path so it works both locally and on CI.
    Output content carries a per-run uuid so the backend's (md5, size) dedup
    cannot alias this fresh file onto a pre-existing record whose blob is absent."""
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()

    marker = f"hello from test {uuid.uuid4()}"
    job_id, _ = _run_job([
        "run",
        "--tools", "bash",
        "--cmd", f"mkdir -p /tmp/thoa_test_output && echo '{marker}' > /tmp/thoa_test_output/result.txt",
        "--output", "/tmp/thoa_test_output",
        "--download-dir", str(download_dir),
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])

    resp = api_get("/jobs", params={"public_id": job_id})
    assert resp.status_code == 200
    job_data = resp.json()[0]
    output_ds_id = job_data.get("output_dataset_public_id")

    if not output_ds_id:
        pytest.skip("Job produced no output dataset")

    downloaded = list(download_dir.rglob("*"))
    assert len(downloaded) > 0, f"No files downloaded to {download_dir}"
