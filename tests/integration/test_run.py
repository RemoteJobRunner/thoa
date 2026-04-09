"""
Real job submission tests against staging.
Each test submits a job via CLI, waits for completion via WebSocket stream.
Requires THOA_API_URL and THOA_API_KEY env vars.

Run with: pytest tests/integration/test_run.py -v -m slow
"""

import re
import pytest
from typer.testing import CliRunner
from thoa.cli import app
from tests.integration.helpers import get_job_status, poll_job_until_terminal

runner = CliRunner()


@pytest.fixture(autouse=True)
def _require_backend(backend_url_and_key):
    pass


def _extract_job_id(output: str) -> str | None:
    m = re.search(r"jobs/([0-9a-f-]{36})", output)
    return m.group(1) if m else None


@pytest.mark.slow
def test_job_2cpu_4ram_no_input():
    result = runner.invoke(app, [
        "run",
        "--tools", "samtools",
        "--cmd", "samtools --version",
        "--n-cores", "2",
        "--ram", "4",
        "--storage", "50",
    ])
    assert result.exit_code == 0, f"CLI failed: {result.output}"
    job_id = _extract_job_id(result.output)
    assert job_id, f"No job ID in output: {result.output}"
    status = get_job_status(job_id)
    assert status in {"completed", "cleanup"}, f"Job {job_id} status: {status}"


@pytest.mark.slow
def test_job_with_input_file(tmp_path):
    test_file = tmp_path / "input.txt"
    test_file.write_text("Hello from integration test")

    result = runner.invoke(app, [
        "run",
        "--input", str(test_file),
        "--tools", "bash",
        "--cmd", "find / -name input.txt -type f 2>/dev/null",
        "--n-cores", "2",
        "--ram", "4",
        "--storage", "50",
    ])
    assert result.exit_code == 0, f"CLI failed: {result.output}"
    job_id = _extract_job_id(result.output)
    assert job_id, f"No job ID in output: {result.output}"
    status = get_job_status(job_id)
    assert status in {"completed", "cleanup"}, f"Job {job_id} status: {status}"


@pytest.mark.slow
def test_job_with_invalid_tool():
    """Submit a job with a nonexistent tool via API (not CliRunner, because
    stream_logs_blocking hangs when the job never reaches 'running').
    Poll until terminal state and verify it fails validation."""
    from tests.integration.helpers import api_post, api_put

    script = api_post("/scripts", json={
        "name": "invalid tool test",
        "script_content": "echo hello",
        "description": "test",
        "security_status": "pending",
    }).json()

    job = api_post("/jobs", json={
        "requested_ram": 4,
        "requested_cpu": 2,
        "requested_disk_space": 50,
        "has_input_data": False,
        "client_home": "/tmp",
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
    assert final_status in {"failed", "failed_validation"}, (
        f"Job {job['public_id']} ended '{final_status}', expected failed/failed_validation"
    )
