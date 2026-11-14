import re
import time
import httpx
import os

from typer.testing import CliRunner
from thoa.cli import app
from thoa.core.version_check import current_version

_runner = CliRunner()


def api_url() -> str:
    return os.environ.get("THOA_STAGING_API_URL", os.environ.get("THOA_API_URL", ""))


def api_key() -> str:
    return os.environ.get("THOA_STAGING_API_KEY", os.environ.get("THOA_API_KEY", ""))


def api_headers() -> dict:
    return {"X-API-Key": api_key(), "Accept": "application/json", "X-Client-Version": current_version()}


def api_get(path: str, **kwargs) -> httpx.Response:
    return httpx.get(f"{api_url()}/api{path}", headers=api_headers(), timeout=30, **kwargs)


def api_post(path: str, **kwargs) -> httpx.Response:
    return httpx.post(f"{api_url()}/api{path}", headers=api_headers(), timeout=30, **kwargs)


def api_put(path: str, **kwargs) -> httpx.Response:
    return httpx.put(f"{api_url()}/api{path}", headers=api_headers(), timeout=30, **kwargs)


def api_delete(path: str, **kwargs) -> httpx.Response:
    return httpx.delete(f"{api_url()}/api{path}", headers=api_headers(), timeout=30, **kwargs)


def extract_job_id(output: str) -> str | None:
    m = re.search(r"jobs/([0-9a-f-]{36})", output)
    return m.group(1) if m else None


def run_job(cli_args: list[str], allow_failure: bool = False) -> tuple[str, str]:
    """Run a job via CLI, return (job_id, output).

    Forces --max-attempts=1 and --disable-preflight on every integration job so
    AI retry/preflight paths never engage — they make assertions non-deterministic
    and previously hung nightly runs in 'retrying' indefinitely.

    Asserts exit_code == 0 by default. Pass allow_failure=True for tests that
    intentionally submit a failing job (CLI exits non-zero when the job ends
    in a failed terminal state, which is the correct behavior).
    """
    cli_args = [*cli_args, "--max-attempts", "1", "--disable-preflight"]
    result = _runner.invoke(app, cli_args)
    if not allow_failure:
        assert result.exit_code == 0, f"CLI failed: {result.output}"
    job_id = extract_job_id(result.output)
    assert job_id, f"No job ID in output: {result.output}"
    return job_id, result.output


def get_job_status(job_public_id: str) -> str:
    resp = api_get("/jobs", params={"public_id": job_public_id})
    if resp.status_code != 200 or not resp.json():
        return "unknown"
    return resp.json()[0].get("status", "unknown")


def poll_job_until_terminal(job_public_id: str, timeout: int = 600, interval: int = 10) -> str:
    """Poll job status until it reaches a terminal state or times out.
    Returns the final status string.
    Terminal states: completed, cleanup, failed, failed_validation,
    failed_execution, cancelled, error
    """
    terminal = {
        "completed", "cleanup", "failed",
        "failed_validation", "failed_execution", "cancelled", "error",
    }
    deadline = time.time() + timeout
    status = "unknown"
    while time.time() < deadline:
        status = get_job_status(job_public_id)
        if status in terminal:
            return status
        time.sleep(interval)
    return status
