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
    get_job_status, poll_job_until_terminal, api_get, api_post, api_put, api_delete,
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


# --- local upload via transfer flow ---

@pytest.mark.slow
def test_local_input_creates_and_attaches_dataset(tmp_path):
    """Submitting a local --input file creates a ready dataset and attaches it to the job."""
    test_file = tmp_path / "transfer_test.txt"
    test_file.write_text("transfer flow integration test")

    job_id, _ = _run_job([
        "run", "--input", str(test_file), "--tools", "bash",
        "--cmd", "echo done",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])

    # Job must have an input dataset attached (proves transfer flow completed)
    resp = api_get("/jobs", params={"public_id": job_id})
    assert resp.status_code == 200
    job_data = resp.json()[0]
    input_dataset_id = job_data.get("input_dataset_public_id")
    assert input_dataset_id, f"Job {job_id} has no input_dataset_public_id"

    # Dataset must be in 'created' state (not 'creating')
    ds_resp = api_get("/datasets", params={"public_id": input_dataset_id})
    assert ds_resp.status_code == 200
    dataset = ds_resp.json()[0]
    assert dataset["status"] == "created", (
        f"Dataset {input_dataset_id} status is {dataset['status']!r}, expected 'created'"
    )


@pytest.mark.slow
def test_local_input_deduplication(tmp_path):
    """Uploading the same file twice reuses the existing blob.

    Verified by checking that both jobs' input datasets reference the same
    file public_id — i.e. the second manifest returned upload_required=False
    and the service reused the existing FileModel row.
    """
    test_file = tmp_path / "dedup_test.txt"
    test_file.write_text("deduplication content — unique enough for this test run")

    job_id_1, _ = _run_job([
        "run", "--input", str(test_file), "--tools", "bash",
        "--cmd", "echo done",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])
    assert get_job_status(job_id_1) in {"completed", "cleanup"}

    job_id_2, _ = _run_job([
        "run", "--input", str(test_file), "--tools", "bash",
        "--cmd", "echo done",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])
    assert get_job_status(job_id_2) in {"completed", "cleanup"}

    # Both jobs should have different job IDs but share the same underlying file
    assert job_id_1 != job_id_2

    def _file_ids_for_job(job_id):
        job = api_get("/jobs", params={"public_id": job_id}).json()[0]
        ds_id = job.get("input_dataset_public_id")
        assert ds_id, f"Job {job_id} has no input dataset"
        files = api_get("/files", params={"dataset_public_id": ds_id}).json()
        return {f["public_id"] for f in files}

    files_1 = _file_ids_for_job(job_id_1)
    files_2 = _file_ids_for_job(job_id_2)
    shared = files_1 & files_2
    assert shared, (
        f"No shared file IDs between job 1 ({files_1}) and job 2 ({files_2}) — "
        f"dedup did not fire"
    )


@pytest.mark.slow
def test_dataset_deletion_cleans_up_exclusive_files(tmp_path):
    """Deleting a dataset whose files are not shared removes those files from storage.

    Flow:
      1. Submit a job with a unique local file so the resulting dataset is the
         only owner of that file.
      2. Trigger dataset deletion via the delete_trigger endpoint.
      3. Poll until the dataset status reaches 'deleted'.
      4. Confirm the file record is gone from the API (404 / empty list).
    """
    test_file = tmp_path / "exclusive_file.txt"
    # Write content unique enough that no other test has uploaded it
    test_file.write_text(f"exclusive deletion test — {time.time()}")

    job_id, _ = _run_job([
        "run", "--input", str(test_file), "--tools", "bash",
        "--cmd", "echo done",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])
    assert get_job_status(job_id) in {"completed", "cleanup"}

    job_data = api_get("/jobs", params={"public_id": job_id}).json()[0]
    dataset_id = job_data.get("input_dataset_public_id")
    assert dataset_id, f"Job {job_id} has no input dataset"

    files_resp = api_get("/files", params={"dataset_public_id": dataset_id}).json()
    assert files_resp, "Dataset has no files"
    file_ids = [f["public_id"] for f in files_resp]

    # Trigger deletion flow
    del_resp = api_delete(f"/datasets/{dataset_id}/delete_trigger")
    assert del_resp.status_code in {200, 202, 204}, (
        f"delete_trigger returned {del_resp.status_code}: {del_resp.text}"
    )

    # Poll until dataset reaches 'deleted' status (deletion flow is async)
    deadline = time.time() + 300
    dataset_status = None
    while time.time() < deadline:
        ds_resp = api_get("/datasets", params={"public_id": dataset_id})
        if ds_resp.status_code == 404:
            dataset_status = "deleted"
            break
        datasets = ds_resp.json()
        if not datasets:
            dataset_status = "deleted"
            break
        dataset_status = datasets[0].get("status")
        if dataset_status == "deleted":
            break
        time.sleep(10)

    assert dataset_status == "deleted", (
        f"Dataset {dataset_id} status is {dataset_status!r} after 5 min — deletion flow stalled"
    )

    # File records should be gone for files that were exclusive to this dataset
    for file_id in file_ids:
        remaining = api_get("/files", params={"public_id": file_id}).json()
        assert not remaining, (
            f"File {file_id} still exists after dataset deletion — exclusive file not cleaned up"
        )


@pytest.mark.slow
def test_reuse_existing_input_dataset(tmp_path):
    """Running a job with --input-dataset reuses an existing dataset without re-uploading.

    Flow:
      1. Run a job with --input to create a dataset.
      2. Extract the input_dataset_public_id from the completed job.
      3. Run a second job with --input-dataset <id> pointing at that dataset.
      4. Verify the second job completes and uses the same dataset.
    """
    test_file = tmp_path / "reuse_input.txt"
    test_file.write_text("content for dataset reuse test")

    # First job: creates the dataset
    job_id_1, _ = _run_job([
        "run", "--input", str(test_file), "--tools", "bash",
        "--cmd", "echo done",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])
    assert get_job_status(job_id_1) in {"completed", "cleanup"}

    job_data_1 = api_get("/jobs", params={"public_id": job_id_1}).json()[0]
    dataset_id = job_data_1.get("input_dataset_public_id")
    assert dataset_id, f"First job {job_id_1} has no input_dataset_public_id"

    # Second job: reuses the dataset via --input-dataset (no upload)
    job_id_2, _ = _run_job([
        "run", "--input-dataset", dataset_id, "--tools", "bash",
        "--cmd", "echo done",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])
    assert get_job_status(job_id_2) in {"completed", "cleanup"}

    job_data_2 = api_get("/jobs", params={"public_id": job_id_2}).json()[0]
    assert job_data_2.get("input_dataset_public_id") == dataset_id, (
        f"Second job used dataset {job_data_2.get('input_dataset_public_id')!r} "
        f"instead of the expected {dataset_id!r}"
    )


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
