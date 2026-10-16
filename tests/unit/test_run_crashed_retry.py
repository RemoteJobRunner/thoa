"""A retry attempt that crashes on our side must end `thoa run`, not leave it streaming."""
from itertools import count
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from thoa.cli import app
from thoa.core.api_utils import StreamOutcome

runner = CliRunner()


def test_run_ends_when_the_retry_attempt_crashes(tmp_path):
    input_file = tmp_path / "data.txt"
    input_file.write_text("hello")

    fake_job = {"public_id": "job-001"}
    attempt_1 = {"public_id": "attempt-001", "attempt_number": 1, "status": "running"}
    attempt_2 = {"public_id": "attempt-002", "attempt_number": 2, "status": "crashed", "ai_note": "fixed the script"}
    fake_link = {
        "public_id": "link-001",
        "file_public_id": "file-001",
        "url": "https://fake.blob.core.windows.net/container/file-001?sas=xxx",
        "client_path": str(input_file),
    }
    # Attempt 1 fails on the user's command, the AI retries, attempt 2 crashes on our side.
    state = {"phase": "running", "retrying_reads": 0}

    def job_status():
        if state["phase"] == "retrying":
            state["retrying_reads"] += 1
            if state["retrying_reads"] > 2:
                state["phase"] = "crashed"
                return "crashed"
            return "retrying"
        return state["phase"]

    def api_post_side_effect(path, **kwargs):
        return {
            "/scripts": {"public_id": "script-001"},
            "/jobs": fake_job,
            "/environments": {"public_id": "env-001"},
            "/files": {"public_id": "file-001", "filename": str(input_file)},
            "/datasets": {"public_id": "dataset-001"},
        }.get(path, {})

    def api_get_side_effect(path, **kwargs):
        if path == "/users/validate_job_request":
            return {"valid": True}
        if "temporary_links" in path:
            return [fake_link]
        if path.startswith("/jobs?public_id="):
            return [{"status": job_status(), "public_id": "job-001"}]
        if path.endswith("/attempts"):
            return [attempt_1] if state["phase"] == "running" else [attempt_1, attempt_2]
        if path == "/attempts/attempt-002":
            return attempt_2
        return {}

    def stream(*args, **kwargs):
        if state["phase"] != "running":
            raise AssertionError("streamed an attempt that crashed: nothing ever ends that stream")
        state["phase"] = "retrying"
        return StreamOutcome.FAILED

    mock_api = MagicMock()
    mock_api.post.side_effect = api_post_side_effect
    mock_api.put.side_effect = lambda path, **kw: fake_job
    mock_api.get.side_effect = api_get_side_effect
    mock_api.stream_logs_blocking.side_effect = stream

    mock_time = MagicMock()
    mock_time.time.side_effect = count(0.0, 5.0)

    fake_dataset = {
        "dataset_public_id": "dataset-001",
        "input_context": {str(input_file): "file-001"},
    }

    with patch("thoa.cli.commands.run.api_client", mock_api), \
         patch("thoa.core.job_utils.api_client", mock_api), \
         patch("thoa.cli.commands.run.create_mixed_dataset", return_value=fake_dataset), \
         patch("thoa.cli.commands.run.upload_all"), \
         patch("thoa.cli.commands.run.all_files_have_upload_links", return_value=True), \
         patch("thoa.cli.commands.run.time", mock_time), \
         patch("thoa.core.resolve_environment_spec", return_value=""):

        result = runner.invoke(app, [
            "run",
            "--input", str(input_file),
            "--tools", "bash",
            "--cmd", "echo hello",
        ])

    output = " ".join(result.output.split())
    assert "This job crashed due to a failure on our end" in output, result.output
    assert mock_api.stream_logs_blocking.call_count == 1
    assert result.exit_code == 1
