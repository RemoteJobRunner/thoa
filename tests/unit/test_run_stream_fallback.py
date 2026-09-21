"""`thoa run` must not guess an attempt's fate from a broken log stream."""
from itertools import count
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from thoa.cli import app
from thoa.core.api_utils import StreamOutcome

runner = CliRunner()


def test_unavailable_log_stream_falls_back_to_attempt_status(tmp_path):
    input_file = tmp_path / "data.txt"
    input_file.write_text("hello")

    fake_job = {"public_id": "job-001"}
    fake_attempt = {"public_id": "attempt-001", "attempt_number": 1, "status": "running"}
    fake_link = {
        "public_id": "link-001",
        "file_public_id": "file-001",
        "url": "https://fake.blob.core.windows.net/container/file-001?sas=xxx",
        "client_path": str(input_file),
    }

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
            # The attempt failed; only polling can reveal it.
            return [{"status": "failed_execution", "public_id": "job-001"}]
        if path.endswith("/attempts"):
            return [fake_attempt]
        if path == "/attempts/attempt-001":
            return {**fake_attempt, "status": "failed_execution"}
        return {}

    mock_api = MagicMock()
    mock_api.post.side_effect = api_post_side_effect
    mock_api.put.side_effect = lambda path, **kw: fake_job
    mock_api.get.side_effect = api_get_side_effect
    mock_api.stream_logs_blocking.return_value = StreamOutcome.UNAVAILABLE

    mock_time = MagicMock()
    # Advancing clock: a frozen one never reaches the retry-wait deadline.
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

    polled = [c for c in mock_api.get.call_args_list if "/attempts/attempt-001" in str(c)]
    assert polled, f"an unavailable stream must be followed by polling the attempt. Output: {result.output}"
    assert result.exit_code == 1, \
        f"a failed attempt must not be reported as success. Output: {result.output}"
