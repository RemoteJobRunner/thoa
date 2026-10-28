"""`thoa run` must end on a crash and say it was on our side."""
from itertools import count
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from thoa.cli import app
from thoa.core.api_utils import StreamOutcome

runner = CliRunner()


def test_run_tells_the_user_a_crash_was_on_our_side(tmp_path):
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
            return [{"status": "crashed", "public_id": "job-001"}]
        if path.endswith("/attempts"):
            return [fake_attempt]
        if path == "/attempts/attempt-001":
            return {**fake_attempt, "status": "crashed"}
        return {}

    mock_api = MagicMock()
    mock_api.post.side_effect = api_post_side_effect
    mock_api.put.side_effect = lambda path, **kw: fake_job
    mock_api.get.side_effect = api_get_side_effect
    mock_api.stream_logs_blocking.return_value = StreamOutcome.UNAVAILABLE

    mock_time = MagicMock()
    mock_time.time.side_effect = count(0.0, 5.0)
    sleeps = count()

    def bounded_sleep(_):
        # A status the CLI does not treat as terminal would otherwise spin forever.
        if next(sleeps) > 200:
            raise AssertionError("thoa run kept waiting on a terminal status")

    mock_time.sleep.side_effect = bounded_sleep

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
    assert "No further attempts. Job failed." not in output
    # No crash path marks the log stream done, so streaming a crashed job hangs forever.
    assert mock_api.stream_logs_blocking.call_count == 0
    assert result.exit_code == 1
