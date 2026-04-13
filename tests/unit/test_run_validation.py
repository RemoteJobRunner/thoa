"""
Client-side validation tests for `thoa run`.
These do NOT require a live backend - they test CLI argument validation.
"""

from unittest.mock import patch, MagicMock
from typer.testing import CliRunner
from thoa.cli import app

runner = CliRunner()


def test_run_rejects_more_than_1000_files(tmp_path):
    for i in range(1001):
        (tmp_path / f"file_{i}.txt").touch()

    result = runner.invoke(app, [
        "run",
        "--input", str(tmp_path),
        "--tools", "bash",
        "--cmd", "echo hello",
    ])

    assert result.exit_code == 1
    assert "1000" in result.output


def test_run_requires_tools_or_env_source():
    result = runner.invoke(app, [
        "run",
        "--cmd", "echo hello",
    ])

    assert result.exit_code == 1
    assert "tools" in result.output.lower() or "env-source" in result.output.lower()


def test_run_input_dataset_not_implemented():
    result = runner.invoke(app, [
        "run",
        "--input-dataset", "abc123",
        "--tools", "bash",
        "--cmd", "echo hello",
    ])

    assert result.exit_code != 0


def test_run_async_exits_after_upload(tmp_path):
    input_file = tmp_path / "data.txt"
    input_file.write_text("hello")

    fake_job = {"public_id": "job-async-001"}
    fake_script = {"public_id": "script-001"}
    fake_env = {"public_id": "env-001"}
    fake_dataset = {"public_id": "dataset-001"}
    fake_file = {"public_id": "file-001", "filename": str(input_file)}
    fake_link = {
        "public_id": "link-001",
        "file_public_id": "file-001",
        "url": "https://fake.blob.core.windows.net/container/file-001?sas=xxx",
        "client_path": str(input_file),
    }

    def api_post_side_effect(path, **kwargs):
        if path == "/scripts":
            return fake_script
        if path == "/jobs":
            return fake_job
        if path == "/environments":
            return fake_env
        if path == "/files":
            return fake_file
        if path == "/datasets":
            return fake_dataset
        return {}

    def api_put_side_effect(path, **kwargs):
        return fake_job

    def api_get_side_effect(path, **kwargs):
        if path == "/users/validate_job_request":
            return {"valid": True}
        if "/temporary_links" in path or (isinstance(path, str) and "temporary_links" in path):
            return [fake_link]
        return {}

    mock_api = MagicMock()
    mock_api.post.side_effect = api_post_side_effect
    mock_api.put.side_effect = api_put_side_effect
    mock_api.get.side_effect = api_get_side_effect

    mock_time = MagicMock()

    with patch("thoa.cli.commands.run.api_client", mock_api), \
         patch("thoa.core.job_utils.api_client", mock_api), \
         patch("thoa.cli.commands.run.upload_all") as mock_upload, \
         patch("thoa.cli.commands.run.all_files_have_upload_links", return_value=True), \
         patch("thoa.cli.commands.run.time", mock_time), \
         patch("thoa.core.resolve_environment_spec", return_value=""):

        result = runner.invoke(app, [
            "run",
            "--input", str(input_file),
            "--tools", "bash",
            "--cmd", "echo hello",
            "--run-async",
        ])

    assert mock_api.stream_logs_blocking.call_count == 0, \
        "stream_logs_blocking should not be called with --run-async"
    assert "job-async-001" in result.output, \
        f"Job ID should appear in output, got: {result.output}"
    assert result.exit_code == 0, \
        f"Expected exit_code 0, got {result.exit_code}. Output: {result.output}"


def test_run_async_no_inputs_exits_before_provisioning():
    fake_job = {"public_id": "job-async-002"}
    fake_script = {"public_id": "script-002"}
    fake_env = {"public_id": "env-002"}

    def api_post_side_effect(path, **kwargs):
        if path == "/scripts":
            return fake_script
        if path == "/jobs":
            return fake_job
        if path == "/environments":
            return fake_env
        return {}

    def api_put_side_effect(path, **kwargs):
        return fake_job

    def api_get_side_effect(path, **kwargs):
        if path == "/users/validate_job_request":
            return {"valid": True}
        return {}

    mock_api = MagicMock()
    mock_api.post.side_effect = api_post_side_effect
    mock_api.put.side_effect = api_put_side_effect
    mock_api.get.side_effect = api_get_side_effect

    mock_time = MagicMock()

    with patch("thoa.cli.commands.run.api_client", mock_api), \
         patch("thoa.core.job_utils.api_client", mock_api), \
         patch("thoa.cli.commands.run.time", mock_time), \
         patch("thoa.core.resolve_environment_spec", return_value=""):

        result = runner.invoke(app, [
            "run",
            "--tools", "bash",
            "--cmd", "echo hello",
            "--run-async",
        ])

    assert mock_api.stream_logs_blocking.call_count == 0, \
        "stream_logs_blocking should not be called with --run-async"
    assert "job-async-002" in result.output, \
        f"Job ID should appear in output, got: {result.output}"
    assert result.exit_code == 0, \
        f"Expected exit_code 0, got {result.exit_code}. Output: {result.output}"
