"""
Client-side validation tests for `thoa run`.
These do NOT require a live backend - they test CLI argument validation.
"""

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
