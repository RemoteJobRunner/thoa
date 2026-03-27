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
