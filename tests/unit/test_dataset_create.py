"""Unit tests for 'thoa dataset create'.

The transfer helpers are patched: these tests only check that the command
drives them in order and reports the dataset the import produced.
"""
import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from thoa.cli import app

# thoa.cli re-exports the Typer app as "dataset_app", which shadows the module.
dataset_app = importlib.import_module("thoa.cli.dataset_app")

runner = CliRunner()

_TRANSFER_ID = "transfer-1"
_DATASET_ID = "11111111-1111-1111-1111-111111111111"


def _prepared(has_local_uploads=True):
    return SimpleNamespace(transfer_id=_TRANSFER_ID, has_local_uploads=has_local_uploads)


def _invoke(args, *, prepared=None, transfer_view=None, track_side_effect=None):
    calls = []
    mock_api = MagicMock()
    mock_api.get.return_value = (
        {"public_id": _TRANSFER_ID, "status": "completed", "dataset_public_id": _DATASET_ID}
        if transfer_view is None else transfer_view
    )
    heartbeat = MagicMock()
    heartbeat.return_value.__enter__.side_effect = lambda *a: calls.append("heartbeat")
    with patch.object(dataset_app, "api_client", mock_api), \
         patch.object(dataset_app, "prepare_mixed_transfer",
                      side_effect=lambda specs, cwd: calls.append(("prepare", specs)) or (prepared or _prepared())), \
         patch.object(dataset_app, "ClientHeartbeat", heartbeat), \
         patch.object(dataset_app, "start_transfer", side_effect=lambda p: calls.append("start")), \
         patch.object(dataset_app, "upload_local_items", side_effect=lambda p: calls.append("upload")), \
         patch.object(dataset_app, "track_transfer",
                      side_effect=track_side_effect or (lambda p: calls.append("track"))):
        result = runner.invoke(app, ["dataset", "create", *args])
    return result, calls, mock_api


def test_local_input_uploads_and_prints_dataset_id(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("x")
    result, calls, mock_api = _invoke(["--input", str(f)])

    assert result.exit_code == 0, result.output
    assert calls[0][0] == "prepare"
    assert [s.kind for s in calls[0][1]] == ["local"]
    assert calls[1:] == ["heartbeat", "start", "upload", "track"]
    mock_api.get.assert_called_once_with(f"/data-transfers/{_TRANSFER_ID}")
    assert _DATASET_ID in result.output
    assert f"--input-dataset {_DATASET_ID}" in result.output


def test_server_side_only_inputs_skip_upload_and_heartbeat():
    result, calls, _ = _invoke(
        ["-i", "https://drive.google.com/drive/folders/abc::data"],
        prepared=_prepared(has_local_uploads=False),
    )

    assert result.exit_code == 0, result.output
    assert [s.kind for s in calls[0][1]] == ["google_drive"]
    assert calls[1:] == ["start", "track"]


def test_unknown_input_exits_before_any_transfer():
    result, calls, _ = _invoke(["-i", "/definitely/not/here.txt"])

    assert result.exit_code == 1
    assert "Unsupported --input" in result.output
    assert calls == []


def test_input_is_required():
    result, calls, _ = _invoke([])

    assert result.exit_code != 0
    assert calls == []


def test_failed_import_exits_with_error(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("x")

    def fail(_):
        raise RuntimeError("Dataset import failed: disk full")

    result, _, mock_api = _invoke(["-i", str(f)], track_side_effect=fail)

    assert result.exit_code == 1
    assert "disk full" in result.output
    mock_api.get.assert_not_called()


def test_missing_dataset_id_is_an_error(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("x")
    result, _, _ = _invoke(
        ["-i", str(f)],
        transfer_view={"public_id": _TRANSFER_ID, "status": "completed", "dataset_public_id": None},
    )

    assert result.exit_code == 1
    assert "did not return its dataset" in result.output
