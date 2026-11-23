"""Unit tests for 'thoa dataset create'.

The transfer helpers are patched: these tests only check that the command
drives them in order and always tells the user the dataset's ID.
"""
import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
from typer.testing import CliRunner

from thoa.cli import app

# thoa.cli re-exports the Typer app as "dataset_app", which shadows the module.
dataset_app = importlib.import_module("thoa.cli.dataset_app")

runner = CliRunner()

_TRANSFER_ID = "transfer-1"
_DATASET_ID = "11111111-1111-1111-1111-111111111111"
_GDRIVE = "https://drive.google.com/drive/folders/abc::data"


def _prepared(has_local_uploads=True, items=None):
    return SimpleNamespace(
        transfer_id=_TRANSFER_ID,
        has_local_uploads=has_local_uploads,
        items=[{"provider": "local"}] if items is None else items,
    )


def _invoke(args, *, prepared=None, transfer_view=None, upload_side_effect=None, track_side_effect=None):
    calls = []
    mock_api = MagicMock()
    mock_api.get.return_value = (
        {"public_id": _TRANSFER_ID, "status": "manifest_created", "dataset_public_id": _DATASET_ID}
        if transfer_view is None else transfer_view
    )
    heartbeat = MagicMock()
    heartbeat.return_value.__enter__.side_effect = lambda *a: calls.append("heartbeat")
    with patch.object(dataset_app, "api_client", mock_api), \
         patch.object(dataset_app, "prepare_mixed_transfer",
                      side_effect=lambda specs, cwd: calls.append(("prepare", specs)) or (prepared or _prepared())), \
         patch.object(dataset_app, "ClientHeartbeat", heartbeat), \
         patch.object(dataset_app, "start_transfer", side_effect=lambda p: calls.append("start")), \
         patch.object(dataset_app, "upload_local_items",
                      side_effect=upload_side_effect or (lambda p: calls.append("upload"))), \
         patch.object(dataset_app, "track_transfer",
                      side_effect=track_side_effect or (lambda p: calls.append("track"))):
        result = runner.invoke(app, ["dataset", "create", *args])
    return result, calls, mock_api


def _out(result):
    # Rich wraps long lines at the terminal width; compare on single spaces.
    return " ".join(result.output.split())


def _file(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("x")
    return str(f)


def _raise(exc):
    def side_effect(_):
        raise exc
    return side_effect


def test_local_input_uploads_and_prints_dataset_id(tmp_path):
    result, calls, mock_api = _invoke(["--input", _file(tmp_path)])

    assert result.exit_code == 0, result.output
    assert calls[0][0] == "prepare"
    assert [s.kind for s in calls[0][1]] == ["local"]
    assert calls[1:] == ["heartbeat", "start", "upload", "track"]
    mock_api.get.assert_called_once_with(f"/data-transfers/{_TRANSFER_ID}")
    assert "Dataset created" in _out(result)
    assert f"--input-dataset {_DATASET_ID}" in _out(result)


def test_dataset_id_is_printed_before_the_upload_starts(tmp_path):
    printed_before_upload = []

    def upload(_):
        printed_before_upload.append(_DATASET_ID in result_output())

    with patch.object(dataset_app.console, "print", wraps=dataset_app.console.print) as spy:
        def result_output():
            return " ".join(str(c.args[0]) for c in spy.call_args_list if c.args)
        result, _, _ = _invoke(["-i", _file(tmp_path)], upload_side_effect=upload)

    assert result.exit_code == 0, result.output
    assert printed_before_upload == [True]


def test_server_side_only_inputs_skip_upload_and_heartbeat():
    result, calls, _ = _invoke(["-i", _GDRIVE], prepared=_prepared(has_local_uploads=False))

    assert result.exit_code == 0, result.output
    assert [s.kind for s in calls[0][1]] == ["google_drive"]
    assert calls[1:] == ["start", "track"]


def test_tilde_in_local_input_is_expanded(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "a.txt").write_text("x")

    result, calls, _ = _invoke(["--input=~/data"])

    assert result.exit_code == 0, result.output
    assert calls[0][1][0].source == str(tmp_path / "data")


def test_empty_local_directory_exits_before_any_transfer(tmp_path):
    (tmp_path / "empty").mkdir()
    result, calls, _ = _invoke(["-i", str(tmp_path / "empty")])

    assert result.exit_code == 1
    assert "No files found" in _out(result)
    assert calls == []


def test_empty_manifest_exits_before_starting():
    result, calls, mock_api = _invoke(["-i", _GDRIVE], prepared=_prepared(has_local_uploads=False, items=[]))

    assert result.exit_code == 1
    assert "contain no files" in _out(result)
    assert [c for c in calls if c != "prepare" and not isinstance(c, tuple)] == []
    mock_api.get.assert_not_called()


def test_unknown_input_exits_before_any_transfer():
    result, calls, _ = _invoke(["-i", "/definitely/not/here.txt"])

    assert result.exit_code == 1
    assert "Unsupported --input" in _out(result)
    assert calls == []


def test_input_is_required():
    result, calls, _ = _invoke([])

    assert result.exit_code != 0
    assert calls == []


def test_missing_dataset_id_exits_before_uploading(tmp_path):
    result, calls, _ = _invoke(
        ["-i", _file(tmp_path)],
        transfer_view={"public_id": _TRANSFER_ID, "status": "manifest_created", "dataset_public_id": None},
    )

    assert result.exit_code == 1
    assert "did not return the dataset" in _out(result)
    assert "start" not in calls and "upload" not in calls


def test_failed_import_exits_with_error(tmp_path):
    result, _, _ = _invoke(
        ["-i", _file(tmp_path)], track_side_effect=_raise(RuntimeError("Dataset import failed: disk full")),
    )

    assert result.exit_code == 1
    assert "disk full" in _out(result)


def test_ctrl_c_while_importing_detaches_and_keeps_the_id(tmp_path):
    result, _, _ = _invoke(["-i", _file(tmp_path)], track_side_effect=_raise(KeyboardInterrupt()))

    assert result.exit_code == 0
    assert "Detached" in _out(result)
    assert f"--input-dataset {_DATASET_ID}" in _out(result)


def test_network_error_while_importing_keeps_the_id(tmp_path):
    result, _, _ = _invoke(["-i", _file(tmp_path)], track_side_effect=_raise(httpx.ReadTimeout("timed out")))

    assert result.exit_code == 1
    assert "Lost connection (ReadTimeout: timed out)" in _out(result)
    assert f"--input-dataset {_DATASET_ID}" in _out(result)
    assert "Traceback" not in _out(result)


def test_ctrl_c_during_local_upload_says_to_rerun(tmp_path):
    result, _, _ = _invoke(["-i", _file(tmp_path)], upload_side_effect=_raise(KeyboardInterrupt()))

    assert result.exit_code == 130
    assert "before all local files were uploaded" in _out(result)
    assert "Run the same command again" in _out(result)
    assert _DATASET_ID in _out(result)


def test_network_error_during_local_upload_says_to_rerun(tmp_path):
    result, _, _ = _invoke(["-i", _file(tmp_path)], upload_side_effect=_raise(httpx.ConnectError("refused")))

    assert result.exit_code == 1
    assert "Lost connection (ConnectError: refused)" in _out(result)
    assert "Run the same command again" in _out(result)
