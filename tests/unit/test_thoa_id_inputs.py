"""Dataset and file ids as --input values: detection, server-side resolution,
manifest items, and the deprecated --input-dataset alias."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

import thoa.core.local_transfer as lt
from thoa.cli import app
from thoa.core.input_specs import InputSpecError, parse_input_spec, resolve_thoa_ids
from thoa.core.local_transfer import PreparedTransfer

DATASET_ID = "0f8fad5b-d9cb-469f-a165-70867728950e"
FILE_ID = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
runner = CliRunner()


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------

def test_uuid_is_a_thoa_id(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    spec = parse_input_spec(DATASET_ID.upper() + "::inputs/")
    assert (spec.kind, spec.source, spec.mount_path) == ("thoa", DATASET_ID, "inputs/")


def test_uuid_that_is_also_a_local_path_is_an_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / DATASET_ID).mkdir()
    with pytest.raises(InputSpecError) as exc:
        parse_input_spec(DATASET_ID)
    assert f"./{DATASET_ID}" in str(exc.value)
    assert parse_input_spec(f"./{DATASET_ID}").kind == "local"


@pytest.mark.parametrize("value", ["0f8fad5b-d9cb-469f-a165", "0f8fad5bd9cb469fa16570867728950e"])
def test_non_uuids_are_not_thoa_ids(tmp_path, monkeypatch, value):
    monkeypatch.chdir(tmp_path)
    assert parse_input_spec(value).kind == "unknown"


# ---------------------------------------------------------------------------
# resolution
# ---------------------------------------------------------------------------

def _resolver(types):
    api = MagicMock()
    api.get.side_effect = lambda path, **kw: {"public_id": path.rsplit("/", 1)[1], "type": types[path.rsplit("/", 1)[1]]}
    return api


def test_resolve_sets_dataset_and_file_kinds(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    specs = [parse_input_spec(DATASET_ID), parse_input_spec(FILE_ID), parse_input_spec("SRR390728")]
    api = _resolver({DATASET_ID: "dataset", FILE_ID: "file"})
    with patch("thoa.core.api_utils.api_client", api):
        resolve_thoa_ids(specs)
    assert [s.kind for s in specs] == ["dataset", "file", "sra"]
    assert [c.args[0] for c in api.get.call_args_list] == [f"/resolve/{DATASET_ID}", f"/resolve/{FILE_ID}"]


def test_resolve_unknown_id_is_an_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with patch("thoa.core.api_utils.api_client", _resolver({DATASET_ID: None})):
        with pytest.raises(InputSpecError) as exc:
            resolve_thoa_ids([parse_input_spec(DATASET_ID)])
    assert "No dataset or file" in str(exc.value)


# ---------------------------------------------------------------------------
# manifest items
# ---------------------------------------------------------------------------

def _manifest_items(specs, cwd="/home/me/proj"):
    api = MagicMock()
    api.post.side_effect = lambda path, **kw: (
        {"public_id": "t-1"} if path == "/data-transfers" else {"items": []}
    )
    with patch.object(lt, "api_client", api):
        lt.prepare_mixed_transfer(specs, cwd=cwd)
    manifest_call = next(c for c in api.post.call_args_list if c.args[0].endswith("/manifest/unified"))
    return manifest_call.kwargs["json"]["items"]


def test_thoa_items_sent_with_mounts():
    specs = [
        SimpleNamespace(source=DATASET_ID, kind="dataset", mount_path=None),
        SimpleNamespace(source=DATASET_ID, kind="dataset", mount_path="in/"),
        SimpleNamespace(source=FILE_ID, kind="file", mount_path="refs/"),
        SimpleNamespace(source=FILE_ID, kind="file", mount_path="genome.fa"),
    ]
    assert _manifest_items(specs) == [
        {"provider": "thoa", "source_ref": {"dataset_id": DATASET_ID}, "base_dir": "/home/me/proj"},
        {"provider": "thoa", "source_ref": {"dataset_id": DATASET_ID}, "mount_path": "/home/me/proj/in"},
        # A trailing "/" on a file's mount means "into this directory".
        {"provider": "thoa", "source_ref": {"file_id": FILE_ID}, "mount_path": "/home/me/proj/refs/"},
        {"provider": "thoa", "source_ref": {"file_id": FILE_ID}, "mount_path": "/home/me/proj/genome.fa"},
    ]


# ---------------------------------------------------------------------------
# thoa run
# ---------------------------------------------------------------------------

def _run(args):
    calls, prepared_specs = [], []

    def post(path, **kwargs):
        calls.append(("POST", path, kwargs.get("json")))
        return {"public_id": {"/scripts": "script-1", "/jobs": "job-1", "/environments": "env-1"}.get(path, "x")}

    def put(path, **kwargs):
        calls.append(("PUT", path, kwargs.get("json")))
        return {"public_id": "job-1"}

    def get(path, **kwargs):
        if path == "/users/validate_job_request":
            return {"valid": True}
        if path.startswith("/resolve/"):
            return {"type": "dataset" if path.endswith(DATASET_ID) else "file"}
        return {}

    api = MagicMock()
    api.post.side_effect, api.put.side_effect, api.get.side_effect = post, put, get

    def prepare(specs, cwd):
        prepared_specs.extend((s.kind, s.source, s.mount_path) for s in specs)
        return PreparedTransfer(transfer_id="t-1", manifest={"items": [{"provider": "thoa", "size": 1}]})

    mock_time = MagicMock()
    mock_time.time.return_value = 0.0
    with patch("thoa.cli.commands.run.api_client", api), \
         patch("thoa.core.api_utils.api_client", api), \
         patch("thoa.core.job_utils.api_client", api), \
         patch("thoa.cli.commands.run.prepare_mixed_transfer", side_effect=prepare), \
         patch("thoa.cli.commands.run.start_transfer"), \
         patch("thoa.cli.commands.run.track_transfer"), \
         patch("thoa.cli.commands.run.current_job_status", return_value="completed"), \
         patch("thoa.cli.commands.run.time", mock_time), \
         patch("thoa.core.resolve_environment_spec", return_value=""):
        result = runner.invoke(app, ["run", *args, "--tools", "bash", "--cmd", "echo hi", "--run-async"])
    return result, calls, prepared_specs


def test_run_with_dataset_and_file_ids_goes_through_the_import():
    result, calls, specs = _run(["-i", DATASET_ID, "-i", f"{FILE_ID}::refs/"])

    assert result.exit_code == 0, result.output
    assert specs == [("dataset", DATASET_ID, None), ("file", FILE_ID, "refs/")]
    job = next(body for m, p, body in calls if m == "POST" and p == "/jobs")
    assert job["input_transfer_public_id"] == "t-1"
    assert job["use_existing_input_dataset"] is False
    assert not any(m == "PUT" and "input_dataset_public_id" in (body or {}) for m, _, body in calls)


def test_input_dataset_is_a_deprecated_alias_and_mixes_with_input(tmp_path):
    local = tmp_path / "a.txt"
    local.write_text("x")
    result, _, specs = _run(["--input-dataset", DATASET_ID, "-i", str(local)])

    assert result.exit_code == 0, result.output
    assert "deprecated" in result.output
    assert sorted(kind for kind, _, _ in specs) == ["dataset", "local"]


def test_unknown_id_fails_before_anything_is_created():
    with patch("thoa.core.input_specs.THOA_ID_KINDS", frozenset()):
        result, calls, specs = _run(["-i", DATASET_ID])
    assert result.exit_code == 1
    assert "No dataset or file" in result.output
    assert not specs and not any(p == "/jobs" for _, p, _ in calls)
