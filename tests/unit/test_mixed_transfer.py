"""
Unit tests for thoa.core.local_transfer: prepare_mixed_transfer, then start_transfer,
upload_local_items and track_transfer, the steps `thoa run` takes around job creation.

Patches api_client, collect_files, file_sizes_in_bytes, hash_all, upload_file_sas,
and the remote_inputs helpers so no real filesystem, network, or OAuth is needed.
"""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

import thoa.core.local_transfer as lt

# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------

_TRANSFER_ID = "transfer-mix"
_FILE_A = Path("/tmp/a.txt")


# ---------------------------------------------------------------------------
# spec helpers
# ---------------------------------------------------------------------------

def _local_spec(path: str = str(_FILE_A), mount_path=None):
    return SimpleNamespace(source=path, kind="local", mount_path=mount_path)


def _gdrive_spec(url: str = "https://drive.google.com/drive/folders/folder-xyz", mount_path=None):
    return SimpleNamespace(source=url, kind="google_drive", mount_path=mount_path)


# ---------------------------------------------------------------------------
# mock helpers
# ---------------------------------------------------------------------------

def _manifest_resp(items):
    return {"items": items}


def _local_manifest_item(path=str(_FILE_A), upload_required=True):
    return {
        "provider": "local",
        "path": path,
        "upload_required": upload_required,
        "upload_url": "https://sas/a" if upload_required else None,
        "item_public_id": "item-local-a",
        "mount_path": path,
    }


def _remote_manifest_item(upload_required=True):
    return {
        "provider": "google_drive",
        "path": "report.csv",
        "upload_required": upload_required,
        "file_public_id": "file-id-remote",
        "item_public_id": "item-remote-b",
        "mount_path": "/home/user/my-folder/report.csv",
    }


def _patch_all(
    *,
    specs=None,
    files=None,
    sizes=None,
    hashes=None,
    manifest_items=None,
    folder_id="folder-xyz",
    file_id=None,
    track_result=None,
):
    if specs is None:
        specs = [_local_spec(), _gdrive_spec()]
    if files is None:
        files = [_FILE_A]
    if sizes is None:
        sizes = {_FILE_A: 100}
    if hashes is None:
        hashes = {_FILE_A: "md5-a"}
    if manifest_items is None:
        manifest_items = [_local_manifest_item(), _remote_manifest_item()]
    if track_result is None:
        track_result = {"status": "completed"}

    mock_api = MagicMock()
    mock_api.post.side_effect = [
        {"public_id": _TRANSFER_ID},          # POST /data-transfers
        _manifest_resp(manifest_items),        # POST .../manifest/unified
        {},                                    # POST .../start
        {},                                    # POST .../complete (local upload)
    ]

    patches = [
        patch.object(lt, "api_client", mock_api),
        patch.object(lt, "collect_files", return_value=files),
        patch.object(lt, "file_sizes_in_bytes", return_value=sizes),
        patch.object(lt, "hash_all", return_value=hashes),
        patch.object(lt, "upload_file_sas", MagicMock()),
        patch("thoa.core.remote_inputs.authorize_google_drive_transfer", MagicMock()),
        patch("thoa.core.remote_inputs.extract_google_drive_folder_id", return_value=folder_id),
        patch("thoa.core.remote_inputs.extract_google_drive_file_id", return_value=file_id),
        patch("thoa.core.remote_inputs.track_transfer_progress", return_value=track_result),
    ]
    return patches, mock_api


def _import(specs, cwd="/home/user"):
    """The steps `thoa run` takes, minus the job it creates in between."""
    prepared = lt.prepare_mixed_transfer(specs, cwd=cwd)
    lt.start_transfer(prepared)
    lt.upload_local_items(prepared)
    lt.track_transfer(prepared)
    return prepared


def _run(specs, cwd="/home/user", patches=None, mock_api=None):
    if patches is None:
        patches, mock_api = _patch_all(specs=specs)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        return _import(specs, cwd=cwd), mock_api


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_happy_path_starts_the_import_uploads_and_never_fetches_a_dataset():
    """The backend attached the dataset to the job at creation, so the CLI never asks for it."""
    specs = [_local_spec(), _gdrive_spec()]
    prepared, mock_api = _run(specs)
    posted = [c.args[0] for c in mock_api.post.call_args_list]
    assert posted[2] == f"/data-transfers/{_TRANSFER_ID}/start"
    assert posted[3] == f"/data-transfers/{_TRANSFER_ID}/import/items/item-local-a/complete"
    assert mock_api.get.call_args_list == []
    assert prepared.has_local_uploads is True


def test_creates_transfer_then_posts_unified_manifest():
    specs = [_local_spec(), _gdrive_spec()]
    _, mock_api = _run(specs)
    post_calls = mock_api.post.call_args_list

    # First POST creates the transfer
    assert post_calls[0] == call("/data-transfers", json={
        "direction": "import",
        "remote_ref": {},
    })
    # Second POST hits the unified manifest endpoint
    assert f"/data-transfers/{_TRANSFER_ID}/manifest/unified" in str(post_calls[1])


def test_gdrive_authorizes_before_manifest():
    specs = [_gdrive_spec()]
    patches, mock_api = _patch_all(
        specs=specs,
        files=[],
        sizes={},
        hashes={},
        manifest_items=[_remote_manifest_item(upload_required=False)],
    )
    auth_mock = MagicMock()
    patches[5] = patch("thoa.core.remote_inputs.authorize_google_drive_transfer", auth_mock)

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        _import(specs, cwd="/home/user")

    auth_mock.assert_called_once_with(_TRANSFER_ID)
    # Auth must happen before the manifest POST
    manifest_post_idx = next(
        i for i, c in enumerate(mock_api.post.call_args_list)
        if "manifest/unified" in str(c)
    )
    assert manifest_post_idx > 0  # transfer create was first


def test_local_only_skips_gdrive_auth():
    specs = [_local_spec()]
    patches, _ = _patch_all(
        specs=specs,
        manifest_items=[_local_manifest_item()],
    )
    auth_mock = MagicMock()
    patches[5] = patch("thoa.core.remote_inputs.authorize_google_drive_transfer", auth_mock)

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        _import(specs, cwd="/home/user")

    auth_mock.assert_not_called()


def test_local_file_uploaded_gdrive_file_not():
    """upload_file_sas is called for local items but not for GDrive items."""
    specs = [_local_spec(), _gdrive_spec()]
    patches, mock_api = _patch_all(specs=specs)
    upload_mock = MagicMock()
    patches[4] = patch.object(lt, "upload_file_sas", upload_mock)

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        _import(specs, cwd="/home/user")

    # Called exactly once (for the local file only)
    upload_mock.assert_called_once()
    args = upload_mock.call_args[0]
    assert args[0] == _FILE_A  # local path
    assert args[1] == "https://sas/a"


def test_local_upload_skipped_when_not_required():
    """Local file with upload_required=False: upload_file_sas not called."""
    specs = [_local_spec()]
    patches, _ = _patch_all(
        specs=specs,
        manifest_items=[_local_manifest_item(upload_required=False)],
    )
    upload_mock = MagicMock()
    patches[4] = patch.object(lt, "upload_file_sas", upload_mock)

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        _import(specs, cwd="/home/user")

    upload_mock.assert_not_called()


def test_remote_skipped_message_printed(capsys):
    """When GDrive files have upload_required=False, the 'already in storage' message appears."""
    specs = [_gdrive_spec()]
    patches, _ = _patch_all(
        specs=specs,
        files=[],
        sizes={},
        hashes={},
        manifest_items=[_remote_manifest_item(upload_required=False)],
    )

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        _import(specs, cwd="/home/user")

    # Rich console output isn't captured by capsys — check api calls instead:
    # the start POST must still happen (flow runs even for skipped items)
    _, mock_api = _patch_all(
        specs=specs,
        files=[],
        sizes={},
        hashes={},
        manifest_items=[_remote_manifest_item(upload_required=False)],
    )
    with patch.object(lt, "api_client", mock_api), \
         patch.object(lt, "collect_files", return_value=[]), \
         patch.object(lt, "file_sizes_in_bytes", return_value={}), \
         patch.object(lt, "hash_all", return_value={}), \
         patch.object(lt, "upload_file_sas", MagicMock()), \
         patch("thoa.core.remote_inputs.authorize_google_drive_transfer", MagicMock()), \
         patch("thoa.core.remote_inputs.extract_google_drive_folder_id", return_value="folder-xyz"), \
         patch("thoa.core.remote_inputs.extract_google_drive_file_id", return_value=None), \
         patch("thoa.core.remote_inputs.track_transfer_progress", return_value={"status": "completed"}):
        prepared = _import(specs, cwd="/home/user")

    # No upload_file_sas call (GDrive skipped), flow still started
    assert prepared.has_local_uploads is False
    assert any(c.args[0].endswith("/start") for c in mock_api.post.call_args_list)


def test_local_skipped_message_not_uploaded(capsys):
    """Local files with upload_required=False are not uploaded; start still called."""
    specs = [_local_spec()]
    patches, mock_api = _patch_all(
        specs=specs,
        manifest_items=[_local_manifest_item(upload_required=False)],
    )
    upload_mock = MagicMock()
    patches[4] = patch.object(lt, "upload_file_sas", upload_mock)

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        _import(specs, cwd="/home/user")

    upload_mock.assert_not_called()
    # start POST still happened
    start_call = next(c for c in mock_api.post.call_args_list if "start" in str(c))
    assert start_call is not None


def test_failed_transfer_raises_runtime_error():
    specs = [_local_spec(), _gdrive_spec()]
    patches, mock_api = _patch_all(
        specs=specs,
        track_result={"status": "failed"},
    )
    mock_api.get.side_effect = [
        {"error_message": "disk full"},  # GET /data-transfers/{id}
    ]

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        with pytest.raises(RuntimeError, match="Dataset import failed"):
            _import(specs, cwd="/home/user")


def test_gdrive_source_ref_forwarded_in_manifest():
    """The folder_id is extracted and forwarded in source_ref to the manifest endpoint."""
    specs = [_gdrive_spec()]
    patches, mock_api = _patch_all(
        specs=specs,
        files=[],
        sizes={},
        hashes={},
        manifest_items=[_remote_manifest_item(upload_required=False)],
        folder_id="folder-abc",
    )

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        _import(specs, cwd="/home/user")

    manifest_call = mock_api.post.call_args_list[1]
    body = manifest_call.kwargs.get("json") or manifest_call[1].get("json") or manifest_call[0][1]
    gdrive_items = [i for i in body["items"] if i.get("provider") == "google_drive"]
    assert len(gdrive_items) == 1
    assert gdrive_items[0]["source_ref"] == {"folder_id": "folder-abc"}


def test_mount_path_spec_forwarded_for_gdrive():
    """GDrive spec with explicit mount_path sends mount_path (not base_dir) in manifest."""
    specs = [_gdrive_spec(mount_path="./remote-data")]
    patches, mock_api = _patch_all(
        specs=specs,
        files=[],
        sizes={},
        hashes={},
        manifest_items=[_remote_manifest_item(upload_required=False)],
    )

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        _import(specs, cwd="/home/user")

    manifest_call = mock_api.post.call_args_list[1]
    body = manifest_call.kwargs.get("json") or manifest_call[1].get("json") or manifest_call[0][1]
    gdrive_item = next(i for i in body["items"] if i.get("provider") == "google_drive")
    assert gdrive_item.get("mount_path") == "./remote-data"
    assert "base_dir" not in gdrive_item


def test_no_gdrive_specs_sends_base_dir_not_mount_path():
    """GDrive spec without mount_path sends base_dir (CWD) in the manifest item."""
    specs = [_gdrive_spec()]  # no mount_path
    patches, mock_api = _patch_all(
        specs=specs,
        files=[],
        sizes={},
        hashes={},
        manifest_items=[_remote_manifest_item(upload_required=False)],
    )

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        _import(specs, cwd="/home/user")

    manifest_call = mock_api.post.call_args_list[1]
    body = manifest_call.kwargs.get("json") or manifest_call[1].get("json") or manifest_call[0][1]
    gdrive_item = next(i for i in body["items"] if i.get("provider") == "google_drive")
    assert gdrive_item.get("base_dir") == "/home/user"
    assert "mount_path" not in gdrive_item


# ---------------------------------------------------------------------------
# public accessions
# ---------------------------------------------------------------------------

def _public_spec(accession="SRR390728", kind="sra", mount_path=None):
    return SimpleNamespace(source=accession, kind=kind, mount_path=mount_path)


def _public_manifest_item(path="SRR390728_1.fastq.gz", size=101304405):
    return {
        "provider": "sra",
        "path": path,
        "upload_required": True,
        "file_public_id": f"file-{path}",
        "item_public_id": f"item-{path}",
        "mount_path": f"/home/user/{path}",
        "size": size,
    }


def _public_manifest_payload(specs, cwd="/home/user", manifest_items=None):
    patches, mock_api = _patch_all(
        specs=specs, files=[], sizes={}, hashes={},
        manifest_items=manifest_items or [_public_manifest_item()],
    )
    auth_mock = MagicMock()
    patches[5] = patch("thoa.core.remote_inputs.authorize_google_drive_transfer", auth_mock)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        prepared = lt.prepare_mixed_transfer(specs, cwd=cwd)
    manifest_call = next(c for c in mock_api.post.call_args_list if "manifest/unified" in str(c))
    return prepared, manifest_call.kwargs["json"]["items"], auth_mock


def test_public_accession_is_sent_as_source_ref_with_base_dir():
    _, items, auth_mock = _public_manifest_payload([_public_spec()])
    assert items == [{"provider": "sra", "source_ref": {"accession": "SRR390728"}, "base_dir": "/home/user"}]
    auth_mock.assert_not_called()  # public data needs no Google (or any) authorization


def test_public_mount_path_is_resolved_against_cwd():
    _, items, _ = _public_manifest_payload([_public_spec("PRJNA1", mount_path="reads/")])
    assert items[0]["mount_path"] == "/home/user/reads"


def test_assembly_accession_uses_its_own_provider():
    _, items, _ = _public_manifest_payload([_public_spec("GCF_000005845.2", kind="ncbi_assembly")])
    assert items[0]["provider"] == "ncbi_assembly"


def test_prepare_does_not_upload_or_start():
    prepared, _, _ = _public_manifest_payload([_public_spec()])
    assert prepared.transfer_id == _TRANSFER_ID
    assert prepared.has_public_items is True


def test_prepare_prints_resolved_size(capsys):
    _public_manifest_payload(
        [_public_spec()],
        manifest_items=[_public_manifest_item(), _public_manifest_item("SRR390728_2.fastq.gz", 101858469)],
    )
    out = capsys.readouterr().out
    assert "Resolved 1 public accession(s)" in out and "2 file(s)" in out and "193.8 MB" in out


def test_track_uses_byte_progress_for_public_imports():
    prepared = lt.PreparedTransfer(transfer_id=_TRANSFER_ID, manifest={"items": [_public_manifest_item()]})
    mock_api = MagicMock()
    track = MagicMock(return_value={"status": "completed"})
    with patch.object(lt, "api_client", mock_api), patch("thoa.core.remote_inputs.track_transfer_progress", track):
        lt.track_transfer(prepared)
    assert track.call_args.kwargs["by_bytes"] is True
    mock_api.get.assert_not_called()  # progress only: no resolved-context fetch


def test_provider_notes_are_printed(capsys):
    patches, mock_api = _patch_all(specs=[_public_spec("GSE348257", kind="geo")], files=[], sizes={}, hashes={})
    mock_api.post.side_effect = [
        {"public_id": _TRANSFER_ID},
        {"items": [], "notes": ["Raw reads for GSE348257 (PRJNA1533317) are not available at ENA yet"]},
    ]
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
        lt.prepare_mixed_transfer([_public_spec("GSE348257", kind="geo")], cwd="/home/user")
    assert "not available at ENA yet" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# local uploads: smallest first, under a client heartbeat
# ---------------------------------------------------------------------------

def test_local_files_upload_smallest_first():
    """Samplesheets and configs land within seconds, so the job can stage them early."""
    big, small = Path("/tmp/big.bam"), Path("/tmp/sheet.csv")
    prepared = lt.PreparedTransfer(
        transfer_id=_TRANSFER_ID,
        manifest={"items": [
            {**_local_manifest_item(str(big)), "item_public_id": "item-big", "size": 5_000_000},
            {**_local_manifest_item(str(small)), "item_public_id": "item-small", "size": 120},
        ]},
        local_files=[big, small],
        hashes={big: "md5-big", small: "md5-small"},
        sizes={big: 5_000_000, small: 120},
    )
    upload = MagicMock()
    with patch.object(lt, "api_client", MagicMock()), patch.object(lt, "upload_file_sas", upload):
        lt.upload_local_items(prepared)
    assert [c.args[0] for c in upload.call_args_list] == [small, big]


def test_client_heartbeat_beats_at_once_and_stops_on_exit():
    mock_api = MagicMock()
    with patch.object(lt, "api_client", mock_api):
        with lt.ClientHeartbeat(_TRANSFER_ID) as heartbeat:
            mock_api.post.assert_called_once()
            assert mock_api.post.call_args.args[0] == f"/data-transfers/{_TRANSFER_ID}/client-heartbeat"
            # Failures are silenced: a beat must never print over the upload progress bar.
            assert 404 in mock_api.post.call_args.kwargs["silent_status_codes"]
        heartbeat._thread.join(timeout=1)
    assert not heartbeat._thread.is_alive()


def test_client_heartbeat_survives_network_errors():
    mock_api = MagicMock()
    mock_api.post.side_effect = OSError("network down")
    with patch.object(lt, "api_client", mock_api):
        with lt.ClientHeartbeat(_TRANSFER_ID):
            pass
