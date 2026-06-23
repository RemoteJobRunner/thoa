"""
Unit tests for thoa.core.local_transfer.create_dataset().

Patches api_client, collect_files, file_sizes_in_bytes, hash_all, and
upload_file_sas so no real filesystem or network access is needed.
"""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

import thoa.core.local_transfer as lt


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_TRANSFER_ID = "transfer-abc"
_FILE_A = Path("/tmp/a.txt")
_FILE_B = Path("/tmp/b.txt")
_DATASET_ID = "dataset-xyz"
_INPUT_CONTEXT = {"/a.txt": "file-id-a"}


def _manifest_resp(items):
    return {"items": items}


def _completed_manifest():
    return {"status": "completed", "total_items": 1}


def _resolved_context():
    return {"dataset_public_id": _DATASET_ID, "input_context": _INPUT_CONTEXT}


def _patch_all(
    *,
    files=None,
    sizes=None,
    hashes=None,
    manifest_items=None,
):
    if files is None:
        files = [_FILE_A]
    if sizes is None:
        sizes = {_FILE_A: 100}
    if hashes is None:
        hashes = {_FILE_A: "md5-a"}
    if manifest_items is None:
        manifest_items = [
            {
                "path": str(_FILE_A),
                "upload_required": True,
                "upload_url": "https://sas/a",
                "item_public_id": "item-a",
            }
        ]

    mock_api = MagicMock()
    mock_api.post.side_effect = [
        {"public_id": _TRANSFER_ID},          # POST /data-transfers
        _manifest_resp(manifest_items),        # POST .../local/manifest
        {},                                    # POST .../start
        {},                                    # POST .../complete (if upload needed)
    ]
    mock_api.get.side_effect = [
        _completed_manifest(),                 # GET .../manifest (poll)
        _resolved_context(),                   # GET .../resolved-context
    ]

    patches = [
        patch.object(lt, "api_client", mock_api),
        patch.object(lt, "collect_files", return_value=files),
        patch.object(lt, "file_sizes_in_bytes", return_value=sizes),
        patch.object(lt, "hash_all", return_value=hashes),
        patch.object(lt, "upload_file_sas"),
        patch("thoa.core.local_transfer.time.sleep"),
    ]
    return patches, mock_api


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_empty_input_returns_early():
    with patch.object(lt, "collect_files", return_value=[]):
        result = lt.create_dataset(["/nonexistent"])
    assert result == {"dataset_public_id": None, "input_context": {}}


def test_happy_path_calls_endpoints_in_order():
    patches, mock_api = _patch_all()
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        result = lt.create_dataset([str(_FILE_A)])

    assert result["dataset_public_id"] == _DATASET_ID
    assert result["input_context"] == _INPUT_CONTEXT

    post_calls = mock_api.post.call_args_list
    assert post_calls[0] == call("/data-transfers", json={
        "provider": "local",
        "direction": "import",
        "remote_ref": {"provider": "local"},
    })
    assert f"/data-transfers/{_TRANSFER_ID}/local/manifest" in str(post_calls[1])
    assert f"/data-transfers/{_TRANSFER_ID}/start" in str(post_calls[2])


def test_upload_skipped_when_not_required():
    manifest_items = [
        {
            "path": str(_FILE_A),
            "upload_required": False,
            "upload_url": None,
            "item_public_id": "item-a",
        }
    ]
    patches, mock_api = _patch_all(manifest_items=manifest_items)

    upload_mock = MagicMock()
    patches[4] = patch.object(lt, "upload_file_sas", upload_mock)

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        lt.create_dataset([str(_FILE_A)])

    upload_mock.assert_not_called()


def test_upload_called_for_required_files():
    patches, mock_api = _patch_all()
    upload_mock = MagicMock()
    patches[4] = patch.object(lt, "upload_file_sas", upload_mock)

    # Re-wire post side_effect: create_transfer, manifest, complete, start
    mock_api.post.side_effect = [
        {"public_id": _TRANSFER_ID},
        _manifest_resp([{
            "path": str(_FILE_A),
            "upload_required": True,
            "upload_url": "https://sas/a",
            "item_public_id": "item-a",
        }]),
        {},  # /complete
        {},  # /start
    ]

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        lt.create_dataset([str(_FILE_A)])

    upload_mock.assert_called_once_with(_FILE_A, "https://sas/a", "md5-a")


def test_raises_on_failed_transfer():
    patches, mock_api = _patch_all()
    mock_api.get.side_effect = [
        {"status": "failed"},
        {"error_message": "something went wrong"},
    ]

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        with pytest.raises(RuntimeError, match="Local dataset import failed"):
            lt.create_dataset([str(_FILE_A)])


def test_partial_upload_only_uploads_required_files():
    """Two files; only second requires upload."""
    patches, mock_api = _patch_all(
        files=[_FILE_A, _FILE_B],
        sizes={_FILE_A: 100, _FILE_B: 200},
        hashes={_FILE_A: "md5-a", _FILE_B: "md5-b"},
        manifest_items=[
            {
                "path": str(_FILE_A),
                "upload_required": False,
                "upload_url": None,
                "item_public_id": "item-a",
            },
            {
                "path": str(_FILE_B),
                "upload_required": True,
                "upload_url": "https://sas/b",
                "item_public_id": "item-b",
            },
        ],
    )
    # Reorder post side_effects: transfer, manifest, complete-b, start
    mock_api.post.side_effect = [
        {"public_id": _TRANSFER_ID},
        _manifest_resp([
            {"path": str(_FILE_A), "upload_required": False, "upload_url": None, "item_public_id": "item-a"},
            {"path": str(_FILE_B), "upload_required": True, "upload_url": "https://sas/b", "item_public_id": "item-b"},
        ]),
        {},  # /complete for B
        {},  # /start
    ]
    upload_mock = MagicMock()
    patches[4] = patch.object(lt, "upload_file_sas", upload_mock)

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        lt.create_dataset([str(_FILE_A), str(_FILE_B)])

    upload_mock.assert_called_once_with(_FILE_B, "https://sas/b", "md5-b")
