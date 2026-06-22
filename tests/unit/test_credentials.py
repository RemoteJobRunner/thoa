"""Unit tests for thoa.core.credentials — read / write / delete."""
import os
import pytest
import thoa.core.credentials as creds_module


@pytest.fixture(autouse=True)
def isolated_credentials(tmp_path, monkeypatch):
    """Redirect all credential I/O to a throwaway tmp directory."""
    thoa_dir = tmp_path / ".thoa"
    creds_file = thoa_dir / "credentials"
    monkeypatch.setattr(creds_module, "_THOA_DIR", thoa_dir)
    monkeypatch.setattr(creds_module, "_CREDENTIALS_FILE", creds_file)


# ---------------------------------------------------------------------------
# read_credentials
# ---------------------------------------------------------------------------

def test_read_returns_none_when_file_missing():
    assert creds_module.read_credentials() is None


def test_read_returns_none_on_corrupt_json():
    creds_module._THOA_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    creds_module._CREDENTIALS_FILE.write_text("not valid json {{{")
    assert creds_module.read_credentials() is None


def test_read_returns_none_on_empty_file():
    creds_module._THOA_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    creds_module._CREDENTIALS_FILE.write_text("")
    assert creds_module.read_credentials() is None


# ---------------------------------------------------------------------------
# write_credentials
# ---------------------------------------------------------------------------

def test_write_and_read_round_trip():
    data = {"api_key": "rs_abc123", "email": "test@example.com", "expires_at": "2026-09-01T10:00:00"}
    creds_module.write_credentials(data)
    assert creds_module.read_credentials() == data


def test_write_creates_directory_if_missing():
    assert not creds_module._THOA_DIR.exists()
    creds_module.write_credentials({"api_key": "rs_test"})
    assert creds_module._THOA_DIR.exists()


def test_write_sets_file_permissions_to_600():
    creds_module.write_credentials({"api_key": "rs_test"})
    mode = os.stat(creds_module._CREDENTIALS_FILE).st_mode & 0o777
    assert mode == 0o600


def test_write_overwrites_existing_credentials():
    creds_module.write_credentials({"api_key": "rs_old"})
    creds_module.write_credentials({"api_key": "rs_new"})
    assert creds_module.read_credentials()["api_key"] == "rs_new"


def test_write_preserves_all_fields():
    data = {
        "api_key": "rs_xyz",
        "public_id": "pub-abc-123",
        "expires_at": "2026-12-31T23:59:59",
        "email": "user@thoa.io",
    }
    creds_module.write_credentials(data)
    assert creds_module.read_credentials() == data


# ---------------------------------------------------------------------------
# delete_credentials
# ---------------------------------------------------------------------------

def test_delete_removes_file():
    creds_module.write_credentials({"api_key": "rs_test"})
    creds_module.delete_credentials()
    assert creds_module.read_credentials() is None


def test_delete_noop_when_file_missing():
    creds_module.delete_credentials()  # must not raise


def test_delete_then_write_works():
    creds_module.write_credentials({"api_key": "rs_first"})
    creds_module.delete_credentials()
    creds_module.write_credentials({"api_key": "rs_second"})
    assert creds_module.read_credentials()["api_key"] == "rs_second"
