"""
Integration tests for CLI commands against a live test backend.
Requires the backend_url_and_key fixture from conftest.py.
"""

import pytest
import httpx
from typer.testing import CliRunner
from thoa.cli import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def use_test_backend(backend_url_and_key):
    """Ensure THOA_API_URL and THOA_API_KEY are set for every test."""
    pass


# --- helpers ---

def _create_file(url: str, api_key: str, filename: str = "test.txt") -> str:
    """Create a file record and return its public_id."""
    resp = httpx.post(
        f"{url}/files",
        json={"filename": filename, "size": 100, "md5sum": "abc123abc123abc123abc123abc12312"},
        headers={"X-API-Key": api_key},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["public_id"]


def _create_dataset(url: str, api_key: str, file_ids: list[str]) -> dict:
    """Create a dataset from file_ids and return the response JSON."""
    resp = httpx.post(
        f"{url}/datasets",
        json={"files": file_ids},
        headers={"X-API-Key": api_key},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


# --- jobs ---

def test_jobs_list_returns_zero_code():
    result = runner.invoke(app, ["jobs", "list"])
    assert result.exit_code == 0


def test_jobs_list_invalid_sort_exits_nonzero():
    result = runner.invoke(app, ["jobs", "list", "--sort-by", "invalid"])
    assert result.exit_code != 0


def test_jobs_list_with_limit():
    result = runner.invoke(app, ["jobs", "list", "--number", "5"])
    assert result.exit_code == 0


def test_jobs_list_sort_by_status():
    result = runner.invoke(app, ["jobs", "list", "--sort-by", "status"])
    assert result.exit_code == 0


# --- datasets ---

def test_dataset_list_returns_zero_code():
    result = runner.invoke(app, ["dataset", "list"])
    assert result.exit_code == 0


def test_dataset_list_invalid_sort_exits_nonzero():
    result = runner.invoke(app, ["dataset", "list", "--sort-by", "invalid"])
    assert result.exit_code != 0


def test_dataset_list_with_limit():
    result = runner.invoke(app, ["dataset", "list", "--number", "5"])
    assert result.exit_code == 0


def test_dataset_list_sort_by_size():
    result = runner.invoke(app, ["dataset", "list", "--sort-by", "size"])
    assert result.exit_code == 0


def test_dataset_list_sort_by_files():
    result = runner.invoke(app, ["dataset", "list", "--sort-by", "files"])
    assert result.exit_code == 0


def test_dataset_list_after_create(backend_url_and_key):
    url, api_key = backend_url_and_key

    file_id = _create_file(url, api_key)
    ds = _create_dataset(url, api_key, [file_id])

    result = runner.invoke(app, ["dataset", "list"])
    assert result.exit_code == 0
    assert ds["public_id"][:8] in result.output


def test_dataset_ls_shows_dataset_tree(backend_url_and_key):
    url, api_key = backend_url_and_key

    file_id = _create_file(url, api_key, filename="subdir/data.csv")
    ds = _create_dataset(url, api_key, [file_id])
    ds_id = ds["public_id"]

    result = runner.invoke(app, ["dataset", "ls", ds_id])
    assert result.exit_code == 0


def test_dataset_ls_unknown_id_exits_ok():
    """dataset ls with unknown UUID should print error but exit 0 (CLI handles it gracefully)."""
    result = runner.invoke(app, ["dataset", "ls", "00000000-0000-0000-0000-000000000000"])
    assert result.exit_code == 0
    assert "not found" in result.output.lower() or "error" in result.output.lower()


# --- dataset download ---

def test_dataset_download_real(download_fixture, tmp_path):
    """Downloads a real blob from Azurite and checks the file content on disk."""
    ds_id = download_fixture["dataset_public_id"]

    result = runner.invoke(app, ["dataset", "download", ds_id, str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "hello.txt").exists()
    assert (tmp_path / "hello.txt").read_text() == "Hello, Azurite!"


def test_dataset_download_empty(backend_url_and_key, tmp_path):
    """dataset download on a dataset without context (no job) — CLI exits 0 and reports no files."""
    url, api_key = backend_url_and_key

    file_id = _create_file(url, api_key)
    ds = _create_dataset(url, api_key, [file_id])
    ds_id = ds["public_id"]

    result = runner.invoke(app, ["dataset", "download", ds_id, str(tmp_path)])
    assert result.exit_code == 0
    assert "no files" in result.output.lower() or "0" in result.output


def test_dataset_download_unknown_id(tmp_path):
    """dataset download with non existing UUID — CLI goes with 0, sends and eeror"""
    result = runner.invoke(app, ["dataset", "download", "00000000-0000-0000-0000-000000000000", str(tmp_path)])
    assert result.exit_code == 0
    assert "not found" in result.output.lower() or "error" in result.output.lower()


# --- tools ---

def test_tools_shows_bioconda_url():
    result = runner.invoke(app, ["tools"])
    assert result.exit_code == 0
    assert "bioconda" in result.output.lower()


def test_tools_shows_conda_forge_url():
    result = runner.invoke(app, ["tools"])
    assert result.exit_code == 0
    assert "conda-forge" in result.output.lower()
