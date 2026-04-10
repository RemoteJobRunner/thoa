"""
Integration tests for CLI commands against staging backend.
Requires THOA_STAGING_API_URL and THOA_STAGING_API_KEY env vars (or THOA_API_URL/THOA_API_KEY as fallback).
"""

import pytest
from typer.testing import CliRunner
from thoa.cli import app
from tests.integration.helpers import create_file, create_dataset

runner = CliRunner()


@pytest.fixture(autouse=True)
def _require_backend(backend_url_and_key):
    pass


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


def test_dataset_list_after_create():
    file_id = create_file()
    ds = create_dataset([file_id])
    result = runner.invoke(app, ["dataset", "list"])
    assert result.exit_code == 0
    assert ds["public_id"][:8] in result.output


def test_dataset_ls_shows_dataset_tree():
    file_id = create_file(filename="subdir/data.csv")
    ds = create_dataset([file_id])
    result = runner.invoke(app, ["dataset", "ls", ds["public_id"]])
    assert result.exit_code == 0


def test_dataset_ls_unknown_id_exits_ok():
    result = runner.invoke(app, ["dataset", "ls", "00000000-0000-0000-0000-000000000000"])
    assert result.exit_code == 0
    assert "not found" in result.output.lower() or "error" in result.output.lower()


# --- dataset download ---

def test_dataset_download_empty(tmp_path):
    file_id = create_file()
    ds = create_dataset([file_id])
    result = runner.invoke(app, ["dataset", "download", ds["public_id"], str(tmp_path)])
    assert result.exit_code == 0
    assert "no files" in result.output.lower() or "0" in result.output


def test_dataset_download_unknown_id(tmp_path):
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
