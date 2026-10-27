"""
Integration tests for CLI commands against staging backend.
Requires THOA_STAGING_API_URL and THOA_STAGING_API_KEY env vars (or THOA_API_URL/THOA_API_KEY as fallback).
"""

import pytest
from typer.testing import CliRunner
from thoa.cli import app
from tests.integration.helpers import api_get, run_job

runner = CliRunner()


@pytest.fixture(autouse=True)
def _require_backend(backend_url_and_key):
    pass


@pytest.fixture(scope="module")
def job_input_dataset_id(backend_url_and_key, tmp_path_factory):
    input_dir = tmp_path_factory.mktemp("cli_api_input")
    (input_dir / "subdir").mkdir()
    (input_dir / "subdir" / "data.csv").write_text("a,b\n1,2\n")

    job_id, _ = run_job([
        "run", "--input", str(input_dir), "--tools", "bash",
        "--cmd", "echo done",
        "--n-cores", "2", "--ram", "4", "--storage", "20",
    ])

    resp = api_get("/jobs", params={"public_id": job_id})
    assert resp.status_code == 200, resp.text
    dataset_id = resp.json()[0].get("input_dataset_public_id")
    assert dataset_id, f"Job {job_id} has no input_dataset_public_id"
    return dataset_id


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


@pytest.mark.slow
def test_dataset_list_after_create(job_input_dataset_id):
    result = runner.invoke(app, ["dataset", "list"])
    assert result.exit_code == 0
    assert job_input_dataset_id[:8] in result.output


@pytest.mark.slow
def test_dataset_ls_shows_dataset_tree(job_input_dataset_id):
    result = runner.invoke(app, ["dataset", "ls", job_input_dataset_id])
    assert result.exit_code == 0


def test_dataset_ls_unknown_id_exits_ok():
    result = runner.invoke(app, ["dataset", "ls", "00000000-0000-0000-0000-000000000000"])
    assert result.exit_code == 0
    assert "not found" in result.output.lower() or "error" in result.output.lower()


# --- dataset download ---

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
