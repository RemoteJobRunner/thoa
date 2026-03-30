"""
Integration tests for CLI commands against a live test backend.
Requires the backend_url_and_key fixture from conftest.py.
"""

import pytest
from typer.testing import CliRunner
from thoa.cli import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def use_test_backend(backend_url_and_key):
    """Ensure THOA_API_URL and THOA_API_KEY are set for every test."""
    pass


def test_jobs_list_returns_zero_code():
    result = runner.invoke(app, ["jobs", "list"])
    assert result.exit_code == 0


def test_dataset_list_returns_zero_code():
    result = runner.invoke(app, ["dataset", "list"])
    assert result.exit_code == 0
