"""
conftest.py for integration tests against staging.

Requires env vars:
  THOA_STAGING_API_URL  - e.g. https://test-api.thoa.io
  THOA_STAGING_API_KEY  - valid API key for that instance
Falls back to THOA_API_URL / THOA_API_KEY if staging vars are not set.
"""

import os
import pytest


def _patch_api_client(base_url: str, api_key: str) -> None:
    """
    Replace module-level api_client singletons with a client pointing at the
    staging URL. The ApiClient._request prepends /api to every path (nginx
    strips it on the server side), which matches staging behavior.
    """
    from thoa.core.api_utils import ApiClient
    import thoa.core.dataset_utils as _ds
    import thoa.core.job_utils as _jobs
    import thoa.cli.commands.run as _run

    client = ApiClient(base_url=base_url, api_key=api_key)
    _ds.client = client
    _jobs.api_client = client
    _run.api_client = client


@pytest.fixture(scope="session")
def backend_url_and_key():
    url = os.environ.get("THOA_STAGING_API_URL", os.environ.get("THOA_API_URL"))
    key = os.environ.get("THOA_STAGING_API_KEY", os.environ.get("THOA_API_KEY"))

    if not url or not key:
        pytest.skip("THOA_STAGING_API_URL/THOA_STAGING_API_KEY (or THOA_API_URL/THOA_API_KEY) must be set")

    _patch_api_client(url, key)

    yield url, key
