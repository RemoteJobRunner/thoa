from importlib.metadata import version as pkg_version
from thoa.core.api_utils import ApiClient


def test_api_client_sends_client_version_header():
    client = ApiClient(base_url="http://example.invalid", api_key="dummy")
    headers = client.client.headers
    assert "x-client-version" in {k.lower() for k in headers.keys()}
    assert headers["X-Client-Version"] == pkg_version("thoa")


def test_api_client_still_sends_api_key_and_accept():
    client = ApiClient(base_url="http://example.invalid", api_key="dummy")
    headers = client.client.headers
    assert headers["X-API-Key"] == "dummy"
    assert headers["Accept"] == "application/json"
