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


def test_request_timeout_returns_none_and_prints_clean_message(capsys):
    import httpx

    client = ApiClient(base_url="http://example.invalid", api_key="dummy")

    def raise_timeout(*args, **kwargs):
        raise httpx.TimeoutException("timed out")

    client.client.request = raise_timeout

    result = client.get("/data-transfers/x/manifest")

    assert result is None
    out = capsys.readouterr().out
    assert "timed out" in out.lower()


def test_request_transport_error_returns_none_and_prints_clean_message(capsys):
    import httpx

    client = ApiClient(base_url="http://example.invalid", api_key="dummy")

    def raise_transport_error(*args, **kwargs):
        raise httpx.ConnectError("connection refused")

    client.client.request = raise_transport_error

    result = client.get("/data-transfers/x/manifest")

    assert result is None
    out = capsys.readouterr().out
    assert "could not reach the server" in out.lower()
