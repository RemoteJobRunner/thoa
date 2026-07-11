from importlib.metadata import PackageNotFoundError
from unittest.mock import patch, MagicMock
import httpx

import thoa.core.version_check as vc


def _resp(json_body, status=200):
    r = MagicMock(spec=httpx.Response)
    r.status_code = status
    r.json.return_value = json_body
    return r


def test_silent_when_current_version_is_recent(capsys, monkeypatch):
    monkeypatch.setattr(vc, "current_version", lambda: "0.5.0")
    with patch.object(httpx, "get", return_value=_resp({"min_client_version": "0.1.4"})):
        vc.check_min_client_version("http://api.example")
    err = capsys.readouterr().err
    assert err == ""


def test_warns_when_below_min(capsys, monkeypatch):
    monkeypatch.setattr(vc, "current_version", lambda: "0.1.3")
    with patch.object(httpx, "get", return_value=_resp({"min_client_version": "0.1.4"})):
        vc.check_min_client_version("http://api.example")
    err = capsys.readouterr().err
    assert "0.1.4" in err
    assert "0.1.3" in err
    assert "pip install" in err.lower()


def test_silent_on_network_error(capsys, monkeypatch):
    monkeypatch.setattr(vc, "current_version", lambda: "0.1.3")
    with patch.object(httpx, "get", side_effect=httpx.ConnectError("boom")):
        vc.check_min_client_version("http://api.example")
    err = capsys.readouterr().err
    assert err == ""


def test_silent_on_bad_status(capsys, monkeypatch):
    monkeypatch.setattr(vc, "current_version", lambda: "0.1.3")
    with patch.object(httpx, "get", return_value=_resp({}, status=500)):
        vc.check_min_client_version("http://api.example")
    err = capsys.readouterr().err
    assert err == ""


def test_silent_on_malformed_body(capsys, monkeypatch):
    monkeypatch.setattr(vc, "current_version", lambda: "0.1.3")
    with patch.object(httpx, "get", return_value=_resp({"unrelated": "junk"})):
        vc.check_min_client_version("http://api.example")
    err = capsys.readouterr().err
    assert err == ""


# ---------------------------------------------------------------------------
# current_version() fallback to pyproject.toml
# ---------------------------------------------------------------------------

def test_current_version_returns_installed_version():
    with patch.object(vc, "_pkg_version", return_value="1.2.3"):
        assert vc.current_version() == "1.2.3"


def test_current_version_falls_back_to_pyproject_toml():
    """When the package isn't installed, reads version from the real pyproject.toml."""
    with patch.object(vc, "_pkg_version", side_effect=PackageNotFoundError("thoa")):
        version = vc.current_version()
    # The real thoa/pyproject.toml exists next to version_check.py and has a version
    assert version != "0.0.0"
    assert "." in version  # e.g. "0.1.4"


def test_current_version_returns_zero_when_pyproject_missing():
    """No installed package and pyproject.toml absent → returns '0.0.0'."""
    with patch.object(vc, "_pkg_version", side_effect=PackageNotFoundError("thoa")):
        with patch("pathlib.Path.exists", return_value=False):
            result = vc.current_version()
    assert result == "0.0.0"
