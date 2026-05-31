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
