from unittest.mock import patch, MagicMock
import httpx

import thoa.core.maintenance_check as mc


MESSAGE = (
    "Thoa has scheduled maintenance coming up. Job submission will be temporarily "
    "unavailable until approximately 25 Jul 2026, 18:00 UTC."
)


def _resp(json_body, status=200):
    r = MagicMock(spec=httpx.Response)
    r.status_code = status
    r.json.return_value = json_body
    return r


def test_prints_message_when_active(capsys):
    with patch.object(httpx, "get", return_value=_resp({"active": True, "message": MESSAGE})):
        mc.check_maintenance_notice("http://api.example", "ak_123")
    captured = capsys.readouterr()
    assert MESSAGE in captured.err
    assert captured.out == ""


def test_requests_maintenance_with_cli_headers(monkeypatch):
    monkeypatch.setattr(mc, "current_version", lambda: "0.5.0")
    with patch.object(httpx, "get", return_value=_resp({"active": False})) as get:
        mc.check_maintenance_notice("http://api.example/", "ak_123")
    args, kwargs = get.call_args
    assert args[0] == "http://api.example/api/maintenance"
    assert kwargs["headers"]["X-API-Key"] == "ak_123"
    assert kwargs["headers"]["X-Client-Version"] == "0.5.0"


def test_silent_when_inactive(capsys):
    with patch.object(httpx, "get", return_value=_resp({"active": False, "message": None})):
        mc.check_maintenance_notice("http://api.example", "ak_123")
    assert capsys.readouterr().err == ""


def test_skips_request_without_api_key(capsys):
    with patch.object(httpx, "get") as get:
        mc.check_maintenance_notice("http://api.example", None)
    get.assert_not_called()
    assert capsys.readouterr().err == ""


def test_silent_on_network_error(capsys):
    with patch.object(httpx, "get", side_effect=httpx.ConnectError("boom")):
        mc.check_maintenance_notice("http://api.example", "ak_123")
    assert capsys.readouterr().err == ""


def test_silent_on_bad_status(capsys):
    with patch.object(httpx, "get", return_value=_resp({"detail": "Unauthorized"}, status=401)):
        mc.check_maintenance_notice("http://api.example", "ak_123")
    assert capsys.readouterr().err == ""


def test_silent_on_malformed_body(capsys):
    r = _resp(None)
    r.json.side_effect = ValueError("not json")
    with patch.object(httpx, "get", return_value=r):
        mc.check_maintenance_notice("http://api.example", "ak_123")
    assert capsys.readouterr().err == ""


def test_silent_on_non_dict_body(capsys):
    with patch.object(httpx, "get", return_value=_resp(["junk"])):
        mc.check_maintenance_notice("http://api.example", "ak_123")
    assert capsys.readouterr().err == ""
