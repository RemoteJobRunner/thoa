from unittest.mock import patch

import thoa.entrypoint as ep


def test_main_runs_maintenance_probe_with_active_api_key(monkeypatch):
    monkeypatch.setattr(ep, "block_windows_unless_wsl", lambda: None)
    monkeypatch.setattr(ep, "check_min_client_version", lambda base_url: None)
    monkeypatch.setattr(ep, "get_api_key", lambda: "ak_123")
    monkeypatch.setattr(ep, "app", lambda: None)
    with patch.object(ep, "check_maintenance_notice") as probe:
        ep.main()
    probe.assert_called_once_with(ep.settings.THOA_API_URL, "ak_123")
