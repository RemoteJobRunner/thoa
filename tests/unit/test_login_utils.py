"""Unit tests for login/logout helpers.

Covers:
  - _fmt_expiry          (pure formatting, no I/O)
  - _check_existing_session  (checks stored creds against server)
  - logout command       (revokes key, deletes local credentials)
"""
from unittest.mock import MagicMock, patch
import pytest
from typer.testing import CliRunner


# ---------------------------------------------------------------------------
# _fmt_expiry
# ---------------------------------------------------------------------------

class TestFmtExpiry:
    def _fmt(self, value):
        from thoa.cli.commands.login import _fmt_expiry
        return _fmt_expiry(value)

    def test_valid_iso_contains_month_and_year(self):
        result = self._fmt("2026-09-20T11:10:13.759396")
        assert "Sep 2026" in result
        assert "11:10" in result

    def test_none_returns_na(self):
        assert self._fmt(None) == "N/A"

    def test_empty_string_returns_na(self):
        assert self._fmt("") == "N/A"

    def test_invalid_string_returns_original(self):
        assert self._fmt("not-a-date") == "not-a-date"

    def test_date_only_iso_no_time_component(self):
        result = self._fmt("2026-01-15T00:00:00")
        assert "Jan 2026" in result
        assert "00:00" in result


# ---------------------------------------------------------------------------
# _check_existing_session
# ---------------------------------------------------------------------------

class TestCheckExistingSession:
    def _check(self):
        from thoa.cli.commands.login import _check_existing_session
        return _check_existing_session()

    def test_returns_false_when_no_credentials(self):
        with patch("thoa.cli.commands.login.read_credentials", return_value=None):
            assert self._check() is False

    def test_returns_false_when_credentials_missing_api_key(self):
        with patch("thoa.cli.commands.login.read_credentials", return_value={"email": "x@x.com"}):
            assert self._check() is False

    def test_returns_true_when_server_confirms_valid_key(self):
        creds = {"api_key": "rs_live", "email": "user@thoa.io", "expires_at": "2026-12-01T10:00:00"}
        mock_client = MagicMock()
        mock_client.get.return_value = [{"name": "CLI key"}]
        with patch("thoa.cli.commands.login.read_credentials", return_value=creds), \
             patch("thoa.cli.commands.login.ApiClient", return_value=mock_client):
            result = self._check()
        assert result is True
        mock_client.get.assert_called_once_with("/api_keys", silent_status_codes={401, 403, 426})
        mock_client.close.assert_called_once()

    def test_returns_false_when_server_rejects_key(self):
        creds = {"api_key": "rs_expired", "email": "user@thoa.io"}
        mock_client = MagicMock()
        mock_client.get.return_value = None  # ApiClient returns None for 401/403/426
        with patch("thoa.cli.commands.login.read_credentials", return_value=creds), \
             patch("thoa.cli.commands.login.ApiClient", return_value=mock_client):
            result = self._check()
        assert result is False

    def test_returns_false_when_credentials_have_empty_api_key(self):
        with patch("thoa.cli.commands.login.read_credentials", return_value={"api_key": ""}):
            assert self._check() is False


# ---------------------------------------------------------------------------
# logout command
# ---------------------------------------------------------------------------

class TestLogout:
    def setup_method(self):
        from thoa.cli import app
        self.runner = CliRunner()
        self.app = app

    def _invoke(self):
        return self.runner.invoke(self.app, ["logout"])

    def test_not_logged_in_exits_cleanly(self):
        with patch("thoa.cli.commands.logout.read_credentials", return_value=None):
            result = self._invoke()
        assert result.exit_code == 0
        assert "Not logged in" in result.output

    def test_revokes_key_and_deletes_credentials(self):
        creds = {"api_key": "rs_live", "public_id": "pub-abc-123"}
        mock_client = MagicMock()
        with patch("thoa.cli.commands.logout.read_credentials", return_value=creds), \
             patch("thoa.cli.commands.logout.ApiClient", return_value=mock_client), \
             patch("thoa.cli.commands.logout.delete_credentials") as mock_delete:
            result = self._invoke()
        assert result.exit_code == 0
        mock_client.delete.assert_called_once_with(
            f"/api_keys/{creds['public_id']}", silent_status_codes={404}
        )
        mock_client.close.assert_called_once()
        mock_delete.assert_called_once()

    def test_still_deletes_credentials_when_server_unreachable(self):
        creds = {"api_key": "rs_live", "public_id": "pub-abc-123"}
        mock_client = MagicMock()
        mock_client.delete.side_effect = Exception("connection refused")
        with patch("thoa.cli.commands.logout.read_credentials", return_value=creds), \
             patch("thoa.cli.commands.logout.ApiClient", return_value=mock_client), \
             patch("thoa.cli.commands.logout.delete_credentials") as mock_delete:
            result = self._invoke()
        assert result.exit_code == 0
        mock_delete.assert_called_once()

    def test_missing_public_id_skips_api_revocation(self):
        """If public_id is absent we can't revoke server-side, but local cleanup still happens."""
        creds = {"api_key": "rs_live"}
        with patch("thoa.cli.commands.logout.read_credentials", return_value=creds), \
             patch("thoa.cli.commands.logout.delete_credentials") as mock_delete:
            result = self._invoke()
        assert result.exit_code == 0
        mock_delete.assert_called_once()

    def test_missing_api_key_skips_api_revocation(self):
        """If the stored api_key is absent we cannot authenticate the DELETE call."""
        creds = {"public_id": "pub-abc-123"}
        with patch("thoa.cli.commands.logout.read_credentials", return_value=creds), \
             patch("thoa.cli.commands.logout.delete_credentials") as mock_delete:
            result = self._invoke()
        assert result.exit_code == 0
        mock_delete.assert_called_once()

    def test_prints_logged_out_on_success(self):
        creds = {"api_key": "rs_live", "public_id": "pub-abc-123"}
        mock_client = MagicMock()
        with patch("thoa.cli.commands.logout.read_credentials", return_value=creds), \
             patch("thoa.cli.commands.logout.ApiClient", return_value=mock_client), \
             patch("thoa.cli.commands.logout.delete_credentials"):
            result = self._invoke()
        assert "Logged out" in result.output
