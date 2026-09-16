"""Unit tests for Google Drive URL parsing and input-spec detection."""

import pytest

from thoa.core.input_specs import parse_input_spec
from thoa.core.remote_inputs import (
    detect_input_source_kind,
    extract_google_drive_file_id,
    extract_google_drive_folder_id,
)

FILE_URL = (
    "https://drive.google.com/file/d/1uGuTAXo8l4Nb42e166_kQ0zZg0UE-XfF/"
    "view?usp=drive_link"
)
FOLDER_URL = "https://drive.google.com/drive/folders/1AbCdEfGhIjKlMnOpQrStUvWxYz"


def test_extract_file_id_from_sharing_link():
    assert extract_google_drive_file_id(FILE_URL) == "1uGuTAXo8l4Nb42e166_kQ0zZg0UE-XfF"


def test_extract_file_id_ignores_folder_links():
    assert extract_google_drive_file_id(FOLDER_URL) is None


def test_extract_folder_id_ignores_file_links():
    assert extract_google_drive_folder_id(FILE_URL) is None


def test_detect_input_source_kind_recognizes_file_and_folder_links():
    assert detect_input_source_kind(FILE_URL) == "google_drive"
    assert detect_input_source_kind(FOLDER_URL) == "google_drive"
    assert detect_input_source_kind("s3://bucket/key") == "s3"
    assert detect_input_source_kind("not-a-url") == "unknown"


def test_parse_input_spec_splits_file_link_and_mount_path():
    spec = parse_input_spec(f"{FILE_URL}::/data/inputs")
    assert spec.kind == "google_drive"
    assert spec.source == FILE_URL
    assert spec.mount_path == "/data/inputs"


class TestTrackTransferProgressTimeout:
    """The polling loop must not hang forever if the backend never marks a
    transfer completed/failed — this is the CLI's defensive backstop for
    the backend hang bug (invalid Google Drive input never surfacing an
    error). It must also NOT kill a large, slow-but-healthy transfer just
    because it's taking a long time — only genuine inactivity should trip
    it."""

    @staticmethod
    def _fake_time_factory():
        fake_now = [1_000_000.0]

        def fake_time():
            fake_now[0] += 1.0
            return fake_now[0]

        return fake_time

    def test_gives_up_after_stall_timeout_with_zero_progress(self, monkeypatch, capsys):
        import typer

        from thoa.core import remote_inputs

        # Manifest never changes at all, so the only way out of the loop
        # is the stall timeout.
        poll_calls = []

        def fake_get(*args, **kwargs):
            poll_calls.append(1)
            return {
                "status": "importing", "total_items": 1, "skipped_items": 0,
                "completed_bytes": 0, "completed_paths": [],
            }

        monkeypatch.setattr(remote_inputs.api_client, "get", fake_get)
        monkeypatch.setattr(remote_inputs.time, "sleep", lambda *_: None)
        monkeypatch.setattr(remote_inputs.time, "time", self._fake_time_factory())

        with pytest.raises(typer.Exit):
            remote_inputs.track_transfer_progress(
                "transfer-123", label="Importing", stall_timeout_seconds=5,
            )

        # Actually polled multiple times before giving up, not just once.
        assert len(poll_calls) >= 3
        out = capsys.readouterr().out
        assert "transfer-123" in out
        assert "no progress" in out.lower()

    def test_does_not_time_out_while_bytes_keep_completing(self, monkeypatch):
        """A large transfer that keeps completing files must never trip the
        stall timeout, no matter how long it runs in total — this is the
        big-data case: the clock only measures inactivity, not duration."""
        from thoa.core import remote_inputs

        # Each poll reports more completed bytes than the last, simulating
        # a large multi-file import that's slow but always moving. Runs
        # for far longer (in simulated time) than the stall window.
        polls = {"n": 0}

        def fake_get(*args, **kwargs):
            polls["n"] += 1
            if polls["n"] >= 50:
                return {
                    "status": "completed", "total_items": 50, "skipped_items": 0,
                    "completed_bytes": polls["n"] * 1_000_000,
                    "completed_paths": [f"file-{i}.txt" for i in range(polls["n"])],
                }
            return {
                "status": "importing", "total_items": 50, "skipped_items": 0,
                "completed_bytes": polls["n"] * 1_000_000,
                "completed_paths": [f"file-{i}.txt" for i in range(polls["n"])],
            }

        monkeypatch.setattr(remote_inputs.api_client, "get", fake_get)
        monkeypatch.setattr(remote_inputs.time, "sleep", lambda *_: None)
        monkeypatch.setattr(remote_inputs.time, "time", self._fake_time_factory())

        # stall_timeout_seconds (5) is far shorter than the ~50s of
        # simulated elapsed time this takes — it must still finish clean.
        result = remote_inputs.track_transfer_progress(
            "transfer-456", label="Importing", stall_timeout_seconds=5, show_paths=False,
        )

        assert result["status"] == "completed"
        assert polls["n"] >= 50
