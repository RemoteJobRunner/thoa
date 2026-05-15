"""Unit tests for Google Drive URL parsing and input-spec detection."""

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
