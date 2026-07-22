import pytest
from collections import Counter
from pathlib import Path
from unittest.mock import MagicMock

import thoa.core.dataset_utils as dataset_utils
from thoa.core.dataset_utils import (
    _filter_files_by_id_or_path,
    _fmt_bytes,
    _nearest_existing_parent,
    _required_with_headroom,
    _format_timestamp,
    _parse_timestamp,
    _sizes_match,
    _normalize_md5_hex_or_b64_to_hex,
    _extract_url,
    _safe_dest,
    _build_tree,
    _filtered_download_size,
    _should_decrement_downloads,
    download_dataset,
)


class TestFilterFilesByIdOrPath:

    def test_no_filters_returns_all(self):
        files = {"a/b.txt": "id1", "c/d.txt": "id2"}
        assert _filter_files_by_id_or_path(files, None, None) == files

    def test_include_by_glob(self):
        files = {"data/file.bam": "id1", "data/file.vcf": "id2", "logs/out.log": "id3"}
        result = _filter_files_by_id_or_path(files, include=["*.bam"], exclude=None)
        assert result == {"data/file.bam": "id1"}

    def test_include_by_file_id(self):
        files = {"a.txt": "id1", "b.txt": "id2"}
        result = _filter_files_by_id_or_path(files, include=["id2"], exclude=None)
        assert result == {"b.txt": "id2"}

    def test_exclude_by_glob(self):
        files = {"a.txt": "id1", "b.log": "id2", "c.txt": "id3"}
        result = _filter_files_by_id_or_path(files, include=None, exclude=["*.log"])
        assert result == {"a.txt": "id1", "c.txt": "id3"}

    def test_exclude_by_file_id(self):
        files = {"a.txt": "id1", "b.txt": "id2"}
        result = _filter_files_by_id_or_path(files, include=None, exclude=["id1"])
        assert result == {"b.txt": "id2"}

    def test_include_and_exclude_combined(self):
        files = {"a.bam": "id1", "b.bam": "id2", "c.txt": "id3"}
        result = _filter_files_by_id_or_path(files, include=["*.bam"], exclude=["id2"])
        assert result == {"a.bam": "id1"}

    def test_empty_files(self):
        assert _filter_files_by_id_or_path({}, ["*.txt"], None) == {}


class TestFmtBytes:

    def test_bytes(self):
        assert _fmt_bytes(500) == "500.00 B"

    def test_kibibytes(self):
        result = _fmt_bytes(2048)
        assert "KiB" in result

    def test_mebibytes(self):
        result = _fmt_bytes(5 * 1024 * 1024)
        assert "MiB" in result

    def test_gibibytes(self):
        result = _fmt_bytes(2 * 1024 ** 3)
        assert "GiB" in result

    def test_zero(self):
        assert _fmt_bytes(0) == "0.00 B"


class TestNearestExistingParent:

    def test_existing_directory(self, tmp_path):
        assert _nearest_existing_parent(tmp_path) == tmp_path

    def test_nonexistent_child(self, tmp_path):
        deep = tmp_path / "a" / "b" / "c"
        assert _nearest_existing_parent(deep) == tmp_path


class TestRequiredWithHeadroom:

    def test_small_file_min_headroom(self):
        # 200 MiB min headroom
        result = _required_with_headroom(1000)
        assert result == 1000 + 200 * 1024 * 1024

    def test_large_file_5_percent_headroom(self):
        size = 10 * 1024 ** 3  # 10 GiB
        result = _required_with_headroom(size)
        assert result == size + int(size * 0.05)


class TestTimestampHelpers:

    def test_format_iso_z(self):
        assert _format_timestamp("2025-03-15T10:30:45Z") == "Mar 15 2025, 10:30"

    def test_format_iso_microseconds(self):
        result = _format_timestamp("2025-03-15T10:30:45.123456Z")
        assert result == "Mar 15 2025, 10:30"

    def test_format_garbage_returns_input(self):
        assert _format_timestamp("nope") == "nope"

    def test_parse_returns_datetime(self):
        from datetime import datetime
        dt = _parse_timestamp("2025-03-15T10:30:45Z")
        assert isinstance(dt, datetime)
        assert dt.year == 2025

    def test_parse_garbage_returns_none(self):
        assert _parse_timestamp("nope") is None


class TestSizesMatch:

    def test_matching_size(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")  # 5 bytes
        assert _sizes_match(f, 5) is True

    def test_wrong_size(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")
        assert _sizes_match(f, 999) is False

    def test_missing_file(self, tmp_path):
        f = tmp_path / "nope.txt"
        assert _sizes_match(f, 5) is False


class TestNormalizeMd5:

    def test_hex_32_chars(self):
        md5 = "5eb63bbbe01eeed093cb22bb8f5acdc3"
        assert _normalize_md5_hex_or_b64_to_hex(md5) == md5

    def test_hex_uppercase(self):
        md5 = "5EB63BBBE01EEED093CB22BB8F5ACDC3"
        assert _normalize_md5_hex_or_b64_to_hex(md5) == md5.lower()

    def test_base64_16_bytes(self):
        import base64, hashlib
        raw = hashlib.md5(b"test").digest()  # 16 bytes
        b64 = base64.b64encode(raw).decode()
        result = _normalize_md5_hex_or_b64_to_hex(b64)
        assert result == raw.hex()

    def test_none_returns_none(self):
        assert _normalize_md5_hex_or_b64_to_hex(None) is None

    def test_empty_returns_none(self):
        assert _normalize_md5_hex_or_b64_to_hex("") is None

    def test_garbage_returns_none(self):
        assert _normalize_md5_hex_or_b64_to_hex("not-md5") is None


class TestExtractUrl:

    def test_url_key(self):
        assert _extract_url({"url": "https://example.com"}) == "https://example.com"

    def test_sas_url_key(self):
        assert _extract_url({"sas_url": "https://blob.azure"}) == "https://blob.azure"

    def test_none_input(self):
        assert _extract_url(None) is None

    def test_empty_dict(self):
        assert _extract_url({}) is None


class TestSafeDest:

    def test_relative_path(self, tmp_path):
        result = _safe_dest(tmp_path, "data/file.txt")
        assert result == (tmp_path / "data" / "file.txt").resolve()

    def test_absolute_path_stays_under_base(self, tmp_path):
        result = _safe_dest(tmp_path, "/home/user/data/file.txt")
        assert str(result).startswith(str(tmp_path.resolve()))
        assert result.name == "file.txt"


class TestBuildTree:

    def test_flat_files(self):
        files = {"a.txt": "id1", "b.txt": "id2"}
        tree = _build_tree(files)
        assert tree["a.txt"] == {"__file_id__": "id1"}
        assert tree["b.txt"] == {"__file_id__": "id2"}

    def test_nested_files(self):
        files = {"data/sub/file.txt": "id1"}
        tree = _build_tree(files)
        assert tree["data"]["sub"]["file.txt"] == {"__file_id__": "id1"}

    def test_empty(self):
        assert _build_tree({}) == {}


class FakeClient:
    """Minimal stand-in for api_client, routing by path prefix."""

    def __init__(self, dataset, files=None):
        self.dataset = dataset
        self.files = files or []
        self.get_calls = []
        self.put_calls = []
        self.post_calls = []

    def get(self, path, **kwargs):
        self.get_calls.append(path)
        if path.startswith("/datasets?public_id="):
            return [self.dataset]
        if path.startswith("/files?dataset_public_id="):
            return self.files
        return None

    def put(self, path, **kwargs):
        self.put_calls.append(path)
        return {}

    def post(self, path, **kwargs):
        self.post_calls.append(path)
        return {"url": "https://example.com/fake-blob"}


class TestFilteredDownloadSize:

    def test_no_filter_returns_total_size_without_extra_lookup(self, monkeypatch):
        fake_client = MagicMock()
        monkeypatch.setattr(dataset_utils, "client", fake_client)

        result = _filtered_download_size(
            total_size=10_000,
            files={"a.bam": "id1"},
            include=None,
            exclude=None,
            dataset_id="ds1",
        )

        assert result == 10_000
        fake_client.get.assert_not_called()

    def test_filter_sums_only_matched_subset(self, monkeypatch):
        fake = FakeClient(
            dataset=None,
            files=[
                {"public_id": "id1", "size": 1_000},
                {"public_id": "id2", "size": 34 * 1024**3},
            ],
        )
        monkeypatch.setattr(dataset_utils, "client", fake)

        result = _filtered_download_size(
            total_size=34 * 1024**3 + 1_000,
            files={"logs/hs_err_pid42.log": "id1"},  # only id1 survived the include filter
            include=["*/hs_err_pid42.log"],
            exclude=None,
            dataset_id="ds1",
        )

        assert result == 1_000

    def test_falls_back_to_total_size_when_sizes_unavailable(self, monkeypatch):
        fake = FakeClient(dataset=None, files=[])
        monkeypatch.setattr(dataset_utils, "client", fake)

        result = _filtered_download_size(
            total_size=5_000,
            files={"a.bam": "id1"},
            include=["*.bam"],
            exclude=None,
            dataset_id="ds1",
        )

        assert result == 5_000


class TestShouldDecrementDownloads:

    def test_false_when_nothing_succeeded(self):
        assert _should_decrement_downloads(Counter({"failed": 2})) is False

    def test_false_when_empty(self):
        assert _should_decrement_downloads(Counter()) is False

    def test_false_when_only_skipped(self):
        # Files already present locally aren't a fresh transfer.
        assert _should_decrement_downloads(Counter({"skipped": 2})) is False

    def test_true_when_any_success(self):
        assert _should_decrement_downloads(Counter({"success": 1, "failed": 3})) is True


class TestDownloadDatasetQuotaGating:
    """Integration-style tests: quota should only be consumed on actual transfer."""

    @staticmethod
    def _dataset(remaining=1, total_size=1000, context=None):
        return {
            "remaining_downloads": remaining,
            "total_size": total_size,
            "adjusted_context": context if context is not None else {"a.txt": "id1"},
        }

    def test_no_files_matched_filter_does_not_decrement(self, tmp_path, monkeypatch):
        fake = FakeClient(dataset=self._dataset(context={"a.txt": "id1"}))
        monkeypatch.setattr(dataset_utils, "client", fake)

        download_dataset("ds1", str(tmp_path), include=["*.nomatch"])

        assert fake.put_calls == []

    def test_disk_space_failure_does_not_decrement(self, tmp_path, monkeypatch):
        fake = FakeClient(dataset=self._dataset(total_size=10**12))
        monkeypatch.setattr(dataset_utils, "client", fake)
        monkeypatch.setattr(dataset_utils, "_available_bytes", lambda p: 0)

        download_dataset("ds1", str(tmp_path))

        assert fake.put_calls == []

    def test_all_transfers_failed_does_not_decrement(self, tmp_path, monkeypatch):
        fake = FakeClient(dataset=self._dataset())
        monkeypatch.setattr(dataset_utils, "client", fake)
        monkeypatch.setattr(dataset_utils, "_available_bytes", lambda p: 10**12)
        monkeypatch.setattr(
            dataset_utils,
            "_download_one",
            lambda path_string, file_id, link_info, base_dir, verify_md5, per_blob_concurrency, chunk_size: (
                path_string, file_id, False, "error:boom",
            ),
        )

        download_dataset("ds1", str(tmp_path))

        assert fake.put_calls == []

    def test_successful_transfer_decrements_once(self, tmp_path, monkeypatch):
        fake = FakeClient(dataset=self._dataset())
        monkeypatch.setattr(dataset_utils, "client", fake)
        monkeypatch.setattr(dataset_utils, "_available_bytes", lambda p: 10**12)
        monkeypatch.setattr(
            dataset_utils,
            "_download_one",
            lambda path_string, file_id, link_info, base_dir, verify_md5, per_blob_concurrency, chunk_size: (
                path_string, file_id, True, "downloaded",
            ),
        )

        download_dataset("ds1", str(tmp_path))

        assert fake.put_calls == ["/datasets/ds1/decrement_downloads"]

    def test_zero_remaining_downloads_does_not_decrement(self, tmp_path, monkeypatch):
        fake = FakeClient(dataset=self._dataset(remaining=0))
        monkeypatch.setattr(dataset_utils, "client", fake)

        download_dataset("ds1", str(tmp_path))

        assert fake.put_calls == []
