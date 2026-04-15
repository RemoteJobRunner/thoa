import pytest
from pathlib import Path
from datetime import datetime

from thoa.core.job_utils import (
    collect_files,
    compute_md5_buffered,
    compute_md5_mmap,
    choose_hash_strategy,
    hash_all,
    file_sizes_in_bytes,
    _parse_job_timestamp,
    _fmt_job_timestamp,
)


class TestCollectFiles:

    def test_collects_files_from_directory(self, tmp_path):
        for i in range(3):
            (tmp_path / f"file_{i}.txt").touch()
        result = collect_files([tmp_path])
        assert len(result) == 3

    def test_collects_single_file(self, tmp_path):
        f = tmp_path / "single.txt"
        f.touch()
        result = collect_files([f])
        assert len(result) == 1
        assert result[0].name == "single.txt"

    def test_collects_nested_files(self, tmp_path):
        sub = tmp_path / "a" / "b"
        sub.mkdir(parents=True)
        (tmp_path / "root.txt").touch()
        (sub / "nested.txt").touch()
        result = collect_files([tmp_path])
        assert len(result) == 2

    def test_excludes_directories(self, tmp_path):
        (tmp_path / "subdir").mkdir()
        (tmp_path / "file.txt").touch()
        result = collect_files([tmp_path])
        assert len(result) == 1

    def test_empty_directory(self, tmp_path):
        result = collect_files([tmp_path])
        assert result == []

    def test_multiple_paths(self, tmp_path):
        d1 = tmp_path / "dir1"
        d2 = tmp_path / "dir2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a.txt").touch()
        (d2 / "b.txt").touch()
        result = collect_files([d1, d2])
        assert len(result) == 2

    def test_symlink_to_file(self, tmp_path):
        real = tmp_path / "real.txt"
        real.write_text("data")
        link = tmp_path / "link.txt"
        link.symlink_to(real)
        result = collect_files([tmp_path])
        assert len(result) == 2


class TestMd5Hashing:

    def test_buffered_md5(self, tmp_path):
        f = tmp_path / "test.bin"
        f.write_bytes(b"hello world")
        md5 = compute_md5_buffered(f)
        assert md5 == "5eb63bbbe01eeed093cb22bb8f5acdc3"

    def test_mmap_md5_matches_buffered(self, tmp_path):
        f = tmp_path / "test.bin"
        f.write_bytes(b"hello world")
        assert compute_md5_mmap(f) == compute_md5_buffered(f)

    def test_choose_hash_strategy_small_file_uses_buffered(self, tmp_path):
        f = tmp_path / "small.bin"
        f.write_bytes(b"x" * 100)
        path, md5 = choose_hash_strategy(f, mmap_threshold_bytes=1024)
        assert path == f
        assert isinstance(md5, str)
        assert len(md5) == 32

    def test_choose_hash_strategy_returns_error_on_missing_file(self, tmp_path):
        f = tmp_path / "missing.bin"
        path, result = choose_hash_strategy(f)
        assert path == f
        assert result.startswith("ERROR:")

    def test_hash_all_multiple_files(self, tmp_path):
        files = []
        for i in range(5):
            f = tmp_path / f"file_{i}.txt"
            f.write_text(f"content_{i}")
            files.append(f)
        result = hash_all(files, workers=2)
        assert len(result) == 5
        assert all(len(v) == 32 for v in result.values())

    def test_hash_all_empty_list(self):
        result = hash_all([], workers=1)
        assert result == {}


class TestFileSizesInBytes:

    def test_single_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")  # 5 bytes
        result = file_sizes_in_bytes([f])
        assert result[f] == 5

    def test_directory_recursive(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (tmp_path / "a.txt").write_text("aaa")  # 3 bytes
        (sub / "b.txt").write_text("bb")  # 2 bytes
        result = file_sizes_in_bytes([tmp_path])
        assert len(result) == 2
        sizes = sorted(result.values())
        assert sizes == [2, 3]

    def test_symlink_followed_by_default(self, tmp_path):
        real = tmp_path / "real.txt"
        real.write_text("data")  # 4 bytes
        link = tmp_path / "link.txt"
        link.symlink_to(real)
        result = file_sizes_in_bytes([link])
        assert result[link] == 4

    def test_symlink_skipped_when_disabled(self, tmp_path):
        real = tmp_path / "real.txt"
        real.write_text("data")
        link = tmp_path / "link.txt"
        link.symlink_to(real)
        result = file_sizes_in_bytes([link], follow_symlinks=False)
        assert len(result) == 0

    def test_empty_list(self):
        result = file_sizes_in_bytes([])
        assert result == {}


class TestTimestampHelpers:

    def test_parse_iso_with_microseconds_z(self):
        dt = _parse_job_timestamp("2025-03-15T10:30:45.123456Z")
        assert isinstance(dt, datetime)
        assert dt.year == 2025
        assert dt.month == 3
        assert dt.hour == 10

    def test_parse_iso_without_microseconds(self):
        dt = _parse_job_timestamp("2025-03-15T10:30:45Z")
        assert isinstance(dt, datetime)

    def test_parse_iso_no_z(self):
        dt = _parse_job_timestamp("2025-03-15T10:30:45.123456")
        assert isinstance(dt, datetime)

    def test_parse_empty_returns_none(self):
        assert _parse_job_timestamp("") is None
        assert _parse_job_timestamp(None) is None

    def test_parse_garbage_returns_none(self):
        assert _parse_job_timestamp("not-a-date") is None

    def test_fmt_timestamp(self):
        result = _fmt_job_timestamp("2025-03-15T10:30:45Z")
        assert result == "Mar 15 2025, 10:30"

    def test_fmt_empty_returns_input(self):
        assert _fmt_job_timestamp("") == ""

    def test_fmt_garbage_returns_input(self):
        assert _fmt_job_timestamp("not-a-date") == "not-a-date"
