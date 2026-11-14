"""Unit tests for thoa.core.hash_cache."""

import json
import os
import time
from unittest.mock import MagicMock

import pytest

import thoa.core.hash_cache as hc
from thoa.core.job_utils import hash_all


DAY = 24 * 60 * 60


def _make_file(path, content=b"hello world", age_seconds=60):
    path.write_bytes(content)
    old = time.time() - age_seconds
    os.utime(path, (old, old))
    return path


def _counting_hash_fn():
    calls = []

    def fn(files, on_progress=None):
        calls.append(list(files))
        return hash_all(files, workers=1, on_progress=on_progress)

    return fn, calls


@pytest.fixture
def cache_path(tmp_path):
    return tmp_path / "cache" / "hash_cache.json"


def _run(files, cache_path, use_cache=True, on_progress=None):
    fn, calls = _counting_hash_fn()
    result = hc.hash_files_cached(
        files, hash_fn=fn, use_cache=use_cache, on_progress=on_progress,
        cache=hc.HashCache(cache_path),
    )
    return result, calls


class TestHashFilesCached:
    def test_second_run_uses_cache(self, tmp_path, cache_path):
        f = _make_file(tmp_path / "a.txt")
        first, calls1 = _run([f], cache_path)
        second, calls2 = _run([f], cache_path)
        assert calls1 == [[f]]
        assert calls2 == []
        assert first == second == {f: hash_all([f], workers=1)[f]}

    def test_default_cache_location(self, tmp_path):
        f = _make_file(tmp_path / "a.txt")
        hc.hash_files_cached([f], hash_fn=lambda fs, on_progress=None: hash_all(fs, workers=1))
        assert hc.CACHE_FILE.exists()
        assert str(f) in json.loads(hc.CACHE_FILE.read_text())["entries"]

    def test_size_change_rehashes(self, tmp_path, cache_path):
        f = _make_file(tmp_path / "a.txt", b"hello")
        _run([f], cache_path)
        _make_file(f, b"hello world, longer")
        result, calls = _run([f], cache_path)
        assert calls == [[f]]
        assert result[f] == hash_all([f], workers=1)[f]

    def test_same_size_mtime_change_rehashes(self, tmp_path, cache_path):
        f = _make_file(tmp_path / "a.txt", b"aaaa", age_seconds=120)
        _run([f], cache_path)
        _make_file(f, b"bbbb", age_seconds=60)
        result, calls = _run([f], cache_path)
        assert calls == [[f]]
        assert result[f] == hash_all([f], workers=1)[f]

    def test_replaced_file_rehashes_even_with_same_size_and_mtime(self, tmp_path, cache_path):
        f = _make_file(tmp_path / "a.txt", b"aaaa", age_seconds=60)
        st = f.stat()
        _run([f], cache_path)
        other = tmp_path / "other.txt"
        other.write_bytes(b"bbbb")
        os.utime(other, ns=(st.st_atime_ns, st.st_mtime_ns))
        os.replace(other, f)  # new inode, same size and mtime
        assert f.stat().st_ino != st.st_ino
        result, calls = _run([f], cache_path)
        assert calls == [[f]]
        assert result[f] == hash_all([f], workers=1)[f]

    def test_no_cache_skips_lookup_but_writes_back(self, tmp_path, cache_path):
        f = _make_file(tmp_path / "a.txt")
        _run([f], cache_path)
        _, calls = _run([f], cache_path, use_cache=False)
        assert calls == [[f]]
        _, calls = _run([f], cache_path)
        assert calls == []

    def test_recently_modified_file_not_cached(self, tmp_path, cache_path):
        f = _make_file(tmp_path / "a.txt", age_seconds=0)
        _run([f], cache_path)
        _, calls = _run([f], cache_path)
        assert calls == [[f]]

    def test_hash_errors_not_cached(self, tmp_path, cache_path):
        f = _make_file(tmp_path / "a.txt")
        fn = MagicMock(return_value={f: "ERROR: boom"})
        result = hc.hash_files_cached([f], hash_fn=fn, cache=hc.HashCache(cache_path))
        assert result == {f: "ERROR: boom"}
        assert not cache_path.exists()

    def test_missing_file_passed_to_hash_fn(self, tmp_path, cache_path):
        missing = tmp_path / "nope.txt"
        fn = MagicMock(return_value={missing: "ERROR: not found"})
        result = hc.hash_files_cached([missing], hash_fn=fn, cache=hc.HashCache(cache_path))
        fn.assert_called_once()
        assert result == {missing: "ERROR: not found"}

    def test_progress_called_once_per_file(self, tmp_path, cache_path):
        a = _make_file(tmp_path / "a.txt", b"a")
        b = _make_file(tmp_path / "b.txt", b"b")
        _run([a], cache_path)
        progress = MagicMock()
        _run([a, b], cache_path, on_progress=progress)
        assert progress.call_count == 2

    def test_mixed_hits_and_misses(self, tmp_path, cache_path):
        a = _make_file(tmp_path / "a.txt", b"a")
        b = _make_file(tmp_path / "b.txt", b"b")
        _run([a], cache_path)
        result, calls = _run([a, b], cache_path)
        assert calls == [[b]]
        assert result == hash_all([a, b], workers=1)


class TestHashCacheFile:
    def test_corrupt_cache_file_ignored(self, tmp_path, cache_path):
        cache_path.parent.mkdir(parents=True)
        cache_path.write_text("{not json")
        f = _make_file(tmp_path / "a.txt")
        result, calls = _run([f], cache_path)
        assert calls == [[f]]
        assert result[f] == hash_all([f], workers=1)[f]
        assert json.loads(cache_path.read_text())["version"] == hc.CACHE_VERSION

    def test_wrong_version_ignored(self, tmp_path, cache_path):
        cache_path.parent.mkdir(parents=True)
        cache_path.write_text(json.dumps({"version": 999, "entries": {"x": {}}}))
        assert hc.HashCache(cache_path).entries == {}

    @pytest.mark.skipif(os.name != "posix", reason="POSIX permissions")
    def test_cache_file_is_private(self, tmp_path, cache_path):
        f = _make_file(tmp_path / "a.txt")
        _run([f], cache_path)
        assert (cache_path.stat().st_mode & 0o777) == 0o600

    def test_prune_drops_stale_and_missing_entries(self, tmp_path, cache_path):
        a = _make_file(tmp_path / "a.txt", b"a")
        b = _make_file(tmp_path / "b.txt", b"b")
        c = _make_file(tmp_path / "c.txt", b"c")
        cache = hc.HashCache(cache_path)
        now = time.time()
        assert cache.put(a, a.stat(), "md5a", now=now)
        assert cache.put(b, b.stat(), "md5b", now=now)
        assert cache.put(c, c.stat(), "md5c", now=now)
        cache.entries[str(a)]["last_used"] = now - 31 * DAY
        c.unlink()
        cache.prune(now=now)
        assert set(cache.entries) == {str(b)}

    def test_get_refreshes_last_used(self, tmp_path, cache_path):
        a = _make_file(tmp_path / "a.txt", b"a")
        cache = hc.HashCache(cache_path)
        now = time.time()
        assert cache.put(a, a.stat(), "md5a", now=now)
        cache.entries[str(a)]["last_used"] = now - 29 * DAY
        assert cache.get(a, a.stat(), now=now) == "md5a"
        cache.prune(now=now + 2 * DAY)
        assert str(a) in cache.entries

    def test_save_merges_concurrent_writes(self, tmp_path, cache_path):
        a = _make_file(tmp_path / "a.txt", b"a")
        b = _make_file(tmp_path / "b.txt", b"b")
        c1 = hc.HashCache(cache_path)
        c2 = hc.HashCache(cache_path)
        c1.put(a, a.stat(), "md5a")
        c1.save()
        c2.put(b, b.stat(), "md5b")
        c2.save()
        assert set(hc.HashCache(cache_path).entries) == {str(a), str(b)}

    def test_save_ignores_unwritable_location(self, tmp_path):
        blocker = tmp_path / "blocker"
        blocker.write_text("not a dir")
        f = _make_file(tmp_path / "a.txt")
        cache = hc.HashCache(blocker / "hash_cache.json")
        cache.put(f, f.stat(), "md5")
        cache.save()  # must not raise
