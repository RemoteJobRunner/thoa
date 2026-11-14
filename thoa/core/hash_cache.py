"""Local cache of file MD5 hashes, stored in ~/.thoa/hash_cache.json.

Hashing large inputs can take minutes, so hashes are reused across runs as
long as the file hasn't changed since it was hashed. A cached hash is only
used when the file's size, mtime (ns) and inode all still match the values
recorded at hashing time.

Hashes of files modified within RACY_WINDOW_SECONDS of being hashed are not
cached: on filesystems with coarse mtime resolution, a second edit in the
same tick could otherwise leave the mtime unchanged.

Entries that haven't been used for PRUNE_AFTER_SECONDS, or whose file no
longer exists, are dropped when the cache is saved.
"""

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional

from thoa.core.credentials import _THOA_DIR

CACHE_FILE = _THOA_DIR / "hash_cache.json"
CACHE_VERSION = 1
RACY_WINDOW_SECONDS = 2
PRUNE_AFTER_SECONDS = 30 * 24 * 60 * 60


def _fingerprint(st: os.stat_result) -> dict:
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "ino": st.st_ino}


def _key(path) -> str:
    return os.path.abspath(str(path))


class HashCache:
    def __init__(self, path: Optional[Path] = None):
        self.path = Path(path) if path is not None else CACHE_FILE
        self.entries: Dict[str, dict] = self._read()
        self._dirty = False

    def _read(self) -> Dict[str, dict]:
        try:
            with open(self.path) as f:
                data = json.load(f)
        except (OSError, ValueError):
            return {}
        if not isinstance(data, dict) or data.get("version") != CACHE_VERSION:
            return {}
        entries = data.get("entries")
        if not isinstance(entries, dict):
            return {}
        return {k: v for k, v in entries.items() if isinstance(v, dict)}

    def get(self, path, st: os.stat_result, now: Optional[float] = None) -> Optional[str]:
        """Return the cached md5 if the file is unchanged since it was cached."""
        entry = self.entries.get(_key(path))
        if not entry or not isinstance(entry.get("md5"), str):
            return None
        fp = _fingerprint(st)
        if any(entry.get(k) != v for k, v in fp.items()):
            return None
        entry["last_used"] = now if now is not None else time.time()
        self._dirty = True
        return entry["md5"]

    def put(self, path, st: os.stat_result, md5: str, now: Optional[float] = None) -> bool:
        """Cache md5 for a file whose stat was taken before hashing.

        Returns False (and caches nothing) if the file was modified too
        recently to trust its mtime.
        """
        now = now if now is not None else time.time()
        if now - st.st_mtime_ns / 1e9 < RACY_WINDOW_SECONDS:
            return False
        self.entries[_key(path)] = {
            "md5": md5,
            **_fingerprint(st),
            "cached_at": now,
            "last_used": now,
        }
        self._dirty = True
        return True

    def prune(self, now: Optional[float] = None) -> None:
        now = now if now is not None else time.time()
        keep = {}
        for k, entry in self.entries.items():
            last_used = entry.get("last_used")
            if not isinstance(last_used, (int, float)) or now - last_used > PRUNE_AFTER_SECONDS:
                continue
            if not os.path.exists(k):
                continue
            keep[k] = entry
        if len(keep) != len(self.entries):
            self._dirty = True
        self.entries = keep

    def save(self) -> None:
        """Write the cache atomically. Errors are ignored: the cache is optional."""
        if not self._dirty:
            return
        try:
            # Merge with entries written by a concurrent run since we loaded.
            merged = self._read()
            merged.update(self.entries)
            self.entries = merged
            self.prune()
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".hash_cache.", suffix=".tmp")
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump({"version": CACHE_VERSION, "entries": self.entries}, f)
                os.chmod(tmp, 0o600)
                os.replace(tmp, self.path)
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
            self._dirty = False
        except OSError:
            pass


def hash_files_cached(
    files: Iterable,
    hash_fn: Callable,
    use_cache: bool = True,
    on_progress: Optional[Callable[[], None]] = None,
    cache: Optional[HashCache] = None,
) -> dict:
    """Hash files, reusing cached hashes for files unchanged since they were cached.

    hash_fn(files, on_progress=...) -> {path: md5} computes hashes for misses.
    With use_cache=False, cache lookups are skipped but fresh hashes are
    still written back.
    """
    files = list(files)
    cache = cache if cache is not None else HashCache()
    results: dict = {}
    stats: dict = {}
    misses = []

    for f in files:
        try:
            st = os.stat(f)
        except OSError:
            misses.append(f)
            continue
        stats[f] = st
        cached = cache.get(f, st) if use_cache else None
        if cached is not None:
            results[f] = cached
            if on_progress:
                on_progress()
        else:
            misses.append(f)

    fresh = hash_fn(misses, on_progress=on_progress) if misses else {}
    results.update(fresh)

    for f, digest in fresh.items():
        before = stats.get(f)
        if before is None or not isinstance(digest, str) or digest.startswith("ERROR"):
            continue
        try:
            after = os.stat(f)
        except OSError:
            continue
        # Skip files that changed while being hashed.
        if _fingerprint(after) != _fingerprint(before):
            continue
        cache.put(f, before, digest)

    cache.save()
    return results
