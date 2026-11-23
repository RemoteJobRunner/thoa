import pytest


@pytest.fixture(autouse=True)
def _isolate_hash_cache(tmp_path, monkeypatch):
    """Keep unit tests from reading or writing the real ~/.thoa/hash_cache.json."""
    import thoa.core.hash_cache as hc

    monkeypatch.setattr(hc, "CACHE_FILE", tmp_path / "thoa_home" / "hash_cache.json")
