"""Regression: presenting junk API keys can't grow the key cache without end.

``_KeyStore`` cached every key it looked up, misses included, in a plain dict
that nothing pruned. A loop of random ``Authorization: Bearer`` values, which
needs no account, added an entry per request (about 220 bytes each), so a few
addresses could push each API worker into the memory limit and get it
OOM-killed. The cache is now a capped LRU and misses expire after seconds.
"""

from __future__ import annotations

import asyncio

from src.api import security


class _Conn:
    def __init__(self, valid: dict[str, dict]):
        self._valid = valid
        self.lookups = 0

    async def fetchrow(self, sql, key_hash):
        self.lookups += 1
        return self._valid.get(key_hash)


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _Acquire(self._conn)


def _store(conn, monkeypatch, *, max_entries: int = 50, miss_ttl: float = 10.0):
    monkeypatch.setenv("API_KEY_CACHE_MAX_ENTRIES", str(max_entries))
    monkeypatch.setenv("API_KEY_MISS_CACHE_TTL_SECONDS", str(miss_ttl))
    store = security._KeyStore()
    pool = _Pool(conn)
    store.configure(lambda: pool)
    return store


def test_a_flood_of_bad_keys_stays_within_the_cap(monkeypatch):
    conn = _Conn({})
    store = _store(conn, monkeypatch, max_entries=50)

    async def flood():
        for i in range(5_000):
            assert await store.lookup(f"junk-{i}") is None

    asyncio.run(flood())
    assert len(store._cache) <= 50


def test_a_valid_key_still_resolves_and_is_cached(monkeypatch):
    good = security._hash_key("good-key")
    conn = _Conn({good: {"id": 1, "user_id": "u1", "name": "k", "scopes": []}})
    store = _store(conn, monkeypatch)

    async def run():
        first = await store.lookup("good-key")
        second = await store.lookup("good-key")
        return first, second

    first, second = asyncio.run(run())
    assert first is not None and first["user_id"] == "u1"
    assert second == first
    assert conn.lookups == 1, "the second lookup came from the cache"


def test_a_miss_expires_quickly(monkeypatch):
    conn = _Conn({})
    store = _store(conn, monkeypatch, miss_ttl=10.0)
    now = [1_000.0]
    monkeypatch.setattr(security._time, "monotonic", lambda: now[0])

    async def look():
        return await store.lookup("typo-key")

    asyncio.run(look())
    asyncio.run(look())
    assert conn.lookups == 1, "a repeated bad key is absorbed for a few seconds"
    now[0] += 11.0
    asyncio.run(look())
    assert conn.lookups == 2, "and then looked up again, not cached for the hit lifetime"
