"""`/api/max-pain/current` must cache on its own (longer) TTL.

The endpoint is a pure read of a daily OI snapshot that the off-process
scheduled job (src.tools.max_pain_refresh) rewrites at most once a day.
Sharing the 5 s ``ANALYTICS_CACHE_TTL_SECONDS`` forced a DB round-trip
~every 5 s for data that changes ~once a day.  These tests pin the
dedicated, longer TTL so a regression back to the shared 5 s TTL fails
loudly.
"""

import asyncio
from contextlib import asynccontextmanager

from src.api.database import DatabaseManager


def test_default_and_env_override_for_max_pain_current_ttl(monkeypatch):
    monkeypatch.delenv("MAX_PAIN_CURRENT_CACHE_TTL_SECONDS", raising=False)
    assert DatabaseManager()._max_pain_current_cache_ttl_seconds == 120.0

    monkeypatch.setenv("MAX_PAIN_CURRENT_CACHE_TTL_SECONDS", "45")
    assert DatabaseManager()._max_pain_current_cache_ttl_seconds == 45.0

    # The shared analytics TTL stays independent (unchanged at its default).
    monkeypatch.delenv("ANALYTICS_CACHE_TTL_SECONDS", raising=False)
    db = DatabaseManager()
    assert db._analytics_cache_ttl_seconds == 5.0
    assert db._max_pain_current_cache_ttl_seconds == 45.0


class _FakeConn:
    def __init__(self, fetchrow_values, fetch_value):
        # ``get_max_pain_current`` issues two fetchrow calls: snapshot
        # first, then the live overlay.  Pass each as an item in the
        # list.
        self._fetchrow_values = list(fetchrow_values)
        self._fetch_value = fetch_value
        self.fetchrow_calls = 0
        self.fetch_calls = 0

    async def fetchrow(self, *_a, **_k):
        idx = self.fetchrow_calls
        self.fetchrow_calls += 1
        if idx >= len(self._fetchrow_values):
            return None
        return self._fetchrow_values[idx]

    async def fetch(self, *_a, **_k):
        self.fetch_calls += 1
        return self._fetch_value


def test_get_max_pain_current_caches_with_dedicated_ttl(monkeypatch):
    db = DatabaseManager()

    snapshot_row = {
        "symbol": "SPY",
        "as_of_date": "2026-05-15",
        "source_timestamp": "2026-05-15T20:00:00+00:00",
        "underlying_price": 500.0,
        "max_pain": 505.0,
        "difference": 5.0,
    }
    live_row = {
        "live_timestamp": "2026-05-15T20:05:00+00:00",
        "live_max_pain": 506.0,
        "live_underlying_price": 501.0,
    }
    expiration_rows = [
        {
            "expiration": "2026-05-18",
            "max_pain": 505.0,
            "difference_from_underlying": 5.0,
            "strikes": [],
        }
    ]
    conn = _FakeConn([snapshot_row, live_row], expiration_rows)

    @asynccontextmanager
    async def _fake_acquire():
        yield conn

    monkeypatch.setattr(db, "_acquire_connection", _fake_acquire)

    captured_ttls = []
    real_cache_set = db._cache_set

    def _spy_cache_set(key, payload, ttl_seconds):
        if key == "max_pain_current:SPY":
            captured_ttls.append(ttl_seconds)
        return real_cache_set(key, payload, ttl_seconds)

    monkeypatch.setattr(db, "_cache_set", _spy_cache_set)

    async def _run():
        first = await db.get_max_pain_current("SPY", strike_limit=100)
        # Second call within the TTL must be served from the in-process
        # cache — no further DB work.
        second = await db.get_max_pain_current("SPY", strike_limit=100)
        return first, second

    first, second = asyncio.run(_run())

    assert first is not None
    assert second == first
    # Pure read: snapshot + live overlay each ran exactly once on the
    # first call; the second call hit the in-process cache (no extra
    # fetchrow/fetch, no recompute).
    assert conn.fetchrow_calls == 2
    assert conn.fetch_calls == 1
    # And it was cached on the dedicated max-pain TTL, not the 5 s
    # shared analytics TTL.
    assert captured_ttls == [db._max_pain_current_cache_ttl_seconds]
    assert db._max_pain_current_cache_ttl_seconds != db._analytics_cache_ttl_seconds
