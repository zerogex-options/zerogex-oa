"""`/api/flow/series` must cache on its own (longer) TTL.

get_flow_series's unfiltered read is a flow_series_5min snapshot the
Analytics Engine only rewrites ~once per cycle (~60s); the live tail is
polled via ``intervals=N`` which bypasses the cache. Sharing the 3 s
``FLOW_ENDPOINT_CACHE_TTL_SECONDS`` forced redundant snapshot reads and
gave no amortisation of the heavier strike/expiration-filtered CTE
(measured 6-26x the snapshot). These tests pin the dedicated, longer
TTL so a regression back to the shared flow TTL fails loudly.
``/api/flow/by-contract`` and ``/api/flow/contracts`` intentionally
stay on the shared knob.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from src.api.database import DatabaseManager


def test_default_and_env_override_for_flow_series_ttl(monkeypatch):
    monkeypatch.delenv("FLOW_SERIES_ENDPOINT_CACHE_TTL_SECONDS", raising=False)
    assert DatabaseManager()._flow_series_endpoint_cache_ttl_seconds == 30.0

    monkeypatch.setenv("FLOW_SERIES_ENDPOINT_CACHE_TTL_SECONDS", "12")
    assert DatabaseManager()._flow_series_endpoint_cache_ttl_seconds == 12.0


def test_flow_series_ttl_is_independent_of_the_shared_flow_ttl(monkeypatch):
    # The shared flow TTL (by-contract / contracts) stays at its 3 s
    # default and is unaffected by the dedicated series knob.
    monkeypatch.delenv("FLOW_ENDPOINT_CACHE_TTL_SECONDS", raising=False)
    monkeypatch.setenv("FLOW_SERIES_ENDPOINT_CACHE_TTL_SECONDS", "45")
    db = DatabaseManager()
    assert db._flow_endpoint_cache_ttl_seconds == 3.0
    assert db._flow_series_endpoint_cache_ttl_seconds == 45.0


def test_get_flow_series_caches_with_the_dedicated_ttl(monkeypatch):
    # Wiring guard: get_flow_series must hand the dedicated series TTL to
    # _cache_set, not the shared flow TTL. Exercises the no-session-data
    # path so no DB/snapshot stubbing is needed.
    db = DatabaseManager()

    class _Conn:
        pass

    @asynccontextmanager
    async def _acquire():
        yield _Conn()

    async def _noop_refresh(*_a, **_k):
        return None

    now = datetime.now(timezone.utc)

    async def _resolve(*_a, **_k):
        return (now, now, False)

    captured = {}

    def _capture(_key, _payload, ttl):
        captured["ttl"] = ttl

    monkeypatch.setattr(db, "_acquire_connection", _acquire)
    monkeypatch.setattr(db, "_refresh_flow_cache", _noop_refresh)
    monkeypatch.setattr(db, "_resolve_flow_series_session", _resolve)
    monkeypatch.setattr(db, "_cache_set", _capture)

    out = asyncio.run(db.get_flow_series(symbol="SPY", session="current"))

    assert out == []
    assert captured["ttl"] == db._flow_series_endpoint_cache_ttl_seconds == 30.0
    assert captured["ttl"] != db._flow_endpoint_cache_ttl_seconds
