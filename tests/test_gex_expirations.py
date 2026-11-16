"""Regression: /api/gex/expirations must read from a trailing 24h window
on gex_by_strike — NOT just the latest snapshot.

The Strike-Profile chart's expiry dropdown was previously sourced from
``/api/gex/by-strike`` (a latest-snapshot endpoint).  After 4 PM ET the
analytics engine stops writing rows for today's now-expired contracts,
so today's expiration vanishes from the latest snapshot — and the
dropdown — even though the rewind feature still has access to data
from when those contracts were live.  This endpoint widens the lookup
so today's date stays available for the rest of the trading shift.

These tests pin:
  * the DB method scans gex_by_strike, NOT just the latest snapshot;
  * the trailing window is parameterised (so the chart can ask for a
    different lookback if needed) and bounded;
  * the response is sorted ascending so the dropdown renders in order;
  * /api/gex/expirations rejects out-of-range lookback values.
"""

from __future__ import annotations

import asyncio
import sys
import time
from contextlib import asynccontextmanager
from datetime import date
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.api.database import DatabaseManager


class _RecordingConn:
    def __init__(self, fetch_rows=None):
        self._fetch_rows = fetch_rows or []
        self.queries = []
        self.args = []

    async def fetch(self, query, *args, timeout=None):
        self.queries.append(query)
        self.args.append(args)
        return list(self._fetch_rows)


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# DB-layer query shape
# ---------------------------------------------------------------------------


def test_query_scans_trailing_window_not_latest_snapshot():
    """The whole point of the new endpoint: query reads a TRAILING window
    of gex_by_strike rows, NOT (timestamp = latest snapshot).  A snapshot-
    based read drops today's expiration the moment the analytics engine
    stops writing rows for it post-close — exactly the bug this endpoint
    fixes.  Pin the window-based query shape."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_expirations("SPY"))

    sql = conn.queries[0]
    assert "FROM gex_by_strike" in sql
    assert "DISTINCT expiration" in sql
    # The trailing window is parameterised — neither a latest-snapshot
    # nested SELECT nor a hard-coded interval.
    assert "ORDER BY timestamp DESC LIMIT 1" not in sql
    assert "make_interval(hours => $2)" in sql


def test_default_lookback_hours_is_24():
    """Default lookback covers a full post-close shift (4 PM → next day's
    pre-market).  Tighter than that risks dropping today's expiration in
    the late-evening browse window the user reported the bug on."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_expirations("SPY"))
    args = conn.args[0]
    assert args[1] == 24


def test_response_returns_distinct_expirations_in_order():
    """The dropdown needs ASC-sorted dates; the DB ORDER BY handles that,
    so the method must NOT re-sort or de-duplicate in Python (which would
    silently mask a missing ORDER BY in the SQL)."""
    rows = [
        {"expiration": date(2026, 6, 8)},
        {"expiration": date(2026, 6, 9)},
        {"expiration": date(2026, 6, 20)},
    ]
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=rows)
    _install_conn(db, conn)
    result = asyncio.run(db.get_gex_expirations("SPY"))
    assert result == [date(2026, 6, 8), date(2026, 6, 9), date(2026, 6, 20)]


def test_response_cached():
    """The expirations universe changes at most once per trading day, so
    every poll re-running the DISTINCT scan is wasted work."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[{"expiration": date(2026, 6, 8)}])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_expirations("SPY"))
    asyncio.run(db.get_gex_expirations("SPY"))
    assert len(conn.queries) == 1  # second call hit the cache


def test_cache_outlives_the_analytics_ttl(monkeypatch: pytest.MonkeyPatch):
    """At the 5s analytics TTL this read was the database's second-heaviest
    on 2026-11-16 (a week of gex_by_strike per miss).  It keeps its entry
    for GEX_EXPIRATIONS_CACHE_TTL_SECONDS, five minutes by default."""
    monkeypatch.delenv("GEX_EXPIRATIONS_CACHE_TTL_SECONDS", raising=False)
    monkeypatch.delenv("ANALYTICS_CACHE_TTL_SECONDS", raising=False)
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[{"expiration": date(2026, 6, 8)}])
    _install_conn(db, conn)
    before = time.monotonic()
    asyncio.run(db.get_gex_expirations("SPY", 168))
    expires_at, _payload, _units = db._read_cache["gex_expirations:SPY:168"]
    assert expires_at - before >= 300
    assert db._gex_expirations_cache_ttl_seconds > db._analytics_cache_ttl_seconds


def test_cache_ttl_is_configurable_and_zero_disables(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GEX_EXPIRATIONS_CACHE_TTL_SECONDS", "0")
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[{"expiration": date(2026, 6, 8)}])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_expirations("SPY"))
    asyncio.run(db.get_gex_expirations("SPY"))
    assert len(conn.queries) == 2


def test_concurrent_misses_share_one_read():
    """On a loaded database the scan outlasted the old TTL, so every poll
    that landed during it ran its own copy.  Callers that miss together on
    one key wait for a single read and all get its result."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[{"expiration": date(2026, 6, 8)}])
    _install_conn(db, conn)

    async def _three_at_once():
        return await asyncio.gather(
            db.get_gex_expirations("SPY", 168),
            db.get_gex_expirations("SPY", 168),
            db.get_gex_expirations("SPY", 168),
        )

    results = asyncio.run(_three_at_once())
    assert len(conn.queries) == 1
    assert results == [[date(2026, 6, 8)]] * 3


def test_failed_read_is_not_cached_and_reaches_every_caller():
    """A failed read raises for the callers that shared it and leaves no
    entry behind, so the next poll tries again instead of serving nothing
    for five minutes."""

    class _FailingConn(_RecordingConn):
        async def fetch(self, query, *args, timeout=None):
            self.queries.append(query)
            raise RuntimeError("statement timeout")

    db = DatabaseManager()
    conn = _FailingConn()
    _install_conn(db, conn)

    async def _two_at_once():
        return await asyncio.gather(
            db.get_gex_expirations("SPY"),
            db.get_gex_expirations("SPY"),
            return_exceptions=True,
        )

    results = asyncio.run(_two_at_once())
    assert len(conn.queries) == 1
    assert all(isinstance(r, RuntimeError) for r in results)
    assert "gex_expirations:SPY:24" not in db._read_cache
    with pytest.raises(RuntimeError):
        asyncio.run(db.get_gex_expirations("SPY"))
    assert len(conn.queries) == 2


# ---------------------------------------------------------------------------
# Endpoint-layer validation
# ---------------------------------------------------------------------------


def _build_app(monkeypatch: pytest.MonkeyPatch, returns):
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)
    dbmod.DatabaseManager.get_gex_expirations = AsyncMock(return_value=returns)

    from src.api.main import app

    return TestClient(app)


def test_endpoint_returns_dates_as_iso_strings(monkeypatch: pytest.MonkeyPatch):
    client = _build_app(monkeypatch, returns=[date(2026, 6, 8), date(2026, 6, 20)])
    with client:
        response = client.get("/api/gex/expirations?symbol=SPY")
    assert response.status_code == 200
    assert response.json() == ["2026-06-08", "2026-06-20"]


def test_endpoint_empty_returns_200_with_empty_list(monkeypatch: pytest.MonkeyPatch):
    """Matches the list-endpoint convention (W4.4): 200 + [] when there's
    no data, NOT 404.  Frontend treats no-data the same as no-symbol so
    a fresh deployment doesn't 404 the rewind chart."""
    client = _build_app(monkeypatch, returns=[])
    with client:
        response = client.get("/api/gex/expirations?symbol=SPY")
    assert response.status_code == 200
    assert response.json() == []


def test_endpoint_rejects_lookback_out_of_range(monkeypatch: pytest.MonkeyPatch):
    """1 ≤ lookback_hours ≤ 168 (1 week).  Anything outside bound the
    DISTINCT scan; FastAPI 422s on either end."""
    client = _build_app(monkeypatch, returns=[])
    with client:
        response = client.get("/api/gex/expirations?symbol=SPY&lookback_hours=999")
    assert response.status_code == 422
