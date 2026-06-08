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
    every poll re-running the DISTINCT scan is wasted work.  Method must
    cache at the analytics TTL."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[{"expiration": date(2026, 6, 8)}])
    _install_conn(db, conn)
    asyncio.run(db.get_gex_expirations("SPY"))
    asyncio.run(db.get_gex_expirations("SPY"))
    assert len(conn.queries) == 1  # second call hit the cache


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
