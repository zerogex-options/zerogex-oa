"""Tests for picking a past session on the flow endpoints.

Flow Analysis and Smart Money offer the last several trading days, not just
``current`` and ``prior``. That rests on two pieces covered here:

* ``/api/flow/sessions`` lists the recent days that have flow, newest first.
* ``date=YYYY-MM-DD`` on ``/api/flow/series``, ``/api/flow/contracts`` and
  ``/api/flow/smart-money`` reads that day's 09:30-16:15 ET session.

The SQL runs only against a real Postgres; these drive canned connections
and AsyncMock'd DB methods to pin windows, cache keys and the HTTP surface.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

UTC = timezone.utc
# A Wednesday, well in the past, so no window is capped by now().
PAST_DAY = date(2026, 4, 22)
# 09:30 / 16:15 ET on PAST_DAY (EDT) in UTC.
PAST_OPEN_UTC = datetime(2026, 4, 22, 13, 30, tzinfo=UTC)
PAST_CLOSE_UTC = datetime(2026, 4, 22, 20, 15, tzinfo=UTC)


class _Conn:
    def __init__(
        self,
        *,
        fetchvals: Optional[List[Any]] = None,
        fetch_rows: Optional[List[Dict[str, Any]]] = None,
        fetchrow_value: Optional[Dict[str, Any]] = None,
    ):
        self._fetchvals = list(fetchvals or [])
        self._fetch_rows = fetch_rows or []
        self._fetchrow_value = fetchrow_value
        self.fetch_calls: List[tuple] = []
        self.fetchval_calls: List[tuple] = []
        self.fetchrow_calls: List[tuple] = []

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        return self._fetchvals.pop(0) if self._fetchvals else None

    async def fetch(self, query, *args):
        self.fetch_calls.append((query, args))
        return self._fetch_rows

    async def fetchrow(self, query, *args):
        self.fetchrow_calls.append((query, args))
        return self._fetchrow_value


def _make_db(conn: _Conn):
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    async def _noop(*_args, **_kwargs):
        return None

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    db._refresh_flow_cache = _noop  # type: ignore[method-assign]
    return db


# ---------------------------------------------------------------------------
# DB layer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("use_snapshot", [True, False])
def test_dated_series_reads_that_days_session(use_snapshot: bool):
    conn = _Conn(fetchvals=[1])  # symbols table: known
    db = _make_db(conn)
    db._flow_series_use_snapshot = use_snapshot

    asyncio.run(db.get_flow_series(symbol="SPY", session_date=PAST_DAY))

    _, args = conn.fetch_calls[0]
    assert args[0] == "SPY"
    assert args[1] == PAST_OPEN_UTC
    assert args[2] == PAST_CLOSE_UTC
    # The day is resolved arithmetically: nobody asks flow_by_contract for
    # its newest date.
    assert not any("MAX(timestamp)" in q for q, _ in conn.fetchval_calls)


def test_dated_series_is_cached_apart_from_current():
    conn = _Conn(fetchvals=[1, date(2026, 4, 24), 1])
    db = _make_db(conn)
    db._flow_series_endpoint_cache_ttl_seconds = 60.0

    asyncio.run(db.get_flow_series(symbol="SPY", session="current"))
    asyncio.run(db.get_flow_series(symbol="SPY", session="current", session_date=PAST_DAY))

    # Same `session`, different day: the second read must reach the DB rather
    # than be handed the current session out of the cache.
    assert len(conn.fetch_calls) == 2
    assert conn.fetch_calls[1][1][1] == PAST_OPEN_UTC


def test_dated_1min_series_uses_the_minute_grid_window():
    conn = _Conn(fetchvals=[1])
    db = _make_db(conn)

    asyncio.run(db.get_flow_series(symbol="SPY", session_date=PAST_DAY, timeframe="1min"))

    _, args = conn.fetch_calls[0]
    assert (args[1], args[2]) == (PAST_OPEN_UTC, PAST_CLOSE_UTC)


def test_dated_window_on_a_minute_grid_floors_now_to_the_minute():
    from src.api.database import DatabaseManager

    today = datetime.now(UTC).date()
    _, end_5 = DatabaseManager._session_window_for_date(today)
    _, end_1 = DatabaseManager._session_window_for_date(today, bar_seconds=60)
    assert end_5.timestamp() % 300 == 0
    assert end_1.timestamp() % 60 == 0
    assert end_1 >= end_5


def test_dated_contracts_read_that_days_session():
    conn = _Conn(fetchvals=[1], fetchrow_value={"strikes": [700], "expirations": [PAST_DAY]})
    db = _make_db(conn)

    payload = asyncio.run(db.get_flow_contracts(symbol="SPY", session_date=PAST_DAY))

    assert payload == {"strikes": [700.0], "expirations": ["2026-04-22"]}
    _, args = conn.fetchrow_calls[0]
    assert (args[1], args[2]) == (PAST_OPEN_UTC, PAST_CLOSE_UTC)


def test_dated_smart_money_reads_that_days_session():
    conn = _Conn()
    db = _make_db(conn)

    asyncio.run(db.get_smart_money_flow(symbol="SPY", limit=50, session_date=PAST_DAY))

    query, args = conn.fetch_calls[0]
    assert args[0] == "SPY"
    assert args[1] == PAST_OPEN_UTC
    assert args[2] == PAST_CLOSE_UTC
    # DTE is counted from the print's own session day, so a past session's
    # 0DTE block still reads 0 instead of going negative.
    assert "CURRENT_DATE" not in query
    assert "(expiration - (timestamp AT TIME ZONE 'America/New_York')::date)::int AS dte" in query


def test_flow_sessions_lists_days_newest_first():
    days = [date(2026, 4, 24), date(2026, 4, 23), date(2026, 4, 22)]
    conn = _Conn(fetch_rows=[{"session_date": d} for d in days])
    db = _make_db(conn)

    assert asyncio.run(db.get_flow_sessions(symbol="spy", limit=3)) == days
    query, args = conn.fetch_calls[0]
    assert args == ("SPY", 3)
    # Each step is bounded on the raw timestamp so it stays an index probe.
    flat = " ".join(query.split())
    assert "f.timestamp < (days.session_date::timestamp AT TIME ZONE 'America/New_York')" in flat


def test_flow_sessions_unknown_symbol_is_none():
    db = _make_db(_Conn(fetch_rows=[]))
    assert asyncio.run(db.get_flow_sessions(symbol="NOPE")) is None


# ---------------------------------------------------------------------------
# HTTP surface
# ---------------------------------------------------------------------------


def _build_app(monkeypatch: pytest.MonkeyPatch):
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

    from src.api import main as mainmod

    return mainmod.app, dbmod


def test_http_sessions_shape(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    mock = AsyncMock(return_value=[date(2026, 4, 24), date(2026, 4, 23)])
    dbmod.DatabaseManager.get_flow_sessions = mock  # type: ignore[method-assign]

    with TestClient(app) as client:
        response = client.get("/api/flow/sessions?symbol=spy")

    assert response.status_code == 200, response.text
    assert response.json() == {
        "symbol": "SPY",
        "count": 2,
        "sessions": [{"date": "2026-04-24"}, {"date": "2026-04-23"}],
    }
    assert mock.await_args.kwargs == {"symbol": "SPY", "limit": 7}


def test_http_sessions_unknown_symbol_404(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    mock = AsyncMock(return_value=None)
    dbmod.DatabaseManager.get_flow_sessions = mock  # type: ignore[method-assign]

    with TestClient(app) as client:
        assert client.get("/api/flow/sessions?symbol=NOPE").status_code == 404
        assert client.get("/api/flow/sessions?symbol=SPY&limit=61").status_code == 422


@pytest.mark.parametrize(
    "path, method",
    [
        ("/api/flow/series?symbol=SPY", "get_flow_series"),
        ("/api/flow/contracts?symbol=SPY", "get_flow_contracts"),
        ("/api/flow/smart-money?symbol=SPY", "get_smart_money_flow"),
    ],
)
def test_http_date_reaches_the_db_and_bad_dates_400(
    monkeypatch: pytest.MonkeyPatch, path: str, method: str
):
    app, dbmod = _build_app(monkeypatch)
    returns: Any = {"strikes": [], "expirations": []} if method == "get_flow_contracts" else []
    mock = AsyncMock(return_value=returns)
    setattr(dbmod.DatabaseManager, method, mock)

    with TestClient(app) as client:
        response = client.get(f"{path}&date=2026-04-22")
        assert response.status_code == 200, response.text
        assert mock.await_args.kwargs["session_date"] == PAST_DAY

        assert client.get(f"{path}&date=2026-4-22").status_code == 400
        assert client.get(f"{path}&date=2026-02-30").status_code == 400
