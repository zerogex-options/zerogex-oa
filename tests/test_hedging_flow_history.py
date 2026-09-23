"""Dated Hedging Flow reads: ?date=, the snapshot path, and the session list.

What these pin, and why each one is worth a test
------------------------------------------------
The feature is "a permalink for a past session", and every way it can fail is
quiet. A dated read that silently resolves to today serves the wrong chart
under the right URL. A dated read that reaches for ``flow_contract_facts``
works for 90 days and then returns an empty session that looks exactly like a
quiet day. A dated page that 404s on a blank response costs the permalink its
place in the search index. None of those throw.

The SQL itself is exercised against a real Postgres in
``tests/test_hedging_flow_snapshot_sql.py``; this file drives the connection
with canned rows to pin routing, parameter plumbing and the response contract.

Harness mirrors tests/test_api_hedging_flow.py so the two read side by side.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.hedging_flow_sql import SCOPE_0DTE, SCOPE_ALL

UTC = timezone.utc
SESSION_DATE = date(2026, 6, 12)
SESSION_START_UTC = datetime(2026, 6, 12, 13, 30, tzinfo=UTC)


def _bar_ts(minute_offset: int) -> datetime:
    return SESSION_START_UTC + timedelta(minutes=minute_offset)


def _row(minute: int, *, cum_net: float = 0.0, synthetic: bool = False) -> Dict[str, Any]:
    return {
        "bar_start": _bar_ts(minute),
        "call_flow_usd": 1000.0,
        "put_flow_usd": -400.0,
        "net_flow_usd": 600.0,
        "cum_call_usd": 1000.0,
        "cum_put_usd": -400.0,
        "cum_net_usd": cum_net,
        "classified_ratio": 0.8,
        "underlying_price": 604.5,
        "contract_count": 7,
        "is_synthetic": synthetic,
    }


class _CannedConn:
    """Fake asyncpg connection returning scripted responses."""

    def __init__(
        self,
        *,
        fetchval_sequence: Optional[List[Any]] = None,
        fetch_rows: Optional[List[Dict[str, Any]]] = None,
    ):
        self._fetchvals = list(fetchval_sequence or [])
        self._fetch_rows = fetch_rows or []
        self.fetch_calls: List[tuple] = []
        self.fetchval_queries: List[str] = []

    async def fetchval(self, query, *args):
        self.fetchval_queries.append(query)
        if self._fetchvals:
            return self._fetchvals.pop(0)
        return None

    async def fetch(self, query, *args):
        self.fetch_calls.append((query, args))
        return self._fetch_rows

    async def fetchrow(self, query, *args):
        return None

    def transaction(self):
        @asynccontextmanager
        async def _cm():
            yield

        return _cm()


def _make_db(conn: _CannedConn):
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    async def _noop(*_args, **_kwargs):
        return None

    db._refresh_flow_cache = _noop  # type: ignore[method-assign]
    db._flow_series_endpoint_cache_ttl_seconds = 0.0
    return db


# --------------------------------------------------------------------------- #
# Session resolution
# --------------------------------------------------------------------------- #
def test_dated_read_does_not_ask_the_pruned_table_whether_the_day_existed():
    """The whole point of the snapshot is that the source rows are gone.

    ``_resolve_flow_series_session`` normally probes ``flow_by_contract`` to
    decide whether a session has data. For a dated read that probe would
    report "no such session" for every day older than DATA_RETENTION_DAYS —
    which is exactly the set of days a permalink exists for. So the dated
    branch validates the SYMBOL and resolves the window arithmetically.
    """
    conn = _CannedConn(fetchval_sequence=[1], fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(db.get_hedging_flow_series(symbol="SPY", session_date=SESSION_DATE))

    assert len(conn.fetchval_queries) == 1
    assert "FROM symbols" in conn.fetchval_queries[0]
    assert not any("MAX(timestamp)" in q for q in conn.fetchval_queries)


def test_dated_window_is_the_session_not_today():
    conn = _CannedConn(fetchval_sequence=[1], fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(db.get_hedging_flow_series(symbol="SPY", session_date=SESSION_DATE))

    _query, args = conn.fetch_calls[0]
    start, end = args[2], args[3]
    assert start == SESSION_START_UTC
    # 09:30 -> 16:15 ET is 6h45m.
    assert end - start == timedelta(hours=6, minutes=45)


def test_unknown_symbol_still_404s_on_a_dated_read():
    conn = _CannedConn(fetchval_sequence=[None, None])
    db = _make_db(conn)

    assert asyncio.run(db.get_hedging_flow_series(symbol="NOPE", session_date=SESSION_DATE)) is None
    assert conn.fetch_calls == []


def test_a_stored_day_with_no_rows_is_empty_not_missing():
    """Empty, never None — None is what the caller turns into a 404.

    A dated page that 404s because a session was quiet (or because the API
    blinked mid-crawl) loses the URL its place in the index, which is the
    failure ``tests/datedPermalinks.test.ts`` on the web side exists to
    prevent. The two halves have to agree.
    """
    conn = _CannedConn(fetchval_sequence=[1], fetch_rows=[])
    db = _make_db(conn)

    assert asyncio.run(db.get_hedging_flow_series(symbol="SPY", session_date=SESSION_DATE)) == []


# --------------------------------------------------------------------------- #
# Which read path runs
# --------------------------------------------------------------------------- #
def test_live_read_still_runs_the_cte():
    """No behaviour change for the live page: it is the fresher source."""
    conn = _CannedConn(fetchval_sequence=[1, SESSION_DATE], fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(db.get_hedging_flow_series(symbol="SPY"))

    query, _args = conn.fetch_calls[0]
    assert "WITH filtered AS" in query
    assert "FROM hedging_flow_5min" not in query


def test_dated_unfiltered_read_serves_the_all_scope():
    conn = _CannedConn(fetchval_sequence=[1], fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(db.get_hedging_flow_series(symbol="SPY", session_date=SESSION_DATE))

    query, args = conn.fetch_calls[0]
    assert "FROM hedging_flow_5min" in query
    assert args[1] == SCOPE_ALL


def test_dated_0dte_read_serves_the_0dte_scope():
    """The toggle sends the SESSION's date, and that is what identifies 0DTE.

    On a historical session it is not today's date, which is the bug this
    routing exists to make impossible: matching on ``etTodayDateKey()`` would
    classify every past 0DTE view as an arbitrary expiration filter and drop
    it onto the pruned CTE path.
    """
    conn = _CannedConn(fetchval_sequence=[1], fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(
        db.get_hedging_flow_series(
            symbol="SPY", session_date=SESSION_DATE, expirations=[SESSION_DATE]
        )
    )

    query, args = conn.fetch_calls[0]
    assert "FROM hedging_flow_5min" in query
    assert args[1] == SCOPE_0DTE


def test_dated_read_with_an_unstored_filter_falls_back_to_the_cte():
    """Two materialised scopes is a closed set; anything else runs live.

    That inherits the 90-day horizon, which is the same tradeoff
    flow_series_5min makes for filtered reads — documented rather than hidden.
    """
    conn = _CannedConn(fetchval_sequence=[1], fetch_rows=[])
    db = _make_db(conn)

    for kwargs in (
        {"strikes": [604.0]},
        {"expirations": [date(2026, 6, 19)]},
        {"expirations": [SESSION_DATE, date(2026, 6, 19)]},
    ):
        conn.fetch_calls.clear()
        conn._fetchvals = [1]
        asyncio.run(db.get_hedging_flow_series(symbol="SPY", session_date=SESSION_DATE, **kwargs))
        query, _args = conn.fetch_calls[0]
        assert "WITH filtered AS" in query, kwargs


def test_dated_and_live_reads_do_not_share_a_cache_entry():
    from src.api.database import DatabaseManager

    db = DatabaseManager()
    assert DatabaseManager._hedging_snapshot_scope(SESSION_DATE, None, None) == SCOPE_ALL
    assert DatabaseManager._hedging_snapshot_scope(SESSION_DATE, None, [SESSION_DATE]) == SCOPE_0DTE
    assert DatabaseManager._hedging_snapshot_scope(SESSION_DATE, [604.0], None) is None
    del db


# --------------------------------------------------------------------------- #
# The structure panel travels with the flow panel
# --------------------------------------------------------------------------- #
def test_regime_series_takes_the_same_dated_window():
    """Both panels share a crosshair, so they must share a window.

    If the structure series resolved its own dates it could drift a bar from
    the flow series, and the page stacks them on the assumption that bar N is
    the same five minutes in both.
    """
    conn = _CannedConn(fetchval_sequence=[1], fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(db.get_gamma_regime_series(symbol="SPY", session_date=SESSION_DATE))

    _query, args = conn.fetch_calls[0]
    assert args[1] == SESSION_START_UTC
    assert args[2] - args[1] == timedelta(hours=6, minutes=45)


# --------------------------------------------------------------------------- #
# HTTP surface
# --------------------------------------------------------------------------- #
def _build_app_with_mock_db(monkeypatch: pytest.MonkeyPatch):
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

    from src.api.main import app
    from src.api import main as mainmod

    return app, mainmod


def _attach(mainmod, method: str, returns):
    from src.api import database as dbmod

    setattr(dbmod.DatabaseManager, method, AsyncMock(return_value=returns))
    mainmod.db_manager = mainmod.db_manager or mainmod.DatabaseManager()
    setattr(mainmod.db_manager, method, AsyncMock(return_value=returns))


def test_date_is_echoed_as_the_session(monkeypatch: pytest.MonkeyPatch):
    """A dated payload names its day, so a client cannot mistake it for live."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, "get_hedging_flow_series", [_row(0, cum_net=600.0)])
        body = client.get("/api/flow/hedging?symbol=SPY&date=2026-06-12").json()

    assert body["session"] == "2026-06-12"
    # The disclosure is part of the contract on a historical read too: a
    # day-old estimate is not an observation, and storing it does not promote
    # it to one.
    assert body["basis"] == "aggressor_inferred"
    assert body["disclosure"]


def test_malformed_date_is_rejected_rather_than_ignored(monkeypatch: pytest.MonkeyPatch):
    """Silently falling back to `current` would serve today under a permalink."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, "get_hedging_flow_series", [])
        for bad in ("2026-13-45", "yesterday", "06/12/2026", "2026-6-12"):
            assert client.get(f"/api/flow/hedging?symbol=SPY&date={bad}").status_code == 400


def test_empty_date_behaves_as_absent(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, "get_hedging_flow_series", [])
        body = client.get("/api/flow/hedging?symbol=SPY&date=").json()

    assert body["session"] == "current"


def test_sessions_endpoint_shape(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    rows = [
        {
            "session_date": date(2026, 6, 12),
            "bar_count": 82,
            "real_bar_count": 74,
            "had_0dte": True,
            "cum_net_usd": -4.123e8,
            "first_bar": SESSION_START_UTC,
            "last_bar": SESSION_START_UTC + timedelta(hours=6, minutes=45),
        },
        {
            "session_date": date(2026, 6, 11),
            "bar_count": 30,
            "real_bar_count": 12,
            "had_0dte": False,
            "cum_net_usd": None,
            "first_bar": None,
            "last_bar": None,
        },
    ]

    with TestClient(app) as client:
        _attach(mainmod, "get_hedging_flow_sessions", rows)
        response = client.get("/api/flow/hedging/sessions?symbol=SPY")

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "SPY"
    assert body["count"] == 2
    first = body["sessions"][0]
    assert first["date"] == "2026-06-12"
    assert first["bar_count"] == 82
    assert first["real_bar_count"] == 74
    assert first["had_0dte"] is True
    assert first["last_bar"] == "2026-06-12T20:15:00Z"
    # A day with nothing to summarise still lists — the card just says less.
    assert body["sessions"][1]["cum_net_usd"] is None


def test_sessions_endpoint_404s_an_unknown_symbol(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, "get_hedging_flow_sessions", None)
        assert client.get("/api/flow/hedging/sessions?symbol=NOPE").status_code == 404


def test_sessions_endpoint_empty_list_is_not_an_error(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, "get_hedging_flow_sessions", [])
        response = client.get("/api/flow/hedging/sessions?symbol=SPY")

    assert response.status_code == 200
    assert response.json() == {"symbol": "SPY", "count": 0, "sessions": []}
