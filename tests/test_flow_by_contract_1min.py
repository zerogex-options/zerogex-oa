"""``/api/flow/by-contract?timeframe=1min``: one-minute rows beside the 5-minute default.

flow_by_contract only exists at five minutes, so one-minute rows come from
``_FLOW_BY_CONTRACT_1MIN_SQL`` over flow_contract_facts, through their own read
path, cache key and window cap. These tests pin that plumbing with canned rows;
that the rows match the 5-minute ones is asserted against Postgres by
``tests/test_flow_by_contract_1min_parity.py``.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from src.api.database import FLOW_BY_CONTRACT_1MIN_MAX_MINUTES
from tests.test_api_flow_series import (
    _CannedConn,
    _attach_by_contract_mock,
    _build_app_with_mock_db,
    _make_db,
)

ET = ZoneInfo("America/New_York")


def _et(h: int, m: int, s: int = 0) -> datetime:
    return datetime(2026, 10, 2, h, m, s, tzinfo=ET)


class _Conn(_CannedConn):
    """Also records ``execute`` calls and whether each ran inside a transaction."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.executed: list = []
        self.fetched_in_transaction: list = []
        self._in_transaction = False

    async def execute(self, query, *args):
        self.executed.append((query, self._in_transaction))

    async def fetch(self, query, *args):
        self.fetched_in_transaction.append(self._in_transaction)
        return await super().fetch(query, *args)

    def transaction(self):
        conn = self

        @asynccontextmanager
        async def _cm():
            conn._in_transaction = True
            try:
                yield
            finally:
                conn._in_transaction = False

        return _cm()


def _db_at(monkeypatch, start: datetime, end: datetime, conn: _Conn):
    from src.api import database as dbmod

    monkeypatch.setattr(dbmod, "_get_flow_session_bounds", lambda session: (start, end))
    return _make_db(conn)


def _window(conn: _Conn) -> tuple:
    """(symbol, open, first minute, last minute) bound to the 1-minute query."""
    from src.api.database import _FLOW_BY_CONTRACT_1MIN_SQL

    query, args = conn.fetch_calls[-1]
    assert query == _FLOW_BY_CONTRACT_1MIN_SQL
    return args


# ---------------------------------------------------------------------------
# DatabaseManager.get_flow(timeframe="1min")
# ---------------------------------------------------------------------------


def test_one_minute_defaults_to_the_trailing_window_of_a_live_session(monkeypatch):
    conn = _Conn()
    db = _db_at(monkeypatch, _et(9, 30), _et(10, 4, 30), conn)

    asyncio.run(db.get_flow("spy", "current", timeframe="1min"))

    first = _et(10, 4).timestamp() - (FLOW_BY_CONTRACT_1MIN_MAX_MINUTES - 1) * 60
    assert _window(conn) == ("SPY", _et(9, 30), datetime.fromtimestamp(first, ET), _et(10, 4))


def test_one_minute_window_never_starts_before_the_open(monkeypatch):
    conn = _Conn()
    db = _db_at(monkeypatch, _et(9, 30), _et(9, 40, 10), conn)

    asyncio.run(db.get_flow("SPY", "current", timeframe="1min"))

    assert _window(conn) == ("SPY", _et(9, 30), _et(9, 30), _et(9, 40))


def test_one_minute_window_ends_on_the_minute_before_the_close(monkeypatch):
    """A close at exactly 16:15:00 ends on the 16:14 minute, as the 5-minute
    rows end on the 16:10 bucket."""
    conn = _Conn()
    db = _db_at(monkeypatch, _et(9, 30), _et(16, 15), conn)

    asyncio.run(db.get_flow("SPY", "prior", intervals=5, timeframe="1min"))

    assert _window(conn) == ("SPY", _et(9, 30), _et(16, 10), _et(16, 14))


def test_one_minute_direct_callers_are_clamped_to_the_cap(monkeypatch):
    conn = _Conn()
    db = _db_at(monkeypatch, _et(9, 30), _et(16, 15), conn)

    asyncio.run(db.get_flow("SPY", "prior", intervals=390, timeframe="1min"))

    first = _et(16, 14).timestamp() - (FLOW_BY_CONTRACT_1MIN_MAX_MINUTES - 1) * 60
    assert _window(conn)[2] == datetime.fromtimestamp(first, ET)


def test_one_minute_query_runs_with_jit_off_in_its_own_transaction(monkeypatch):
    conn = _Conn()
    db = _db_at(monkeypatch, _et(9, 30), _et(16, 15), conn)

    asyncio.run(db.get_flow("SPY", "prior", timeframe="1min"))

    assert conn.executed == [("SET LOCAL jit = off", True)]
    assert conn.fetched_in_transaction == [True]


def test_one_minute_and_five_minute_rows_are_cached_apart(monkeypatch):
    conn = _Conn(fetch_rows=[{"timestamp": _et(10, 0)}])
    db = _db_at(monkeypatch, _et(9, 30), _et(16, 15), conn)

    asyncio.run(db.get_flow("SPY", "prior"))
    asyncio.run(db.get_flow("SPY", "prior", timeframe="1min"))
    asyncio.run(db.get_flow("SPY", "prior", timeframe="1min"))
    asyncio.run(db.get_flow("SPY", "prior", intervals=5, timeframe="1min"))

    queries = [q for q, _ in conn.fetch_calls]
    assert len(queries) == 3, "the repeated 1-minute call is a cache hit"
    assert "FROM flow_by_contract" in queries[0]
    assert "FROM flow_by_contract" not in queries[1]
    assert _window(conn)[2] == _et(16, 10)


def test_default_timeframe_still_reads_the_five_minute_rollup(monkeypatch):
    conn = _Conn()
    db = _db_at(monkeypatch, _et(9, 30), _et(16, 15), conn)

    asyncio.run(db.get_flow("SPY", "prior"))

    assert "FROM flow_by_contract" in conn.fetch_calls[0][0]
    assert conn.executed == []


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def _flow_row() -> dict:
    return {
        "timestamp": _et(10, 4),
        "symbol": "SPY",
        "option_type": "C",
        "strike": 665.0,
        "expiration": date(2026, 10, 2),
        "dte": 0,
        "raw_volume": 120,
        "raw_premium": 30000.0,
        "net_volume": 40,
        "net_premium": 10000.0,
        "underlying_price": 664.8,
    }


@pytest.mark.parametrize("prefix", ["/api", "/api/v2"])
def test_http_one_minute_passes_the_timeframe_through(monkeypatch, prefix):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        db = _attach_by_contract_mock(mainmod, [_flow_row()])
        response = client.get(f"{prefix}/flow/by-contract?symbol=SPY&timeframe=1min&intervals=5")

    assert response.status_code == 200, response.text
    kwargs = db.get_flow.await_args.kwargs
    assert kwargs["timeframe"] == "1min"
    assert kwargs["intervals"] == 5


def test_http_by_contract_defaults_to_five_minutes(monkeypatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        db = _attach_by_contract_mock(mainmod, [_flow_row()])
        response = client.get("/api/flow/by-contract?symbol=SPY")

    assert response.status_code == 200, response.text
    assert db.get_flow.await_args.kwargs["timeframe"] == "5min"


def test_http_one_minute_accepts_the_cap(monkeypatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    cap = FLOW_BY_CONTRACT_1MIN_MAX_MINUTES

    with TestClient(app) as client:
        _attach_by_contract_mock(mainmod, [])
        response = client.get(f"/api/flow/by-contract?symbol=SPY&timeframe=1min&intervals={cap}")

    assert response.status_code == 200, response.text


def test_http_one_minute_rejects_intervals_past_the_cap(monkeypatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    cap = FLOW_BY_CONTRACT_1MIN_MAX_MINUTES

    with TestClient(app) as client:
        db = _attach_by_contract_mock(mainmod, [])
        response = client.get(
            f"/api/flow/by-contract?symbol=SPY&timeframe=1min&intervals={cap + 1}"
        )
        five = client.get(f"/api/flow/by-contract?symbol=SPY&intervals={cap + 1}")

    assert response.status_code == 400
    assert str(cap) in response.json()["detail"]
    assert five.status_code == 200, "the cap is for one-minute rows only"
    assert db.get_flow.await_count == 1


def test_http_by_contract_rejects_an_unknown_timeframe(monkeypatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_by_contract_mock(mainmod, [])
        response = client.get("/api/flow/by-contract?symbol=SPY&timeframe=15min")

    assert response.status_code == 422
