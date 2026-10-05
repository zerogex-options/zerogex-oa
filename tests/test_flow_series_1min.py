"""``/api/flow/series?timeframe=1min``: one-minute bars beside the 5-minute default.

The 1-minute series is a separate CTE over ``flow_contract_facts`` that shares
the 5-minute pipeline's accumulation stages, plus its own read path and cache.
These tests pin the SQL shape and the Python plumbing with canned rows, the
same way ``tests/test_api_flow_series.py`` does for the 5-minute series. That
the two series agree on real rows is asserted against Postgres by
``tests/test_flow_series_1min_parity.py``.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from src.flow_series_sql import FLOW_SERIES_1MIN_CTE_ASYNCPG, FLOW_SERIES_CTE_ASYNCPG
from tests.test_api_flow_series import (
    _CannedConn,
    _attach_series_mock,
    _bar_ts,
    _build_app_with_mock_db,
    _make_db,
    _mock_session_resolution_rows,
)


def _block(sql: str, name: str) -> str:
    """One CTE's body, from its header to the next top-level CTE."""
    rest = sql[sql.index(f"{name} AS (") :]
    return rest[: rest.index("\n                    ),")]


# ---------------------------------------------------------------------------
# SQL shape
# ---------------------------------------------------------------------------


def test_one_minute_sql_reads_facts_on_a_one_minute_grid():
    sql = FLOW_SERIES_1MIN_CTE_ASYNCPG
    filtered = _block(sql, "filtered")
    assert "FROM flow_contract_facts" in filtered
    assert "flow_by_contract" not in sql, "flow_by_contract only exists on the 5-minute grid"
    assert "date_trunc('minute', timestamp) AS bar_start" in filtered
    assert "INTERVAL '1 minute') AS g(bar_start)" in _block(sql, "timeline")
    assert "INTERVAL '5 minutes'" not in sql


def test_one_minute_sql_shares_the_five_minute_accumulation_stages():
    """Everything from ``joined`` on is one text, so the two series can differ
    only in how a bar is formed, never in how it is accumulated or emitted."""
    tail = "                    joined AS ("
    one = FLOW_SERIES_1MIN_CTE_ASYNCPG
    five = FLOW_SERIES_CTE_ASYNCPG
    assert one[one.index(tail) :] == five[five.index(tail) :]


def test_one_minute_sql_takes_net_flow_from_the_aggressor_split():
    per_bar = _block(FLOW_SERIES_1MIN_CTE_ASYNCPG, "per_bar")
    filtered = _block(FLOW_SERIES_1MIN_CTE_ASYNCPG, "filtered")
    # flow_by_contract.net_volume / net_premium are these same sums.
    assert "(buy_volume  - sell_volume)  AS net_volume" in filtered
    assert "(buy_premium - sell_premium) AS net_premium" in filtered
    # Puts subtract, as on the 5-minute series.
    assert "THEN net_volume ELSE -net_volume END" in per_bar
    # The field contract: contracts that traded in THIS bar.
    assert "COUNT(DISTINCT option_symbol)::int AS contract_count" in per_bar


def test_one_minute_underlying_price_ignores_the_filters():
    """Price comes from the tape and is identical under every filter."""
    block = _block(FLOW_SERIES_1MIN_CTE_ASYNCPG, "underlying_by_bar")
    assert "FROM underlying_quotes" in block
    assert "strike" not in block
    assert "expiration" not in block


def test_one_minute_sql_binds_the_same_five_parameters():
    for n in range(1, 6):
        assert f"${n}" in FLOW_SERIES_1MIN_CTE_ASYNCPG
    assert "$6" not in FLOW_SERIES_1MIN_CTE_ASYNCPG
    assert ":symbol" not in FLOW_SERIES_1MIN_CTE_ASYNCPG


# ---------------------------------------------------------------------------
# DatabaseManager.get_flow_series(timeframe="1min")
# ---------------------------------------------------------------------------


def _minute_rows(n: int):
    """``n`` newest-first one-minute rows."""
    return [
        {"bar_start": _bar_ts(n - 1 - i), "call_premium_cum": float(n - i), "is_synthetic": False}
        for i in range(n)
    ]


def test_get_flow_series_1min_runs_the_one_minute_cte():
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=_minute_rows(3)
    )
    db = _make_db(conn)

    rows = asyncio.run(
        db.get_flow_series(symbol="spy", session="current", strikes=[700.0], timeframe="1min")
    )

    assert [r["bar_start"] for r in rows] == [_bar_ts(2), _bar_ts(1), _bar_ts(0)]
    query, args = conn.fetch_calls[0]
    assert query == FLOW_SERIES_1MIN_CTE_ASYNCPG
    assert args[0] == "SPY"
    assert args[3] == [700.0]
    assert args[4] is None


def test_get_flow_series_default_is_still_five_minutes():
    conn = _CannedConn(fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(db.get_flow_series(symbol="SPY", session="current"))

    assert conn.fetch_calls[0][0] == FLOW_SERIES_CTE_ASYNCPG


def test_get_flow_series_1min_floors_the_live_session_end_to_the_minute(monkeypatch):
    from src.api import database as dbmod

    # 2026-04-24 10:07:42 ET: the 5-minute series ends at 10:05, this at 10:07.
    frozen = datetime(2026, 4, 24, 14, 7, 42, tzinfo=timezone.utc)

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return frozen.astimezone(tz) if tz is not None else frozen.replace(tzinfo=None)

    monkeypatch.setattr(dbmod, "datetime", _FrozenDateTime)
    conn = _CannedConn(fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(db.get_flow_series(symbol="SPY", session="current", timeframe="1min"))

    _query, args = conn.fetch_calls[0]
    assert args[1] == datetime(2026, 4, 24, 13, 30, tzinfo=timezone.utc)
    assert args[2] == datetime(2026, 4, 24, 14, 7, tzinfo=timezone.utc)


def test_get_flow_series_1min_tail_is_sliced_from_one_cached_series():
    """A tail poll must not run the CTE each time: the website polls every few
    seconds per open chart, and the 1-minute read has no snapshot behind it."""
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows() * 3, fetch_rows=_minute_rows(5)
    )
    db = _make_db(conn)

    first = asyncio.run(db.get_flow_series("SPY", "current", intervals=1, timeframe="1min"))
    second = asyncio.run(db.get_flow_series("SPY", "current", intervals=2, timeframe="1min"))
    full = asyncio.run(db.get_flow_series("SPY", "current", timeframe="1min"))

    assert len(conn.fetch_calls) == 1
    assert [r["bar_start"] for r in first] == [_bar_ts(4)]
    assert [r["bar_start"] for r in second] == [_bar_ts(4), _bar_ts(3)]
    assert len(full) == 5


def test_get_flow_series_1min_and_5min_keep_separate_cache_entries():
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows() * 3, fetch_rows=_minute_rows(2)
    )
    db = _make_db(conn)

    asyncio.run(db.get_flow_series("SPY", "current", timeframe="1min"))
    asyncio.run(db.get_flow_series("SPY", "current"))
    asyncio.run(db.get_flow_series("SPY", "current", timeframe="1min"))  # cached

    assert [q for q, _a in conn.fetch_calls] == [
        FLOW_SERIES_1MIN_CTE_ASYNCPG,
        FLOW_SERIES_CTE_ASYNCPG,
    ]


def test_get_flow_series_1min_caches_with_its_own_short_ttl(monkeypatch):
    conn = _CannedConn(fetchval_sequence=_mock_session_resolution_rows(), fetch_rows=[])
    db = _make_db(conn)
    captured = {}
    monkeypatch.setattr(
        db, "_cache_set", lambda key, _payload, ttl: captured.update(key=key, ttl=ttl)
    )

    asyncio.run(db.get_flow_series("SPY", "current", timeframe="1min"))

    assert captured["key"] == "flow_series_1min:SPY:current::"
    assert captured["ttl"] == db._flow_series_1min_cache_ttl_seconds == 5.0


def test_get_flow_series_1min_unknown_symbol_returns_none():
    conn = _CannedConn(fetchval_sequence=[None])
    db = _make_db(conn)

    assert asyncio.run(db.get_flow_series("ZZZZZ", "current", timeframe="1min")) is None
    assert conn.fetch_calls == []


def test_get_flow_series_1min_no_prior_session_returns_empty():
    conn = _CannedConn(fetchval_sequence=[1, date(2026, 4, 24), None])
    db = _make_db(conn)

    assert asyncio.run(db.get_flow_series("SPY", "prior", timeframe="1min")) == []
    assert conn.fetch_calls == []


# ---------------------------------------------------------------------------
# HTTP surface
# ---------------------------------------------------------------------------


def _row(minute: int) -> dict:
    return {"bar_start": _bar_ts(minute), "call_premium_cum": 1.0, "is_synthetic": False}


def test_http_series_1min_bars_end_one_minute_later(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        db = _attach_series_mock(mainmod, [_row(7)])
        response = client.get("/api/flow/series?symbol=SPY&timeframe=1min&intervals=1")

    assert response.status_code == 200
    body = response.json()
    assert body[0]["bar_start"] == "2026-04-24T13:37:00Z"
    assert body[0]["bar_end"] == "2026-04-24T13:38:00Z"
    kwargs = db.get_flow_series.await_args.kwargs
    assert kwargs["timeframe"] == "1min"
    assert kwargs["intervals"] == 1


def test_http_series_defaults_to_five_minute_bars(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        db = _attach_series_mock(mainmod, [_row(5)])
        response = client.get("/api/flow/series?symbol=SPY")

    body = response.json()
    assert body[0]["bar_end"] == (
        (_bar_ts(5) + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    assert db.get_flow_series.await_args.kwargs["timeframe"] == "5min"


def test_http_series_rejects_an_unknown_timeframe(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=SPY&timeframe=15min")

    assert response.status_code == 422
