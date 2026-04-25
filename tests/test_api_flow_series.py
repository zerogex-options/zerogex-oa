"""Tests for /api/flow/series and /api/flow/contracts.

The endpoints aggregate per-contract 5-minute rows from ``flow_by_contract``
into session-cumulative series that the frontend can render directly (no
client-side accumulator). Cover the spec's T1–T7 test vectors at both the
HTTP-surface and DB-method layers, plus validation and caching behaviour.

The SQL itself — in particular the delta-by-LAG, generate_series timeline,
underlying-price carry-forward, and cumulative-window math — is only
exercised against a real Postgres; the Python-layer tests here drive the
connection with canned row fixtures so we test parsing, slicing, caching,
and response shape without booting a DB.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

UTC = timezone.utc
# 2026-04-24 is a Friday — the T1/T2 spec fixture day. 09:30 ET == 13:30 UTC
# (EDT; DST in effect).
SESSION_START_UTC = datetime(2026, 4, 24, 13, 30, tzinfo=UTC)


def _bar_ts(minute_offset: int) -> datetime:
    return SESSION_START_UTC + timedelta(minutes=minute_offset)


class _CannedConn:
    """Fake asyncpg connection that returns scripted responses.

    ``fetchval_sequence`` is a list of values the connection returns from
    ``conn.fetchval(...)`` calls, consumed in order. Any extra fetchval
    calls beyond the scripted sequence return ``None`` — that's the DB
    saying "no rows." ``fetch_rows`` is a single list returned from
    ``conn.fetch(...)`` — there's only one main fetch per series call.
    """

    def __init__(
        self,
        *,
        fetchval_sequence: Optional[List[Any]] = None,
        fetch_rows: Optional[List[Dict[str, Any]]] = None,
        fetchrow_value: Optional[Dict[str, Any]] = None,
    ):
        self._fetchvals = list(fetchval_sequence or [])
        self._fetch_rows = fetch_rows or []
        self._fetchrow_value = fetchrow_value
        self.fetch_calls: List[tuple] = []
        self.fetchval_calls: List[tuple] = []
        self.fetchrow_calls: List[tuple] = []

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        if self._fetchvals:
            return self._fetchvals.pop(0)
        return None

    async def fetch(self, query, *args):
        self.fetch_calls.append((query, args))
        return self._fetch_rows

    async def fetchrow(self, query, *args):
        self.fetchrow_calls.append((query, args))
        return self._fetchrow_value

    # Swallow transactions used by _refresh_flow_cache (which we bypass).
    def transaction(self):
        @asynccontextmanager
        async def _cm():
            yield

        return _cm()


def _make_db(conn: _CannedConn):
    """Build a DatabaseManager whose pool is stubbed out to yield ``conn``."""
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    # _refresh_flow_cache has its own throttle that we don't want to
    # exercise in unit tests — stub to no-op.
    async def _noop(*_args, **_kwargs):
        return None

    db._refresh_flow_cache = _noop  # type: ignore[method-assign]
    return db


# ---------------------------------------------------------------------------
# DatabaseManager.get_flow_series — covers T1, T3 (tail), T4 (unknown), T5
# ---------------------------------------------------------------------------


def _mock_session_resolution_rows(current_date: date = date(2026, 4, 24)):
    """Return a fetchval sequence that makes _resolve_flow_series_session
    resolve to ``current_date`` for ``session=current``:

      1. EXISTS check → 1  (symbol has data)
      2. MAX ET date  → ``current_date``
    """
    return [1, current_date]


def test_get_flow_series_t4_unknown_symbol_returns_none():
    conn = _CannedConn(fetchval_sequence=[None])  # EXISTS → None
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0  # disable cache

    result = asyncio.run(db.get_flow_series(symbol="ABCDE", session="current"))

    assert result is None  # endpoint surfaces 404
    # Only the EXISTS check ran — no wasted main-query work.
    assert conn.fetch_calls == []


def test_get_flow_series_t5_filter_matches_nothing_returns_empty():
    # Session resolves fine, main query returns zero rows (because the
    # strikes filter matched no contracts → filtered CTE empty →
    # timeline gated empty).
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows(),
        fetch_rows=[],
    )
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0

    result = asyncio.run(
        db.get_flow_series(
            symbol="SPY",
            session="current",
            strikes=[999.0],
        )
    )

    assert result == []
    # strikes arg is forwarded as a list
    assert conn.fetch_calls
    _query, args = conn.fetch_calls[0]
    assert args[0] == "SPY"
    assert args[3] == [999.0]  # strikes_arg
    assert args[4] is None  # expirations_arg


def test_get_flow_series_t1_happy_path_passes_through_rows():
    happy_rows = [
        {
            "bar_start": _bar_ts(0),
            "call_premium_cum": 50000.0,
            "put_premium_cum": -30000.0,
            "call_volume_cum": 100,
            "put_volume_cum": 80,
            "net_volume_cum": 20,
            "raw_volume_cum": 180,
            "call_position_cum": 40,
            "put_position_cum": -20,
            "net_premium_cum": 20000.0,
            "put_call_ratio": 0.8,
            "underlying_price": 710.00,
            "contract_count": 2,
            "is_synthetic": False,
        },
        {
            "bar_start": _bar_ts(5),
            "call_premium_cum": 62000.0,
            "put_premium_cum": 60000.0,
            "call_volume_cum": 150,
            "put_volume_cum": 200,
            "net_volume_cum": 90,
            "raw_volume_cum": 350,
            "call_position_cum": 50,
            "put_position_cum": 40,
            "net_premium_cum": 122000.0,
            "put_call_ratio": 1.3333333333,
            "underlying_price": 710.25,
            "contract_count": 2,
            "is_synthetic": False,
        },
    ]
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows(),
        fetch_rows=happy_rows,
    )
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0

    result = asyncio.run(db.get_flow_series(symbol="SPY", session="current"))

    assert result is not None and len(result) == 2
    assert result[0]["bar_start"] == _bar_ts(0)
    assert result[0]["call_premium_cum"] == 50000.0
    assert result[1]["call_premium_cum"] == 62000.0


def test_get_flow_series_t3_intervals_one_returns_only_tail():
    # Build a 3-row session; intervals=1 must return just the last row
    # with its true session-cumulative values — not a per-bar delta.
    rows = [
        {"bar_start": _bar_ts(0), "call_premium_cum": 50000.0, "is_synthetic": False},
        {"bar_start": _bar_ts(5), "call_premium_cum": 50000.0, "is_synthetic": True},
        {"bar_start": _bar_ts(10), "call_premium_cum": 55000.0, "is_synthetic": False},
    ]
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows(),
        fetch_rows=rows,
    )
    db = _make_db(conn)

    result = asyncio.run(db.get_flow_series(symbol="SPY", session="current", intervals=1))

    assert result is not None and len(result) == 1
    assert result[0]["bar_start"] == _bar_ts(10)
    assert result[0]["call_premium_cum"] == 55000.0


def test_get_flow_series_intervals_bypasses_cache():
    """intervals=N must always re-query: the spec's incremental polling
    path explicitly bypasses the cache so the tail reflects fresh data."""
    rows = [{"bar_start": _bar_ts(0), "call_premium_cum": 1.0, "is_synthetic": False}]
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows() * 2,  # 2 calls
        fetch_rows=rows,
    )
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 60.0  # cache on; should still bypass

    asyncio.run(db.get_flow_series(symbol="SPY", session="current", intervals=1))
    asyncio.run(db.get_flow_series(symbol="SPY", session="current", intervals=1))

    # Both calls hit the DB (no cache reuse).
    assert len(conn.fetch_calls) == 2


def test_get_flow_series_full_session_uses_cache():
    rows = [{"bar_start": _bar_ts(0), "call_premium_cum": 1.0, "is_synthetic": False}]
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows(),
        fetch_rows=rows,
    )
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 60.0

    first = asyncio.run(db.get_flow_series(symbol="SPY", session="current"))
    second = asyncio.run(db.get_flow_series(symbol="SPY", session="current"))

    assert first == second
    # Second call was served from cache.
    assert len(conn.fetch_calls) == 1


def test_get_flow_series_session_prior_no_prior_returns_empty():
    """Symbol exists but has only today's data — session=prior → 200+[]."""
    # Fetchval sequence: EXISTS→1, current_date→2026-04-24, prior_date→None
    conn = _CannedConn(
        fetchval_sequence=[1, date(2026, 4, 24), None],
    )
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0

    result = asyncio.run(db.get_flow_series(symbol="SPY", session="prior"))

    assert result == []  # not None — symbol exists, just no prior session


# ---------------------------------------------------------------------------
# DatabaseManager.get_flow_contracts — companion endpoint semantics
# ---------------------------------------------------------------------------


def test_get_flow_contracts_happy_path():
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows(),
        fetchrow_value={
            "strikes": [655.0, 660.0, 700.0],
            "expirations": [date(2026, 4, 24), date(2026, 4, 27)],
        },
    )
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0

    result = asyncio.run(db.get_flow_contracts(symbol="spy", session="current"))

    assert result == {
        "strikes": [655.0, 660.0, 700.0],
        "expirations": ["2026-04-24", "2026-04-27"],
    }


def test_get_flow_contracts_unknown_symbol_returns_none():
    conn = _CannedConn(fetchval_sequence=[None])
    db = _make_db(conn)

    result = asyncio.run(db.get_flow_contracts(symbol="NOPE", session="current"))

    assert result is None


def test_get_flow_contracts_session_prior_no_prior_returns_empty_lists():
    conn = _CannedConn(fetchval_sequence=[1, date(2026, 4, 24), None])
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0

    result = asyncio.run(db.get_flow_contracts(symbol="SPY", session="prior"))

    assert result == {"strikes": [], "expirations": []}


# ---------------------------------------------------------------------------
# HTTP surface — validation, response shape, error codes
# ---------------------------------------------------------------------------


def _build_app_with_mock_db(monkeypatch: pytest.MonkeyPatch):
    """Reload src.api.main with an AsyncMock DatabaseManager.

    The lifespan runs DatabaseManager().connect(); the returned app
    lets callers override db.get_flow_series/get_flow_contracts per-test.
    """
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


def _attach_series_mock(mainmod, returns):
    mainmod.db_manager = mainmod.db_manager or mainmod.DatabaseManager()
    mainmod.db_manager.get_flow_series = AsyncMock(return_value=returns)  # type: ignore[method-assign]
    return mainmod.db_manager


def _attach_contracts_mock(mainmod, returns):
    mainmod.db_manager = mainmod.db_manager or mainmod.DatabaseManager()
    mainmod.db_manager.get_flow_contracts = AsyncMock(return_value=returns)  # type: ignore[method-assign]
    return mainmod.db_manager


def test_http_series_response_shape_matches_spec(monkeypatch: pytest.MonkeyPatch):
    """Response rows must have the full field contract from the spec,
    with timestamps formatted as trailing-Z UTC ISO strings."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    canned = [
        {
            "bar_start": _bar_ts(0),
            "call_premium_cum": 50000.0,
            "put_premium_cum": -30000.0,
            "call_volume_cum": 100,
            "put_volume_cum": 80,
            "net_volume_cum": 20,
            "raw_volume_cum": 180,
            "call_position_cum": 40,
            "put_position_cum": -20,
            "net_premium_cum": 20000.0,
            "put_call_ratio": 0.8,
            "underlying_price": 710.00,
            "contract_count": 2,
            "is_synthetic": False,
        },
    ]

    with TestClient(app) as client:
        _attach_series_mock(mainmod, canned)
        response = client.get("/api/flow/series?symbol=SPY")

    assert response.status_code == 200, response.text
    payload = response.json()
    assert isinstance(payload, list)
    assert len(payload) == 1
    row = payload[0]
    # Timestamp format: exactly "YYYY-MM-DDTHH:MM:SSZ", no +00:00.
    assert row["timestamp"] == "2026-04-24T13:30:00Z"
    assert row["bar_start"] == "2026-04-24T13:30:00Z"
    assert row["bar_end"] == "2026-04-24T13:35:00Z"
    # Every documented field is present.
    for field in (
        "call_premium_cum",
        "put_premium_cum",
        "call_volume_cum",
        "put_volume_cum",
        "net_volume_cum",
        "raw_volume_cum",
        "call_position_cum",
        "put_position_cum",
        "net_premium_cum",
        "put_call_ratio",
        "underlying_price",
        "contract_count",
        "is_synthetic",
    ):
        assert field in row


def test_http_series_t6_put_call_ratio_null(monkeypatch: pytest.MonkeyPatch):
    """No put volume all session → put_call_ratio must be null, not 0/Inf."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    canned = [
        {
            "bar_start": _bar_ts(0),
            "call_premium_cum": 1.0,
            "put_premium_cum": 0.0,
            "call_volume_cum": 100,
            "put_volume_cum": 0,
            "net_volume_cum": 10,
            "raw_volume_cum": 100,
            "call_position_cum": 10,
            "put_position_cum": 0,
            "net_premium_cum": 1.0,
            "put_call_ratio": None,
            "underlying_price": 710.0,
            "contract_count": 1,
            "is_synthetic": False,
        },
    ]
    with TestClient(app) as client:
        _attach_series_mock(mainmod, canned)
        response = client.get("/api/flow/series?symbol=SPY")

    assert response.status_code == 200
    assert response.json()[0]["put_call_ratio"] is None


def test_http_series_t4_unknown_symbol_returns_404(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_series_mock(mainmod, None)  # DB sentinel for unknown
        response = client.get("/api/flow/series?symbol=NOPE")

    assert response.status_code == 404
    assert "symbol not found" in response.json()["detail"].lower()


def test_http_series_t5_filter_drops_all_returns_empty_200(monkeypatch: pytest.MonkeyPatch):
    """Filter matches zero contracts → 200 + []. Must NOT emit 81
    synthetic zero-cumulative rows."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=SPY&strikes=999")

    assert response.status_code == 200
    assert response.json() == []


def test_http_series_symbol_lowercase_accepted(monkeypatch: pytest.MonkeyPatch):
    """Lowercase symbols are uppercased server-side."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        mock = _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=spy")

    assert response.status_code == 200
    mock.get_flow_series.assert_awaited_once()
    call_kwargs = mock.get_flow_series.await_args.kwargs
    assert call_kwargs["symbol"] == "SPY"


def test_http_series_rejects_bad_symbol_pattern(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=BAD1")  # digit not allowed

    assert response.status_code == 400


def test_http_series_rejects_overlong_symbol(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=" + "A" * 11)

    # FastAPI's Query(max_length=10) catches this as 422, but pattern
    # validation would also reject it as 400. Either is acceptable — what
    # matters is that the request doesn't reach the DB.
    assert response.status_code in (400, 422)


def test_http_series_strikes_silent_drop_keeps_valid_entries(monkeypatch: pytest.MonkeyPatch):
    """Unparseable strikes are dropped silently; parseable ones forwarded."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        mock = _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=SPY&strikes=700,abc,705.5")

    assert response.status_code == 200
    call_kwargs = mock.get_flow_series.await_args.kwargs
    assert call_kwargs["strikes"] == [700.0, 705.5]


def test_http_series_all_bad_strikes_returns_400(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=SPY&strikes=abc,xyz")

    assert response.status_code == 400


def test_http_series_expirations_silent_drop(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        mock = _attach_series_mock(mainmod, [])
        response = client.get(
            "/api/flow/series?symbol=SPY&expirations=2026-04-24,not-a-date,2026-04-27"
        )

    assert response.status_code == 200
    call_kwargs = mock.get_flow_series.await_args.kwargs
    assert call_kwargs["expirations"] == [date(2026, 4, 24), date(2026, 4, 27)]


def test_http_series_all_bad_expirations_returns_400(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=SPY&expirations=bad,2020-13-99")

    assert response.status_code == 400


def test_http_series_intervals_upper_bound(monkeypatch: pytest.MonkeyPatch):
    """intervals > 390 must be rejected (spec caps generously at 390)."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=SPY&intervals=500")

    # FastAPI's le=390 returns 422; the validation intent is satisfied.
    assert response.status_code in (400, 422)


def test_http_series_intervals_lower_bound(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=SPY&intervals=0")

    assert response.status_code in (400, 422)


def test_http_series_bad_session_enum(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_series_mock(mainmod, [])
        response = client.get("/api/flow/series?symbol=SPY&session=nonsense")

    assert response.status_code in (400, 422)


# ---------------------------------------------------------------------------
# /api/flow/contracts HTTP surface
# ---------------------------------------------------------------------------


def test_http_contracts_happy_path(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_contracts_mock(
            mainmod,
            {"strikes": [700.0, 705.0], "expirations": ["2026-04-24", "2026-04-27"]},
        )
        response = client.get("/api/flow/contracts?symbol=SPY")

    assert response.status_code == 200
    payload = response.json()
    assert payload["strikes"] == [700.0, 705.0]
    assert payload["expirations"] == ["2026-04-24", "2026-04-27"]


def test_http_contracts_unknown_symbol_returns_404(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach_contracts_mock(mainmod, None)
        response = client.get("/api/flow/contracts?symbol=NOPE")

    assert response.status_code == 404


# ---------------------------------------------------------------------------
# SQL-shape regression: underlying_price must come from the tape, not the
# per-contract flow_by_contract.underlying_price column, and must be
# invariant under strike/expiration filters.
# ---------------------------------------------------------------------------


def test_series_sql_pulls_underlying_from_tape_not_per_contract_column():
    """Observed in prod: 20–30 minutes of flat underlying_price followed by
    a sudden jump, because the SQL was aggregating
    flow_by_contract.underlying_price (per-contract last-trade price). The
    fix pulls the bar's underlying from underlying_quotes (the tape). This
    test pins the SQL shape so a future refactor can't silently regress."""
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows(),
        fetch_rows=[],
    )
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0

    asyncio.run(db.get_flow_series(symbol="SPY", session="current"))

    assert conn.fetch_calls, "main query should have run"
    main_query = conn.fetch_calls[0][0]
    # The tape source is joined in.
    assert "underlying_quotes" in main_query
    # Neither the per-contract CTE nor the per-bar aggregate may carry the
    # flow_by_contract.underlying_price column — that was the bug.
    # "ARRAY_AGG(underlying_price" was the stair-step aggregation; it
    # must not appear.
    assert "ARRAY_AGG(underlying_price" not in main_query
    # Per-bar CTE pulls underlying_price from the tape join, not from
    # flow_by_contract.
    assert "pb.underlying_price" not in main_query
    assert "ub.underlying_price" in main_query


def test_series_sql_underlying_cte_ignores_strike_expiration_filters():
    """Filter invariance: underlying_price is a property of the tape, not of
    the filtered options. The underlying_by_bar CTE must read purely from
    underlying_quotes without applying the strikes/expirations filters, so
    two requests for the same (symbol, bar_start) with different filters
    return identical underlying values."""
    conn = _CannedConn(
        fetchval_sequence=_mock_session_resolution_rows(),
        fetch_rows=[],
    )
    db = _make_db(conn)
    db._flow_endpoint_cache_ttl_seconds = 0.0

    asyncio.run(
        db.get_flow_series(
            symbol="SPY",
            session="current",
            strikes=[700.0, 705.0],
            expirations=[date(2026, 4, 24)],
        )
    )

    main_query = conn.fetch_calls[0][0]
    # Isolate the underlying_by_bar block and assert it does not reference
    # the strike/expiration filter parameters ($4, $5).
    start = main_query.find("underlying_by_bar AS")
    assert start != -1, "underlying_by_bar CTE must exist"
    end = main_query.find(")", start)
    # Find the matching closing paren by counting depth instead of string
    # search (the CTE contains parens in ARRAY_AGG/INTERVAL etc.).
    depth = 0
    i = main_query.find("(", start)
    assert i != -1
    for j in range(i, len(main_query)):
        ch = main_query[j]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                end = j
                break
    cte_body = main_query[start : end + 1]
    # The strikes/expirations params ($4, $5) must NOT appear inside the
    # underlying CTE — only $1 (symbol), $2 (ts_start), $3 (ts_end).
    assert "$4" not in cte_body
    assert "$5" not in cte_body
    # And it reads from underlying_quotes, not flow_by_contract.
    assert "underlying_quotes" in cte_body
    assert "flow_by_contract" not in cte_body
