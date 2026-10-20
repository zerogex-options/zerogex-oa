"""Tests for /api/flow/by-strike.

Contracts bought and sold at each strike, per 5-minute bar, read from
``flow_contract_facts`` (``src/flow_by_strike_sql.py``). The SQL itself only
runs against a real Postgres; these drive the connection with canned rows to
pin the parts that live in Python: the window arithmetic, the tail sliced from
the cached full session, single-flight, the timeout contract, the response
shape, and the futures projection of the strike axis.
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

UTC = timezone.utc
# 2026-04-24 is a Friday. 09:30 ET == 13:30 UTC (EDT), 16:15 ET == 20:15 UTC.
SESSION_DATE = date(2026, 4, 24)
SESSION_START_UTC = datetime(2026, 4, 24, 13, 30, tzinfo=UTC)
LAST_BAR_UTC = datetime(2026, 4, 24, 20, 15, tzinfo=UTC)


def _row(
    bar: datetime, strike: float, cb: int = 10, cs: int = 5, pb: int = 3, ps: int = 7
) -> Dict[str, Any]:
    return {
        "bar_start": bar,
        "strike": strike,
        "call_bought": cb,
        "call_sold": cs,
        "put_bought": pb,
        "put_sold": ps,
    }


class _CannedConn:
    """Fake asyncpg connection: scripted fetchvals, one canned fetch result."""

    def __init__(
        self,
        *,
        fetchval_sequence: Optional[List[Any]] = None,
        fetch_rows: Optional[List[Dict[str, Any]]] = None,
        fetch_gate: Optional[asyncio.Event] = None,
        fetch_error: Optional[BaseException] = None,
    ):
        self._fetchvals = list(fetchval_sequence or [])
        self._fetch_rows = fetch_rows or []
        self._fetch_gate = fetch_gate
        self._fetch_error = fetch_error
        self.fetch_calls: List[tuple] = []

    async def fetchval(self, query, *args):
        if self._fetchvals:
            return self._fetchvals.pop(0)
        return None

    async def fetch(self, query, *args, **kwargs):
        self.fetch_calls.append((query, args))
        if self._fetch_gate is not None:
            await self._fetch_gate.wait()
        if self._fetch_error is not None:
            raise self._fetch_error
        return self._fetch_rows


def _make_db(conn: _CannedConn, ttl: float = 30.0):
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    async def _noop(*_args, **_kwargs):
        return None

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    db._refresh_flow_cache = _noop  # type: ignore[method-assign]
    db._flow_by_strike_cache_ttl_seconds = ttl
    return db


# A dated read resolves arithmetically after one symbol check, so the window
# is fixed: 13:30 UTC through the 20:15 bar, which ends at 20:20.
_KNOWN_SYMBOL = [1]

_FULL_SESSION = [
    _row(LAST_BAR_UTC, 709.0),
    _row(LAST_BAR_UTC, 710.0),
    _row(LAST_BAR_UTC - timedelta(minutes=5), 710.0),
    _row(LAST_BAR_UTC - timedelta(minutes=10), 711.0),
]


# ---------------------------------------------------------------------------
# DatabaseManager.get_flow_by_strike
# ---------------------------------------------------------------------------


def test_unknown_symbol_returns_none_without_reading_facts():
    conn = _CannedConn(fetchval_sequence=[None])  # current: EXISTS -> None
    db = _make_db(conn)

    assert asyncio.run(db.get_flow_by_strike(symbol="ABCDE")) is None
    assert conn.fetch_calls == []


def test_session_without_data_returns_empty():
    # prior: EXISTS -> 1, current date -> the 24th, prior date -> None.
    conn = _CannedConn(fetchval_sequence=[1, SESSION_DATE, None])
    db = _make_db(conn)

    assert asyncio.run(db.get_flow_by_strike(symbol="SPY", session="prior")) == []
    assert conn.fetch_calls == []


def test_full_session_passes_rows_through_with_the_window_and_filters():
    conn = _CannedConn(fetchval_sequence=list(_KNOWN_SYMBOL), fetch_rows=_FULL_SESSION)
    db = _make_db(conn)

    rows = asyncio.run(
        db.get_flow_by_strike(
            symbol="spy",
            session_date=SESSION_DATE,
            strikes=[710.0, 709.0],
            expirations=[date(2026, 4, 24)],
            band=0.025,
        )
    )

    assert rows == _FULL_SESSION
    assert len(conn.fetch_calls) == 1
    _query, args = conn.fetch_calls[0]
    window_end = LAST_BAR_UTC + timedelta(minutes=5)
    assert args == (
        "SPY",
        SESSION_START_UTC,  # first bar returned
        window_end,  # one bar past the newest bar's start
        SESSION_START_UTC,  # the band is built on the whole session
        window_end,
        0.025,
        [710.0, 709.0],
        [date(2026, 4, 24)],
    )


def test_tail_is_sliced_from_the_cached_full_session():
    conn = _CannedConn(fetchval_sequence=list(_KNOWN_SYMBOL), fetch_rows=_FULL_SESSION)
    db = _make_db(conn)

    async def _both():
        full = await db.get_flow_by_strike(symbol="SPY", session_date=SESSION_DATE)
        tail = await db.get_flow_by_strike(symbol="SPY", session_date=SESSION_DATE, intervals=2)
        return full, tail

    full, tail = asyncio.run(_both())

    assert full == _FULL_SESSION
    # The last two bars: 20:15 and 20:10, not 20:05.
    assert tail == _FULL_SESSION[:3]
    # One read served both; tail polls never reach the database on their own.
    assert len(conn.fetch_calls) == 1


def test_tail_of_one_bar_is_the_newest_bar_only():
    conn = _CannedConn(fetchval_sequence=list(_KNOWN_SYMBOL), fetch_rows=_FULL_SESSION)
    db = _make_db(conn)

    tail = asyncio.run(db.get_flow_by_strike(symbol="SPY", session_date=SESSION_DATE, intervals=1))

    assert tail == _FULL_SESSION[:2]


def test_filters_and_band_get_their_own_cache_entries():
    conn = _CannedConn(
        fetchval_sequence=list(_KNOWN_SYMBOL) * 3,
        fetch_rows=_FULL_SESSION,
    )
    db = _make_db(conn)

    async def _three():
        await db.get_flow_by_strike(symbol="SPY", session_date=SESSION_DATE)
        await db.get_flow_by_strike(symbol="SPY", session_date=SESSION_DATE, strikes=[710.0])
        await db.get_flow_by_strike(symbol="SPY", session_date=SESSION_DATE, band=0.05)

    asyncio.run(_three())

    assert len(conn.fetch_calls) == 3


def test_concurrent_misses_share_one_read():
    async def _scenario():
        gate = asyncio.Event()
        conn = _CannedConn(
            fetchval_sequence=list(_KNOWN_SYMBOL) * 2,
            fetch_rows=_FULL_SESSION,
            fetch_gate=gate,
        )
        db = _make_db(conn)
        first = asyncio.ensure_future(
            db.get_flow_by_strike(symbol="SPY", session_date=SESSION_DATE)
        )
        second = asyncio.ensure_future(
            db.get_flow_by_strike(symbol="SPY", session_date=SESSION_DATE, intervals=1)
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        gate.set()
        return await first, await second, conn

    full, tail, conn = asyncio.run(_scenario())

    assert full == _FULL_SESSION
    assert tail == _FULL_SESSION[:2]
    assert len(conn.fetch_calls) == 1


def test_a_timeout_raises_and_is_not_cached():
    conn = _CannedConn(
        fetchval_sequence=list(_KNOWN_SYMBOL) * 2,
        fetch_error=asyncio.TimeoutError(),
    )
    db = _make_db(conn)

    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(db.get_flow_by_strike(symbol="SPY", session_date=SESSION_DATE))
    # Nothing was cached, so the next call reads again rather than serving
    # the failure as an empty answer.
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(db.get_flow_by_strike(symbol="SPY", session_date=SESSION_DATE))
    assert len(conn.fetch_calls) == 2


# ---------------------------------------------------------------------------
# HTTP surface
# ---------------------------------------------------------------------------


def _build_app_with_mock_db(monkeypatch: pytest.MonkeyPatch):
    """Reload src.api.main against an AsyncMock DatabaseManager."""
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


def _attach(mainmod, **kwargs):
    mainmod.db_manager = mainmod.db_manager or mainmod.DatabaseManager()
    mainmod.db_manager.get_flow_by_strike = AsyncMock(**kwargs)  # type: ignore[method-assign]
    return mainmod.db_manager


def test_http_response_shape(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    canned = [
        {
            "bar_start": SESSION_START_UTC,
            "strike": 710.5,
            "call_bought": 12,
            "call_sold": 4,
            "put_bought": 0,
            "put_sold": 9,
        }
    ]

    with TestClient(app) as client:
        _attach(mainmod, return_value=canned)
        response = client.get("/api/flow/by-strike?symbol=SPY")

    assert response.status_code == 200, response.text
    assert response.json() == [
        {
            "timestamp": "2026-04-24T13:30:00Z",
            "strike": 710.5,
            "call_bought": 12,
            "call_sold": 4,
            "put_bought": 0,
            "put_sold": 9,
        }
    ]


def test_http_forwards_filters_session_and_band_as_a_fraction(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        db = _attach(mainmod, return_value=[])
        response = client.get(
            "/api/flow/by-strike?symbol=qqq&date=2026-04-24&strikes=500,501&expirations=2026-04-24"
            "&intervals=2&band_pct=2.5"
        )

    assert response.status_code == 200, response.text
    assert response.json() == []
    kwargs = db.get_flow_by_strike.call_args.kwargs
    assert kwargs["symbol"] == "QQQ"
    assert kwargs["session_date"] == SESSION_DATE
    assert kwargs["strikes"] == [500.0, 501.0]
    assert kwargs["expirations"] == [date(2026, 4, 24)]
    assert kwargs["intervals"] == 2
    assert kwargs["band"] == pytest.approx(0.025)


def test_http_unknown_symbol_is_404(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, return_value=None)
        response = client.get("/api/flow/by-strike?symbol=ZZZZ")

    assert response.status_code == 404


def test_http_timeout_is_504_not_an_empty_list(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, side_effect=asyncio.TimeoutError())
        response = client.get("/api/flow/by-strike?symbol=SPY")

    assert response.status_code == 504


@pytest.mark.parametrize(
    "query",
    [
        "symbol=SP-Y",  # pattern
        "symbol=SPY&band_pct=0",  # below the band floor
        "symbol=SPY&band_pct=25",  # past the band ceiling
        "symbol=SPY&intervals=0",
        "symbol=SPY&intervals=83",  # longer than a session
        "symbol=SPY&date=2026-4-24",
    ],
)
def test_http_rejects_bad_parameters(monkeypatch: pytest.MonkeyPatch, query: str):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, return_value=[])
        response = client.get(f"/api/flow/by-strike?{query}")

    assert response.status_code in (400, 422), response.text


# ---------------------------------------------------------------------------
# ES / NQ: the strike axis is projected, the counts are not
# ---------------------------------------------------------------------------


def test_futures_request_projects_strikes_and_leaves_counts(monkeypatch: pytest.MonkeyPatch):
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    from src.api import futures_middleware as fm
    from src.jobs.futures_projection import FuturesBasis

    basis = FuturesBasis(
        index_symbol="SPX",
        futures_symbol="ES",
        ratio=1.0067,
        source="measured",
        observed_at=datetime(2026, 8, 21, 20, 0, tzinfo=UTC),
        sample_count=5,
        feed_symbol="@ES",
    )

    async def fake_resolve_basis(db, symbol, **kwargs):
        return basis if symbol.upper() in ("ES", "SPX") else None

    async def fake_live_spot(index_symbol):
        return 6650.25

    monkeypatch.setattr(fm, "resolve_basis", fake_resolve_basis)
    monkeypatch.setattr(fm, "_live_futures_spot", fake_live_spot)
    monkeypatch.setattr(fm, "_db_manager", lambda: object())

    seen: Dict[str, Optional[str]] = {}

    async def by_strike(request):
        seen["symbol"] = request.query_params.get("symbol")
        return JSONResponse(
            [
                {
                    "timestamp": "2026-08-21T19:55:00Z",
                    "strike": 6700.0,
                    "call_bought": 120,
                    "call_sold": 80,
                    "put_bought": 40,
                    "put_sold": 60,
                }
            ]
        )

    app = Starlette(routes=[Route("/api/flow/by-strike", by_strike)])
    app.add_middleware(fm.FuturesProjectionMiddleware)

    response = TestClient(app).get("/api/flow/by-strike?symbol=ES")

    assert response.status_code == 200, response.text
    assert seen["symbol"] == "SPX"  # the handler read the index chain
    body = response.json()
    rows = body["data"] if isinstance(body, dict) and "data" in body else body
    row = rows[0]
    assert row["strike"] == pytest.approx(6700.0 * 1.0067, abs=0.25)
    assert (row["call_bought"], row["call_sold"], row["put_bought"], row["put_sold"]) == (
        120,
        80,
        40,
        60,
    )
