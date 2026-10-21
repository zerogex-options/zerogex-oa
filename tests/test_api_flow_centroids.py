"""Tests for /api/flow/centroids.

Call and put centroids per 5-minute bar, from per-(bar, side) sums read out of
``flow_contract_facts`` (``src/flow_centroids_sql.py``). The SQL itself only
runs against a real Postgres; these drive the connection with canned rows to
pin the parts that live in Python: the window arithmetic, the cache and
single-flight, the timeout contract, the response shape, and the futures
projection of the centroid levels.
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


def _agg(bar: datetime, side: str, volume: int, strike: float) -> Dict[str, Any]:
    """A (bar, side) sum where every contract traded at one strike."""
    return {
        "bar_start": bar,
        "option_type": side,
        "volume": volume,
        "strike_volume": strike * volume,
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
    db._flow_centroids_cache_ttl_seconds = ttl
    return db


# A dated read resolves arithmetically after one symbol check, so the window
# is fixed: 13:30 UTC through the 20:15 bar, which ends at 20:20.
_KNOWN_SYMBOL = [1]

_SESSION_ROWS = [
    _agg(LAST_BAR_UTC - timedelta(minutes=5), "C", 100, 710.0),
    _agg(LAST_BAR_UTC - timedelta(minutes=5), "P", 50, 700.0),
    _agg(LAST_BAR_UTC, "C", 100, 712.0),
]


# ---------------------------------------------------------------------------
# DatabaseManager.get_flow_centroids
# ---------------------------------------------------------------------------


def test_unknown_symbol_returns_none_without_reading_facts():
    conn = _CannedConn(fetchval_sequence=[None])  # current: EXISTS -> None
    db = _make_db(conn)

    assert asyncio.run(db.get_flow_centroids(symbol="ABCDE")) is None
    assert conn.fetch_calls == []


def test_session_without_data_returns_empty():
    # prior: EXISTS -> 1, current date -> the 24th, prior date -> None.
    conn = _CannedConn(fetchval_sequence=[1, SESSION_DATE, None])
    db = _make_db(conn)

    assert asyncio.run(db.get_flow_centroids(symbol="SPY", session="prior")) == []
    assert conn.fetch_calls == []


def test_full_session_passes_rows_through_with_the_window_and_filter():
    conn = _CannedConn(fetchval_sequence=list(_KNOWN_SYMBOL), fetch_rows=_SESSION_ROWS)
    db = _make_db(conn)

    rows = asyncio.run(
        db.get_flow_centroids(
            symbol="spy",
            session_date=SESSION_DATE,
            expirations=[date(2026, 4, 24)],
        )
    )

    assert rows == _SESSION_ROWS
    assert len(conn.fetch_calls) == 1
    _query, args = conn.fetch_calls[0]
    assert args == (
        "SPY",
        SESSION_START_UTC,
        LAST_BAR_UTC + timedelta(minutes=5),  # one bar past the newest bar's start
        [date(2026, 4, 24)],
    )


def test_repeat_reads_are_served_from_the_cache():
    conn = _CannedConn(fetchval_sequence=list(_KNOWN_SYMBOL), fetch_rows=_SESSION_ROWS)
    db = _make_db(conn)

    async def _twice():
        await db.get_flow_centroids(symbol="SPY", session_date=SESSION_DATE)
        return await db.get_flow_centroids(symbol="SPY", session_date=SESSION_DATE)

    assert asyncio.run(_twice()) == _SESSION_ROWS
    assert len(conn.fetch_calls) == 1


def test_an_expiration_filter_gets_its_own_cache_entry():
    conn = _CannedConn(fetchval_sequence=list(_KNOWN_SYMBOL) * 2, fetch_rows=_SESSION_ROWS)
    db = _make_db(conn)

    async def _two():
        await db.get_flow_centroids(symbol="SPY", session_date=SESSION_DATE)
        await db.get_flow_centroids(
            symbol="SPY", session_date=SESSION_DATE, expirations=[SESSION_DATE]
        )

    asyncio.run(_two())

    assert len(conn.fetch_calls) == 2


def test_concurrent_misses_share_one_read():
    async def _scenario():
        gate = asyncio.Event()
        conn = _CannedConn(
            fetchval_sequence=list(_KNOWN_SYMBOL) * 2,
            fetch_rows=_SESSION_ROWS,
            fetch_gate=gate,
        )
        db = _make_db(conn)
        first = asyncio.ensure_future(
            db.get_flow_centroids(symbol="SPY", session_date=SESSION_DATE)
        )
        second = asyncio.ensure_future(
            db.get_flow_centroids(symbol="SPY", session_date=SESSION_DATE)
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        gate.set()
        return await first, await second, conn

    first, second, conn = asyncio.run(_scenario())

    assert first == second == _SESSION_ROWS
    assert len(conn.fetch_calls) == 1


def test_a_timeout_raises_and_is_not_cached():
    conn = _CannedConn(
        fetchval_sequence=list(_KNOWN_SYMBOL) * 2,
        fetch_error=asyncio.TimeoutError(),
    )
    db = _make_db(conn)

    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(db.get_flow_centroids(symbol="SPY", session_date=SESSION_DATE))
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(db.get_flow_centroids(symbol="SPY", session_date=SESSION_DATE))
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
    mainmod.db_manager.get_flow_centroids = AsyncMock(**kwargs)  # type: ignore[method-assign]
    return mainmod.db_manager


def test_http_response_shape(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    bar0 = SESSION_START_UTC
    bar1 = SESSION_START_UTC + timedelta(minutes=5)
    canned = [
        _agg(bar0, "C", 30, 7710.0),
        _agg(bar0, "C", 10, 7700.0),
        _agg(bar0, "P", 20, 7650.0),
        _agg(bar1, "C", 60, 7720.0),
    ]

    with TestClient(app) as client:
        _attach(mainmod, return_value=canned)
        response = client.get("/api/flow/centroids?symbol=SPX&window_minutes=5")

    assert response.status_code == 200, response.text
    assert response.json() == [
        {
            "timestamp": "2026-04-24T13:30:00Z",
            "call_centroid": 7707.5,
            "put_centroid": 7650.0,
            "call_centroid_recent": 7707.5,
            "put_centroid_recent": 7650.0,
            "call_volume": 40,
            "put_volume": 20,
        },
        {
            "timestamp": "2026-04-24T13:35:00Z",
            # (7707.5 * 40 + 7720 * 60) / 100
            "call_centroid": 7715.0,
            "put_centroid": 7650.0,
            "call_centroid_recent": 7720.0,
            "put_centroid_recent": None,
            "call_volume": 100,
            "put_volume": 20,
        },
    ]


def test_http_rounds_levels_to_the_cent(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    canned = [
        {
            "bar_start": SESSION_START_UTC,
            "option_type": "C",
            "volume": 3,
            "strike_volume": 7700.0 + 7705.0 + 7705.0,
        }
    ]

    with TestClient(app) as client:
        _attach(mainmod, return_value=canned)
        response = client.get("/api/flow/centroids?symbol=SPX")

    assert response.status_code == 200, response.text
    assert response.json()[0]["call_centroid"] == 7703.33


def test_http_default_window_is_fifteen_minutes(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    canned = [
        _agg(SESSION_START_UTC + timedelta(minutes=5 * n), "C", 100, 7700.0 + 10 * n)
        for n in range(4)
    ]

    with TestClient(app) as client:
        _attach(mainmod, return_value=canned)
        response = client.get("/api/flow/centroids?symbol=SPX")

    assert response.status_code == 200, response.text
    # The last bar's recent window holds bars 1-3: (7710 + 7720 + 7730) / 3.
    assert response.json()[-1]["call_centroid_recent"] == 7720.0


def test_http_forwards_symbol_date_and_expirations(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        db = _attach(mainmod, return_value=[])
        response = client.get(
            "/api/flow/centroids?symbol=qqq&date=2026-04-24&expirations=2026-04-24"
        )

    assert response.status_code == 200, response.text
    assert response.json() == []
    kwargs = db.get_flow_centroids.call_args.kwargs
    assert kwargs["symbol"] == "QQQ"
    assert kwargs["session_date"] == SESSION_DATE
    assert kwargs["expirations"] == [date(2026, 4, 24)]


def test_http_unknown_symbol_is_404(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, return_value=None)
        response = client.get("/api/flow/centroids?symbol=ZZZZ")

    assert response.status_code == 404


def test_http_timeout_is_504_not_an_empty_list(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, side_effect=asyncio.TimeoutError())
        response = client.get("/api/flow/centroids?symbol=SPY")

    assert response.status_code == 504


@pytest.mark.parametrize(
    "query",
    [
        "symbol=SP-Y",  # pattern
        "symbol=SPY&window_minutes=0",
        "symbol=SPY&window_minutes=7",  # not a whole number of bars
        "symbol=SPY&window_minutes=65",  # past the ceiling
        "symbol=SPY&date=2026-4-24",
        "symbol=SPY&session=today",
    ],
)
def test_http_rejects_bad_parameters(monkeypatch: pytest.MonkeyPatch, query: str):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, return_value=[])
        response = client.get(f"/api/flow/centroids?{query}")

    assert response.status_code in (400, 422), response.text


# ---------------------------------------------------------------------------
# ES / NQ: the centroids are projected, the volumes are not
# ---------------------------------------------------------------------------


def test_futures_request_projects_centroids_and_leaves_volumes(
    monkeypatch: pytest.MonkeyPatch,
):
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

    async def centroids(request):
        seen["symbol"] = request.query_params.get("symbol")
        return JSONResponse(
            [
                {
                    "timestamp": "2026-08-21T19:55:00Z",
                    "call_centroid": 6700.0,
                    "put_centroid": 6600.0,
                    "call_centroid_recent": 6690.0,
                    "put_centroid_recent": None,
                    "call_volume": 1200,
                    "put_volume": 900,
                }
            ]
        )

    app = Starlette(routes=[Route("/api/flow/centroids", centroids)])
    app.add_middleware(fm.FuturesProjectionMiddleware)

    response = TestClient(app).get("/api/flow/centroids?symbol=ES")

    assert response.status_code == 200, response.text
    assert seen["symbol"] == "SPX"  # the handler read the index chain
    body = response.json()
    rows = body["data"] if isinstance(body, dict) and "data" in body else body
    row = rows[0]
    assert row["call_centroid"] == pytest.approx(6700.0 * 1.0067, abs=0.25)
    assert row["put_centroid"] == pytest.approx(6600.0 * 1.0067, abs=0.25)
    assert row["call_centroid_recent"] == pytest.approx(6690.0 * 1.0067, abs=0.25)
    assert row["put_centroid_recent"] is None
    assert (row["call_volume"], row["put_volume"]) == (1200, 900)
