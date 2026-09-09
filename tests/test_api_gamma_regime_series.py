"""Tests for /api/gex/regime-series and get_gamma_regime_series.

The series maths lives in tests/test_gamma_regime_series.py; these pin the
read path and the wire contract — including the property the whole design
rests on, that a cache miss returns empty rather than falling back to
computing the series on the request path.
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
SESSION_START_UTC = datetime(2026, 4, 24, 13, 30, tzinfo=UTC)


def _bar_ts(minute_offset: int) -> datetime:
    return SESSION_START_UTC + timedelta(minutes=minute_offset)


def _row(
    minute: int,
    *,
    anchored_stability: float = 0.0,
    anchored_lean: float = 0.0,
    rolling_stability: Optional[float] = None,
    rolling_lean: Optional[float] = None,
    spot: Optional[float] = 700.0,
    expired: Optional[List[date]] = None,
) -> Dict[str, Any]:
    return {
        "bar_start": _bar_ts(minute),
        "spot": spot,
        "anchored_lean": anchored_lean,
        "anchored_stability": anchored_stability,
        "anchored_net_shift": 1.0,
        "anchored_gross_shift": 2.0,
        "rolling_lean": rolling_lean,
        "rolling_stability": rolling_stability,
        "rolling_net_shift": None,
        "rolling_gross_shift": None,
        "sigma_price": 7.0,
        "near_spot_stock": 5e6,
        "strike_count": 42,
        "expired_expirations": expired or [],
        "rolling_bars": 6,
    }


class _CannedConn:
    def __init__(self, *, fetchval_sequence=None, fetch_rows=None):
        self._fetchvals = list(fetchval_sequence or [])
        self._fetch_rows = fetch_rows or []
        self.fetch_calls: List[tuple] = []

    async def fetchval(self, query, *args):
        return self._fetchvals.pop(0) if self._fetchvals else None

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

    async def _noop(*_a, **_k):
        return None

    db._refresh_flow_cache = _noop  # type: ignore[method-assign]
    db._flow_series_endpoint_cache_ttl_seconds = 0.0
    return db


def _session_resolution(d: date = date(2026, 4, 24)):
    return [1, d]


# --------------------------------------------------------------------------- #
# DatabaseManager.get_gamma_regime_series
# --------------------------------------------------------------------------- #
def test_unknown_symbol_returns_none():
    conn = _CannedConn(fetchval_sequence=[None])
    db = _make_db(conn)

    assert asyncio.run(db.get_gamma_regime_series(symbol="ABCDE")) is None
    assert conn.fetch_calls == []


def test_session_with_nothing_written_returns_empty():
    conn = _CannedConn(fetchval_sequence=_session_resolution(), fetch_rows=[])
    db = _make_db(conn)

    assert asyncio.run(db.get_gamma_regime_series(symbol="SPY")) == []


def test_read_never_falls_back_to_computing_the_series():
    """The design rests on this. A compute-on-miss fallback would put a
    two-chain diff per bar back on the request path — the stampede shape the
    materialised table exists to avoid."""
    conn = _CannedConn(fetchval_sequence=_session_resolution(), fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(db.get_gamma_regime_series(symbol="SPY"))

    assert len(conn.fetch_calls) == 1
    query = conn.fetch_calls[0][0]
    assert "gamma_regime_5min" in query
    assert "gex_by_strike" not in query
    assert "option_chains" not in query


def test_intervals_takes_the_leading_rows():
    rows = [_row(15), _row(10), _row(5), _row(0)]
    conn = _CannedConn(fetchval_sequence=_session_resolution(), fetch_rows=rows)
    db = _make_db(conn)

    result = asyncio.run(db.get_gamma_regime_series(symbol="SPY", intervals=2))

    assert [r["bar_start"] for r in result] == [_bar_ts(15), _bar_ts(10)]


# --------------------------------------------------------------------------- #
# HTTP surface
# --------------------------------------------------------------------------- #
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

    from src.api.main import app
    from src.api import main as mainmod

    return app, mainmod


def _attach(mainmod, returns):
    from src.api import database as dbmod

    dbmod.DatabaseManager.get_gamma_regime_series = AsyncMock(  # type: ignore[method-assign]
        return_value=returns
    )
    mainmod.db_manager = mainmod.db_manager or mainmod.DatabaseManager()
    mainmod.db_manager.get_gamma_regime_series = AsyncMock(  # type: ignore[method-assign]
        return_value=returns
    )


def test_http_response_shape(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app(monkeypatch)
    canned = [_row(0, anchored_stability=2.5e6, anchored_lean=-1.1e6)]

    with TestClient(app) as client:
        _attach(mainmod, canned)
        response = client.get("/api/gex/regime-series?symbol=SPY")

    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload["symbol"] == "SPY"
    assert payload["rolling_bars"] == 6

    bar = payload["bars"][0]
    assert bar["timestamp"] == "2026-04-24T13:30:00Z"
    assert bar["bar_end"] == "2026-04-24T13:35:00Z"
    assert bar["anchored_stability"] == 2.5e6
    assert bar["anchored_lean"] == -1.1e6
    assert bar["spot"] == 700.0
    assert bar["strike_count"] == 42


def test_http_rolling_nulls_survive_serialization(monkeypatch: pytest.MonkeyPatch):
    """Null, not zero — a zero draws a measured 'no change' through the open."""
    app, mainmod = _build_app(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [_row(0)])
        bar = client.get("/api/gex/regime-series?symbol=SPY").json()["bars"][0]

    assert bar["rolling_stability"] is None
    assert bar["rolling_lean"] is None


def test_http_expired_expirations_serialize_as_iso_dates(
    monkeypatch: pytest.MonkeyPatch,
):
    app, mainmod = _build_app(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [_row(0, expired=[date(2026, 4, 24)])])
        bar = client.get("/api/gex/regime-series?symbol=SPY").json()["bars"][0]

    assert bar["expired_expirations"] == ["2026-04-24"]


def test_http_bars_align_with_the_hedging_flow_grid(monkeypatch: pytest.MonkeyPatch):
    """Both series must key identically or they cannot be stacked."""
    app, mainmod = _build_app(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [_row(10), _row(5), _row(0)])
        payload = client.get("/api/gex/regime-series?symbol=SPY").json()

    assert [b["timestamp"] for b in payload["bars"]] == [
        "2026-04-24T13:40:00Z",
        "2026-04-24T13:35:00Z",
        "2026-04-24T13:30:00Z",
    ]


def test_http_empty_session_returns_envelope(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [])
        payload = client.get("/api/gex/regime-series?symbol=SPY").json()

    assert payload["bars"] == []
    assert payload["rolling_bars"] is None


def test_http_unknown_symbol_404(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, None)
        assert client.get("/api/gex/regime-series?symbol=NOPE").status_code == 404


def test_http_rejects_bad_symbol(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [])
        assert client.get("/api/gex/regime-series?symbol=SP%20Y").status_code == 400
