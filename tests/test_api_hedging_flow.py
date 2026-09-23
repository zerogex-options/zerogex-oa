"""Tests for /api/flow/hedging and DatabaseManager.get_hedging_flow_series.

The SQL itself (5-minute bucketing, the delta-notional product, the
generate_series timeline, price carry-forward and the cumulative window) only
runs against a real Postgres; these drive the connection with canned rows to
pin parsing, ordering, slicing, caching, the derived rate/flip layer and the
response contract — including the disclosure fields, which are part of the
contract rather than decoration.

Harness mirrors tests/test_api_flow_series.py so the two suites stay legible
side by side.
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
    call: float = 0.0,
    put: float = 0.0,
    cum_call: float = 0.0,
    cum_put: float = 0.0,
    price: Optional[float] = 710.0,
    contracts: int = 2,
    classified: Optional[float] = 1.0,
    synthetic: bool = False,
) -> Dict[str, Any]:
    return {
        "bar_start": _bar_ts(minute),
        "call_flow_usd": call,
        "put_flow_usd": put,
        "net_flow_usd": call + put,
        "cum_call_usd": cum_call,
        "cum_put_usd": cum_put,
        "cum_net_usd": cum_call + cum_put,
        "classified_ratio": classified,
        "underlying_price": price,
        "contract_count": contracts,
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

    async def fetchval(self, query, *args):
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


def _session_resolution(current_date: date = date(2026, 4, 24)):
    """fetchval sequence resolving session=current: EXISTS → 1, MAX date."""
    return [1, current_date]


# --------------------------------------------------------------------------- #
# DatabaseManager.get_hedging_flow_series
# --------------------------------------------------------------------------- #
def test_unknown_symbol_returns_none_without_running_main_query():
    conn = _CannedConn(fetchval_sequence=[None])
    db = _make_db(conn)

    assert asyncio.run(db.get_hedging_flow_series(symbol="ABCDE")) is None
    assert conn.fetch_calls == []


def test_filter_matching_nothing_returns_empty_list():
    conn = _CannedConn(fetchval_sequence=_session_resolution(), fetch_rows=[])
    db = _make_db(conn)

    assert asyncio.run(db.get_hedging_flow_series(symbol="SPY", strikes=[999.0])) == []


def test_filters_are_forwarded_as_positional_args():
    conn = _CannedConn(fetchval_sequence=_session_resolution(), fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(
        db.get_hedging_flow_series(
            symbol="SPY",
            strikes=[710.0],
            expirations=[date(2026, 4, 24)],
        )
    )

    _query, args = conn.fetch_calls[0]
    assert args[0] == "SPY"
    assert args[3] == [710.0]
    assert args[4] == [date(2026, 4, 24)]


def test_intervals_takes_the_leading_rows_newest_first():
    rows = [_row(15), _row(10), _row(5), _row(0)]
    conn = _CannedConn(fetchval_sequence=_session_resolution(), fetch_rows=rows)
    db = _make_db(conn)

    result = asyncio.run(db.get_hedging_flow_series(symbol="SPY", intervals=2))

    assert [r["bar_start"] for r in result] == [_bar_ts(15), _bar_ts(10)]


def test_symbol_is_upcased_before_query():
    conn = _CannedConn(fetchval_sequence=_session_resolution(), fetch_rows=[])
    db = _make_db(conn)

    asyncio.run(db.get_hedging_flow_series(symbol="spy"))

    assert conn.fetch_calls[0][1][0] == "SPY"


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


def _attach(mainmod, returns):
    from src.api import database as dbmod

    dbmod.DatabaseManager.get_hedging_flow_series = AsyncMock(  # type: ignore[method-assign]
        return_value=returns
    )
    mainmod.db_manager = mainmod.db_manager or mainmod.DatabaseManager()
    mainmod.db_manager.get_hedging_flow_series = AsyncMock(  # type: ignore[method-assign]
        return_value=returns
    )


def test_http_response_shape(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    canned = [_row(0, call=120_000.0, put=-20_000.0, cum_call=120_000.0, cum_put=-20_000.0)]

    with TestClient(app) as client:
        _attach(mainmod, canned)
        response = client.get("/api/flow/hedging?symbol=SPY")

    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload["symbol"] == "SPY"
    assert payload["session"] == "current"
    assert payload["basis"] == "aggressor_inferred"
    assert payload["smoothing_bars"] == 3

    bar = payload["bars"][0]
    assert bar["timestamp"] == "2026-04-24T13:30:00Z"
    assert bar["bar_end"] == "2026-04-24T13:35:00Z"
    assert bar["call_flow_usd"] == 120_000.0
    assert bar["put_flow_usd"] == -20_000.0
    assert bar["net_flow_usd"] == 100_000.0
    assert bar["cum_net_usd"] == 100_000.0
    assert bar["net_flow_ma_usd"] is None  # window not filled on bar 1
    assert bar["classified_ratio"] == 1.0
    assert bar["is_synthetic"] is False


def test_http_disclosure_is_present_and_names_the_assumption(
    monkeypatch: pytest.MonkeyPatch,
):
    """The terminology rules make this part of the contract, not decoration —
    a UI cannot carry the caveat through if the payload drops it."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [_row(0, call=1.0)])
        payload = client.get("/api/flow/hedging?symbol=SPY").json()

    disclosure = payload["disclosure"].lower()
    assert "estimated" in disclosure
    assert "market maker" in disclosure
    assert "not observed dealer flow" in disclosure


def test_http_bars_are_newest_first_and_ma_aligns_chronologically(
    monkeypatch: pytest.MonkeyPatch,
):
    """The derivation runs chronologically but the wire order is newest-first;
    a reversal bug here would silently attach each MA to the wrong bar."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    # Chronologically: 0→100k, 5→100k, 10→100k, 15→400k. DB order is reversed.
    canned = [
        _row(15, call=400_000.0),
        _row(10, call=100_000.0),
        _row(5, call=100_000.0),
        _row(0, call=100_000.0),
    ]

    with TestClient(app) as client:
        _attach(mainmod, canned)
        payload = client.get("/api/flow/hedging?symbol=SPY").json()

    bars = payload["bars"]
    assert [b["timestamp"] for b in bars] == [
        "2026-04-24T13:45:00Z",
        "2026-04-24T13:40:00Z",
        "2026-04-24T13:35:00Z",
        "2026-04-24T13:30:00Z",
    ]
    # Newest bar's trailing 3-bar mean = (100k + 100k + 400k) / 3.
    assert bars[0]["net_flow_ma_usd"] == pytest.approx(200_000.0)
    # Next one back (13:40) is the earliest bar with a full window: three 100k.
    assert bars[1]["net_flow_ma_usd"] == pytest.approx(100_000.0)
    # The two oldest cannot have one — the window never filled behind them.
    assert bars[2]["net_flow_ma_usd"] is None
    assert bars[3]["net_flow_ma_usd"] is None


def test_http_reports_a_rate_flip(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    nets = [80_000.0] * 4 + [-80_000.0] * 4
    canned = [_row(i * 5, call=n) for i, n in enumerate(nets)]
    canned.reverse()  # DB hands back newest-first

    with TestClient(app) as client:
        _attach(mainmod, canned)
        payload = client.get("/api/flow/hedging?symbol=SPY").json()

    rate_flips = [f for f in payload["flips"] if f["kind"] == "rate"]
    assert len(rate_flips) == 1
    assert rate_flips[0]["direction"] == "to_selling"
    assert rate_flips[0]["magnitude_usd"] > 0


def test_http_smoothing_param_changes_flip_detection(monkeypatch: pytest.MonkeyPatch):
    """smoothing=1 is raw, so a single-bar spike flips twice; the default
    smoothing absorbs it. This is the parameter doing its job.

    The deadband is pinned off here so the assertion isolates smoothing —
    otherwise the band suppresses the same spike and the test would pass for
    the wrong reason.
    """
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    nets = [90_000.0, 90_000.0, -30_000.0, 90_000.0, 90_000.0]
    canned = [_row(i * 5, call=n) for i, n in enumerate(nets)]
    canned.reverse()

    with TestClient(app) as client:
        _attach(mainmod, canned)
        raw = client.get("/api/flow/hedging?symbol=SPY&smoothing=1&flat_band=0").json()
        smoothed = client.get("/api/flow/hedging?symbol=SPY&flat_band=0").json()

    assert len([f for f in raw["flips"] if f["kind"] == "rate"]) == 2
    assert [f for f in smoothed["flips"] if f["kind"] == "rate"] == []
    assert raw["smoothing_bars"] == 1


def test_http_flat_band_suppresses_chatter(monkeypatch: pytest.MonkeyPatch):
    """The live-session fix, at the wire: a rate nicking across zero reports a
    flip per nick with the band off, and none with it on."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    nets = [900_000.0, -900_000.0] * 3 + [9_000.0, -7_000.0, 8_000.0, -6_000.0]
    canned = [_row(i * 5, call=n) for i, n in enumerate(nets)]
    canned.reverse()

    with TestClient(app) as client:
        _attach(mainmod, canned)
        off = client.get("/api/flow/hedging?symbol=SPY&smoothing=1&flat_band=0").json()
        on = client.get("/api/flow/hedging?symbol=SPY&smoothing=1").json()

    assert len([f for f in off["flips"] if f["kind"] == "rate"]) > len(
        [f for f in on["flips"] if f["kind"] == "rate"]
    )
    assert on["flat_band_ratio"] == 0.5


def test_http_empty_session_returns_envelope_with_no_bars(
    monkeypatch: pytest.MonkeyPatch,
):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [])
        response = client.get("/api/flow/hedging?symbol=SPY")

    assert response.status_code == 200
    payload = response.json()
    assert payload["bars"] == []
    assert payload["flips"] == []
    assert payload["basis"] == "aggressor_inferred"


def test_http_unknown_symbol_returns_404(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, None)
        assert client.get("/api/flow/hedging?symbol=NOPE").status_code == 404


def test_http_rejects_bad_symbol_pattern(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [])
        assert client.get("/api/flow/hedging?symbol=SP%20Y").status_code == 400


def test_http_lowercase_symbol_accepted(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [_row(0, call=1.0)])
        payload = client.get("/api/flow/hedging?symbol=spy").json()

    assert payload["symbol"] == "SPY"


def test_http_rejects_out_of_range_smoothing(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [])
        assert client.get("/api/flow/hedging?symbol=SPY&smoothing=0").status_code == 422
        assert client.get("/api/flow/hedging?symbol=SPY&smoothing=99").status_code == 422


def test_http_zero_dte_expiration_filter_is_forwarded(monkeypatch: pytest.MonkeyPatch):
    """Isolating 0DTE is just the expirations filter carrying today's date."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [_row(0, call=1.0)])
        response = client.get("/api/flow/hedging?symbol=SPY&expirations=2026-04-24")

    assert response.status_code == 200
    call = mainmod.db_manager.get_hedging_flow_series.call_args
    assert call.kwargs["expirations"] == [date(2026, 4, 24)]


def test_http_null_price_bar_serializes_as_null(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [_row(0, call=1.0, price=None, classified=None)])
        bar = client.get("/api/flow/hedging?symbol=SPY").json()["bars"][0]

    assert bar["underlying_price"] is None
    assert bar["classified_ratio"] is None
