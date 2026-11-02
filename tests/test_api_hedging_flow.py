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
# DatabaseManager.get_hedging_flow_series(timeframe="1min")
#
# Above the HTTP section on purpose: _attach replaces get_hedging_flow_series on
# the DatabaseManager class itself, so a database test placed after it would
# call the mock instead of the method.
# --------------------------------------------------------------------------- #
def _spy_on_session_resolution(db) -> List[int]:
    """Record the bar each session resolution floors to, then defer to it."""
    seen: List[int] = []
    real = db._resolve_flow_series_session

    async def _spy(conn, symbol, session, session_date=None, bar_seconds=300):
        seen.append(bar_seconds)
        return await real(conn, symbol, session, session_date, bar_seconds=bar_seconds)

    db._resolve_flow_series_session = _spy  # type: ignore[method-assign]
    return seen


def test_one_minute_read_runs_the_minute_query_on_a_minute_grid():
    from src.hedging_flow_sql import HEDGING_FLOW_1MIN_CTE_ASYNCPG

    conn = _CannedConn(fetchval_sequence=_session_resolution(), fetch_rows=[_row(1), _row(0)])
    db = _make_db(conn)
    bar_seconds = _spy_on_session_resolution(db)

    result = asyncio.run(db.get_hedging_flow_series(symbol="spy", timeframe="1min"))

    assert [r["bar_start"] for r in result] == [_bar_ts(1), _bar_ts(0)]
    assert bar_seconds == [60]
    query, args = conn.fetch_calls[0]
    assert query == HEDGING_FLOW_1MIN_CTE_ASYNCPG
    assert args[0] == "SPY"


def test_one_minute_dated_read_runs_the_query_not_the_five_minute_snapshot():
    """hedging_flow_5min holds 5-minute bars only. An unfiltered dated read is
    exactly the one the 5-minute path serves from the snapshot, so it is the
    one a 1-minute read must not."""
    from src.hedging_flow_sql import HEDGING_FLOW_1MIN_CTE_ASYNCPG

    # Dated resolution probes the symbols table once, then resolves by date.
    conn = _CannedConn(fetchval_sequence=[1], fetch_rows=[_row(0)])
    db = _make_db(conn)

    asyncio.run(
        db.get_hedging_flow_series(symbol="SPY", session_date=date(2026, 4, 24), timeframe="1min")
    )

    query, args = conn.fetch_calls[0]
    assert query == HEDGING_FLOW_1MIN_CTE_ASYNCPG
    assert args[1] == datetime(2026, 4, 24, 13, 30, tzinfo=UTC)
    assert args[3] is None and args[4] is None


def test_one_minute_unknown_symbol_returns_none_without_running_the_query():
    conn = _CannedConn(fetchval_sequence=[None])
    db = _make_db(conn)

    assert asyncio.run(db.get_hedging_flow_series(symbol="ABCDE", timeframe="1min")) is None
    assert conn.fetch_calls == []


def test_one_minute_tail_is_sliced_from_one_cached_read():
    """The website polls; a tail must not run the query again inside the TTL."""
    rows = [_row(3), _row(2), _row(1), _row(0)]
    conn = _CannedConn(fetchval_sequence=_session_resolution(), fetch_rows=rows)
    db = _make_db(conn)
    db._flow_series_1min_cache_ttl_seconds = 60.0

    full = asyncio.run(db.get_hedging_flow_series(symbol="SPY", timeframe="1min"))
    tail = asyncio.run(db.get_hedging_flow_series(symbol="SPY", timeframe="1min", intervals=2))

    assert len(full) == 4
    assert [r["bar_start"] for r in tail] == [_bar_ts(3), _bar_ts(2)]
    assert len(conn.fetch_calls) == 1


def test_one_and_five_minute_reads_are_cached_apart():
    """Same symbol, session and filters: a cached 5-minute series must never
    be served to a 1-minute request, or the other way round."""
    from src.hedging_flow_sql import HEDGING_FLOW_1MIN_CTE_ASYNCPG, HEDGING_FLOW_CTE_ASYNCPG

    conn = _CannedConn(
        fetchval_sequence=_session_resolution() + _session_resolution(), fetch_rows=[_row(0)]
    )
    db = _make_db(conn)
    db._flow_series_endpoint_cache_ttl_seconds = 60.0
    db._flow_series_1min_cache_ttl_seconds = 60.0

    asyncio.run(db.get_hedging_flow_series(symbol="SPY"))
    asyncio.run(db.get_hedging_flow_series(symbol="SPY", timeframe="1min"))

    assert [q for q, _ in conn.fetch_calls] == [
        HEDGING_FLOW_CTE_ASYNCPG,
        HEDGING_FLOW_1MIN_CTE_ASYNCPG,
    ]


def test_one_minute_timeout_returns_empty_and_is_not_cached():
    class _SlowOnce(_CannedConn):
        async def fetch(self, query, *args):
            self.fetch_calls.append((query, args))
            if len(self.fetch_calls) == 1:
                raise asyncio.TimeoutError
            return self._fetch_rows

    conn = _SlowOnce(
        fetchval_sequence=_session_resolution() + _session_resolution(), fetch_rows=[_row(0)]
    )
    db = _make_db(conn)
    db._flow_series_1min_cache_ttl_seconds = 60.0

    assert asyncio.run(db.get_hedging_flow_series(symbol="SPY", timeframe="1min")) == []
    retry = asyncio.run(db.get_hedging_flow_series(symbol="SPY", timeframe="1min"))

    assert len(retry) == 1
    assert len(conn.fetch_calls) == 2


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


# timeframe=1min at the wire.
def test_http_default_timeframe_is_five_minutes(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [_row(0, call=1.0)])
        payload = client.get("/api/flow/hedging?symbol=SPY").json()

    assert payload["timeframe"] == "5min"
    assert mainmod.db_manager.get_hedging_flow_series.call_args.kwargs["timeframe"] == "5min"


def test_http_one_minute_bars(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    canned = [_row(1, call=2.0), _row(0, call=1.0)]

    with TestClient(app) as client:
        _attach(mainmod, canned)
        response = client.get("/api/flow/hedging?symbol=SPY&timeframe=1min")

    assert response.status_code == 200, response.text
    payload = response.json()
    assert mainmod.db_manager.get_hedging_flow_series.call_args.kwargs["timeframe"] == "1min"
    assert payload["timeframe"] == "1min"
    assert [(b["bar_start"], b["bar_end"]) for b in payload["bars"]] == [
        ("2026-04-24T13:31:00Z", "2026-04-24T13:32:00Z"),
        ("2026-04-24T13:30:00Z", "2026-04-24T13:31:00Z"),
    ]


def test_http_one_minute_smoothing_defaults_to_fifteen_minutes(
    monkeypatch: pytest.MonkeyPatch,
):
    """15 one-minute bars, not 3: the same 15 minutes as the 5-minute default.
    Asserted on the average itself, not only on the reported length."""
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    nets = [float(i) for i in range(20)]
    canned = [_row(i, call=n) for i, n in enumerate(nets)]
    canned.reverse()

    with TestClient(app) as client:
        _attach(mainmod, canned)
        payload = client.get("/api/flow/hedging?symbol=SPY&timeframe=1min").json()

    assert payload["smoothing_bars"] == 15
    bars = payload["bars"]  # newest first
    assert bars[0]["net_flow_ma_usd"] == pytest.approx(sum(nets[-15:]) / 15)
    assert bars[5]["net_flow_ma_usd"] == pytest.approx(sum(nets[:15]) / 15)
    assert bars[6]["net_flow_ma_usd"] is None


def test_http_explicit_smoothing_is_in_bars_of_the_timeframe(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    canned = [_row(i, call=float(i)) for i in range(5)]
    canned.reverse()

    with TestClient(app) as client:
        _attach(mainmod, canned)
        payload = client.get("/api/flow/hedging?symbol=SPY&timeframe=1min&smoothing=3").json()

    assert payload["smoothing_bars"] == 3
    assert payload["bars"][0]["net_flow_ma_usd"] == pytest.approx((2.0 + 3.0 + 4.0) / 3)


def test_http_smoothing_cap_is_two_hours_of_either_bar(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [])
        get = lambda q: client.get(f"/api/flow/hedging?symbol=SPY&{q}").status_code  # noqa: E731
        assert get("smoothing=24") == 200
        assert get("smoothing=25") == 422
        assert get("timeframe=1min&smoothing=120") == 200
        assert get("timeframe=1min&smoothing=121") == 422


def test_http_intervals_reaches_a_full_one_minute_session(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [])
        ok = client.get("/api/flow/hedging?symbol=SPY&timeframe=1min&intervals=406")
        too_many = client.get("/api/flow/hedging?symbol=SPY&timeframe=1min&intervals=407")

    assert ok.status_code == 200
    assert too_many.status_code == 422


def test_http_rejects_an_unsupported_timeframe(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [])
        assert client.get("/api/flow/hedging?symbol=SPY&timeframe=15min").status_code == 422


def test_http_empty_one_minute_session_reports_its_timeframe(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [])
        payload = client.get("/api/flow/hedging?symbol=SPY&timeframe=1min").json()

    assert payload["bars"] == []
    assert payload["timeframe"] == "1min"
    assert payload["smoothing_bars"] == 15
