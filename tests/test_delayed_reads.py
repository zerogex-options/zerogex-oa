"""``delay_minutes``: the free surfaces' delay has to be real.

The website's free, no-login surfaces (the gamma-levels pages, the embed
widget, llms.txt, /mcp and the public /chart) all say the data is delayed about
15 minutes. They were delayed only by a 15-minute page cache, so a visitor saw
anything from seconds-old to 15-minute-old data: on 2026-09-29 llms.txt served
SPY "as of 3:57 PM" at 4:03 PM. The reads they make now take ``delay_minutes``
and answer from the database with the newest data at least that old, whatever
the caller caches. See src/api/delayed_read.py.

Two promises are pinned here, and the second matters as much as the first:

* a delayed read never returns a bucket that closed less than the delay ago,
  on every read the free surfaces make, including the ES/NQ spot the futures
  middleware substitutes after the handler has answered;
* a live read makes exactly the call it made before: same SQL, same arguments,
  same caching. The paid dashboard runs on these reads.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from src.api import database as dbmod
from src.api import futures_middleware as fm
from src.api import main
from src.api.delayed_read import MAX_DELAY_MINUTES, delayed_ceiling
from src.jobs import futures_projection as fp
from src.jobs.futures_projection import FuturesBasis

# 16:03:51 ET on Tuesday 2026-09-29: the minute llms.txt was caught serving a
# 3:57 PM snapshot as "delayed".
NOW = datetime(2026, 9, 29, 20, 3, 51, tzinfo=timezone.utc)
AS_OF = delayed_ceiling(15, now=NOW)


# ── The ceiling ──────────────────────────────────────────────────────────────


def test_no_delay_is_the_live_read():
    assert delayed_ceiling(0, now=NOW) is None
    assert delayed_ceiling(-5, now=NOW) is None


def test_the_ceiling_sits_one_bucket_behind_the_delay():
    assert AS_OF == NOW - timedelta(minutes=16)


@pytest.mark.parametrize("second", [0, 1, 30, 59])
def test_the_newest_bucket_a_delayed_read_can_return_closed_a_full_delay_ago(second):
    """A bucket stamped T carries prints up to T + 59 s, so it is the bucket's
    END that has to be ``delay`` old. And the delay is not overshot: the next
    bucket, which the ceiling excludes, had not closed ``delay`` ago."""
    now = NOW.replace(second=second)
    ceiling = delayed_ceiling(15, now=now)
    newest = ceiling.replace(second=0, microsecond=0)
    assert now - (newest + timedelta(minutes=1)) >= timedelta(minutes=15)
    assert now - (newest + timedelta(minutes=2)) < timedelta(minutes=15)


def test_the_route_parameter_accepts_zero_to_a_day_and_nothing_else():
    probe = FastAPI()

    @probe.get("/probe")
    async def _probe(delay_minutes: main._DelayMinutes = 0):
        return {"delay_minutes": delay_minutes}

    with TestClient(probe) as client:
        assert client.get("/probe").json() == {"delay_minutes": 0}
        assert client.get("/probe?delay_minutes=15").json() == {"delay_minutes": 15}
        assert client.get("/probe?delay_minutes=-1").status_code == 422
        assert client.get(f"/probe?delay_minutes={MAX_DELAY_MINUTES + 1}").status_code == 422


# ── The database reads ───────────────────────────────────────────────────────


class _Conn:
    """Records every statement and answers from queues (None when empty)."""

    def __init__(self, rows=None, vals=None, fetched=None):
        self.calls = []
        self._rows = list(rows or [])
        self._vals = list(vals or [])
        self._fetched = list(fetched or [])

    async def fetchrow(self, query, *args, **kwargs):
        self.calls.append((query, args))
        return self._rows.pop(0) if self._rows else None

    async def fetchval(self, query, *args, **kwargs):
        self.calls.append((query, args))
        return self._vals.pop(0) if self._vals else None

    async def fetch(self, query, *args, **kwargs):
        self.calls.append((query, args))
        return self._fetched.pop(0) if self._fetched else []


def _db(conn):
    db = dbmod.DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return db


def test_gex_summary_live_read_is_unchanged():
    conn = _Conn()
    asyncio.run(_db(conn).get_latest_gex_summary("SPY"))
    probe, (main_sql, args) = conn.calls
    assert "-- newest-row probe" in probe[0]
    assert "$3" not in main_sql
    assert args == ("SPY", dbmod.DEFAULT_WALL_LADDER_DEPTH)


def test_gex_summary_delayed_read_caps_the_snapshot_and_the_spot():
    conn = _Conn(rows=[{"timestamp": AS_OF, "spot_price": 660.0, "call_wall": 670.0}])
    db = _db(conn)
    payload = asyncio.run(db.get_latest_gex_summary("spy", as_of=AS_OF))

    [(sql, args)] = conn.calls  # no newest-row probe: that is about the live row
    assert "gs.timestamp <= $3" in sql
    assert "uq.timestamp <= $3" in sql
    assert args == ("SPY", dbmod.DEFAULT_WALL_LADDER_DEPTH, AS_OF)
    assert "call_walls" in payload and "put_walls" in payload  # assembled like a live row
    # Nothing the live read relies on was touched.
    assert db._latest_gex_summary_served_ts == {}
    assert db._cache_get("latest_gex_summary:SPY") is None


def test_gex_profile_delayed_read_is_capped_and_never_cached():
    conn = _Conn()
    asyncio.run(_db(conn).get_latest_gex_profile("SPY"))
    asyncio.run(_db(conn).get_latest_gex_profile("SPY", as_of=AS_OF))
    (live_sql, live_args), (delayed_sql, delayed_args) = conn.calls
    assert "$2" not in live_sql and live_args == ("SPY",)
    assert "gp.timestamp <= $2" in delayed_sql and delayed_args == ("SPY", AS_OF)


def test_quote_delayed_read_caps_the_bar_and_the_days_volume():
    conn = _Conn()
    asyncio.run(_db(conn).get_latest_quote("SPY"))
    asyncio.run(_db(conn).get_latest_quote("SPY", as_of=AS_OF))
    (live_sql, live_args), (delayed_sql, delayed_args) = conn.calls
    assert "$2" not in live_sql and "v.timestamp <= lq.timestamp" not in live_sql
    assert live_args == ("SPY",)
    assert "uq.timestamp <= $2" in delayed_sql
    # The day's volume would otherwise count every live bar after the delayed one.
    assert "v.timestamp <= lq.timestamp" in delayed_sql
    assert delayed_args == ("SPY", AS_OF)


def test_session_closes_delayed_read_takes_now_from_the_ceiling():
    """At 16:05 a live read already has today's close. A read delayed to 15:49
    must not: that session had not closed yet at the instant it describes."""
    conn = _Conn()
    asyncio.run(_db(conn).get_session_closes("SPY"))
    live = conn.calls[:]
    conn.calls.clear()
    asyncio.run(_db(conn).get_session_closes("SPY", as_of=AS_OF))

    (live_sql, live_args), (live_fallback, live_fallback_args) = live
    assert "NOW()" in live_sql and "$2" not in live_sql and live_args == ("SPY",)
    assert "$2" not in live_fallback and live_fallback_args == ("SPY",)

    (sql, args), (fallback, fallback_args) = conn.calls
    assert "NOW()" not in sql
    assert sql.count("$2::timestamptz") == 4
    assert args == ("SPY", AS_OF)
    assert "timestamp <= $2" in fallback and fallback_args == ("SPY", AS_OF)


def test_future_quote_delayed_read_caps_the_bar_and_its_reference():
    start = datetime(2026, 9, 28, 20, 0, tzinfo=timezone.utc)
    conn = _Conn()
    asyncio.run(_db(conn).get_latest_future_quote("SPX", start))
    asyncio.run(_db(conn).get_latest_future_quote("SPX", start, as_of=AS_OF))
    (live_sql, live_args), (delayed_sql, delayed_args) = conn.calls
    assert "$3" not in live_sql and live_args == ("SPX", start)
    assert delayed_sql.count("timestamp <= $3") == 2
    assert delayed_args == ("SPX", start, AS_OF)


def test_futures_session_closes_delayed_read_takes_now_from_the_ceiling():
    conn = _Conn()
    asyncio.run(_db(conn).get_futures_session_closes("SPX"))
    asyncio.run(_db(conn).get_futures_session_closes("SPX", as_of=AS_OF))
    (live_sql, live_args), (delayed_sql, delayed_args) = conn.calls
    assert "NOW()" in live_sql and live_args == ("SPX",)
    assert "NOW()" not in delayed_sql and delayed_sql.count("$2::timestamptz") == 4
    assert delayed_args == ("SPX", AS_OF)


def test_technicals_delayed_read_stops_every_bar_and_the_opening_range_at_the_ceiling():
    # A ceiling at 09:45 ET falls inside the 09:30-09:59 opening range, so the
    # range must stop there too, not run on to 09:59 on live bars.
    as_of = datetime(2026, 9, 29, 13, 45, tzinfo=timezone.utc)
    conn = _Conn(
        rows=[{"asset_type": "ETF"}],
        vals=[as_of - timedelta(seconds=45), date(2026, 9, 29)],
    )
    payload = asyncio.run(_db(conn).get_technicals_timeseries("SPY", as_of=as_of))

    _, (latest_sql, latest_args), (orb_sql, orb_args), (_, query_args) = conn.calls
    assert "timestamp <= $2" in latest_sql and latest_args == ("SPY", as_of)
    assert "timestamp <= $2" in orb_sql and orb_args == ("SPY", as_of)
    bars_end, orb_end = query_args[4], query_args[6]
    assert bars_end == as_of
    assert orb_end == as_of
    # The payload still reports the session's own boundaries.
    assert payload["session_end_et"] == "2026-09-29T20:00:00-04:00"


def test_technicals_live_read_is_unchanged():
    latest = datetime(2026, 9, 29, 19, 58, tzinfo=timezone.utc)
    conn = _Conn(rows=[{"asset_type": "ETF"}], vals=[latest, date(2026, 9, 29)])
    asyncio.run(_db(conn).get_technicals_timeseries("SPY"))
    _, (latest_sql, latest_args), (orb_sql, orb_args), (_, query_args) = conn.calls
    assert "$2" not in latest_sql and latest_args == ("SPY",)
    assert "$2" not in orb_sql and orb_args == ("SPY",)
    assert query_args[4].isoformat() == "2026-09-29T20:00:00-04:00"


@pytest.mark.parametrize("symbol,param", [("SPY", "$4"), ("SPX", "$5")])
def test_strike_profile_delayed_read_caps_the_window_anchor(symbol, param):
    """One predicate on the ``latest`` anchor is the whole ceiling: the floor,
    the reps, the OHLC and the strike probe are all bounded by that anchor.
    Cash indices bind the holiday list first, so the ceiling moves along."""
    conn = _Conn()
    db = _db(conn)
    asyncio.run(db.get_strike_profile_timeseries(symbol, "5min", 3, None, as_of=AS_OF))
    [(sql, args)] = conn.calls
    latest_cte = sql.split("bounds AS")[0]
    assert f"AND timestamp <= {param}::timestamptz" in latest_cte
    assert args[-1] == AS_OF
    assert db._cache_get(dbmod._strike_profile_ts_cache_key(symbol, "5min", 3, None)) is None


def test_strike_profile_live_read_has_no_ceiling():
    conn = _Conn()
    asyncio.run(_db(conn)._get_strike_profile_timeseries_uncached("SPY", "5min", 3))
    [(sql, args)] = conn.calls
    assert "timestamp <=" not in sql.split("bounds AS")[0]
    assert len(args) == 3  # symbol, window_units, expirations


# ── The routes ───────────────────────────────────────────────────────────────


class _RecordingDB:
    """Answers every read with ``answer`` and records the kwargs it was given."""

    def __init__(self, answer):
        self.answer = answer
        self.calls = {}

    def __getattr__(self, name):
        async def method(*args, **kwargs):
            self.calls[name] = (args, kwargs)
            return self.answer

        return method


def _call(coro_fn, db, **kwargs):
    with (
        patch.object(main, "_db", lambda: db),
        patch.object(main, "delayed_ceiling", lambda minutes: AS_OF if minutes > 0 else None),
    ):
        return asyncio.run(coro_fn(**kwargs))


_ROUTES = [
    (main.get_gex_summary, {"symbol": "SPY"}, "get_latest_gex_summary"),
    (main.get_gex_profile, {"symbol": "SPY"}, "get_latest_gex_profile"),
    (main.get_session_closes, {"symbol": "SPY"}, "get_session_closes"),
    (main.get_technicals, {"symbol": "SPY", "intervals": None}, "get_technicals_timeseries"),
    (
        main.get_strike_profile_timeseries,
        {"symbol": "SPY", "timeframe": "5min", "window_units": 3, "expirations": "all"},
        "get_strike_profile_timeseries",
    ),
]


def _call_answering_none(route, kwargs):
    """Call a route whose reads all answer None; that is a 404 for the
    single-object routes, and the database call is all these tests read."""
    db = _RecordingDB(None)
    try:
        _call(route, db, **kwargs)
    except HTTPException as exc:
        assert exc.status_code == 404
    return db


@pytest.mark.parametrize("route,kwargs,db_method", _ROUTES)
def test_a_live_route_call_passes_no_ceiling(route, kwargs, db_method):
    db = _call_answering_none(route, kwargs)
    assert "as_of" not in db.calls[db_method][1]


@pytest.mark.parametrize("route,kwargs,db_method", _ROUTES)
def test_a_delayed_route_call_passes_its_ceiling(route, kwargs, db_method):
    db = _call_answering_none(route, {**kwargs, "delay_minutes": 15})
    assert db.calls[db_method][1]["as_of"] == AS_OF


def _historical(db, **overrides):
    kwargs = dict(
        symbol="SPY",
        start_date=None,
        end_date=None,
        window_units=180,
        timeframe="5min",
        allow_futures=False,
        delay_minutes=15,
    )
    kwargs.update(overrides)
    return _call(main.get_historical_quotes, db, **kwargs)


def test_historical_bars_end_at_the_ceiling():
    db = _RecordingDB([])
    _historical(db)
    assert db.calls["get_historical_quotes"][0][2] == AS_OF


def test_historical_bars_keep_an_earlier_end_date_and_cap_a_later_one():
    earlier = AS_OF - timedelta(hours=1)
    db = _RecordingDB([])
    _historical(db, end_date=earlier.isoformat())
    assert db.calls["get_historical_quotes"][0][2] == earlier

    # A naive end_date reads as UTC.
    db = _RecordingDB([])
    _historical(db, end_date=(AS_OF + timedelta(minutes=5)).replace(tzinfo=None).isoformat())
    assert db.calls["get_historical_quotes"][0][2] == AS_OF

    db = _RecordingDB([])
    _historical(db, delay_minutes=0)
    assert db.calls["get_historical_quotes"][0][2] is None


def test_a_delayed_quote_is_labeled_as_of_its_ceiling_and_leaves_the_live_tracker_alone():
    # Ceiling 15:54 ET on a Tuesday: the market was open then, whatever "now" is.
    as_of = datetime(2026, 9, 29, 19, 54, tzinfo=timezone.utc)
    quote = {
        "timestamp": as_of - timedelta(seconds=40),
        "symbol": "SPY",
        "open": 660.1,
        "high": 660.4,
        "low": 659.9,
        "close": 660.2,
        "cumulative_daily_volume": 1000,
        "asset_type": "ETF",
    }

    class _DB(_RecordingDB):
        async def has_todays_close_landed(self, *args):  # pragma: no cover
            raise AssertionError("the close-landed gate watches the live tape")

    db = _DB(quote)
    trackers = dict(main._soft_close_trackers)
    with (
        patch.object(main, "_db", lambda: db),
        patch.object(main, "delayed_ceiling", lambda minutes: as_of),
    ):
        result = asyncio.run(main.get_current_quote(symbol="SPY", delay_minutes=15))
    assert result.session == "open"
    assert float(result.close) == 660.2
    assert db.calls["get_latest_quote"][1] == {"as_of": as_of}
    assert main._soft_close_trackers == trackers


def test_a_delayed_futures_quote_is_stale_only_against_its_ceiling():
    """A 16-minute-old bar is exactly what a delayed read asks for, so it is
    not stale. ``data_age_seconds`` still reports the true age."""
    bar = {
        "timestamp": AS_OF - timedelta(seconds=30),
        "future_symbol": "@ES",
        "open": 6650.0,
        "high": 6651.0,
        "low": 6649.0,
        "close": 6650.5,
        "up_volume": 10,
        "down_volume": 8,
        "volume": 18,
    }
    db = _RecordingDB(bar)
    with patch.object(main, "_db", lambda: db):
        quote = asyncio.run(main._native_futures_quote("ES", "SPX", AS_OF))
    assert quote.stale is False
    assert quote.data_age_seconds >= 16 * 60
    args, kwargs = db.calls["get_latest_future_quote"]
    assert kwargs == {"as_of": AS_OF}
    assert args[1] == main.current_cash_close_reference(AS_OF)


# The last minutes before the calendar moves @ES to December (Fri 2026-09-11
# 00:00 UTC, seven days before the Sep 18 expiry). Every real "now" this suite
# runs at is past it, so a September label can only have come from the ceiling.
PRE_ROLL_CEILING = datetime(2026, 9, 10, 23, 50, tzinfo=timezone.utc)


def test_a_delayed_futures_quote_names_the_contract_in_force_at_its_ceiling():
    """The quote and the summary sit side by side on a free page. Labeled at
    the same instant, the chart's chip and the levels card can never name two
    different contracts for the same delayed read."""
    assert main.contract_display_fields is fp.contract_display_fields
    bar = {
        "timestamp": PRE_ROLL_CEILING - timedelta(seconds=30),
        "future_symbol": "@ES",
        "open": 6650.0,
        "high": 6651.0,
        "low": 6649.0,
        "close": 6650.5,
    }
    with patch.object(main, "_db", lambda: _RecordingDB(bar)):
        delayed = asyncio.run(main._native_futures_quote("ES", "SPX", PRE_ROLL_CEILING))
        live = asyncio.run(main._native_futures_quote("ES", "SPX", None))
    assert delayed.data_contract == "ESU26"
    assert delayed.data_contract_expiry == date(2026, 9, 18)
    # A live read still names the contract in force now.
    assert live.data_contract == fp.active_contract_code("@ES")


def test_the_delayed_parameter_is_mirrored_onto_v2():
    schema = main.app.openapi()
    for path in (
        "/api/gex/summary",
        "/api/v2/gex/summary",
        "/api/gex/profile",
        "/api/gex/strike-profile-timeseries",
        "/api/market/quote",
        "/api/market/session-closes",
        "/api/market/historical",
        "/api/technicals",
    ):
        params = {p["name"]: p for p in schema["paths"][path]["get"]["parameters"]}
        assert params["delay_minutes"]["schema"]["minimum"] == 0, path
        assert params["delay_minutes"]["schema"]["maximum"] == MAX_DELAY_MINUTES, path


# ── ES / NQ through the futures middleware ───────────────────────────────────

BASIS = FuturesBasis(
    index_symbol="SPX",
    futures_symbol="ES",
    ratio=1.0067,
    source="carry",
    observed_at=NOW,
    sample_count=0,
    feed_symbol="@ES",
)


def _es_client(monkeypatch, seen):
    async def resolve(db, symbol, **kwargs):
        seen["basis_at"] = kwargs.get("at")
        return BASIS

    async def live_spot(index_symbol):
        seen["live_spot"] = True
        return 6700.0

    async def delayed_spot(index_symbol, ceiling):
        seen["delayed_spot_at"] = ceiling
        return 6650.25

    monkeypatch.setattr(fm, "resolve_basis", resolve)
    monkeypatch.setattr(fm, "_live_futures_spot", live_spot)
    monkeypatch.setattr(fm, "_delayed_futures_spot", delayed_spot)
    monkeypatch.setattr(fm, "_db_manager", lambda: object())
    monkeypatch.setattr(fm, "delayed_ceiling", lambda minutes: AS_OF if minutes > 0 else None)

    async def summary(request):
        seen["query"] = dict(request.query_params)
        return JSONResponse({"symbol": "SPX", "spot_price": 6600.0, "call_wall": 6700.0})

    app = Starlette(routes=[Route("/api/gex/summary", summary)])
    return TestClient(fm.FuturesProjectionMiddleware(app))


def test_a_delayed_es_read_substitutes_the_print_at_its_ceiling_not_the_live_one(monkeypatch):
    seen: dict = {}
    body = _es_client(monkeypatch, seen).get("/api/gex/summary?symbol=ES&delay_minutes=15").json()

    # The handler got the delay, on the index it actually reads.
    assert seen["query"] == {"symbol": "SPX", "delay_minutes": "15"}
    assert seen["basis_at"] == AS_OF
    assert seen["delayed_spot_at"] == AS_OF
    assert "live_spot" not in seen
    assert body["spot_price"] == 6650.25


def test_a_live_es_read_still_substitutes_the_live_print(monkeypatch):
    seen: dict = {}
    body = _es_client(monkeypatch, seen).get("/api/gex/summary?symbol=ES").json()
    assert seen["basis_at"] is None
    assert seen["live_spot"] is True
    assert "delayed_spot_at" not in seen
    assert body["spot_price"] == 6700.0


def test_a_delayed_es_summary_names_the_contract_at_its_ceiling(monkeypatch):
    """The free ES page reads this summary 15 minutes delayed. Its contract is
    the one in force at the instant the numbers describe, like its basis and
    its spot: the ceiling, not now."""
    seen: dict = {}
    client = _es_client(monkeypatch, seen)
    monkeypatch.setattr(
        fm, "delayed_ceiling", lambda minutes: PRE_ROLL_CEILING if minutes > 0 else None
    )
    body = client.get("/api/gex/summary?symbol=ES&delay_minutes=15").json()
    assert seen["basis_at"] == PRE_ROLL_CEILING
    assert body["data_contract"] == "ESU26"
    assert body["data_contract_expiry"] == "2026-09-18"


@pytest.mark.parametrize(
    "query,expected",
    [
        ("symbol=ES", None),
        ("symbol=ES&delay_minutes=0", None),
        ("symbol=ES&delay_minutes=15", "ceiling"),
        ("symbol=ES&delay_minutes=abc", None),
        (f"symbol=ES&delay_minutes={MAX_DELAY_MINUTES + 1}", None),
        # The route reads the last value, so the middleware does too.
        ("symbol=ES&delay_minutes=0&delay_minutes=15", "ceiling"),
        ("symbol=ES&delay_minutes=15&delay_minutes=0", None),
    ],
)
def test_the_middleware_reads_the_delay_the_way_the_route_does(query, expected):
    scope = {"query_string": query.encode()}
    with patch.object(fm, "delayed_ceiling", lambda minutes: AS_OF if minutes > 0 else None):
        got = fm._request_delay_ceiling(scope)
    assert got == (AS_OF if expected == "ceiling" else None)


def test_the_delayed_spot_reads_the_bar_at_or_before_the_ceiling(monkeypatch):
    calls = {}

    class _Mgr:
        async def get_latest_future_quote(self, index_symbol, session_start=None, **kwargs):
            calls["args"] = (index_symbol, session_start, kwargs)
            return {"close": 6650.25}

    # By path, not on the module object imported above: the helper imports
    # src.api.main when called, and other tests reload that module.
    monkeypatch.setattr("src.api.main.db_manager", _Mgr())
    spot = asyncio.run(fm._delayed_futures_spot("SPX", AS_OF))
    assert spot == 6650.25
    index_symbol, session_start, kwargs = calls["args"]
    assert index_symbol == "SPX"
    assert session_start == main.current_cash_close_reference(AS_OF)
    assert kwargs == {"as_of": AS_OF}
