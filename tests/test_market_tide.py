import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from src.api.market_tide import _capped_weights, calculate_market_tide
from src.api.database import DatabaseManager
from src.api.models import MarketTideResponse, MarketTideHistoryResponse
from src.tools.market_tide_healthcheck import _evaluate

ANCHOR = datetime(2026, 7, 29, 15, 30, tzinfo=timezone.utc)


def _row(symbol: str, flow: float, gamma: float, *, gross: float = 1_000_000) -> dict:
    return {
        "symbol": symbol,
        "gex_timestamp": ANCHOR - timedelta(minutes=1),
        "flow_timestamp": ANCHOR - timedelta(minutes=1),
        "signed_premium": flow,
        "gross_premium": gross,
        "flow_p95": 100.0,
        "net_gex_at_spot": gamma,
        "gamma_p95": 100.0,
    }


def test_negative_gamma_amplifies_directional_flow():
    positive_gamma = calculate_market_tide([_row("AAA", 100, 100)], anchor=ANCHOR)
    negative_gamma = calculate_market_tide([_row("AAA", 100, -100)], anchor=ANCHOR)

    assert negative_gamma["score"] > positive_gamma["score"] > 0
    assert negative_gamma["gamma_label"] == "amplifying"
    assert positive_gamma["gamma_label"] == "dampening"


def test_bearish_flow_remains_bearish_in_negative_gamma():
    result = calculate_market_tide([_row("AAA", -100, -100)], anchor=ANCHOR)

    assert result["score"] < 0
    assert result["label"] in {"bearish", "strong_bearish"}


def test_stale_inputs_withhold_score_and_report_participation():
    fresh = _row("FRESH", 100, 0)
    stale = _row("STALE", 100, 0)
    stale["flow_timestamp"] = ANCHOR - timedelta(minutes=11)

    result = calculate_market_tide([fresh, stale], anchor=ANCHOR)

    assert result["score"] is None
    assert result["label"] == "insufficient_data"
    assert result["participation_pct"] == 50.0
    assert result["stale_symbols"] == ["STALE"]


def test_quiet_symbol_with_fresh_chain_heartbeat_remains_eligible():
    quiet = _row("QUIET", 0, 25, gross=0)

    result = calculate_market_tide([quiet], anchor=ANCHOR)

    assert result["score"] == 0
    assert result["eligible_symbols"] == 1
    assert result["stale_symbols"] == []


def test_same_session_index_close_remains_eligible_after_hours():
    after_hours_anchor = datetime(2026, 7, 30, 1, 49, tzinfo=timezone.utc)
    index_close = datetime(2026, 7, 29, 19, 59, tzinfo=timezone.utc)
    row = _row("SPX", 50, -25)
    row["gex_timestamp"] = index_close
    row["flow_timestamp"] = index_close

    result = calculate_market_tide([row], anchor=after_hours_anchor)

    assert result["score"] is not None
    assert result["eligible_symbols"] == 1


def test_liquidity_weights_are_capped_with_sufficient_breadth():
    weights = _capped_weights([10_000.0] + [1.0] * 9)

    assert sum(weights) == pytest.approx(1.0)
    assert max(weights) <= 0.15


def test_response_contract_accepts_calculator_output():
    result = calculate_market_tide(
        [_row(f"S{i}", 100 if i < 5 else -50, -25) for i in range(10)],
        anchor=ANCHOR,
    )

    response = MarketTideResponse(**result)
    assert response.configured_symbols == 10
    assert response.eligible_symbols == 10
    assert response.leaders
    assert response.laggards


def test_market_tide_endpoint_is_registered_with_response_contract():
    from src.api.main import MarketTideWindow, app

    route = next(
        route for route in app.routes if getattr(route, "path", None) == "/api/flow/market-tide"
    )
    assert route.methods == {"GET"}
    assert route.response_model is MarketTideResponse

    window_param = next(param for param in route.dependant.query_params if param.name == "window")
    parsed, errors = window_param.validate("15", {}, loc=("query", "window"))
    assert errors == []
    assert parsed is MarketTideWindow.FIFTEEN_MINUTES


def test_healthcheck_identifies_missing_and_stale_upstreams():
    rows = [
        ("READY", ANCHOR, ANCHOR, None, 0, 10),
        ("NOCHAIN", ANCHOR, None, None, 0, 10),
        ("FLOWCOVERS", ANCHOR, ANCHOR - timedelta(minutes=20), ANCHOR, 1, 10),
        ("STALEGEX", ANCHOR - timedelta(minutes=11), ANCHOR, ANCHOR, 5, 10),
    ]

    statuses = _evaluate(rows, ANCHOR, timedelta(minutes=10))

    assert [item.status for item in statuses] == [
        "ready",
        "missing_flow_input",
        "ready",
        "stale_or_misaligned",
    ]


def test_database_query_binds_window_as_integer_interval():
    class FakeConnection:
        anchor_query = ""
        query = ""
        args = ()
        snapshot_query = ""

        async def fetchrow(self, query, *args):
            # No persisted snapshot → get_market_tide falls back to the live
            # compute path this test validates.
            self.snapshot_query = query
            return None

        async def fetchval(self, query):
            self.anchor_query = query
            return ANCHOR

        async def fetch(self, query, *args):
            self.query = query
            self.args = args
            return []

    class FakeAcquire:
        def __init__(self, connection):
            self.connection = connection

        async def __aenter__(self):
            return self.connection

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    connection = FakeConnection()
    database = DatabaseManager()
    database._acquire_connection = lambda: FakeAcquire(connection)

    asyncio.run(database.get_market_tide(window_minutes=15))

    assert "$2::text" not in connection.query
    assert "TIME '16:00'" in connection.anchor_query
    assert "AT TIME ZONE 'America/New_York'" in connection.anchor_query
    assert "make_interval(mins => $2::integer)" in connection.query
    assert connection.query.count("FROM flow_contract_facts") == 2
    assert "GREATEST(ch.flow_timestamp, ff.fact_timestamp)" in connection.query
    assert "symbol_anchors" in connection.query
    assert "f.timestamp > sa.symbol_anchor" in connection.query
    assert "FROM component_normalizer_cache" in connection.query
    assert "FROM gex_historical_stats" in connection.query
    assert "PERCENTILE_CONT" not in connection.query
    assert connection.args == (ANCHOR, 15)
    # The persisted snapshot is consulted first (frozen-at-close read).
    assert "FROM market_tide_snapshots" in connection.snapshot_query


class _AcquireCtx:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def test_get_market_tide_prefers_persisted_snapshot_over_live_compute():
    """When a snapshot exists the endpoint returns it and never computes live."""

    payload = {
        "timestamp": "2026-07-24T16:00:00+00:00",
        "score": 42.5,
        "label": "bullish",
        "flow_direction": 0.3,
        "gamma_regime": -0.2,
        "gamma_label": "amplifying",
        "bullish_breadth_pct": 60.0,
        "bearish_breadth_pct": 20.0,
        "neutral_breadth_pct": 20.0,
        "participation_pct": 88.0,
        "eligible_symbols": 8,
        "configured_symbols": 9,
        "stale_symbols": [],
        "leaders": [],
        "laggards": [],
    }

    class FakeConnection:
        computed = False

        async def fetchrow(self, query, *args):
            return {"payload": payload}

        async def fetchval(self, query):  # live-compute anchor — must not run
            self.computed = True
            return ANCHOR

        async def fetch(self, query, *args):  # live-compute rows — must not run
            self.computed = True
            return []

    connection = FakeConnection()
    database = DatabaseManager()
    database._acquire_connection = lambda: _AcquireCtx(connection)

    result = asyncio.run(database.get_market_tide(window_minutes=15))

    assert result == payload
    assert connection.computed is False
    # And it validates against the response contract the endpoint serves.
    assert MarketTideResponse(**result).score == 42.5


def test_market_tide_bucket_floors_to_five_minute_utc():
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    # 16:00 ET cash close lands on a clean 5-minute UTC boundary.
    close = datetime(2026, 7, 24, 16, 0, tzinfo=et)
    close_bucket = DatabaseManager._market_tide_bucket(close)
    assert int(close_bucket.timestamp()) % 300 == 0
    assert close_bucket == close  # already on a boundary → unchanged

    # An arbitrary intraday anchor floors down to its 5-minute bucket.
    mid = datetime(2026, 7, 24, 13, 7, 47, tzinfo=et)
    mid_bucket = DatabaseManager._market_tide_bucket(mid)
    assert int(mid_bucket.timestamp()) % 300 == 0
    assert mid_bucket.astimezone(et).strftime("%H:%M:%S") == "13:05:00"


def test_snapshot_payload_roundtrips_through_response_model():
    """The JSON persisted by the writer parses back into MarketTideResponse."""
    import json

    result = calculate_market_tide(
        [_row(f"S{i}", 100 if i < 5 else -50, -25) for i in range(10)],
        anchor=ANCHOR,
    )
    # Mirror _upsert_market_tide_snapshot's serialization.
    payload_obj = dict(result)
    payload_obj["timestamp"] = result["timestamp"].isoformat()
    payload = json.dumps(payload_obj, default=str)

    restored = json.loads(payload)
    model = MarketTideResponse(**restored)
    assert model.score == result["score"]
    assert model.timestamp == ANCHOR


def test_write_market_tide_snapshots_stores_only_publishable_windows():
    publishable = {"score": 30.0, "label": "bullish", "participation_pct": 80.0}
    withheld = {"score": None, "label": "insufficient_data", "participation_pct": 40.0}
    by_window = {5: publishable, 15: withheld, 30: publishable}

    upserted: list[int] = []

    async def fake_compute(conn, anchor, window_minutes):
        return by_window[window_minutes]

    async def fake_upsert(conn, anchor, window_minutes, result):
        upserted.append(window_minutes)

    database = DatabaseManager()
    database._acquire_connection = lambda: _AcquireCtx(object())
    database._compute_market_tide = fake_compute
    database._upsert_market_tide_snapshot = fake_upsert

    summary = asyncio.run(database.write_market_tide_snapshots(windows=[5, 15, 30], anchor=ANCHOR))

    assert summary["written"] == [5, 30]
    assert summary["skipped"] == [15]
    assert upserted == [5, 30]  # the withheld window is never persisted
    assert summary["anchor"] == ANCHOR


def test_write_market_tide_snapshots_dry_run_writes_nothing():
    async def fake_compute(conn, anchor, window_minutes):
        return {"score": 12.0, "label": "bullish", "participation_pct": 90.0}

    async def fail_upsert(conn, anchor, window_minutes, result):
        raise AssertionError("dry_run must not upsert")

    database = DatabaseManager()
    database._acquire_connection = lambda: _AcquireCtx(object())
    database._compute_market_tide = fake_compute
    database._upsert_market_tide_snapshot = fail_upsert

    summary = asyncio.run(
        database.write_market_tide_snapshots(windows=[5], anchor=ANCHOR, dry_run=True)
    )
    assert summary["written"] == [5]  # would-write, but nothing persisted


def test_refresh_tool_gates_to_cash_session():
    from src.tools import market_tide_refresh as refresh
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    friday = lambda h, m: datetime(2026, 7, 24, h, m, tzinfo=et)  # noqa: E731 (2026-07-24 is a Fri)
    assert refresh._within_refresh_window(friday(10, 0)) is True
    assert refresh._within_refresh_window(friday(9, 29)) is False  # before the open
    assert refresh._within_refresh_window(friday(16, 10)) is True  # close grace
    assert refresh._within_refresh_window(friday(16, 11)) is False  # past the grace
    assert refresh._within_refresh_window(datetime(2026, 7, 25, 10, 0, tzinfo=et)) is False  # Sat


def test_backfill_anchors_and_trading_days():
    from datetime import date

    from src.tools import market_tide_backfill as backfill

    # Close-only default → exactly one 16:00 ET anchor.
    closes = backfill._session_anchors(date(2026, 7, 24), 0)
    assert len(closes) == 1
    assert closes[0].astimezone(closes[0].tzinfo).strftime("%H:%M") == "16:00"

    # Intraday cadence includes the 09:30 open and always ends on the close.
    hourly = backfill._session_anchors(date(2026, 7, 24), 60)
    labels = [a.strftime("%H:%M") for a in (a.astimezone(closes[0].tzinfo) for a in hourly)]
    assert labels[0] == "09:30"
    assert labels[-1] == "16:00"

    # Weekends are skipped: Fri 2026-07-24 .. Mon 2026-07-27 → just Fri + Mon.
    days = list(backfill._trading_days(date(2026, 7, 24), date(2026, 7, 27)))
    assert days == [date(2026, 7, 24), date(2026, 7, 27)]


def test_components_include_every_eligible_symbol():
    """The full flow-vs-gamma map needs all eligible names, not just top/bottom 5."""
    result = calculate_market_tide(
        [_row(f"S{i}", 100 if i < 3 else -100, -25) for i in range(6)],
        anchor=ANCHOR,
    )
    assert {c["symbol"] for c in result["components"]} == {f"S{i}" for i in range(6)}
    assert len(result["components"]) == 6  # leaders/laggards still cap at 5 each
    assert MarketTideResponse(**result).components[0].symbol


def test_market_tide_history_route_is_registered():
    from src.api.main import app

    route = next(
        r for r in app.routes if getattr(r, "path", None) == "/api/flow/market-tide/history"
    )
    assert route.methods == {"GET"}
    assert route.response_model is MarketTideHistoryResponse


def test_get_market_tide_history_intraday_reads_snapshots():
    from datetime import date

    day = date(2026, 7, 29)
    ts0 = datetime(2026, 7, 29, 19, 30, tzinfo=timezone.utc)
    rows = [
        {  # payload persisted as a JSON string (asyncpg default)
            "session_date": day,
            "snapshot_ts": ts0,
            "score": 5.0,
            "label": "bullish",
            "participation_pct": 100.0,
            "payload": '{"flow_direction":0.1,"gamma_regime":-0.8}',
        },
        {  # payload already decoded to a dict
            "session_date": day,
            "snapshot_ts": ts0 + timedelta(minutes=5),
            "score": 8.0,
            "label": "bullish",
            "participation_pct": 100.0,
            "payload": {"flow_direction": 0.2, "gamma_regime": -0.7},
        },
    ]

    class FakeConn:
        async def fetchval(self, query, *args):
            return day

        async def fetch(self, query, *args):
            return rows

    database = DatabaseManager()
    database._acquire_connection = lambda: _AcquireCtx(FakeConn())

    out = asyncio.run(database.get_market_tide_history(window_minutes=15, mode="intraday"))

    assert out["window_minutes"] == 15 and out["mode"] == "intraday"
    assert [p["score"] for p in out["points"]] == [5.0, 8.0]
    assert out["points"][0]["flow_direction"] == 0.1  # parsed from JSON string
    assert out["points"][1]["gamma_regime"] == -0.7  # read from dict
    assert MarketTideHistoryResponse(**out).points[0].label == "bullish"
