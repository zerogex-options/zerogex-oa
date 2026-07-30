import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from src.api.market_tide import _capped_weights, calculate_market_tide
from src.api.database import DatabaseManager
from src.api.models import MarketTideResponse
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
