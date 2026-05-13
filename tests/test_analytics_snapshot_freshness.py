"""Tests for ``AnalyticsEngine._get_snapshot`` last-quote semantics.

The analytics snapshot must reflect each option contract's most recent
quote regardless of how old that quote is, only rolling a contract off
once it has cleared its 16:15 ET expiry-day settlement.  These tests pin
down that behavior so we don't reintroduce the time-based staleness
filter that previously emptied the chain after hours.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytz

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")


def _row(option_symbol, strike, expiration, option_type, quote_ts, *, gamma=0.01, oi=100):
    """Construct a 15-tuple matching the SELECT column order in ``_get_snapshot``."""
    return (
        option_symbol,
        strike,
        expiration,
        option_type,
        1.0,  # last
        0.99,  # bid
        1.01,  # ask
        10,  # volume
        oi,  # open_interest
        0.5,  # delta
        gamma,
        -0.05,  # theta
        0.1,  # vega
        0.2,  # implied_volatility
        quote_ts,
    )


def _mock_db_connection(latest_ts, underlying_price, option_rows):
    """Return a context-manager-compatible mock that scripts the three
    sequential queries ``_get_snapshot`` issues."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (latest_ts,),  # query 1: latest option_chains timestamp
        (underlying_price,),  # query 2: underlying close
    ]
    cursor.fetchall.return_value = option_rows  # query 3: option rows

    conn = MagicMock()
    conn.cursor.return_value = cursor

    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, cursor


def test_old_quote_for_contract_is_still_returned():
    """A contract whose only quote is 30 minutes old must remain in the snapshot."""
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 18, 0)).astimezone(timezone.utc)
    old_quote_ts = snapshot_ts - timedelta(minutes=30)
    expiration = (snapshot_ts.astimezone(ET).date()) + timedelta(days=7)

    rows = [_row("SPY260520C00500000", 500.0, expiration, "C", old_quote_ts)]
    cm, _ = _mock_db_connection(snapshot_ts, 500.0, rows)

    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()

    assert result is not None
    assert len(result["options"]) == 1
    assert result["options"][0]["option_symbol"] == "SPY260520C00500000"


def test_expiration_filter_excludes_yesterday_expirations_when_before_1615():
    engine = AnalyticsEngine(underlying="SPY")
    # 14:00 ET (before 16:15) — yesterday's contracts should already be rolled off.
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 14, 0)).astimezone(timezone.utc)
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    # The third execute() carries the min_expiration arg.
    options_call = cursor.execute.call_args_list[2]
    params = options_call[0][1]
    min_expiration = params[3]
    # Before 16:15 ET: min_expiration = yesterday => today's expirations still pass.
    assert min_expiration == ET.localize(datetime(2026, 5, 12)).date()


def test_expiration_filter_rolls_off_today_after_1615():
    engine = AnalyticsEngine(underlying="SPY")
    # 16:30 ET (after 16:15) — today's expirations should be rolled off.
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 16, 30)).astimezone(timezone.utc)
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    options_call = cursor.execute.call_args_list[2]
    params = options_call[0][1]
    min_expiration = params[3]
    # After 16:15 ET: min_expiration = today => today's expirations are excluded.
    assert min_expiration == ET.localize(datetime(2026, 5, 13)).date()


def test_lookback_uses_configured_hours_not_minutes():
    engine = AnalyticsEngine(underlying="SPY")
    engine.snapshot_lookback_hours = 48
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 10, 0)).astimezone(timezone.utc)
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    options_call = cursor.execute.call_args_list[2]
    params = options_call[0][1]
    lookback_start = params[2]
    expected = snapshot_ts - timedelta(hours=48)
    assert lookback_start == expected


def test_snapshot_query_includes_expiration_clause():
    """The expiration roll-off must be enforced in SQL, not Python — otherwise
    expired contracts could leak in via the per-contract DISTINCT ON."""
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 12, 0)).astimezone(timezone.utc)
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    options_sql = cursor.execute.call_args_list[2][0][0]
    assert "oc.expiration >" in options_sql


def test_engine_no_longer_carries_freshness_attribute():
    """Guard against reintroducing the seconds-based staleness filter."""
    engine = AnalyticsEngine(underlying="SPY")
    assert not hasattr(engine, "snapshot_freshness_seconds")
    assert not hasattr(engine, "snapshot_lookback_minutes")


def test_default_lookback_hours_covers_long_weekend():
    """96h default must reach across a 3-day weekend (Memorial Day, etc.)."""
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_lookback_hours >= 72


def test_lookback_hours_env_var_overrides_default(monkeypatch):
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "24")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_lookback_hours == 24


def test_expiration_boundary_at_exactly_1615_rolls_off():
    """At precisely 16:15 ET, today's expirations should be considered rolled off."""
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 16, 15)).astimezone(timezone.utc)
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    options_call = cursor.execute.call_args_list[2]
    min_expiration = options_call[0][1][3]
    # 16:15 is the boundary; per the rule (ts.time() < 16:15) is False, so roll off today.
    assert min_expiration == ET.localize(datetime(2026, 5, 13)).date()
