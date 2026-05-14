"""Tests for ``AnalyticsEngine._get_snapshot`` cold-start lookback behavior.

The first cycle after process start uses a wider lookback window (default 96h)
to bridge weekend / overnight gaps; every subsequent cycle uses the steady-state
window (default 2h).  These tests pin down that the cold-start lookback is
consumed exactly once per worker -- even if the cold-start query itself fails
-- so a slow first cycle can never wedge the cycle loop indefinitely.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import psycopg2
import pytz

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")


def _row(option_symbol, strike, expiration, option_type, quote_ts, *, gamma=0.01, oi=100):
    return (
        option_symbol,
        strike,
        expiration,
        option_type,
        1.0,
        0.99,
        1.01,
        10,
        oi,
        0.5,
        gamma,
        -0.05,
        0.1,
        0.2,
        quote_ts,
    )


def _mock_db_connection(latest_ts, underlying_price, option_rows, raise_on_step3=False):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(latest_ts,), (underlying_price,)]
    if raise_on_step3:
        # Step-3 cursor.execute raises; step-1 and step-2 return normally so
        # fetchone resolves underlying_ts + price before the failure point.
        call_count = {"n": 0}

        def execute_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 3:
                raise psycopg2.errors.QueryCanceled("canceling statement due to statement timeout")
            return None

        cursor.execute.side_effect = execute_side_effect
    cursor.fetchall.return_value = option_rows

    conn = MagicMock()
    conn.cursor.return_value = cursor

    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, cursor


def _step3_lookback_start(cursor):
    """Return the lookback_start parameter passed to the 3rd cursor.execute call."""
    third_call = cursor.execute.call_args_list[2]
    params = third_call.args[1] if len(third_call.args) >= 2 else third_call.kwargs["vars"]
    # _get_snapshot passes (self.db_symbol, timestamp, lookback_start, min_expiration)
    return params[2]


def test_first_cycle_uses_cold_start_lookback_then_switches_to_steady_state(monkeypatch):
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", "96")
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = ET.localize(datetime(2026, 5, 14, 13, 0)).astimezone(timezone.utc)
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", snapshot_ts)]

    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm):
        first = engine._get_snapshot()
    assert first is not None
    assert _step3_lookback_start(cursor) == snapshot_ts - timedelta(hours=96)
    assert engine._snapshot_cold_start_consumed is True

    cm2, cursor2 = _mock_db_connection(snapshot_ts, 500.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm2):
        second = engine._get_snapshot()
    assert second is not None
    assert _step3_lookback_start(cursor2) == snapshot_ts - timedelta(hours=2)


def test_cold_start_flag_flips_even_when_step3_query_times_out(monkeypatch):
    """A timeout on the cold-start cycle must not cause the next cycle to retry
    the wide window -- otherwise a slow first cycle would wedge the loop."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", "96")
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = ET.localize(datetime(2026, 5, 14, 13, 0)).astimezone(timezone.utc)
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", snapshot_ts)]

    cm, _ = _mock_db_connection(snapshot_ts, 500.0, rows, raise_on_step3=True)
    with patch.object(main_engine, "db_connection", return_value=cm):
        assert engine._get_snapshot() is None
    assert engine._snapshot_cold_start_consumed is True

    cm2, cursor2 = _mock_db_connection(snapshot_ts, 500.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm2):
        result = engine._get_snapshot()
    assert result is not None
    assert _step3_lookback_start(cursor2) == snapshot_ts - timedelta(hours=2)


def test_cold_start_lookback_floor_is_steady_state_value(monkeypatch):
    """If the operator misconfigures cold-start < steady-state, the engine
    silently uses the steady-state value rather than narrowing further."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "12")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", "4")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_lookback_hours == 12
    assert engine.snapshot_cold_start_lookback_hours == 12
