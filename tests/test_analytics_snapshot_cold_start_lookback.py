"""Tests for ``AnalyticsEngine._get_snapshot`` cold-start lookback behavior.

The first cycle after process start MAY use a wider lookback window
(default 96h) to bridge weekend / overnight gaps -- but only when the
newest option_chains row is itself stale (older than the steady-state
lookback).  When ingestion is live and the newest row is fresh, the
narrow steady-state window already covers the active universe, so the
expensive wide scan is skipped even on cycle 1.

When the wide window IS used it runs under a dedicated (higher)
per-statement timeout via ``SET LOCAL`` so a cold buffer pool doesn't
kill it at the lower pool-wide ceiling.  If it still fails the engine
rolls back and retries the SAME cycle with the cheap steady-state
window, so the first cycle yields a narrower-but-non-empty result
instead of a hard error.  The cold-start flag is consumed exactly once
per worker regardless of outcome so a slow/failed first cycle can never
wedge the cycle loop.
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


def _mock_db_connection(latest_ts, underlying_price, option_rows, fail_cold_start=False):
    """Script the three sequential queries ``_get_snapshot`` issues.

    When ``fail_cold_start`` is set, the FIRST snapshot query (the wide
    cold-start scan, preceded by its ``SET LOCAL statement_timeout``)
    raises QueryCanceled; the steady-state retry then succeeds.
    """
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(latest_ts,), (underlying_price,)]
    cursor.fetchall.return_value = option_rows

    state = {"snapshot_calls": 0}

    def execute_side_effect(sql, params=None):
        if "DISTINCT ON" in sql:
            state["snapshot_calls"] += 1
            if fail_cold_start and state["snapshot_calls"] == 1:
                raise psycopg2.errors.QueryCanceled(
                    "canceling statement due to statement timeout"
                )
        return None

    cursor.execute.side_effect = execute_side_effect

    conn = MagicMock()
    conn.cursor.return_value = cursor

    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, conn, cursor


def _snapshot_calls(cursor):
    return [c for c in cursor.execute.call_args_list if "DISTINCT ON" in c[0][0]]


def _set_local_calls(cursor):
    return [
        c
        for c in cursor.execute.call_args_list
        if "statement_timeout" in c[0][0] and "SET LOCAL" in c[0][0]
    ]


def _stale_ts():
    """A snapshot ts old enough that data_age > steady-state lookback."""
    return datetime.now(timezone.utc) - timedelta(days=3)


def _fresh_ts():
    """A snapshot ts recent enough that data_age <= steady-state lookback."""
    return datetime.now(timezone.utc) - timedelta(minutes=1)


def test_first_cycle_uses_cold_start_window_when_data_is_stale(monkeypatch):
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", "96")
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = _stale_ts()
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", snapshot_ts)]

    cm, _, cursor = _mock_db_connection(snapshot_ts, 500.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm):
        first = engine._get_snapshot()
    assert first is not None
    calls = _snapshot_calls(cursor)
    assert len(calls) == 1
    assert calls[0][0][1][2] == snapshot_ts - timedelta(hours=96)
    assert engine._snapshot_cold_start_consumed is True

    # Cycle 2 always uses steady-state.
    cm2, _, cursor2 = _mock_db_connection(snapshot_ts, 500.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm2):
        second = engine._get_snapshot()
    assert second is not None
    assert _snapshot_calls(cursor2)[0][0][1][2] == snapshot_ts - timedelta(hours=2)


def test_first_cycle_skips_cold_start_when_data_is_fresh(monkeypatch):
    """Smart gate: a mid-session restart with live ingestion (fresh newest
    row) must NOT pay for the wide cold-start scan."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", "96")
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = _fresh_ts()
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", snapshot_ts)]

    cm, _, cursor = _mock_db_connection(snapshot_ts, 500.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()
    assert result is not None
    # Steady-state window even though it's the first cycle.
    assert _snapshot_calls(cursor)[0][0][1][2] == snapshot_ts - timedelta(hours=2)
    # No SET LOCAL override on the cheap path.
    assert _set_local_calls(cursor) == []
    assert engine._snapshot_cold_start_consumed is True


def test_cold_start_applies_configured_statement_timeout(monkeypatch):
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", "96")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_STATEMENT_TIMEOUT_MS", "175000")
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = _stale_ts()
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", snapshot_ts)]

    cm, _, cursor = _mock_db_connection(snapshot_ts, 500.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    set_local = _set_local_calls(cursor)
    assert len(set_local) == 1
    # params passed as ("175000",)
    assert set_local[0][0][1] == ("175000",)


def test_cold_start_failure_falls_back_to_steady_state_same_cycle(monkeypatch):
    """A timeout on the wide cold-start scan must NOT return None: the engine
    rolls back and retries this same cycle with the steady-state window."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", "96")
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = _stale_ts()
    expiration = snapshot_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", snapshot_ts)]

    cm, conn, cursor = _mock_db_connection(snapshot_ts, 500.0, rows, fail_cold_start=True)
    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()

    assert result is not None
    assert len(result["options"]) == 1
    conn.rollback.assert_called()  # aborted tx rolled back before retry
    calls = _snapshot_calls(cursor)
    # Two snapshot attempts: failed 96h cold-start, then 2h steady-state.
    assert len(calls) == 2
    assert calls[0][0][1][2] == snapshot_ts - timedelta(hours=96)
    assert calls[1][0][1][2] == snapshot_ts - timedelta(hours=2)
    assert engine._snapshot_cold_start_consumed is True


def test_cold_start_flag_consumed_even_when_fallback_also_fails(monkeypatch):
    """If both the cold-start AND the fallback fail, _get_snapshot returns
    None, but the flag is still consumed so cycle 2 won't retry the wide
    window (no wedge)."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", "96")
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = _stale_ts()

    cursor = MagicMock()
    cursor.fetchone.side_effect = [(snapshot_ts,), (500.0,)]

    def execute_side_effect(sql, params=None):
        if "DISTINCT ON" in sql:
            raise psycopg2.errors.QueryCanceled("timeout")
        return None

    cursor.execute.side_effect = execute_side_effect
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False

    with patch.object(main_engine, "db_connection", return_value=cm):
        assert engine._get_snapshot() is None
    assert engine._snapshot_cold_start_consumed is True

    # Cycle 2: data still stale, but cold-start was consumed -> steady-state.
    cm2, _, cursor2 = _mock_db_connection(snapshot_ts, 500.0, [])
    with patch.object(main_engine, "db_connection", return_value=cm2):
        engine._get_snapshot()
    assert _snapshot_calls(cursor2)[0][0][1][2] == snapshot_ts - timedelta(hours=2)


def test_cold_start_lookback_floor_is_steady_state_value(monkeypatch):
    """If the operator misconfigures cold-start < steady-state, the engine
    silently uses the steady-state value rather than narrowing further."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "12")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", "4")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_lookback_hours == 12
    assert engine.snapshot_cold_start_lookback_hours == 12
