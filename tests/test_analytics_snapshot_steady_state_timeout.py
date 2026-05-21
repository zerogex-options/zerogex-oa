"""Tests for ``ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS``.

The pool-wide ``DB_STATEMENT_TIMEOUT_MS`` (default 90s) is sized for
sub-second API queries.  The steady-state snapshot is usually fast but
can spike past 90s under autovacuum + concurrent-ingestion bursts -- in
which case the pool kills every cycle, the snapshot exits at the
``if not snapshot`` guard, and the downstream flow_series_5min
refresh is never run (surfaced by the API as
``flow_series_5min shortfall`` warnings).

``ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS`` lets operators relax just
the snapshot query via ``SET LOCAL`` without raising the pool ceiling.
Default 0 = no override (current behavior).
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytz

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")


def _row(option_symbol, strike, expiration, option_type, quote_ts, *, gamma=0.01, oi=100):
    return (
        option_symbol, strike, expiration, option_type,
        1.0, 0.99, 1.01, 10, oi, 0.5, gamma, -0.05, 0.1, 0.2, quote_ts,
    )


def _mock_db_connection(latest_ts, underlying_price, option_rows):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(latest_ts,), (underlying_price,)]
    cursor.fetchall.return_value = option_rows
    cursor.execute.side_effect = lambda *a, **kw: None
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, conn, cursor


def _set_local_calls(cursor):
    return [
        c
        for c in cursor.execute.call_args_list
        if "statement_timeout" in c[0][0] and "SET LOCAL" in c[0][0]
    ]


def _fresh_ts():
    return datetime.now(timezone.utc) - timedelta(minutes=1)


def test_steady_state_uses_pool_default_when_env_unset(monkeypatch):
    """Cycle 2+ on the cheap path with env unset issues NO SET LOCAL,
    so the pool-wide statement_timeout applies.  The first cycle gets
    the cold-start budget regardless -- see
    ``test_first_cycle_steady_state_uses_cold_start_budget`` -- so we
    simulate cycle 2 by pre-flipping ``_snapshot_cold_start_consumed``."""
    monkeypatch.delenv("ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS", raising=False)
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    engine = AnalyticsEngine(underlying="SPY")
    engine._snapshot_cold_start_consumed = True  # simulate cycle 2+
    assert engine.snapshot_statement_timeout_ms == 0

    ts = _fresh_ts()
    expiration = ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", ts)]
    cm, _, cursor = _mock_db_connection(ts, 500.0, rows)

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    assert _set_local_calls(cursor) == []


def test_steady_state_applies_configured_statement_timeout(monkeypatch):
    """Cycle 2+ on the cheap path issues a SET LOCAL with the configured
    steady-state timeout (cycle 1 uses the cold-start budget regardless,
    covered separately)."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS", "150000")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    engine = AnalyticsEngine(underlying="SPY")
    engine._snapshot_cold_start_consumed = True  # simulate cycle 2+
    assert engine.snapshot_statement_timeout_ms == 150000

    ts = _fresh_ts()
    expiration = ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", ts)]
    cm, _, cursor = _mock_db_connection(ts, 500.0, rows)

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    set_local = _set_local_calls(cursor)
    assert len(set_local) == 1
    assert set_local[0][0][1] == ("150000",)


def test_first_cycle_steady_state_uses_cold_start_budget(monkeypatch):
    """Buffer-pool warmup is a separate concern from the data-staleness
    gate: a mid-session restart with live ingestion picks the cheap 2h
    steady-state path (data is fresh, want_cold_start is False) but the
    pool is still cold from the restart.  Cycle 1 on the cheap path
    therefore gets the SAME budget as the wide cold-start scan; cycle
    2+ falls back to the configured steady-state budget."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS", "150000")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_STATEMENT_TIMEOUT_MS", "180000")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2")
    engine = AnalyticsEngine(underlying="SPY")

    ts = _fresh_ts()  # data IS fresh -> cold-start window is skipped
    expiration = ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", ts)]
    cm, _, cursor = _mock_db_connection(ts, 500.0, rows)

    # Cycle 1 -- should issue SET LOCAL with the cold-start budget.
    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()
    set_local = _set_local_calls(cursor)
    assert len(set_local) == 1, (
        "first cycle on the steady-state path must issue exactly one "
        "SET LOCAL statement_timeout (got: %r)" % (set_local,)
    )
    assert set_local[0][0][1] == ("180000",), (
        "first cycle on the steady-state path must use the cold-start "
        "budget so a cold buffer pool right after restart can warm up"
    )

    # Cycle 2 -- should drop back to the configured steady-state budget.
    cm2, _, cursor2 = _mock_db_connection(ts, 500.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm2):
        engine._get_snapshot()
    set_local_2 = _set_local_calls(cursor2)
    assert len(set_local_2) == 1
    assert set_local_2[0][0][1] == ("150000",), (
        "cycle 2+ must use the configured steady-state budget once the "
        "buffer pool has warmed up; otherwise a runaway plan can wedge "
        "a backend for the cold-start budget every cycle forever"
    )


def test_first_cycle_steady_state_no_set_local_when_both_budgets_zero(monkeypatch):
    """If an operator explicitly disables BOTH timeouts (e.g. for a
    debug session), the first cycle must NOT silently install a
    SET LOCAL that no longer maps to any configured budget."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS", "0")
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_COLD_START_STATEMENT_TIMEOUT_MS", "0")
    engine = AnalyticsEngine(underlying="SPY")
    ts = _fresh_ts()
    expiration = ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260521C00500000", 500.0, expiration, "C", ts)]
    cm, _, cursor = _mock_db_connection(ts, 500.0, rows)

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    assert _set_local_calls(cursor) == []


def test_fractional_lookback_hours_allowed(monkeypatch):
    """Operators on cold-storage buffer pools can dial the snapshot
    working set down via fractional ANALYTICS_SNAPSHOT_LOOKBACK_HOURS
    (e.g. 0.5 = 30 min, 0.25 = 15 min) without a code change.  Floored
    at 5 min to avoid silently losing recently-quoted contracts."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "0.5")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_lookback_hours == 0.5

    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "0.01")
    engine = AnalyticsEngine(underlying="SPY")
    # 0.01h = 36s is below the 5-min floor; clamps to 5 min (1/12 h).
    assert engine.snapshot_lookback_hours == 1.0 / 12.0


def test_steady_state_timeout_negative_floored_to_zero(monkeypatch):
    """Negative values floor to 0 -- guards against operator typos that
    would otherwise be silently negative-cast to a positive uint by libpq."""
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS", "-1")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_statement_timeout_ms == 0
