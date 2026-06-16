"""Tests for the flow_series_5min snapshot top-up when the analytics
pipeline short-circuits.

After cash close (16:00 ET) the underlying feed freezes, so
``_get_snapshot`` returns either the same frozen timestamp (skip-guard
branch) or fresh NULL-Greek rows (empty-options branch). In both branches
``run_calculation`` returns ``True`` without running the GEX pipeline —
which previously also bypassed ``_refresh_flow_series_snapshot``. The
result: the snapshot stopped at the 15:55 ET bar (the last bar the
pre-close cycle wrote) while the API's ``/api/flow/series?session=current``
window for SPX extends to 16:15 ET, leaving a permanent 4-bar shortfall
that fires ``flow_series_5min shortfall`` warnings on every poll.

These tests verify that the skip paths now refresh the snapshot while we
are still inside the session window (so bars 16:00, 16:05, 16:10, 16:15
ET get written), and stop firing once the window closes (so overnight
cycles don't re-upsert the same closed bars at the off-hours interval).
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytz

from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")


def _summary():
    return {
        "max_gamma_strike": 500.0,
        "max_gamma_value": 1_000_000.0,
        "gamma_flip_point": 499.0,
        "flip_distance": 0.002,
        "local_gex": 1.0,
        "convexity_risk": 1.0,
        "max_pain": 505.0,
        "put_call_ratio": 0.9,
        "total_net_gex": 1_000_000.0,
    }


def _stub_pipeline(engine):
    engine._calculate_gex_by_strike = MagicMock(return_value=[{"net_gex": 1.0}])
    engine._calculate_gex_summary = MagicMock(return_value=_summary())
    engine._validate_gex_calculations = MagicMock()
    engine._store_calculation_results = MagicMock()
    engine._refresh_flow_caches = MagicMock()
    engine._refresh_flow_series_snapshot = MagicMock()


def test_helper_fires_when_first_cycle_regardless_of_wallclock():
    """An engine restart with no prior processed ts must fire the refresh
    so any tail bars an earlier instance failed to write get backfilled,
    even if wall-clock is far past session_close."""
    engine = AnalyticsEngine(underlying="SPY")
    assert engine._last_processed_snapshot_ts is None

    latest_ts = datetime(2026, 6, 15, 19, 55, tzinfo=timezone.utc)  # 15:55 ET
    # Well past session_close + 5min (Jun 15 20:20 UTC).
    far_after_close = datetime(2026, 6, 15, 23, 0, tzinfo=timezone.utc)

    assert engine._skip_path_should_refresh_snapshot(latest_ts, now=far_after_close) is True


def test_helper_fires_during_session_window():
    engine = AnalyticsEngine(underlying="SPY")
    engine._last_processed_snapshot_ts = datetime(2026, 6, 15, 19, 55, tzinfo=timezone.utc)

    latest_ts = datetime(2026, 6, 15, 20, 1, tzinfo=timezone.utc)  # 16:01 ET
    # session_close for Jun 15 = 20:15 UTC, grace = +5min = 20:20 UTC.
    inside_window = datetime(2026, 6, 15, 20, 14, tzinfo=timezone.utc)  # 16:14 ET

    assert engine._skip_path_should_refresh_snapshot(latest_ts, now=inside_window) is True


def test_helper_fires_through_grace_past_session_close():
    """The 16:15 ET bar boundary lands at 20:15 UTC; the gate must stay
    open for one ANALYTICS_INTERVAL of grace so a cycle that lands at
    20:16 UTC still writes the trailing bar."""
    engine = AnalyticsEngine(underlying="SPY")
    engine._last_processed_snapshot_ts = datetime(2026, 6, 15, 20, 14, tzinfo=timezone.utc)

    latest_ts = datetime(2026, 6, 15, 20, 14, tzinfo=timezone.utc)  # frozen at 16:14 ET
    just_after_close = datetime(2026, 6, 15, 20, 19, 30, tzinfo=timezone.utc)  # 16:19:30 ET

    assert engine._skip_path_should_refresh_snapshot(latest_ts, now=just_after_close) is True


def test_helper_stops_firing_after_grace():
    """Once we are past session_close + 5min the snapshot is complete;
    overnight cycles must not keep re-upserting it."""
    engine = AnalyticsEngine(underlying="SPY")
    engine._last_processed_snapshot_ts = datetime(2026, 6, 15, 20, 16, tzinfo=timezone.utc)

    latest_ts = datetime(2026, 6, 15, 20, 16, tzinfo=timezone.utc)  # frozen at 16:16 ET
    # 16:30 ET — well past session_close (16:15) + 5min grace.
    overnight = datetime(2026, 6, 15, 20, 30, tzinfo=timezone.utc)

    assert engine._skip_path_should_refresh_snapshot(latest_ts, now=overnight) is False


def test_helper_stops_firing_next_morning_pre_open():
    """Overnight into next trading day, before the new session opens, the
    gate must stay closed for the previous day's frozen timestamp so we
    don't burn cycles re-writing yesterday's already-complete snapshot."""
    engine = AnalyticsEngine(underlying="SPY")
    yesterday = datetime(2026, 6, 15, 20, 14, tzinfo=timezone.utc)  # 16:14 ET Mon
    engine._last_processed_snapshot_ts = yesterday

    # latest_ts still frozen at yesterday because no new option_chains row yet.
    next_morning = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)  # 08:00 ET Tue

    assert engine._skip_path_should_refresh_snapshot(yesterday, now=next_morning) is False


def test_skip_guard_branch_refreshes_snapshot_when_in_window():
    """Steady-state cycle during the 16:00–16:15 ET window: GEX recompute
    skips (identical input -> identical output) but the snapshot writer
    still has a new bar to materialise, so it must run."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 6, 15, 20, 1, tzinfo=timezone.utc)  # 16:01 ET

    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": [{"x": 1}]}
    )
    _stub_pipeline(engine)
    engine._last_processed_snapshot_ts = ts  # skip-guard precondition
    engine._skip_path_should_refresh_snapshot = MagicMock(return_value=True)

    assert engine.run_calculation() is True
    # GEX pipeline still skipped.
    engine._calculate_gex_by_strike.assert_not_called()
    engine._store_calculation_results.assert_not_called()
    engine._refresh_flow_caches.assert_not_called()
    # But the flow snapshot writer DOES fire so the late-session bars land.
    engine._refresh_flow_series_snapshot.assert_called_once_with(ts)


def test_skip_guard_branch_skips_snapshot_when_out_of_window():
    """Overnight after the session window has closed: nothing left to
    refresh, the call must be elided."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 6, 15, 20, 1, tzinfo=timezone.utc)

    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": [{"x": 1}]}
    )
    _stub_pipeline(engine)
    engine._last_processed_snapshot_ts = ts
    engine._skip_path_should_refresh_snapshot = MagicMock(return_value=False)

    assert engine.run_calculation() is True
    engine._refresh_flow_series_snapshot.assert_not_called()


def test_empty_options_branch_refreshes_snapshot_when_in_window():
    """Post-close NULL-Greek cycle: the empty-options branch returns True
    without the GEX pipeline, but flow_by_contract is fed by a separate
    pipeline that prints through 16:15 ET, so the snapshot writer must
    still run while we are in the gate."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 6, 15, 20, 5, tzinfo=timezone.utc)  # 16:05 ET

    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": []}
    )
    _stub_pipeline(engine)
    # Not the first cycle — a prior cycle already armed the dedupe.
    engine._last_processed_snapshot_ts = ts - timedelta(minutes=1)
    engine._skip_path_should_refresh_snapshot = MagicMock(return_value=True)

    assert engine.run_calculation() is True
    engine._calculate_gex_by_strike.assert_not_called()
    engine._refresh_flow_series_snapshot.assert_called_once_with(ts)
    # The empty-options branch still records the timestamp so the next
    # cycle hits the skip-guard.
    assert engine._last_processed_snapshot_ts == ts


def test_empty_options_branch_skips_snapshot_when_out_of_window():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 6, 15, 23, 0, tzinfo=timezone.utc)  # 19:00 ET, well after close

    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": []}
    )
    _stub_pipeline(engine)
    engine._last_processed_snapshot_ts = ts - timedelta(minutes=1)
    engine._skip_path_should_refresh_snapshot = MagicMock(return_value=False)

    assert engine.run_calculation() is True
    engine._refresh_flow_series_snapshot.assert_not_called()
    assert engine._last_processed_snapshot_ts == ts


def test_first_cycle_with_empty_options_still_refreshes_snapshot():
    """Engine restart overnight: the first cycle hits the empty-options
    branch (NULL Greeks while underlying is stale). The helper's
    first-cycle clause must fire the refresh so any bars an earlier
    instance failed to write get backfilled — independent of where
    wall-clock sits relative to session_close."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 6, 15, 22, 0, tzinfo=timezone.utc)  # 18:00 ET — after close

    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": []}
    )
    _stub_pipeline(engine)
    assert engine._last_processed_snapshot_ts is None  # fresh process

    assert engine.run_calculation() is True
    engine._refresh_flow_series_snapshot.assert_called_once_with(ts)
    assert engine._last_processed_snapshot_ts == ts
