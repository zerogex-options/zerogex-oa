"""Tests for ``AnalyticsEngine.run_calculation`` redundant-compute skip.

Off-hours the snapshot is anchored to the latest option_chains row,
which is frozen until the next session.  Without a guard every
off_hours_interval would recompute the full GEX / vanna-charm /
per-expiration max-pain / walls pipeline for the SAME (underlying,
timestamp) — identical input -> identical output -> an already no-op
``IS DISTINCT FROM``-guarded upsert.  The guard skips the recompute when
``_get_snapshot`` returns the same timestamp as the last SUCCESSFULLY
processed cycle (the interval is still slept by ``run()``).

It must NOT suppress legitimate intraday recompute: during RTH a new bar
advances the timestamp every minute, so only a *truly unchanged*
timestamp skips.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from src.analytics.main_engine import AnalyticsEngine


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
    """Stub the heavy compute/store steps so run_calculation needs no DB."""
    engine._calculate_gex_by_strike = MagicMock(return_value=[{"net_gex": 1.0}])
    engine._calculate_gex_summary = MagicMock(return_value=_summary())
    engine._validate_gex_calculations = MagicMock()
    engine._store_calculation_results = MagicMock()
    engine._refresh_flow_caches = MagicMock()
    engine._refresh_flow_series_snapshot = MagicMock()


def test_skips_recompute_when_snapshot_timestamp_unchanged():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 16, 21, 0, tzinfo=timezone.utc)  # a Saturday

    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": [{"x": 1}]}
    )
    _stub_pipeline(engine)

    # Pretend the prior cycle already processed this exact timestamp.
    engine._last_processed_snapshot_ts = ts

    result = engine.run_calculation()

    assert result is True
    # The whole pipeline is skipped — no recompute, no store, no refresh.
    engine._calculate_gex_by_strike.assert_not_called()
    engine._calculate_gex_summary.assert_not_called()
    engine._store_calculation_results.assert_not_called()
    engine._refresh_flow_caches.assert_not_called()
    engine._refresh_flow_series_snapshot.assert_not_called()
    # A skipped cycle is not a "completed calculation".
    assert engine.calculations_completed == 0
    assert engine._last_processed_snapshot_ts == ts


def test_recomputes_when_snapshot_timestamp_advances():
    """RTH: a new bar advances the timestamp every minute — must recompute."""
    engine = AnalyticsEngine(underlying="SPY")
    prev_ts = datetime(2026, 5, 15, 14, 30, tzinfo=timezone.utc)
    new_ts = prev_ts + timedelta(minutes=1)

    engine._get_snapshot = MagicMock(
        return_value={"timestamp": new_ts, "underlying_price": 500.0, "options": [{"x": 1}]}
    )
    _stub_pipeline(engine)

    engine._last_processed_snapshot_ts = prev_ts

    result = engine.run_calculation()

    assert result is True
    engine._calculate_gex_by_strike.assert_called_once()
    engine._store_calculation_results.assert_called_once()
    engine._refresh_flow_caches.assert_called_once()
    assert engine.calculations_completed == 1
    # The newly processed timestamp is recorded for next-cycle comparison.
    assert engine._last_processed_snapshot_ts == new_ts


def test_first_cycle_computes_then_subsequent_identical_cycle_skips():
    """End-to-end: first cycle has no prior ts (computes); a second cycle
    against the SAME frozen snapshot then skips."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 16, 21, 0, tzinfo=timezone.utc)

    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": [{"x": 1}]}
    )
    _stub_pipeline(engine)

    assert engine._last_processed_snapshot_ts is None
    assert engine.run_calculation() is True
    assert engine.calculations_completed == 1
    engine._store_calculation_results.assert_called_once()

    # Second cycle, identical frozen snapshot -> skip (no further store).
    assert engine.run_calculation() is True
    assert engine.calculations_completed == 1  # unchanged
    engine._store_calculation_results.assert_called_once()  # still just once
