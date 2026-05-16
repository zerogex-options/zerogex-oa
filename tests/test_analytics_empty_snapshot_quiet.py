"""Tests for ``AnalyticsEngine.run_calculation`` closed-market quieting.

A weekday night is inside the 24x5 engine run window, so the engine
keeps cycling after the close.  Once the underlying feed stops,
ingestion writes option_chains rows with NULL Greeks (stale underlying)
that advance ``max(timestamp)`` while no row in the lookback window has
gamma — so ``_get_snapshot`` returns a snapshot with an empty options
list.

Previously this logged ``WARNING`` and returned ``False`` every cycle,
so ``run()`` logged "Calculation cycle had issues" every interval all
evening/overnight, and the unchanged-snapshot dedupe never armed (it
only records on success).  It must now be treated as a benign no-op:
logged once per closed period at INFO, with the snapshot timestamp
recorded so a frozen timestamp hits the unchanged-snapshot skip.
"""

import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from src.analytics.main_engine import AnalyticsEngine

_EMPTY_MARKER = "No options with Greeks for snapshot"
_SKIP_MARKER = "skipping recompute"


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


def test_empty_snapshot_is_a_benign_no_op_not_a_warning(caplog):
    """Empty options -> True (not False), dedupe armed, logged once at INFO."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 15, 22, 0, tzinfo=timezone.utc)  # Friday night
    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": []}
    )
    _stub_pipeline(engine)

    with caplog.at_level(logging.INFO):
        result = engine.run_calculation()

    assert result is True
    # The heavy pipeline never runs for an empty snapshot.
    engine._calculate_gex_by_strike.assert_not_called()
    engine._store_calculation_results.assert_not_called()
    # A skipped cycle is not a "completed calculation".
    assert engine.calculations_completed == 0
    # Dedupe is armed so a frozen timestamp stops re-attempting.
    assert engine._last_processed_snapshot_ts == ts
    assert engine._empty_snapshot_state is True
    # Logged once, at INFO — never WARNING.
    assert sum(_EMPTY_MARKER in r.message for r in caplog.records) == 1
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)


def test_repeated_empty_cycles_log_once_even_as_timestamp_advances(caplog):
    """Off-hours NULL-Greek rows advance max(timestamp); still log only once."""
    engine = AnalyticsEngine(underlying="SPY")
    ts1 = datetime(2026, 5, 15, 22, 0, tzinfo=timezone.utc)
    ts2 = ts1 + timedelta(minutes=1)
    _stub_pipeline(engine)

    with caplog.at_level(logging.INFO):
        engine._get_snapshot = MagicMock(
            return_value={"timestamp": ts1, "underlying_price": 500.0, "options": []}
        )
        assert engine.run_calculation() is True
        # New (advancing) timestamp: the unchanged-snapshot guard does NOT
        # fire, so we re-enter the empty branch — but it must stay silent.
        engine._get_snapshot = MagicMock(
            return_value={"timestamp": ts2, "underlying_price": 500.0, "options": []}
        )
        assert engine.run_calculation() is True

    assert engine._last_processed_snapshot_ts == ts2
    assert engine._empty_snapshot_state is True
    assert sum(_EMPTY_MARKER in r.message for r in caplog.records) == 1
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)


def test_frozen_empty_timestamp_hits_unchanged_snapshot_skip(caplog):
    """Recording the ts on the empty path arms the unchanged-snapshot skip."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 16, 21, 0, tzinfo=timezone.utc)  # a Saturday
    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": []}
    )
    _stub_pipeline(engine)

    with caplog.at_level(logging.INFO):
        assert engine.run_calculation() is True  # first: benign empty
        assert engine.run_calculation() is True  # second: frozen ts -> skip

    # Second cycle short-circuits on the unchanged-snapshot guard, so the
    # empty-snapshot line is logged exactly once and the skip line appears.
    assert sum(_EMPTY_MARKER in r.message for r in caplog.records) == 1
    assert any(_SKIP_MARKER in r.message for r in caplog.records)
    engine._calculate_gex_by_strike.assert_not_called()
    assert engine.calculations_completed == 0


def test_latch_clears_when_greek_bearing_data_resumes():
    """Once options return the latch clears so a later empty period re-logs."""
    engine = AnalyticsEngine(underlying="SPY")
    ts1 = datetime(2026, 5, 15, 22, 0, tzinfo=timezone.utc)
    ts2 = datetime(2026, 5, 18, 14, 0, tzinfo=timezone.utc)  # next session
    _stub_pipeline(engine)

    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts1, "underlying_price": 500.0, "options": []}
    )
    assert engine.run_calculation() is True
    assert engine._empty_snapshot_state is True

    engine._get_snapshot = MagicMock(
        return_value={"timestamp": ts2, "underlying_price": 500.0, "options": [{"x": 1}]}
    )
    assert engine.run_calculation() is True

    assert engine._empty_snapshot_state is False
    assert engine.calculations_completed == 1
    engine._calculate_gex_by_strike.assert_called_once()
    engine._store_calculation_results.assert_called_once()
    assert engine._last_processed_snapshot_ts == ts2
