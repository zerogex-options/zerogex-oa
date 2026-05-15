"""Tests for ``AnalyticsEngine`` off-hours (weekend / holiday) behavior.

Off-hours mode keeps the engine cycling outside the 24x5 run window
instead of sleeping until the next session.  The snapshot is anchored to
the latest option_chains row (not wall-clock NOW()), so an off-hours
cycle recomputes against the most recent available data (e.g. Friday's
close on a Saturday) rather than reporting nothing.  A slower interval is
used off-hours since the underlying data is static until the next
session.
"""

from unittest.mock import MagicMock, patch

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine


def test_off_hours_enabled_by_default():
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.off_hours_enabled is True


def test_off_hours_can_be_disabled(monkeypatch):
    monkeypatch.setenv("ANALYTICS_OFF_HOURS_ENABLED", "false")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.off_hours_enabled is False


def test_off_hours_interval_default_and_floor(monkeypatch):
    monkeypatch.setenv("ANALYTICS_OFF_HOURS_INTERVAL_SECONDS", "300")
    engine = AnalyticsEngine(underlying="SPY", calculation_interval=60)
    assert engine.off_hours_interval == 300

    # Off-hours interval can never be shorter than the steady-state interval.
    monkeypatch.setenv("ANALYTICS_OFF_HOURS_INTERVAL_SECONDS", "5")
    engine2 = AnalyticsEngine(underlying="SPY", calculation_interval=60)
    assert engine2.off_hours_interval == 60


def test_off_hours_disabled_sleeps_until_next_window(monkeypatch):
    """Legacy behavior: off-hours OFF + outside run window => sleep, no cycle."""
    monkeypatch.setenv("ANALYTICS_OFF_HOURS_ENABLED", "false")
    engine = AnalyticsEngine(underlying="SPY")

    monkeypatch.setattr(main_engine, "is_engine_run_window", lambda: False)
    monkeypatch.setattr(
        main_engine, "seconds_until_engine_run_window", lambda: 123
    )
    run_calc = MagicMock(return_value=True)
    engine.run_calculation = run_calc

    sleeps = []

    def fake_sleep(secs):
        sleeps.append(secs)
        engine.running = False  # break the loop after the first sleep

    monkeypatch.setattr(main_engine.time, "sleep", fake_sleep)
    engine.run()

    run_calc.assert_not_called()
    assert sleeps == [123]


def test_off_hours_enabled_runs_cycle_at_off_hours_interval(monkeypatch):
    """Off-hours ON + outside run window => still run a cycle, then sleep for
    the off-hours interval (not the steady-state interval)."""
    monkeypatch.setenv("ANALYTICS_OFF_HOURS_ENABLED", "true")
    monkeypatch.setenv("ANALYTICS_OFF_HOURS_INTERVAL_SECONDS", "300")
    engine = AnalyticsEngine(underlying="SPY", calculation_interval=60)

    monkeypatch.setattr(main_engine, "is_engine_run_window", lambda: False)
    run_calc = MagicMock(return_value=True)
    engine.run_calculation = run_calc

    sleeps = []

    def fake_sleep(secs):
        sleeps.append(secs)
        engine.running = False

    monkeypatch.setattr(main_engine.time, "sleep", fake_sleep)
    # Make cycle duration ~0 so sleep == full off-hours interval.
    monkeypatch.setattr(main_engine.time, "time", lambda: 1000.0)
    engine.run()

    run_calc.assert_called_once()
    assert sleeps == [300]


def test_in_run_window_uses_steady_state_interval(monkeypatch):
    """Inside the run window the steady-state interval is used regardless of
    off-hours configuration."""
    monkeypatch.setenv("ANALYTICS_OFF_HOURS_ENABLED", "true")
    monkeypatch.setenv("ANALYTICS_OFF_HOURS_INTERVAL_SECONDS", "300")
    engine = AnalyticsEngine(underlying="SPY", calculation_interval=60)

    monkeypatch.setattr(main_engine, "is_engine_run_window", lambda: True)
    run_calc = MagicMock(return_value=True)
    engine.run_calculation = run_calc

    sleeps = []

    def fake_sleep(secs):
        sleeps.append(secs)
        engine.running = False

    monkeypatch.setattr(main_engine.time, "sleep", fake_sleep)
    monkeypatch.setattr(main_engine.time, "time", lambda: 1000.0)
    engine.run()

    run_calc.assert_called_once()
    assert sleeps == [60]
