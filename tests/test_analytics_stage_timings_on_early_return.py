"""Tests that ``run_calculation`` records stage timings on EARLY returns.

When ``_get_snapshot`` times out and returns None, the cycle exits at
``if not snapshot: return False`` BEFORE the success path that stores
``self._last_stage_timings``.  The cycle-overrun warning in ``run()``
then prints stale stage_timings from a prior successful cycle, leaving
an operator with a misleading ``snapshot=39.3s`` breakdown next to a
``cycle_duration=90.0s`` overrun -- pointing at the wrong root cause.

Fix: write ``self._last_stage_timings`` on early returns too so the
warning reflects the *failing* cycle.
"""

from datetime import datetime, timezone
from unittest.mock import patch

from src.analytics.main_engine import AnalyticsEngine


def _engine() -> AnalyticsEngine:
    return AnalyticsEngine(underlying="SPY")


def test_last_stage_timings_recorded_when_snapshot_returns_none(monkeypatch):
    engine = _engine()
    # Seed _last_stage_timings with a prior "successful" cycle's data so
    # we can verify the FAILING cycle overwrites it instead of leaving
    # the stale numbers behind.
    engine._last_stage_timings = {
        "snapshot": 39.3,
        "gex_by_strike": 0.1,
        "gex_summary": 0.1,
    }

    with patch.object(engine, "_get_snapshot", return_value=None):
        assert engine.run_calculation() is False

    # Stale data MUST be replaced with the failing cycle's partial
    # timings (snapshot only -- everything else never ran).
    assert set(engine._last_stage_timings.keys()) == {"snapshot"}


def test_last_stage_timings_recorded_when_gex_by_strike_empty(monkeypatch):
    engine = _engine()
    engine._last_stage_timings = {"snapshot": 0.0, "old_stage": 42.0}

    fake_snapshot = {
        "timestamp": datetime(2026, 5, 21, 14, 0, tzinfo=timezone.utc),
        "underlying_price": 500.0,
        "options": [{"option_symbol": "X", "gamma": 0.01, "open_interest": 1}],
    }
    with patch.object(engine, "_get_snapshot", return_value=fake_snapshot), patch.object(
        engine, "_calculate_gex_by_strike", return_value=[]
    ):
        assert engine.run_calculation() is False

    # snapshot stage was run + gex_by_strike stage was run; both recorded.
    # The stale "old_stage" must be gone.
    assert "old_stage" not in engine._last_stage_timings
    assert "snapshot" in engine._last_stage_timings
    assert "gex_by_strike" in engine._last_stage_timings
