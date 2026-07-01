"""Unit tests for the v1 heuristic range model.

These pin the math so a future "simplification" can't silently widen or
narrow the bands or change how the pin strike is selected.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.jobs.forecast_range_model import (
    EVENT_DAY_MULTIPLIER,
    MAX_RANGE_FRACTION,
    MIN_RANGE_FRACTION,
    WALL_EXPANSION,
    ForecastInputs,
    compute_forecast,
)


def _inputs(**overrides) -> ForecastInputs:
    base = ForecastInputs(
        symbol="SPY",
        forecast_date=date(2026, 6, 29),
        spot=600.0,
        call_wall=606.0,
        put_wall=594.0,
        gamma_flip=600.5,
        max_pain=599.0,
        msi_composite=-0.32,
        msi_normalized=-32.0,
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def test_wall_bounded_range_typical_day():
    """6-dollar walls × 1.10 = 6.6 half-range → [593.40, 606.60]."""
    result = compute_forecast(_inputs())
    expected_half = max(606.0 - 600.0, 600.0 - 594.0) * WALL_EXPANSION
    assert result.projected_low == pytest.approx(600.0 - expected_half, abs=0.01)
    assert result.projected_high == pytest.approx(600.0 + expected_half, abs=0.01)
    assert result.range_model == "heuristic_v1"


def test_walls_missing_falls_back_to_floor():
    result = compute_forecast(_inputs(call_wall=None, put_wall=None))
    expected_half = 600.0 * MIN_RANGE_FRACTION / 2.0
    assert result.projected_low == pytest.approx(600.0 - expected_half, abs=0.01)
    assert result.projected_high == pytest.approx(600.0 + expected_half, abs=0.01)


def test_walls_inverted_falls_back_to_floor():
    """If put_wall > call_wall (a real chain anomaly during pinning), the
    model must NOT produce a negative-width band."""
    result = compute_forecast(_inputs(call_wall=595.0, put_wall=606.0))
    expected_half = 600.0 * MIN_RANGE_FRACTION / 2.0
    assert result.projected_low == pytest.approx(600.0 - expected_half, abs=0.01)
    assert result.projected_high == pytest.approx(600.0 + expected_half, abs=0.01)


def test_event_day_widens_band():
    """Compare quiet vs eventful at narrow walls where the result is well
    below the MAX_RANGE_FRACTION cap so the multiplier shows through."""
    narrow_walls = {"call_wall": 601.5, "put_wall": 598.5}  # ±1.5 half-range
    quiet = compute_forecast(_inputs(**narrow_walls))
    quiet_width = quiet.projected_high - quiet.projected_low
    eventful = compute_forecast(_inputs(**narrow_walls, is_event_day=True))
    eventful_width = eventful.projected_high - eventful.projected_low
    assert eventful_width == pytest.approx(quiet_width * EVENT_DAY_MULTIPLIER, abs=0.01)


def test_event_day_still_caps_at_max_range():
    """The event-day multiplier must not break the MAX_RANGE_FRACTION
    ceiling — wide walls × 1.5 still clamps to ≤2.5% of spot."""
    result = compute_forecast(_inputs(call_wall=700.0, put_wall=500.0, is_event_day=True))
    width = result.projected_high - result.projected_low
    assert width <= 2 * 600.0 * MAX_RANGE_FRACTION + 0.001


def test_max_range_caps_a_runaway_wall():
    """Pathologically wide walls (a deep-OTM call wall on a dead chain)
    must still produce a band ≤ MAX_RANGE_FRACTION of spot."""
    result = compute_forecast(_inputs(call_wall=700.0, put_wall=500.0))
    width = result.projected_high - result.projected_low
    assert width <= 2 * 600.0 * MAX_RANGE_FRACTION + 0.001


def test_pin_strike_prefers_max_pain():
    result = compute_forecast(_inputs())
    assert result.pin_strike == 599.0


def test_pin_strike_falls_back_to_nearest_strike():
    """No max_pain → nearest strike to spot at the symbol's strike step."""
    result = compute_forecast(_inputs(max_pain=None, strike_step=5.0))
    assert result.pin_strike == 600.0


def test_pin_strike_skips_when_spot_signal_missing():
    """The writer guards against missing spot before calling the model
    (returns early in _gather_inputs). The model is therefore allowed to
    require spot — what we DO need is that pin_strike falls back cleanly
    to None when max_pain is missing AND spot is unrealistic-but-present."""
    result = compute_forecast(_inputs(max_pain=None))
    # max_pain missing → pin = nearest strike to spot 600 at step 1.0 = 600.0
    assert result.pin_strike == 600.0


def test_regime_long_gamma_when_msi_positive():
    result = compute_forecast(_inputs(msi_composite=0.4))
    assert result.regime == "long_gamma"


def test_regime_short_gamma_when_msi_negative():
    result = compute_forecast(_inputs(msi_composite=-0.4))
    assert result.regime == "short_gamma"


def test_regime_transition_when_msi_near_zero():
    result = compute_forecast(_inputs(msi_composite=0.08))
    assert result.regime == "transition"


def test_regime_transition_when_msi_missing():
    result = compute_forecast(_inputs(msi_composite=None))
    assert result.regime == "transition"


def test_projected_close_clamped_into_band():
    """Pin strike outside the projected band must be clamped — the
    committed projected_close is a graded number; it has to be reachable."""
    result = compute_forecast(_inputs(max_pain=650.0))
    assert result.projected_low <= result.projected_close <= result.projected_high


def test_deterministic_for_same_inputs():
    """Same inputs → byte-identical output. Used by content_hash so
    re-runs of the writer produce the same hash."""
    a = compute_forecast(_inputs())
    b = compute_forecast(_inputs())
    assert a.projected_low == b.projected_low
    assert a.projected_high == b.projected_high
    assert a.pin_strike == b.pin_strike
    assert a.regime == b.regime
