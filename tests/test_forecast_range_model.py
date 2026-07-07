"""Unit tests for the v1.2 feature-weighted range model.

Anchors the math for every input the model consumes so a future
"simplification" can't silently widen / narrow / re-lean the bands or
change how the pin is selected.  Layer-2 calibration is exercised via
the ``calibration`` input.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.jobs.forecast_range_model import (
    BASE_WALL_EXPANSION,
    EVENT_DAY_MULTIPLIER,
    MAX_RANGE_FRACTION,
    MIN_RANGE_FRACTION,
    OPEX_FRIDAY_MULTIPLIER,
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


# ---------------------------------------------------------------------------
# Structural asymmetric bands
# ---------------------------------------------------------------------------


def test_range_model_tag():
    result = compute_forecast(_inputs())
    assert result.range_model == "heuristic_v1_3"


def test_asymmetric_walls_produce_asymmetric_band():
    """Put wall closer than call wall → downside half-band tighter."""
    result = compute_forecast(_inputs(
        call_wall=610.0, put_wall=598.0,   # up=10, down=2
        msi_normalized=0.0,                  # neutralize lean
        vix_close=None,                      # no vol blend
        atr_5d=None,
    ))
    up_half = result.projected_high - 600.0
    down_half = 600.0 - result.projected_low
    assert up_half > down_half
    # up ~10 * 1.10 = 11; down ~2 * 1.10 = 2.2
    assert up_half == pytest.approx(10.0 * BASE_WALL_EXPANSION, abs=0.05)
    assert down_half == pytest.approx(2.0 * BASE_WALL_EXPANSION, abs=0.05)


def test_walls_missing_falls_back_to_floor():
    result = compute_forecast(_inputs(
        call_wall=None, put_wall=None,
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    expected_half = 600.0 * MIN_RANGE_FRACTION
    assert result.projected_low == pytest.approx(600.0 - expected_half, abs=0.02)
    assert result.projected_high == pytest.approx(600.0 + expected_half, abs=0.02)


def test_walls_inverted_uses_floor_on_that_side():
    """put_wall above spot → downside falls back to floor; upside still
    honors the (valid) call_wall."""
    result = compute_forecast(_inputs(
        call_wall=606.0, put_wall=602.0,  # put wall inverted
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    down_half = 600.0 - result.projected_low
    assert down_half == pytest.approx(600.0 * MIN_RANGE_FRACTION, abs=0.02)


def test_max_range_caps_a_runaway_wall():
    result = compute_forecast(_inputs(
        call_wall=700.0, put_wall=500.0,
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    width = result.projected_high - result.projected_low
    assert width <= 2 * 600.0 * MAX_RANGE_FRACTION + 0.02


# ---------------------------------------------------------------------------
# Vol regime blend (VIX / VXN / ATR)
# ---------------------------------------------------------------------------


def test_vix_blend_widens_band_on_high_vol_day():
    """VIX 30 on a $600 spot implies ±$11.4 daily move — narrower walls
    should get lifted by the VIX blend."""
    narrow = compute_forecast(_inputs(
        call_wall=602.0, put_wall=598.0,     # ±$2 walls
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    with_vix = compute_forecast(_inputs(
        call_wall=602.0, put_wall=598.0,
        vix_close=30.0, msi_normalized=0.0, atr_5d=None,
    ))
    assert (with_vix.projected_high - with_vix.projected_low) > \
           (narrow.projected_high - narrow.projected_low)


def test_atr_floor_lifts_below_realized_move():
    """5-day ATR of $12 → half of $6 becomes a floor on the half-band."""
    no_atr = compute_forecast(_inputs(
        call_wall=601.0, put_wall=599.0,
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    with_atr = compute_forecast(_inputs(
        call_wall=601.0, put_wall=599.0,
        vix_close=None, msi_normalized=0.0, atr_5d=12.0,
    ))
    assert (with_atr.projected_high - with_atr.projected_low) > \
           (no_atr.projected_high - no_atr.projected_low)


# ---------------------------------------------------------------------------
# Directional lean + intensity amplifier
# ---------------------------------------------------------------------------


def test_msi_lean_shifts_upside_when_bullish():
    baseline = compute_forecast(_inputs(
        msi_normalized=0.0,
        vix_close=None, atr_5d=None,
    ))
    bullish = compute_forecast(_inputs(
        msi_normalized=60.0,  # 60/400 = +0.15 lean
        vix_close=None, atr_5d=None,
    ))
    up_baseline = baseline.projected_high - 600.0
    up_bullish = bullish.projected_high - 600.0
    assert up_bullish > up_baseline
    down_baseline = 600.0 - baseline.projected_low
    down_bullish = 600.0 - bullish.projected_low
    assert down_bullish < down_baseline


def test_screaming_bearish_tightens_downside_widens_upside():
    """Screaming bearish (>0.6 |composite| + PCR>1.5) leaves room for a
    retracement rally by widening upside and tightening downside."""
    result = compute_forecast(_inputs(
        msi_composite=-0.75,
        msi_normalized=0.0,       # neutralize the MSI lean so this test
                                    # isolates the intensity amplifier
        put_call_ratio=1.7,
        vix_close=None, atr_5d=None,
    ))
    up = result.projected_high - 600.0
    down = 600.0 - result.projected_low
    assert up > down


# ---------------------------------------------------------------------------
# Sticky gamma nodes
# ---------------------------------------------------------------------------


def test_sticky_node_inside_band_tightens_it():
    baseline = compute_forecast(_inputs(
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    stuck = compute_forecast(_inputs(
        vix_close=None, msi_normalized=0.0, atr_5d=None,
        top_gamma_nodes=[{"strike": 600.0, "net_gex": 5e8}],  # big node at spot
    ))
    assert (stuck.projected_high - stuck.projected_low) < \
           (baseline.projected_high - baseline.projected_low)


def test_small_node_does_not_tighten():
    baseline = compute_forecast(_inputs(
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    small = compute_forecast(_inputs(
        vix_close=None, msi_normalized=0.0, atr_5d=None,
        top_gamma_nodes=[{"strike": 600.0, "net_gex": 1e5}],  # below threshold
    ))
    assert (small.projected_high - small.projected_low) == pytest.approx(
        baseline.projected_high - baseline.projected_low, abs=0.001,
    )


# ---------------------------------------------------------------------------
# Special-day handlers
# ---------------------------------------------------------------------------


def test_event_day_widens_band():
    narrow = {"call_wall": 601.5, "put_wall": 598.5}
    quiet = compute_forecast(_inputs(
        **narrow, vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    eventful = compute_forecast(_inputs(
        **narrow, is_event_day=True,
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    q = quiet.projected_high - quiet.projected_low
    e = eventful.projected_high - eventful.projected_low
    assert e == pytest.approx(q * EVENT_DAY_MULTIPLIER, abs=0.05)


def test_opex_friday_widens_band():
    quiet = compute_forecast(_inputs(
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    opex = compute_forecast(_inputs(
        is_opex_friday=True,
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    q = quiet.projected_high - quiet.projected_low
    o = opex.projected_high - opex.projected_low
    assert o == pytest.approx(q * OPEX_FRIDAY_MULTIPLIER, abs=0.05)


def test_opex_friday_uses_0dte_walls_when_present():
    """0DTE walls blended 70/30 with full-chain on OPEX Fridays."""
    both = compute_forecast(_inputs(
        call_wall=606.0, put_wall=594.0,
        call_wall_0dte=602.0, put_wall_0dte=598.0,   # tighter 0DTE
        is_opex_friday=True,
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    full_only = compute_forecast(_inputs(
        call_wall=606.0, put_wall=594.0,
        is_opex_friday=True,
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    # With tighter 0DTE walls blended, band width should be strictly narrower
    # than with the full-chain walls alone (after the same OPEX widen).
    assert (both.projected_high - both.projected_low) < \
           (full_only.projected_high - full_only.projected_low)


# ---------------------------------------------------------------------------
# Pin strike + dynamic tolerance
# ---------------------------------------------------------------------------


def test_pin_strike_prefers_max_pain():
    result = compute_forecast(_inputs())
    assert result.pin_strike == 599.0


def test_pin_strike_falls_back_to_nearest_strike():
    result = compute_forecast(_inputs(max_pain=None, strike_step=5.0))
    assert result.pin_strike == 600.0


def test_pin_tolerance_dynamic_spy():
    result = compute_forecast(_inputs(strike_step=1.0))
    # v1.3: max(1.0 * 0.5, 600 * 0.0015) = max(0.5, 0.9) = 0.9
    assert result.pin_tolerance == pytest.approx(0.9, abs=0.005)


def test_pin_tolerance_dynamic_spx():
    result = compute_forecast(_inputs(spot=5000.0, strike_step=5.0))
    # v1.3: max(5.0 * 0.5, 5000 * 0.0015) = max(2.5, 7.5) = 7.5
    assert result.pin_tolerance == pytest.approx(7.5, abs=0.005)


def test_projected_close_clamped_into_band():
    result = compute_forecast(_inputs(max_pain=650.0))
    assert result.projected_low <= result.projected_close <= result.projected_high


# ---------------------------------------------------------------------------
# Regime
# ---------------------------------------------------------------------------


def test_regime_long_gamma_when_msi_positive():
    result = compute_forecast(_inputs(msi_composite=0.4))
    assert result.regime == "long_gamma"


def test_regime_short_gamma_when_msi_negative():
    result = compute_forecast(_inputs(msi_composite=-0.4))
    assert result.regime == "short_gamma"


def test_regime_transition_when_msi_near_zero():
    result = compute_forecast(_inputs(msi_composite=0.08))
    assert result.regime == "transition"


# ---------------------------------------------------------------------------
# Layer-2 calibration
# ---------------------------------------------------------------------------


def test_calibration_widens_band_and_records_raw():
    raw = compute_forecast(_inputs(
        vix_close=None, msi_normalized=0.0, atr_5d=None,
    ))
    corrected = compute_forecast(_inputs(
        vix_close=None, msi_normalized=0.0, atr_5d=None,
        calibration={
            "band_width_mult": 1.20,
            "pin_tolerance_mult": 1.0,
            "upside_lean": 0.0,
            "downside_lean": 0.0,
        },
    ))
    # Corrected band is 20% wider.
    corrected_width = corrected.projected_high - corrected.projected_low
    raw_width = raw.projected_high - raw.projected_low
    assert corrected_width == pytest.approx(raw_width * 1.20, abs=0.05)
    # But the RAW pre-correction band is stored on the corrected result
    # too — for the receipt writer to grade separately.
    assert corrected.raw_projected_low == pytest.approx(raw.projected_low, abs=0.02)
    assert corrected.raw_projected_high == pytest.approx(raw.projected_high, abs=0.02)


def test_calibration_lean_shifts_asymmetrically():
    result = compute_forecast(_inputs(
        vix_close=None, msi_normalized=0.0, atr_5d=None,
        calibration={
            "band_width_mult": 1.0,
            "pin_tolerance_mult": 1.0,
            "upside_lean": 0.10,   # +10% wider upside
            "downside_lean": -0.05,  # -5% tighter downside
        },
    ))
    up = result.projected_high - 600.0
    down = 600.0 - result.projected_low
    # Upside > downside now
    assert up > down


def test_neutral_calibration_is_noop():
    a = compute_forecast(_inputs())
    b = compute_forecast(_inputs(calibration={
        "band_width_mult": 1.0,
        "pin_tolerance_mult": 1.0,
        "upside_lean": 0.0,
        "downside_lean": 0.0,
    }))
    assert a.projected_low == pytest.approx(b.projected_low, abs=0.001)
    assert a.projected_high == pytest.approx(b.projected_high, abs=0.001)


def test_deterministic_for_same_inputs():
    a = compute_forecast(_inputs())
    b = compute_forecast(_inputs())
    assert a.projected_low == b.projected_low
    assert a.projected_high == b.projected_high
    assert a.pin_strike == b.pin_strike
    assert a.regime == b.regime
