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
    assert result.range_model == "heuristic_v1_4"


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


def test_regime_long_gamma_when_net_gex_positive():
    # Dealer regime is the sign of net GEX — positive = dealers long gamma.
    result = compute_forecast(_inputs(net_gex=5.0e9))
    assert result.regime == "long_gamma"


def test_regime_short_gamma_when_net_gex_negative():
    result = compute_forecast(_inputs(net_gex=-5.0e9))
    assert result.regime == "short_gamma"


def test_regime_ignores_msi_sign():
    # The MSI composite must NOT drive the regime anymore — a strongly
    # positive MSI with negative net_gex is still a short-gamma tape.
    result = compute_forecast(_inputs(net_gex=-5.0e9, msi_composite=0.9))
    assert result.regime == "short_gamma"


def test_regime_falls_back_to_spot_vs_flip_when_net_gex_missing():
    above = compute_forecast(_inputs(net_gex=None, spot=605.0, gamma_flip=600.0))
    assert above.regime == "long_gamma"
    below = compute_forecast(_inputs(net_gex=None, spot=595.0, gamma_flip=600.0))
    assert below.regime == "short_gamma"


def test_regime_transition_when_surface_stale():
    # Even with a clear net_gex sign, a stale/missing GEX surface degrades to
    # transition rather than asserting a positioning we can't substantiate.
    result = compute_forecast(_inputs(net_gex=5.0e9, gex_surface_fresh=False))
    assert result.regime == "transition"


def test_regime_transition_when_no_dealer_signal():
    result = compute_forecast(_inputs(net_gex=None, gamma_flip=None))
    assert result.regime == "transition"


# ---------------------------------------------------------------------------
# VIX-normalized regime threshold (v1.3)
# ---------------------------------------------------------------------------


def test_regime_threshold_scales_up_with_vix():
    """VIX=20 → implied ~1.26%; threshold = 0.6 × 0.0126 ≈ 0.756%.
    VIX=12 → implied ~0.756%; threshold ≈ 0.453% → clamped to 0.5%? No,
    floor is 0.3% so 0.453% is fine."""
    low = compute_forecast(_inputs(vix_close=12.0)).regime_move_threshold
    mid = compute_forecast(_inputs(vix_close=20.0)).regime_move_threshold
    high = compute_forecast(_inputs(vix_close=30.0)).regime_move_threshold
    assert low < mid < high


def test_regime_threshold_uses_vxn_for_nasdaq():
    """QQQ uses VXN, not VIX.  When VIX is None and VXN is provided,
    threshold should reflect VXN."""
    result = compute_forecast(_inputs(vix_close=None, vxn_close=22.0))
    # Should not be the fallback 0.5%
    assert result.regime_move_threshold != pytest.approx(0.005, abs=0.0001)
    assert result.regime_move_threshold > 0.003


def test_regime_threshold_falls_back_when_no_vol():
    """No VIX and no VXN → fall back to legacy 0.5% (0.005)."""
    result = compute_forecast(_inputs(vix_close=None, vxn_close=None))
    assert result.regime_move_threshold == pytest.approx(0.005, abs=0.0001)


def test_regime_threshold_clamped_at_floor():
    """VIX ~5 (basically 0) → threshold below floor → clamp to 0.3%."""
    result = compute_forecast(_inputs(vix_close=4.0))
    assert result.regime_move_threshold == pytest.approx(0.003, abs=0.0002)


def test_regime_threshold_clamped_at_ceiling():
    """VIX 60 → threshold above ceiling → clamp to 2%."""
    result = compute_forecast(_inputs(vix_close=60.0))
    assert result.regime_move_threshold == pytest.approx(0.020, abs=0.0002)


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


# ---------------------------------------------------------------------------
# v1.4 reframe — expected volatility + level touch odds (no direction claim)
# ---------------------------------------------------------------------------

from src.jobs.forecast_range_model import (  # noqa: E402
    LEVEL_TOUCH_MAX,
    LEVEL_TOUCH_MIN,
    RANGE_OVER_SIGMA,
    VOL_NORMAL_HIGH,
    VOL_NORMAL_LOW,
    _barrier_touch_prob,
    _classify_vol_state,
    implied_move_sanity,
    predict_expected_vol,
    robust_persistence_anchor,
)


def _vol_inputs(**overrides) -> ForecastInputs:
    """Inputs with the vol-character fields populated (VIX + gamma fresh)."""
    base = _inputs(
        vix_close=15.0,
        vix_z_score_20d=0.0,
        gex_surface_fresh=True,
        net_gex=0.0,
        local_gex=0.0,
        flip_distance=0.5,  # far from flip so proximity is a no-op
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def test_expected_vol_compression_on_strong_long_gamma():
    r = compute_forecast(_vol_inputs(net_gex=4.0e9, local_gex=6.0e8, vix_z_score_20d=-0.8))
    assert r.expected_vol_state == "compression"
    assert r.expected_vol_ratio < VOL_NORMAL_LOW
    assert r.implied_move is not None and r.implied_move > 0


def test_expected_vol_expansion_on_short_gamma_near_flip():
    r = compute_forecast(_vol_inputs(net_gex=-3.0e9, vix_z_score_20d=1.5, flip_distance=0.001))
    assert r.expected_vol_state == "expansion"
    assert r.expected_vol_ratio > VOL_NORMAL_HIGH


def test_expected_vol_normal_without_gamma_signal():
    # No net_gex and neutral VIX-z -> ratio stays ~1.0 -> normal (no strong call).
    r = compute_forecast(_vol_inputs(net_gex=None, vix_z_score_20d=0.0))
    assert r.expected_vol_state == "normal"
    assert VOL_NORMAL_LOW < r.expected_vol_ratio < VOL_NORMAL_HIGH


def test_expected_vol_ignores_gamma_when_surface_stale():
    fresh = compute_forecast(_vol_inputs(net_gex=4.0e9, gex_surface_fresh=True))
    stale = compute_forecast(_vol_inputs(net_gex=4.0e9, gex_surface_fresh=False))
    # A stale surface must not let a big net_gex drive the vol call.
    assert stale.expected_vol_ratio > fresh.expected_vol_ratio
    assert stale.expected_vol_state != "compression"


def test_level_touch_probs_present_and_bounded():
    r = compute_forecast(_vol_inputs(net_gex=1.0e9))
    assert set(r.level_touch_probs) == {"call_wall", "put_wall"}
    for p in r.level_touch_probs.values():
        assert LEVEL_TOUCH_MIN <= p <= LEVEL_TOUCH_MAX
    assert r.flip_cross_prob is not None


def test_level_touch_prob_decreases_with_distance():
    near = compute_forecast(_vol_inputs(call_wall=601.0))   # 1 pt away
    far = compute_forecast(_vol_inputs(call_wall=615.0))    # 15 pts away
    assert near.level_touch_probs["call_wall"] > far.level_touch_probs["call_wall"]


def test_wall_touch_odds_higher_in_short_gamma_than_long():
    long_g = compute_forecast(_vol_inputs(net_gex=3.0e9))    # dealers defend walls
    short_g = compute_forecast(_vol_inputs(net_gex=-3.0e9))  # walls give way
    assert short_g.level_touch_probs["call_wall"] > long_g.level_touch_probs["call_wall"]


def test_flip_cross_prob_not_regime_tilted():
    # Same geometry, opposite regime — the flip-cross prob must be identical
    # (crossing the flip IS the regime change, so it isn't regime-tilted).
    long_g = compute_forecast(_vol_inputs(net_gex=3.0e9))
    short_g = compute_forecast(_vol_inputs(net_gex=-3.0e9))
    assert long_g.flip_cross_prob == pytest.approx(short_g.flip_cross_prob)


def test_gravity_center_prefers_top_gamma_node():
    r = compute_forecast(_vol_inputs(
        top_gamma_nodes=[{"strike": 602.0, "net_gex": 2.0e8}], max_pain=599.0,
    ))
    assert r.gravity_center == pytest.approx(602.0)
    # Falls back to max_pain when no nodes.
    r2 = compute_forecast(_vol_inputs(top_gamma_nodes=[], max_pain=599.0))
    assert r2.gravity_center == pytest.approx(599.0)


def test_no_level_probs_without_implied_move():
    # No VIX/VXN -> no implied move -> touch odds can't be computed honestly.
    r = compute_forecast(_vol_inputs(vix_close=None, vxn_close=None))
    assert r.level_touch_probs == {}
    assert r.flip_cross_prob is None
    assert r.implied_move is None


def test_barrier_touch_prob_bounds_and_monotonic():
    assert _barrier_touch_prob(0.0, 10.0) == LEVEL_TOUCH_MAX      # on spot -> clamped ceiling
    assert _barrier_touch_prob(1000.0, 10.0) == LEVEL_TOUCH_MIN   # miles away -> clamped floor
    assert _barrier_touch_prob(5.0, 10.0) > _barrier_touch_prob(15.0, 10.0)
    assert _barrier_touch_prob(5.0, None) is None


def test_classify_vol_state_band_edges():
    assert _classify_vol_state(VOL_NORMAL_LOW) == "compression"
    assert _classify_vol_state(VOL_NORMAL_HIGH) == "expansion"
    assert _classify_vol_state(1.0) == "normal"


# ---------------------------------------------------------------------------
# v1.5 expected-vol rework — persistence anchor + gamma tilt
# ---------------------------------------------------------------------------


def test_persistence_anchor_needs_min_obs():
    # Below VOL_PERSISTENCE_MIN_OBS (5) there isn't enough history to trust it.
    assert robust_persistence_anchor([1.0, 1.1, 0.9]) is None
    # Exactly 5 → median of the observations.
    assert robust_persistence_anchor([0.6, 0.7, 0.6, 0.65, 0.6]) == pytest.approx(0.6)


def test_persistence_anchor_uses_recent_window_and_is_outlier_robust():
    # Only the trailing VOL_PERSISTENCE_WINDOW (10) count; two stale hot days
    # at the front are dropped, and the median ignores a single spike.
    ratios = [2.0, 2.0] + [0.5] * 9 + [3.0]
    assert robust_persistence_anchor(ratios) == pytest.approx(0.5)


def test_implied_move_sanity_flags_mis_scaled_move():
    assert implied_move_sanity(None, 5.0) is None
    assert implied_move_sanity(5.0, None) is None
    ok = implied_move_sanity(5.0, 6.5)          # ATR/implied = 1.3 → healthy
    assert ok["sane"] is True and ok["atr_over_implied"] == pytest.approx(1.3)
    too_big = implied_move_sanity(10.0, 4.0)    # ratio 0.4 → implied too large
    assert too_big["sane"] is False
    too_small = implied_move_sanity(1.0, 5.0)   # ratio 5.0 → implied too small
    assert too_small["sane"] is False


def test_predict_vol_anchors_on_persistence_not_flat_normal():
    # THE rework: a compression-heavy trailing regime (anchor 0.6) predicts
    # compression — NOT the flat-1.0 "normal" default that scored below the
    # realized base rate. No gamma signal, so the anchor drives it.
    state, ratio, msg = predict_expected_vol(
        persistence_anchor=0.6, atr_5d=None, implied_move=5.0, vol_basis=1.0,
        net_gex=None, gex_surface_fresh=True, local_gex=None,
        vix_z_score_20d=None, flip_distance=None,
    )
    assert ratio == pytest.approx(0.6)
    assert state == "compression"
    assert "anchor=persistence" in msg


def test_predict_vol_falls_back_to_atr_anchor():
    # No history → ATR anchor: recent avg range 1.4× the calibrated normal
    # range reads expansion.
    implied, basis = 5.0, 1.0
    atr = 1.4 * RANGE_OVER_SIGMA * basis * implied
    state, ratio, msg = predict_expected_vol(
        persistence_anchor=None, atr_5d=atr, implied_move=implied, vol_basis=basis,
        net_gex=None, gex_surface_fresh=True, local_gex=None,
        vix_z_score_20d=None, flip_distance=None,
    )
    assert ratio == pytest.approx(1.4, abs=1e-3)
    assert state == "expansion"
    assert "anchor=atr" in msg


def test_predict_vol_gamma_tilts_the_anchor():
    # Same neutral anchor; short gamma tilts up (expansion), long gamma down.
    def r(net_gex):
        return predict_expected_vol(
            persistence_anchor=1.0, atr_5d=None, implied_move=5.0, vol_basis=1.0,
            net_gex=net_gex, gex_surface_fresh=True, local_gex=None,
            vix_z_score_20d=None, flip_distance=None,
        )[1]
    assert r(-3.0e9) > r(None) > r(3.0e9)


def test_predict_vol_calibrated_basis_shifts_atr_anchor():
    # A VRP regime: the SAME recent ATR reads compression under basis 1.0 but
    # normal once the basis learns the symbol's lower center (0.7).
    implied = 5.0
    atr = 0.7 * RANGE_OVER_SIGMA * 1.0 * implied  # 0.7× the pure-Parkinson normal
    tight = predict_expected_vol(
        persistence_anchor=None, atr_5d=atr, implied_move=implied, vol_basis=1.0,
        net_gex=None, gex_surface_fresh=True, local_gex=None,
        vix_z_score_20d=None, flip_distance=None,
    )
    recentered = predict_expected_vol(
        persistence_anchor=None, atr_5d=atr, implied_move=implied, vol_basis=0.7,
        net_gex=None, gex_surface_fresh=True, local_gex=None,
        vix_z_score_20d=None, flip_distance=None,
    )
    assert tight[0] == "compression"
    assert recentered[1] == pytest.approx(1.0, abs=1e-3)
    assert recentered[0] == "normal"


# ---------------------------------------------------------------------------
# v1.4 receipt grading — grade_realized_claims (pure, magnitude-only)
# ---------------------------------------------------------------------------

from src.jobs.forecast_range_model import grade_realized_claims  # noqa: E402


def test_grade_normal_day_reads_normal_not_expansion():
    # THE regression test for the range/σ scale fix: a statistically ORDINARY
    # day realizes a high-low range of √(8/π)·implied ≈ 1.6× the 1-σ implied
    # move.  It must grade "normal" (ratio ≈ 1.0), NOT "expansion".  Before the
    # fix this exact day scored expansion at every VIX level.
    implied = 50.0
    normal_range = RANGE_OVER_SIGMA * implied  # ≈ 79.79
    g = grade_realized_claims(
        open_spot=600.0,
        actual_low=600.0 - normal_range / 2,
        actual_high=600.0 + normal_range / 2,
        implied_move=implied, expected_vol_state="normal",
        call_wall=None, put_wall=None, gamma_flip=None,
        level_touch_probs=None, flip_cross_prob=None,
    )
    assert g["realized_vol_ratio"] == pytest.approx(1.0, abs=1e-3)
    assert g["vol_state_correct"] is True


def test_grade_vol_compression_hit():
    # A genuinely quiet day: a $30 range against a $50 1-σ move is only ~0.38×
    # a normal day's range (√(8/π)·50 ≈ 80), well inside compression.
    g = grade_realized_claims(
        open_spot=600.0, actual_low=585.0, actual_high=615.0,  # range 30
        implied_move=50.0, expected_vol_state="compression",
        call_wall=None, put_wall=None, gamma_flip=None,
        level_touch_probs=None, flip_cross_prob=None,
    )
    assert g["realized_vol_ratio"] == pytest.approx(30.0 / (RANGE_OVER_SIGMA * 50.0), abs=1e-3)
    assert g["realized_vol_ratio"] < VOL_NORMAL_LOW
    assert g["vol_state_correct"] is True


def test_grade_expansion_is_path_independent():
    # A genuinely violent two-way day (range 120 ≈ 1.5× a normal day's range)
    # that closes right back at the open is still graded EXPANSION — the whole
    # point of the reframe. (Under the corrected scale a "violent" day is one
    # whose RANGE clears ~1.15× the ~1.6·σ normal range, not merely 1.15·σ.)
    g = grade_realized_claims(
        open_spot=600.0, actual_low=540.0, actual_high=660.0,  # range 120
        implied_move=50.0, expected_vol_state="expansion",
        call_wall=None, put_wall=None, gamma_flip=None,
        level_touch_probs=None, flip_cross_prob=None,
    )
    assert g["realized_vol_ratio"] == pytest.approx(120.0 / (RANGE_OVER_SIGMA * 50.0), abs=1e-3)
    assert g["realized_vol_ratio"] > VOL_NORMAL_HIGH
    assert g["vol_state_correct"] is True


def test_grade_vol_state_wrong_when_bucket_mismatches():
    g = grade_realized_claims(
        open_spot=600.0, actual_low=595.0, actual_high=605.0,  # range 10 -> compression
        implied_move=50.0, expected_vol_state="expansion",
        call_wall=None, put_wall=None, gamma_flip=None,
        level_touch_probs=None, flip_cross_prob=None,
    )
    assert g["vol_state_correct"] is False


def test_grade_levels_outcomes_and_flip_cross():
    g = grade_realized_claims(
        open_spot=600.0, actual_low=570.0, actual_high=615.0,
        implied_move=50.0, expected_vol_state="expansion",
        call_wall=610.0, put_wall=590.0, gamma_flip=595.0,
        level_touch_probs={"call_wall": 0.3, "put_wall": 0.4},
        flip_cross_prob=0.2,
    )
    assert g["level_touch_outcomes"] == {
        "call_wall": True, "put_wall": True, "gamma_flip": True,
    }
    assert g["flip_crossed"] is True
    # Brier: mean[(0.3-1)^2, (0.4-1)^2, (0.2-1)^2] = (0.49+0.36+0.64)/3
    assert g["levels_brier"] == pytest.approx((0.49 + 0.36 + 0.64) / 3, abs=1e-4)


def test_grade_flip_not_crossed_when_extreme_stays_one_side():
    g = grade_realized_claims(
        open_spot=600.0, actual_low=598.0, actual_high=615.0,
        implied_move=50.0, expected_vol_state="normal",
        call_wall=None, put_wall=None, gamma_flip=595.0,
        level_touch_probs=None, flip_cross_prob=0.1,
    )
    assert g["flip_crossed"] is False
    # Brier from the flip term only: (0.1 - 0)^2 = 0.01
    assert g["levels_brier"] == pytest.approx(0.01, abs=1e-4)


def test_grade_no_implied_move_leaves_vol_ungraded():
    g = grade_realized_claims(
        open_spot=600.0, actual_low=590.0, actual_high=610.0,
        implied_move=None, expected_vol_state="compression",
        call_wall=610.0, put_wall=590.0, gamma_flip=None,
        level_touch_probs={"call_wall": 0.5, "put_wall": 0.5}, flip_cross_prob=None,
    )
    assert g["realized_vol_ratio"] is None
    assert g["vol_state_correct"] is None
    # Levels still grade even without an implied move.
    assert g["level_touch_outcomes"]["call_wall"] is True
    assert g["levels_brier"] is not None
