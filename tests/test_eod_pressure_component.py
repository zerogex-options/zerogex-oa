"""Tests for eod_pressure component."""

from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.advanced.eod_pressure import EODPressureSignal, _CHARM_NORM


def _ctx(hour=19, minute=50, **overrides) -> MarketContext:
    # Default timestamp: 19:50 UTC = 15:50 ET = T-10min; Tuesday 2026-04-14
    # (not OpEx, not quad-witching).
    defaults = dict(
        timestamp=datetime(2026, 4, 14, hour, minute, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=2.0e8,  # positive gamma by default
        gamma_flip=500.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[500.0] * 5,
        iv_rank=None,
    )
    defaults.update(overrides)
    return MarketContext(**defaults)


comp = EODPressureSignal()


def test_pre_window_is_neutral():
    # 13:00 UTC = 09:00 ET; before open. Must return 0 regardless of inputs.
    ctx = _ctx(hour=13, minute=0)
    ctx.extra["gex_by_strike"] = [{"strike": 500.0, "dealer_charm_exposure": _CHARM_NORM}]
    assert comp.compute(ctx) == 0.0


def test_morning_is_neutral():
    # 17:00 UTC = 13:00 ET, T-180min. Still before EOD window.
    ctx = _ctx(hour=17, minute=0)
    ctx.extra["gex_by_strike"] = [{"strike": 500.0, "dealer_charm_exposure": _CHARM_NORM}]
    assert comp.compute(ctx) == 0.0


def test_post_close_is_neutral():
    ctx = _ctx(hour=20, minute=30)
    ctx.extra["gex_by_strike"] = [{"strike": 500.0, "dealer_charm_exposure": _CHARM_NORM}]
    assert comp.compute(ctx) == 0.0


def test_positive_dealer_charm_at_spot_is_bullish():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "dealer_charm_exposure": _CHARM_NORM},
        {"strike": 500.5, "dealer_charm_exposure": _CHARM_NORM * 0.5},
    ]
    # Kill pin gravity so charm dominates the signal.
    ctx.max_pain = ctx.close
    ctx.extra["max_gamma_strike"] = ctx.close
    assert comp.compute(ctx) > 0.3


def test_negative_dealer_charm_at_spot_is_bearish():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [{"strike": 500.0, "dealer_charm_exposure": -_CHARM_NORM}]
    ctx.max_pain = ctx.close
    ctx.extra["max_gamma_strike"] = ctx.close
    assert comp.compute(ctx) < -0.3


def test_legacy_market_aggregate_charm_is_negated():
    """Legacy rows with raw ``charm_exposure`` only: market-aggregate
    positive => dealer negative => bearish EOD lean."""
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "charm_exposure": _CHARM_NORM},
    ]
    ctx.max_pain = ctx.close
    ctx.extra["max_gamma_strike"] = ctx.close
    assert comp.compute(ctx) < -0.3


def test_out_of_band_charm_is_ignored():
    """Charm on strikes far from spot should not contribute."""
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 600.0, "dealer_charm_exposure": _CHARM_NORM}  # 20% OTM
    ]
    ctx.max_pain = ctx.close
    ctx.extra["max_gamma_strike"] = ctx.close
    assert comp.compute(ctx) == 0.0


def test_pin_gravity_positive_gamma_pulls_toward_pin():
    """Pin above spot in positive-gamma regime => bullish EOD lean."""
    ctx = _ctx(net_gex=5.0e8)  # strongly positive gamma
    ctx.close = 495.0
    ctx.max_pain = 500.0
    ctx.extra["max_gamma_strike"] = 500.0
    # No charm in context.
    assert comp.compute(ctx) > 0.1


def test_pin_gravity_negative_gamma_repels_from_pin():
    """Pin above spot in negative-gamma regime => bearish EOD lean (amplifies away)."""
    ctx = _ctx(net_gex=-5.0e8)  # negative gamma
    ctx.close = 495.0
    ctx.max_pain = 500.0
    ctx.extra["max_gamma_strike"] = 500.0
    assert comp.compute(ctx) < -0.1


def test_ramp_grows_into_close():
    rows = [{"strike": 500.0, "dealer_charm_exposure": _CHARM_NORM * 0.5}]
    far = _ctx(hour=19, minute=0)  # T-60min
    far.extra["gex_by_strike"] = rows
    far.max_pain = far.close
    far.extra["max_gamma_strike"] = far.close

    near = _ctx(hour=19, minute=50)  # T-10min
    near.extra["gex_by_strike"] = rows
    near.max_pain = near.close
    near.extra["max_gamma_strike"] = near.close

    assert abs(comp.compute(near)) > abs(comp.compute(far))


def test_opex_amplifier():
    """2026-04-17 is the 3rd Friday of April (OpEx, non-quad)."""
    base_rows = [{"strike": 500.0, "dealer_charm_exposure": _CHARM_NORM * 0.3}]
    non_opex = MarketContext(
        timestamp=datetime(2026, 4, 14, 19, 50, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=2.0e8,
        gamma_flip=500.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[500.0] * 5,
        iv_rank=None,
    )
    non_opex.extra["gex_by_strike"] = base_rows
    non_opex.extra["max_gamma_strike"] = 500.0

    opex = MarketContext(
        timestamp=datetime(2026, 4, 17, 19, 50, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=2.0e8,
        gamma_flip=500.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[500.0] * 5,
        iv_rank=None,
    )
    opex.extra["gex_by_strike"] = base_rows
    opex.extra["max_gamma_strike"] = 500.0

    assert abs(comp.compute(opex)) > abs(comp.compute(non_opex))


def test_quad_witching_flag():
    # 2026-03-20 is 3rd Friday of March => quad-witching.
    quad = datetime(2026, 3, 20, 19, 50, tzinfo=timezone.utc)
    flags = comp._calendar_flags(quad)
    assert flags["opex"] is True
    assert flags["quad_witching"] is True


def test_score_bounded():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [{"strike": 500.0, "dealer_charm_exposure": 1e20}]
    ctx.close = 400.0
    ctx.max_pain = 500.0
    ctx.extra["max_gamma_strike"] = 500.0
    assert abs(comp.compute(ctx)) <= 1.0


def test_context_values_shape():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [{"strike": 500.0, "dealer_charm_exposure": _CHARM_NORM * 0.2}]
    cv = comp.context_values(ctx)
    assert "time_ramp" in cv
    assert "charm_at_spot" in cv
    assert "pin_target" in cv
    assert "gamma_regime" in cv
    assert cv["gamma_regime"] == "positive"
    assert "calendar_flags" in cv


def test_zero_pin_anchor_does_not_saturate_pin_gravity():
    """``_calculate_max_pain`` historically returned 0.0 on empty option
    sets and the signal then treated 0.0 as a legitimate pin, saturating
    ``_pin_gravity_score`` to ±1.0. After the fix the signal must drop
    out (score 0) instead of swinging hard with no anchor.
    """
    ctx = _ctx(max_pain=0.0)
    # No max_gamma_strike fallback either — signal should fall through
    # to "no pin anchor".
    ctx.extra.pop("max_gamma_strike", None)
    # No charm data either, so the only possible contribution would have
    # been the (broken) pin-gravity saturation.
    ctx.extra.pop("gex_by_strike", None)
    assert comp.compute(ctx) == 0.0


def test_zero_score_in_window_logs_warning(caplog):
    """Active-window readings of exactly zero point to missing inputs;
    surface the diagnosis in the engine log so on-call can see why."""
    import logging

    ctx = _ctx(max_pain=None)
    ctx.extra.pop("max_gamma_strike", None)
    ctx.extra.pop("gex_by_strike", None)
    caplog.set_level(logging.WARNING, logger="src.signals.advanced.eod_pressure")
    score = comp.compute(ctx)
    assert score == 0.0
    assert any(
        "eod_pressure score=0" in record.getMessage()
        for record in caplog.records
    ), "expected a WARN-level diagnostic when the signal collapses in the window"


def test_zero_score_outside_window_is_silent(caplog):
    """Pre-window zeros are by design (ramp == 0). Don't spam the log."""
    import logging

    ctx = _ctx(hour=13, minute=0, max_pain=None)
    ctx.extra.pop("max_gamma_strike", None)
    ctx.extra.pop("gex_by_strike", None)
    caplog.set_level(logging.WARNING, logger="src.signals.advanced.eod_pressure")
    score = comp.compute(ctx)
    assert score == 0.0
    assert not any(
        "eod_pressure score=0" in record.getMessage()
        for record in caplog.records
    ), "ramp-gated zero score should not trip the diagnostic warning"
