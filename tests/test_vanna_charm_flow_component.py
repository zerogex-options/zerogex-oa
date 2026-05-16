"""Tests for vanna_charm_flow component."""

from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.basic.vanna_charm_flow import (
    VannaCharmFlowComponent,
    _VC_NORM,
    _CHARM_AMP_MAX,
)


def _ctx(hour=16, minute=0, **overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 14, hour, minute, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=-2.0e8,
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


comp = VannaCharmFlowComponent()


def test_no_data_is_neutral():
    assert comp.compute(_ctx()) == 0.0


def test_positive_dealer_vanna_is_bullish():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "dealer_vanna_exposure": _VC_NORM, "dealer_charm_exposure": 0.0}
    ]
    assert comp.compute(ctx) > 0.5


def test_negative_dealer_vanna_is_bearish():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "dealer_vanna_exposure": -_VC_NORM, "dealer_charm_exposure": 0.0}
    ]
    assert comp.compute(ctx) < -0.5


def test_legacy_market_aggregate_vanna_negated():
    """Legacy rows (vanna_exposure only) use market-aggregate sign; the
    component must negate them to get dealer-sign convention."""
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "vanna_exposure": _VC_NORM, "charm_exposure": 0.0}
    ]
    # Market-aggregate +VC_NORM ⇒ dealer_vanna = -VC_NORM ⇒ bearish score.
    assert comp.compute(ctx) < -0.5


def test_charm_amplification_near_close():
    """Charm should count more near the close than in the morning."""
    rows = [{"strike": 500.0, "dealer_vanna_exposure": 0.0, "dealer_charm_exposure": _VC_NORM / 4}]
    morning = _ctx(hour=14, minute=0)  # 10:00 ET
    morning.extra["gex_by_strike"] = rows
    close = _ctx(hour=19, minute=55)  # 15:55 ET
    close.extra["gex_by_strike"] = rows
    assert abs(comp.compute(close)) >= abs(comp.compute(morning))


def test_charm_amplification_max_at_close():
    assert comp._charm_amplification(_ctx(hour=20, minute=0)) == _CHARM_AMP_MAX


def test_score_bounded():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "dealer_vanna_exposure": 1e20, "dealer_charm_exposure": 1e20}
    ]
    assert abs(comp.compute(ctx)) <= 1.0


def test_context_values_unavailable():
    cv = comp.context_values(_ctx())
    assert cv["source"] == "unavailable"


def test_scale_invariant_when_field_and_normalizer_rescale_together():
    """Behavior-preservation guarantee for the #4 unit fix: if the
    stored vanna unit changes by a factor k, the data-derived
    per-symbol normalizer (p95 of the same column) changes by k too, so
    the score is unchanged.  Pin that property explicitly."""
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "dealer_vanna_exposure": 3.7e6, "dealer_charm_exposure": -2.1e8}
    ]
    ctx.extra["normalizers"] = {
        "dealer_vanna_exposure": 1.0e7,
        "dealer_charm_exposure": 9.0e8,
    }
    before = comp.compute(ctx)

    k = 0.01  # the exact storage rescale applied to vanna in #4
    ctx.extra["gex_by_strike"] = [
        {
            "strike": 500.0,
            "dealer_vanna_exposure": 3.7e6 * k,
            "dealer_charm_exposure": -2.1e8,  # charm unit unchanged
        }
    ]
    ctx.extra["normalizers"] = {
        "dealer_vanna_exposure": 1.0e7 * k,  # cache p95 scales with the column
        "dealer_charm_exposure": 9.0e8,
    }
    after = comp.compute(ctx)
    assert abs(after - before) < 1e-12


def test_vanna_and_charm_normalized_independently():
    """A charm value at its own scale must not be drowned/inflated by
    vanna being on a different dollar scale (the old single-norm bug)."""
    ctx = _ctx(hour=14)  # morning: charm_amplification == 1.0
    ctx.extra["normalizers"] = {
        "dealer_vanna_exposure": 1.0e7,
        "dealer_charm_exposure": 1.0e9,
    }
    # Vanna negligible vs its scale, charm exactly at its own scale.
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "dealer_vanna_exposure": 1.0e3, "dealer_charm_exposure": 1.0e9}
    ]
    score = comp.compute(ctx)
    # c_term saturates (1e9/1e9 = 1), vanna ~0 → score ≈ +1.
    assert score > 0.95


def test_dynamic_normalizer_from_context_reduces_score_magnitude():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "dealer_vanna_exposure": _VC_NORM, "dealer_charm_exposure": 0.0}
    ]
    base = comp.compute(ctx)
    ctx.extra["normalizers"] = {
        "dealer_vanna_exposure": _VC_NORM * 4.0,
        "dealer_charm_exposure": _VC_NORM * 4.0,
    }
    scaled = comp.compute(ctx)
    assert abs(scaled) < abs(base)
