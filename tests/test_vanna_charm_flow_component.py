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
    rows = [
        {"strike": 500.0, "dealer_vanna_exposure": 0.0, "dealer_charm_exposure": _VC_NORM / 4}
    ]
    morning = _ctx(hour=14, minute=0)  # 10:00 ET
    morning.extra["gex_by_strike"] = rows
    close = _ctx(hour=19, minute=55)   # 15:55 ET
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
