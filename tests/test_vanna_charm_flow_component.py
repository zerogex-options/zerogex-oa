"""Tests for vanna_charm_flow component."""
from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.components.vanna_charm_flow import (
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


def test_positive_vanna_is_bullish():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "vanna_exposure": _VC_NORM, "charm_exposure": 0.0}
    ]
    assert comp.compute(ctx) > 0.5


def test_negative_vanna_is_bearish():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "vanna_exposure": -_VC_NORM, "charm_exposure": 0.0}
    ]
    assert comp.compute(ctx) < -0.5


def test_charm_amplification_near_close():
    """Charm should count more near the close than in the morning."""
    rows = [
        {"strike": 500.0, "vanna_exposure": 0.0, "charm_exposure": _VC_NORM / 4}
    ]
    morning = _ctx(hour=14, minute=0)
    morning.extra["gex_by_strike"] = rows
    close = _ctx(hour=19, minute=55)
    close.extra["gex_by_strike"] = rows
    assert abs(comp.compute(close)) >= abs(comp.compute(morning))


def test_charm_amplification_max_at_close():
    assert comp._charm_amplification(_ctx(hour=20, minute=0)) == _CHARM_AMP_MAX


def test_score_bounded():
    ctx = _ctx()
    ctx.extra["gex_by_strike"] = [
        {"strike": 500.0, "vanna_exposure": 1e20, "charm_exposure": 1e20}
    ]
    assert abs(comp.compute(ctx)) <= 1.0


def test_context_values_unavailable():
    cv = comp.context_values(_ctx())
    assert cv["source"] == "unavailable"
