"""Tests for DealerRegimeComponent scoring behavior."""

from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.components.dealer_regime import DealerRegimeComponent


def _ctx(**overrides) -> MarketContext:
    base = MarketContext(
        timestamp=datetime(2026, 4, 14, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=693.0,
        net_gex=1.0,
        gamma_flip=690.0,
        put_call_ratio=1.0,
        max_pain=690.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[689.0, 690.0, 691.0, 692.0, 693.0],
        iv_rank=0.5,
        vwap=692.0,
        extra={"call_wall": 700.0, "max_gamma_strike": 691.0},
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def test_drs_strong_support_maps_to_high_positive_score():
    component = DealerRegimeComponent()
    score = component.compute(_ctx())
    # +30 +25 +10 +15 +10 = +90
    assert score == 0.9


def test_drs_breakdown_maps_to_negative_score():
    component = DealerRegimeComponent()
    ctx = _ctx(
        close=687.0,
        net_gex=-1.0,
        gamma_flip=690.0,
        vwap=689.0,
        recent_closes=[692.0, 691.0, 690.0, 688.5, 687.0],
        extra={"call_wall": 700.0, "max_gamma_strike": 691.0},
    )
    score = component.compute(ctx)
    # -30 -25 +0 -15 -10 = -80
    assert score == -0.8


def test_drs_handles_missing_optional_inputs():
    component = DealerRegimeComponent()
    ctx = _ctx(gamma_flip=None, vwap=None, extra={})
    score = component.compute(ctx)
    # net_gex + momentum fallback only
    assert score == 0.4
