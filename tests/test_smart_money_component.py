from datetime import datetime, timezone

import pytest

from src.signals.components.base import MarketContext
from src.signals.basic.smart_money import SmartMoneyComponent


comp = SmartMoneyComponent()


def _ctx(**kwargs) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=510.0,
        net_gex=-100_000_000,
        gamma_flip=508.0,
        put_call_ratio=1.0,
        max_pain=509.0,
        smart_call=500_000.0,
        smart_put=500_000.0,
        recent_closes=[510.0] * 5,
        iv_rank=None,
    )
    defaults.update(kwargs)
    return MarketContext(**defaults)


def test_low_flow_is_neutral():
    ctx = _ctx(smart_call=40_000, smart_put=40_000)
    assert comp.compute(ctx) == pytest.approx(0.0)


def test_put_skew_scores_bearish():
    ctx = _ctx(smart_call=200_000, smart_put=800_000)
    assert comp.compute(ctx) < 0


def test_call_skew_scores_bullish():
    ctx = _ctx(smart_call=800_000, smart_put=200_000)
    assert comp.compute(ctx) > 0


def test_up_momentum_plus_heavy_put_flow_adds_bearish_divergence():
    base = 510.0
    closes = [base] * 4 + [base * 1.002]  # +0.2% grind up
    ctx = _ctx(smart_call=200_000, smart_put=800_000, recent_closes=closes)
    score_with_divergence = comp.compute(ctx)

    ctx_no_momentum = _ctx(smart_call=200_000, smart_put=800_000, recent_closes=[base] * 5)
    score_without_divergence = comp.compute(ctx_no_momentum)

    assert score_with_divergence < score_without_divergence


def test_context_values_include_intraday_fields():
    cv = comp.context_values(_ctx())
    assert "imbalance" in cv
    assert "momentum_5bar" in cv


def test_signed_inputs_with_offsetting_flows_stay_neutral():
    # Large but offsetting signed flow should not force a directional read.
    ctx = _ctx(smart_call=500_000, smart_put=500_000)
    assert comp.compute(ctx) == pytest.approx(0.0)
