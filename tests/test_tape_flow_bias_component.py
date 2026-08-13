"""Tests for tape_flow_bias component."""

from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.basic.tape_flow_bias import TapeFlowBiasComponent


def _ctx(**overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 14, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=0.0,
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


comp = TapeFlowBiasComponent()


def test_no_data_is_neutral():
    assert comp.compute(_ctx()) == 0.0


def test_thin_premium_dampened():
    """Thin flow tapers via a confidence ramp instead of hard-zeroing."""
    ctx = _ctx()
    ctx.extra["flow_by_type"] = [
        {"option_type": "C", "buy_premium": 1000, "sell_premium": 0},
    ]
    score = comp.compute(ctx)
    # Dampened toward zero (confidence ~ 1000/250000 ≈ 0.004) but non-zero.
    assert 0.0 < score < 0.05


def test_heavy_call_buying_is_bullish():
    ctx = _ctx()
    ctx.extra["flow_by_type"] = [
        {"option_type": "C", "buy_premium": 2_000_000, "sell_premium": 200_000},
        {"option_type": "P", "buy_premium": 100_000, "sell_premium": 100_000},
    ]
    assert comp.compute(ctx) > 0.5


def test_heavy_put_buying_is_bearish():
    ctx = _ctx()
    ctx.extra["flow_by_type"] = [
        {"option_type": "C", "buy_premium": 100_000, "sell_premium": 100_000},
        {"option_type": "P", "buy_premium": 2_000_000, "sell_premium": 200_000},
    ]
    assert comp.compute(ctx) < -0.5


def test_balanced_flow_is_neutral_ish():
    ctx = _ctx()
    ctx.extra["flow_by_type"] = [
        {"option_type": "C", "buy_premium": 500_000, "sell_premium": 500_000},
        {"option_type": "P", "buy_premium": 500_000, "sell_premium": 500_000},
    ]
    assert abs(comp.compute(ctx)) < 1e-9


def test_moderate_net_lean_is_not_saturated():
    """Regression: a moderate net lean sitting on heavy two-way premium must
    read as moderate, not pin to the ±1 extreme.

    Calls net -$400k and puts net +$400k on ~$4M gross flow is a bearish lean,
    but under the old net-sum normalization the ratio was
    (-400k - 400k) / (|-400k| + |400k|) = -1.0, saturating the score to -100.
    Gross normalization keeps it proportional: -800k / 4M = -0.2 → ~-0.4.
    """
    ctx = _ctx()
    ctx.extra["flow_by_type"] = [
        {"option_type": "C", "buy_premium": 1_000_000, "sell_premium": 1_400_000},
        {"option_type": "P", "buy_premium": 1_000_000, "sell_premium": 600_000},
    ]
    score = comp.compute(ctx)
    assert -0.6 < score < -0.2


def test_tiny_opposite_nets_do_not_saturate():
    """Regression: a near-flat two-way tape must stay near zero rather than
    saturating on a trivial opposite-signed net (the old failure mode)."""
    ctx = _ctx()
    ctx.extra["flow_by_type"] = [
        {"option_type": "C", "buy_premium": 495_000, "sell_premium": 505_000},
        {"option_type": "P", "buy_premium": 505_000, "sell_premium": 495_000},
    ]
    score = comp.compute(ctx)
    # directional = (-10k) - (+10k) = -20k on ~$2M gross → ratio ≈ -0.01.
    assert -0.05 < score < 0.0


def test_score_bounded():
    ctx = _ctx()
    ctx.extra["flow_by_type"] = [
        {"option_type": "C", "buy_premium": 1e12, "sell_premium": 0},
        {"option_type": "P", "buy_premium": 0, "sell_premium": 1e12},
    ]
    assert comp.compute(ctx) <= 1.0


def test_context_values_unavailable():
    cv = comp.context_values(_ctx())
    assert cv["source"] == "unavailable"
