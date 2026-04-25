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


def test_thin_premium_ignored():
    ctx = _ctx()
    ctx.extra["flow_by_type"] = [
        {"option_type": "C", "buy_premium": 1000, "sell_premium": 0},
    ]
    assert comp.compute(ctx) == 0.0


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
