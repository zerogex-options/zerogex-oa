"""Tests for the order_flow_imbalance MSI component (Phase 3.1)."""

from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.components.order_flow_imbalance import OrderFlowImbalanceComponent


def _ctx(*, smart_call: float, smart_put: float) -> MarketContext:
    return MarketContext(
        timestamp=datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=0.0,
        gamma_flip=500.0,
        put_call_ratio=1.0,
        max_pain=None,
        smart_call=smart_call,
        smart_put=smart_put,
        recent_closes=[],
        iv_rank=None,
    )


def test_balanced_flow_yields_neutral_score():
    cmp = OrderFlowImbalanceComponent()
    assert cmp.compute(_ctx(smart_call=500_000.0, smart_put=500_000.0)) == 0.0


def test_call_dominant_flow_yields_positive_score():
    cmp = OrderFlowImbalanceComponent()
    score = cmp.compute(_ctx(smart_call=750_000.0, smart_put=250_000.0))
    # ratio = (750k - 250k) / 1M = 0.50, divided by saturation 0.50 = 1.0
    assert score == 1.0


def test_put_dominant_flow_yields_negative_score():
    cmp = OrderFlowImbalanceComponent()
    score = cmp.compute(_ctx(smart_call=200_000.0, smart_put=800_000.0))
    # ratio = (200k - 800k) / 1M = -0.60, divided by saturation 0.50 = -1.2 -> clamp -1.0
    assert score == -1.0


def test_below_min_premium_returns_zero():
    """Sub-noise flow ($50k total < $100k min premium) abstains."""
    cmp = OrderFlowImbalanceComponent()
    assert cmp.compute(_ctx(smart_call=30_000.0, smart_put=20_000.0)) == 0.0


def test_score_saturates_at_plus_one_on_extreme_imbalance():
    cmp = OrderFlowImbalanceComponent()
    score = cmp.compute(_ctx(smart_call=10_000_000.0, smart_put=0.0))
    assert score == 1.0


def test_score_saturates_at_minus_one_on_extreme_put_imbalance():
    cmp = OrderFlowImbalanceComponent()
    score = cmp.compute(_ctx(smart_call=0.0, smart_put=10_000_000.0))
    assert score == -1.0


def test_context_values_record_inputs_and_score():
    cmp = OrderFlowImbalanceComponent()
    ctx = _ctx(smart_call=600_000.0, smart_put=400_000.0)
    payload = cmp.context_values(ctx)
    assert payload["smart_call_premium"] == 600_000.0
    assert payload["smart_put_premium"] == 400_000.0
    assert payload["gross_premium"] == 1_000_000.0
    assert payload["imbalance_ratio"] == 0.2
    # ratio 0.2 / saturation 0.5 = 0.4
    assert payload["score"] == 0.4
