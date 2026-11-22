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


def test_below_min_premium_dampens_score():
    """Sub-noise flow tapers the score by a confidence ramp instead of hard-zeroing.

    With ``$50k`` gross / ``$100k`` min premium the confidence is 0.5; the
    ratio is ``+0.20`` saturated to ``+0.40``, scaled to ``+0.20``.
    """
    cmp = OrderFlowImbalanceComponent()
    score = cmp.compute(_ctx(smart_call=30_000.0, smart_put=20_000.0))
    assert abs(score - 0.20) < 1e-9


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


def _ctx_with_gross(*, call_net: float, put_net: float, call_gross: float, put_gross: float):
    ctx = _ctx(smart_call=call_net, smart_put=put_net)
    ctx.extra = {"smart_call_gross": call_gross, "smart_put_gross": put_gross}
    return ctx


def test_a_small_opposite_signed_net_on_a_heavy_two_way_tape_is_near_neutral():
    # Regression: dividing the two signed NETS by |call| + |put| read +1.00 for
    # any opposite-signed pair, however large the tape. A $54M two-way session
    # with calls net +$60k and puts net -$50k is barely tilted.
    cmp = OrderFlowImbalanceComponent()
    score = cmp.compute(
        _ctx_with_gross(call_net=60_000.0, put_net=-50_000.0, call_gross=30e6, put_gross=24e6)
    )
    assert abs(score) < 0.01
    flipped = cmp.compute(
        _ctx_with_gross(call_net=-60_000.0, put_net=50_000.0, call_gross=30e6, put_gross=24e6)
    )
    assert abs(flipped) < 0.01


def test_a_genuinely_one_sided_tape_still_saturates():
    # Calls bought $3M of a $4M gross, puts net 0: ratio 0.75 > saturation 0.5.
    cmp = OrderFlowImbalanceComponent()
    score = cmp.compute(
        _ctx_with_gross(call_net=3e6, put_net=0.0, call_gross=3.5e6, put_gross=0.5e6)
    )
    assert score == 1.0


def test_positioning_trap_and_market_pressure_use_the_same_gross_denominator():
    from src.signals.components.utils import net_flow_ratio

    ratio, gross = net_flow_ratio(
        60_000.0, -50_000.0, {"smart_call_gross": 30e6, "smart_put_gross": 24e6}
    )
    assert gross == 54e6
    assert abs(ratio) < 0.003
    # No gross on the context: the old bounded fallback.
    ratio, gross = net_flow_ratio(750_000.0, 250_000.0, None)
    assert (ratio, gross) == (0.5, 1_000_000.0)
