import pytest

from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.advanced.engine import AdvancedSignalEngine

NOW = datetime(2026, 1, 2, 15, 0, tzinfo=timezone.utc)


def _ctx(**extra) -> MarketContext:
    overrides = dict(extra)
    base_extra = {
        "call_flow_delta": 0.0,
        "put_flow_delta": 0.0,
        "net_gex_delta": 0.0,
        "call_wall": 605.0,
        "max_gamma_strike": 602.0,
    }
    base_extra.update(overrides.pop("extra", {}))
    payload = dict(
        timestamp=NOW,
        underlying="SPY",
        close=600.0,
        net_gex=-100_000_000.0,
        gamma_flip=598.0,
        put_call_ratio=1.0,
        max_pain=600.0,
        smart_call=500_000.0,
        smart_put=400_000.0,
        recent_closes=[595.0, 596.0, 597.0, 598.0, 599.0, 600.0],
        iv_rank=0.4,
        vwap=601.0,
        extra=base_extra,
    )
    payload.update(overrides)
    return MarketContext(**payload)


def test_squeeze_setup_bullish_triggers():
    engine = AdvancedSignalEngine()
    ctx = _ctx(
        close=601.0,
        net_gex=-250_000_000.0,
        gamma_flip=599.0,
        extra={"call_flow_delta": 300_000.0, "put_flow_delta": -50_000.0},
    )
    results = {r.name: r for r in engine.evaluate(ctx)}
    squeeze = results["squeeze_setup"]
    assert squeeze.score > 0
    assert squeeze.context["signal"] == "bullish_squeeze"


def test_trap_detection_bearish_fade_on_upside_breakout():
    engine = AdvancedSignalEngine()
    ctx = _ctx(
        close=606.0,
        net_gex=300_000_000.0,
        extra={
            "net_gex_delta": 700_000_000.0,
            "net_gex_delta_pct": 0.02,  # +2% of prior book → strengthening
            "call_wall": 603.0,
            "prior_call_wall": 603.0,  # wall did not migrate up
            "max_gamma_strike": 602.0,
        },
    )
    results = {r.name: r for r in engine.evaluate(ctx)}
    trap = results["trap_detection"]
    assert trap.score < 0
    assert trap.context["signal"] == "bearish_fade"


def test_trap_detection_neutral_without_strengthening_gamma():
    engine = AdvancedSignalEngine()
    ctx = _ctx(
        close=606.0,
        net_gex=300_000_000.0,
        extra={
            "net_gex_delta": -200_000_000.0,
            "net_gex_delta_pct": -0.01,
            "call_wall": 603.0,
            "prior_call_wall": 603.0,
            "max_gamma_strike": 602.0,
        },
    )
    results = {r.name: r for r in engine.evaluate(ctx)}
    trap = results["trap_detection"]
    assert abs(trap.score) < 0.15
    assert trap.context["triggered"] is False


def test_trap_detection_bullish_fade_on_downside_breakdown():
    engine = AdvancedSignalEngine()
    ctx = _ctx(
        close=594.0,
        net_gex=300_000_000.0,
        gamma_flip=598.0,
        vwap=599.0,
        extra={
            "net_gex_delta": 700_000_000.0,
            "net_gex_delta_pct": 0.02,
            "put_wall": 597.0,
            "prior_put_wall": 597.0,  # put wall did not migrate down
            "max_gamma_strike": 598.0,
        },
    )
    results = {r.name: r for r in engine.evaluate(ctx)}
    trap = results["trap_detection"]
    assert trap.score > 0
    assert trap.context["signal"] == "bullish_fade"
    assert trap.context["put_wall"] == 597.0
    assert trap.context["put_wall_migrated_down"] is False


def test_trap_detection_bullish_fade_invalidated_when_put_wall_migrates_down():
    engine = AdvancedSignalEngine()
    base = dict(
        close=594.0,
        net_gex=300_000_000.0,
        gamma_flip=598.0,
        vwap=599.0,
    )
    held = _ctx(
        **base,
        extra={
            "net_gex_delta": 700_000_000.0,
            "net_gex_delta_pct": 0.02,
            "put_wall": 597.0,
            "prior_put_wall": 597.0,
            "max_gamma_strike": 598.0,
        },
    )
    migrated = _ctx(
        **base,
        extra={
            "net_gex_delta": 700_000_000.0,
            "net_gex_delta_pct": 0.02,
            "put_wall": 593.0,  # support shifted down by ~0.7%
            "prior_put_wall": 597.0,
            "max_gamma_strike": 598.0,
        },
    )
    held_trap = {r.name: r for r in engine.evaluate(held)}["trap_detection"]
    migrated_trap = {r.name: r for r in engine.evaluate(migrated)}["trap_detection"]
    assert migrated_trap.context["put_wall_migrated_down"] is True
    assert migrated_trap.score < held_trap.score


def test_zero_dte_position_imbalance_call_heavy():
    engine = AdvancedSignalEngine()
    ctx = _ctx(
        put_call_ratio=0.82,
        smart_call=800_000.0,
        smart_put=250_000.0,
        extra={
            "flow_by_type": [
                {"option_type": "C", "buy_premium": 900_000.0, "sell_premium": 250_000.0},
                {"option_type": "P", "buy_premium": 200_000.0, "sell_premium": 350_000.0},
            ]
        },
    )
    results = {r.name: r for r in engine.evaluate(ctx)}
    imbalance = results["zero_dte_position_imbalance"]
    assert imbalance.score > 0.25
    assert imbalance.context["signal"] == "call_heavy"


def test_gamma_vwap_confluence_bullish_when_levels_cluster_and_price_above():
    engine = AdvancedSignalEngine()
    ctx = _ctx(
        close=600.8,
        gamma_flip=600.0,
        vwap=600.1,
        net_gex=-200_000_000.0,
    )
    results = {r.name: r for r in engine.evaluate(ctx)}
    confluence = results["gamma_vwap_confluence"]
    assert confluence.score > 0.2
    assert confluence.context["signal"] == "bullish_confluence"
    # Continuation away from the cluster, so the target sits beyond spot on the
    # side price already left. Only ever checked on the bullish side before,
    # which is exactly why the sign error below went unnoticed.
    assert confluence.context["expected_target"] > ctx.close


def test_gamma_vwap_confluence_bearish_continuation_targets_below_spot():
    """A bearish continuation must project DOWN, not up.

    Reported from a live SPY card: score -37.92 with spot 770.19 and the
    cluster at 771.35, but an expected target of 772.50 -- above spot, with an
    up arrow, while the signal read bearish. The target was computed as
    ``close + dir_sign * (close - confluence_level) * 2``, and because
    ``dir_sign`` was the sign of that same quantity the product collapsed to
    ``abs(close - confluence_level)``: always positive, so the continuation
    target always pointed upward. Bullish continuations hid it; bearish ones
    contradicted their own score.
    """
    engine = AdvancedSignalEngine()
    ctx = _ctx(
        close=770.19,
        gamma_flip=772.28,
        vwap=770.41,
        max_pain=770.00,
        net_gex=-200_000_000.0,
        extra={"call_wall": 775.0, "max_gamma_strike": 760.0},
    )
    confluence = {r.name: r for r in engine.evaluate(ctx)}["gamma_vwap_confluence"]
    ctxd = confluence.context

    assert confluence.score < -0.2
    assert ctxd["signal"] == "bearish_confluence"
    assert ctxd["regime_direction"] == "continuation"
    # Price sits BELOW the cluster, so continuation means further below it.
    assert ctxd["confluence_level"] > ctx.close
    assert ctxd["expected_target"] < ctx.close, "bearish target must not point up"
    assert ctxd["expected_target"] == pytest.approx(767.88, abs=0.05)


def test_gamma_vwap_confluence_target_never_contradicts_its_own_score():
    """The invariant behind both cases above, stated once.

    Whatever the regime, a continuation target must land on the same side of
    spot as the score's sign. A card whose number says bearish and whose arrow
    says up is not a judgement call, it is a defect.
    """
    engine = AdvancedSignalEngine()
    for close, flip, vwap in ((600.8, 600.0, 600.1), (770.19, 772.28, 770.41)):
        ctx = _ctx(close=close, gamma_flip=flip, vwap=vwap, net_gex=-200_000_000.0)
        c = {r.name: r for r in engine.evaluate(ctx)}["gamma_vwap_confluence"]
        if c.context["regime_direction"] != "continuation" or abs(c.score) < 0.2:
            continue
        assert (c.context["expected_target"] - close) * c.score > 0, (
            f"target {c.context['expected_target']} contradicts score {c.score} "
            f"at spot {close}"
        )
