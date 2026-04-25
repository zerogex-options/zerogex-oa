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
    assert trap.score == 0.0
    assert trap.context["triggered"] is False


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
