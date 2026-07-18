"""Phase 3 — momentum + swing-reversal components, tactical fusion, and the
graded override (confirmed / divergent / override), plus an engine-level
end-to-end of the "bounce overrides the negative-gamma playbook" scenario.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from src.signals.components.base import MarketContext
from src.signals.components.momentum import MomentumComponent
from src.signals.components.swing_reversal import SwingReversalComponent, swing_reversal_score
from src.signals.trade_bias.bias import BiasInput, compute_bias
from src.signals.trade_bias.engine import TradeBiasEngine
from src.signals.trade_bias.fusion import (
    INTRADAY_PROFILE,
    SWING_PROFILE,
    compute_tactical,
    fuse,
    profile_for,
)


# --- fixtures --------------------------------------------------------------

def _rising(n=30):
    out, p = [], 100.0
    for i in range(n):
        p += 0.3 if i % 2 == 0 else -0.1  # net up, with variation so sigma > 0
        out.append(round(p, 4))
    return out


def _falling(n=30):
    out, p = [], 100.0
    for i in range(n):
        p += -0.3 if i % 2 == 0 else 0.1  # net down
        out.append(round(p, 4))
    return out


# Prior window flat at 100; recent window pierces to a new low then reclaims.
BOUNCE_CLOSES = [100.0] * 20 + [99.9, 98.0, 98.5, 99.5, 100.3]
BOUNCE_LOWS = [100.0] * 20 + [99.8, 97.8, 98.2, 99.3, 100.1]
BOUNCE_HIGHS = [100.0] * 20 + [100.0, 98.5, 99.0, 100.0, 100.5]

# Recent window pierces to a new high then fails back.
REJECT_CLOSES = [100.0] * 20 + [100.1, 102.0, 101.5, 100.2, 99.6]
REJECT_LOWS = [100.0] * 20 + [100.0, 101.0, 100.5, 99.5, 99.3]
REJECT_HIGHS = [100.0] * 20 + [100.3, 102.3, 101.8, 100.5, 99.9]


def _ctx(**over) -> MarketContext:
    base = dict(
        timestamp=datetime(2026, 7, 17, 18, 55, tzinfo=timezone.utc),
        underlying="SPY",
        close=100.3,
        net_gex=-1.2e9,
        gamma_flip=99.0,
        put_call_ratio=1.1,
        max_pain=99.0,
        smart_call=1.0,
        smart_put=1.0,
        recent_closes=list(BOUNCE_CLOSES),
        iv_rank=0.4,
    )
    base.update(over)
    ctx = MarketContext(**base)
    return ctx


# --- momentum component ----------------------------------------------------

def test_momentum_positive_on_rising():
    assert MomentumComponent().compute(_ctx(recent_closes=_rising())) > 0.2


def test_momentum_negative_on_falling():
    assert MomentumComponent().compute(_ctx(recent_closes=_falling())) < -0.2


def test_momentum_zero_on_flat_or_insufficient():
    assert MomentumComponent().compute(_ctx(recent_closes=[100.0] * 30)) == 0.0
    assert MomentumComponent().compute(_ctx(recent_closes=[100.0, 101.0])) == 0.0


# --- swing reversal component ----------------------------------------------

def test_swing_reversal_bullish_bounce():
    score = swing_reversal_score(BOUNCE_CLOSES, BOUNCE_LOWS, BOUNCE_HIGHS)
    assert score > 0.5


def test_swing_reversal_bearish_rejection():
    score = swing_reversal_score(REJECT_CLOSES, REJECT_LOWS, REJECT_HIGHS)
    assert score < -0.5


def test_swing_reversal_none_on_clean_trend():
    trend = [round(100 + i * 0.2, 4) for i in range(25)]
    assert swing_reversal_score(trend) == 0.0


def test_swing_reversal_component_reads_context():
    ctx = _ctx(recent_closes=BOUNCE_CLOSES, recent_lows=BOUNCE_LOWS, recent_highs=BOUNCE_HIGHS)
    assert SwingReversalComponent().compute(ctx) > 0.5


# --- tactical fusion -------------------------------------------------------

def test_compute_tactical_all_bullish():
    t = compute_tactical(price_action=0.8, flow=0.7, tape=0.6, momentum=0.9)
    assert t.direction > 0.5
    assert t.aligned_count == 4
    assert t.available_count == 4


def test_compute_tactical_excludes_missing_pillars():
    t = compute_tactical(price_action=0.8, flow=None, tape=0.6, momentum=None)
    assert t.available_count == 2
    assert t.direction > 0


# --- graded override state machine -----------------------------------------

def _trend_up():
    return compute_bias(
        BiasInput(netGEX=50, gexGradient=60, tapeFlow=80, vannaCharm=60, odtePositioning=60, msi=30)
    )


def _trend_down():
    return compute_bias(
        BiasInput(
            netGEX=50, gexGradient=60, tapeFlow=-80, vannaCharm=-60, odtePositioning=-60, msi=-50
        )
    )


def test_fuse_confirmed_when_tactical_agrees():
    structural = _trend_up()
    assert structural.trend == "bullish"
    tactical = compute_tactical(price_action=0.4, flow=0.4, tape=0.4, momentum=0.3)
    f = fuse(structural, tactical)
    assert f.state == "confirmed"
    assert f.direction == "long"
    assert f.override_active is False


def test_fuse_divergent_when_tactical_mildly_opposes():
    structural = _trend_down()
    assert structural.trend == "bearish"
    tactical = compute_tactical(price_action=0.3, flow=0.3, tape=0.3, momentum=0.3)
    f = fuse(structural, tactical)
    assert f.state == "divergent"
    assert f.direction == "short"  # bias not flipped
    assert f.override_active is False


def test_fuse_override_flips_bias_when_tactical_screams():
    structural = _trend_down()  # bearish baseline
    tactical = compute_tactical(price_action=0.85, flow=1.0, tape=0.8, momentum=0.6)
    f = fuse(structural, tactical)
    assert f.state == "override"
    assert f.direction == "long"
    assert f.bias_code == "REVERSAL_LONG"
    assert f.override_active is True
    assert f.overruled_posture == structural.biasLabel
    assert f.bias_score > 0  # flipped positive


def test_fuse_baseline_when_tactical_quiet():
    structural = _trend_up()
    tactical = compute_tactical(price_action=0.05, flow=0.05, tape=0.05, momentum=0.05)
    f = fuse(structural, tactical)
    assert f.state == "baseline"
    assert f.override_active is False


# --- engine end-to-end: the exact user scenario ----------------------------

def test_engine_bounce_overrides_negative_gamma_playbook():
    # Negative gamma + bearish structure + bullish flow => structural TRAP_REVERSAL
    # (Fade Strength, bearish). The live read (bounce off the low + strong order
    # flow + bullish tape) is loud and broad => it should OVERRIDE to long.
    ctx = _ctx(
        net_gex=-1.5e9,
        recent_closes=BOUNCE_CLOSES,
        recent_lows=BOUNCE_LOWS,
        recent_highs=BOUNCE_HIGHS,
        smart_call=300000.0,
        smart_put=15000.0,
    )
    score = SimpleNamespace(composite_score=40.0)
    basic = [
        SimpleNamespace(name="tape_flow_bias", score=0.8),
        SimpleNamespace(name="positioning_trap", score=-0.4),
    ]
    advanced = [SimpleNamespace(name="trap_detection", score=-0.6)]

    eng = TradeBiasEngine("SPY")
    inputs = eng.build_inputs(ctx, score, advanced, basic)
    structural = compute_bias(inputs)
    tactical = eng.build_tactical(ctx, inputs)
    fused = fuse(structural, tactical)
    snap = eng.build_snapshot(ctx, inputs, structural, tactical, fused)

    assert structural.trend == "bearish"  # the playbook we're overruling
    assert snap.state == "override"
    assert snap.direction == "long"
    assert snap.bias_code == "REVERSAL_LONG"
    assert snap.payload["override"]["active"] is True
    assert snap.payload["tactical"]["direction"] > 0.5
    # structural regime detail is preserved even under override
    assert snap.payload["structural_bias"]["label"] == structural.biasLabel


def test_intraday_profile_lowers_the_override_bar():
    # A read that only DIVERGES under the conservative swing gate should
    # OVERRIDE under the faster intraday gate (lower |D| + fewer pillars).
    structural = _trend_down()  # bearish baseline
    swing_t = compute_tactical(0.6, 0.6, 0.2, 0.1, SWING_PROFILE)
    intraday_t = compute_tactical(0.6, 0.6, 0.2, 0.1, INTRADAY_PROFILE)
    assert fuse(structural, swing_t, SWING_PROFILE).state == "divergent"
    assert fuse(structural, intraday_t, INTRADAY_PROFILE).state == "override"


def test_intraday_flow_pillar_uses_0dte_positioning():
    # Intraday feeds its flow pillar from the 0DTE positioning input; swing uses
    # the smart-money order-flow imbalance (here ~0 because smart_call≈smart_put).
    ctx = _ctx(smart_call=1.0, smart_put=1.0)
    eng = TradeBiasEngine("SPY")
    inputs = eng.build_inputs(
        ctx,
        SimpleNamespace(composite_score=50.0),
        [SimpleNamespace(name="zero_dte_position_imbalance", score=0.7)],
        [],
    )
    intraday = eng.build_tactical(ctx, inputs, INTRADAY_PROFILE)
    assert intraday.pillars["flow"] == inputs.odtePositioning / 100.0


def test_profile_for_defaults_to_swing():
    assert profile_for("swing").name == "swing"
    assert profile_for("intraday").name == "intraday"
    assert profile_for("nonsense").name == "swing"


def test_engine_builds_intraday_snapshot():
    ctx = _ctx()
    eng = TradeBiasEngine("SPY")
    # Exercise the snapshot path with the intraday profile without touching the DB.
    inputs = eng.build_inputs(ctx, SimpleNamespace(composite_score=50.0), [], [])
    structural = compute_bias(inputs)
    tactical = eng.build_tactical(ctx, inputs, INTRADAY_PROFILE)
    fused = fuse(structural, tactical, INTRADAY_PROFILE)
    snapshot = eng.build_snapshot(ctx, inputs, structural, tactical, fused, tenor="intraday")
    assert snapshot.tenor == "intraday"
    assert snapshot.payload["tenor"] == "intraday"


def test_engine_baseline_when_no_tactical_signal():
    ctx = _ctx(recent_closes=[100.0] * 30, recent_lows=[100.0] * 30, recent_highs=[100.0] * 30,
               smart_call=1.0, smart_put=1.0)
    score = SimpleNamespace(composite_score=50.0)
    eng = TradeBiasEngine("SPY")
    inputs = eng.build_inputs(ctx, score, [], [])
    structural = compute_bias(inputs)
    tactical = eng.build_tactical(ctx, inputs)
    fused = fuse(structural, tactical)
    snap = eng.build_snapshot(ctx, inputs, structural, tactical, fused)
    assert snap.state in ("baseline", "confirmed", "divergent")  # never override on a quiet tape
    assert snap.payload["override"]["active"] is False
