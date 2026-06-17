"""Unit tests for the Copilot regime classifier.

The classifier is a pure function over ``PlaybookContext``. These tests
hand-build contexts that satisfy (or break) each rule and assert the
expected label and confidence.

Fixture style mirrors ``tests/test_playbook_call_wall_fade.py`` for
consistency with the rest of the playbook test suite.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Optional

from src.copilot.regime_narrative import (
    LABEL_CHARM_DRIFT,
    LABEL_LONG_GAMMA_PIN,
    LABEL_SHORT_GAMMA_TREND,
    LABEL_TRANSITION,
    LABEL_UNDEFINED,
    LABEL_VANNA_GLIDE,
    LABEL_VOL_EXPANSION,
    RegimeNarrative,
    classify_regime,
)
from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _ctx(
    *,
    close: float = 5847.0,
    net_gex: float = 0.0,
    gamma_flip: float = 5847.0,
    max_pain: float = 5850.0,
    call_wall: float = 5870.0,
    put_wall: float = 5830.0,
    msi_regime: str = "chop_range",
    vix_level: float = 16.0,
    vanna_charm_score: float = 0.0,
    vol_expansion_triggered: bool = False,
    tape_flow_score: float = 0.0,
    eod_pressure_score: float = 0.0,
    zero_dte_score: float = 0.0,
    timestamp: Optional[datetime] = None,
) -> PlaybookContext:
    # Default to 11:30 AM ET → 16:30 UTC. Tests that need the close window
    # pass an explicit timestamp.
    ts = timestamp or datetime(2026, 5, 1, 16, 30, tzinfo=timezone.utc)
    market = MarketContext(
        timestamp=ts,
        underlying="SPY",
        close=close,
        net_gex=net_gex,
        gamma_flip=gamma_flip,
        put_call_ratio=0.5,
        max_pain=max_pain,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[],
        iv_rank=None,
        extra={
            "vix_level": vix_level,
            "call_wall": call_wall,
            "put_wall": put_wall,
        },
    )
    market.vix_level = vix_level  # type: ignore[attr-defined]

    advanced: dict[str, SignalSnapshot] = {
        "vol_expansion": SignalSnapshot(
            name="vol_expansion",
            score=80.0 if vol_expansion_triggered else 10.0,
            clamped_score=0.80 if vol_expansion_triggered else 0.10,
            triggered=vol_expansion_triggered,
        ),
        "eod_pressure": SignalSnapshot(
            name="eod_pressure",
            score=eod_pressure_score,
            clamped_score=eod_pressure_score / 100.0,
        ),
        "zero_dte_position_imbalance": SignalSnapshot(
            name="zero_dte_position_imbalance",
            score=zero_dte_score,
            clamped_score=zero_dte_score / 100.0,
        ),
    }
    basic: dict[str, SignalSnapshot] = {
        "vanna_charm_flow": SignalSnapshot(
            name="vanna_charm_flow",
            score=vanna_charm_score,
            clamped_score=vanna_charm_score / 100.0,
        ),
        "tape_flow_bias": SignalSnapshot(
            name="tape_flow_bias",
            score=tape_flow_score,
            clamped_score=tape_flow_score / 100.0,
        ),
    }
    return PlaybookContext(
        market=market,
        msi_score=50.0,
        msi_regime=msi_regime,
        msi_components={},
        advanced_signals=advanced,
        basic_signals=basic,
        levels={
            "gamma_flip": gamma_flip,
            "max_pain": max_pain,
            "call_wall": call_wall,
            "put_wall": put_wall,
        },
        open_positions=[],
        recently_emitted={},
    )


# ---------------------------------------------------------------------------
# UNDEFINED — required inputs missing
# ---------------------------------------------------------------------------


def test_undefined_when_msi_regime_missing():
    ctx = _ctx(msi_regime=None)  # type: ignore[arg-type]
    narrative = classify_regime(ctx)
    assert narrative.label == LABEL_UNDEFINED
    assert narrative.confidence == 0.0


def test_undefined_carries_inputs_snapshot():
    ctx = _ctx(msi_regime=None)  # type: ignore[arg-type]
    narrative = classify_regime(ctx)
    assert isinstance(narrative.inputs_snapshot, dict)
    assert "net_gex" in narrative.inputs_snapshot


# ---------------------------------------------------------------------------
# VOL_EXPANSION — rule 1
# ---------------------------------------------------------------------------


def test_vol_expansion_when_vix_spikes():
    ctx = _ctx(
        vol_expansion_triggered=True,
        net_gex=-2.0e8,
        vix_level=22.0,
    )
    narrative = classify_regime(ctx, vix_change_pct=8.0)
    assert narrative.label == LABEL_VOL_EXPANSION
    assert narrative.confidence > 0.0


def test_vol_expansion_when_realized_catching_implied():
    ctx = _ctx(
        vol_expansion_triggered=True,
        net_gex=-1.0e8,
        vix_level=18.0,
    )
    # realized / (vix/100) = 0.20 / 0.18 = 1.11 > 0.85
    narrative = classify_regime(
        ctx, vix_change_pct=1.0, realized_vol_30m=0.20
    )
    assert narrative.label == LABEL_VOL_EXPANSION


def test_no_vol_expansion_when_long_gamma():
    ctx = _ctx(
        vol_expansion_triggered=True,
        net_gex=+2.0e9,  # strongly long gamma blocks rule 1
        vix_level=22.0,
    )
    narrative = classify_regime(ctx, vix_change_pct=8.0)
    assert narrative.label != LABEL_VOL_EXPANSION


# ---------------------------------------------------------------------------
# VANNA_GLIDE — rule 2
# ---------------------------------------------------------------------------


def test_vanna_glide_when_score_high_and_vix_moving():
    ctx = _ctx(vanna_charm_score=80.0)
    narrative = classify_regime(ctx, vix_change_pct=-3.5)
    assert narrative.label == LABEL_VANNA_GLIDE


def test_no_vanna_glide_when_vix_flat():
    ctx = _ctx(vanna_charm_score=80.0)
    narrative = classify_regime(ctx, vix_change_pct=0.5)
    assert narrative.label != LABEL_VANNA_GLIDE


# ---------------------------------------------------------------------------
# SHORT_GAMMA_TREND — rule 3
# ---------------------------------------------------------------------------


def test_short_gamma_trend_bullish_break():
    ctx = _ctx(
        net_gex=-2.0e9,
        gamma_flip=5840.0,
        close=5850.0,           # above flip → up direction
        tape_flow_score=60.0,   # tape agrees
        msi_regime="trend_expansion",
    )
    narrative = classify_regime(ctx)
    assert narrative.label == LABEL_SHORT_GAMMA_TREND


def test_short_gamma_trend_bearish_break():
    ctx = _ctx(
        net_gex=-2.0e9,
        gamma_flip=5850.0,
        close=5840.0,           # below flip → down direction
        tape_flow_score=-60.0,  # tape agrees
        msi_regime="controlled_trend",
    )
    narrative = classify_regime(ctx)
    assert narrative.label == LABEL_SHORT_GAMMA_TREND


def test_short_gamma_trend_blocked_when_tape_opposes():
    ctx = _ctx(
        net_gex=-2.0e9,
        gamma_flip=5850.0,
        close=5840.0,
        tape_flow_score=+60.0,  # tape opposes spot move
        msi_regime="trend_expansion",
    )
    narrative = classify_regime(ctx)
    assert narrative.label != LABEL_SHORT_GAMMA_TREND


# ---------------------------------------------------------------------------
# CHARM_DRIFT — rule 4
# ---------------------------------------------------------------------------


def test_charm_drift_into_close():
    # 3:00 PM ET → 60 min to close
    ts = datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc)
    ctx = _ctx(
        timestamp=ts,
        close=5850.0,
        max_pain=5852.0,        # within 0.5%
        zero_dte_score=70.0,
        msi_regime="chop_range",
    )
    narrative = classify_regime(ctx, max_pain_convergence=-0.5)
    assert narrative.label == LABEL_CHARM_DRIFT


def test_no_charm_drift_when_too_early():
    # 11:30 AM ET → too early for charm drift window
    ts = datetime(2026, 5, 1, 15, 30, tzinfo=timezone.utc)
    ctx = _ctx(
        timestamp=ts,
        close=5850.0,
        max_pain=5851.0,
        zero_dte_score=70.0,
    )
    narrative = classify_regime(ctx, max_pain_convergence=-0.5)
    assert narrative.label != LABEL_CHARM_DRIFT


# ---------------------------------------------------------------------------
# LONG_GAMMA_PIN — rule 5
# ---------------------------------------------------------------------------


def test_long_gamma_pin_at_max_pain():
    ctx = _ctx(
        net_gex=+2.0e9,
        close=5850.0,
        max_pain=5850.0,
        msi_regime="chop_range",
    )
    narrative = classify_regime(ctx, realized_vol_30m=0.08)
    assert narrative.label == LABEL_LONG_GAMMA_PIN
    assert "chop" in narrative.expected_behavior.lower()


def test_long_gamma_pin_blocked_when_high_realized_vol():
    ctx = _ctx(
        net_gex=+2.0e9,
        close=5850.0,
        max_pain=5850.0,
        msi_regime="chop_range",
    )
    narrative = classify_regime(ctx, realized_vol_30m=0.25)
    assert narrative.label != LABEL_LONG_GAMMA_PIN


def test_long_gamma_pin_requires_chop_or_reversal_regime():
    ctx = _ctx(
        net_gex=+2.0e9,
        close=5850.0,
        max_pain=5850.0,
        msi_regime="trend_expansion",  # wrong regime for pin
    )
    narrative = classify_regime(ctx, realized_vol_30m=0.08)
    assert narrative.label != LABEL_LONG_GAMMA_PIN


# ---------------------------------------------------------------------------
# TRANSITION
# ---------------------------------------------------------------------------


def test_transition_when_near_miss():
    # Strong net_gex but spot not at a magnet — long gamma pin near-misses.
    ctx = _ctx(
        net_gex=+2.0e9,
        close=5862.0,  # 0.2% from max_pain but not within strict 0.3%
        max_pain=5850.0,
        msi_regime="chop_range",
    )
    narrative = classify_regime(ctx, realized_vol_30m=0.08)
    assert narrative.label in (LABEL_TRANSITION, LABEL_LONG_GAMMA_PIN)


# ---------------------------------------------------------------------------
# Hysteresis
# ---------------------------------------------------------------------------


def test_hysteresis_holds_prior_label_on_low_confidence_swap():
    prior = RegimeNarrative(
        timestamp=datetime(2026, 5, 1, 16, 29, tzinfo=timezone.utc),
        symbol="SPY",
        label=LABEL_LONG_GAMMA_PIN,
        confidence=0.80,
        spot=5850.0,
        expected_behavior="",
        favored_patterns=[],
        avoid=[],
        what_would_flip_it="",
        msi_regime="chop_range",
        inputs_snapshot={},
    )
    # New cycle: criteria marginally meet vanna_glide
    ctx = _ctx(vanna_charm_score=60.0)
    narrative = classify_regime(ctx, prior=prior, vix_change_pct=2.0)
    # Should emit TRANSITION, not flip directly to VANNA_GLIDE
    assert narrative.label == LABEL_TRANSITION


def test_hysteresis_allows_swap_on_strong_confidence_jump():
    prior = RegimeNarrative(
        timestamp=datetime(2026, 5, 1, 16, 29, tzinfo=timezone.utc),
        symbol="SPY",
        label=LABEL_LONG_GAMMA_PIN,
        confidence=0.40,  # low prior confidence
        spot=5850.0,
        expected_behavior="",
        favored_patterns=[],
        avoid=[],
        what_would_flip_it="",
        msi_regime="chop_range",
        inputs_snapshot={},
    )
    ctx = _ctx(
        vol_expansion_triggered=True,
        net_gex=-1.0e8,
        vix_level=22.0,
    )
    narrative = classify_regime(ctx, prior=prior, vix_change_pct=10.0)
    assert narrative.label == LABEL_VOL_EXPANSION


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------


def test_output_carries_audit_trail():
    ctx = _ctx(
        net_gex=+2.0e9,
        close=5850.0,
        max_pain=5850.0,
        msi_regime="chop_range",
    )
    narrative = classify_regime(ctx, realized_vol_30m=0.08)
    snapshot = narrative.inputs_snapshot
    assert snapshot["net_gex"] == 2.0e9
    assert snapshot["spot"] == 5850.0
    assert snapshot["max_pain"] == 5850.0


def test_to_dict_is_json_safe():
    import json

    ctx = _ctx()
    narrative = classify_regime(ctx)
    payload = narrative.to_dict()
    json.dumps(payload)  # would raise on non-serializable content


def test_confidence_never_exceeds_cap():
    # Saturate the highest-fit case
    ctx = _ctx(
        vol_expansion_triggered=True,
        net_gex=-2.0e9,
        vix_level=22.0,
    )
    narrative = classify_regime(ctx, vix_change_pct=100.0, realized_vol_30m=0.5)
    assert 0.0 <= narrative.confidence <= 0.95


def test_descriptors_align_with_label():
    ctx = _ctx(
        net_gex=+2.0e9,
        close=5850.0,
        max_pain=5850.0,
        msi_regime="chop_range",
    )
    narrative = classify_regime(ctx, realized_vol_30m=0.08)
    assert narrative.expected_behavior
    assert narrative.favored_patterns
    assert "Chasing breakouts" in narrative.avoid
