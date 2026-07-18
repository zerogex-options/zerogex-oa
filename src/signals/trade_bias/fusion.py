"""Two-layer fusion + graded override for Trade Bias.

The structural baseline (``compute_bias`` — gamma + volatility regime and the
existing signal votes) sets the default posture. A tactical directional read —
price action, order flow, tape, momentum — then either:

  * confirms it (agrees) → keep the bias, raise confidence;
  * diverges (leans against it, but not decisively) → keep the bias, cut
    confidence, flag caution; or
  * overrides it (loud AND broad enough) → flip the bias and swap to a
    reversal/squeeze playbook.

Override is the "we bounced off the low and price action, flow, tape and
momentum are all screaming buy" behavior: it can overrule a negative-gamma
downside playbook when the live read clears a tunable gate.

Every threshold is env-overridable so the override can be calibrated (and
backtested against the persisted ``trade_bias_scores`` history) without a code
change.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from src.signals.trade_bias.bias import BiasResult

# Tactical pillar weights. Price action + momentum carry a reversal; flow + tape
# corroborate. Renormalized over whichever pillars are available each cycle.
_W_PRICE = float(os.getenv("TRADE_BIAS_W_PRICE_ACTION", "0.30"))
_W_FLOW = float(os.getenv("TRADE_BIAS_W_FLOW", "0.25"))
_W_TAPE = float(os.getenv("TRADE_BIAS_W_TAPE", "0.25"))
_W_MOMENTUM = float(os.getenv("TRADE_BIAS_W_MOMENTUM", "0.20"))

# Override gate — the "loud enough" bar. BOTH must clear: net directional
# magnitude (|D|) and breadth of agreement (how many pillars point the same way
# with conviction). These are the primary calibration knobs.
_OVERRIDE_D = float(os.getenv("TRADE_BIAS_OVERRIDE_D", "0.50"))
_OVERRIDE_MIN_ALIGNED = int(os.getenv("TRADE_BIAS_OVERRIDE_MIN_ALIGNED", "3"))
# Minimum |pillar| for a pillar to count toward the aligned tally.
_ALIGN_MIN = float(os.getenv("TRADE_BIAS_ALIGN_MIN", "0.33"))
# Below WEAK_MIN the tactical read is treated as directionless (quiet tape).
_WEAK_MIN = float(os.getenv("TRADE_BIAS_WEAK_MIN", "0.15"))
# Minimum opposing |D| to register a divergence (caution) short of an override.
_DIVERGENT_MIN = float(os.getenv("TRADE_BIAS_DIVERGENT_MIN", "0.25"))

_PILLAR_WEIGHTS = {
    "price_action": _W_PRICE,
    "flow": _W_FLOW,
    "tape": _W_TAPE,
    "momentum": _W_MOMENTUM,
}

_TREND_SIGN = {"bullish": 1, "bearish": -1, "neutral": 0}
_SIGN_DIR = {1: "long", -1: "short", 0: "neutral"}


def _sign(x: float, dead: float = 1e-9) -> int:
    if x > dead:
        return 1
    if x < -dead:
        return -1
    return 0


@dataclass
class TacticalRead:
    direction: float  # D in [-1, 1]
    conviction: float  # |D| in [0, 1]
    aligned_count: int
    available_count: int
    pillars: dict  # {price_action, flow, tape, momentum}: value | None


def compute_tactical(
    price_action: Optional[float],
    flow: Optional[float],
    tape: Optional[float],
    momentum: Optional[float],
) -> TacticalRead:
    """Fuse the four directional pillars into a signed direction + conviction."""
    pillars = {
        "price_action": price_action,
        "flow": flow,
        "tape": tape,
        "momentum": momentum,
    }
    num = 0.0
    den = 0.0
    available = 0
    for key, value in pillars.items():
        if value is None:
            continue
        available += 1
        clamped = max(-1.0, min(1.0, float(value)))
        num += _PILLAR_WEIGHTS[key] * clamped
        den += _PILLAR_WEIGHTS[key]
    direction = (num / den) if den > 0 else 0.0
    d_sign = _sign(direction)

    aligned = 0
    if d_sign != 0:
        for value in pillars.values():
            if value is None:
                continue
            if abs(value) >= _ALIGN_MIN and _sign(value) == d_sign:
                aligned += 1

    return TacticalRead(
        direction=direction,
        conviction=abs(direction),
        aligned_count=aligned,
        available_count=available,
        pillars=pillars,
    )


@dataclass
class FusedBias:
    state: str  # baseline | confirmed | divergent | override
    direction: str  # long | short | neutral
    bias_code: str
    bias_label: str
    setup: str
    playbook: list[str]
    expected_behavior: list[str]
    bias_score: float  # signed [-100, 100]
    confidence: float  # [0, 100]
    override_active: bool
    override_reason: Optional[str]
    overruled_posture: Optional[str]


_OVERRIDE_LONG = {
    "bias_code": "REVERSAL_LONG",
    "bias_label": "Reversal Long",
    "setup": "Bounce / Reversal (Long)",
    "playbook": [
        "Confirm the low held — higher low or reclaim of the level",
        "Enter longs on the reclaim, risk defined under the low",
        "Target VWAP / prior resistance",
        "Trail as flow, tape and momentum keep pushing up",
    ],
    "expected_behavior": [
        "Sharp rejection of the lows",
        "Flow, tape and momentum flipping up together",
        "Short-covering / squeeze potential",
    ],
}
_OVERRIDE_SHORT = {
    "bias_code": "REVERSAL_SHORT",
    "bias_label": "Reversal Short",
    "setup": "Rejection / Reversal (Short)",
    "playbook": [
        "Confirm the high rejected — lower high or failed breakout",
        "Enter shorts on the failure, risk defined above the high",
        "Target VWAP / prior support",
        "Trail as flow, tape and momentum keep pushing down",
    ],
    "expected_behavior": [
        "Sharp rejection of the highs",
        "Flow, tape and momentum flipping down together",
        "Long liquidation potential",
    ],
}


def fuse(structural: BiasResult, tactical: TacticalRead) -> FusedBias:
    """Combine the structural baseline with the tactical read into a graded bias."""
    struct_sign = _TREND_SIGN.get(structural.trend, 0)
    struct_conf_pct = (
        (structural.confidence / structural.maxConfidence) * 100.0
        if structural.maxConfidence
        else 0.0
    )
    struct_signed = struct_sign * struct_conf_pct
    struct_dir = _SIGN_DIR[struct_sign]

    d = tactical.direction
    tact_signed = d * 100.0
    tact_sign = _sign(d) if tactical.conviction >= _WEAK_MIN else 0
    aligned_frac = (
        tactical.aligned_count / tactical.available_count if tactical.available_count else 0.0
    )
    gate = tactical.conviction >= _OVERRIDE_D and tactical.aligned_count >= _OVERRIDE_MIN_ALIGNED

    def keep_structural(state: str, confidence: float, bias_score: float) -> FusedBias:
        return FusedBias(
            state=state,
            direction=struct_dir,
            bias_code=structural.bias,
            bias_label=structural.biasLabel,
            setup=structural.setup,
            playbook=list(structural.playbook),
            expected_behavior=list(structural.expectedBehavior),
            bias_score=round(max(-100.0, min(100.0, bias_score)), 4),
            confidence=round(max(0.0, min(100.0, confidence)), 2),
            override_active=False,
            override_reason=None,
            overruled_posture=None,
        )

    # OVERRIDE — tactical is loud + broad and either opposes a directional
    # baseline or drives a directional call out of a flat one.
    if gate and tact_sign != 0 and (struct_sign == 0 or tact_sign != struct_sign):
        tmpl = _OVERRIDE_LONG if tact_sign > 0 else _OVERRIDE_SHORT
        regime = structural.regimeLabel.lower()
        if struct_sign != 0:
            reason = (
                f"Live read (price action, flow, tape, momentum) overruled the "
                f"{regime} — {structural.biasLabel.lower()} bias."
            )
        else:
            reason = f"Live read drove a directional call out of the {regime}."
        confidence = tactical.conviction * 100.0 * (0.6 + 0.4 * aligned_frac)
        return FusedBias(
            state="override",
            direction=_SIGN_DIR[tact_sign],
            bias_code=tmpl["bias_code"],
            bias_label=tmpl["bias_label"],
            setup=tmpl["setup"],
            playbook=list(tmpl["playbook"]),
            expected_behavior=list(tmpl["expected_behavior"]),
            bias_score=round(max(-100.0, min(100.0, tact_signed)), 4),
            confidence=round(max(0.0, min(100.0, confidence)), 2),
            override_active=True,
            override_reason=reason,
            overruled_posture=structural.biasLabel,
        )

    # DIVERGENT — tactical leans against a directional baseline but not enough
    # to flip it. Keep the bias; attenuate score + confidence; flag caution.
    if struct_sign != 0 and tact_sign == -struct_sign and tactical.conviction >= _DIVERGENT_MIN:
        confidence = struct_conf_pct * (1.0 - 0.4 * tactical.conviction)
        bias_score = struct_signed * (1.0 - 0.5 * tactical.conviction)
        return keep_structural("divergent", confidence, bias_score)

    # CONFIRMED — tactical agrees with a directional baseline. Reinforce.
    if struct_sign != 0 and tact_sign == struct_sign:
        confidence = struct_conf_pct + 25.0 * tactical.conviction * aligned_frac
        bias_score = 0.55 * struct_signed + 0.45 * tact_signed
        return keep_structural("confirmed", confidence, bias_score)

    # BASELINE — tactical quiet, or a flat baseline with no loud read to promote.
    return keep_structural("baseline", struct_conf_pct, struct_signed)
