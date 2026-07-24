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

import math
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


@dataclass
class TenorProfile:
    """Per-horizon calibration of the fusion.

    Swing is structural / gamma-led with a conservative override bar. Intraday
    is tactical / 0DTE-led: the same-day read carries more weight and the
    override fires on a lower bar because 0DTE reversals happen fast. The engine
    also feeds the intraday ``flow`` pillar from the 0DTE positioning signal
    rather than the all-expiry smart-money flow (see engine.build_tactical).
    """

    name: str
    override_d: float
    override_min_aligned: int
    weak_min: float
    divergent_min: float
    align_min: float
    w_price: float
    w_flow: float
    w_tape: float
    w_momentum: float

    def weights(self) -> dict:
        return {
            "price_action": self.w_price,
            "flow": self.w_flow,
            "tape": self.w_tape,
            "momentum": self.w_momentum,
        }


SWING_PROFILE = TenorProfile(
    name="swing",
    override_d=_OVERRIDE_D,
    override_min_aligned=_OVERRIDE_MIN_ALIGNED,
    weak_min=_WEAK_MIN,
    divergent_min=_DIVERGENT_MIN,
    align_min=_ALIGN_MIN,
    w_price=_W_PRICE,
    w_flow=_W_FLOW,
    w_tape=_W_TAPE,
    w_momentum=_W_MOMENTUM,
)

INTRADAY_PROFILE = TenorProfile(
    name="intraday",
    override_d=float(os.getenv("TRADE_BIAS_INTRADAY_OVERRIDE_D", "0.40")),
    override_min_aligned=int(os.getenv("TRADE_BIAS_INTRADAY_OVERRIDE_MIN_ALIGNED", "2")),
    weak_min=_WEAK_MIN,
    divergent_min=_DIVERGENT_MIN,
    align_min=_ALIGN_MIN,
    w_price=float(os.getenv("TRADE_BIAS_INTRADAY_W_PRICE_ACTION", "0.30")),
    w_flow=float(os.getenv("TRADE_BIAS_INTRADAY_W_FLOW", "0.30")),
    w_tape=float(os.getenv("TRADE_BIAS_INTRADAY_W_TAPE", "0.25")),
    w_momentum=float(os.getenv("TRADE_BIAS_INTRADAY_W_MOMENTUM", "0.15")),
)

PROFILES = {"swing": SWING_PROFILE, "intraday": INTRADAY_PROFILE}


def profile_for(tenor: str) -> TenorProfile:
    return PROFILES.get(tenor, SWING_PROFILE)


_TREND_SIGN = {"bullish": 1, "bearish": -1, "neutral": 0}
_SIGN_DIR = {1: "long", -1: "short", 0: "neutral"}

# ---------------------------------------------------------------------------
# Continuous bias score
#
# The signed bias number must FLOW — it should read 0 only when the inputs are
# genuinely balanced, never pin there because a discrete regime went neutral.
# So the number is a weighted mean of every *directional* signal (each on
# [-1, 1]), passed through tanh(gain * x) for a smooth, magnet-free map onto
# [-100, 100]. gamma sign / MSI are regime-strength, not direction, so they are
# deliberately excluded from the directional aggregate.
# ---------------------------------------------------------------------------
_DIR_WEIGHTS = {
    "tape_flow": 0.14,
    "order_flow": 0.14,
    "momentum": 0.12,
    "price_action": 0.12,
    "odte": 0.08,
    "vanna_charm": 0.08,
    "positioning_trap": 0.08,
    "trap_detection": 0.08,
    "gamma_vwap": 0.08,
    "gex_gradient": 0.04,
    "dealer_delta": 0.04,
}
# Gain into tanh — higher spreads moderate leans further across the range so
# the tape uses its full span instead of hugging center. Env-tunable.
_BIAS_GAIN = float(os.getenv("TRADE_BIAS_SCORE_GAIN", "2.0"))


@dataclass
class ContinuousBias:
    score: float  # signed [-100, 100], continuous
    aggregate: float  # signed [-1, 1] weighted mean of the directional signals
    breadth: float  # [0, 1] weighted share of signals agreeing with the sign
    confidence: float  # [0, 100]
    available: int


def continuous_bias(
    signals: dict[str, Optional[float]], gain: float = _BIAS_GAIN
) -> ContinuousBias:
    """Continuous signed bias from the directional signals (each on [-1, 1]).

    ``signals`` keys are those in ``_DIR_WEIGHTS``; missing / None values are
    excluded and the surviving weights renormalize, so the read stays centered
    only when the *present* signals genuinely cancel.
    """
    num = 0.0
    den = 0.0
    available = 0
    for key, weight in _DIR_WEIGHTS.items():
        value = signals.get(key)
        if value is None or weight <= 0:
            continue
        available += 1
        num += weight * max(-1.0, min(1.0, float(value)))
        den += weight
    aggregate = (num / den) if den > 0 else 0.0
    score = 100.0 * math.tanh(gain * aggregate)

    # Breadth: weighted share of present signals pointing the aggregate's way.
    agg_sign = _sign(aggregate)
    bnum = 0.0
    bden = 0.0
    for key, weight in _DIR_WEIGHTS.items():
        value = signals.get(key)
        if value is None or weight <= 0:
            continue
        bden += weight
        if agg_sign != 0 and _sign(value) == agg_sign:
            bnum += weight
    breadth = (bnum / bden) if bden > 0 else 0.0

    # Conviction = how far off center AND how broad the agreement.
    confidence = min(100.0, abs(score) * (0.5 + 0.5 * breadth))

    return ContinuousBias(
        score=round(score, 2),
        aggregate=round(aggregate, 4),
        breadth=round(breadth, 4),
        confidence=round(confidence, 2),
        available=available,
    )


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
    profile: TenorProfile = SWING_PROFILE,
) -> TacticalRead:
    """Fuse the four directional pillars into a signed direction + conviction."""
    pillars = {
        "price_action": price_action,
        "flow": flow,
        "tape": tape,
        "momentum": momentum,
    }
    weights = profile.weights()
    num = 0.0
    den = 0.0
    available = 0
    for key, value in pillars.items():
        if value is None:
            continue
        available += 1
        clamped = max(-1.0, min(1.0, float(value)))
        num += weights[key] * clamped
        den += weights[key]
    direction = (num / den) if den > 0 else 0.0
    d_sign = _sign(direction)

    aligned = 0
    if d_sign != 0:
        for value in pillars.values():
            if value is None:
                continue
            if abs(value) >= profile.align_min and _sign(value) == d_sign:
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


def fuse(
    structural: BiasResult, tactical: TacticalRead, profile: TenorProfile = SWING_PROFILE
) -> FusedBias:
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
    tact_sign = _sign(d) if tactical.conviction >= profile.weak_min else 0
    aligned_frac = (
        tactical.aligned_count / tactical.available_count if tactical.available_count else 0.0
    )
    gate = (
        tactical.conviction >= profile.override_d
        and tactical.aligned_count >= profile.override_min_aligned
    )

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
    opposes = struct_sign != 0 and tact_sign == -struct_sign
    if opposes and tactical.conviction >= profile.divergent_min:
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
