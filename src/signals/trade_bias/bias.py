"""Directional trade-bias synthesis.

Faithful Python port of the front-end ``computeBias()``
(``frontend/core/tradeBias.ts``). The regime detection, conviction-weighted
voting, market-state machine, playbook copy, confidence, checklist,
conviction-driven flag, and CHOP "watching" logic are reproduced 1:1 so the
backend engine yields the same output the dashboard produces today.

Inputs are on the same ``-100..+100`` scale the front end feeds:
``computeBias`` receives each signal's API ``score`` (``clamped_score * 100``),
``netGEX`` collapsed to ``±50`` on the sign of net GEX, and ``msi`` as the
0-100 composite score.

Later phases (see the package docstring) extend this with a fused
price-action/flow/tape/momentum layer and a graded override; the vote
thresholds are exposed as env-overridable module constants so that calibration
can happen without a code change (matching the ``src/signals/advanced/base.py``
idiom).
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Optional

# Vote thresholds — mirror the exported constants in tradeBias.ts.
STRONG = float(os.getenv("TRADE_BIAS_STRONG", "25"))
MODERATE = float(os.getenv("TRADE_BIAS_MODERATE", "12"))
DOMINANT = float(os.getenv("TRADE_BIAS_DOMINANT", "65"))
MSI_TOLERANCE = float(os.getenv("TRADE_BIAS_MSI_TOLERANCE", "10"))
MAX_CONFIDENCE = 10.0

# Front-end signal keys used for the CHOP "watching" chips.
SIGNAL_LABELS = {
    "tapeFlow": "Tape Flow",
    "vannaCharm": "Vanna/Charm",
    "odtePositioning": "0DTE Positioning",
    "positioningTrap": "Positioning Trap",
    "trapDetection": "Trap Detection",
    "gammaVWAP": "Gamma/VWAP",
}


@dataclass
class BiasInput:
    """The nine directional inputs, each on the ``-100..+100`` scale.

    ``None`` means the signal is unavailable this cycle (it is excluded from the
    ``available`` count and every vote). ``netGEX`` is ``±50`` (sign of net GEX)
    and ``msi`` is the 0-100 composite score.
    """

    netGEX: Optional[float] = None
    gexGradient: Optional[float] = None
    tapeFlow: Optional[float] = None
    vannaCharm: Optional[float] = None
    odtePositioning: Optional[float] = None
    positioningTrap: Optional[float] = None
    trapDetection: Optional[float] = None
    gammaVWAP: Optional[float] = None
    msi: Optional[float] = None


@dataclass
class WatchingSignal:
    key: str
    label: str
    direction: str  # 'bullish' | 'bearish'


@dataclass
class ChecklistItem:
    label: str
    passed: bool


@dataclass
class BiasResult:
    marketState: str  # TRAP_REVERSAL|TRAP_SQUEEZE|TREND_UP|TREND_DOWN|CHOP|UNKNOWN
    regimeLabel: str
    regimeDesc: str
    bias: str
    biasLabel: str
    trend: str  # 'bullish' | 'bearish' | 'neutral'
    confidence: float
    maxConfidence: float
    setup: str
    playbook: list[str]
    expectedBehavior: list[str]
    checklist: list[ChecklistItem]
    hasData: bool
    convictionDriven: bool
    watching: list[WatchingSignal] = field(default_factory=list)


def compute_bias(inp: BiasInput) -> BiasResult:
    """Synthesize a regime, directional bias, and playbook from the nine inputs.

    A 1:1 port of ``computeBias`` in ``frontend/core/tradeBias.ts`` — keep the
    two in lockstep.
    """
    netGEX = inp.netGEX
    gexGradient = inp.gexGradient
    tapeFlow = inp.tapeFlow
    vannaCharm = inp.vannaCharm
    odtePositioning = inp.odtePositioning
    positioningTrap = inp.positioningTrap
    trapDetection = inp.trapDetection
    gammaVWAP = inp.gammaVWAP
    msi = inp.msi

    available = sum(
        1
        for v in (
            netGEX,
            gexGradient,
            tapeFlow,
            vannaCharm,
            odtePositioning,
            positioningTrap,
            trapDetection,
            gammaVWAP,
            msi,
        )
        if v is not None
    )

    # Gamma regime: net GEX sign defines it; gradient acts as a veto only when it
    # strongly contradicts (magnitude >= MODERATE the other way).
    is_short_gamma = (
        netGEX is not None and netGEX < 0 and (gexGradient is None or gexGradient < MODERATE)
    )
    is_long_gamma = (
        netGEX is not None and netGEX > 0 and (gexGradient is None or gexGradient > -MODERATE)
    )

    # Conviction-weighted voting: a signal contributes 1 vote past its base
    # threshold, or 2 votes past DOMINANT (when boost is on) so a single
    # high-conviction reading can carry a 2-of-3 majority alone. The strict ``>``
    # comparison on the opposing tally cancels ties.
    def conviction(v: Optional[float], sign: int, base: float, boost: bool = True) -> int:
        if v is None:
            return 0
        aligned = v * sign
        if boost and aligned > DOMINANT:
            return 2
        if aligned > base:
            return 1
        return 0

    def flow_votes(sign: int, boost: bool = True) -> int:
        return (
            conviction(tapeFlow, sign, STRONG, boost)
            + conviction(vannaCharm, sign, MODERATE, boost)
            + conviction(odtePositioning, sign, MODERATE, boost)
        )

    bull_flow_votes = flow_votes(1)
    bear_flow_votes = flow_votes(-1)
    bullish_flow = bull_flow_votes >= 2 and bull_flow_votes > bear_flow_votes
    bearish_flow = bear_flow_votes >= 2 and bear_flow_votes > bull_flow_votes

    def structure_votes(sign: int, boost: bool = True) -> int:
        return (
            conviction(positioningTrap, sign, MODERATE, boost)
            + conviction(trapDetection, sign, STRONG, boost)
            + conviction(gammaVWAP, sign, MODERATE, boost)
        )

    bull_struct_votes = structure_votes(1)
    bear_struct_votes = structure_votes(-1)
    bullish_structure = bull_struct_votes >= 2 and bull_struct_votes > bear_struct_votes
    bearish_structure = bear_struct_votes >= 2 and bear_struct_votes > bull_struct_votes

    market_state = "UNKNOWN"
    if is_short_gamma and bullish_flow and bearish_structure:
        market_state = "TRAP_REVERSAL"
    elif is_short_gamma and bearish_flow and bullish_structure:
        market_state = "TRAP_SQUEEZE"
    elif is_long_gamma and bullish_flow and (msi is None or msi >= -MSI_TOLERANCE):
        market_state = "TREND_UP"
    elif is_long_gamma and bearish_flow and (msi is None or msi <= MSI_TOLERANCE):
        market_state = "TREND_DOWN"
    elif available >= 4:
        market_state = "CHOP"

    bias_scores: list[float] = []

    def push(v: Optional[float], expected_sign: int) -> None:
        if v is None:
            return
        bias_scores.append(max(0.0, (v * expected_sign) / 100.0))

    trend = "neutral"
    bias_label = "Neutral"
    bias = "WAIT"
    regime_label = "Awaiting confluence"
    regime_desc = "Signals mixed. Wait for alignment."
    setup = "No defined setup"
    playbook = ["Wait for signal alignment", "Avoid premium decay exposure", "Re-assess every 15m"]
    expected_behavior = ["Range-bound / choppy", "Low directional conviction"]

    if market_state == "TRAP_REVERSAL":
        trend = "bearish"
        bias = "FADE_STRENGTH"
        bias_label = "Sell Strength"
        regime_label = "Trap / Reversal Regime"
        regime_desc = "Short gamma + bullish flow + crowded structure = reversal setup."
        setup = "Trap / Reversal"
        playbook = [
            "Wait for upside push to stall",
            "Watch for rejection at resistance / VWAP",
            "Enter puts on failure confirmation",
            "Target liquidity sweep / downside expansion",
        ]
        expected_behavior = [
            "Early strength fails",
            "Choppy rejection at resistance",
            "Short-gamma expansion lower",
        ]
        push(tapeFlow, 1)
        push(vannaCharm, 1)
        push(odtePositioning, 1)
        push(positioningTrap, -1)
        push(trapDetection, -1)
        push(gammaVWAP, -1)
        push(netGEX, -1)
        push(gexGradient, -1)
    elif market_state == "TRAP_SQUEEZE":
        trend = "bullish"
        bias = "FADE_WEAKNESS"
        bias_label = "Buy Weakness"
        regime_label = "Trap / Squeeze Regime"
        regime_desc = "Short gamma + bearish flow + trapped shorts = squeeze setup."
        setup = "Trap / Squeeze"
        playbook = [
            "Wait for downside flush to stall",
            "Watch for reclaim of VWAP / support",
            "Enter calls on reversal confirmation",
            "Target short-cover squeeze / upside expansion",
        ]
        expected_behavior = [
            "Early weakness fails",
            "Flush reclaims support",
            "Short-gamma expansion higher",
        ]
        push(tapeFlow, -1)
        push(vannaCharm, -1)
        push(odtePositioning, -1)
        push(positioningTrap, 1)
        push(trapDetection, 1)
        push(gammaVWAP, 1)
        push(netGEX, -1)
        push(gexGradient, -1)
    elif market_state == "TREND_UP":
        trend = "bullish"
        bias = "BUY_DIPS"
        bias_label = "Buy Dips"
        regime_label = "Trend Up Regime"
        regime_desc = "Long gamma + aligned bullish flow + positive MSI."
        setup = "Trend Continuation (Up)"
        playbook = [
            "Buy dips toward VWAP / gamma support",
            "Target prior highs & call-wall magnet",
            "Trail stops under rising support",
        ]
        expected_behavior = [
            "Steady grind higher",
            "Shallow pullbacks bought",
            "Call-wall pin effect",
        ]
        push(tapeFlow, 1)
        push(vannaCharm, 1)
        push(odtePositioning, 1)
        push(positioningTrap, 1)
        push(trapDetection, 1)
        push(gammaVWAP, 1)
        push(netGEX, 1)
        push(msi, 1)
    elif market_state == "TREND_DOWN":
        trend = "bearish"
        bias = "SELL_RIPS"
        bias_label = "Sell Rips"
        regime_label = "Trend Down Regime"
        regime_desc = "Long gamma + aligned bearish flow + negative MSI."
        setup = "Trend Continuation (Down)"
        playbook = [
            "Short rips into VWAP / resistance",
            "Target prior lows & put-wall magnet",
            "Trail stops above declining resistance",
        ]
        expected_behavior = [
            "Steady drift lower",
            "Shallow bounces sold",
            "Put-wall pin effect",
        ]
        push(tapeFlow, -1)
        push(vannaCharm, -1)
        push(odtePositioning, -1)
        push(positioningTrap, -1)
        push(trapDetection, -1)
        push(gammaVWAP, -1)
        push(netGEX, 1)
        push(msi, -1)
    elif market_state == "CHOP":
        trend = "neutral"
        bias = "RANGE_FADE"
        bias_label = "Range-Bound"
        regime_label = "Chop / Range Regime"
        regime_desc = "Mixed signals — no dominant directional thesis."
        setup = "Mean Reversion"
        playbook = [
            "Fade extremes of the session range",
            "Avoid premium breakout trades",
            "Favor theta / defined-risk structures",
        ]
        expected_behavior = [
            "Two-way chop",
            "VWAP magnetism",
            "Failed breakout attempts",
        ]

        # Chop confidence rises as directional signals sit near zero; extreme
        # readings on either side reduce conviction in the range thesis.
        def push_chop(v: Optional[float]) -> None:
            if v is None:
                return
            bias_scores.append(max(0.0, (100.0 - abs(v)) / 100.0))

        push_chop(tapeFlow)
        push_chop(vannaCharm)
        push_chop(odtePositioning)
        push_chop(positioningTrap)
        push_chop(trapDetection)
        push_chop(gammaVWAP)
        push_chop(msi)

    max_confidence = MAX_CONFIDENCE
    raw_avg = sum(bias_scores) / len(bias_scores) if bias_scores else 0.0
    # JS ``Math.round(rawAvg * 10 * 10) / 10`` — round-half-up (inputs are >= 0).
    confidence = max(0.0, min(max_confidence, math.floor(raw_avg * 100.0 + 0.5) / 10.0))

    td = trapDetection if trapDetection is not None else 0.0
    checklist = [
        ChecklistItem("Short-gamma regime", is_short_gamma),
        ChecklistItem("Call-heavy tape flow", tapeFlow is not None and tapeFlow > MODERATE),
        ChecklistItem("Trap detection triggered", td < -STRONG or td > STRONG),
        ChecklistItem(
            "Structure/flow divergence",
            (bullish_flow and bearish_structure) or (bearish_flow and bullish_structure),
        ),
    ]

    # True when the active regime would NOT have triggered without the DOMINANT
    # conviction bonus — a single high-conviction signal carried the majority.
    def flow_majority_flat(sign: int) -> bool:
        return flow_votes(sign, False) >= 2

    def structure_majority_flat(sign: int) -> bool:
        return structure_votes(sign, False) >= 2

    conviction_driven = False
    if market_state == "TREND_UP":
        conviction_driven = not flow_majority_flat(1)
    elif market_state == "TREND_DOWN":
        conviction_driven = not flow_majority_flat(-1)
    elif market_state == "TRAP_REVERSAL":
        conviction_driven = (not flow_majority_flat(1)) or (not structure_majority_flat(-1))
    elif market_state == "TRAP_SQUEEZE":
        conviction_driven = (not flow_majority_flat(-1)) or (not structure_majority_flat(1))

    # In CHOP, surface any signal at conviction levels but not yet directional —
    # an early warning that a regime swap may be brewing.
    watching: list[WatchingSignal] = []
    if market_state == "CHOP":

        def check(v: Optional[float], key: str) -> None:
            if v is None or abs(v) <= DOMINANT:
                return
            watching.append(
                WatchingSignal(
                    key=key,
                    label=SIGNAL_LABELS[key],
                    direction="bullish" if v > 0 else "bearish",
                )
            )

        check(tapeFlow, "tapeFlow")
        check(vannaCharm, "vannaCharm")
        check(odtePositioning, "odtePositioning")
        check(positioningTrap, "positioningTrap")
        check(trapDetection, "trapDetection")
        check(gammaVWAP, "gammaVWAP")

    return BiasResult(
        marketState=market_state,
        regimeLabel=regime_label,
        regimeDesc=regime_desc,
        bias=bias,
        biasLabel=bias_label,
        trend=trend,
        confidence=confidence,
        maxConfidence=max_confidence,
        setup=setup,
        playbook=playbook,
        expectedBehavior=expected_behavior,
        checklist=checklist,
        hasData=available >= 3,
        convictionDriven=conviction_driven,
        watching=watching,
    )
