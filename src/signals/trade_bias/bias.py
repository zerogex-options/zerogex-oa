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

``msi`` is regime strength, not direction, so both trend states treat it
identically: it adds to a trend call's confidence and never gates it. It
used to gate them as if it ran -100..+100 (``msi >= -10`` for TREND_UP,
``msi <= 10`` for TREND_DOWN). On the real 0-100 scale the first always
passed and the second needed the gauge at 10 or below, so TREND_UP fired
freely and TREND_DOWN almost never did.

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
MAX_CONFIDENCE = 10.0

#: The copy below describes where positioning and flow stand; it gives no trade
#: instruction and makes no forecast. ZeroGEX's own replays of every minute
#: since 2026-07-17 (research/short_gamma_trend, research/trade_bias_inputs,
#: research/trade_bias_movement) found that no state predicted the direction
#: or the size of the next move, and no input predicted its direction.
#: ``frontend/core/tradeBias.ts`` carries the same strings; keep them in step.
NOT_A_FORECAST = "A description of the current read, not a forecast"

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
    elif is_long_gamma and bullish_flow:
        market_state = "TREND_UP"
    elif is_long_gamma and bearish_flow:
        market_state = "TREND_DOWN"
    elif available >= 4:
        market_state = "CHOP"

    bias_scores: list[float] = []

    def push(v: Optional[float], expected_sign: int) -> None:
        if v is None:
            return
        bias_scores.append(max(0.0, (v * expected_sign) / 100.0))

    # Defaults are the UNKNOWN state's copy: fewer than four inputs reporting.
    trend = "neutral"
    bias_label = "No Read"
    bias = "WAIT"
    regime_label = "Not Enough Data"
    regime_desc = "Fewer than four of the nine inputs are reporting."
    setup = "No Defined State"
    # ``playbook`` is what would move the panel out of its current state;
    # ``expected_behavior`` is what the inputs show. Field names are the API's.
    playbook = ["More of the nine inputs reporting"]
    expected_behavior = ["Waiting on more inputs to report", NOT_A_FORECAST]

    if market_state == "TRAP_REVERSAL":
        trend = "bearish"
        bias = "FADE_STRENGTH"
        bias_label = "Structure Bearish"
        regime_label = "Short Gamma \u00b7 Flow vs. Structure"
        regime_desc = (
            "Dealers are net short gamma. Flow leans bullish while the structure signals lean "
            "bearish."
        )
        setup = "Flow/Structure Split"
        playbook = [
            "Flow or structure losing its majority",
            "Net GEX turning positive, or the gradient strongly against it",
        ]
        expected_behavior = [
            "Net GEX negative: dealers net short gamma",
            "Flow majority bullish; structure majority bearish",
            NOT_A_FORECAST,
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
        bias_label = "Structure Bullish"
        regime_label = "Short Gamma \u00b7 Flow vs. Structure"
        regime_desc = (
            "Dealers are net short gamma. Flow leans bearish while the structure signals lean "
            "bullish."
        )
        setup = "Flow/Structure Split"
        playbook = [
            "Flow or structure losing its majority",
            "Net GEX turning positive, or the gradient strongly against it",
        ]
        expected_behavior = [
            "Net GEX negative: dealers net short gamma",
            "Flow majority bearish; structure majority bullish",
            NOT_A_FORECAST,
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
        bias_label = "Flow Bullish"
        regime_label = "Long Gamma \u00b7 Bullish Flow"
        regime_desc = "Dealers are net long gamma, and most flow signals lean bullish."
        setup = "Aligned Flow"
        playbook = [
            "Flow losing its bullish majority",
            "Net GEX turning negative, or the gradient strongly against it",
        ]
        expected_behavior = [
            "Net GEX positive: dealers net long gamma",
            "Tape, vanna/charm and 0DTE flow: majority bullish",
            NOT_A_FORECAST,
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
        bias_label = "Flow Bearish"
        regime_label = "Long Gamma \u00b7 Bearish Flow"
        regime_desc = "Dealers are net long gamma, and most flow signals lean bearish."
        setup = "Aligned Flow"
        playbook = [
            "Flow losing its bearish majority",
            "Net GEX turning negative, or the gradient strongly against it",
        ]
        expected_behavior = [
            "Net GEX positive: dealers net long gamma",
            "Tape, vanna/charm and 0DTE flow: majority bearish",
            NOT_A_FORECAST,
        ]
        push(tapeFlow, -1)
        push(vannaCharm, -1)
        push(odtePositioning, -1)
        push(positioningTrap, -1)
        push(trapDetection, -1)
        push(gammaVWAP, -1)
        # Regime strength, like netGEX: pushed the same way as in TREND_UP.
        push(netGEX, 1)
        push(msi, 1)
    elif market_state == "CHOP":
        trend = "neutral"
        bias = "RANGE_FADE"
        bias_label = "Mixed"
        regime_label = "Mixed Signals"
        regime_desc = (
            "The gamma regime, flow and structure signals do not line up into a defined state."
        )
        setup = "No Defined State"
        playbook = [
            "Flow forming a majority while dealers are long gamma",
            "Flow and structure splitting while dealers are short gamma",
        ]
        expected_behavior = [
            "No flow majority in long gamma, and no flow/structure split in short gamma",
            "Most minutes read this way; it does not mean the market is quiet",
            NOT_A_FORECAST,
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
