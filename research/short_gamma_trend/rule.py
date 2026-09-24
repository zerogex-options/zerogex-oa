"""The candidate short-gamma trend state, beside the production rule it extends.

Production (``src/signals/trade_bias/bias.py``) has trend states only in long
gamma. In short gamma, flow that leans one way with no opposing structure falls
through to CHOP -- "Range-Bound: fade extremes of the session range". The
candidate relabels exactly those CHOP minutes and nothing else; every other
state comes from calling production's own ``compute_bias``.

``compute_bias`` does not expose its votes, so :func:`votes` mirrors the
arithmetic. ``tests/test_short_gamma_trend.py`` rebuilds production's state
from this mirror on randomized inputs and requires an exact match, so the two
cannot drift apart without a failing test.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from src.signals.trade_bias.bias import DOMINANT, MODERATE, STRONG, BiasInput, compute_bias

#: Candidate variants. ``flow`` is the one the verdict is about.
VARIANTS = ("flow", "aligned", "momentum")

#: The call each state makes: +1 up, -1 down. States absent here (CHOP,
#: UNKNOWN) make no directional call.
STATE_DIRECTION = {
    "TREND_UP": 1,
    "TREND_DOWN": -1,
    "TRAP_SQUEEZE": 1,
    "TRAP_REVERSAL": -1,
    "SG_TREND_UP": 1,
    "SG_TREND_DOWN": -1,
}

#: ``payload.inputs`` key -> ``BiasInput`` field.
_INPUT_FIELDS = {
    "net_gex": "netGEX",
    "gex_gradient": "gexGradient",
    "tape_flow": "tapeFlow",
    "vanna_charm": "vannaCharm",
    "odte_positioning": "odtePositioning",
    "positioning_trap": "positioningTrap",
    "trap_detection": "trapDetection",
    "gamma_vwap": "gammaVWAP",
    "msi": "msi",
}


def bias_input_from_payload(inputs: Any) -> Optional[BiasInput]:
    """``BiasInput`` from a persisted ``payload.inputs`` dict, or ``None``."""
    if not isinstance(inputs, Mapping):
        return None
    kwargs: dict[str, Optional[float]] = {}
    for key, field_name in _INPUT_FIELDS.items():
        value = inputs.get(key)
        if value is None:
            kwargs[field_name] = None
            continue
        try:
            kwargs[field_name] = float(value)
        except (TypeError, ValueError):
            kwargs[field_name] = None
    if all(v is None for v in kwargs.values()):
        return None
    return BiasInput(**kwargs)


@dataclass(frozen=True)
class Votes:
    is_short_gamma: bool
    is_long_gamma: bool
    bullish_flow: bool
    bearish_flow: bool
    bullish_structure: bool
    bearish_structure: bool
    available: int


def _conviction(value: Optional[float], sign: int, base: float) -> int:
    if value is None:
        return 0
    aligned = value * sign
    if aligned > DOMINANT:
        return 2
    if aligned > base:
        return 1
    return 0


def votes(inp: BiasInput) -> Votes:
    """The regime and vote booleans ``compute_bias`` derives internally."""
    net, grad = inp.netGEX, inp.gexGradient
    short = net is not None and net < 0 and (grad is None or grad < MODERATE)
    long_ = net is not None and net > 0 and (grad is None or grad > -MODERATE)

    def flow(sign: int) -> int:
        return (
            _conviction(inp.tapeFlow, sign, STRONG)
            + _conviction(inp.vannaCharm, sign, MODERATE)
            + _conviction(inp.odtePositioning, sign, MODERATE)
        )

    def structure(sign: int) -> int:
        return (
            _conviction(inp.positioningTrap, sign, MODERATE)
            + _conviction(inp.trapDetection, sign, STRONG)
            + _conviction(inp.gammaVWAP, sign, MODERATE)
        )

    bull_f, bear_f = flow(1), flow(-1)
    bull_s, bear_s = structure(1), structure(-1)
    available = sum(1 for f in _INPUT_FIELDS.values() if getattr(inp, f) is not None)
    return Votes(
        is_short_gamma=short,
        is_long_gamma=long_,
        bullish_flow=bull_f >= 2 and bull_f > bear_f,
        bearish_flow=bear_f >= 2 and bear_f > bull_f,
        bullish_structure=bull_s >= 2 and bull_s > bear_s,
        bearish_structure=bear_s >= 2 and bear_s > bull_s,
        available=available,
    )


def current_state(inp: BiasInput) -> str:
    """Production's market state, from production's own code."""
    return compute_bias(inp).marketState


def legacy_state(inp: BiasInput, current: Optional[str] = None) -> str:
    """The market state the rule stored before the 2026-09-23 MSI fix.

    The old rule gated TREND_DOWN on ``msi <= 10`` (and TREND_UP on
    ``msi >= -10``, which a 0-100 score always passes). A TREND_DOWN that the
    gate would have blocked fell through to CHOP, or UNKNOWN with fewer than
    four inputs. Used only to check that the stored inputs reproduce the
    stored states.
    """
    state = current if current is not None else current_state(inp)
    if state == "TREND_DOWN" and inp.msi is not None and inp.msi > 10:
        return "CHOP" if votes(inp).available >= 4 else "UNKNOWN"
    if state == "TREND_UP" and inp.msi is not None and inp.msi < -10:
        return "CHOP" if votes(inp).available >= 4 else "UNKNOWN"
    return state


def candidate_state(
    inp: BiasInput,
    variant: str,
    *,
    current: Optional[str] = None,
    prior_move: int = 0,
) -> str:
    """The market state under one candidate variant.

    ``current`` is production's state for ``inp`` when the caller already has
    it. ``prior_move`` is the sign of the prior-30-minute move within the same
    session (+1 / -1 / 0); only the ``momentum`` variant reads it.
    """
    if variant not in VARIANTS:
        raise ValueError(f"unknown variant {variant!r}")
    state = current if current is not None else current_state(inp)
    if state != "CHOP":
        return state
    v = votes(inp)
    if not v.is_short_gamma:
        return state
    if variant == "flow":
        # Within CHOP, short gamma with bearish flow cannot have bullish
        # structure (that is TRAP_SQUEEZE), so flow alone decides it.
        if v.bearish_flow:
            return "SG_TREND_DOWN"
        if v.bullish_flow:
            return "SG_TREND_UP"
    elif variant == "aligned":
        if v.bearish_flow and v.bearish_structure:
            return "SG_TREND_DOWN"
        if v.bullish_flow and v.bullish_structure:
            return "SG_TREND_UP"
    else:  # momentum
        if prior_move < 0:
            return "SG_TREND_DOWN"
        if prior_move > 0:
            return "SG_TREND_UP"
    return state
