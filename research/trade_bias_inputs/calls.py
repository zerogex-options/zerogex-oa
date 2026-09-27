"""The call each of the nine Trade Bias inputs makes, and its reading bands.

The bars are pre-registered in README.md and written here as numbers rather
than read from ``src.signals.trade_bias.bias``: production's thresholds can be
overridden from the environment, and a setting on the box must not move a
research verdict. ``tests/test_trade_bias_inputs.py`` pins them to
production's defaults.

Positive is bullish for every input, which is how every production consumer
reads them (the panel's votes and the continuous bias score). An input calls
up when its reading is past the bar above its center, down when it is past
the bar below, and makes no call in between -- strictly past, as production's
``conviction`` compares.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from src.signals.trade_bias.bias import BiasInput

#: Production's default vote bars (``bias.STRONG`` / ``MODERATE`` / ``DOMINANT``).
STRONG = 25.0
MODERATE = 12.0
DOMINANT = 65.0

#: The MSI is 0-100 with 50 as its neutral.
MSI_CENTER = 50.0
MSI_BAR = 12.0
#: The MSI's own regime-label edges (``ScoringEngine._regime_label``).
MSI_BAND_EDGES = (20.0, 40.0, 70.0)
MSI_BAND_LABELS = (
    "below 20 (high-risk reversal)",
    "20-40 (chop / range)",
    "40-70 (controlled trend)",
    "70+ (trend expansion)",
)

#: How an input's reading is banded for the strength table.
VOTE = "vote"
GAMMA_SIGN = "gamma_sign"
INDEX = "index"


def _fmt(x: float) -> str:
    return f"{x:g}"


@dataclass(frozen=True)
class InputRule:
    #: ``payload.inputs`` key.
    key: str
    #: ``BiasInput`` attribute.
    field: str
    #: The panel's name for it.
    title: str
    kind: str
    center: float = 0.0
    bar: float = 0.0

    def value(self, inp: Optional[BiasInput]) -> Optional[float]:
        if inp is None:
            return None
        v = getattr(inp, self.field)
        if v is None:
            return None
        v = float(v)
        return v if math.isfinite(v) else None

    def call(self, value: Optional[float]) -> int:
        """+1 up, -1 down, 0 no call."""
        if value is None:
            return 0
        x = value - self.center
        if x > self.bar:
            return 1
        if x < -self.bar:
            return -1
        return 0

    @property
    def rule_text(self) -> str:
        if self.kind == GAMMA_SIGN:
            return "long gamma = up, short gamma = down"
        up, down = _fmt(self.center + self.bar), _fmt(self.center - self.bar)
        if self.center == 0.0:
            up = "+" + up
        return f"above {up} = up, below {down} = down"

    @property
    def band_labels(self) -> tuple[str, ...]:
        if self.kind == GAMMA_SIGN:
            return ("short gamma", "long gamma")
        if self.kind == INDEX:
            return MSI_BAND_LABELS
        b, d = _fmt(self.bar), _fmt(DOMINANT)
        return (
            f"below -{d}",
            f"-{d} to -{b}",
            f"within +/-{b}",
            f"+{b} to +{d}",
            f"above +{d}",
        )

    def band(self, value: Optional[float]) -> Optional[int]:
        """Index into :attr:`band_labels`, or ``None`` without a reading.

        The vote bands split exactly where the call and production's
        two-vote ``DOMINANT`` level do, so bands 0-1 are the down calls, 2 is
        no call and 3-4 are the up calls.
        """
        if value is None:
            return None
        if self.kind == GAMMA_SIGN:
            if value < 0:
                return 0
            return 1 if value > 0 else None
        if self.kind == INDEX:
            return sum(1 for edge in MSI_BAND_EDGES if value >= edge)
        if value < -DOMINANT:
            return 0
        if value < -self.bar:
            return 1
        if value <= self.bar:
            return 2
        return 3 if value <= DOMINANT else 4


#: The nine, flow votes first, then structure votes, then the regime inputs.
INPUTS: tuple[InputRule, ...] = (
    InputRule("tape_flow", "tapeFlow", "Tape Flow", VOTE, bar=STRONG),
    InputRule("vanna_charm", "vannaCharm", "Vanna/Charm", VOTE, bar=MODERATE),
    InputRule("odte_positioning", "odtePositioning", "0DTE Positioning", VOTE, bar=MODERATE),
    InputRule("positioning_trap", "positioningTrap", "Positioning Trap", VOTE, bar=MODERATE),
    InputRule("trap_detection", "trapDetection", "Trap Detection", VOTE, bar=STRONG),
    InputRule("gamma_vwap", "gammaVWAP", "Gamma/VWAP", VOTE, bar=MODERATE),
    InputRule("gex_gradient", "gexGradient", "GEX Gradient", VOTE, bar=MODERATE),
    InputRule("net_gex", "netGEX", "Net GEX (gamma sign)", GAMMA_SIGN),
    InputRule("msi", "msi", "MSI", INDEX, center=MSI_CENTER, bar=MSI_BAR),
)
INPUTS_BY_KEY = {rule.key: rule for rule in INPUTS}
