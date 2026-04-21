"""Gamma flip scoring component — continuous distance-from-flip model.

The legacy implementation returned -1/0/+1 with a 0.3% dead zone.  This
version uses a smooth tanh mapping over the percent-distance to the
flip point, preserving both sign (bullish when price is above flip,
bearish below) and magnitude (stronger signal the further into the
regime we are).

Sign convention is unchanged: above the flip = negative-GEX territory
= momentum amplification = bullish for price.  This component can
still contradict gex_regime if the *aggregate* GEX sign and the
price-vs-flip position disagree — that's a real feature, not a bug.
"""
from __future__ import annotations

import math
import os

from src.signals.components.base import ComponentBase, MarketContext

# Percent distance from flip that yields ~tanh(1)≈0.76. A 1% distance
# from the flip produces a near-saturated score; 0.1% is effectively
# neutral (previously we had a hard 0.3% dead zone).
_DIST_NORM = float(os.getenv("SIGNAL_GAMMA_FLIP_NORM", "0.005"))


class GammaFlipComponent(ComponentBase):
    name = "gamma_flip"
    weight = 0.05

    def compute(self, ctx: MarketContext) -> float:
        if not ctx.gamma_flip or ctx.gamma_flip <= 0 or _DIST_NORM <= 0:
            return 0.0
        dist = (ctx.close - ctx.gamma_flip) / ctx.gamma_flip
        return math.tanh(dist / _DIST_NORM)

    def context_values(self, ctx: MarketContext) -> dict:
        dist_pct = None
        score = 0.0
        if ctx.gamma_flip and ctx.gamma_flip > 0:
            dist_pct = round((ctx.close - ctx.gamma_flip) / ctx.gamma_flip, 6)
            if _DIST_NORM > 0:
                score = round(math.tanh(dist_pct / _DIST_NORM), 6)
        return {
            "gamma_flip": ctx.gamma_flip,
            "close": ctx.close,
            "distance_pct": dist_pct,
            "dist_norm": _DIST_NORM,
            "score": score,
        }
