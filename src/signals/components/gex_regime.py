"""GEX regime scoring component — continuous magnitude-aware model.

The legacy version of this component emitted -1 or +1 with no regard
for the *magnitude* of the dealer exposure. That threw away the single
most informative piece of the data: how *decisively* dealers are long
or short gamma.

We now use a smooth tanh mapping anchored at a configurable GEX norm.
The result is still in [-1, +1], but moves monotonically through zero
and never saturates early:

    score = -tanh(net_gex / GEX_NORM)

Sign convention is unchanged (negative net_gex -> bullish score for
the composite, because dealers short gamma amplify directional moves).
"""
from __future__ import annotations

import math
import os

from src.signals.components.base import ComponentBase, MarketContext

# GEX magnitude at which score reaches ~tanh(1) ≈ 0.76. A full saturation
# to ~0.99 happens near 3x this value.
_GEX_NORM = float(os.getenv("SIGNAL_GEX_REGIME_NORM", "2.5e8"))


class GexRegimeComponent(ComponentBase):
    name = "gex_regime"
    weight = 0.07

    def compute(self, ctx: MarketContext) -> float:
        if _GEX_NORM <= 0:
            return 0.0
        return -math.tanh(ctx.net_gex / _GEX_NORM)

    def context_values(self, ctx: MarketContext) -> dict:
        return {
            "net_gex": ctx.net_gex,
            "gex_norm": _GEX_NORM,
            "score": round(-math.tanh(ctx.net_gex / _GEX_NORM), 6)
            if _GEX_NORM > 0
            else 0.0,
        }
