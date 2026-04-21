"""GEX regime scoring component.

Net GEX controls *how much* price moves are amplified vs damped:

* net_gex < 0 (short gamma): dealers amplify the prevailing move.
* net_gex > 0 (long gamma): dealers damp both directions / pin.

That means net_gex sign alone is not directional. Direction comes from
price state (vs gamma flip) and recent momentum; regime sign controls
whether that directional anchor is amplified (short-gamma) or damped
(long-gamma).
"""
from __future__ import annotations

import math
import os

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import pct_change_n_bar

# GEX magnitude at which score reaches ~tanh(1) ≈ 0.76. A full saturation
# to ~0.99 happens near 3x this value.
_GEX_NORM = float(os.getenv("SIGNAL_GEX_REGIME_NORM", "2.5e8"))
_LONG_GAMMA_DAMPING = max(
    0.0,
    min(1.0, float(os.getenv("SIGNAL_GEX_REGIME_LONG_GAMMA_DAMPING", "0.35"))),
)
_FLIP_NEUTRAL_BAND_PCT = max(
    0.0, float(os.getenv("SIGNAL_GEX_REGIME_FLIP_NEUTRAL_BAND_PCT", "0.001"))
)
_MOMENTUM_NEUTRAL_BAND = max(
    0.0, float(os.getenv("SIGNAL_GEX_REGIME_MOMENTUM_NEUTRAL_BAND", "0.0008"))
)


class GexRegimeComponent(ComponentBase):
    name = "gex_regime"
    weight = 0.07

    def compute(self, ctx: MarketContext) -> float:
        if _GEX_NORM <= 0:
            return 0.0
        regime_strength = math.tanh(abs(ctx.net_gex) / _GEX_NORM)
        direction_anchor = self._direction_anchor(ctx)
        if direction_anchor == 0.0:
            return 0.0
        if ctx.net_gex < 0:
            # Short gamma: amplify prevailing direction.
            score = direction_anchor * regime_strength
        else:
            # Long gamma: same directional anchor, but dampened.
            score = direction_anchor * regime_strength * _LONG_GAMMA_DAMPING
        return max(-1.0, min(1.0, score))

    def context_values(self, ctx: MarketContext) -> dict:
        regime_strength = math.tanh(abs(ctx.net_gex) / _GEX_NORM) if _GEX_NORM > 0 else 0.0
        direction_anchor = self._direction_anchor(ctx)
        score = self.compute(ctx)
        return {
            "net_gex": ctx.net_gex,
            "gex_norm": _GEX_NORM,
            "regime": "short_gamma" if ctx.net_gex < 0 else "long_gamma",
            "regime_strength": round(regime_strength, 6),
            "direction_anchor": round(direction_anchor, 6),
            "long_gamma_damping": _LONG_GAMMA_DAMPING,
            "score": round(score, 6),
        }

    @staticmethod
    def _direction_anchor(ctx: MarketContext) -> float:
        if ctx.gamma_flip and ctx.gamma_flip > 0:
            flip_dist = (ctx.close - ctx.gamma_flip) / ctx.gamma_flip
            if abs(flip_dist) >= _FLIP_NEUTRAL_BAND_PCT:
                return 1.0 if flip_dist > 0 else -1.0
        mom = pct_change_n_bar(ctx.recent_closes or [], 5)
        if abs(mom) < _MOMENTUM_NEUTRAL_BAND:
            return 0.0
        return max(-1.0, min(1.0, mom / 0.003))
