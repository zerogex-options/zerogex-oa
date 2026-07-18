"""Directional momentum component.

Promotes the shared ``vol_normalized_momentum`` helper (utils.py) — until now
consumed privately by vol_expansion / squeeze_setup — into a first-class signed
component so Trade Bias can read momentum as one of its tactical pillars.

Score convention (ComponentBase): +1 strong up-momentum, -1 strong down, 0 flat
/ insufficient data. The n-bar return is divided by its vol-projected sigma and
clipped to ±3 sigma, then mapped onto [-1, 1] — so the read is vol-regime aware
(a 0.3% push means more in a dead tape than in a fast one).
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import vol_normalized_momentum

# Lookback for the momentum return, the realized-vol window, and the sigma clip.
_MOMENTUM_N = int(os.getenv("TRADE_BIAS_MOMENTUM_N", "5"))
_MOMENTUM_VOL_WINDOW = int(os.getenv("TRADE_BIAS_MOMENTUM_VOL_WINDOW", "60"))
_CLIP = 3.0


class MomentumComponent(ComponentBase):
    name = "momentum"
    weight = 0.0  # Not an MSI component; consumed by the Trade Bias tactical read.

    def _z(self, ctx: MarketContext) -> tuple[float, float]:
        closes = ctx.recent_closes or []
        return vol_normalized_momentum(
            closes, n=_MOMENTUM_N, vol_window=_MOMENTUM_VOL_WINDOW, clip=_CLIP
        )

    def compute(self, ctx: MarketContext) -> float:
        _pct, z = self._z(ctx)
        return max(-1.0, min(1.0, z / _CLIP))

    def context_values(self, ctx: MarketContext) -> dict:
        pct, z = self._z(ctx)
        return {
            "pct_change": round(pct, 6),
            "z": round(z, 4),
            "n_bar": _MOMENTUM_N,
            "score": round(max(-1.0, min(1.0, z / _CLIP)), 6),
        }
