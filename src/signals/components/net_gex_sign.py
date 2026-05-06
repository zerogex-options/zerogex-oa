"""Market State component: net GEX sign contribution.

Previously binary (+/-1/0) which discarded magnitude entirely — every reading
collapsed to one of three values.  We now use a tanh-shaped continuous
response so that the score spans the full [-1, +1] spectrum and lands at 0
only when net GEX is genuinely zero (a structurally rare event).
"""

from __future__ import annotations

import math
import os

from src.signals.components.base import ComponentBase, MarketContext

# Net-GEX magnitude (industry-standard $ gamma per 1% move) at which the
# tanh response approaches saturation.  Calibrated for SPY-magnitude
# underlyings; per-symbol normalizers from ``component_normalizer_cache``
# override at runtime when populated.
_GEX_SCALE = float(os.getenv("SIGNAL_NET_GEX_SIGN_SCALE", "2.0e9"))


class NetGexSignComponent(ComponentBase):
    name = "net_gex_sign"
    weight = 20.0

    def compute(self, ctx: MarketContext) -> float:
        net_gex = float(ctx.net_gex or 0.0)
        scale = self._scale(ctx)
        if scale <= 0:
            return 0.0
        # Negative GEX (short-gamma) amplifies volatility -> +1 leaning;
        # positive GEX damps moves -> -1 leaning.  tanh keeps the response
        # smooth and bounded in [-1, +1].
        return max(-1.0, min(1.0, math.tanh(-net_gex / scale)))

    @staticmethod
    def _scale(ctx: MarketContext) -> float:
        norms = (ctx.extra or {}).get("normalizers") if ctx.extra else None
        if isinstance(norms, dict):
            v = norms.get("net_gex")
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except (TypeError, ValueError):
                pass
        return _GEX_SCALE

    def context_values(self, ctx: MarketContext) -> dict:
        score = self.compute(ctx)
        return {
            "net_gex": float(ctx.net_gex or 0.0),
            "scale": round(self._scale(ctx), 2),
            "score": round(score, 6),
            "max_points": 20,
            "points": round(20.0 * score, 4),
        }
