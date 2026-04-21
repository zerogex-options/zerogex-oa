"""Market State component: volatility regime (+/-10)."""
from __future__ import annotations

import math

from src.signals.components.base import ComponentBase, MarketContext


class VolatilityRegimeComponent(ComponentBase):
    name = "volatility_regime"
    weight = 10.0  # +/-10 points

    def compute(self, ctx: MarketContext) -> float:
        vix = (ctx.extra or {}).get("vix_level")
        if vix is not None:
            try:
                return max(-1.0, min(1.0, (float(vix) - 20.0) / 10.0))
            except (TypeError, ValueError):
                pass

        closes = ctx.recent_closes or []
        if len(closes) < 2:
            return 0.0
        rets = []
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            curr = closes[i]
            if prev and prev > 0:
                rets.append((curr - prev) / prev)
        if not rets:
            return 0.0
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / len(rets)
        realized = math.sqrt(max(var, 0.0))
        return max(-1.0, min(1.0, (realized - 0.002) / 0.003))

    def context_values(self, ctx: MarketContext) -> dict:
        return {
            "vix_level": (ctx.extra or {}).get("vix_level"),
            "score": round(self.compute(ctx), 6),
        }
