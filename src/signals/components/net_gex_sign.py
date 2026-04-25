"""Market State component: net GEX sign contribution."""

from __future__ import annotations

from src.signals.components.base import ComponentBase, MarketContext


class NetGexSignComponent(ComponentBase):
    name = "net_gex_sign"
    weight = 20.0

    def compute(self, ctx: MarketContext) -> float:
        if ctx.net_gex < 0:
            return 1.0
        if ctx.net_gex > 0:
            return -1.0
        return 0.0

    def context_values(self, ctx: MarketContext) -> dict:
        score = self.compute(ctx)
        return {
            "net_gex": float(ctx.net_gex or 0.0),
            "score": score,
            "max_points": 20,
            "points": round(20.0 * score, 4),
        }
