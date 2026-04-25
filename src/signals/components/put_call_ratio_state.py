"""Market-state put/call ratio component."""

from __future__ import annotations

from src.signals.components.base import ComponentBase, MarketContext


class PutCallRatioStateComponent(ComponentBase):
    name = "put_call_ratio"
    weight = 0.15

    def compute(self, ctx: MarketContext) -> float:
        pcr = float(ctx.put_call_ratio or 1.0)
        # Higher PCR => more fragile state / larger potential move.
        return max(-1.0, min(1.0, (pcr - 1.0) / 0.4))

    def context_values(self, ctx: MarketContext) -> dict:
        score = self.compute(ctx)
        return {
            "put_call_ratio": float(ctx.put_call_ratio or 1.0),
            "score": round(score, 6),
            "component_points": round(score * self.weight, 4),
        }
