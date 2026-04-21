"""Market State component: distance to gamma flip (±25 bucket)."""
from __future__ import annotations

from src.signals.components.base import ComponentBase, MarketContext


class FlipDistanceComponent(ComponentBase):
    name = "flip_distance"
    weight = 0.25

    def compute(self, ctx: MarketContext) -> float:
        fd = (ctx.extra or {}).get("flip_distance")
        if fd is None and ctx.gamma_flip is not None and ctx.close > 0:
            try:
                fd = (ctx.close - float(ctx.gamma_flip)) / ctx.close
            except (TypeError, ValueError, ZeroDivisionError):
                return 0.0
        if fd is None:
            return 0.0
        # Near flip => +1 (higher volatility potential), far => -1 (stable).
        return max(-1.0, min(1.0, 1.0 - (abs(float(fd)) / 0.02)))

    def context_values(self, ctx: MarketContext) -> dict:
        score = self.compute(ctx)
        return {
            "flip_distance": (ctx.extra or {}).get("flip_distance"),
            "score": round(score, 6),
            "weight_points": 25,
            "contribution_points": round(score * 25.0, 4),
        }
