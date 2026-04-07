"""Smart money flow scoring component."""
from src.signals.components.base import ComponentBase, MarketContext


class SmartMoneyComponent(ComponentBase):
    name = "smart_money"
    weight = 0.16

    def compute(self, ctx: MarketContext) -> float:
        sm_total = ctx.smart_call + ctx.smart_put
        if sm_total < 100_000:
            return 0.0  # Insufficient premium flow -- no edge
        sm_ratio = (ctx.smart_call + 1.0) / (ctx.smart_put + 1.0)
        if sm_ratio > 1.2:
            return 1.0
        if sm_ratio < 0.8:
            return -1.0
        return 0.0

    def context_values(self, ctx: MarketContext) -> dict:
        return {
            "smart_call": ctx.smart_call,
            "smart_put": ctx.smart_put,
            "sm_total": ctx.smart_call + ctx.smart_put,
            "sm_ratio": round((ctx.smart_call + 1.0) / (ctx.smart_put + 1.0), 4),
        }
