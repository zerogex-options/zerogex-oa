"""GEX regime scoring component."""
from src.signals.components.base import ComponentBase, MarketContext


class GexRegimeComponent(ComponentBase):
    name = "gex_regime"
    weight = 0.18

    def compute(self, ctx: MarketContext) -> float:
        return -1.0 if ctx.net_gex < 0 else 1.0

    def context_values(self, ctx: MarketContext) -> dict:
        return {"net_gex": ctx.net_gex}
