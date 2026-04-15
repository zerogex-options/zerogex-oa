"""Put/call ratio scoring component with linear interpolation."""
from src.signals.components.base import ComponentBase, MarketContext


class PutCallRatioComponent(ComponentBase):
    name = "put_call_ratio"
    weight = 0.05

    def compute(self, ctx: MarketContext) -> float:
        pcr = ctx.put_call_ratio
        if pcr <= 0.8:
            return 1.0
        if pcr >= 1.2:
            return -1.0
        # Linear interpolation: 0.8 -> +1.0, 1.0 -> 0.0, 1.2 -> -1.0
        return 1.0 - ((pcr - 0.8) / 0.4) * 2.0

    def context_values(self, ctx: MarketContext) -> dict:
        return {"put_call_ratio": ctx.put_call_ratio}
