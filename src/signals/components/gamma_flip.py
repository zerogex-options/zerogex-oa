"""Gamma flip scoring component.

Sign convention: above the flip = bullish score because price is in
negative-GEX territory where dealer hedging amplifies moves.

This component can contradict gex_regime when GEX sign and price/flip
position disagree -- this is intentional, as both are meaningful
independent signals.
"""
from src.signals.components.base import ComponentBase, MarketContext


class GammaFlipComponent(ComponentBase):
    name = "gamma_flip"
    weight = 0.12

    def compute(self, ctx: MarketContext) -> float:
        if not ctx.gamma_flip:
            return 0.0
        dist = (ctx.close - ctx.gamma_flip) / ctx.gamma_flip
        if abs(dist) < 0.003:
            return 0.0  # Uncertainty zone -- near the regime inflection point
        if dist > 0:
            return 1.0  # Above flip: negative-GEX territory -> momentum amplification
        return -1.0  # Below flip: positive-GEX territory -> mean-reversion dampening

    def context_values(self, ctx: MarketContext) -> dict:
        return {
            "gamma_flip": ctx.gamma_flip,
            "close": ctx.close,
            "distance_pct": round((ctx.close - ctx.gamma_flip) / ctx.gamma_flip, 6) if ctx.gamma_flip else None,
        }
