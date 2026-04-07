"""Volatility expansion scoring component with asymmetry fix."""
from src.signals.components.base import ComponentBase, MarketContext


class VolExpansionComponent(ComponentBase):
    name = "vol_expansion"
    weight = 0.16

    # Mean-reversion signals are intentionally dampened relative to momentum signals.
    # In negative-GEX regimes, dealer hedging amplifies directional moves (full weight).
    # In positive-GEX regimes, dealer hedging dampens moves toward the flip (half weight).
    # This asymmetry reflects the empirical observation that momentum amplification
    # is a stronger and faster-acting force than mean-reversion pull.
    _MEAN_REVERSION_DAMPENER: float = 0.5

    def compute(self, ctx: MarketContext) -> float:
        vol_pressure = min(1.0, abs(ctx.net_gex) / 5_000_000_000)

        closes = ctx.recent_closes
        price_momentum_dir = 0.0
        if len(closes) >= 5 and closes[-5] > 0:
            price_momentum_dir = 1.0 if closes[-1] > closes[-5] else -1.0

        if ctx.net_gex < 0:
            # Negative GEX: dealers amplify moves -- directional with price momentum
            return price_momentum_dir * vol_pressure
        else:
            # Positive GEX: dealers dampen moves -- mean-reversion pull toward gamma flip
            if ctx.gamma_flip and ctx.gamma_flip > 0:
                flip_dist_ratio = (ctx.close - ctx.gamma_flip) / ctx.gamma_flip
                if abs(flip_dist_ratio) > 0.003:
                    pull = min(abs(flip_dist_ratio) / 0.01, 1.0) * vol_pressure * self._MEAN_REVERSION_DAMPENER
                    return pull * (-1.0 if flip_dist_ratio > 0 else 1.0)
            return 0.0

    def context_values(self, ctx: MarketContext) -> dict:
        vol_pressure = min(1.0, abs(ctx.net_gex) / 5_000_000_000)
        closes = ctx.recent_closes
        price_momentum_dir = 0.0
        if len(closes) >= 5 and closes[-5] > 0:
            price_momentum_dir = 1.0 if closes[-1] > closes[-5] else -1.0
        return {
            "net_gex": ctx.net_gex,
            "vol_pressure": vol_pressure,
            "price_momentum_dir": price_momentum_dir,
            "gex_regime": "negative" if ctx.net_gex < 0 else "positive",
        }
