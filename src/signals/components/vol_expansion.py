"""Volatility expansion scoring component."""
from src.signals.components.base import ComponentBase, MarketContext

# 0.5% price change over 5 bars saturates momentum to ±1.
# Smaller moves produce proportionally smaller scores.
_MOMENTUM_NORM = 0.005

# $5B net GEX saturates vol_pressure to 1.0.
_GEX_NORM = 5_000_000_000


class VolExpansionComponent(ComponentBase):
    name = "vol_expansion"
    weight = 0.16

    def compute(self, ctx: MarketContext) -> float:
        """Score vol expansion readiness in [-1, +1].

        Positive GEX is a vol-suppression regime (dealers dampen moves by
        hedging against the crowd), so it contributes 0.  Only negative GEX
        creates the dealer feedback loop that amplifies directional moves.

        Score = momentum * vol_pressure, where:
          vol_pressure  — how much amplification dealers can provide (GEX magnitude)
          momentum      — direction and strength of the move they must chase
        """
        if ctx.net_gex >= 0:
            return 0.0

        vol_pressure = min(1.0, abs(ctx.net_gex) / _GEX_NORM)

        closes = ctx.recent_closes
        if len(closes) < 5 or closes[-5] <= 0:
            return 0.0

        pct_change = (closes[-1] - closes[-5]) / closes[-5]
        momentum = max(-1.0, min(1.0, pct_change / _MOMENTUM_NORM))

        return momentum * vol_pressure

    def context_values(self, ctx: MarketContext) -> dict:
        vol_pressure = min(1.0, abs(ctx.net_gex) / _GEX_NORM)
        closes = ctx.recent_closes
        pct_change_5bar = None
        if len(closes) >= 5 and closes[-5] > 0:
            pct_change_5bar = round((closes[-1] - closes[-5]) / closes[-5], 6)
        return {
            "net_gex": ctx.net_gex,
            "gex_regime": "negative" if ctx.net_gex < 0 else "positive",
            "vol_pressure": round(vol_pressure, 4),
            "pct_change_5bar": pct_change_5bar,
        }
