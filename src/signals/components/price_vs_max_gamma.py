"""Price-vs-max-gamma component for Market State Index.

Close to max gamma implies pinning (lower move potential).
Far from max gamma implies freer movement (higher potential).
"""
from __future__ import annotations

from src.signals.components.base import ComponentBase, MarketContext


class PriceVsMaxGammaComponent(ComponentBase):
    name = "price_vs_max_gamma"
    weight = 10.0

    def compute(self, ctx: MarketContext) -> float:
        max_gamma_strike = (ctx.extra or {}).get("max_gamma_strike")
        if max_gamma_strike is None or ctx.close <= 0:
            return 0.0
        try:
            distance = abs((ctx.close - float(max_gamma_strike)) / ctx.close)
        except (TypeError, ValueError, ZeroDivisionError):
            return 0.0
        # 1% away is "free"; near 0 distance is "pinned".
        return max(-1.0, min(1.0, (distance / 0.01) - 1.0))

    def context_values(self, ctx: MarketContext) -> dict:
        max_gamma_strike = (ctx.extra or {}).get("max_gamma_strike")
        distance_pct = None
        score = 0.0
        if max_gamma_strike is not None and ctx.close > 0:
            try:
                distance_pct = abs((ctx.close - float(max_gamma_strike)) / ctx.close)
                score = max(-1.0, min(1.0, (distance_pct / 0.01) - 1.0))
            except (TypeError, ValueError, ZeroDivisionError):
                distance_pct = None
                score = 0.0
        return {
            "close": ctx.close,
            "max_gamma_strike": max_gamma_strike,
            "distance_pct": distance_pct,
            "score": round(score, 6),
        }
