"""Positioning trap component.

Detects intraday "crowded one-way positioning" setups where tape behavior
starts invalidating crowd direction and can trigger a squeeze / flush.

+score => upside squeeze risk
-score => downside air-pocket risk
"""
from src.signals.components.base import ComponentBase, MarketContext


class PositioningTrapComponent(ComponentBase):
    name = "positioning_trap"
    weight = 0.10

    @staticmethod
    def _momentum(closes: list[float], bars_back: int) -> float:
        if len(closes) < bars_back or closes[-bars_back] <= 0:
            return 0.0
        return (closes[-1] - closes[-bars_back]) / closes[-bars_back]

    def compute(self, ctx: MarketContext) -> float:
        closes = ctx.recent_closes
        mom5 = self._momentum(closes, 5)

        sm_total = ctx.smart_call + ctx.smart_put
        imbalance = (ctx.smart_call - ctx.smart_put) / sm_total if sm_total >= 100_000 else 0.0

        # Position crowding proxies from options flow and broad p/c ratio.
        short_crowding = max(0.0, min(1.0, (ctx.put_call_ratio - 1.05) / 0.35))
        long_crowding = max(0.0, min(1.0, (0.95 - ctx.put_call_ratio) / 0.35))

        put_skew = max(0.0, -imbalance)
        call_skew = max(0.0, imbalance)

        above_flip = 1.0 if (ctx.gamma_flip and ctx.close > ctx.gamma_flip) else 0.0
        below_flip = 1.0 if (ctx.gamma_flip and ctx.close < ctx.gamma_flip) else 0.0
        neg_gex = 1.0 if ctx.net_gex < 0 else 0.0

        # Squeeze setup: crowd is short/defensive, but price is resilient and in
        # an amplifying regime. Any positive catalyst can force upside repricing.
        squeeze = (
            0.45 * short_crowding
            + 0.25 * put_skew
            + 0.15 * max(0.0, min(1.0, mom5 / 0.004))
            + 0.10 * above_flip
            + 0.05 * neg_gex
        )

        # Flush setup: crowd is long/complacent while tape weakens.
        flush = (
            0.45 * long_crowding
            + 0.25 * call_skew
            + 0.15 * max(0.0, min(1.0, (-mom5) / 0.004))
            + 0.10 * below_flip
            + 0.05 * neg_gex
        )

        return max(-1.0, min(1.0, squeeze - flush))

    def context_values(self, ctx: MarketContext) -> dict:
        sm_total = ctx.smart_call + ctx.smart_put
        imbalance = (ctx.smart_call - ctx.smart_put) / sm_total if sm_total > 0 else 0.0
        mom5 = self._momentum(ctx.recent_closes, 5)
        return {
            "put_call_ratio": ctx.put_call_ratio,
            "smart_imbalance": round(imbalance, 4),
            "momentum_5bar": round(mom5, 6),
            "close": ctx.close,
            "gamma_flip": ctx.gamma_flip,
            "net_gex": ctx.net_gex,
        }
