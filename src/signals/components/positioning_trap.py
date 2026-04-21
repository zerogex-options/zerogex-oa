"""Positioning trap component.

Detects intraday "crowded one-way positioning" setups where tape behavior
starts invalidating crowd direction and can trigger a squeeze / flush.

Uses SIGNED smart-money deltas (buy_premium - sell_premium) from
flow_contract_facts when available on the context (``smart_call_gross``
/ ``smart_put_gross`` under ctx.extra).  Signed net premium is more
informative than the legacy total_premium — a big put-buy and big
put-sell net out, which should not count as crowd skew.

+score => upside squeeze risk
-score => downside air-pocket risk
"""
from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import pct_change_n_bar


class PositioningTrapComponent(ComponentBase):
    name = "positioning_trap"
    weight = 0.06

    def compute(self, ctx: MarketContext) -> float:
        mom5 = pct_change_n_bar(ctx.recent_closes, 5)

        imbalance = self._signed_imbalance(ctx)

        short_crowding = max(0.0, min(1.0, (ctx.put_call_ratio - 1.05) / 0.35))
        long_crowding = max(0.0, min(1.0, (0.95 - ctx.put_call_ratio) / 0.35))

        put_skew = max(0.0, -imbalance)
        call_skew = max(0.0, imbalance)

        above_flip = 1.0 if (ctx.gamma_flip and ctx.close > ctx.gamma_flip) else 0.0
        below_flip = 1.0 if (ctx.gamma_flip and ctx.close < ctx.gamma_flip) else 0.0
        neg_gex = 1.0 if ctx.net_gex < 0 else 0.0

        squeeze = (
            0.45 * short_crowding
            + 0.25 * put_skew
            + 0.15 * max(0.0, min(1.0, mom5 / 0.004))
            + 0.10 * above_flip
            + 0.05 * neg_gex
        )

        flush = (
            0.45 * long_crowding
            + 0.25 * call_skew
            + 0.15 * max(0.0, min(1.0, (-mom5) / 0.004))
            + 0.10 * below_flip
            + 0.05 * neg_gex
        )

        return max(-1.0, min(1.0, squeeze - flush))

    def context_values(self, ctx: MarketContext) -> dict:
        imbalance = self._signed_imbalance(ctx)
        mom5 = pct_change_n_bar(ctx.recent_closes, 5)
        return {
            "put_call_ratio": ctx.put_call_ratio,
            "smart_imbalance": round(imbalance, 4),
            "smart_imbalance_source": self._imbalance_source(ctx),
            "momentum_5bar": round(mom5, 6),
            "close": ctx.close,
            "gamma_flip": ctx.gamma_flip,
            "net_gex": ctx.net_gex,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _signed_imbalance(ctx: MarketContext) -> float:
        """Prefer signed (buy - sell) net premium; fall back to total_premium."""
        call_signed = None
        put_signed = None
        if ctx.extra:
            call_signed = ctx.extra.get("smart_call_gross")
            put_signed = ctx.extra.get("smart_put_gross")
        if call_signed is not None and put_signed is not None:
            try:
                c = float(call_signed)
                p = float(put_signed)
            except (TypeError, ValueError):
                c = p = 0.0
            denom = abs(c) + abs(p)
            if denom >= 100_000:
                return (c - p) / denom
            return 0.0
        # Legacy fallback: unsigned total_premium.
        total = ctx.smart_call + ctx.smart_put
        if total < 100_000:
            return 0.0
        return (ctx.smart_call - ctx.smart_put) / total

    @staticmethod
    def _imbalance_source(ctx: MarketContext) -> str:
        if ctx.extra and ctx.extra.get("smart_call_gross") is not None:
            return "signed_net_premium"
        return "total_premium"
