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

import math

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

        # Smooth flip-position and gex-regime gauges instead of binary
        # 1/0 flags so the score stays continuous near transition points.
        above_flip = self._flip_lean(ctx, side="above")
        below_flip = self._flip_lean(ctx, side="below")
        neg_gex = self._neg_gex_lean(ctx)

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

    @staticmethod
    def _flip_lean(ctx: MarketContext, side: str) -> float:
        flip = ctx.gamma_flip
        if not flip or ctx.close <= 0:
            return 0.0
        try:
            distance = (ctx.close - float(flip)) / ctx.close
        except (TypeError, ValueError, ZeroDivisionError):
            return 0.0
        # Saturates by ~0.5% from flip; preserves a continuous read instead
        # of a binary 0/1 jump at the flip strike.
        leaning = max(-1.0, min(1.0, distance / 0.005))
        return max(0.0, leaning) if side == "above" else max(0.0, -leaning)

    @staticmethod
    def _neg_gex_lean(ctx: MarketContext) -> float:
        net_gex = float(ctx.net_gex or 0.0)
        # Smooth tanh transition centered on zero so the regime tilt
        # contribution is graded rather than a 1/0 flip at sign change.
        scale = 5.0e8
        return max(0.0, min(1.0, 0.5 * (1.0 - math.tanh(net_gex / scale))))

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
            call_signed = ctx.extra.get("smart_call")
            put_signed = ctx.extra.get("smart_put")
            if call_signed is None or put_signed is None:
                call_signed = ctx.extra.get("smart_call_net")
                put_signed = ctx.extra.get("smart_put_net")
        if call_signed is not None and put_signed is not None:
            try:
                c = float(call_signed)
                p = float(put_signed)
            except (TypeError, ValueError):
                c = p = 0.0
            denom = abs(c) + abs(p)
            if denom <= 0:
                return 0.0
            ratio = (c - p) / denom
            confidence = min(1.0, denom / 100_000.0)
            return ratio * confidence
        # Fallback to top-level signed smart-call/smart-put fields.
        c = float(ctx.smart_call or 0.0)
        p = float(ctx.smart_put or 0.0)
        total = abs(c) + abs(p)
        if total <= 0:
            return 0.0
        ratio = (c - p) / total
        confidence = min(1.0, total / 100_000.0)
        return ratio * confidence

    @staticmethod
    def _imbalance_source(ctx: MarketContext) -> str:
        if ctx.extra and (
            ctx.extra.get("smart_call") is not None or ctx.extra.get("smart_call_net") is not None
        ):
            return "signed_net_premium"
        return "signed_top_level"
