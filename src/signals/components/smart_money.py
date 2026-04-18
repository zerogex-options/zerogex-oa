"""Smart money flow scoring component."""
from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import pct_change_n_bar


class SmartMoneyComponent(ComponentBase):
    name = "smart_money"
    weight = 0.09

    def compute(self, ctx: MarketContext) -> float:
        sm_total = ctx.smart_call + ctx.smart_put
        if sm_total < 100_000:
            return 0.0  # Insufficient premium flow -- no edge

        # Base directional score from premium imbalance.
        # +1 = heavily call-skewed flow, -1 = heavily put-skewed flow.
        imbalance = (ctx.smart_call - ctx.smart_put) / sm_total

        # Scale confidence up with notional flow size; avoid maxing out score on
        # marginal premium differences.
        flow_confidence = min(1.0, sm_total / 1_000_000)
        score = imbalance * flow_confidence

        # Divergence boost:
        # If price grinds one way while smart-money flow is positioned hard the
        # other way, amplify the contrarian warning. This helps detect "melt-up
        # into put buying" and "drip-down into call buying" setups.
        closes = ctx.recent_closes
        if len(closes) >= 5 and closes[-5] > 0:
            momentum_5bar = pct_change_n_bar(closes, 5)
            if momentum_5bar > 0.001 and imbalance < -0.30:
                # Up drift + heavy put flow -> bearish crack risk.
                score -= 0.25
            elif momentum_5bar < -0.001 and imbalance > 0.30:
                # Down drift + heavy call flow -> bullish squeeze risk.
                score += 0.25

        return max(-1.0, min(1.0, score))

    def context_values(self, ctx: MarketContext) -> dict:
        sm_total = ctx.smart_call + ctx.smart_put
        imbalance = ((ctx.smart_call - ctx.smart_put) / sm_total) if sm_total > 0 else 0.0
        momentum_5bar = None
        if len(ctx.recent_closes) >= 5 and ctx.recent_closes[-5] > 0:
            momentum_5bar = round(pct_change_n_bar(ctx.recent_closes, 5), 6)
        return {
            "smart_call": ctx.smart_call,
            "smart_put": ctx.smart_put,
            "sm_total": sm_total,
            "sm_ratio": round((ctx.smart_call + 1.0) / (ctx.smart_put + 1.0), 4),
            "imbalance": round(imbalance, 4),
            "momentum_5bar": momentum_5bar,
        }
