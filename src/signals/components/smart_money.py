"""Smart money flow scoring component."""
from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import pct_change_n_bar


class SmartMoneyComponent(ComponentBase):
    name = "smart_money"
    weight = 0.09

    def compute(self, ctx: MarketContext) -> float:
        # UnifiedSignalEngine now feeds signed net premium (buy - sell) into
        # smart_call/smart_put. Use absolute notional as participation scale.
        call_net = float(ctx.smart_call or 0.0)
        put_net = float(ctx.smart_put or 0.0)
        participation = abs(call_net) + abs(put_net)
        if participation < 100_000:
            return 0.0  # Insufficient premium flow -- no edge

        # Base directional score from signed premium imbalance.
        # +1 = call-buying dominance, -1 = put-buying dominance.
        imbalance = (call_net - put_net) / max(participation, 1.0)

        # Scale confidence up with notional flow size; avoid maxing out score on
        # marginal premium differences.
        flow_confidence = min(1.0, participation / 1_000_000)
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
        call_net = float(ctx.smart_call or 0.0)
        put_net = float(ctx.smart_put or 0.0)
        participation = abs(call_net) + abs(put_net)
        imbalance = (call_net - put_net) / participation if participation > 0 else 0.0
        momentum_5bar = None
        if len(ctx.recent_closes) >= 5 and ctx.recent_closes[-5] > 0:
            momentum_5bar = round(pct_change_n_bar(ctx.recent_closes, 5), 6)
        return {
            "smart_call": call_net,
            "smart_put": put_net,
            "sm_total": participation,
            "sm_ratio": round((abs(call_net) + 1.0) / (abs(put_net) + 1.0), 4),
            "imbalance": round(imbalance, 4),
            "momentum_5bar": momentum_5bar,
        }
