"""Dealer Regime Score (DRS) component.

DRS should not infer direction from GEX sign itself.  Direction comes from
flow + positioning + structure (vs gamma flip / walls); GEX sign controls
stability:
  * net_gex > 0 => stabilizing / mean-reverting
  * net_gex < 0 => destabilizing / trend-amplifying

Raw score range: [-100, +100]
Normalized contribution to composite: raw / 100 -> [-1, +1]
"""
from __future__ import annotations

from src.signals.components.base import ComponentBase, MarketContext


class DealerRegimeComponent(ComponentBase):
    name = "dealer_regime"
    weight = 0.08

    def compute(self, ctx: MarketContext) -> float:
        breakdown = self._breakdown(ctx)
        raw_score = sum(breakdown.values())
        return max(-1.0, min(1.0, raw_score / 100.0))

    def context_values(self, ctx: MarketContext) -> dict:
        breakdown = self._breakdown(ctx)
        raw_score = sum(breakdown.values())
        return {
            "drs_raw": round(raw_score, 3),
            "drs_normalized": round(max(-1.0, min(1.0, raw_score / 100.0)), 4),
            **{k: round(v, 3) for k, v in breakdown.items()},
            "call_wall": ctx.extra.get("call_wall"),
            "max_gamma_strike": ctx.extra.get("max_gamma_strike"),
            "vwap": ctx.vwap,
        }

    def _breakdown(self, ctx: MarketContext) -> dict[str, float]:
        return {
            "stability_regime": self._stability_regime_score(ctx.net_gex),
            "vs_gamma_flip": self._gamma_flip_score(ctx.close, ctx.gamma_flip),
            "call_wall_pressure": self._call_wall_pressure_score(
                ctx.close, ctx.extra.get("call_wall")
            ),
            "distance_to_max_gamma": self._distance_to_max_gamma_score(
                ctx.close, ctx.extra.get("max_gamma_strike")
            ),
            "flow_positioning": self._flow_positioning_score(ctx),
        }

    @staticmethod
    def _stability_regime_score(net_gex: float) -> float:
        # Non-directional contribution: positive = stabilizing, negative = destabilizing.
        if net_gex > 0:
            return 15.0
        if net_gex < 0:
            return -15.0
        return 0.0

    @staticmethod
    def _gamma_flip_score(close: float, gamma_flip: float | None) -> float:
        if gamma_flip is None or gamma_flip <= 0:
            return 0.0
        return 25.0 if close > gamma_flip else -25.0

    @staticmethod
    def _call_wall_pressure_score(close: float, call_wall: float | None) -> float:
        if call_wall is None:
            return 0.0
        distance = call_wall - close
        if distance < 0:
            return 0.0
        if distance <= 5:
            return 20.0
        if distance <= 10:
            return 10.0
        return 0.0

    @staticmethod
    def _distance_to_max_gamma_score(close: float, max_gamma_strike: float | None) -> float:
        if max_gamma_strike is None:
            return 0.0
        return 15.0 if close > max_gamma_strike else -15.0

    @staticmethod
    def _flow_positioning_score(ctx: MarketContext) -> float:
        call_net = float(ctx.smart_call or 0.0)
        put_net = float(ctx.smart_put or 0.0)
        participation = abs(call_net) + abs(put_net)
        flow_bias = 0.0
        if participation >= 100_000:
            flow_bias = max(-1.0, min(1.0, (call_net - put_net) / participation))

        pcr = float(ctx.put_call_ratio or 1.0)
        # pcr < 1 bullish positioning, >1 bearish positioning.
        positioning_bias = max(-1.0, min(1.0, (1.0 - pcr) / 0.35))

        vwap_bias = 0.0
        if ctx.vwap is not None and ctx.vwap > 0:
            vwap_dist = (ctx.close - ctx.vwap) / ctx.vwap
            vwap_bias = max(-1.0, min(1.0, vwap_dist / 0.004))
        elif len(ctx.recent_closes) >= 5 and ctx.recent_closes[-5] > 0:
            mom = (ctx.recent_closes[-1] - ctx.recent_closes[-5]) / ctx.recent_closes[-5]
            vwap_bias = max(-1.0, min(1.0, mom / 0.004))

        composite = (0.55 * flow_bias) + (0.25 * positioning_bias) + (0.20 * vwap_bias)
        return max(-25.0, min(25.0, composite * 25.0))
