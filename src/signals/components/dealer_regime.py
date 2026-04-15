"""Dealer Regime Score (DRS) component.

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
            "net_gex": self._net_gex_score(ctx.net_gex),
            "vs_gamma_flip": self._gamma_flip_score(ctx.close, ctx.gamma_flip),
            "call_wall_pressure": self._call_wall_pressure_score(
                ctx.close, ctx.extra.get("call_wall")
            ),
            "distance_to_max_gamma": self._distance_to_max_gamma_score(
                ctx.close, ctx.extra.get("max_gamma_strike")
            ),
            "momentum_confirmation": self._momentum_confirmation_score(ctx),
        }

    @staticmethod
    def _net_gex_score(net_gex: float) -> float:
        if net_gex > 0:
            return 30.0
        if net_gex < 0:
            return -30.0
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
    def _momentum_confirmation_score(ctx: MarketContext) -> float:
        if ctx.vwap is not None and ctx.vwap > 0:
            return 10.0 if ctx.close > ctx.vwap else -10.0
        closes = ctx.recent_closes
        if len(closes) >= 5 and closes[-5] > 0:
            return 10.0 if closes[-1] > closes[-5] else -10.0
        return 0.0
