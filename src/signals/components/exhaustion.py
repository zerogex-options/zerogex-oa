"""Exhaustion scoring component with direction fix.

Uses price momentum direction instead of smart-money direction to determine
the sign of the exhaustion signal.
"""
from typing import Optional

from src.signals.components.base import ComponentBase, MarketContext


class ExhaustionComponent(ComponentBase):
    name = "exhaustion"
    weight = 0.09

    @staticmethod
    def _compute_rsi(closes: list[float], period: int = 14) -> Optional[float]:
        if len(closes) < period + 1:
            return None
        gains = losses = 0.0
        for i in range(-period, 0):
            delta = closes[i] - closes[i - 1]
            if delta >= 0:
                gains += delta
            else:
                losses += abs(delta)
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _compute_magnitude(self, closes: list[float]) -> tuple[float, str]:
        """Compute exhaustion magnitude and state label.

        Ported from UnifiedSignalEngine._compute_exhaustion().
        """
        if len(closes) < 8:
            return 0.0, "insufficient_data"
        short = sum(closes[-5:]) / 5
        long_ = sum(closes[-8:]) / 8
        drift = (short - long_) / long_ if long_ else 0.0
        drift_score = min(1.0, abs(drift) * 20)

        # RSI extreme: overbought (>72) or oversold (<28) signals potential exhaustion
        rsi_score = 0.0
        if len(closes) >= 15:
            rsi = self._compute_rsi(closes, 14)
            if rsi is not None:
                if rsi > 72:
                    rsi_score = min(1.0, (rsi - 72) / 18.0)
                elif rsi < 28:
                    rsi_score = min(1.0, (28 - rsi) / 18.0)

        # Price extension beyond 8-bar mean: >1.5% extension signals overextension
        extension_score = 0.0
        if long_ > 0:
            extension = abs(closes[-1] - long_) / long_
            extension_score = min(1.0, extension / 0.015)

        score = 0.50 * drift_score + 0.30 * rsi_score + 0.20 * extension_score
        label = "exhausting" if score > 0.6 else "controlled"
        return score, label

    def compute(self, ctx: MarketContext) -> float:
        closes = ctx.recent_closes
        exhaustion_magnitude, _ = self._compute_magnitude(closes)

        if len(closes) >= 5:
            recent_momentum_dir = 1.0 if closes[-1] > closes[-5] else -1.0
        elif len(closes) >= 2:
            recent_momentum_dir = 1.0 if closes[-1] > closes[0] else -1.0
        else:
            return 0.0

        return -recent_momentum_dir * exhaustion_magnitude

    def context_values(self, ctx: MarketContext) -> dict:
        closes = ctx.recent_closes
        exhaustion_magnitude, exhaustion_state = self._compute_magnitude(closes)

        recent_momentum_dir = 0.0
        if len(closes) >= 5:
            recent_momentum_dir = 1.0 if closes[-1] > closes[-5] else -1.0
        elif len(closes) >= 2:
            recent_momentum_dir = 1.0 if closes[-1] > closes[0] else -1.0

        rsi = self._compute_rsi(closes, 14) if len(closes) >= 15 else None

        return {
            "exhaustion_magnitude": exhaustion_magnitude,
            "exhaustion_state": exhaustion_state,
            "recent_momentum_dir": recent_momentum_dir,
            "rsi": rsi,
        }
