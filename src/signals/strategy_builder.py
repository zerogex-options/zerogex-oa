"""Regime-aware strategy selection for the position optimizer.

The StrategyBuilder decouples market-regime inference from the optimizer's
candidate generation. Instead of hardcoding ``trade_type="trend_follow"``,
callers provide the current score + market context and receive the most
appropriate strategy regime plus a whitelist of structures to generate.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from typing import Optional


@dataclass(frozen=True)
class StrategyDecision:
    trade_type: str
    optimizer_direction: str
    preferred_strategies: list[str]
    regime: str
    regime_score: float
    diagnostics: dict


class StrategyBuilder:
    """Map live context into an optimizer trade regime."""

    def __init__(self, underlying: str):
        self.underlying = underlying.upper()

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(low, min(value, high))

    @staticmethod
    def _vol_expansion_readiness(net_gex: float) -> float:
        # Mirrors VolExpansionComponent._gex_readiness in lightweight form.
        normalized = max(-1.0, min(1.0, -float(net_gex or 0.0) / 300_000_000.0))
        return 0.15 + (1.0 - 0.15) * ((normalized + 1.0) / 2.0)

    @staticmethod
    def _direction_signal(recent_closes: list[float]) -> float:
        if not recent_closes or len(recent_closes) < 6:
            return 0.0
        base = recent_closes[-6]
        if base <= 0:
            return 0.0
        move = (recent_closes[-1] / base) - 1.0
        returns = []
        for i in range(1, len(recent_closes)):
            p0 = recent_closes[i - 1]
            p1 = recent_closes[i]
            if p0 > 0 and p1 > 0:
                returns.append(math.log(p1 / p0))
        if len(returns) < 10:
            return 0.0
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / max(len(returns) - 1, 1)
        sigma = math.sqrt(max(var, 0.0))
        if sigma <= 0:
            return 0.0
        z = move / max(sigma * math.sqrt(5.0), 1e-6)
        return max(-1.0, min(1.0, z))

    @staticmethod
    def _term_structure_iv(option_rows: Optional[list[dict]]) -> tuple[Optional[float], Optional[float]]:
        if not option_rows:
            return None, None
        by_expiry: dict = {}
        for row in option_rows:
            expiry = row.get("expiration")
            iv = float(row.get("iv") or 0.0)
            if expiry is None or iv <= 0:
                continue
            by_expiry.setdefault(expiry, []).append(iv)
        if len(by_expiry) < 2:
            return None, None
        expiries = sorted(by_expiry.keys())
        near = sum(by_expiry[expiries[0]]) / len(by_expiry[expiries[0]])
        far = sum(by_expiry[expiries[-1]]) / len(by_expiry[expiries[-1]])
        return float(near), float(far)

    def decide(
        self,
        *,
        score_direction: str,
        score_normalized: float,
        market_ctx: dict,
        option_rows: Optional[list[dict]] = None,
    ) -> StrategyDecision:
        iv_rank = self._clamp(float(market_ctx.get("iv_rank") or 0.5), 0.0, 1.0)
        expansion = self._vol_expansion_readiness(float(market_ctx.get("net_gex") or 0.0))
        direction_signal = self._direction_signal(market_ctx.get("recent_closes") or [])
        momentum_mag = abs(direction_signal)
        momentum_dead = momentum_mag <= 0.25

        near_iv, far_iv = self._term_structure_iv(option_rows)
        term_contango = 0.0
        if near_iv is not None and far_iv is not None:
            term_contango = max(0.0, far_iv - near_iv)

        trend_score = self._clamp(
            max(score_normalized, momentum_mag) * (0.75 if (iv_rank > 0.7 and momentum_dead) else 1.0),
            0.0,
            1.0,
        )
        vol_neutral_score = (
            expansion * self._clamp(1.0 - (momentum_mag / 0.25), 0.0, 1.0)
            if expansion >= 0.55
            else 0.0
        )
        premium_sell_score = (
            iv_rank * self._clamp(1.0 - (momentum_mag / 0.25), 0.0, 1.0)
            if iv_rank >= 0.55
            else 0.0
        )
        calendar_score = self._clamp(term_contango / 0.08, 0.0, 1.0)

        regime_scores = {
            "trend_follow": trend_score,
            "long_volatility": vol_neutral_score,
            "premium_sell": premium_sell_score,
            "calendar": calendar_score,
        }
        regime = max(regime_scores, key=regime_scores.get)
        regime_score = float(regime_scores[regime])

        optimizer_direction = score_direction
        preferred: list[str]
        trade_type = regime

        if regime == "long_volatility" and regime_score >= 0.35:
            optimizer_direction = "neutral"
            preferred = ["long_straddle", "long_strangle", "iron_condor"]
        elif regime == "premium_sell" and regime_score >= 0.35:
            optimizer_direction = "neutral"
            preferred = ["short_strangle", "iron_butterfly", "iron_condor"]
        elif regime == "calendar" and regime_score >= 0.30:
            optimizer_direction = (
                score_direction if momentum_mag >= 0.35 and score_direction != "neutral" else "neutral"
            )
            preferred = ["calendar", "iron_condor"]
        else:
            trade_type = "trend_follow" if score_direction != "neutral" else "range"
            if score_direction == "bullish":
                preferred = ["bull_call_debit", "bull_put_credit"]
            elif score_direction == "bearish":
                preferred = ["bear_put_debit", "bear_call_credit"]
            else:
                preferred = ["iron_condor", "short_strangle", "iron_butterfly"]

        return StrategyDecision(
            trade_type=trade_type,
            optimizer_direction=optimizer_direction,
            preferred_strategies=preferred,
            regime=regime,
            regime_score=round(regime_score, 4),
            diagnostics={
                "timestamp": market_ctx.get("timestamp", datetime.utcnow()),
                "iv_rank": round(iv_rank, 4),
                "expansion": round(expansion, 4),
                "direction_signal": round(direction_signal, 4),
                "term_contango": round(term_contango, 4),
                "near_iv": round(near_iv, 4) if near_iv is not None else None,
                "far_iv": round(far_iv, 4) if far_iv is not None else None,
            },
        )
