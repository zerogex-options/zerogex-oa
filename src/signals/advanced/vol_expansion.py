"""Independent volatility expansion signal — continuous spectrum model."""
from __future__ import annotations

import math

from src.signals.components.base import MarketContext
from src.signals.components.utils import (
    pct_change_n_bar,
    realized_sigma,
    vol_normalized_momentum,
)
from src.signals.advanced.base import IndependentSignalResult

_DIRECTION_Z_NORM = 1.0
_GEX_NORM = 300_000_000
_GEX_FLOOR = 0.15


class VolExpansionSignal:
    name = "vol_expansion"

    @staticmethod
    def expansion(ctx: MarketContext) -> float:
        return round(VolExpansionSignal._gex_readiness(ctx.net_gex) * 100.0, 2)

    @staticmethod
    def direction_score(ctx: MarketContext) -> float:
        _, z = vol_normalized_momentum(ctx.recent_closes, n=5)
        momentum = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        return round(momentum * 100.0, 2)

    @staticmethod
    def magnitude(ctx: MarketContext) -> float:
        exp = VolExpansionSignal._gex_readiness(ctx.net_gex)
        _, z = vol_normalized_momentum(ctx.recent_closes, n=5)
        momentum = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        return round(exp * abs(momentum) * 100.0, 2)

    @staticmethod
    def expected_5min_move_bps(ctx: MarketContext) -> float | None:
        closes = ctx.recent_closes
        if not closes or len(closes) < 5:
            return None
        sigma = realized_sigma(closes, window=60)
        if sigma <= 0:
            return None
        exp = VolExpansionSignal._gex_readiness(ctx.net_gex)
        _, z = vol_normalized_momentum(closes, n=5)
        direction = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        projected_5b = sigma * math.sqrt(5)
        return round(direction * exp * projected_5b * 10000.0, 2)

    def compute(self, ctx: MarketContext) -> float:
        exp = self._gex_readiness(ctx.net_gex)
        closes = ctx.recent_closes
        if len(closes) < 5 or closes[-5] <= 0:
            return 0.0
        _, z = vol_normalized_momentum(closes, n=5)
        momentum = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        return exp * momentum

    @staticmethod
    def _gex_readiness(net_gex: float) -> float:
        normalized = max(-1.0, min(1.0, -net_gex / _GEX_NORM))
        return _GEX_FLOOR + (1.0 - _GEX_FLOOR) * (normalized + 1.0) / 2.0

    def context_values(self, ctx: MarketContext) -> dict:
        closes = ctx.recent_closes
        pct_change_5bar = None
        momentum_z = None
        momentum = None
        if len(closes) >= 5 and closes[-5] > 0:
            pct_change_5bar = round(pct_change_n_bar(closes, 5), 6)
            _, z = vol_normalized_momentum(closes, n=5)
            momentum_z = round(z, 4)
            momentum = round(max(-1.0, min(1.0, z / _DIRECTION_Z_NORM)), 4)
        sigma = realized_sigma(closes, window=60)
        return {
            "net_gex": ctx.net_gex,
            "gex_regime": "negative" if ctx.net_gex < 0 else "positive",
            "expansion": self.expansion(ctx),
            "direction": self.direction_score(ctx),
            "magnitude": self.magnitude(ctx),
            "expected_5min_move_bps": self.expected_5min_move_bps(ctx),
            "gex_readiness": round(self._gex_readiness(ctx.net_gex), 4),
            "pct_change_5bar": pct_change_5bar,
            "momentum_z": momentum_z,
            "momentum": momentum,
            "realized_sigma_bar": round(sigma, 6) if sigma > 0 else None,
        }

    def evaluate(self, ctx: MarketContext) -> IndependentSignalResult:
        score = max(-1.0, min(1.0, float(self.compute(ctx))))
        triggered = abs(score) >= 0.25
        return IndependentSignalResult(
            name=self.name,
            score=score,
            context={
                **self.context_values(ctx),
                "triggered": triggered,
                "signal": (
                    "bullish_expansion"
                    if score > 0.25
                    else ("bearish_expansion" if score < -0.25 else "none")
                ),
            },
        )


# Backward-compat name retained for tests/importers.
VolExpansionComponent = VolExpansionSignal
VolExpansionSignalComponent = VolExpansionSignal
