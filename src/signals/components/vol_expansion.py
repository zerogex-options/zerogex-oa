"""Volatility expansion scoring component — continuous spectrum model.

Exposes three trader-facing dimensions:
  * **expansion** (0–100): How likely is volatility to expand?
    Driven by the GEX regime.  0 = deeply positive GEX, maximum dealer
    suppression.  100 = deeply negative GEX, dealers amplifying moves.
  * **direction** (-100–+100): If vol expands, which way?
    Driven by vol-normalized momentum (z-score vs realized sigma) so the
    same raw 5-bar return does not fire identically in a dead-vol regime
    as in a panicky one.
  * **magnitude** (0–100): How *big* is the impulse likely to be?
    Amplitude component for UI sizing; distinct from directionality.
  * **expected_5min_move_bps** (float): rough model-implied 5-bar move
    in basis points = sign(direction) * expansion% * projected_sigma_5b
    * 10000.  Scales with realized vol so the same score means a larger
    move in higher-vol regimes.

The composite-score contribution (used by ScoringEngine) combines both
into a single [-1, +1] value: ``expansion/100 * direction/100``.
"""
from __future__ import annotations

import math

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import (
    pct_change_n_bar,
    realized_sigma,
    vol_normalized_momentum,
)

# Target z-score for "full" direction (1 sigma over 5 bars).
_DIRECTION_Z_NORM = 1.0

# $300M GEX magnitude saturates the expansion scale.
_GEX_NORM = 300_000_000

# Minimum expansion score even with deeply positive GEX.  Volatility can
# always expand — positive GEX suppresses but never eliminates it.
_GEX_FLOOR = 0.15


class VolExpansionComponent(ComponentBase):
    name = "vol_expansion"
    weight = 0.08

    # ------------------------------------------------------------------
    # Public scores
    # ------------------------------------------------------------------

    @staticmethod
    def expansion(ctx: MarketContext) -> float:
        """How likely is vol to expand?  0 → suppressed, 100 → primed."""
        return round(VolExpansionComponent._gex_readiness(ctx.net_gex) * 100.0, 2)

    @staticmethod
    def direction_score(ctx: MarketContext) -> float:
        """If vol expands, which way?  -100 bearish … 0 neutral … +100 bullish.

        Driven by vol-normalized momentum (z-score over realized sigma).
        """
        _, z = vol_normalized_momentum(ctx.recent_closes, n=5)
        momentum = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        return round(momentum * 100.0, 2)

    @staticmethod
    def magnitude(ctx: MarketContext) -> float:
        """Unsigned impulse amplitude (0-100).

        magnitude = expansion% * |momentum|.  A highly negative-GEX regime
        with strong directional momentum prints near 100; a suppressed
        regime or flat tape prints near 0.
        """
        exp = VolExpansionComponent._gex_readiness(ctx.net_gex)
        _, z = vol_normalized_momentum(ctx.recent_closes, n=5)
        momentum = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        return round(exp * abs(momentum) * 100.0, 2)

    @staticmethod
    def expected_5min_move_bps(ctx: MarketContext) -> float | None:
        """Rough expected 5-bar move in basis points.

        sign(direction) * expansion * sigma_5b * 10000.  Returns None if
        there's insufficient history to estimate realized vol.
        """
        closes = ctx.recent_closes
        if not closes or len(closes) < 5:
            return None
        sigma = realized_sigma(closes, window=60)
        if sigma <= 0:
            return None
        exp = VolExpansionComponent._gex_readiness(ctx.net_gex)
        _, z = vol_normalized_momentum(closes, n=5)
        direction = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        projected_5b = sigma * math.sqrt(5)
        return round(direction * exp * projected_5b * 10000.0, 2)

    # ------------------------------------------------------------------
    # Composite contribution  (ScoringEngine interface)
    # ------------------------------------------------------------------

    def compute(self, ctx: MarketContext) -> float:
        """Combined score in [-1, +1] for the weighted composite."""
        exp = self._gex_readiness(ctx.net_gex)  # [_GEX_FLOOR, 1.0]

        closes = ctx.recent_closes
        if len(closes) < 5 or closes[-5] <= 0:
            return 0.0

        _, z = vol_normalized_momentum(closes, n=5)
        momentum = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        return exp * momentum

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _gex_readiness(net_gex: float) -> float:
        """Map net_gex to a continuous readiness factor in [_GEX_FLOOR, 1.0]."""
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
