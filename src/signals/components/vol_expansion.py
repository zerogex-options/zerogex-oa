"""Volatility expansion scoring component — continuous spectrum model.

Exposes two trader-facing dimensions:
  * **expansion** (0–100): How likely is volatility to expand?
    Driven by the GEX regime.  0 = deeply positive GEX, maximum dealer
    suppression.  100 = deeply negative GEX, dealers amplifying moves.
  * **direction** (-100–+100): If vol expands, which way?
    Driven by recent price momentum.  +100 = strong bullish momentum,
    -100 = strong bearish momentum, 0 = flat/no bias.

The composite-score contribution (used by ScoringEngine) combines both
into a single [-1, +1] value: ``expansion * direction / 10000``.
"""
from src.signals.components.base import ComponentBase, MarketContext

# 0.5% price change over 5 bars fully shifts the direction score.
_MOMENTUM_NORM = 0.005

# $300M GEX magnitude saturates the expansion scale.
_GEX_NORM = 300_000_000

# Minimum expansion score even with deeply positive GEX.  Volatility can
# always expand — positive GEX suppresses but never eliminates it.
_GEX_FLOOR = 0.15


class VolExpansionComponent(ComponentBase):
    name = "vol_expansion"
    weight = 0.12

    # ------------------------------------------------------------------
    # Public two-dimensional scores (0-100 and -100 to +100)
    # ------------------------------------------------------------------

    @staticmethod
    def expansion(ctx: MarketContext) -> float:
        """How likely is vol to expand?  0 → suppressed, 100 → primed.

        Driven entirely by the GEX regime:
          - Deeply positive GEX → approaches _GEX_FLOOR * 100  (≈15)
          - Zero GEX → moderate (~57.5)
          - Deeply negative GEX → approaches 100
        """
        return round(VolExpansionComponent._gex_readiness(ctx.net_gex) * 100.0, 2)

    @staticmethod
    def direction_score(ctx: MarketContext) -> float:
        """If vol expands, which way?  -100 bearish … 0 neutral … +100 bullish.

        Driven by 5-bar price momentum.  Returns 0 when insufficient data.
        """
        closes = ctx.recent_closes
        if len(closes) < 5 or closes[-5] <= 0:
            return 0.0
        pct_change = (closes[-1] - closes[-5]) / closes[-5]
        momentum = max(-1.0, min(1.0, pct_change / _MOMENTUM_NORM))
        return round(momentum * 100.0, 2)

    # ------------------------------------------------------------------
    # Composite contribution  (ScoringEngine interface)
    # ------------------------------------------------------------------

    def compute(self, ctx: MarketContext) -> float:
        """Combined score in [-1, +1] for the weighted composite.

        ``expansion/100 * direction/100`` = readiness * signed_momentum.
        This keeps the component neutral when momentum is flat while still
        letting GEX control the *magnitude* of any directional impulse.
        """
        exp = self._gex_readiness(ctx.net_gex)  # [_GEX_FLOOR, 1.0]

        closes = ctx.recent_closes
        if len(closes) < 5 or closes[-5] <= 0:
            return 0.0  # no momentum data -> directional score unavailable

        pct_change = (closes[-1] - closes[-5]) / closes[-5]
        momentum = max(-1.0, min(1.0, pct_change / _MOMENTUM_NORM))
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
        exp = self.expansion(ctx)
        dirn = self.direction_score(ctx)
        closes = ctx.recent_closes
        pct_change_5bar = None
        momentum = None
        if len(closes) >= 5 and closes[-5] > 0:
            pct_change_5bar = round((closes[-1] - closes[-5]) / closes[-5], 6)
            momentum = round(max(-1.0, min(1.0, pct_change_5bar / _MOMENTUM_NORM)), 4)
        return {
            "net_gex": ctx.net_gex,
            "gex_regime": "negative" if ctx.net_gex < 0 else "positive",
            "expansion": exp,
            "direction": dirn,
            "gex_readiness": round(self._gex_readiness(ctx.net_gex), 4),
            "pct_change_5bar": pct_change_5bar,
            "momentum": momentum,
        }
