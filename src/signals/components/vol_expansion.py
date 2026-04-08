"""Volatility expansion scoring component — continuous spectrum model."""
from src.signals.components.base import ComponentBase, MarketContext

# 0.5% price change over 5 bars fully shifts the score toward bearish.
_MOMENTUM_NORM = 0.005

# $300M GEX magnitude saturates the readiness scale.
_GEX_NORM = 300_000_000

# Minimum readiness even with deeply positive GEX.  Volatility can always
# expand — positive GEX suppresses but never eliminates the possibility.
_GEX_FLOOR = 0.15


class VolExpansionComponent(ComponentBase):
    name = "vol_expansion"
    weight = 0.16

    def compute(self, ctx: MarketContext) -> float:
        """Score vol expansion readiness in [-1, +1] — continuous spectrum.

        GEX modulates readiness on a continuous scale rather than acting as a
        binary gate:
          - Deeply negative GEX → readiness approaches 1.0 (dealers amplify)
          - Zero GEX → moderate readiness (~0.575)
          - Deeply positive GEX → readiness approaches _GEX_FLOOR (dealers
            suppress, but some expansion potential always remains)

        Momentum (5-bar price change) determines direction:
          - Rising or flat price: positive score (bullish expansion)
          - Falling price: shifts toward negative (bearish expansion)

        Semantics on the scaled [-100, +100] output:
          +100  Deep negative GEX, price flat or rising — maximum expansion
                readiness; dealers amplify any continued upward move.
           +15  Deep positive GEX, price flat or rising — dealers suppress but
                vol expansion is not impossible.
           -15  Deep positive GEX, price falling hard — small bearish signal
                despite dealer suppression.
          -100  Deep negative GEX, price falling hard — dealers forced to sell
                into the drop, amplifying bearish vol expansion.
        """
        gex_readiness = self._gex_readiness(ctx.net_gex)

        closes = ctx.recent_closes
        if len(closes) < 5 or closes[-5] <= 0:
            return gex_readiness

        pct_change = (closes[-1] - closes[-5]) / closes[-5]
        momentum = max(-1.0, min(1.0, pct_change / _MOMENTUM_NORM))

        if momentum >= 0:
            return gex_readiness

        # Falling price: shift linearly from +gex_readiness (momentum=0)
        # toward -gex_readiness (momentum=-1).
        return gex_readiness * (1.0 + 2.0 * momentum)

    @staticmethod
    def _gex_readiness(net_gex: float) -> float:
        """Map net_gex to a continuous readiness factor in [_GEX_FLOOR, 1.0].

        Negative GEX → high readiness (approaching 1.0).
        Positive GEX → low readiness (approaching _GEX_FLOOR).
        """
        # Flip sign so negative GEX maps to +1, positive to -1, then clamp.
        normalized = max(-1.0, min(1.0, -net_gex / _GEX_NORM))
        # Linear map from [-1, +1] → [_GEX_FLOOR, 1.0]
        return _GEX_FLOOR + (1.0 - _GEX_FLOOR) * (normalized + 1.0) / 2.0

    def context_values(self, ctx: MarketContext) -> dict:
        gex_readiness = self._gex_readiness(ctx.net_gex)
        closes = ctx.recent_closes
        pct_change_5bar = None
        momentum = None
        if len(closes) >= 5 and closes[-5] > 0:
            pct_change_5bar = round((closes[-1] - closes[-5]) / closes[-5], 6)
            momentum = round(max(-1.0, min(1.0, pct_change_5bar / _MOMENTUM_NORM)), 4)
        return {
            "net_gex": ctx.net_gex,
            "gex_regime": "negative" if ctx.net_gex < 0 else "positive",
            "gex_readiness": round(gex_readiness, 4),
            "pct_change_5bar": pct_change_5bar,
            "momentum": momentum,
        }
