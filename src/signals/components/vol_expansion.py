"""Volatility expansion scoring component — primed-environment model."""
from src.signals.components.base import ComponentBase, MarketContext

# 0.5% price change over 5 bars fully shifts the score toward bearish.
_MOMENTUM_NORM = 0.005

# $300M negative GEX saturates vol_pressure to 1.0.
# Calibrated so that moderately negative readings register in the 60-80 range.
_GEX_NORM = 300_000_000


class VolExpansionComponent(ComponentBase):
    name = "vol_expansion"
    weight = 0.16

    def compute(self, ctx: MarketContext) -> float:
        """Score vol expansion readiness in [-1, +1] — primed-environment model.

        Positive GEX suppresses volatility (dealers dampen moves), so it scores 0.
        Negative GEX creates a dealer feedback loop that amplifies moves, so the
        environment is treated as *loaded* by default.

        Primed-environment logic:
          - Negative GEX alone fires a positive readiness score (gun is loaded).
          - Rising or flat price keeps the score at +vol_pressure (primed, no
            contradiction from price action).
          - Falling price shifts the score linearly toward -vol_pressure, reaching
            maximum bearish at a drop of _MOMENTUM_NORM (0.5%) over 5 bars.

        Semantics on the scaled [-100, +100] output:
          +100  Deep negative GEX, price flat or rising — maximum bullish expansion
                readiness; dealers will amplify any continued upward move.
          0     Negative GEX but price falling ~0.25% over 5 bars — readiness and
                bearish momentum exactly balanced.
          -100  Deep negative GEX, price falling hard — dealers are being forced to
                sell into the drop, amplifying bearish vol expansion.
          0     Zero or positive GEX — vol-suppression regime, no expansion signal.
        """
        if ctx.net_gex >= 0:
            return 0.0

        vol_pressure = min(1.0, abs(ctx.net_gex) / _GEX_NORM)

        closes = ctx.recent_closes
        if len(closes) < 5 or closes[-5] <= 0:
            # No momentum data — return pure readiness score.
            return vol_pressure

        pct_change = (closes[-1] - closes[-5]) / closes[-5]
        momentum = max(-1.0, min(1.0, pct_change / _MOMENTUM_NORM))

        if momentum >= 0:
            # Flat or rising price: environment is primed and uncontradicted.
            return vol_pressure

        # Falling price: shift linearly from +vol_pressure (momentum=0)
        # toward -vol_pressure (momentum=-1).
        return vol_pressure * (1.0 + 2.0 * momentum)

    def context_values(self, ctx: MarketContext) -> dict:
        vol_pressure = min(1.0, abs(ctx.net_gex) / _GEX_NORM)
        closes = ctx.recent_closes
        pct_change_5bar = None
        momentum = None
        if len(closes) >= 5 and closes[-5] > 0:
            pct_change_5bar = round((closes[-1] - closes[-5]) / closes[-5], 6)
            momentum = round(max(-1.0, min(1.0, pct_change_5bar / _MOMENTUM_NORM)), 4)
        return {
            "net_gex": ctx.net_gex,
            "gex_regime": "negative" if ctx.net_gex < 0 else "positive",
            "vol_pressure": round(vol_pressure, 4),
            "pct_change_5bar": pct_change_5bar,
            "momentum": momentum,
        }
