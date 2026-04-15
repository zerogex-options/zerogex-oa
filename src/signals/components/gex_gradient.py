"""Gamma gradient scoring component.

Decomposes per-strike gamma exposure around spot into four buckets:

  * **below_spot_gamma** — dealer gamma support beneath spot
  * **above_spot_gamma** — dealer gamma resistance above spot
  * **wing_gamma** — far-OTM concentration (pinning pressure)
  * **atm_gamma** — near-ATM concentration (short-dated convexity)

A heavy "above spot" skew means dealers are short gamma into a rally — any
up-move triggers call-unwinding (bullish). A heavy "below spot" skew is the
mirror — downside dealer shorts that accelerate a sell-off.

The score is the *asymmetry* of dealer gamma around spot, weighted by the
total notional gamma in the surrounding strike window. When gamma is flat
or the data isn't available we return 0 (the component abstains).

Input is drawn from ``ctx.extra['gex_by_strike']`` — a list of dicts
populated by ``UnifiedSignalEngine._fetch_market_context``. The component
degrades gracefully when the data isn't present.
"""
from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext

# Strikes within this percent of spot are treated as "near" (ATM window).
_ATM_WINDOW_PCT = float(os.getenv("SIGNAL_GEX_GRADIENT_ATM_PCT", "0.015"))

# Strikes beyond this percent of spot are treated as "wings".
_WING_WINDOW_PCT = float(os.getenv("SIGNAL_GEX_GRADIENT_WING_PCT", "0.04"))

# Minimum total notional gamma (absolute sum across surveyed strikes) to
# emit a non-zero score. Prevents over-reaction when OI is thin.
_MIN_TOTAL_GAMMA = float(os.getenv("SIGNAL_GEX_GRADIENT_MIN_GAMMA", "5.0e7"))


class GexGradientComponent(ComponentBase):
    name = "gex_gradient"
    weight = 0.08

    def compute(self, ctx: MarketContext) -> float:
        buckets = self._buckets(ctx)
        if buckets is None:
            return 0.0

        above = buckets["above_spot_gamma"]
        below = buckets["below_spot_gamma"]
        total = abs(above) + abs(below)
        if total < _MIN_TOTAL_GAMMA:
            return 0.0

        # Positive value = more gamma above spot than below.
        # In *negative* net-GEX regimes, dealers are short that gamma, so
        # a rally forces them to buy into strength -> bullish.
        # In *positive* net-GEX regimes, dealers are long that gamma, so
        # a rally forces them to sell into strength -> bearish resistance.
        asymmetry = (above - below) / total  # in [-1, +1]

        dealer_sign = -1.0 if ctx.net_gex < 0 else 1.0
        # When dealers are long gamma (positive net_gex), above-spot gamma
        # is *resistance*, so flip the sign of the asymmetry contribution.
        score = asymmetry * (-dealer_sign)

        # Scale confidence by how much of the surveyed gamma is sitting at
        # the wings. Heavy wing concentration = structural pinning, which
        # dampens directional edge.
        wing_fraction = buckets["wing_fraction"]
        confidence = max(0.25, 1.0 - wing_fraction)  # never fully dampen

        return max(-1.0, min(1.0, score * confidence))

    def context_values(self, ctx: MarketContext) -> dict:
        buckets = self._buckets(ctx)
        if buckets is None:
            return {
                "source": "unavailable",
                "above_spot_gamma": None,
                "below_spot_gamma": None,
                "atm_gamma": None,
                "wing_gamma": None,
                "asymmetry": None,
            }
        above = buckets["above_spot_gamma"]
        below = buckets["below_spot_gamma"]
        total = abs(above) + abs(below)
        asymmetry = (above - below) / total if total > 0 else 0.0
        return {
            "source": "gex_by_strike",
            "above_spot_gamma": round(above, 2),
            "below_spot_gamma": round(below, 2),
            "atm_gamma": round(buckets["atm_gamma"], 2),
            "wing_gamma": round(buckets["wing_gamma"], 2),
            "wing_fraction": round(buckets["wing_fraction"], 4),
            "asymmetry": round(asymmetry, 4),
            "strike_count": buckets["strike_count"],
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _buckets(ctx: MarketContext) -> dict | None:
        rows = ctx.extra.get("gex_by_strike") if ctx.extra else None
        if not rows or ctx.close <= 0:
            return None

        above = 0.0
        below = 0.0
        atm = 0.0
        wing = 0.0
        strike_count = 0

        for row in rows:
            strike = row.get("strike") if isinstance(row, dict) else None
            strike_gex = row.get("net_gex") if isinstance(row, dict) else None
            if strike is None or strike_gex is None:
                continue
            try:
                strike_f = float(strike)
                gex_f = float(strike_gex)
            except (TypeError, ValueError):
                continue
            strike_count += 1
            distance = (strike_f - ctx.close) / ctx.close
            abs_distance = abs(distance)
            if distance > 0:
                above += gex_f
            elif distance < 0:
                below += gex_f
            if abs_distance <= _ATM_WINDOW_PCT:
                atm += gex_f
            elif abs_distance >= _WING_WINDOW_PCT:
                wing += gex_f

        total_abs = abs(above) + abs(below)
        wing_fraction = abs(wing) / total_abs if total_abs > 0 else 0.0
        return {
            "above_spot_gamma": above,
            "below_spot_gamma": below,
            "atm_gamma": atm,
            "wing_gamma": wing,
            "wing_fraction": min(1.0, wing_fraction),
            "strike_count": strike_count,
        }
