"""Vanna/Charm flow scoring component.

Second-order greeks drive intraday dealer re-hedging in ways that gamma
alone cannot explain:

  * **Vanna** (dVega/dSpot) captures how dealer deltas change when
    volatility moves. As IV crushes through the session (typical morning
    into midday behavior), vanna-short dealers must buy underlying —
    this is the "vol-crush rally" that kills naked put sellers.
  * **Charm** (dDelta/dTime) captures the decay of short-dated deltas
    toward expiry. In the last 2 hours of an expiry session, charm flow
    accelerates dramatically — dealers short calls above spot are
    forced to sell into weakness, amplifying afternoon drift.

The analytics layer populates ``vanna_exposure`` and ``charm_exposure``
per-strike in ``gex_by_strike``. This component aggregates them into a
single directional score.

Sign convention:
  * Positive aggregate vanna+charm => bullish tailwind (dealer buying pressure)
  * Negative aggregate vanna+charm => bearish headwind (dealer selling pressure)
"""
from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext

# Normalize combined vanna+charm exposure so that a magnitude of this
# value saturates the score.
_VC_NORM = float(os.getenv("SIGNAL_VANNA_CHARM_NORM", "5.0e7"))

# Charm flow grows non-linearly into the close. Minutes-since-open scale.
_SESSION_MINUTES = 390  # US cash session minutes
# Afternoon charm amplification kicks in after this fraction of session.
_CHARM_AMP_START = 0.6  # ~2h before close
_CHARM_AMP_MAX = 1.5


class VannaCharmFlowComponent(ComponentBase):
    name = "vanna_charm_flow"
    weight = 0.07

    def compute(self, ctx: MarketContext) -> float:
        agg = self._aggregate(ctx)
        if agg is None:
            return 0.0
        vanna = agg["vanna"]
        charm = agg["charm"]

        charm_weight = self._charm_amplification(ctx)
        combined = vanna + charm * charm_weight
        normalized = max(-1.0, min(1.0, combined / _VC_NORM))
        # Dealer re-hedging sign: positive aggregate => dealers sell into
        # weakness / buy into strength (negative gamma is already encoded
        # in vanna_exposure & charm_exposure upstream).
        return normalized

    def context_values(self, ctx: MarketContext) -> dict:
        agg = self._aggregate(ctx)
        if agg is None:
            return {
                "vanna_total": None,
                "charm_total": None,
                "charm_amplification": round(self._charm_amplification(ctx), 3),
                "source": "unavailable",
            }
        return {
            "vanna_total": round(agg["vanna"], 2),
            "charm_total": round(agg["charm"], 2),
            "charm_amplification": round(self._charm_amplification(ctx), 3),
            "source": "gex_by_strike",
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _aggregate(ctx: MarketContext) -> dict | None:
        rows = ctx.extra.get("gex_by_strike") if ctx.extra else None
        if not rows:
            return None
        vanna_total = 0.0
        charm_total = 0.0
        saw_any = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                v = row.get("vanna_exposure")
                c = row.get("charm_exposure")
                if v is not None:
                    vanna_total += float(v)
                    saw_any = True
                if c is not None:
                    charm_total += float(c)
                    saw_any = True
            except (TypeError, ValueError):
                continue
        if not saw_any:
            return None
        return {"vanna": vanna_total, "charm": charm_total}

    @staticmethod
    def _charm_amplification(ctx: MarketContext) -> float:
        """Scale charm's contribution upward as we approach the close.

        Returns 1.0 for most of the session; ramps to _CHARM_AMP_MAX in
        the final ~2h when charm flow dominates.
        """
        if ctx.timestamp is None:
            return 1.0
        # UTC-based cash session: 13:30 open, 20:00 close.
        minute = ctx.timestamp.hour * 60 + ctx.timestamp.minute
        open_min = 13 * 60 + 30
        close_min = 20 * 60
        if minute <= open_min:
            return 1.0
        if minute >= close_min:
            return _CHARM_AMP_MAX
        frac = (minute - open_min) / (close_min - open_min)
        if frac < _CHARM_AMP_START:
            return 1.0
        ramp = (frac - _CHARM_AMP_START) / (1.0 - _CHARM_AMP_START)
        return 1.0 + (_CHARM_AMP_MAX - 1.0) * ramp
