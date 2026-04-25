"""Dealer delta pressure (DNI) scoring component.

Gamma tells you *where* dealers will be forced to hedge. Delta tells you
*how much they already are* right now. Delta flow leads gamma exposure by
several minutes intraday — it's the single closest thing to a leading
indicator in 0DTE.

Input is ``ctx.extra['gex_by_strike']``, a list of dicts containing
at minimum per-strike ``call_oi``, ``put_oi``, and (optionally)
``call_delta_oi``/``put_delta_oi`` rolled up by the analytics layer.
When delta-weighted OI isn't available we fall back to call_oi/put_oi
approximation using linear distance from spot as a delta proxy.

Score convention:
  * negative score => dealers are net short delta (they *must* buy into
    a rally), which amplifies upside -> **bullish** for price.
  * positive score => dealers are net long delta (they must sell into a
    rally), which suppresses upside -> **bearish** for price.

We invert so that the composite-score contribution is aligned with
"bullish for SPY" semantics downstream.
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext

# Normalization constant for dealer net delta (shares-equivalent).
# A dealer net delta magnitude of ~|$300M| saturates the score.
_DNI_NORM = float(os.getenv("SIGNAL_DNI_NORM", "3.0e8"))


class DealerDeltaPressureComponent(ComponentBase):
    name = "dealer_delta_pressure"
    weight = 0.08

    def compute(self, ctx: MarketContext) -> float:
        dni = self._estimate_dni(ctx)
        if dni is None:
            return 0.0
        normalized = max(-1.0, min(1.0, dni / _DNI_NORM))
        # Dealers short delta (negative DNI) must buy into strength -> bullish.
        return -normalized

    def context_values(self, ctx: MarketContext) -> dict:
        dni = self._estimate_dni(ctx)
        return {
            "dealer_net_delta_estimated": round(dni, 2) if dni is not None else None,
            "dni_normalized": (
                round(max(-1.0, min(1.0, dni / _DNI_NORM)), 4) if dni is not None else None
            ),
            "source": self._source_used(ctx),
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _source_used(ctx: MarketContext) -> str:
        if ctx.dealer_net_delta:
            return "dealer_net_delta_field"
        rows = ctx.extra.get("gex_by_strike") if ctx.extra else None
        if not rows:
            return "unavailable"
        sample = rows[0] if rows else {}
        if isinstance(sample, dict) and ("call_delta_oi" in sample or "put_delta_oi" in sample):
            return "gex_by_strike.delta_oi"
        return "gex_by_strike.distance_proxy"

    @staticmethod
    def _estimate_dni(ctx: MarketContext) -> float | None:
        # 1. Explicit dealer_net_delta field wins if populated.
        if ctx.dealer_net_delta:
            return float(ctx.dealer_net_delta)

        rows = ctx.extra.get("gex_by_strike") if ctx.extra else None
        if not rows or ctx.close <= 0:
            return None

        # 2. Use delta-weighted OI columns if the analytics layer provided them.
        have_delta_oi = any(
            isinstance(r, dict) and ("call_delta_oi" in r or "put_delta_oi" in r) for r in rows
        )
        if have_delta_oi:
            total = 0.0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    call_d = float(row.get("call_delta_oi") or 0.0)
                    put_d = float(row.get("put_delta_oi") or 0.0)
                except (TypeError, ValueError):
                    continue
                # Dealers are on the opposite side of customer OI; customers
                # are typically long calls and long puts. Dealer delta is
                # therefore approximately -(call_delta_oi + put_delta_oi).
                total -= call_d + put_d
            return total

        # 3. Fallback: use call_oi/put_oi with a linear-distance delta proxy.
        total = 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            strike = row.get("strike")
            call_oi = row.get("call_oi") or 0
            put_oi = row.get("put_oi") or 0
            if strike is None:
                continue
            try:
                strike_f = float(strike)
                call_oi_f = float(call_oi)
                put_oi_f = float(put_oi)
            except (TypeError, ValueError):
                continue
            # Linear delta proxy: 0.5 at ATM decaying to 0 at ±5% OTM.
            distance_pct = (ctx.close - strike_f) / ctx.close
            call_delta = max(0.0, min(1.0, 0.5 - distance_pct * 10))
            put_delta = -max(0.0, min(1.0, 0.5 + distance_pct * 10))
            # 100 shares per contract, dealer sign flipped. Result is in
            # shares-equivalent so it is comparable against _DNI_NORM and the
            # explicit delta_oi branch above.
            total -= (call_oi_f * call_delta + put_oi_f * put_delta) * 100
        return total
