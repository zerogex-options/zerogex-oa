"""Skew delta scoring component.

Short-dated option skew is one of the purest real-time fear gauges: when
puts bid up relative to calls of the same moneyness, it means market
participants are paying up for downside protection — and doing it
*before* the tape confirms bearishness.

This component computes an OTM put vs OTM call implied-volatility
differential, normalizes it against a configurable baseline, and scores
so that elevated put skew is bearish.

Inputs come from ``ctx.extra['skew']`` — a dict with
``otm_put_iv`` and ``otm_call_iv`` populated by the unified signal
engine from the ``option_chains`` table. When the data isn't available
the component returns 0 (abstain).
"""
from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext

# Baseline IV spread (put_iv - call_iv) that counts as "neutral" skew.
# Equity index skew is structurally positive — OTM puts always trade
# richer than OTM calls. This baseline lets us measure *deviation from
# normal* rather than the raw spread.
_SKEW_BASELINE = float(os.getenv("SIGNAL_SKEW_BASELINE", "0.02"))

# Spread magnitude beyond (baseline + this delta) saturates the score.
_SKEW_SATURATION = float(os.getenv("SIGNAL_SKEW_SATURATION", "0.04"))


class SkewDeltaComponent(ComponentBase):
    name = "skew_delta"
    weight = 0.04

    def compute(self, ctx: MarketContext) -> float:
        spread = self._spread(ctx)
        if spread is None:
            return 0.0
        deviation = spread - _SKEW_BASELINE
        # Elevated put skew (positive deviation) = bearish.
        normalized = max(-1.0, min(1.0, deviation / _SKEW_SATURATION))
        return -normalized

    def context_values(self, ctx: MarketContext) -> dict:
        spread = self._spread(ctx)
        skew_info = (ctx.extra or {}).get("skew") or {}
        return {
            "otm_put_iv": skew_info.get("otm_put_iv"),
            "otm_call_iv": skew_info.get("otm_call_iv"),
            "spread": round(spread, 6) if spread is not None else None,
            "baseline": _SKEW_BASELINE,
            "deviation": round(spread - _SKEW_BASELINE, 6) if spread is not None else None,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _spread(ctx: MarketContext) -> float | None:
        skew_info = (ctx.extra or {}).get("skew")
        if not isinstance(skew_info, dict):
            return None
        put_iv = skew_info.get("otm_put_iv")
        call_iv = skew_info.get("otm_call_iv")
        if put_iv is None or call_iv is None:
            return None
        try:
            return float(put_iv) - float(call_iv)
        except (TypeError, ValueError):
            return None
