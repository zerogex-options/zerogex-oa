"""Market-state put/call ratio component."""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext

# Per-symbol PCR distributions differ enough that a single saturation
# scale (formerly hardcoded ``0.4``) over-confidently maps moderate SPX
# PCR into the same -1/+1 buckets as extreme SPY PCR.  Calibration is
# now per-symbol via env override, with a sensible default that applies
# unless the symbol's normalizer cache row carries one.
#
# Override conventions, in order of precedence:
#   1. ``ctx.extra["normalizers"]["put_call_ratio_saturation"]``
#      (typically populated from ``component_normalizer_cache``).
#   2. ``PCR_SATURATION_<SYMBOL>`` env var (e.g. ``PCR_SATURATION_SPX=0.6``).
#   3. ``PCR_SATURATION_DEFAULT`` env var.
#   4. ``0.4`` (legacy default; same scale used before this change).
_DEFAULT_PCR_SATURATION = float(os.getenv("PCR_SATURATION_DEFAULT", "0.4"))


def _resolve_saturation(ctx: MarketContext) -> float:
    extra = ctx.extra or {}
    norms = extra.get("normalizers") or {}
    cached = norms.get("put_call_ratio_saturation")
    if cached and float(cached) > 0:
        return float(cached)
    sym = (getattr(ctx, "symbol", None) or extra.get("symbol") or "").upper()
    if sym:
        per_symbol = os.getenv(f"PCR_SATURATION_{sym}")
        if per_symbol:
            try:
                v = float(per_symbol)
                if v > 0:
                    return v
            except ValueError:
                pass
    return _DEFAULT_PCR_SATURATION


class PutCallRatioStateComponent(ComponentBase):
    name = "put_call_ratio"
    weight = 0.15

    def compute(self, ctx: MarketContext) -> float:
        pcr = float(ctx.put_call_ratio or 1.0)
        sat = _resolve_saturation(ctx)
        # Higher PCR => more fragile state / larger potential move.
        return max(-1.0, min(1.0, (pcr - 1.0) / sat))

    def context_values(self, ctx: MarketContext) -> dict:
        score = self.compute(ctx)
        return {
            "put_call_ratio": float(ctx.put_call_ratio or 1.0),
            "saturation": _resolve_saturation(ctx),
            "score": round(score, 6),
            "component_points": round(score * self.weight, 4),
        }
