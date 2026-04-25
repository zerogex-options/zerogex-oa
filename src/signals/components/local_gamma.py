"""Local gamma density component for Market State Index."""

from __future__ import annotations

from src.signals.components.base import ComponentBase, MarketContext


class LocalGammaComponent(ComponentBase):
    name = "local_gamma"
    weight = 0.20

    def compute(self, ctx: MarketContext) -> float:
        local_gex = float((ctx.extra or {}).get("local_gex") or 0.0)
        normalizer = float((ctx.extra or {}).get("normalizers", {}).get("local_gex") or 0.0)
        if normalizer <= 0:
            normalizer = max(abs(ctx.net_gex), 1.0)
        ratio = local_gex / max(normalizer, 1.0)
        # High local gamma => pinning/stable (-1), low local gamma => air pocket (+1)
        return max(-1.0, min(1.0, 1.0 - 2.0 * min(ratio, 1.0)))

    def context_values(self, ctx: MarketContext) -> dict:
        local_gex = float((ctx.extra or {}).get("local_gex") or 0.0)
        normalizer = float((ctx.extra or {}).get("normalizers", {}).get("local_gex") or 0.0)
        return {
            "local_gex": local_gex,
            "normalizer": normalizer,
            "score": round(self.compute(ctx), 6),
        }
