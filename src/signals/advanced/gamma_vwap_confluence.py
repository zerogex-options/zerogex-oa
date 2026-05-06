"""Advanced gamma + VWAP confluence detector."""

from __future__ import annotations

from src.signals.components.base import MarketContext
from src.signals.advanced.base import (
    CONFLUENCE_MAX_GAP_PCT,
    AdvancedSignalResult,
)


class GammaVwapConfluenceSignal:
    name = "gamma_vwap_confluence"

    def evaluate(self, ctx: MarketContext) -> AdvancedSignalResult:
        flip = ctx.gamma_flip
        vwap = ctx.vwap
        extra = ctx.extra or {}
        max_pain = ctx.max_pain
        call_wall = extra.get("call_wall")
        max_gamma = extra.get("max_gamma_strike")

        def _round(value: float | None, ndigits: int = 4) -> float | None:
            return round(value, ndigits) if isinstance(value, (int, float)) else None

        input_levels = {
            "gamma_flip": _round(flip),
            "vwap": _round(vwap),
            "max_pain": _round(max_pain),
            "max_gamma": _round(max_gamma),
            "call_wall": _round(call_wall),
        }

        if flip is None or vwap is None or ctx.close <= 0:
            return AdvancedSignalResult(
                name=self.name,
                score=0.0,
                context={
                    "triggered": False,
                    "signal": "none",
                    "reason": "missing_levels",
                    "cluster_gap_pct": None,
                    **input_levels,
                },
            )

        core_mid = 0.5 * (flip + vwap)
        cluster_candidates = {"gamma_flip": flip, "vwap": vwap}
        for name, level in [
            ("max_pain", max_pain),
            ("max_gamma", max_gamma),
            ("call_wall", call_wall),
        ]:
            if level is None:
                continue
            if abs(level - core_mid) / ctx.close <= 0.0015:
                cluster_candidates[name] = level

        cluster_gap_pct = abs(flip - vwap) / ctx.close
        # Soft confidence floor (not a hard cutoff): even when flip and VWAP
        # are far apart we keep a fractional read instead of forcing a 0.0
        # — matches the spec that 0 should be a rare extreme.
        raw_quality = 1.0 - cluster_gap_pct / max(CONFLUENCE_MAX_GAP_PCT, 1e-9)
        cluster_quality = max(0.05, min(1.0, raw_quality))

        extra_levels = max(0, len(cluster_candidates) - 2)
        multi_mult = 1.0 + 0.15 * extra_levels

        levels = list(cluster_candidates.values())
        confluence_level = sum(levels) / len(levels)
        dist_from_level = (ctx.close - confluence_level) / ctx.close
        # Continuous direction-magnitude product instead of a binary sign
        # times strength: ``dist_from_level / 0.003`` is already signed
        # and clamped, which keeps the score smooth around the level.
        raw = max(-1.0, min(1.0, dist_from_level / 0.003))
        if ctx.net_gex < 0:
            directional = raw
            regime_direction = "continuation"
        else:
            directional = -raw * 0.7
            regime_direction = "mean_reversion"

        score = directional * cluster_quality * multi_mult
        score = max(-1.0, min(1.0, score))
        dir_sign = 1.0 if dist_from_level > 0 else -1.0 if dist_from_level < 0 else 0.0
        expected_target = (
            confluence_level
            if regime_direction == "mean_reversion"
            else ctx.close + dir_sign * (ctx.close - confluence_level) * 2.0
        )
        triggered = abs(score) >= 0.2

        return AdvancedSignalResult(
            name=self.name,
            score=score,
            context={
                "triggered": triggered,
                "signal": (
                    "bullish_confluence"
                    if score > 0.2
                    else ("bearish_confluence" if score < -0.2 else "neutral")
                ),
                "regime_direction": regime_direction,
                **input_levels,
                "confluence_level": round(confluence_level, 4),
                "cluster_members": list(cluster_candidates.keys()),
                "cluster_gap_pct": round(cluster_gap_pct, 6),
                "cluster_quality": round(cluster_quality, 4),
                "distance_from_level_pct": round(dist_from_level, 6),
                "expected_target": round(expected_target, 4),
                "net_gex": ctx.net_gex,
            },
        )


# Backward-compat alias for existing imports/tests.
GammaVWAPConfluenceSignal = GammaVwapConfluenceSignal
