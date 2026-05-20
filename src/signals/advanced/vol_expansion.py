"""Advanced volatility expansion signal — continuous spectrum model."""

from __future__ import annotations

import math

from src.config import (
    GEX_SCALE_INVARIANT_SATURATION,
    SIGNAL_GEX_NORMALIZATION,
)
from src.signals.components.base import MarketContext
from src.signals.components.utils import (
    pct_change_n_bar,
    realized_sigma,
    vol_normalized_momentum,
)
from src.signals.advanced.base import AdvancedSignalResult

_DIRECTION_Z_NORM = 1.0
_GEX_FLOOR = 0.15


class VolExpansionSignal:
    name = "vol_expansion"

    @staticmethod
    def expansion(ctx: MarketContext) -> float:
        return round(VolExpansionSignal._gex_readiness_ctx(ctx) * 100.0, 2)

    @staticmethod
    def direction_score(ctx: MarketContext) -> float:
        _, z = vol_normalized_momentum(ctx.recent_closes, n=5)
        momentum = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        return round(momentum * 100.0, 2)

    @staticmethod
    def magnitude(ctx: MarketContext) -> float:
        exp = VolExpansionSignal._gex_readiness_ctx(ctx)
        _, z = vol_normalized_momentum(ctx.recent_closes, n=5)
        momentum = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        return round(exp * abs(momentum) * 100.0, 2)

    @staticmethod
    def expected_5min_move_bps(ctx: MarketContext) -> float | None:
        closes = ctx.recent_closes
        if not closes or len(closes) < 5:
            return None
        sigma = realized_sigma(closes, window=60)
        if sigma <= 0:
            return None
        exp = VolExpansionSignal._gex_readiness_ctx(ctx)
        _, z = vol_normalized_momentum(closes, n=5)
        direction = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        projected_5b = sigma * math.sqrt(5)
        return round(direction * exp * projected_5b * 10000.0, 2)

    def compute(self, ctx: MarketContext) -> float:
        exp = self._gex_readiness_ctx(ctx)
        closes = ctx.recent_closes
        if len(closes) < 5 or closes[-5] <= 0:
            return 0.0
        _, z = vol_normalized_momentum(closes, n=5)
        momentum = max(-1.0, min(1.0, z / _DIRECTION_Z_NORM))
        return exp * momentum

    @staticmethod
    def _gex_readiness_ctx(ctx: MarketContext) -> float:
        """Scale-invariant readiness from MarketContext.

        Prefers the scale-invariant formula
        ``net_gex / (S² × total_oi × 100 × 0.01)`` when ``total_oi``
        is available — symbol-agnostic by construction.  Falls back
        to the legacy global ``SIGNAL_GEX_NORMALIZATION`` constant
        when ``total_oi`` is missing (older context builders, tests
        that pre-date the field).
        """
        return VolExpansionSignal._gex_readiness(
            ctx.net_gex,
            spot=ctx.close,
            total_oi=ctx.total_oi,
        )

    @staticmethod
    def _gex_readiness(
        net_gex: float,
        spot: float | None = None,
        total_oi: int | None = None,
    ) -> float:
        """GEX-readiness mapping.

        Scale-invariant path (preferred): ``net_gex / (S² × total_oi ×
        100 × 0.01)`` — a dimensionless balance measure that
        normalizes for spot magnitude AND chain size, so a single
        threshold structure applies uniformly to SPX, SPY, QQQ, and
        any new symbol without per-symbol tuning.  ``×100×0.01``
        cancels to 1 in the denominator; left in the formula for
        symmetry with the dollar-GEX formula (γ·OI·100·S²·0.01) used
        elsewhere.  Multiplied by
        ``GEX_SCALE_INVARIANT_SATURATION`` (default 100) to put the
        typical positive-regime ratio into the [-1, +1] readiness
        clamp range; this scaling constant replaces the old global
        2.1B and is the single tuning knob.

        Legacy path: when ``spot`` or ``total_oi`` is unavailable
        (older callers, tests), falls back to
        ``-net_gex / SIGNAL_GEX_NORMALIZATION``.  Preserves
        bit-for-bit behavior for callers that haven't been plumbed
        through to MarketContext.total_oi yet.
        """
        if spot is not None and spot > 0 and total_oi is not None and total_oi > 0:
            # Scale-invariant ratio.  The "× 100 × 0.01" cancels but is
            # kept as a comment for math symmetry with the dollar-GEX
            # definition (γ·OI·100·S²·0.01).
            denom = spot * spot * total_oi * 100.0 * 0.01
            raw = -net_gex / denom
            normalized = max(-1.0, min(1.0, raw * GEX_SCALE_INVARIANT_SATURATION))
        else:
            normalized = max(-1.0, min(1.0, -net_gex / SIGNAL_GEX_NORMALIZATION))
        return _GEX_FLOOR + (1.0 - _GEX_FLOOR) * (normalized + 1.0) / 2.0

    def context_values(self, ctx: MarketContext) -> dict:
        closes = ctx.recent_closes
        pct_change_5bar = None
        momentum_z = None
        momentum = None
        if len(closes) >= 5 and closes[-5] > 0:
            pct_change_5bar = round(pct_change_n_bar(closes, 5), 6)
            _, z = vol_normalized_momentum(closes, n=5)
            momentum_z = round(z, 4)
            momentum = round(max(-1.0, min(1.0, z / _DIRECTION_Z_NORM)), 4)
        sigma = realized_sigma(closes, window=60)
        return {
            "net_gex": ctx.net_gex,
            "gex_regime": "negative" if ctx.net_gex < 0 else "positive",
            "expansion": self.expansion(ctx),
            "direction": self.direction_score(ctx),
            "magnitude": self.magnitude(ctx),
            "expected_5min_move_bps": self.expected_5min_move_bps(ctx),
            "gex_readiness": round(self._gex_readiness_ctx(ctx), 4),
            "pct_change_5bar": pct_change_5bar,
            "momentum_z": momentum_z,
            "momentum": momentum,
            "realized_sigma_bar": round(sigma, 6) if sigma > 0 else None,
        }

    def evaluate(self, ctx: MarketContext) -> AdvancedSignalResult:
        score = max(-1.0, min(1.0, float(self.compute(ctx))))
        triggered = abs(score) >= 0.25
        return AdvancedSignalResult(
            name=self.name,
            score=score,
            context={
                **self.context_values(ctx),
                "triggered": triggered,
                "signal": (
                    "bullish_expansion"
                    if score > 0.25
                    else ("bearish_expansion" if score < -0.25 else "none")
                ),
            },
        )


# Backward-compat name retained for tests/importers.
VolExpansionComponent = VolExpansionSignal
VolExpansionSignalComponent = VolExpansionSignal
