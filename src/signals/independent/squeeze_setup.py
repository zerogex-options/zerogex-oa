"""Independent squeeze-setup detector."""
from __future__ import annotations

from src.signals.components.base import MarketContext
from src.signals.components.utils import pct_change_n_bar, vol_normalized_momentum
from src.signals.independent.base import (
    IndependentSignalResult,
    flow_flux_norm,
    tanh_scaled,
    vix_regime,
)


class SqueezeSetupSignal:
    name = "squeeze_setup"

    def evaluate(self, ctx: MarketContext) -> IndependentSignalResult:
        extra = ctx.extra or {}
        call_flow_delta = float(extra.get("call_flow_delta") or 0.0)
        put_flow_delta = float(extra.get("put_flow_delta") or 0.0)
        flow_norm = flow_flux_norm(ctx)

        call_flow_z = call_flow_delta / max(flow_norm, 1.0)
        put_flow_z = put_flow_delta / max(flow_norm, 1.0)

        _, mom_z = vol_normalized_momentum(ctx.recent_closes, n=5, vol_window=60)
        mom_5 = pct_change_n_bar(ctx.recent_closes, 5)
        mom_10 = pct_change_n_bar(ctx.recent_closes, 10)
        accel_up = mom_5 > mom_10 > 0
        accel_dn = mom_5 < mom_10 < 0

        flip = ctx.gamma_flip
        above_flip = bool(flip is not None and ctx.close > flip)
        below_flip = bool(flip is not None and ctx.close < flip)
        gex_readiness = 1.0 if ctx.net_gex < 0 else 0.5

        dir_strength_up = max(0.0, min(1.0, mom_z))
        dir_strength_dn = max(0.0, min(1.0, -mom_z))
        accel_mult_up = 1.2 if accel_up else 1.0
        accel_mult_dn = 1.2 if accel_dn else 1.0

        bull = (
            tanh_scaled(call_flow_z)
            * dir_strength_up
            * gex_readiness
            * accel_mult_up
            * (1.0 if above_flip else 0.6)
        )
        bear = (
            tanh_scaled(put_flow_z)
            * dir_strength_dn
            * gex_readiness
            * accel_mult_dn
            * (1.0 if below_flip else 0.6)
        )

        score = 0.0
        if bull > 0 and call_flow_z > 0:
            score = bull
        elif bear > 0 and put_flow_z > 0:
            score = -bear
        score = max(-1.0, min(1.0, score))
        triggered = abs(score) >= 0.25

        vix_level = extra.get("vix_level")
        regime = vix_regime(vix_level)
        return IndependentSignalResult(
            name=self.name,
            score=score,
            context={
                "triggered": triggered,
                "signal": (
                    "bullish_squeeze"
                    if score > 0
                    else ("bearish_squeeze" if score < 0 else "none")
                ),
                "net_gex": ctx.net_gex,
                "gamma_flip": flip,
                "close": ctx.close,
                "call_flow_delta": round(call_flow_delta, 2),
                "put_flow_delta": round(put_flow_delta, 2),
                "call_flow_z": round(call_flow_z, 3),
                "put_flow_z": round(put_flow_z, 3),
                "momentum_5bar": round(mom_5, 6),
                "momentum_10bar": round(mom_10, 6),
                "momentum_z": round(mom_z, 3),
                "accel_up": accel_up,
                "accel_dn": accel_dn,
                "flow_norm_used": round(flow_norm, 2),
                "vix_level": vix_level,
                "vix_regime": regime,
            },
        )
