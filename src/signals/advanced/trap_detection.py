"""Independent trap-detection (fade) signal."""
from __future__ import annotations

from src.signals.components.base import MarketContext
from src.signals.advanced.base import (
    BREAKOUT_BUFFER_MIN,
    BREAKOUT_BUFFER_VOL_MULT,
    IndependentSignalResult,
    flow_flux_norm,
    nearest_above,
    nearest_below,
    realized_pct_sigma,
)


class TrapDetectionSignal:
    name = "trap_detection"

    def evaluate(self, ctx: MarketContext) -> IndependentSignalResult:
        extra = ctx.extra or {}
        call_wall = extra.get("call_wall")
        prior_call_wall = extra.get("prior_call_wall")
        max_gamma = extra.get("max_gamma_strike")
        vwap = ctx.vwap
        flip = ctx.gamma_flip

        up_levels = [call_wall, max_gamma, vwap, flip]
        dn_levels = [max_gamma, vwap, flip]
        resistance = nearest_below(up_levels, ctx.close)
        support = nearest_above(dn_levels, ctx.close)

        sigma = realized_pct_sigma(ctx)
        buffer_pct = max(BREAKOUT_BUFFER_MIN, BREAKOUT_BUFFER_VOL_MULT * sigma * (5 ** 0.5))

        breakout_up = bool(resistance is not None and ctx.close > resistance * (1.0 + buffer_pct))
        breakout_down = bool(support is not None and ctx.close < support * (1.0 - buffer_pct))

        long_gamma = ctx.net_gex > 0
        net_gex_delta = float(extra.get("net_gex_delta") or 0.0)
        net_gex_delta_pct = float(extra.get("net_gex_delta_pct") or 0.0)
        gamma_strengthening = net_gex_delta_pct > 0.005

        wall_migrated_up = (
            prior_call_wall is not None
            and call_wall is not None
            and call_wall > prior_call_wall * 1.0005
        )
        wall_migrated_down = (
            prior_call_wall is not None
            and call_wall is not None
            and call_wall < prior_call_wall * 0.9995
        )

        call_flow_delta = float(extra.get("call_flow_delta") or 0.0)
        put_flow_delta = float(extra.get("put_flow_delta") or 0.0)
        flow_norm = flow_flux_norm(ctx)
        call_decelerating = call_flow_delta < 0
        put_decelerating = put_flow_delta < 0

        upside_fail = breakout_up and long_gamma and gamma_strengthening and not wall_migrated_up
        downside_fail = breakout_down and long_gamma and gamma_strengthening and not wall_migrated_down

        def _magnitude(dist_pct: float) -> float:
            dist_strength = min(1.0, abs(dist_pct) / max(buffer_pct * 3.0, 0.003))
            gex_boost = min(1.0, abs(net_gex_delta_pct) / 0.05)
            return 0.4 + 0.4 * dist_strength + 0.2 * gex_boost

        score = 0.0
        if upside_fail and resistance:
            dist_pct = (ctx.close - resistance) / ctx.close
            mag = _magnitude(dist_pct)
            flow_mult = (
                1.1
                if call_decelerating
                else max(0.3, 1.0 - call_flow_delta / max(flow_norm, 1.0))
            )
            score = -min(1.0, mag * flow_mult)
        elif downside_fail and support:
            dist_pct = (ctx.close - support) / ctx.close
            mag = _magnitude(dist_pct)
            flow_mult = (
                1.1
                if put_decelerating
                else max(0.3, 1.0 - put_flow_delta / max(flow_norm, 1.0))
            )
            score = min(1.0, mag * flow_mult)

        score = max(-1.0, min(1.0, score))
        triggered = abs(score) >= 0.25

        return IndependentSignalResult(
            name=self.name,
            score=score,
            context={
                "triggered": triggered,
                "signal": "bearish_fade" if score < 0 else ("bullish_fade" if score > 0 else "none"),
                "close": ctx.close,
                "resistance_level": resistance,
                "support_level": support,
                "breakout_buffer_pct": round(buffer_pct, 6),
                "realized_sigma": round(sigma, 6),
                "breakout_up": breakout_up,
                "breakout_down": breakout_down,
                "net_gex": ctx.net_gex,
                "net_gex_delta": round(net_gex_delta, 2),
                "net_gex_delta_pct": round(net_gex_delta_pct, 6),
                "long_gamma": long_gamma,
                "gamma_strengthening": gamma_strengthening,
                "call_wall": call_wall,
                "prior_call_wall": prior_call_wall,
                "wall_migrated_up": wall_migrated_up,
                "wall_migrated_down": wall_migrated_down,
                "call_flow_decelerating": call_decelerating,
                "put_flow_decelerating": put_decelerating,
            },
        )
