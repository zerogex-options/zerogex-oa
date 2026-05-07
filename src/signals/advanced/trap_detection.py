"""Advanced trap-detection (fade) setup signal."""

from __future__ import annotations

from src.signals.components.base import MarketContext
from src.signals.advanced.base import (
    BREAKOUT_BUFFER_MIN,
    BREAKOUT_BUFFER_VOL_MULT,
    AdvancedSignalResult,
    flow_flux_norm,
    nearest_above,
    nearest_below,
    realized_pct_sigma,
)


class TrapDetectionSignal:
    name = "trap_detection"

    def evaluate(self, ctx: MarketContext) -> AdvancedSignalResult:
        extra = ctx.extra or {}
        call_wall = extra.get("call_wall")
        prior_call_wall = extra.get("prior_call_wall")
        put_wall = extra.get("put_wall")
        prior_put_wall = extra.get("prior_put_wall")
        max_gamma = extra.get("max_gamma_strike")
        vwap = ctx.vwap
        flip = ctx.gamma_flip

        up_levels = [call_wall, max_gamma, vwap, flip]
        dn_levels = [put_wall, max_gamma, vwap, flip]
        # These are the *broken* levels — the resistance that price has just
        # poked above (now sits below close) and the support that price has
        # just slipped beneath (now sits above close). Naming reflects the
        # post-breakout perspective the trap setup keys off.
        broken_resistance = nearest_below(up_levels, ctx.close)
        broken_support = nearest_above(dn_levels, ctx.close)

        sigma = realized_pct_sigma(ctx)
        buffer_pct = max(BREAKOUT_BUFFER_MIN, BREAKOUT_BUFFER_VOL_MULT * sigma * (5**0.5))

        breakout_up = bool(
            broken_resistance is not None and ctx.close > broken_resistance * (1.0 + buffer_pct)
        )
        breakout_down = bool(
            broken_support is not None and ctx.close < broken_support * (1.0 - buffer_pct)
        )

        long_gamma = ctx.net_gex > 0
        net_gex_delta = float(extra.get("net_gex_delta") or 0.0)
        net_gex_delta_pct = float(extra.get("net_gex_delta_pct") or 0.0)
        gamma_strengthening = net_gex_delta_pct > 0.005

        # Bear-trap (upside fade) is invalidated when the call wall migrates
        # higher — the resistance moved out of the way, signalling a real
        # breakout.  Mirror logic for the bull-trap (downside fade) keys off
        # the put wall: if it migrates lower, the support moved out of the
        # way and the breakdown is real.
        call_wall_migrated_up = (
            prior_call_wall is not None
            and call_wall is not None
            and call_wall > prior_call_wall * 1.0005
        )
        put_wall_migrated_down = (
            prior_put_wall is not None
            and put_wall is not None
            and put_wall < prior_put_wall * 0.9995
        )

        call_flow_delta = float(extra.get("call_flow_delta") or 0.0)
        put_flow_delta = float(extra.get("put_flow_delta") or 0.0)
        flow_norm = flow_flux_norm(ctx)
        call_decelerating = call_flow_delta < 0
        put_decelerating = put_flow_delta < 0

        # Soften the formerly-binary AND gating: each precondition becomes
        # a [0, 1] factor.  ``trap_strength`` is the product of all four;
        # mid-strength reads land between 0 and 1 instead of slamming to 0
        # the moment any one condition softens.
        long_gamma_factor = max(0.0, min(1.0, ctx.net_gex / 1.0e9)) if ctx.net_gex > 0 else 0.0
        strengthening_factor = max(0.0, min(1.0, net_gex_delta_pct / 0.02))
        upside_breakout_strength = self._breakout_strength(ctx.close, broken_resistance, buffer_pct)
        downside_breakout_strength = self._breakout_strength(
            broken_support, ctx.close, buffer_pct
        ) if broken_support is not None else 0.0
        wall_up_factor = 0.3 if call_wall_migrated_up else 1.0
        wall_dn_factor = 0.3 if put_wall_migrated_down else 1.0

        upside_strength = (
            upside_breakout_strength * long_gamma_factor * strengthening_factor * wall_up_factor
        )
        downside_strength = (
            downside_breakout_strength * long_gamma_factor * strengthening_factor * wall_dn_factor
        )

        def _magnitude(dist_pct: float) -> float:
            dist_strength = min(1.0, abs(dist_pct) / max(buffer_pct * 3.0, 0.003))
            gex_boost = min(1.0, abs(net_gex_delta_pct) / 0.05)
            return 0.4 + 0.4 * dist_strength + 0.2 * gex_boost

        bear_score = 0.0
        bull_score = 0.0
        if upside_strength > 0 and broken_resistance:
            dist_pct = (ctx.close - broken_resistance) / ctx.close
            mag = _magnitude(dist_pct)
            flow_mult = (
                1.1 if call_decelerating else max(0.3, 1.0 - call_flow_delta / max(flow_norm, 1.0))
            )
            bear_score = min(1.0, mag * flow_mult * upside_strength)
        if downside_strength > 0 and broken_support:
            dist_pct = (ctx.close - broken_support) / ctx.close
            mag = _magnitude(dist_pct)
            flow_mult = (
                1.1 if put_decelerating else max(0.3, 1.0 - put_flow_delta / max(flow_norm, 1.0))
            )
            bull_score = min(1.0, mag * flow_mult * downside_strength)

        # Net the two trap sides.  Each is non-negative; their difference
        # encodes both direction and conviction continuously.
        score = max(-1.0, min(1.0, bull_score - bear_score))
        triggered = abs(score) >= 0.25

        return AdvancedSignalResult(
            name=self.name,
            score=score,
            context={
                "triggered": triggered,
                "signal": (
                    "bearish_fade" if score < 0 else ("bullish_fade" if score > 0 else "none")
                ),
                "close": ctx.close,
                "broken_resistance_level": broken_resistance,
                "broken_support_level": broken_support,
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
                "put_wall": put_wall,
                "prior_put_wall": prior_put_wall,
                "call_wall_migrated_up": call_wall_migrated_up,
                "put_wall_migrated_down": put_wall_migrated_down,
                "call_flow_decelerating": call_decelerating,
                "put_flow_decelerating": put_decelerating,
            },
        )

    @staticmethod
    def _breakout_strength(upper: float | None, lower: float | None, buffer_pct: float) -> float:
        """Continuous breakout strength in [0, 1].

        Replaces the binary ``breakout_up``/``breakout_down`` flag so the
        signal grades through 0 instead of snapping to it the moment price
        crosses ``level * (1 ± buffer_pct)``.  Returns 1.0 once price is
        ~3× the buffer beyond the broken level; 0.0 when below the buffer.
        """
        if upper is None or lower is None or lower <= 0 or buffer_pct <= 0:
            return 0.0
        excess_pct = (upper - lower) / lower - buffer_pct
        if excess_pct <= 0:
            return 0.0
        return max(0.0, min(1.0, excess_pct / (buffer_pct * 3.0)))
