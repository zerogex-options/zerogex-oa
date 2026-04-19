"""Independent signal detectors (not included in composite scoring)."""
from __future__ import annotations

from dataclasses import dataclass

from src.signals.components.base import MarketContext
from src.signals.components.utils import pct_change_n_bar


@dataclass
class IndependentSignalResult:
    name: str
    score: float
    context: dict


class IndependentSignalEngine:
    """Generate side-channel signals persisted like components with weight=0."""

    def evaluate(self, ctx: MarketContext) -> list[IndependentSignalResult]:
        return [
            self._squeeze_setup(ctx),
            self._trap_detection(ctx),
            self._zero_dte_position_imbalance(ctx),
            self._gamma_vwap_confluence(ctx),
        ]

    def _squeeze_setup(self, ctx: MarketContext) -> IndependentSignalResult:
        extra = ctx.extra or {}
        call_flow_delta = float(extra.get("call_flow_delta") or 0.0)
        put_flow_delta = float(extra.get("put_flow_delta") or 0.0)
        mom5 = pct_change_n_bar(ctx.recent_closes, 5)
        flip = ctx.gamma_flip

        above_flip = bool(flip is not None and ctx.close > flip)
        below_flip = bool(flip is not None and ctx.close < flip)
        neg_gex = ctx.net_gex < 0

        call_flow_increasing = call_flow_delta > 0
        put_flow_increasing = put_flow_delta > 0

        bullish_trigger = neg_gex and above_flip and call_flow_increasing and mom5 >= 0
        bearish_trigger = neg_gex and below_flip and put_flow_increasing and mom5 <= 0

        score = 0.0
        if bullish_trigger:
            score = min(1.0, 0.6 + min(0.4, call_flow_delta / 250_000.0))
        elif bearish_trigger:
            score = -min(1.0, 0.6 + min(0.4, put_flow_delta / 250_000.0))

        return IndependentSignalResult(
            name="squeeze_setup",
            score=score,
            context={
                "triggered": score != 0.0,
                "signal": "bullish_squeeze" if score > 0 else ("bearish_squeeze" if score < 0 else "none"),
                "net_gex": ctx.net_gex,
                "gamma_flip": flip,
                "close": ctx.close,
                "call_flow_delta": round(call_flow_delta, 2),
                "put_flow_delta": round(put_flow_delta, 2),
                "momentum_5bar": round(mom5, 6),
            },
        )

    def _trap_detection(self, ctx: MarketContext) -> IndependentSignalResult:
        extra = ctx.extra or {}
        net_gex_delta = float(extra.get("net_gex_delta") or 0.0)
        call_wall = extra.get("call_wall")
        max_gamma = extra.get("max_gamma_strike")
        vwap = ctx.vwap
        flip = ctx.gamma_flip

        resistance_levels = [x for x in [call_wall, max_gamma, vwap, flip] if x is not None]
        support_levels = [x for x in [max_gamma, vwap, flip] if x is not None]
        resistance = max(resistance_levels) if resistance_levels else None
        support = min(support_levels) if support_levels else None

        breakout_up = bool(resistance is not None and ctx.close > resistance * 1.001)
        breakout_down = bool(support is not None and ctx.close < support * 0.999)

        long_gamma = ctx.net_gex > 0
        gamma_strengthening = net_gex_delta > 0

        upside_fail_trigger = breakout_up and long_gamma and gamma_strengthening
        downside_fail_trigger = breakout_down and long_gamma and gamma_strengthening

        score = 0.0
        if upside_fail_trigger:
            # Breakout likely to fail -> bearish fade.
            score = -min(1.0, 0.55 + min(0.45, net_gex_delta / 500_000_000.0))
        elif downside_fail_trigger:
            # Breakdown likely to fail -> bullish fade.
            score = min(1.0, 0.55 + min(0.45, net_gex_delta / 500_000_000.0))

        return IndependentSignalResult(
            name="trap_detection",
            score=score,
            context={
                "triggered": score != 0.0,
                "signal": "bearish_fade" if score < 0 else ("bullish_fade" if score > 0 else "none"),
                "close": ctx.close,
                "resistance_level": resistance,
                "support_level": support,
                "breakout_up": breakout_up,
                "breakout_down": breakout_down,
                "net_gex": ctx.net_gex,
                "net_gex_delta": round(net_gex_delta, 2),
                "long_gamma": long_gamma,
                "gamma_strengthening": gamma_strengthening,
            },
        )

    def _zero_dte_position_imbalance(self, ctx: MarketContext) -> IndependentSignalResult:
        """Proxy for same-day positioning crowding from flow + breadth metrics."""
        extra = ctx.extra or {}
        flow_rows = extra.get("flow_by_type") or []

        call_buy = call_sell = put_buy = put_sell = 0.0
        for row in flow_rows:
            option_type = row.get("option_type")
            buy = float(row.get("buy_premium") or 0.0)
            sell = float(row.get("sell_premium") or 0.0)
            if option_type == "C":
                call_buy += buy
                call_sell += sell
            elif option_type == "P":
                put_buy += buy
                put_sell += sell

        call_net = call_buy - call_sell
        put_net = put_buy - put_sell
        total_net = abs(call_net) + abs(put_net)
        flow_imbalance = ((call_net - put_net) / total_net) if total_net > 50_000 else 0.0

        sm_total = ctx.smart_call + ctx.smart_put
        smart_imbalance = ((ctx.smart_call - ctx.smart_put) / sm_total) if sm_total > 100_000 else 0.0

        pcr_tilt = max(-1.0, min(1.0, (1.0 - ctx.put_call_ratio) / 0.35))
        combined = 0.50 * flow_imbalance + 0.35 * smart_imbalance + 0.15 * pcr_tilt
        score = max(-1.0, min(1.0, combined))

        return IndependentSignalResult(
            name="zero_dte_position_imbalance",
            score=score,
            context={
                "triggered": abs(score) >= 0.25,
                "signal": "call_heavy" if score > 0.25 else ("put_heavy" if score < -0.25 else "balanced"),
                "call_net_premium": round(call_net, 2),
                "put_net_premium": round(put_net, 2),
                "flow_imbalance": round(flow_imbalance, 4),
                "smart_imbalance": round(smart_imbalance, 4),
                "put_call_ratio": round(ctx.put_call_ratio, 4),
            },
        )

    def _gamma_vwap_confluence(self, ctx: MarketContext) -> IndependentSignalResult:
        """Directional confluence when gamma flip and VWAP cluster tightly."""
        flip = ctx.gamma_flip
        vwap = ctx.vwap
        if flip is None or vwap is None or ctx.close <= 0:
            return IndependentSignalResult(
                name="gamma_vwap_confluence",
                score=0.0,
                context={"triggered": False, "signal": "none", "reason": "missing_levels"},
            )

        cluster_gap_pct = abs(flip - vwap) / ctx.close
        clustered = cluster_gap_pct <= 0.0025  # 25 bps
        if not clustered:
            return IndependentSignalResult(
                name="gamma_vwap_confluence",
                score=0.0,
                context={
                    "triggered": False,
                    "signal": "none",
                    "gamma_flip": flip,
                    "vwap": vwap,
                    "cluster_gap_pct": round(cluster_gap_pct, 6),
                },
            )

        confluence_level = 0.5 * (flip + vwap)
        dist_from_level = (ctx.close - confluence_level) / ctx.close
        dir_sign = 1.0 if dist_from_level > 0 else -1.0 if dist_from_level < 0 else 0.0
        distance_strength = min(1.0, abs(dist_from_level) / 0.003)

        # Negative GEX amplifies continuation away from confluence; positive GEX
        # dampens the move due to dealer mean-reversion pressure.
        gamma_mult = 1.0 if ctx.net_gex < 0 else 0.55
        score = dir_sign * distance_strength * gamma_mult
        score = max(-1.0, min(1.0, score))

        return IndependentSignalResult(
            name="gamma_vwap_confluence",
            score=score,
            context={
                "triggered": abs(score) >= 0.2,
                "signal": "bullish_confluence" if score > 0.2 else ("bearish_confluence" if score < -0.2 else "neutral"),
                "gamma_flip": round(flip, 4),
                "vwap": round(vwap, 4),
                "confluence_level": round(confluence_level, 4),
                "cluster_gap_pct": round(cluster_gap_pct, 6),
                "distance_from_level_pct": round(dist_from_level, 6),
                "net_gex": ctx.net_gex,
            },
        )
