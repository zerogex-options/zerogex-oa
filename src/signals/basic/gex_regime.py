"""GEX regime scoring component.

Direction is anchored by FLOW + POSITIONING; GEX sign controls *how* that
direction transmits:

* net_gex < 0  -> destabilizing / trend amplification
* net_gex > 0  -> stabilizing / mean reversion

In long-gamma regimes, pull toward max-gamma strike is blended in as the
dominant stabilizing force when available.
"""
from __future__ import annotations

import math
import os

from src.signals.components.base import ComponentBase, MarketContext

# GEX magnitude at which score reaches ~tanh(1) ≈ 0.76. A full saturation
# to ~0.99 happens near 3x this value.
_GEX_NORM = float(os.getenv("SIGNAL_GEX_REGIME_NORM", "2.5e8"))
_SHORT_GAMMA_AMPLIFICATION = max(
    0.5,
    min(2.0, float(os.getenv("SIGNAL_GEX_REGIME_SHORT_GAMMA_AMPLIFICATION", "1.15"))),
)
_LONG_GAMMA_DAMPING = max(
    0.0,
    min(1.0, float(os.getenv("SIGNAL_GEX_REGIME_LONG_GAMMA_DAMPING", "0.70"))),
)
_FLOW_MIN_NOTIONAL = max(
    0.0, float(os.getenv("SIGNAL_GEX_REGIME_FLOW_MIN_NOTIONAL", "100000"))
)
_ANCHOR_DEADBAND = max(0.0, float(os.getenv("SIGNAL_GEX_REGIME_ANCHOR_DEADBAND", "0.05")))


class GexRegimeComponent(ComponentBase):
    name = "gex_regime"
    weight = 0.07

    def compute(self, ctx: MarketContext) -> float:
        if _GEX_NORM <= 0:
            return 0.0
        regime_strength = math.tanh(abs(ctx.net_gex) / _GEX_NORM)
        direction_anchor = self._flow_positioning_anchor(ctx)
        if direction_anchor == 0.0:
            return 0.0
        if ctx.net_gex < 0:
            # Destabilizing: flow/positioning trend transmits faster.
            score = direction_anchor * regime_strength * _SHORT_GAMMA_AMPLIFICATION
        else:
            # Stabilizing: directional edge is damped and pulled toward pin.
            mean_revert_anchor = self._mean_reversion_anchor(ctx)
            blended_anchor = (
                0.65 * mean_revert_anchor + 0.35 * direction_anchor
                if mean_revert_anchor is not None
                else direction_anchor
            )
            score = blended_anchor * regime_strength * _LONG_GAMMA_DAMPING
        return max(-1.0, min(1.0, score))

    def context_values(self, ctx: MarketContext) -> dict:
        regime_strength = math.tanh(abs(ctx.net_gex) / _GEX_NORM) if _GEX_NORM > 0 else 0.0
        direction_anchor = self._flow_positioning_anchor(ctx)
        mean_revert_anchor = self._mean_reversion_anchor(ctx)
        score = self.compute(ctx)
        return {
            "net_gex": ctx.net_gex,
            "gex_norm": _GEX_NORM,
            "regime": "short_gamma" if ctx.net_gex < 0 else "long_gamma",
            "regime_state": (
                "destabilizing_trend_amplifying"
                if ctx.net_gex < 0
                else "stabilizing_mean_reverting"
            ),
            "regime_strength": round(regime_strength, 6),
            "direction_anchor_flow_positioning": round(direction_anchor, 6),
            "mean_reversion_anchor": (
                round(mean_revert_anchor, 6) if mean_revert_anchor is not None else None
            ),
            "short_gamma_amplification": _SHORT_GAMMA_AMPLIFICATION,
            "long_gamma_damping": _LONG_GAMMA_DAMPING,
            "score": round(score, 6),
        }

    @staticmethod
    def _flow_positioning_anchor(ctx: MarketContext) -> float:
        call_net = float(ctx.smart_call or 0.0)
        put_net = float(ctx.smart_put or 0.0)
        participation = abs(call_net) + abs(put_net)
        flow_bias = ((call_net - put_net) / participation) if participation >= _FLOW_MIN_NOTIONAL else 0.0

        pcr = float(ctx.put_call_ratio or 1.0)
        positioning_bias = max(-1.0, min(1.0, (1.0 - pcr) / 0.35))

        tape_bias = 0.0
        rows = (ctx.extra or {}).get("flow_by_type") or []
        if isinstance(rows, list):
            call_tape = 0.0
            put_tape = 0.0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                opt = str(row.get("option_type") or "").upper()
                try:
                    net = float(row.get("buy_premium") or 0.0) - float(row.get("sell_premium") or 0.0)
                except (TypeError, ValueError):
                    continue
                if opt == "C":
                    call_tape += net
                elif opt == "P":
                    put_tape += net
            tape_denom = abs(call_tape) + abs(put_tape)
            if tape_denom >= _FLOW_MIN_NOTIONAL:
                tape_bias = (call_tape - put_tape) / tape_denom

        anchor = (0.55 * flow_bias) + (0.30 * tape_bias) + (0.15 * positioning_bias)
        if abs(anchor) < _ANCHOR_DEADBAND:
            return 0.0
        return max(-1.0, min(1.0, anchor))

    @staticmethod
    def _mean_reversion_anchor(ctx: MarketContext) -> float | None:
        pin = (ctx.extra or {}).get("max_gamma_strike")
        if pin is None or ctx.close <= 0:
            return None
        try:
            pin_f = float(pin)
        except (TypeError, ValueError):
            return None
        distance = (pin_f - ctx.close) / ctx.close
        return max(-1.0, min(1.0, distance / 0.004))
