"""Advanced 0DTE position-imbalance detector."""
from __future__ import annotations

import math

from src.signals.components.base import MarketContext
from src.signals.components.utils import (
    SESSION_CLOSE_MIN_ET,
    SESSION_OPEN_MIN_ET,
    minute_of_day_et,
)
from src.signals.advanced.base import AdvancedSignalResult


class ZeroDtePositionImbalanceSignal:
    name = "zero_dte_position_imbalance"

    def evaluate(self, ctx: MarketContext) -> AdvancedSignalResult:
        extra = ctx.extra or {}
        flow_rows = extra.get("flow_zero_dte") or []
        used_zero_dte = bool(flow_rows)
        if not flow_rows:
            flow_rows = extra.get("flow_by_type") or []

        close = ctx.close if ctx.close > 0 else 1.0

        def bucket_moneyness(option_type: str, strike: float) -> str:
            m = (strike - close) / close
            if option_type == "C":
                if m > 0.005:
                    return "otm"
                if m < -0.005:
                    return "itm"
                return "atm"
            if m < -0.005:
                return "otm"
            if m > 0.005:
                return "itm"
            return "atm"

        buckets = {
            ("C", "otm"): 0.0,
            ("C", "atm"): 0.0,
            ("C", "itm"): 0.0,
            ("P", "otm"): 0.0,
            ("P", "atm"): 0.0,
            ("P", "itm"): 0.0,
        }
        call_net_total = 0.0
        put_net_total = 0.0
        for row in flow_rows:
            option_type = row.get("option_type")
            if option_type not in ("C", "P"):
                continue
            strike = float(row.get("strike") or 0.0)
            buy = float(row.get("buy_premium") or 0.0)
            sell = float(row.get("sell_premium") or 0.0)
            net = buy - sell
            bucket_key = (
                option_type,
                bucket_moneyness(option_type, strike) if strike > 0 else "atm",
            )
            buckets[bucket_key] = buckets.get(bucket_key, 0.0) + net
            if option_type == "C":
                call_net_total += net
            else:
                put_net_total += net

        weighted = (
            0.6 * buckets[("C", "otm")]
            + 0.3 * buckets[("C", "atm")]
            + 0.1 * buckets[("C", "itm")]
            - 0.6 * buckets[("P", "otm")]
            - 0.3 * buckets[("P", "atm")]
            - 0.1 * buckets[("P", "itm")]
        )
        total_abs = sum(abs(v) for v in buckets.values())
        flow_imbalance = weighted / total_abs if total_abs > 50_000 else 0.0

        sm_call = ctx.smart_call
        sm_put = ctx.smart_put
        sm_gross = float(extra.get("smart_call_gross") or 0.0) + float(
            extra.get("smart_put_gross") or 0.0
        )
        smart_imbalance = ((sm_call - sm_put) / sm_gross) if sm_gross > 100_000 else 0.0

        pcr_tilt = max(-1.0, min(1.0, (1.0 - ctx.put_call_ratio) / 0.35))
        combined = 0.55 * flow_imbalance + 0.30 * smart_imbalance + 0.15 * pcr_tilt

        minute = minute_of_day_et(ctx.timestamp)
        if minute is not None and SESSION_OPEN_MIN_ET <= minute < SESSION_CLOSE_MIN_ET:
            hours_to_close = max(0.1, (SESSION_CLOSE_MIN_ET - minute) / 60.0)
            tod_mult = min(1.0, math.sqrt(hours_to_close / 6.5)) * 1.1
        else:
            tod_mult = 0.0
        combined *= tod_mult

        score = max(-1.0, min(1.0, combined))
        triggered = abs(score) >= 0.25

        return AdvancedSignalResult(
            name=self.name,
            score=score,
            context={
                "triggered": triggered,
                "signal": (
                    "call_heavy"
                    if score > 0.25
                    else ("put_heavy" if score < -0.25 else "balanced")
                ),
                "call_net_premium": round(call_net_total, 2),
                "put_net_premium": round(put_net_total, 2),
                "otm_call_net": round(buckets[("C", "otm")], 2),
                "atm_call_net": round(buckets[("C", "atm")], 2),
                "otm_put_net": round(buckets[("P", "otm")], 2),
                "atm_put_net": round(buckets[("P", "atm")], 2),
                "flow_imbalance": round(flow_imbalance, 4),
                "smart_imbalance": round(smart_imbalance, 4),
                "pcr_tilt": round(pcr_tilt, 4),
                "put_call_ratio": round(ctx.put_call_ratio, 4),
                "tod_multiplier": round(tod_mult, 3),
                "flow_source": "zero_dte" if used_zero_dte else "all_expiry_fallback",
            },
        )


# Backward-compat alias.
ZeroDTEPositionImbalanceSignal = ZeroDtePositionImbalanceSignal
