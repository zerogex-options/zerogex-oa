"""Market State component: premium-weighted aggressor-classified imbalance.

The legacy ``smart_*`` fields measure premium attributed to the side crossing
the quoted spread. They do not identify opening versus closing, ultimate owner,
dealer/customer status, multi-leg intent, or information advantage. This is a
directional house heuristic lifted into the MSI composite, not proof of
"smart money" or a calibrated leading relationship.

Score convention:
  * +1.0 — aggressor-classified call premium dominates (bullish model output)
  *  0.0 — balanced flow / insufficient activity
  * -1.0 — aggressor-classified put premium dominates (bearish model output)

Inputs (from MarketContext):
  * ``smart_call`` — net premium dollars bought on calls in the rolling
    ingestion window (signed, > 0 = buy).
  * ``smart_put``  — net premium dollars bought on puts.

Both fields are populated by ``unified_signal_engine._fetch_market_context``
and reflect the most recent ingestion window of Lee-Ready classified flow.
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext

# Minimum total premium (in $ / window) before the component will return
# a non-zero score.  Below this we treat the read as noise and abstain.
_MIN_TOTAL_PREMIUM = float(os.getenv("SIGNAL_ORDER_FLOW_MIN_PREMIUM", "100000"))

# Imbalance ratio ((call - put) / |call| + |put|) at which the score
# saturates to ±1.0.  0.50 means a 75/25 call-vs-put split saturates.
_SATURATION_RATIO = float(os.getenv("SIGNAL_ORDER_FLOW_SATURATION", "0.50"))


class OrderFlowImbalanceComponent(ComponentBase):
    name = "order_flow_imbalance"
    weight = 0.13  # See COMPONENT_POINTS in scoring_engine for actual MSI points.

    def compute(self, ctx: MarketContext) -> float:
        call_flow = float(ctx.smart_call or 0.0)
        put_flow = float(ctx.smart_put or 0.0)
        gross = abs(call_flow) + abs(put_flow)
        if gross <= 0:
            return 0.0
        # Net premium divided by gross gives the directional skew of
        # *paid-for* flow.  + = call-side aggression, - = put-side.
        ratio = (call_flow - put_flow) / gross
        if _SATURATION_RATIO <= 0:
            score = max(-1.0, min(1.0, ratio))
        else:
            score = max(-1.0, min(1.0, ratio / _SATURATION_RATIO))
        # Soft confidence ramp instead of a hard cutoff: thin-flow reads
        # damp toward zero rather than snapping to it, so the score keeps
        # information content across the full intraday spectrum.
        confidence = min(1.0, gross / _MIN_TOTAL_PREMIUM) if _MIN_TOTAL_PREMIUM > 0 else 1.0
        return score * confidence

    def context_values(self, ctx: MarketContext) -> dict:
        call_flow = float(ctx.smart_call or 0.0)
        put_flow = float(ctx.smart_put or 0.0)
        gross = abs(call_flow) + abs(put_flow)
        ratio = (call_flow - put_flow) / gross if gross > 0 else 0.0
        return {
            "smart_call_premium": round(call_flow, 2),
            "smart_put_premium": round(put_flow, 2),
            "gross_premium": round(gross, 2),
            "imbalance_ratio": round(ratio, 6),
            "saturation_ratio": _SATURATION_RATIO,
            "min_total_premium": _MIN_TOTAL_PREMIUM,
            "score": round(self.compute(ctx), 6),
        }
