"""Signed tape flow bias scoring component.

The ingestion layer already Lee-Ready-classifies every option print into
``buy_volume``/``sell_volume`` and ``buy_premium``/``sell_premium`` — but
nothing in the scoring stack consumes them. This component fixes that.

We look at the short-window premium imbalance split by option type:
  * Aggressive call buying + passive put selling => bullish.
  * Aggressive put buying + passive call selling => bearish.

Unlike ``smart_money`` (which looks at one-shot "smart" premium events),
this component watches the *continuous* order-flow tape, which gives a
much earlier read on directional conviction.

Inputs live in ``ctx.extra['flow_by_type']`` — a list with at most two
rows (one per option_type) holding ``buy_volume``, ``sell_volume``,
``buy_premium``, ``sell_premium`` aggregated over the last window.
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext

# Minimum gross premium to emit a full-confidence signal ($). Below this the
# read tapers toward zero via the confidence ramp.
_MIN_TOTAL_PREMIUM = float(os.getenv("SIGNAL_TAPE_FLOW_MIN_PREMIUM", "2.5e5"))

# Gross-normalized directional ratio at which the score saturates to ±1. A
# value of 0.5 means the net directional premium has to reach half of the
# *gross* premium traded before the score pins to the extreme — so a two-way
# tape with only a modest net lean no longer saturates. (Matches the
# order_flow_imbalance component's saturation for consistency.)
_IMBALANCE_SATURATION = float(os.getenv("SIGNAL_TAPE_FLOW_SATURATION", "0.5"))


class TapeFlowBiasComponent(ComponentBase):
    name = "tape_flow_bias"
    weight = 0.08

    def compute(self, ctx: MarketContext) -> float:
        agg = self._aggregate(ctx)
        if agg is None:
            return 0.0
        call_net = agg["call_net_premium"]
        put_net = agg["put_net_premium"]
        # Normalize by GROSS premium traded (buy + sell across both option
        # types), not by the sum of the two *net* figures. Net-sum
        # normalization made the ratio hypersensitive — a small, opposite-
        # signed net on each side (e.g. calls net -$10k / puts net +$10k) drove
        # |ratio| to 1.0 even on an otherwise quiet, two-way tape, pinning the
        # score to ±100. Dividing by gross keeps the ratio in [-1, +1] but only
        # approaches the extremes when the tape is genuinely one-sided, so a
        # heavy two-way session reads proportionally instead of saturating.
        gross = (
            agg["call_buy_premium"]
            + agg["call_sell_premium"]
            + agg["put_buy_premium"]
            + agg["put_sell_premium"]
        )
        if gross <= 0:
            return 0.0

        # Call buying / put selling = bullish; put buying / call selling =
        # bearish. Net of the two, scaled by total premium traded.
        directional = call_net - put_net
        ratio = directional / gross  # bounded to [-1, +1]
        # Saturate before clamping so mild imbalances don't dominate.
        score = max(-1.0, min(1.0, ratio / _IMBALANCE_SATURATION))
        # Confidence damping rather than a hard cutoff — thin-flow reads
        # taper toward zero smoothly so the score occupies the full range.
        confidence = min(1.0, gross / _MIN_TOTAL_PREMIUM) if _MIN_TOTAL_PREMIUM > 0 else 1.0
        return score * confidence  # type: ignore[no-any-return]

    def context_values(self, ctx: MarketContext) -> dict:
        agg = self._aggregate(ctx)
        if agg is None:
            return {
                "call_net_premium": None,
                "put_net_premium": None,
                "gross_premium": None,
                "imbalance_ratio": None,
                "source": "unavailable",
            }
        gross = (
            agg["call_buy_premium"]
            + agg["call_sell_premium"]
            + agg["put_buy_premium"]
            + agg["put_sell_premium"]
        )
        directional = agg["call_net_premium"] - agg["put_net_premium"]
        ratio = directional / gross if gross > 0 else 0.0
        return {
            "call_net_premium": round(agg["call_net_premium"], 2),
            "put_net_premium": round(agg["put_net_premium"], 2),
            "call_buy_premium": round(agg["call_buy_premium"], 2),
            "call_sell_premium": round(agg["call_sell_premium"], 2),
            "put_buy_premium": round(agg["put_buy_premium"], 2),
            "put_sell_premium": round(agg["put_sell_premium"], 2),
            "gross_premium": round(gross, 2),
            "imbalance_ratio": round(ratio, 6),
            "source": "flow_by_type",
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _aggregate(ctx: MarketContext) -> dict | None:
        rows = ctx.extra.get("flow_by_type") if ctx.extra else None
        if not rows:
            return None
        out = {
            "call_net_premium": 0.0,
            "put_net_premium": 0.0,
            "call_buy_premium": 0.0,
            "call_sell_premium": 0.0,
            "put_buy_premium": 0.0,
            "put_sell_premium": 0.0,
        }
        seen = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            opt = str(row.get("option_type") or "").upper()
            try:
                bp = float(row.get("buy_premium") or 0.0)
                sp = float(row.get("sell_premium") or 0.0)
            except (TypeError, ValueError):
                continue
            net = bp - sp
            if opt == "C":
                out["call_net_premium"] += net
                out["call_buy_premium"] += bp
                out["call_sell_premium"] += sp
                seen = True
            elif opt == "P":
                out["put_net_premium"] += net
                out["put_buy_premium"] += bp
                out["put_sell_premium"] += sp
                seen = True
        return out if seen else None
