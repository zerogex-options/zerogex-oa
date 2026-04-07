"""Opportunity quality scoring component -- optimizer feedback loop.

This component is signed [-1.0, +1.0]:
  Positive score = good structure available for the signaled direction
  Negative score = poor structure (wide spreads, low OI, negative EV)
                   actively reduces composite confidence
  Zero = no optimizer output available this cycle (neutral, does not penalize)
"""
from __future__ import annotations

from typing import Optional

from src.signals.components.base import ComponentBase, MarketContext


class OpportunityQualityComponent(ComponentBase):
    name = "opportunity_quality"
    weight = 0.16

    def __init__(self, underlying: str):
        self.underlying = underlying
        self._optimizer: Optional[object] = None
        self._last_result: Optional[float] = None  # cache last valid score
        # Cache option rows from the latest cycle so the portfolio engine's
        # _select_optimizer_candidate can reuse them instead of running the
        # same expensive fetch_option_snapshot query a second time.
        self._cached_option_rows: Optional[list] = None
        self._cached_option_rows_key: Optional[tuple] = None  # (timestamp, dte_min, dte_max)

    def _get_optimizer(self):
        if self._optimizer is None:
            from src.signals.position_optimizer_engine import PositionOptimizerEngine
            self._optimizer = PositionOptimizerEngine(self.underlying)
        return self._optimizer

    def compute(self, ctx: MarketContext) -> float:
        # Requires a direction signal -- neutral market has no structure to evaluate
        if ctx.net_gex == 0 and ctx.smart_call == 0 and ctx.smart_put == 0:
            return 0.0

        try:
            from src.signals.position_optimizer_engine import (
                PositionOptimizerContext,
                fetch_option_snapshot,
            )
            from src.database import db_connection

            with db_connection() as conn:
                # Infer timeframe from iv_rank as a proxy for urgency
                timeframe = "intraday" if (ctx.iv_rank or 0) > 0.7 else "swing"
                dte_ranges = {"intraday": (0, 2), "swing": (1, 7), "multi_day": (3, 14)}
                dte_min, dte_max = dte_ranges[timeframe]
                trade_date = ctx.timestamp.date()
                option_rows = fetch_option_snapshot(conn, ctx.underlying, ctx.timestamp, trade_date, dte_min, dte_max)
                # Cache for reuse by portfolio engine
                self._cached_option_rows = option_rows
                self._cached_option_rows_key = (ctx.timestamp, dte_min, dte_max)
                if not option_rows:
                    return self._last_result if self._last_result is not None else 0.0

                # Infer direction from net smart money flow + GEX regime
                sm_ratio = (ctx.smart_call + 1.0) / (ctx.smart_put + 1.0)
                if ctx.net_gex >= 0 and sm_ratio >= 1.0:
                    inferred_direction = "bullish"
                elif ctx.net_gex < 0 and sm_ratio < 1.0:
                    inferred_direction = "bearish"
                else:
                    inferred_direction = "neutral"

                optimizer_ctx = PositionOptimizerContext(
                    timestamp=ctx.timestamp,
                    signal_timestamp=ctx.timestamp,
                    signal_timeframe=timeframe,
                    signal_direction=inferred_direction,
                    signal_strength="medium",
                    trade_type="trend_follow",
                    current_price=ctx.close,
                    net_gex=ctx.net_gex,
                    gamma_flip=ctx.gamma_flip,
                    put_call_ratio=ctx.put_call_ratio,
                    max_pain=ctx.max_pain,
                    smart_call_premium=ctx.smart_call,
                    smart_put_premium=ctx.smart_put,
                    dealer_net_delta=ctx.dealer_net_delta,
                    target_dte_min=dte_min,
                    target_dte_max=dte_max,
                    option_rows=option_rows,
                )
                optimizer = self._get_optimizer()
                candidates = optimizer._generate_candidates(optimizer_ctx)
                if not candidates:
                    self._last_result = -0.3  # No viable structure = mild negative signal
                    return self._last_result

                best = candidates[0]
                # Map optimizer outputs to [-1, +1]:
                # POP: 0.5 -> 0.0, 0.7 -> +0.4, 0.3 -> -0.4
                pop_score = (best.probability_of_profit - 0.5) * 2.0
                # EV as fraction of max loss: >0 = positive, <0 = negative, clamp to [-1, +1]
                ev_score = max(-1.0, min(1.0, best.expected_value / max(best.max_loss, 1.0)))
                # Liquidity: [0,1] -> [-0.5, +0.5] (liquidity can hurt but not dominate)
                liq_score = (best.liquidity_score - 0.5)
                # Sharpe-like: clamp to [-1, +1]
                sharpe_score = max(-1.0, min(1.0, best.sharpe_like_ratio))

                raw = (
                    0.35 * pop_score +
                    0.30 * ev_score +
                    0.20 * liq_score +
                    0.15 * sharpe_score
                )
                result = max(-1.0, min(1.0, raw))
                self._last_result = result
                return result
        except Exception as exc:
            from src.utils import get_logger
            get_logger(__name__).warning("OpportunityQualityComponent failed: %s", exc)
            return self._last_result if self._last_result is not None else 0.0

    def context_values(self, ctx: MarketContext) -> dict:
        return {"last_result": self._last_result}
