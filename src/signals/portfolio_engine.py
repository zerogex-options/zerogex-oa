"""
Layer 2 of the ZeroGEX Signal Engine.

PortfolioEngine reads the current composite ScoreSnapshot and the current
state of open signal_trades, computes the optimal target portfolio, and
executes the minimum set of transactions to reach that target.

Key design principles:
- 100% cash is a first-class target state, not a fallback.
- Decisions are made holistically (what should the full portfolio look like?)
  not reactively (did this individual trade hit a stop?).
- Each transaction is recorded independently to signal_trades.
- Every reconciliation cycle is snapshotted to portfolio_snapshots.
"""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from src.config import (
    SIGNALS_DRS_CALL_ENTRY_MIN,
    SIGNALS_DRS_HARD_GATES_ENABLED,
    SIGNALS_DRS_OVERRIDE_ENABLED,
    SIGNALS_DRS_OVERRIDE_THRESHOLD,
    SIGNALS_DRS_PUT_ENTRY_MAX,
    SIGNALS_MAX_OPEN_TRADES,
    SIGNALS_MAX_PORTFOLIO_HEAT_PCT,
    SIGNALS_PORTFOLIO_SIZE,
    SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES,
    SIGNALS_SCALP_SIZE_MULTIPLIER,
    SIGNALS_SCALP_TRIGGER_ENABLED,
    SIGNALS_SCALP_TRIGGER_THRESHOLD,
    SIGNALS_TREND_CONFIRMATION_BARS,
    SIGNALS_TREND_CONFIRMATION_MIN_MATCH,
    SIGNALS_TRIGGER_THRESHOLD,
)
from src.database import db_connection
from src.signals.position_optimizer_engine import (
    PositionOptimizerContext,
    PositionOptimizerEngine,
    fetch_option_snapshot,
)
from src.signals.scoring_engine import ScoreSnapshot
from src.symbols import get_canonical_symbol
from src.utils import get_logger
from src.validation import ET, NYSE_HOLIDAYS

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class TargetPosition:
    direction: str              # 'bullish', 'bearish', 'neutral'
    strategy_type: str          # from optimizer, or 'cash' if no position
    contracts: int              # 0 = full cash
    option_symbol: str
    option_type: str
    expiration: date
    strike: float
    entry_mark: float
    probability_of_profit: float
    expected_value: float
    kelly_fraction: float
    optimizer_payload: dict


@dataclass
class PortfolioTarget:
    underlying: str
    timestamp: datetime
    composite_score: float
    normalized_score: float
    direction: str
    target_positions: list[TargetPosition]  # empty = 100% cash
    total_target_contracts: int
    target_heat_pct: float
    rationale: str  # human-readable explanation of why this target was chosen


# ---------------------------------------------------------------------------
# PortfolioEngine
# ---------------------------------------------------------------------------

class PortfolioEngine:
    def __init__(self, underlying: str):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        self.position_optimizer = PositionOptimizerEngine(self.underlying)

        # Config constants
        self.trigger_threshold = SIGNALS_TRIGGER_THRESHOLD
        self.max_open_trades = SIGNALS_MAX_OPEN_TRADES
        self.max_heat_pct = SIGNALS_MAX_PORTFOLIO_HEAT_PCT
        self.cooldown_minutes = SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES
        self.trend_confirmation_bars = SIGNALS_TREND_CONFIRMATION_BARS
        self.trend_confirmation_min_match = SIGNALS_TREND_CONFIRMATION_MIN_MATCH
        self.drs_hard_gates_enabled = SIGNALS_DRS_HARD_GATES_ENABLED
        self.drs_call_entry_min = SIGNALS_DRS_CALL_ENTRY_MIN
        self.drs_put_entry_max = SIGNALS_DRS_PUT_ENTRY_MAX
        self.drs_override_enabled = SIGNALS_DRS_OVERRIDE_ENABLED
        self.drs_override_threshold = SIGNALS_DRS_OVERRIDE_THRESHOLD
        self.scalp_trigger_enabled = SIGNALS_SCALP_TRIGGER_ENABLED
        self.scalp_trigger_threshold = SIGNALS_SCALP_TRIGGER_THRESHOLD
        self.scalp_size_multiplier = SIGNALS_SCALP_SIZE_MULTIPLIER

    @staticmethod
    def _market_status(dt: Optional[datetime] = None) -> str:
        """Return OPEN only during regular options trading hours (09:30-16:15 ET)."""
        if dt is None:
            dt = datetime.now(ET)
        elif dt.tzinfo is None:
            dt = ET.localize(dt)
        else:
            dt = dt.astimezone(ET)

        if dt.weekday() > 4 or dt.date() in NYSE_HOLIDAYS:
            return "CLOSED"

        current_time = dt.time()
        market_open = datetime.strptime("09:30:00", "%H:%M:%S").time()
        market_close = datetime.strptime("16:15:00", "%H:%M:%S").time()
        return "OPEN" if market_open <= current_time <= market_close else "CLOSED"

    # ------------------------------------------------------------------
    # Connection helper
    # ------------------------------------------------------------------

    @staticmethod
    @contextmanager
    def _use_conn(conn=None):
        if conn is not None:
            yield conn
        else:
            with db_connection() as new_conn:
                yield new_conn

    # ------------------------------------------------------------------
    # compute_target — pure function of score + market context
    # ------------------------------------------------------------------

    def compute_target(
        self, score: ScoreSnapshot, market_ctx: dict, conn=None, cached_option_rows=None
    ) -> PortfolioTarget:
        """Return the optimal portfolio state given the current score.

        This method never reads signal_trades — it is purely a function of
        the score and market context.  All trade-awareness lives in reconcile().
        """
        threshold = self.trigger_threshold
        scalp_threshold = (
            self.scalp_trigger_threshold if self.scalp_trigger_enabled else threshold
        )

        # --- CASE 1: neutral or below *scalp* threshold → 100% cash ---
        # The scalp threshold is the lowest gate we care about; anything
        # below it is pure cash.
        if score.direction == "neutral" or score.normalized_score < scalp_threshold:
            return self._cash_target(
                score,
                f"Score {score.normalized_score:.3f} below scalp threshold "
                f"{scalp_threshold:.3f} / direction {score.direction}",
            )

        # --- Dynamic threshold adjustment based on IV rank ---
        # Note: IV-rank adjustment only affects the *full-size* threshold.
        # The scalp threshold is a floor and is not tightened in high IV --
        # scalps are smaller and explicitly meant to be high-frequency.
        iv_rank = market_ctx.get("iv_rank")
        effective_threshold = threshold
        if iv_rank is not None:
            if iv_rank > 0.70:
                effective_threshold = min(0.72, threshold + 0.07)
            elif iv_rank < 0.25:
                effective_threshold = max(0.52, threshold - 0.04)

        # Classify the tier. The scalp tier runs with reduced sizing and
        # skips the DRS hard-entry gates (which otherwise block reversals).
        is_scalp = (
            self.scalp_trigger_enabled
            and score.normalized_score < effective_threshold
            and score.normalized_score >= scalp_threshold
        )
        tier_label = "scalp" if is_scalp else "full"
        size_multiplier = self.scalp_size_multiplier if is_scalp else 1.0

        # --- CASE 2: trend confirmation ---
        # Scalps intentionally skip trend confirmation -- the whole point is
        # to catch fast reversals and mean-reversion, not to require the
        # preceding bars to agree.
        if not is_scalp and not self._score_trend_confirmation(
            score.direction, score.timestamp, conn=conn
        ):
            return self._cash_target(
                score,
                f"Trend confirmation failed: recent history contradicts {score.direction}",
            )

        # --- CASE 2B: Dealer Regime hard-entry gates ---
        # Strong-conviction composite overrides the DRS positional gate so
        # reversal setups on days already past the flip can still fire.
        drs_override_active = (
            self.drs_override_enabled
            and not is_scalp
            and score.normalized_score >= self.drs_override_threshold
        )
        if is_scalp or drs_override_active:
            passes_drs_gate, drs_reason = True, (
                "DRS hard gate bypassed: scalp tier"
                if is_scalp
                else f"DRS hard gate bypassed: conviction {score.normalized_score:.3f} "
                f">= override {self.drs_override_threshold:.2f}"
            )
        else:
            passes_drs_gate, drs_reason = self._passes_dealer_regime_gates(
                score, market_ctx
            )
        if not passes_drs_gate:
            return self._cash_target(score, drs_reason)

        # --- CASE 3: position optimizer candidate ---
        # Scalp tier forces the intraday DTE window (0-2) regardless of the
        # score-band mapping, because the whole point of a scalp is to hold
        # for minutes-to-hours, not days.
        forced_timeframe = "intraday" if is_scalp else None
        candidate_result = self._select_optimizer_candidate(
            score,
            market_ctx,
            conn=conn,
            cached_option_rows=cached_option_rows,
            forced_timeframe=forced_timeframe,
        )
        if not candidate_result:
            return self._cash_target(
                score,
                "No positive-EV structure available",
            )

        candidate = candidate_result["candidate"]
        sizing = next(
            (p for p in candidate.sizing_profiles if p.profile == "optimal"), None
        )
        if not sizing or sizing.contracts <= 0:
            return self._cash_target(
                score,
                "No positive-EV structure available",
            )

        # --- CASE 4: compute target contracts via Kelly sizing ---
        base_contracts = sizing.contracts
        contracts = max(1, int(base_contracts * score.normalized_score * size_multiplier))

        entry_price = (candidate.entry_debit or candidate.entry_credit) / 100.0
        opt_type = "C" if score.direction == "bullish" else "P"

        # Resolve option symbol for the primary leg
        legs = self._legs_from_candidate(
            {
                "strategy_type": candidate.strategy_type,
                "strikes": candidate.strikes,
                "expiry": candidate.expiry,
            }
        )
        enriched_legs = []
        with self._use_conn(conn) as c:
            for leg in legs:
                option_symbol = self._resolve_option_symbol_for_leg(
                    score.timestamp, leg, conn=c
                )
                enriched_legs.append({**leg, "option_symbol": option_symbol})

        primary_symbol = (
            enriched_legs[0]["option_symbol"]
            if enriched_legs
            else f"{self.db_symbol}-SYNTHETIC"
        )

        optimizer_payload = {
            "strategy_type": candidate.strategy_type,
            "pricing_mode": "debit" if candidate.entry_debit > 0 else "credit",
            "strikes": candidate.strikes,
            "expiry": str(candidate.expiry),
            "legs": enriched_legs,
            "probability_of_profit": candidate.probability_of_profit,
            "expected_value": candidate.expected_value,
            "signal_timeframe": candidate_result["signal_timeframe"],
            "signal_strength": candidate_result["signal_strength"],
            "tier": tier_label,
            "size_multiplier": size_multiplier,
        }

        target_heat = (
            abs(entry_price) * contracts * 100 / max(SIGNALS_PORTFOLIO_SIZE, 1.0)
        )

        tp = TargetPosition(
            direction=score.direction,
            strategy_type=candidate.strategy_type,
            contracts=contracts,
            option_symbol=primary_symbol,
            option_type=opt_type,
            expiration=candidate.expiry,
            strike=(
                round(float(enriched_legs[0]["strike"]), 4)
                if enriched_legs
                else round(float(market_ctx["close"]), 4)
            ),
            entry_mark=entry_price,
            probability_of_profit=candidate.probability_of_profit,
            expected_value=candidate.expected_value,
            kelly_fraction=candidate.kelly_fraction,
            optimizer_payload=optimizer_payload,
        )

        rationale = (
            f"Score {score.normalized_score:.3f} {score.direction} [{tier_label}], "
            f"{candidate.strategy_type} {contracts}c Kelly={candidate.kelly_fraction:.1%}"
            + (" (DRS override)" if drs_override_active else "")
        )

        return PortfolioTarget(
            underlying=self.db_symbol,
            timestamp=score.timestamp,
            composite_score=score.composite_score,
            normalized_score=score.normalized_score,
            direction=score.direction,
            target_positions=[tp],
            total_target_contracts=contracts,
            target_heat_pct=round(target_heat, 6),
            rationale=rationale,
        )

    # ------------------------------------------------------------------
    # reconcile — reads actual state, computes delta, executes
    # ------------------------------------------------------------------

    def reconcile(self, target: PortfolioTarget, conn=None) -> str:
        """Compare target portfolio to actual holdings and execute the delta.

        Returns an action string for logging.
        """
        with self._use_conn(conn) as c:
            open_trades = self._fetch_open_trades(c)
            actual_contracts = sum(t["quantity_open"] for t in open_trades)
            actual_direction = self._majority_direction(open_trades)
            open_trade_count = len(open_trades)

            market_status = self._market_status()
            if market_status != "OPEN":
                for trade in open_trades:
                    self._update_trade_mark(trade, target.timestamp, c)
                action = "held_market_closed"
                action_detail = {
                    "market_status": market_status,
                    "reason": "Signal trades are only allowed during OPEN market status (09:30-16:15 ET).",
                    "target_direction": target.direction,
                    "target_contracts": target.total_target_contracts,
                    "actual_contracts": actual_contracts,
                    "actual_direction": actual_direction,
                }
                self.snapshot(target, action, action_detail, open_trades, c)
                return action

            target_contracts = target.total_target_contracts
            target_direction = target.direction if target.target_positions else "neutral"

            action = "held"
            action_detail: dict = {}

            # CASE A: target is 100% cash
            if not target.target_positions:
                if open_trades:
                    closed_pnl = 0.0
                    for trade in open_trades:
                        pnl = self._close_trade(trade, target.timestamp, c)
                        closed_pnl += pnl
                    action = "closed_all"
                    action_detail = {
                        "closed_count": len(open_trades),
                        "realized_pnl": round(closed_pnl, 4),
                        "reason": target.rationale,
                    }
                else:
                    action = "cash"
                    action_detail = {"reason": target.rationale}

            # CASE B: direction reversal
            elif actual_contracts > 0 and actual_direction != target_direction:
                closed_pnl = 0.0
                for trade in open_trades:
                    pnl = self._close_trade(trade, target.timestamp, c)
                    closed_pnl += pnl
                if target_contracts > 0:
                    self._open_position(target.target_positions[0], target, c)
                action = "reversed"
                action_detail = {
                    "old_direction": actual_direction,
                    "new_direction": target_direction,
                    "closed_contracts": actual_contracts,
                    "opened_contracts": target_contracts,
                    "closed_pnl": round(closed_pnl, 4),
                }

            # CASE C: same direction, adjust size
            elif actual_contracts > 0 and actual_direction == target_direction:
                contracts_delta = target_contracts - actual_contracts
                if contracts_delta > 0:
                    if open_trade_count >= self.max_open_trades:
                        action = "held_max_open_trades"
                        action_detail = {
                            "reason": "Max open trade slots reached",
                            "max_open_trades": self.max_open_trades,
                            "open_trade_count": open_trade_count,
                            "target_contracts": target_contracts,
                            "actual_contracts": actual_contracts,
                        }
                    else:
                        self._open_position(target.target_positions[0], target, c)
                        action = "added"
                        action_detail = {
                            "added_contracts": contracts_delta,
                            "new_total": target_contracts,
                        }
                elif contracts_delta < 0:
                    # Partially close oldest trades first
                    to_close = abs(contracts_delta)
                    closed_pnl = 0.0
                    for trade in open_trades:
                        if to_close <= 0:
                            break
                        pnl = self._close_trade(trade, target.timestamp, c)
                        closed_pnl += pnl
                        to_close -= trade["quantity_open"]
                    action = "trimmed"
                    action_detail = {
                        "trimmed_contracts": abs(contracts_delta),
                        "new_total": target_contracts,
                        "realized_pnl": round(closed_pnl, 4),
                    }
                else:
                    # Hold — just update marks
                    for trade in open_trades:
                        self._update_trade_mark(trade, target.timestamp, c)
                    action = "held"
                    action_detail = {
                        "contracts": actual_contracts,
                        "direction": actual_direction,
                    }

            # CASE D: no open trades, target has position
            elif actual_contracts == 0 and target.target_positions:
                if open_trade_count >= self.max_open_trades:
                    action = "held_max_open_trades"
                    action_detail = {
                        "reason": "Max open trade slots reached",
                        "max_open_trades": self.max_open_trades,
                        "open_trade_count": open_trade_count,
                        "target_contracts": target_contracts,
                    }
                else:
                    self._open_position(target.target_positions[0], target, c)
                    action = "opened"
                    action_detail = {
                        "contracts": target_contracts,
                        "direction": target_direction,
                        "strategy": target.target_positions[0].strategy_type,
                    }

            # Re-fetch to get accurate state for snapshot
            open_trades_after = self._fetch_open_trades(c)
            self.snapshot(target, action, action_detail, open_trades_after, c)

            return action

    # ------------------------------------------------------------------
    # _close_trade
    # ------------------------------------------------------------------

    def _close_trade(self, trade: dict, as_of: datetime, conn) -> float:
        """Close an open trade at current mark. Returns realized PnL."""
        mark, pricing_mode = self._spread_mark(trade, as_of, conn)
        if mark is None:
            mark = trade["current_price"]

        entry = trade["entry_price"]
        qty = trade["quantity_open"]
        realized_pnl = self._spread_pnl(entry, mark, qty, pricing_mode)

        cur = conn.cursor()
        cur.execute(
            """
            UPDATE signal_trades
            SET status = 'closed',
                closed_at = %s,
                current_price = %s,
                quantity_open = 0,
                realized_pnl = realized_pnl + %s,
                unrealized_pnl = 0,
                total_pnl = realized_pnl + %s,
                pnl_percent = CASE
                    WHEN entry_price > 0
                    THEN ROUND(((realized_pnl + %s) / (entry_price * quantity_initial * 100)) * 100, 4)
                    ELSE 0
                END,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'open'
            """,
            (as_of, mark, realized_pnl, realized_pnl, realized_pnl, trade["id"]),
        )
        conn.commit()
        return realized_pnl

    # ------------------------------------------------------------------
    # _open_position
    # ------------------------------------------------------------------

    def _open_position(
        self, tp: TargetPosition, target: PortfolioTarget, conn
    ) -> bool:
        """Insert a new row to signal_trades. Returns True if inserted."""
        components_at_entry = dict(target.composite_score if isinstance(target.composite_score, dict) else {})
        components_at_entry = {"optimizer": tp.optimizer_payload}

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO signal_trades (
                underlying, signal_timestamp, opened_at, updated_at, status,
                direction, score_at_entry, option_symbol, option_type, expiration, strike,
                entry_price, current_price, quantity_initial, quantity_open,
                realized_pnl, unrealized_pnl, total_pnl, pnl_percent,
                components_at_entry
            ) VALUES (
                %s, %s, %s, NOW(), 'open',
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                0, 0, 0, 0,
                %s::jsonb
            )
            """,
            (
                self.db_symbol,
                target.timestamp,
                target.timestamp,
                tp.direction,
                target.composite_score,
                tp.option_symbol,
                tp.option_type,
                tp.expiration,
                tp.strike,
                tp.entry_mark,
                tp.entry_mark,
                tp.contracts,
                tp.contracts,
                json.dumps(components_at_entry, default=str),
            ),
        )
        conn.commit()
        return cur.rowcount > 0

    # ------------------------------------------------------------------
    # _update_trade_mark
    # ------------------------------------------------------------------

    def _update_trade_mark(self, trade: dict, as_of: datetime, conn) -> None:
        """Refresh unrealized PnL for an open trade."""
        mark, pricing_mode = self._spread_mark(trade, as_of, conn)
        if mark is None:
            return

        entry = trade["entry_price"]
        qty = trade["quantity_open"]
        unrealized = self._spread_pnl(entry, mark, qty, pricing_mode)
        realized = trade["realized_pnl"]
        total = realized + unrealized
        basis_qty = max(trade["quantity_initial"], 1)
        pnl_pct = ((total / (entry * basis_qty * 100)) * 100) if entry > 0 else 0

        cur = conn.cursor()
        cur.execute(
            """
            UPDATE signal_trades
            SET current_price = %s,
                unrealized_pnl = %s,
                total_pnl = %s,
                pnl_percent = %s,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'open'
            """,
            (mark, round(unrealized, 4), round(total, 4), round(pnl_pct, 4), trade["id"]),
        )
        conn.commit()

    # ------------------------------------------------------------------
    # snapshot — written every cycle regardless of action
    # ------------------------------------------------------------------

    def snapshot(
        self,
        target: PortfolioTarget,
        action: str,
        action_detail: dict,
        open_trades: list[dict],
        conn,
    ) -> None:
        """Insert a portfolio_snapshots row after each reconciliation cycle."""
        actual_contracts = sum(t["quantity_open"] for t in open_trades)
        actual_direction = self._majority_direction(open_trades) if open_trades else "neutral"
        target_strategy = (
            target.target_positions[0].strategy_type
            if target.target_positions
            else None
        )
        heat = (
            sum(abs(t["entry_price"]) * t["quantity_open"] * 100 for t in open_trades)
            / max(SIGNALS_PORTFOLIO_SIZE, 1.0)
        )

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO portfolio_snapshots (
                underlying, timestamp,
                composite_score, normalized_score, direction,
                target_contracts, target_direction, target_strategy,
                actual_contracts, actual_direction,
                heat_pct, action_taken, action_detail
            ) VALUES (
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s::jsonb
            )
            ON CONFLICT (underlying, timestamp) DO UPDATE SET
                composite_score = EXCLUDED.composite_score,
                normalized_score = EXCLUDED.normalized_score,
                direction = EXCLUDED.direction,
                target_contracts = EXCLUDED.target_contracts,
                target_direction = EXCLUDED.target_direction,
                target_strategy = EXCLUDED.target_strategy,
                actual_contracts = EXCLUDED.actual_contracts,
                actual_direction = EXCLUDED.actual_direction,
                heat_pct = EXCLUDED.heat_pct,
                action_taken = EXCLUDED.action_taken,
                action_detail = EXCLUDED.action_detail
            """,
            (
                self.db_symbol,
                target.timestamp,
                target.composite_score,
                target.normalized_score,
                target.direction,
                target.total_target_contracts,
                target.direction if target.target_positions else "neutral",
                target_strategy,
                actual_contracts,
                actual_direction,
                round(heat, 6),
                action,
                json.dumps(action_detail, default=str),
            ),
        )
        conn.commit()

    # ------------------------------------------------------------------
    # Internal helpers (ported from UnifiedSignalEngine)
    # ------------------------------------------------------------------

    def _fetch_open_trades(self, conn) -> list[dict]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, option_symbol, entry_price, current_price, quantity_open,
                   quantity_initial, status, direction, realized_pnl, components_at_entry
            FROM signal_trades
            WHERE underlying = %s
              AND status = 'open'
            ORDER BY opened_at ASC
            """,
            (self.db_symbol,),
        )
        return [
            {
                "id": r[0],
                "option_symbol": r[1],
                "entry_price": float(r[2]),
                "current_price": float(r[3] or r[2]),
                "quantity_open": int(r[4]),
                "quantity_initial": int(r[5]),
                "status": r[6],
                "direction": r[7],
                "realized_pnl": float(r[8] or 0.0),
                "components_at_entry": r[9] or {},
            }
            for r in cur.fetchall()
        ]

    def _latest_option_mark(
        self, option_symbol: str, as_of: datetime, conn
    ) -> Optional[float]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(mid, (bid + ask)/2.0, last)
            FROM option_chains
            WHERE option_symbol = %s
              AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (option_symbol, as_of),
        )
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None

    def _spread_mark(
        self, trade: dict, as_of: datetime, conn
    ) -> tuple[Optional[float], str]:
        """Mark the spread using every leg, matching how entry debit/credit was formed.

        Entry debit (bull_call_debit, bear_put_debit) was computed in the optimizer
        as ``long_mid - short_mid``. Entry credit (bull_put_credit, bear_call_credit,
        iron_condor) was computed as ``short_mid - long_mid``. We mirror that here
        so the current per-share value is directly comparable to ``entry_price``.

        Returns (value_per_share, pricing_mode). ``pricing_mode`` is "debit" or
        "credit"; on missing/partial chain data returns ``(None, pricing_mode)``.
        """
        payload = (trade.get("components_at_entry") or {}).get("optimizer") or {}
        legs = payload.get("legs") or []
        pricing_mode = payload.get("pricing_mode") or "debit"

        if not legs:
            mark = self._latest_option_mark(trade["option_symbol"], as_of, conn)
            return mark, pricing_mode

        long_sum = 0.0
        short_sum = 0.0
        for leg in legs:
            symbol = leg.get("option_symbol")
            if not symbol:
                return None, pricing_mode
            mid = self._latest_option_mark(symbol, as_of, conn)
            if mid is None:
                return None, pricing_mode
            side = str(leg.get("side") or "").lower()
            if side == "long":
                long_sum += mid
            elif side == "short":
                short_sum += mid
            else:
                return None, pricing_mode

        if pricing_mode == "credit":
            return short_sum - long_sum, pricing_mode
        return long_sum - short_sum, pricing_mode

    @staticmethod
    def _spread_pnl(entry: float, mark: float, qty: int, pricing_mode: str) -> float:
        """Signed dollar P&L for the spread at ``mark`` vs ``entry`` per-share.

        Debit spreads are held long: value up => profit. Credit spreads are held
        short: cost-to-close down => profit, so the sign inverts.
        """
        if pricing_mode == "credit":
            per_share = entry - mark
        else:
            per_share = mark - entry
        return per_share * qty * 100

    def _score_trend_confirmation(
        self, direction: str, as_of: datetime, conn=None
    ) -> bool:
        """Confirm recent signal_scores history agrees with current direction."""
        try:
            with self._use_conn(conn) as c:
                cur = c.cursor()
                lookback = self.trend_confirmation_bars
                if lookback <= 0:
                    return True

                cur.execute(
                    """
                    SELECT direction
                    FROM signal_scores
                    WHERE underlying = %s
                      AND timestamp < %s
                      AND direction != 'neutral'
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    (self.db_symbol, as_of, lookback),
                )
                rows = cur.fetchall()
                if not rows:
                    return True
                matching = sum(1 for r in rows if r[0] == direction)
                min_match = min(self.trend_confirmation_min_match, len(rows))
                return matching >= min_match
        except Exception:
            return True

    def _passes_dealer_regime_gates(self, score: ScoreSnapshot, market_ctx: dict) -> tuple[bool, str]:
        if not self.drs_hard_gates_enabled:
            return True, "DRS hard gates disabled"

        component = score.components.get("dealer_regime", {})
        drs_norm = float(component.get("score", 0.0))
        gamma_flip = market_ctx.get("gamma_flip")
        close = market_ctx.get("close")
        recent = market_ctx.get("recent_closes") or []
        prev_close = recent[-2] if len(recent) >= 2 else None

        if gamma_flip is None or close is None:
            return False, "DRS hard gate blocked: missing gamma flip or close"

        if score.direction == "bullish":
            holds_above_flip = close > gamma_flip and (prev_close is None or prev_close > gamma_flip)
            if not holds_above_flip:
                return False, "DRS hard gate blocked: bullish entry requires hold above gamma flip"
            if drs_norm <= self.drs_call_entry_min:
                return (
                    False,
                    f"DRS hard gate blocked: bullish entry requires DRS > {self.drs_call_entry_min:.2f}",
                )
            return True, "DRS bullish gate passed"

        if score.direction == "bearish":
            crossed_below_flip = close < gamma_flip and (prev_close is not None and prev_close >= gamma_flip)
            if not crossed_below_flip:
                return False, "DRS hard gate blocked: bearish entry requires fresh cross below gamma flip"
            if drs_norm >= self.drs_put_entry_max:
                return (
                    False,
                    f"DRS hard gate blocked: bearish entry requires DRS < {self.drs_put_entry_max:.2f}",
                )
            return True, "DRS bearish gate passed"

        return False, "DRS hard gate blocked: neutral direction"

    def _select_optimizer_candidate(
        self,
        score: ScoreSnapshot,
        ctx: dict,
        conn=None,
        cached_option_rows=None,
        forced_timeframe: Optional[str] = None,
    ) -> Optional[dict]:
        signal_timeframe = forced_timeframe or self._infer_signal_timeframe(
            score.normalized_score
        )
        signal_strength = self._infer_signal_strength(score.normalized_score)

        dte_ranges = {"intraday": (0, 2), "swing": (1, 7), "multi_day": (3, 14)}
        dte_min, dte_max = dte_ranges.get(signal_timeframe, (1, 7))

        # Reuse option rows from OpportunityQualityComponent if available
        # and the DTE window matches, avoiding a duplicate option_chains scan.
        option_rows = None
        if cached_option_rows is not None:
            cache_key, rows = cached_option_rows
            if cache_key == (score.timestamp, dte_min, dte_max) and rows:
                option_rows = rows
        if option_rows is None:
            with self._use_conn(conn) as c:
                option_rows = fetch_option_snapshot(
                    c, self.db_symbol, score.timestamp, score.timestamp.date(), dte_min, dte_max
                )
        if not option_rows:
            return None

        optimizer_ctx = PositionOptimizerContext(
            timestamp=score.timestamp,
            signal_timestamp=score.timestamp,
            signal_timeframe=signal_timeframe,
            signal_direction=score.direction,
            signal_strength=signal_strength,
            trade_type="trend_follow" if score.direction != "neutral" else "range",
            current_price=ctx["close"],
            net_gex=ctx["net_gex"],
            gamma_flip=ctx["gamma_flip"],
            put_call_ratio=ctx["put_call_ratio"],
            max_pain=ctx["max_pain"],
            smart_call_premium=ctx["smart_call"],
            smart_put_premium=ctx["smart_put"],
            dealer_net_delta=0.0,
            target_dte_min=dte_min,
            target_dte_max=dte_max,
            option_rows=option_rows,
        )
        candidates = self.position_optimizer._generate_candidates(optimizer_ctx)
        if not candidates:
            return None

        for candidate in candidates:
            profiles = {p.profile: p for p in candidate.sizing_profiles}
            optimal = profiles.get("optimal")
            if optimal and optimal.contracts > 0:
                return {
                    "candidate": candidate,
                    "signal_timeframe": signal_timeframe,
                    "signal_strength": signal_strength,
                }
        return None

    @staticmethod
    def _majority_direction(trades: list[dict]) -> str:
        if not trades:
            return "neutral"
        bull = sum(t["quantity_open"] for t in trades if t["direction"] == "bullish")
        bear = sum(t["quantity_open"] for t in trades if t["direction"] == "bearish")
        if bull > bear:
            return "bullish"
        if bear > bull:
            return "bearish"
        return "neutral"

    @staticmethod
    def _infer_signal_strength(normalized_score: float) -> str:
        if normalized_score >= 0.82:
            return "high"
        if normalized_score >= 0.64:
            return "medium"
        return "low"

    @staticmethod
    def _infer_signal_timeframe(normalized_score: float) -> str:
        if normalized_score >= 0.84:
            return "intraday"
        if normalized_score >= 0.68:
            return "swing"
        return "multi_day"

    @staticmethod
    def _legs_from_candidate(candidate: dict) -> list[dict]:
        strikes = candidate.get("strikes", "")
        strategy = candidate.get("strategy_type")
        expiry = candidate.get("expiry")
        parts = [float(p) for p in re.findall(r"(\d+(?:\.\d+)?)", strikes)]
        if strategy == "bull_call_debit" and len(parts) >= 2:
            return [
                {"side": "long", "option_type": "C", "strike": parts[0], "expiry": expiry},
                {"side": "short", "option_type": "C", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "bear_put_debit" and len(parts) >= 2:
            return [
                {"side": "long", "option_type": "P", "strike": parts[0], "expiry": expiry},
                {"side": "short", "option_type": "P", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "bull_put_credit" and len(parts) >= 2:
            return [
                {"side": "short", "option_type": "P", "strike": parts[0], "expiry": expiry},
                {"side": "long", "option_type": "P", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "bear_call_credit" and len(parts) >= 2:
            return [
                {"side": "short", "option_type": "C", "strike": parts[0], "expiry": expiry},
                {"side": "long", "option_type": "C", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "iron_condor" and len(parts) >= 4:
            return [
                {"side": "short", "option_type": "P", "strike": parts[0], "expiry": expiry},
                {"side": "short", "option_type": "C", "strike": parts[1], "expiry": expiry},
                {"side": "long", "option_type": "P", "strike": parts[2], "expiry": expiry},
                {"side": "long", "option_type": "C", "strike": parts[3], "expiry": expiry},
            ]
        return []

    def _resolve_option_symbol_for_leg(
        self, as_of: datetime, leg: dict, conn=None
    ) -> Optional[str]:
        with self._use_conn(conn) as c:
            cur = c.cursor()
            cur.execute(
                """
                SELECT option_symbol
                FROM option_chains
                WHERE underlying = %s
                  AND timestamp <= %s
                  AND expiration = %s
                  AND option_type = %s
                  AND ABS(strike - %s) < 0.01
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (self.db_symbol, as_of, leg["expiry"], leg["option_type"], leg["strike"]),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def _cash_target(self, score: ScoreSnapshot, rationale: str) -> PortfolioTarget:
        """Convenience: build a 100% cash PortfolioTarget."""
        return PortfolioTarget(
            underlying=self.db_symbol,
            timestamp=score.timestamp,
            composite_score=score.composite_score,
            normalized_score=score.normalized_score,
            direction=score.direction,
            target_positions=[],
            total_target_contracts=0,
            target_heat_pct=0.0,
            rationale=rationale,
        )
