"""Hypothetical trade lifecycle engine.

Maintains 1:1 signal/trade rows that are mutable while open and immutable once
closed.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from typing import Optional

from src.database import db_connection
from src.signals.position_optimizer_engine import PositionOptimizerEngine
from src.signals.signal_scoring_engine import ScoreSnapshot
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)

OPEN_STATUSES = {"open", "trimmed"}
CLOSED_STATUSES = {"stopped", "target_hit", "score_exit", "expired"}


class HypotheticalTradeEngine:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        self.position_engine = PositionOptimizerEngine(underlying=self.underlying)

    def _latest_underlying(self, as_of: datetime) -> Optional[float]:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT close
                FROM underlying_quotes
                WHERE symbol = %s
                  AND timestamp <= %s
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (self.db_symbol, as_of),
            )
            row = cur.fetchone()
            return float(row[0]) if row else None

    def _fetch_option_mark(self, as_of: datetime, expiry, strikes: str, direction: str) -> Optional[float]:
        parsed = [float(p) for p in __import__("re").findall(r"(\d+(?:\.\d+)?)", strikes or "")]
        if not parsed:
            return None
        nearest = parsed[0]
        option_type = "C" if direction == "bullish" else "P"
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT (bid + ask) / 2.0
                FROM option_chains
                WHERE underlying = %s
                  AND expiration = %s
                  AND option_type = %s
                  AND strike = %s
                  AND timestamp <= %s
                  AND bid IS NOT NULL AND ask IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (self.db_symbol, expiry, option_type, nearest, as_of),
            )
            row = cur.fetchone()
            return float(row[0]) if row and row[0] is not None else None

    def _fetch_open_trades(self) -> list[dict]:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, score_timestamp, status, signal_direction, signal_strength,
                       contracts, entry_price, current_mark, avg_cost,
                       stop_price, target_1, target_2, trim_count,
                       realized_pnl, unrealized_pnl, total_pnl,
                       strategy_type, expiry, strikes
                FROM signal_trades
                WHERE underlying = %s
                  AND status IN ('open', 'trimmed')
                ORDER BY opened_at ASC
                """,
                (self.db_symbol,),
            )
            rows = cur.fetchall()
            return [
                {
                    "id": r[0], "score_timestamp": r[1], "status": r[2], "signal_direction": r[3], "signal_strength": r[4],
                    "contracts": int(r[5]), "entry_price": float(r[6]), "current_mark": float(r[7]), "avg_cost": float(r[8]),
                    "stop_price": float(r[9]), "target_1": float(r[10]), "target_2": float(r[11]), "trim_count": int(r[12]),
                    "realized_pnl": float(r[13]), "unrealized_pnl": float(r[14]), "total_pnl": float(r[15]),
                    "strategy_type": r[16], "expiry": r[17], "strikes": r[18],
                }
                for r in rows
            ]

    def _score_to_trade_context(self, score: ScoreSnapshot):
        signal = {
            "timestamp": score.timestamp,
            "timeframe": score.recommended_timeframe,
            "direction": score.direction,
            "strength": score.strength,
            "trade_type": score.recommended_trade_type,
        }
        return self.position_engine._fetch_context(as_of=score.timestamp, signal=signal)

    def _build_trade_candidate(self, score: ScoreSnapshot) -> Optional[dict]:
        if score.direction == "neutral" or score.strength == "low" or score.recommended_trade_type == "no_trade":
            return None
        ctx = self._score_to_trade_context(score)
        if ctx is None:
            return None
        pos = self.position_engine.compute_signal(ctx)
        if pos is None or not pos.candidates:
            return None
        best = pos.candidates[0]
        profiles = best.sizing_profiles or []
        optimal = next((p for p in profiles if p.profile == "optimal"), profiles[0] if profiles else None)
        contracts = int(optimal.contracts) if optimal else 1
        entry = float(best.entry_debit or best.entry_credit or 0.0)
        if entry <= 0:
            return None
        return {
            "strategy_type": best.strategy_type,
            "expiry": best.expiry,
            "strikes": best.strikes,
            "contracts": max(1, contracts),
            "entry_price": entry,
            "candidate": asdict(best),
        }

    def _insert_trade(self, score: ScoreSnapshot, candidate: dict, mark: float) -> None:
        entry = candidate["entry_price"]
        contracts = candidate["contracts"]
        stop = round(entry * 0.65, 4)
        t1 = round(entry * 1.30, 4)
        t2 = round(entry * 1.60, 4)
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO signal_trades (
                    underlying, score_timestamp, opened_at, status,
                    signal_direction, signal_strength, strategy_type, expiry, strikes,
                    contracts, entry_price, avg_cost, current_mark,
                    stop_price, target_1, target_2,
                    trim_count, add_count,
                    realized_pnl, unrealized_pnl, total_pnl,
                    win_loss_pct, notes, candidate
                ) VALUES (
                    %s, %s, NOW(), 'open',
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    0, 0,
                    0, 0, 0,
                    0, %s, %s::jsonb
                )
                """,
                (
                    self.db_symbol,
                    score.timestamp,
                    score.direction,
                    score.strength,
                    candidate["strategy_type"],
                    candidate["expiry"],
                    candidate["strikes"],
                    contracts,
                    entry,
                    entry,
                    mark,
                    stop,
                    t1,
                    t2,
                    f"Triggered from score {score.composite_score}/{score.max_possible_score}",
                    json.dumps(candidate["candidate"], default=str),
                ),
            )
            conn.commit()

    def _update_trade(self, trade: dict, score: ScoreSnapshot, mark: float) -> None:
        entry = trade["avg_cost"]
        contracts = trade["contracts"]
        realized = float(trade["realized_pnl"])
        status = trade["status"]
        trim_count = int(trade["trim_count"])
        add_count = 0

        if mark <= trade["stop_price"]:
            status = "stopped"
            realized += (mark - entry) * contracts * 100
            contracts_live = 0
        elif mark >= trade["target_2"]:
            status = "target_hit"
            realized += (trade["target_2"] - entry) * contracts * 100
            contracts_live = 0
        elif mark >= trade["target_1"] and trim_count == 0:
            status = "trimmed"
            realized += (trade["target_1"] - entry) * (contracts * 0.5) * 100
            trim_count = 1
            contracts_live = int(max(1, round(contracts * 0.5)))
        elif score.direction != trade["signal_direction"] and score.normalized_score >= 0.40:
            status = "score_exit"
            realized += (mark - entry) * contracts * 100
            contracts_live = 0
        else:
            contracts_live = contracts

        if status in CLOSED_STATUSES:
            unrealized = 0.0
        else:
            unrealized = (mark - entry) * contracts_live * 100

        # Add-to-winner / cut-loser adjustments from evolving score.
        if status in OPEN_STATUSES and score.direction == trade["signal_direction"] and score.strength == "high" and mark > entry:
            add_count = 1
            contracts_live += 1
            unrealized = (mark - entry) * contracts_live * 100

        total = realized + unrealized
        notional = max(entry * max(contracts, 1) * 100, 1e-6)
        win_loss_pct = round((total / notional) * 100.0, 4)

        with db_connection() as conn:
            cur = conn.cursor()
            if status in CLOSED_STATUSES:
                cur.execute(
                    """
                    UPDATE signal_trades
                    SET status = %s,
                        current_mark = %s,
                        contracts = %s,
                        trim_count = %s,
                        add_count = add_count + %s,
                        realized_pnl = %s,
                        unrealized_pnl = %s,
                        total_pnl = %s,
                        win_loss_pct = %s,
                        closed_at = NOW(),
                        notes = %s,
                        updated_at = NOW()
                    WHERE id = %s
                      AND closed_at IS NULL
                    """,
                    (
                        status,
                        mark,
                        contracts_live,
                        trim_count,
                        add_count,
                        round(realized, 2),
                        round(unrealized, 2),
                        round(total, 2),
                        win_loss_pct,
                        f"Closed by {status} at mark {mark:.4f}",
                        trade["id"],
                    ),
                )
            else:
                cur.execute(
                    """
                    UPDATE signal_trades
                    SET status = %s,
                        current_mark = %s,
                        contracts = %s,
                        trim_count = %s,
                        add_count = add_count + %s,
                        realized_pnl = %s,
                        unrealized_pnl = %s,
                        total_pnl = %s,
                        win_loss_pct = %s,
                        notes = %s,
                        updated_at = NOW()
                    WHERE id = %s
                      AND closed_at IS NULL
                    """,
                    (
                        status,
                        mark,
                        contracts_live,
                        trim_count,
                        add_count,
                        round(realized, 2),
                        round(unrealized, 2),
                        round(total, 2),
                        win_loss_pct,
                        f"Live update @ {mark:.4f}",
                        trade["id"],
                    ),
                )
            conn.commit()

    def run_cycle(self, score: Optional[ScoreSnapshot]) -> bool:
        if score is None:
            return False

        updated = False
        open_trades = self._fetch_open_trades()
        for trade in open_trades:
            mark = self._fetch_option_mark(score.timestamp, trade["expiry"], trade["strikes"], trade["signal_direction"])
            if mark is None:
                continue
            self._update_trade(trade, score, mark)
            updated = True

        candidate = self._build_trade_candidate(score)
        if candidate is None:
            return updated

        # Prevent duplicate entry for same score timestamp + contract.
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT 1
                FROM signal_trades
                WHERE underlying = %s
                  AND score_timestamp = %s
                  AND strategy_type = %s
                  AND strikes = %s
                LIMIT 1
                """,
                (self.db_symbol, score.timestamp, candidate["strategy_type"], candidate["strikes"]),
            )
            if cur.fetchone() is not None:
                return updated

        mark = self._fetch_option_mark(score.timestamp, candidate["expiry"], candidate["strikes"], score.direction)
        if mark is None:
            # fallback to model entry if mark unavailable
            mark = candidate["entry_price"]
        self._insert_trade(score, candidate, mark)
        logger.info("HypotheticalTradeEngine [%s] entered %s %s", self.db_symbol, score.direction, candidate["strategy_type"])
        return True
