"""Proprietary Signal Engine with lifecycle, sizing, and live-cost tracking."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional

from src.database import db_connection
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)

STATUS_READY = "ready_to_trigger"
STATUS_ACTIVE = "position_open"
STATUS_TRIMMED = "partial_take_profit"
STATUS_STOPPED = "stopped_out"
STATUS_TARGET_HIT = "target_fully_hit"
STATUS_CLOSED = "closed"


@dataclass
class ManagedTradeIdea:
    underlying: str
    signal_timestamp: datetime
    timestamp: datetime
    status: str
    signal_timeframe: str
    signal_direction: str
    strategy_type: str
    expiry: datetime.date
    strikes: str
    contracts: int
    entry_price: float
    current_mark: float
    stop_price: float
    target_1: float
    target_2: float
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    trade_cost: float
    notes: str


class ProprietarySignalEngine:
    """Turns optimizer candidates into managed trade ideas with lifecycle status."""

    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)

    def _fetch_latest_optimizer_signal(self) -> Optional[dict]:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT timestamp, timeframe, direction, top_candidate
                FROM consolidated_trade_signals
                WHERE underlying = %s
                ORDER BY timestamp DESC
                """,
                (self.db_symbol,),
            )
            row = cur.fetchone()
            if not row:
                return None
            ts, timeframe, direction, candidate = row
            parsed = json.loads(candidate) if isinstance(candidate, str) else (candidate or {})
            if not parsed:
                return None
            return {
                "timestamp": ts,
                "timeframe": timeframe,
                "direction": direction,
                "candidate": parsed,
            }

    def _fetch_active_trades(self) -> list[dict]:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, status, entry_price, target_1, target_2, stop_price, contracts,
                       strategy_type, strikes, expiry, signal_direction
                FROM signal_engine_trade_ideas
                WHERE underlying = %s
                  AND status IN (%s, %s, %s)
                ORDER BY timestamp DESC
                """,
                (self.db_symbol, STATUS_ACTIVE, STATUS_TRIMMED, STATUS_READY),
            )
            rows = cur.fetchall()
            trades: list[dict] = []
            for row in rows:
                trades.append({
                    "id": row[0],
                    "status": row[1],
                    "entry_price": float(row[2]),
                    "target_1": float(row[3]),
                    "target_2": float(row[4]),
                    "stop_price": float(row[5]),
                    "contracts": int(row[6]),
                    "strategy_type": row[7],
                    "strikes": row[8],
                    "expiry": row[9],
                    "signal_direction": row[10],
                })
            return trades

    def _fetch_underlying_mark(self) -> Optional[float]:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT close
                FROM underlying_quotes
                WHERE symbol = %s
                ORDER BY timestamp DESC
                """,
                (self.db_symbol,),
            )
            row = cur.fetchone()
            return float(row[0]) if row else None

    def _build_idea(self, optimizer_signal: dict, mark: float) -> ManagedTradeIdea:
        candidate = optimizer_signal["candidate"]
        entry = float(candidate.get("entry_debit") or candidate.get("entry_credit") or 0.0)
        contracts = max(1, int((candidate.get("sizing_profiles") or [{}])[1].get("contracts") or 1))
        stop = round(entry * 0.65, 4)
        t1 = round(entry * 1.30, 4)
        t2 = round(entry * 1.60, 4)
        trade_cost = round(entry * contracts * 100, 2)
        return ManagedTradeIdea(
            underlying=self.db_symbol,
            signal_timestamp=optimizer_signal["timestamp"],
            timestamp=datetime.utcnow(),
            status=STATUS_ACTIVE,
            signal_timeframe=optimizer_signal["timeframe"],
            signal_direction=optimizer_signal["direction"],
            strategy_type=candidate.get("strategy_type", "unknown"),
            expiry=candidate.get("expiry"),
            strikes=candidate.get("strikes", ""),
            contracts=contracts,
            entry_price=entry,
            current_mark=entry,
            stop_price=stop,
            target_1=t1,
            target_2=t2,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            total_pnl=0.0,
            trade_cost=trade_cost,
            notes=f"Triggered at underlying {mark:.2f}.",
        )

    def _store_new_trade(self, idea: ManagedTradeIdea) -> None:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO signal_engine_trade_ideas (
                    underlying, signal_timestamp, timestamp, status, signal_timeframe,
                    signal_direction, strategy_type, expiry, strikes, contracts,
                    entry_price, current_mark, stop_price, target_1, target_2,
                    realized_pnl, unrealized_pnl, total_pnl, trade_cost, notes
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                """,
                (
                    idea.underlying,
                    idea.signal_timestamp,
                    idea.timestamp,
                    idea.status,
                    idea.signal_timeframe,
                    idea.signal_direction,
                    idea.strategy_type,
                    idea.expiry,
                    idea.strikes,
                    idea.contracts,
                    idea.entry_price,
                    idea.current_mark,
                    idea.stop_price,
                    idea.target_1,
                    idea.target_2,
                    idea.realized_pnl,
                    idea.unrealized_pnl,
                    idea.total_pnl,
                    idea.trade_cost,
                    idea.notes,
                ),
            )
            conn.commit()

    def _update_active_trade(self, trade: dict, mark: float) -> bool:
        entry = trade["entry_price"]
        contracts = trade["contracts"]
        status = STATUS_ACTIVE
        realized = 0.0

        if mark <= trade["stop_price"]:
            status = STATUS_STOPPED
            realized = (mark - entry) * contracts * 100
        elif mark >= trade["target_2"]:
            status = STATUS_TARGET_HIT
            realized = (trade["target_2"] - entry) * contracts * 100
        elif mark >= trade["target_1"]:
            status = STATUS_TRIMMED
            realized = (trade["target_1"] - entry) * (contracts * 0.5) * 100

        unrealized = (mark - entry) * contracts * 100 if status in {STATUS_ACTIVE, STATUS_TRIMMED} else 0.0
        total = realized + unrealized

        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE signal_engine_trade_ideas
                SET current_mark = %s,
                    status = %s,
                    realized_pnl = %s,
                    unrealized_pnl = %s,
                    total_pnl = %s,
                    updated_at = NOW(),
                    notes = %s
                WHERE id = %s
                """,
                (
                    mark,
                    status,
                    round(realized, 2),
                    round(unrealized, 2),
                    round(total, 2),
                    f"Lifecycle update at underlying mark {mark:.2f}",
                    trade["id"],
                ),
            )
            conn.commit()

        logger.info("ProprietarySignalEngine [%s] status=%s pnl=%.2f", self.db_symbol, status, total)
        return True


    def _trade_exists_for_signal(self, signal_ts: datetime, strategy_type: str, strikes: str) -> bool:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT 1
                FROM signal_engine_trade_ideas
                WHERE underlying = %s
                  AND signal_timestamp = %s
                  AND strategy_type = %s
                  AND strikes = %s
                LIMIT 1
                """,
                (self.db_symbol, signal_ts, strategy_type, strikes),
            )
            return cur.fetchone() is not None

    def run_cycle(self) -> bool:
        mark = self._fetch_underlying_mark()
        if mark is None:
            logger.warning("ProprietarySignalEngine: no underlying mark, skipping")
            return False

        active_trades = self._fetch_active_trades()
        updated_any = False
        for trade in active_trades:
            updated_any = self._update_active_trade(trade, mark) or updated_any

        optimizer_signal = self._fetch_latest_optimizer_signal()
        if not optimizer_signal or optimizer_signal["direction"] == "neutral":
            if not updated_any:
                logger.info("ProprietarySignalEngine [%s] %s", self.db_symbol, STATUS_READY)
            return updated_any

        idea = self._build_idea(optimizer_signal, mark)
        if self._trade_exists_for_signal(idea.signal_timestamp, idea.strategy_type, idea.strikes):
            return updated_any

        self._store_new_trade(idea)
        logger.info("ProprietarySignalEngine [%s] launched %s", self.db_symbol, asdict(idea))
        return True
