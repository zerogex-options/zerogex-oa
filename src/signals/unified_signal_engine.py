"""Unified signal + hypothetical trade engine.

This engine is fully self-contained under src/signals and does not depend on
src/analytics modules.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from src.database import db_connection
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)


@dataclass
class ScoreSnapshot:
    timestamp: datetime
    underlying: str
    composite_score: float
    normalized_score: float
    direction: str
    components: dict


class UnifiedSignalEngine:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        self.trigger_threshold = 0.58

    def _fetch_market_context(self) -> Optional[dict]:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT uq.timestamp,
                       uq.close,
                       gs.total_net_gex,
                       gs.gamma_flip_point,
                       gs.put_call_ratio,
                       gs.max_pain
                FROM underlying_quotes uq
                LEFT JOIN LATERAL (
                    SELECT total_net_gex, gamma_flip_point, put_call_ratio, max_pain
                    FROM gex_summary
                    WHERE underlying = %s AND timestamp <= uq.timestamp
                    ORDER BY timestamp DESC
                    LIMIT 1
                ) gs ON TRUE
                WHERE uq.symbol = %s
                ORDER BY uq.timestamp DESC
                LIMIT 1
                """,
                (self.db_symbol, self.db_symbol),
            )
            row = cur.fetchone()
            if not row:
                return None
            ts, close, net_gex, gamma_flip, pcr, max_pain = row

            cur.execute(
                """
                SELECT COALESCE(SUM(CASE WHEN option_type='C' THEN total_premium ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN option_type='P' THEN total_premium ELSE 0 END), 0)
                FROM flow_smart_money
                WHERE symbol = %s
                  AND timestamp BETWEEN %s - INTERVAL '30 minutes' AND %s
                """,
                (self.db_symbol, ts, ts),
            )
            sm_call, sm_put = cur.fetchone() or (0.0, 0.0)

            cur.execute(
                """
                SELECT close
                FROM underlying_quotes
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 20
                """,
                (self.db_symbol,),
            )
            closes = [float(r[0]) for r in cur.fetchall()]

            return {
                "timestamp": ts,
                "close": float(close),
                "net_gex": float(net_gex or 0.0),
                "gamma_flip": float(gamma_flip) if gamma_flip is not None else None,
                "put_call_ratio": float(pcr or 1.0),
                "max_pain": float(max_pain) if max_pain is not None else None,
                "smart_call": float(sm_call or 0.0),
                "smart_put": float(sm_put or 0.0),
                "recent_closes": list(reversed(closes)),
            }

    @staticmethod
    def _direction(score: float) -> str:
        if score > 0:
            return "bullish"
        if score < 0:
            return "bearish"
        return "neutral"

    def _compute_exhaustion(self, closes: list[float]) -> tuple[float, str]:
        if len(closes) < 8:
            return 0.0, "insufficient_data"
        short = sum(closes[-5:]) / 5
        long = sum(closes[-8:]) / 8
        drift = (short - long) / long if long else 0.0
        score = min(1.0, abs(drift) * 20)
        label = "exhausting" if score > 0.6 else "controlled"
        return score, label

    def _compute_score(self, ctx: dict) -> ScoreSnapshot:
        gex_score = -1.0 if ctx["net_gex"] < 0 else 1.0
        flip_score = 0.0
        if ctx["gamma_flip"]:
            dist = (ctx["close"] - ctx["gamma_flip"]) / ctx["gamma_flip"]
            flip_score = -1.0 if abs(dist) < 0.003 else (1.0 if dist > 0 else -1.0)

        pcr = ctx["put_call_ratio"]
        pcr_score = 1.0 if pcr < 0.8 else (-1.0 if pcr > 1.2 else 0.0)

        sm_ratio = (ctx["smart_call"] + 1.0) / (ctx["smart_put"] + 1.0)
        sm_score = 1.0 if sm_ratio > 1.2 else (-1.0 if sm_ratio < 0.8 else 0.0)

        exhaustion, exhaustion_state = self._compute_exhaustion(ctx["recent_closes"])
        exhaustion_dir = -1.0 if sm_score > 0 else (1.0 if sm_score < 0 else 0.0)
        exhaustion_score = exhaustion_dir * exhaustion

        vol_pressure = min(1.0, abs(ctx["net_gex"]) / 5_000_000_000)
        vol_dir = -1.0 if ctx["net_gex"] < 0 else 0.5

        weighted = {
            "gex_regime": {"weight": 0.22, "score": gex_score, "value": ctx["net_gex"]},
            "gamma_flip": {"weight": 0.15, "score": flip_score, "value": ctx["gamma_flip"]},
            "put_call_ratio": {"weight": 0.12, "score": pcr_score, "value": pcr},
            "smart_money": {"weight": 0.16, "score": sm_score, "value": sm_ratio},
            "vol_expansion": {"weight": 0.20, "score": vol_dir * vol_pressure, "value": vol_pressure},
            "exhaustion": {
                "weight": 0.15,
                "score": exhaustion_score,
                "value": exhaustion,
                "state": exhaustion_state,
            },
        }

        composite = sum(c["weight"] * c["score"] for c in weighted.values())
        normalized = abs(composite)
        return ScoreSnapshot(
            timestamp=ctx["timestamp"],
            underlying=self.db_symbol,
            composite_score=round(composite, 6),
            normalized_score=round(normalized, 6),
            direction=self._direction(composite),
            components=weighted,
        )

    def _store_score(self, score: ScoreSnapshot) -> None:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO signal_scores (
                    underlying, timestamp, composite_score, normalized_score, direction, components
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (underlying, timestamp) DO UPDATE SET
                    composite_score = EXCLUDED.composite_score,
                    normalized_score = EXCLUDED.normalized_score,
                    direction = EXCLUDED.direction,
                    components = EXCLUDED.components,
                    updated_at = NOW()
                """,
                (
                    score.underlying,
                    score.timestamp,
                    score.composite_score,
                    score.normalized_score,
                    score.direction,
                    json.dumps(score.components, default=str),
                ),
            )
            conn.commit()

    def _select_contract(self, as_of: datetime, direction: str, spot: float) -> Optional[dict]:
        opt_type = "C" if direction == "bullish" else "P"
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                WITH latest AS (
                    SELECT option_symbol, expiration, strike, option_type,
                           COALESCE(mid, (bid + ask)/2.0, last) AS mark
                    FROM option_chains
                    WHERE underlying = %s
                      AND timestamp <= %s
                      AND expiration >= %s::date
                      AND option_type = %s
                      AND COALESCE(mid, (bid + ask)/2.0, last) IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT 400
                )
                SELECT option_symbol, expiration, strike, mark
                FROM latest
                ORDER BY expiration ASC, ABS(strike - %s)
                LIMIT 1
                """,
                (self.db_symbol, as_of, as_of.date(), opt_type, spot),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "option_symbol": row[0],
                "expiration": row[1],
                "strike": float(row[2]),
                "entry_mark": float(row[3]),
                "option_type": opt_type,
            }

    def _fetch_open_trades(self) -> list[dict]:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, option_symbol, entry_price, current_price, quantity_open,
                       quantity_initial, status, direction, realized_pnl
                FROM signal_trades
                WHERE underlying = %s
                  AND status = 'open'
                ORDER BY opened_at ASC
                """,
                (self.db_symbol,),
            )
            rows = cur.fetchall()
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
                }
                for r in rows
            ]

    def _latest_option_mark(self, option_symbol: str, as_of: datetime) -> Optional[float]:
        with db_connection() as conn:
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

    def _open_trade(self, score: ScoreSnapshot, spot: float) -> bool:
        if score.direction == "neutral" or score.normalized_score < self.trigger_threshold:
            return False

        contract = self._select_contract(score.timestamp, score.direction, spot)
        if not contract:
            return False

        quantity = 1
        with db_connection() as conn:
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
                ON CONFLICT (underlying, signal_timestamp) DO NOTHING
                """,
                (
                    self.db_symbol,
                    score.timestamp,
                    score.timestamp,
                    score.direction,
                    score.composite_score,
                    contract["option_symbol"],
                    contract["option_type"],
                    contract["expiration"],
                    contract["strike"],
                    contract["entry_mark"],
                    contract["entry_mark"],
                    quantity,
                    quantity,
                    json.dumps(score.components, default=str),
                ),
            )
            conn.commit()
            return cur.rowcount > 0

    def _update_open_trade(self, trade: dict, score: ScoreSnapshot) -> None:
        mark = self._latest_option_mark(trade["option_symbol"], score.timestamp)
        if mark is None:
            return

        qty_open = trade["quantity_open"]
        entry = trade["entry_price"]
        realized = trade["realized_pnl"]

        target_trim = entry * 1.25
        stop = entry * 0.70

        # Add size on materially stronger score in same direction.
        add_qty = 0
        if score.direction == trade["direction"] and score.normalized_score >= 0.85 and qty_open == trade["quantity_initial"]:
            add_qty = 1
            qty_open += 1

        # Cut half if score deteriorates.
        if score.normalized_score < 0.35 and qty_open > 1:
            cut_qty = qty_open // 2
            realized += (mark - entry) * cut_qty * 100
            qty_open -= cut_qty

        # Trim into strength.
        if mark >= target_trim and qty_open > 1:
            trim_qty = max(1, qty_open // 2)
            realized += (mark - entry) * trim_qty * 100
            qty_open -= trim_qty

        status = "open"
        closed_at = None

        # Hard stop or opposite signal closes.
        if mark <= stop or (score.direction != "neutral" and score.direction != trade["direction"] and score.normalized_score >= 0.55):
            realized += (mark - entry) * qty_open * 100
            qty_open = 0
            status = "closed"
            closed_at = score.timestamp

        unrealized = (mark - entry) * qty_open * 100
        total = realized + unrealized
        basis_qty = max(trade["quantity_initial"], 1)
        pnl_pct = ((total / (entry * basis_qty * 100)) * 100) if entry > 0 else 0

        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE signal_trades
                SET current_price = %s,
                    quantity_open = %s,
                    quantity_initial = quantity_initial + %s,
                    status = %s,
                    updated_at = NOW(),
                    closed_at = COALESCE(closed_at, %s),
                    realized_pnl = %s,
                    unrealized_pnl = %s,
                    total_pnl = %s,
                    pnl_percent = %s,
                    score_latest = %s,
                    components_latest = %s::jsonb
                WHERE id = %s
                  AND status = 'open'
                """,
                (
                    mark,
                    qty_open,
                    add_qty,
                    status,
                    closed_at,
                    round(realized, 4),
                    round(unrealized, 4),
                    round(total, 4),
                    round(pnl_pct, 4),
                    score.composite_score,
                    json.dumps(score.components, default=str),
                    trade["id"],
                ),
            )
            conn.commit()

    def run_cycle(self) -> bool:
        ctx = self._fetch_market_context()
        if not ctx:
            logger.warning("UnifiedSignalEngine: missing market context")
            return False

        score = self._compute_score(ctx)
        self._store_score(score)

        open_trades = self._fetch_open_trades()
        for trade in open_trades:
            self._update_open_trade(trade, score)

        opened = self._open_trade(score, ctx["close"])
        logger.info(
            "UnifiedSignalEngine [%s] score=%.3f norm=%.3f dir=%s open_trades=%d opened_new=%s",
            self.db_symbol,
            score.composite_score,
            score.normalized_score,
            score.direction,
            len(open_trades),
            opened,
        )
        return True
