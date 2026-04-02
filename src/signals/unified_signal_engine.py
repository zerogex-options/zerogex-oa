"""Unified signal + hypothetical trade engine.

This engine is fully self-contained under src/signals and does not depend on
src/analytics modules.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from src.database import db_connection
from src.signals.position_optimizer_engine import (
    PositionOptimizerContext,
    PositionOptimizerEngine,
    TARGET_DTE_WINDOWS,
)
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
        self.position_optimizer = PositionOptimizerEngine(underlying=self.underlying)

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

    def _score_strength(self, normalized: float) -> str:
        if normalized >= 0.67:
            return "high"
        if normalized >= 0.40:
            return "medium"
        return "low"

    def _score_timeframe(self, normalized: float) -> str:
        if normalized >= 0.90:
            return "multi_day"
        if normalized >= 0.75:
            return "swing"
        return "intraday"

    def _fetch_optimizer_option_rows(self, as_of: datetime, dte_min: int, dte_max: int) -> list[dict]:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT expiration, strike, option_type, bid, ask, last,
                       COALESCE(delta, 0), COALESCE(gamma, 0), COALESCE(theta, 0),
                       COALESCE(volume, 0), COALESCE(open_interest, 0)
                FROM option_chains
                WHERE underlying = %s
                  AND timestamp <= %s
                  AND expiration BETWEEN (%s::date + (%s * INTERVAL '1 day'))
                                     AND (%s::date + (%s * INTERVAL '1 day'))
                  AND COALESCE(mid, (bid + ask)/2.0, last) IS NOT NULL
                  AND bid IS NOT NULL
                  AND ask IS NOT NULL
                  AND ask > 0
                ORDER BY timestamp DESC
                LIMIT 1200
                """,
                (self.db_symbol, as_of, as_of.date(), dte_min, as_of.date(), dte_max),
            )
            rows = cur.fetchall()
            return [
                {
                    "expiration": row[0],
                    "strike": float(row[1]),
                    "option_type": row[2],
                    "bid": float(row[3] or 0),
                    "ask": float(row[4] or 0),
                    "last": float(row[5] or 0),
                    "delta": float(row[6] or 0),
                    "gamma": float(row[7] or 0),
                    "theta": float(row[8] or 0),
                    "volume": int(row[9] or 0),
                    "open_interest": int(row[10] or 0),
                }
                for row in rows
            ]

    @staticmethod
    def _parse_candidate_legs(strikes: str) -> list[dict]:
        legs = []
        for side, strike, opt_type in re.findall(r"(Long|Short)\s+(\d+(?:\.\d+)?)\s*([CP])", strikes):
            legs.append(
                {
                    "side": side.lower(),
                    "strike": float(strike),
                    "option_type": opt_type,
                }
            )
        return legs

    def _select_optimizer_candidate(self, score: ScoreSnapshot, ctx: dict) -> Optional[dict]:
        signal_direction = score.direction
        if signal_direction == "neutral":
            return None

        tf = self._score_timeframe(score.normalized_score)
        signal_strength = self._score_strength(score.normalized_score)
        dte_min, dte_max = TARGET_DTE_WINDOWS.get(tf, (0, 2))
        option_rows = self._fetch_optimizer_option_rows(score.timestamp, dte_min, dte_max)
        if not option_rows:
            return None

        optimizer_ctx = PositionOptimizerContext(
            timestamp=score.timestamp,
            signal_timestamp=score.timestamp,
            signal_timeframe=tf,
            signal_direction=signal_direction,
            signal_strength=signal_strength,
            trade_type="directional",
            current_price=ctx["close"],
            net_gex=ctx["net_gex"],
            gamma_flip=ctx["gamma_flip"],
            put_call_ratio=ctx["put_call_ratio"],
            max_pain=ctx.get("max_pain"),
            smart_call_premium=ctx["smart_call"],
            smart_put_premium=ctx["smart_put"],
            dealer_net_delta=0.0,
            target_dte_min=dte_min,
            target_dte_max=dte_max,
            option_rows=option_rows,
        )
        candidates = self.position_optimizer._generate_candidates(optimizer_ctx)
        for candidate in candidates:
            if candidate.option_type not in {"C", "P"}:
                continue
            if candidate.entry_debit <= 0 and candidate.entry_credit <= 0:
                continue
            pricing_mode = "debit" if candidate.entry_debit > 0 else "credit"
            entry_price = (candidate.entry_debit if pricing_mode == "debit" else candidate.entry_credit) / 100.0
            optimal_contracts = next(
                (p.contracts for p in candidate.sizing_profiles if p.profile == "optimal"),
                1,
            )
            return {
                "strategy_type": candidate.strategy_type,
                "expiry": candidate.expiry,
                "option_type": candidate.option_type,
                "strikes": candidate.strikes,
                "entry_price": round(entry_price, 4),
                "contracts": max(1, int(optimal_contracts)),
                "pricing_mode": pricing_mode,
                "legs": self._parse_candidate_legs(candidate.strikes),
                "candidate": candidate,
            }
        return None

    def _strategy_leg_mark(self, as_of: datetime, expiry, strike: float, option_type: str) -> Optional[float]:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COALESCE(mid, (bid + ask)/2.0, last)
                FROM option_chains
                WHERE underlying = %s
                  AND expiration = %s
                  AND strike = %s
                  AND option_type = %s
                  AND timestamp <= %s
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (self.db_symbol, expiry, strike, option_type, as_of),
            )
            row = cur.fetchone()
            return float(row[0]) if row and row[0] is not None else None

    def _strategy_mark_from_payload(self, as_of: datetime, payload: dict) -> Optional[float]:
        strategy = payload.get("strategy") or {}
        legs = strategy.get("legs") or []
        pricing_mode = strategy.get("pricing_mode", "debit")
        expiry = strategy.get("expiry")
        if not legs or not expiry:
            return None

        prices = []
        for leg in legs:
            leg_mark = self._strategy_leg_mark(
                as_of=as_of,
                expiry=expiry,
                strike=float(leg["strike"]),
                option_type=str(leg["option_type"]),
            )
            if leg_mark is None:
                return None
            prices.append((leg["side"], leg_mark))

        if pricing_mode == "debit":
            value = sum(mark if side == "long" else -mark for side, mark in prices)
        else:
            value = sum(mark if side == "short" else -mark for side, mark in prices)
        return max(round(value, 4), 0.0)

    def _select_contract(self, as_of: datetime, score: ScoreSnapshot, ctx: dict) -> Optional[dict]:
        optimal = self._select_optimizer_candidate(score, ctx)
        if optimal:
            candidate = optimal["candidate"]
            return {
                "option_symbol": f"strategy:{optimal['strategy_type']}:{optimal['expiry']}:{optimal['strikes']}",
                "expiration": optimal["expiry"],
                "strike": candidate.max_loss,
                "entry_mark": optimal["entry_price"],
                "option_type": optimal["option_type"],
                "contracts": optimal["contracts"],
                "strategy_payload": {
                    "strategy": {
                        "strategy_type": optimal["strategy_type"],
                        "pricing_mode": optimal["pricing_mode"],
                        "strikes": optimal["strikes"],
                        "expiry": str(optimal["expiry"]),
                        "legs": optimal["legs"],
                        "expected_value": candidate.expected_value,
                        "probability_of_profit": candidate.probability_of_profit,
                    }
                },
            }

        # Fallback to simple directional single-leg if optimizer cannot build candidates.
        opt_type = "C" if score.direction == "bullish" else "P"
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
                (self.db_symbol, as_of, as_of.date(), opt_type, ctx["close"]),
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
                "contracts": 1,
                "strategy_payload": {"strategy": {"strategy_type": "single_leg_fallback", "pricing_mode": "debit", "legs": []}},
            }

    def _fetch_open_trades(self) -> list[dict]:
        with db_connection() as conn:
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
                    "components_at_entry": r[9] if isinstance(r[9], dict) else json.loads(r[9] or "{}"),
                }
                for r in rows
            ]

    def _latest_option_mark(self, trade: dict, as_of: datetime) -> Optional[float]:
        option_symbol = trade["option_symbol"]
        if isinstance(option_symbol, str) and option_symbol.startswith("strategy:"):
            payload = trade.get("components_at_entry") or {}
            strategy_mark = self._strategy_mark_from_payload(as_of, payload)
            if strategy_mark is not None:
                return strategy_mark

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

    def _open_trade(self, score: ScoreSnapshot, ctx: dict) -> bool:
        if score.direction == "neutral" or score.normalized_score < self.trigger_threshold:
            return False

        contract = self._select_contract(score.timestamp, score, ctx)
        if not contract:
            return False

        quantity = max(1, int(contract.get("contracts", 1)))
        payload = {
            "score_components": score.components,
            **(contract.get("strategy_payload") or {}),
        }
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
                    json.dumps(payload, default=str),
                ),
            )
            conn.commit()
            return cur.rowcount > 0

    def _update_open_trade(self, trade: dict, score: ScoreSnapshot) -> None:
        mark = self._latest_option_mark(trade, score.timestamp)
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
                    json.dumps({"score_components": score.components, **(trade.get("components_at_entry") or {})}, default=str),
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

        opened = self._open_trade(score, ctx)
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
