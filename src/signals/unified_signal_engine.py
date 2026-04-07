"""Unified signal + hypothetical trade engine.

This engine is fully self-contained under src/signals and does not depend on
src/analytics modules.
"""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional

from src.config import (
    SIGNALS_MAX_OPEN_TRADES,
    SIGNALS_MAX_PORTFOLIO_HEAT_PCT,
    SIGNALS_PORTFOLIO_SIZE,
    SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES,
    SIGNALS_STOP_LOSS_PCT,
)
from src.database import db_connection
from src.signals.position_optimizer_engine import PositionOptimizerContext, PositionOptimizerEngine, fetch_option_snapshot
from src.signals.scoring_engine import ScoringEngine, ScoreSnapshot
from src.signals.components.gex_regime import GexRegimeComponent
from src.signals.components.gamma_flip import GammaFlipComponent
from src.signals.components.put_call_ratio import PutCallRatioComponent
from src.signals.components.smart_money import SmartMoneyComponent
from src.signals.components.vol_expansion import VolExpansionComponent
from src.signals.components.exhaustion import ExhaustionComponent
from src.signals.components.opportunity_quality import OpportunityQualityComponent
from src.signals.components.base import MarketContext
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)


class UnifiedSignalEngine:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        self.trigger_threshold = 0.58
        self.position_optimizer = PositionOptimizerEngine(self.underlying)

        self.scoring_engine = ScoringEngine(
            underlying=self.db_symbol,
            components=[
                GexRegimeComponent(),
                GammaFlipComponent(),
                PutCallRatioComponent(),
                SmartMoneyComponent(),
                VolExpansionComponent(),
                ExhaustionComponent(),
                OpportunityQualityComponent(self.underlying),
            ],
        )

    @staticmethod
    @contextmanager
    def _use_conn(conn=None):
        """Yield *conn* if provided, otherwise acquire a fresh one from the pool."""
        if conn is not None:
            yield conn
        else:
            with db_connection() as new_conn:
                yield new_conn

    def _fetch_market_context(self, conn=None) -> Optional[dict]:
        with self._use_conn(conn) as conn:
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
            close_f = float(close)

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

            # IV rank: compare current ATM IV to its 30-day daily range.
            # Used to dynamically adjust the trigger threshold (high IV = be more selective).
            iv_rank = None
            try:
                cur.execute(
                    """
                    WITH current_atm AS (
                        SELECT AVG(implied_volatility) AS current_iv
                        FROM option_chains
                        WHERE underlying = %s
                          AND ABS(strike - %s) / NULLIF(%s, 0) < 0.01
                          AND option_type = 'C'
                          AND implied_volatility IS NOT NULL
                          AND implied_volatility > 0
                          AND timestamp >= %s - INTERVAL '2 hours'
                    ),
                    daily_iv AS (
                        SELECT DATE_TRUNC('day', timestamp) AS day,
                               AVG(implied_volatility) AS avg_iv
                        FROM option_chains
                        WHERE underlying = %s
                          AND ABS(strike - %s) / NULLIF(%s, 0) < 0.01
                          AND option_type = 'C'
                          AND implied_volatility IS NOT NULL
                          AND implied_volatility > 0
                          AND timestamp >= NOW() - INTERVAL '30 days'
                        GROUP BY DATE_TRUNC('day', timestamp)
                    )
                    SELECT
                        (SELECT current_iv FROM current_atm),
                        MIN(avg_iv),
                        MAX(avg_iv)
                    FROM daily_iv
                    """,
                    (self.db_symbol, close_f, close_f, ts, self.db_symbol, close_f, close_f),
                )
                iv_row = cur.fetchone()
                if iv_row and iv_row[0] is not None and iv_row[1] is not None and iv_row[2] is not None:
                    current_iv, iv_low, iv_high = float(iv_row[0]), float(iv_row[1]), float(iv_row[2])
                    iv_range = iv_high - iv_low
                    if iv_range > 0.001:
                        iv_rank = round(min(1.0, max(0.0, (current_iv - iv_low) / iv_range)), 4)
            except Exception:
                pass  # IV rank is supplemental; do not block signal generation if unavailable

            return {
                "timestamp": ts,
                "close": close_f,
                "net_gex": float(net_gex or 0.0),
                "gamma_flip": float(gamma_flip) if gamma_flip is not None else None,
                "put_call_ratio": float(pcr or 1.0),
                "max_pain": float(max_pain) if max_pain is not None else None,
                "smart_call": float(sm_call or 0.0),
                "smart_put": float(sm_put or 0.0),
                "recent_closes": list(reversed(closes)),
                "iv_rank": iv_rank,
            }

    def _select_contract(self, as_of: datetime, direction: str, spot: float, conn=None) -> Optional[dict]:
        opt_type = "C" if direction == "bullish" else "P"
        with self._use_conn(conn) as conn:
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

    def _resolve_option_symbol_for_leg(self, as_of: datetime, leg: dict, conn=None) -> Optional[str]:
        with self._use_conn(conn) as conn:
            cur = conn.cursor()
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

    def _select_optimizer_candidate(self, score: ScoreSnapshot, ctx: dict, conn=None) -> Optional[dict]:
        signal_timeframe = self._infer_signal_timeframe(score.normalized_score)
        signal_strength = self._infer_signal_strength(score.normalized_score)

        dte_ranges = {"intraday": (0, 2), "swing": (1, 7), "multi_day": (3, 14)}
        dte_min, dte_max = dte_ranges.get(signal_timeframe, (1, 7))
        with self._use_conn(conn) as c:
            option_rows = fetch_option_snapshot(c, self.db_symbol, score.timestamp, score.timestamp.date(), dte_min, dte_max)
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
                return {"candidate": candidate, "signal_timeframe": signal_timeframe, "signal_strength": signal_strength}
        return None

    def _fetch_open_trades(self, conn=None) -> list[dict]:
        with self._use_conn(conn) as conn:
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
                    "components_at_entry": r[9] or {},
                }
                for r in rows
            ]

    def _latest_option_mark(self, option_symbol: str, as_of: datetime, conn=None) -> Optional[float]:
        with self._use_conn(conn) as conn:
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

    def _score_trend_confirmation(self, direction: str, as_of: datetime, conn=None) -> bool:
        """Confirm the current direction is consistent with recent signal_scores history.

        Looks at the last 4 scored cycles (written by this engine to signal_scores).
        If the majority pointed a different non-neutral direction, the current bar is
        likely a flip or noise — skip opening a new trade.

        Returns True when history agrees or there is insufficient history to disagree.
        """
        try:
            with self._use_conn(conn) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT direction
                    FROM signal_scores
                    WHERE underlying = %s
                      AND timestamp < %s
                      AND direction != 'neutral'
                    ORDER BY timestamp DESC
                    LIMIT 4
                    """,
                    (self.db_symbol, as_of),
                )
                rows = cur.fetchall()
                if not rows:
                    return True  # No history — don't block
                matching = sum(1 for r in rows if r[0] == direction)
                # Require at least half of recent history to agree
                return matching >= (len(rows) + 1) // 2
        except Exception:
            return True  # Do not block on DB errors

    # ------------------------------------------------------------------
    # Aggregate portfolio exposure
    # ------------------------------------------------------------------

    def _get_portfolio_exposure(self, open_trades: list[dict], conn=None) -> dict:
        """Summarise the current aggregate exposure across all open trades.

        Returns a dict with:
          - open_count: total open trades for this underlying
          - total_notional: sum(entry_price * quantity_open * 100) across trades
          - heat_pct: total_notional / portfolio size
          - net_direction: "bullish", "bearish", or "mixed"
          - bullish_count / bearish_count
          - last_opened_at: timestamp of most recently opened trade (or None)
        """
        if not open_trades:
            return {
                "open_count": 0,
                "total_notional": 0.0,
                "heat_pct": 0.0,
                "net_direction": "neutral",
                "bullish_count": 0,
                "bearish_count": 0,
                "last_opened_at": None,
            }

        total_notional = 0.0
        bullish = 0
        bearish = 0
        last_opened_at = None

        for t in open_trades:
            notional = abs(t["entry_price"]) * t["quantity_open"] * 100
            total_notional += notional
            if t["direction"] == "bullish":
                bullish += 1
            else:
                bearish += 1

        # Fetch the most recent opened_at timestamp so we can enforce cooldowns.
        try:
            with self._use_conn(conn) as c:
                cur = c.cursor()
                cur.execute(
                    """
                    SELECT MAX(opened_at)
                    FROM signal_trades
                    WHERE underlying = %s AND status = 'open'
                    """,
                    (self.db_symbol,),
                )
                row = cur.fetchone()
                last_opened_at = row[0] if row and row[0] else None
        except Exception:
            pass

        portfolio = max(SIGNALS_PORTFOLIO_SIZE, 1.0)
        if bullish > 0 and bearish > 0:
            net_dir = "mixed"
        elif bullish > 0:
            net_dir = "bullish"
        elif bearish > 0:
            net_dir = "bearish"
        else:
            net_dir = "neutral"

        return {
            "open_count": len(open_trades),
            "total_notional": round(total_notional, 2),
            "heat_pct": round(total_notional / portfolio, 6),
            "net_direction": net_dir,
            "bullish_count": bullish,
            "bearish_count": bearish,
            "last_opened_at": last_opened_at,
        }

    def _check_exposure_allows_entry(
        self, exposure: dict, direction: str, as_of: datetime
    ) -> tuple[bool, str]:
        """Return (allowed, reason) indicating whether a new trade may be opened."""

        # Gate 1: hard cap on concurrent open trades per underlying.
        if exposure["open_count"] >= SIGNALS_MAX_OPEN_TRADES:
            return False, (
                f"max open trades reached ({exposure['open_count']}/{SIGNALS_MAX_OPEN_TRADES})"
            )

        # Gate 2: total portfolio heat cap.
        if exposure["heat_pct"] >= SIGNALS_MAX_PORTFOLIO_HEAT_PCT:
            return False, (
                f"portfolio heat {exposure['heat_pct']:.2%} exceeds "
                f"limit {SIGNALS_MAX_PORTFOLIO_HEAT_PCT:.2%}"
            )

        # Gate 3: cooldown — don't stack another trade in the same direction
        # within N minutes of the last entry.
        if (
            exposure["last_opened_at"] is not None
            and exposure["net_direction"] == direction
        ):
            cooldown = timedelta(minutes=SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES)
            last = exposure["last_opened_at"]
            # Handle timezone-naive timestamps from the DB.
            if last.tzinfo is None and as_of.tzinfo is not None:
                last = last.replace(tzinfo=as_of.tzinfo)
            elif last.tzinfo is not None and as_of.tzinfo is None:
                as_of = as_of.replace(tzinfo=last.tzinfo)
            if as_of - last < cooldown:
                remaining = cooldown - (as_of - last)
                return False, (
                    f"same-direction cooldown: {remaining.total_seconds():.0f}s remaining"
                )

        return True, "ok"

    def _scale_size_for_exposure(self, base_qty: int, exposure: dict) -> int:
        """Reduce position size when existing exposure is already elevated.

        Linearly scales down from full size at 0% heat to minimum 1 contract at
        the heat cap.
        """
        if base_qty <= 1:
            return base_qty

        heat_cap = max(SIGNALS_MAX_PORTFOLIO_HEAT_PCT, 0.001)
        current_heat = exposure["heat_pct"]
        remaining_ratio = max(0.0, 1.0 - (current_heat / heat_cap))
        scaled = max(1, int(base_qty * remaining_ratio))
        if scaled != base_qty:
            logger.info(
                "UnifiedSignalEngine [%s]: scaled position %d → %d (heat %.2f%%/%.2f%%)",
                self.db_symbol,
                base_qty,
                scaled,
                current_heat * 100,
                heat_cap * 100,
            )
        return scaled

    # ------------------------------------------------------------------
    # Trade entry
    # ------------------------------------------------------------------

    def _aggregate_net_direction(self, open_trades: list[dict]) -> tuple[int, int]:
        """Return (bullish_contracts, bearish_contracts) across all open trades."""
        bull = sum(t["quantity_open"] for t in open_trades if t["direction"] == "bullish")
        bear = sum(t["quantity_open"] for t in open_trades if t["direction"] == "bearish")
        return bull, bear

    def _should_open_for_environment(
        self, score: ScoreSnapshot, open_trades: list[dict], exposure: dict
    ) -> tuple[bool, str]:
        """Decide whether a new trade improves the aggregate position for the current environment.

        Considers the net direction of all live trades.  If the aggregate is
        already aligned with the score direction, only open if the signal is
        very strong.  If the aggregate is opposed, a new trade acts as a
        hedge or rebalance and is allowed at the normal threshold.
        """
        bull_qty, bear_qty = self._aggregate_net_direction(open_trades)
        net_qty = bull_qty - bear_qty  # positive = net bullish

        if score.direction == "bullish" and net_qty > 0:
            # Already net bullish — only add more if signal is exceptionally strong
            if score.normalized_score < 0.80:
                return False, (
                    f"aggregate already net bullish ({bull_qty}B/{bear_qty}S); "
                    f"score {score.normalized_score:.3f} < 0.80 threshold to add more"
                )
        elif score.direction == "bearish" and net_qty < 0:
            # Already net bearish — same logic
            if score.normalized_score < 0.80:
                return False, (
                    f"aggregate already net bearish ({bull_qty}B/{bear_qty}S); "
                    f"score {score.normalized_score:.3f} < 0.80 threshold to add more"
                )

        return True, "ok"

    def _open_trade(self, score: ScoreSnapshot, market_ctx: dict, exposure: dict, open_trades: list[dict], conn=None) -> bool:
        # Dynamic trigger threshold: raise bar in high-IV environments (>70th percentile)
        # where options premium is expensive and signals are noisier.
        iv_rank = market_ctx.get("iv_rank")
        effective_threshold = self.trigger_threshold
        if iv_rank is not None:
            if iv_rank > 0.70:
                effective_threshold = min(0.72, self.trigger_threshold + 0.07)
            elif iv_rank < 0.25:
                effective_threshold = max(0.52, self.trigger_threshold - 0.04)

        if score.direction == "neutral" or score.normalized_score < effective_threshold:
            return False

        # Trend consistency check: require that recent signal_scores history agrees with
        # the current direction.  A sudden flip after sustained opposite scoring is likely
        # noise rather than a genuine regime change.
        if not self._score_trend_confirmation(score.direction, score.timestamp, conn=conn):
            logger.info(
                "UnifiedSignalEngine [%s]: recent score history contradicts direction=%s — skipping trade",
                self.db_symbol,
                score.direction,
            )
            return False

        # Aggregate exposure gate — check portfolio-level limits before sizing.
        allowed, reason = self._check_exposure_allows_entry(
            exposure, score.direction, score.timestamp
        )
        if not allowed:
            logger.info(
                "UnifiedSignalEngine [%s]: entry blocked — %s",
                self.db_symbol,
                reason,
            )
            return False

        # Aggregate net-position optimization: only open when the new trade
        # improves or rebalances the portfolio for the current environment.
        env_ok, env_reason = self._should_open_for_environment(score, open_trades, exposure)
        if not env_ok:
            logger.info(
                "UnifiedSignalEngine [%s]: entry blocked — %s",
                self.db_symbol,
                env_reason,
            )
            return False

        selected = None
        spot = float(market_ctx["close"])
        optimizer_pick = self._select_optimizer_candidate(score, market_ctx, conn=conn)
        if optimizer_pick:
            candidate = optimizer_pick["candidate"]
            sizing = next((p for p in candidate.sizing_profiles if p.profile == "optimal"), None)
            if sizing and sizing.contracts > 0:
                entry_price = (candidate.entry_debit or candidate.entry_credit) / 100.0
                opt_type = "C" if score.direction == "bullish" else "P"
                legs = self._legs_from_candidate(
                    {
                        "strategy_type": candidate.strategy_type,
                        "strikes": candidate.strikes,
                        "expiry": candidate.expiry,
                    }
                )
                enriched_legs = []
                for leg in legs:
                    option_symbol = self._resolve_option_symbol_for_leg(score.timestamp, leg, conn=conn)
                    enriched_legs.append({**leg, "option_symbol": option_symbol})
                selected = {
                    "option_symbol": enriched_legs[0]["option_symbol"] if enriched_legs else f"{self.db_symbol}-SYNTHETIC",
                    "expiration": candidate.expiry,
                    "strike": round(spot, 4),
                    "entry_mark": entry_price,
                    "option_type": opt_type,
                    "quantity": int(sizing.contracts),
                    "optimizer_payload": {
                        "strategy_type": candidate.strategy_type,
                        "pricing_mode": "debit" if candidate.entry_debit > 0 else "credit",
                        "strikes": candidate.strikes,
                        "expiry": str(candidate.expiry),
                        "legs": enriched_legs,
                        "probability_of_profit": candidate.probability_of_profit,
                        "expected_value": candidate.expected_value,
                        "signal_timeframe": optimizer_pick["signal_timeframe"],
                        "signal_strength": optimizer_pick["signal_strength"],
                    },
                }
        if not selected:
            contract = self._select_contract(score.timestamp, score.direction, spot, conn=conn)
            if not contract:
                return False
            selected = {
                **contract,
                "quantity": 1,
                "optimizer_payload": {
                    "strategy_type": "single_leg_fallback",
                    "pricing_mode": "single_leg",
                    "strikes": str(contract["strike"]),
                    "expiry": str(contract["expiration"]),
                    "legs": [{"side": "long", "option_type": contract["option_type"], "strike": contract["strike"], "option_symbol": contract["option_symbol"], "expiry": str(contract["expiration"])}],
                    "probability_of_profit": None,
                    "expected_value": None,
                },
            }

        quantity = self._scale_size_for_exposure(selected["quantity"], exposure)
        components_at_entry = dict(score.components)
        components_at_entry["optimizer"] = selected["optimizer_payload"]
        with self._use_conn(conn) as conn:
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
                    score.timestamp,
                    score.timestamp,
                    score.direction,
                    score.composite_score,
                    selected["option_symbol"],
                    selected["option_type"],
                    selected["expiration"],
                    selected["strike"],
                    selected["entry_mark"],
                    selected["entry_mark"],
                    quantity,
                    quantity,
                    json.dumps(components_at_entry, default=str),
                ),
            )
            conn.commit()
            return cur.rowcount > 0

    def _latest_trade_mark(self, trade: dict, as_of: datetime, conn=None) -> Optional[float]:
        meta = trade.get("components_at_entry") or {}
        optimizer_meta = meta.get("optimizer") if isinstance(meta, dict) else None
        legs = optimizer_meta.get("legs") if isinstance(optimizer_meta, dict) else None
        pricing_mode = optimizer_meta.get("pricing_mode") if isinstance(optimizer_meta, dict) else None
        if legs and pricing_mode in {"debit", "credit"}:
            total = 0.0
            for leg in legs:
                option_symbol = leg.get("option_symbol")
                if not option_symbol:
                    return self._latest_option_mark(trade["option_symbol"], as_of, conn=conn)
                leg_mark = self._latest_option_mark(option_symbol, as_of, conn=conn)
                if leg_mark is None:
                    return self._latest_option_mark(trade["option_symbol"], as_of, conn=conn)
                total += leg_mark if leg.get("side") == "long" else -leg_mark
            if pricing_mode == "credit":
                total = -total
            return max(total, 0.0)
        return self._latest_option_mark(trade["option_symbol"], as_of, conn=conn)

    def _update_open_trade(self, trade: dict, score: ScoreSnapshot, exposure: dict, conn=None) -> None:
        mark = self._latest_trade_mark(trade, score.timestamp, conn=conn)
        if mark is None:
            return

        qty_open = trade["quantity_open"]
        entry = trade["entry_price"]
        realized = trade["realized_pnl"]

        # Determine pricing mode from the trade metadata for options-appropriate stops/targets.
        meta = trade.get("components_at_entry") or {}
        optimizer_meta = meta.get("optimizer") if isinstance(meta, dict) else {}
        pricing_mode = optimizer_meta.get("pricing_mode") if isinstance(optimizer_meta, dict) else "single_leg"

        # Configurable stop-loss: SIGNALS_STOP_LOSS_PCT is negative (e.g. -0.25 = -25%).
        stop_loss_frac = abs(SIGNALS_STOP_LOSS_PCT)
        if pricing_mode == "credit":
            # Credit: stop when mark rises by stop_loss_frac of entry (e.g. 1.25× entry).
            stop = entry * (1.0 + stop_loss_frac)
            target_trim = entry * 0.50
        else:
            # Debit/single-leg: stop when mark drops by stop_loss_frac of entry (e.g. 0.75× entry).
            stop = entry * (1.0 - stop_loss_frac)
            target_trim = entry * 2.0

        # --- Portfolio-aware: tighten stops when aggregate heat is excessive ---
        over_heat = exposure["heat_pct"] > SIGNALS_MAX_PORTFOLIO_HEAT_PCT
        if over_heat:
            tighter_frac = stop_loss_frac * 0.6  # 60% of normal stop distance
            if pricing_mode == "credit":
                stop = entry * (1.0 + tighter_frac)
            else:
                stop = entry * (1.0 - tighter_frac)

        # Cut half if score deteriorates significantly — signal no longer supports the trade.
        # When portfolio is over heat cap, use a looser trigger so we shed risk faster.
        panic_threshold = 0.45 if over_heat else 0.35
        if score.normalized_score < panic_threshold and qty_open > 1:
            cut_pct = 0.75 if over_heat else 0.50
            cut_qty = max(1, int(qty_open * cut_pct))
            realized += (mark - entry) * cut_qty * 100
            qty_open -= cut_qty

        # Trim into strength at target.
        if pricing_mode == "credit":
            trim_condition = mark <= target_trim and qty_open > 1  # Credit: mark fell (profit)
        else:
            trim_condition = mark >= target_trim and qty_open > 1  # Debit: mark rose (profit)

        if trim_condition:
            trim_qty = max(1, qty_open // 2)
            realized += (mark - entry) * trim_qty * 100
            qty_open -= trim_qty

        status = "open"
        closed_at = None

        # Hard stop or opposite signal closes.
        if pricing_mode == "credit":
            stop_hit = mark >= stop  # Credit: stop when premium rose past threshold (loss)
        else:
            stop_hit = mark <= stop  # Debit: stop when premium fell past threshold (loss)

        if stop_hit or (score.direction != "neutral" and score.direction != trade["direction"] and score.normalized_score >= 0.55):
            realized += (mark - entry) * qty_open * 100
            qty_open = 0
            status = "closed"
            closed_at = score.timestamp

        unrealized = (mark - entry) * qty_open * 100
        total = realized + unrealized
        basis_qty = max(trade["quantity_initial"], 1)
        pnl_pct = ((total / (entry * basis_qty * 100)) * 100) if entry > 0 else 0

        with self._use_conn(conn) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE signal_trades
                SET current_price = %s,
                    quantity_open = %s,
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
        with db_connection() as conn:
            ctx = self._fetch_market_context(conn=conn)
            if not ctx:
                logger.warning("UnifiedSignalEngine: missing market context")
                return False

            market_ctx = MarketContext(
                timestamp=ctx["timestamp"],
                underlying=self.db_symbol,
                close=ctx["close"],
                net_gex=ctx["net_gex"],
                gamma_flip=ctx["gamma_flip"],
                put_call_ratio=ctx["put_call_ratio"],
                max_pain=ctx["max_pain"],
                smart_call=ctx["smart_call"],
                smart_put=ctx["smart_put"],
                recent_closes=ctx["recent_closes"],
                iv_rank=ctx.get("iv_rank"),
            )
            score = self.scoring_engine.score_and_persist(market_ctx, conn=conn)

            open_trades = self._fetch_open_trades(conn=conn)
            exposure = self._get_portfolio_exposure(open_trades, conn=conn)

            for trade in open_trades:
                self._update_open_trade(trade, score, exposure, conn=conn)

            # Re-fetch open trades after updates (some may have been closed).
            open_trades = self._fetch_open_trades(conn=conn)
            exposure = self._get_portfolio_exposure(open_trades, conn=conn)

            opened = self._open_trade(score, ctx, exposure, open_trades, conn=conn)
            logger.info(
                "UnifiedSignalEngine [%s] score=%.3f norm=%.3f dir=%s "
                "open_trades=%d heat=%.2f%% opened_new=%s",
                self.db_symbol,
                score.composite_score,
                score.normalized_score,
                score.direction,
                len(open_trades),
                exposure["heat_pct"] * 100,
                opened,
            )
            return True
