"""Consolidated Signal Engine.

Combines trade-signal, volatility-expansion, and position-optimizer logic into
one unified signal + one unified historical accuracy stream.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Optional

import pytz

from src.analytics.signal_engine import (
    SignalEngine,
    WIN_PCT_DEFAULTS,
    _build_trade_idea,
    _max_possible,
    _normalize,
    _orb_direction,
    _score_components,
    _sm_direction,
    _to_direction,
    _to_strength,
)
from src.analytics.vol_expansion_engine import VolExpansionEngine
from src.signals.position_optimizer_engine import PositionOptimizerEngine
from src.database import db_connection
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)
ET = pytz.timezone("US/Eastern")


class ConsolidatedSignalEngine:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        self.trade_engine = SignalEngine(underlying=self.underlying)
        self.vol_engine = VolExpansionEngine(underlying=self.underlying)
        self.position_engine = PositionOptimizerEngine(underlying=self.underlying)
        self._last_accuracy_update: Optional[datetime.date] = None

    def _fetch_calibrated_win_pct(self, timeframe: str, strength: str, lookback_days: int = 30) -> Optional[float]:
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT SUM(total_signals), SUM(correct_signals)
                    FROM consolidated_signal_accuracy
                    WHERE underlying = %s
                      AND timeframe = %s
                      AND strength_bucket = %s
                      AND trade_date >= CURRENT_DATE - %s
                    """,
                    (self.db_symbol, timeframe, strength, lookback_days),
                )
                row = cur.fetchone()
                if row and row[0] and row[0] > 0:
                    return round(float(row[1]) / float(row[0]), 4)
        except Exception:
            logger.exception("ConsolidatedSignalEngine calibrated win pct lookup failed")
        return None

    @staticmethod
    def _direction_to_sign(direction: str) -> int:
        return 1 if direction == "bullish" else (-1 if direction == "bearish" else 0)

    def run_cycle(self) -> bool:
        tctx = self.trade_engine._fetch_context()
        if tctx is None:
            logger.warning("ConsolidatedSignalEngine: trade context unavailable")
            return False

        self.trade_engine._auto_tune_thresholds()
        self.vol_engine._auto_tune_thresholds()

        # Select best actionable timeframe from trade engine (intraday/swing priority).
        trade_tf_scores = []
        for tf in ("intraday", "swing", "multi_day"):
            composite, comps = _score_components(tctx, tf, self.trade_engine.thresholds)
            norm = _normalize(composite, tf)
            trade_tf_scores.append((tf, composite, norm, comps))
        timeframe, trade_composite, trade_norm, trade_components = max(trade_tf_scores, key=lambda x: abs(x[2]))
        trade_direction = _to_direction(trade_composite)
        trade_strength = _to_strength(trade_norm)

        vctx = self.vol_engine._fetch_context(as_of=tctx.timestamp)
        if vctx is None:
            logger.warning("ConsolidatedSignalEngine: vol context unavailable")
            return False
        vol_signal = self.vol_engine.compute_signal(vctx)

        pctx = self.position_engine._fetch_context(as_of=tctx.timestamp)
        if pctx is None:
            logger.warning("ConsolidatedSignalEngine: position context unavailable")
            return False
        pos_signal = self.position_engine.compute_signal(pctx)
        if pos_signal is None:
            logger.warning("ConsolidatedSignalEngine: position signal unavailable")
            return False

        # One consolidated prediction score.
        directional_blend = (
            (self._direction_to_sign(trade_direction) * trade_norm * 0.45)
            + (self._direction_to_sign(vol_signal.expected_direction) * vol_signal.normalized_score * 0.25)
            + (self._direction_to_sign(pos_signal.signal_direction) * pos_signal.normalized_score * 0.30)
        )
        composite_direction = _to_direction(1 if directional_blend > 0 else (-1 if directional_blend < 0 else 0))
        composite_normalized = round(abs(directional_blend), 4)
        composite_strength = _to_strength(composite_normalized)

        win_pct = self._fetch_calibrated_win_pct(timeframe, composite_strength) or WIN_PCT_DEFAULTS[timeframe][composite_strength]
        trade_type, rationale, expiry, strikes = _build_trade_idea(
            composite_direction,
            composite_strength,
            timeframe,
            tctx.net_gex > 0,
        )

        consolidated_components = {
            "trade_signal_components": [asdict(c) for c in trade_components],
            "volatility_components": [asdict(c) for c in vol_signal.components],
            "position_components": [asdict(c) for c in pos_signal.candidates[0].components],
        }
        top_candidate = asdict(pos_signal.candidates[0])

        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO consolidated_trade_signals (
                    underlying, timestamp, timeframe,
                    composite_score, normalized_score, direction, strength, estimated_win_pct,
                    trade_type, trade_rationale, target_expiry, suggested_strikes,
                    current_price, net_gex, gamma_flip, put_call_ratio, dealer_net_delta,
                    vwap_deviation_pct, move_probability, expected_magnitude_pct,
                    top_strategy_type, top_candidate, components
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s::jsonb
                )
                ON CONFLICT (underlying, timestamp) DO UPDATE SET
                    timeframe = EXCLUDED.timeframe,
                    composite_score = EXCLUDED.composite_score,
                    normalized_score = EXCLUDED.normalized_score,
                    direction = EXCLUDED.direction,
                    strength = EXCLUDED.strength,
                    estimated_win_pct = EXCLUDED.estimated_win_pct,
                    trade_type = EXCLUDED.trade_type,
                    trade_rationale = EXCLUDED.trade_rationale,
                    target_expiry = EXCLUDED.target_expiry,
                    suggested_strikes = EXCLUDED.suggested_strikes,
                    current_price = EXCLUDED.current_price,
                    net_gex = EXCLUDED.net_gex,
                    gamma_flip = EXCLUDED.gamma_flip,
                    put_call_ratio = EXCLUDED.put_call_ratio,
                    dealer_net_delta = EXCLUDED.dealer_net_delta,
                    vwap_deviation_pct = EXCLUDED.vwap_deviation_pct,
                    move_probability = EXCLUDED.move_probability,
                    expected_magnitude_pct = EXCLUDED.expected_magnitude_pct,
                    top_strategy_type = EXCLUDED.top_strategy_type,
                    top_candidate = EXCLUDED.top_candidate,
                    components = EXCLUDED.components,
                    updated_at = NOW()
                """,
                (
                    self.db_symbol,
                    tctx.timestamp,
                    timeframe,
                    round(directional_blend * 100, 2),
                    composite_normalized,
                    composite_direction,
                    composite_strength,
                    win_pct,
                    trade_type,
                    rationale,
                    expiry,
                    strikes,
                    tctx.current_price,
                    tctx.net_gex,
                    tctx.gamma_flip or None,
                    tctx.put_call_ratio,
                    tctx.dealer_net_delta,
                    tctx.vwap_deviation_pct,
                    vol_signal.move_probability,
                    vol_signal.expected_magnitude_pct,
                    top_candidate.get("strategy_type", "unknown"),
                    json.dumps(top_candidate, default=str),
                    json.dumps(consolidated_components, default=str),
                ),
            )
            conn.commit()

        self._update_accuracy()
        logger.info(
            "✅ Consolidated signal [%s] %s %s | tf=%s | score=%.2f | win_pct=%.0f%% | strategy=%s",
            self.db_symbol,
            composite_direction.upper(),
            composite_strength.upper(),
            timeframe,
            directional_blend,
            win_pct * 100,
            top_candidate.get("strategy_type", "unknown"),
        )
        return True

    def _update_accuracy(self) -> None:
        today = datetime.now(ET).date()
        if self._last_accuracy_update == today:
            return
        eval_date = today - timedelta(days=1)
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT timestamp, timeframe, direction, strength,
                           top_strategy_type,
                           COALESCE((top_candidate::jsonb ->> 'expected_value')::numeric, 0),
                           COALESCE((top_candidate::jsonb ->> 'probability_of_profit')::numeric, 0)
                    FROM consolidated_trade_signals
                    WHERE underlying = %s
                      AND DATE(timestamp AT TIME ZONE 'America/New_York') = %s
                    """,
                    (self.db_symbol, eval_date),
                )
                rows = cur.fetchall()
                if not rows:
                    self._last_accuracy_update = today
                    return

                cur.execute(
                    """
                    WITH day_quotes AS (
                        SELECT close, timestamp
                        FROM underlying_quotes
                        WHERE symbol = %s
                          AND DATE(timestamp AT TIME ZONE 'America/New_York') = %s
                    )
                    SELECT
                        (SELECT close FROM day_quotes ORDER BY timestamp ASC LIMIT 1),
                        (SELECT close FROM day_quotes ORDER BY timestamp DESC LIMIT 1)
                    """,
                    (self.db_symbol, eval_date + timedelta(days=1)),
                )
                px = cur.fetchone()
                if not px or px[0] is None or px[1] is None:
                    self._last_accuracy_update = today
                    return

                realized_dir = "bullish" if float(px[1]) > float(px[0]) else ("bearish" if float(px[1]) < float(px[0]) else "neutral")
                buckets: dict[tuple[str, str], dict[str, int]] = {}
                for _, tf, direction, strength, strategy_type, expected_ev, predicted_pop in rows:
                    key = (tf, strength)
                    b = buckets.setdefault(key, {"total": 0, "correct": 0})
                    b["total"] += 1
                    b["correct"] += int(direction == realized_dir)

                for (tf, strength), stats in buckets.items():
                    cur.execute(
                        """
                        INSERT INTO consolidated_signal_accuracy (
                            underlying, trade_date, timeframe, strength_bucket, total_signals, correct_signals, win_pct
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (underlying, trade_date, timeframe, strength_bucket) DO UPDATE SET
                            total_signals = EXCLUDED.total_signals,
                            correct_signals = EXCLUDED.correct_signals,
                            win_pct = EXCLUDED.win_pct,
                            updated_at = NOW()
                        """,
                        (
                            self.db_symbol,
                            eval_date,
                            tf,
                            strength,
                            stats["total"],
                            stats["correct"],
                            round(stats["correct"] / stats["total"], 4) if stats["total"] else None,
                        ),
                    )

                pos_buckets: dict[tuple[str, str], dict[str, float]] = {}
                for _, _, direction, _, strategy_type, expected_ev, predicted_pop in rows:
                    pkey = (direction, strategy_type or "unknown")
                    pb = pos_buckets.setdefault(pkey, {"total": 0, "profitable": 0, "ev_sum": 0.0, "pop_sum": 0.0})
                    pb["total"] += 1
                    pb["profitable"] += int(direction == realized_dir)
                    pb["ev_sum"] += float(expected_ev or 0.0)
                    pb["pop_sum"] += float(predicted_pop or 0.0)

                for (direction, strategy_type), stats in pos_buckets.items():
                    total = int(stats["total"] or 0)
                    if total == 0:
                        continue
                    cur.execute(
                        """
                        INSERT INTO consolidated_position_accuracy (
                            underlying, trade_date, signal_direction, strategy_type,
                            total_signals, profitable_signals, avg_realized_return_pct,
                            avg_expected_value, avg_predicted_pop, avg_realized_move_pct
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (underlying, trade_date, signal_direction, strategy_type) DO UPDATE SET
                            total_signals = EXCLUDED.total_signals,
                            profitable_signals = EXCLUDED.profitable_signals,
                            avg_realized_return_pct = EXCLUDED.avg_realized_return_pct,
                            avg_expected_value = EXCLUDED.avg_expected_value,
                            avg_predicted_pop = EXCLUDED.avg_predicted_pop,
                            avg_realized_move_pct = EXCLUDED.avg_realized_move_pct,
                            updated_at = NOW()
                        """,
                        (
                            self.db_symbol,
                            eval_date,
                            direction,
                            strategy_type,
                            total,
                            int(stats["profitable"]),
                            round((stats["profitable"] / total) * 100.0, 4),
                            round(stats["ev_sum"] / total, 4),
                            round(stats["pop_sum"] / total, 4),
                            round(abs(float(px[1]) - float(px[0])) / max(float(px[0]), 1e-6) * 100.0, 4),
                        ),
                    )
                conn.commit()
                self._last_accuracy_update = today
        except Exception:
            logger.exception("ConsolidatedSignalEngine accuracy update failed")
