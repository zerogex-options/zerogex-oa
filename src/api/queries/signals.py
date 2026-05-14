"""Signals-related database query methods.

Extracted from ``src/api/database.py`` so the main DatabaseManager module
stays under a manageable size.  All methods here were mixed-in into the
``DatabaseManager`` class; they continue to work unchanged because
``DatabaseManager`` now inherits from ``SignalsQueriesMixin``.

These methods rely on instance state defined on DatabaseManager:
``_acquire_connection()``, ``_cache_get``, ``_cache_set``,
``_decode_json_field``, and the module-level ``SIGNAL_HISTORY_LIMIT`` /
``_ET`` from ``src/api/database.py``.  The import at the bottom of this
file preserves that access without creating a circular dependency.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, date, time, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
# Must match the module constants in src/api/database.py; duplicated here
# so these methods remain self-contained.  Keep the two in sync.
SIGNAL_HISTORY_LIMIT = 600
SIGNAL_HISTORY_LOOKBACK_DAYS = 4


def _two_session_cutoff(now: Optional[datetime] = None) -> datetime:
    """Return the ET timestamp marking the start of the older of the two
    most-recent regular trading sessions (09:30 ET).

    Used as the lower bound for Event Timeline queries so each Signal's
    response covers a consistent window — the current session plus the
    immediately preceding one when live in an open session, or the two
    most-recent fully-elapsed sessions otherwise — regardless of how
    often each signal emits scores.

    Weekday-only; does not consult a US holiday calendar (matches the
    convention used by ``_get_session_bounds`` in ``src/api/database.py``).
    """
    now_et = (now or datetime.now(_ET)).astimezone(_ET)
    today = now_et.date()
    market_open = time(9, 30)

    def prev_trading_day(d: date) -> date:
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    is_weekday = today.weekday() < 5
    past_open = now_et.time() >= market_open
    current_session_date = today if (is_weekday and past_open) else prev_trading_day(today)
    prior_date = prev_trading_day(current_session_date)
    return datetime(prior_date.year, prior_date.month, prior_date.day, 9, 30, tzinfo=_ET)


class SignalsQueriesMixin:
    """Read-side methods for the signals feature.

    Inherits nothing; relies on sibling ``DatabaseManager`` state via
    ``self`` when mixed in.  See module docstring for the required
    instance-level interface.
    """

    async def get_trade_signal(
        self,
        symbol: str = "SPY",
        timeframe: str = "intraday",
    ) -> Optional[Dict[str, Any]]:
        """
        Return the most recent trade_signals row for this symbol + timeframe.
        Falls back to the previous row if the latest is >10 min stale.
        """
        query = """
            SELECT
                underlying,
                timestamp,
                timeframe,
                composite_score,
                100 AS max_possible_score,
                normalized_score,
                direction,
                strength,
                estimated_win_pct,
                trade_type,
                trade_rationale,
                target_expiry,
                suggested_strikes,
                current_price,
                net_gex,
                gamma_flip,
                CASE WHEN gamma_flip IS NOT NULL AND gamma_flip <> 0
                     THEN ROUND(((current_price - gamma_flip) / gamma_flip) * 100, 4)
                     ELSE NULL END AS price_vs_flip,
                NULL::numeric AS vwap,
                vwap_deviation_pct,
                put_call_ratio,
                dealer_net_delta,
                direction AS smart_money_direction,
                false AS unusual_volume_detected,
                NULL::text AS orb_breakout_direction,
                components
            FROM consolidated_trade_signals
            WHERE underlying = $1
              AND timeframe  = $2
            ORDER BY timestamp DESC
            LIMIT 1
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol, timeframe)
                if not row:
                    return None
                d = dict(row)
                # components is stored as JSONB; asyncpg returns it as a string
                if isinstance(d.get("components"), str):
                    d["components"] = json.loads(d["components"])
                return d
        except Exception as e:
            logger.error(f"get_trade_signal failed ({symbol}, {timeframe}): {e}")
            return None

    async def get_signal_accuracy(
        self,
        symbol: str = "SPY",
        lookback_days: int = 30,
    ) -> Dict[str, Any]:
        """
        Return calibrated win rates from signal_accuracy for all timeframes
        and strength buckets over the requested lookback window.

        Shape:
        {
          "intraday":  {"high": {"total": N, "correct": M, "win_pct": 0.68}, ...},
          "swing":     {...},
          "multi_day": {...},
        }
        """
        query = """
            SELECT
                timeframe,
                strength_bucket,
                SUM(total_signals)::int   AS total,
                SUM(correct_signals)::int AS correct
            FROM consolidated_signal_accuracy
            WHERE underlying  = $1
              AND trade_date  >= CURRENT_DATE - ($2 * INTERVAL '1 day')
            GROUP BY timeframe, strength_bucket
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, lookback_days)
            result: Dict[str, Any] = {}
            for row in rows:
                tf = row["timeframe"]
                sb = row["strength_bucket"]
                tot = row["total"] or 0
                cor = row["correct"] or 0
                result.setdefault(tf, {})[sb] = {
                    "total": tot,
                    "correct": cor,
                    "win_pct": round(cor / tot, 4) if tot > 0 else None,
                }
            return result
        except Exception as e:
            logger.error(f"get_signal_accuracy failed: {e}")
            return {}

    async def _get_component_score_history(
        self,
        conn: asyncpg.Connection,
        symbol: str,
        component_name: str,
        limit: int = SIGNAL_HISTORY_LIMIT,
        lookback_days: int = SIGNAL_HISTORY_LOOKBACK_DAYS,
    ) -> list[Dict[str, Any]]:
        # Returns newest-first to match the convention used by the rest
        # of the timeseries APIs (sparkline clients should sort by
        # timestamp explicitly if a chronological draw order is needed).
        # Bounded by both a row LIMIT and a calendar-day lookback: the
        # lookback guarantees the response spans the previous trading
        # session even on Monday morning (when ``two sessions back``
        # straddles the weekend), and the LIMIT caps dense signals so a
        # signal that flips on every cycle doesn't return a runaway list.
        rows = await conn.fetch(
            """
            SELECT timestamp, clamped_score
            FROM signal_component_scores
            WHERE underlying = $1
              AND component_name = $2
              AND timestamp >= NOW() - make_interval(days => $4)
            ORDER BY timestamp DESC
            LIMIT $3
            """,
            symbol,
            component_name,
            limit,
            lookback_days,
        )
        return [
            {
                "timestamp": row["timestamp"],
                "score": round(float(row["clamped_score"] or 0.0) * 100.0, 2),
            }
            for row in rows
        ]

    async def get_vol_expansion_signal(
        self,
        symbol: str = "SPY",
    ) -> Optional[Dict[str, Any]]:
        """Return the most recent vol_expansion component score for this symbol.

        Reads from signal_component_scores (populated by VolExpansionSignal
        via AdvancedSignalEngine) and returns the raw score scaled to [0, 100].
        """
        query = """
            SELECT
                scs.underlying,
                scs.timestamp,
                scs.clamped_score,
                scs.weighted_score,
                scs.weight,
                scs.context_values,
                CASE
                    WHEN scs.clamped_score > 0 THEN 'bullish'
                    WHEN scs.clamped_score < 0 THEN 'bearish'
                    ELSE 'neutral'
                END AS direction
            FROM signal_component_scores scs
            WHERE scs.underlying = $1
              AND scs.component_name = 'vol_expansion'
            ORDER BY scs.timestamp DESC
            LIMIT 1
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol)
                if not row:
                    return None
                d = dict(row)
                raw = d.get("clamped_score") or 0.0
                d["score"] = round(float(raw) * 100.0, 2)
                ctx = d.get("context_values") or {}
                if isinstance(ctx, str):
                    ctx = json.loads(ctx)
                d["context_values"] = ctx
                d["score_history"] = await self._get_component_score_history(
                    conn,
                    symbol=symbol,
                    component_name="vol_expansion",
                    limit=SIGNAL_HISTORY_LIMIT,
                )
                return d
        except Exception as e:
            logger.error(f"get_vol_expansion_signal failed ({symbol}): {e}")
            return None

    async def get_eod_pressure_signal(
        self,
        symbol: str = "SPY",
    ) -> Optional[Dict[str, Any]]:
        """Return the most recent eod_pressure component score for this symbol.

        Reads from signal_component_scores (populated by EODPressureSignal
        via AdvancedSignalEngine). The signal is gated off before 14:30 ET, so
        a score of 0.0 with context_values.time_ramp == 0 means "outside
        window" rather than "no data".
        """
        query = """
            SELECT
                scs.underlying,
                scs.timestamp,
                scs.clamped_score,
                scs.weighted_score,
                scs.weight,
                scs.context_values,
                CASE
                    WHEN scs.clamped_score > 0 THEN 'bullish'
                    WHEN scs.clamped_score < 0 THEN 'bearish'
                    ELSE 'neutral'
                END AS direction
            FROM signal_component_scores scs
            WHERE scs.underlying = $1
              AND scs.component_name = 'eod_pressure'
            ORDER BY scs.timestamp DESC
            LIMIT 1
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol)
                if not row:
                    return None
                d = dict(row)
                raw = d.get("clamped_score") or 0.0
                d["score"] = round(float(raw) * 100.0, 2)
                ctx = d.get("context_values") or {}
                if isinstance(ctx, str):
                    ctx = json.loads(ctx)
                d["context_values"] = ctx
                d["score_history"] = await self._get_component_score_history(
                    conn,
                    symbol=symbol,
                    component_name="eod_pressure",
                    limit=SIGNAL_HISTORY_LIMIT,
                )
                return d
        except Exception as e:
            logger.error(f"get_eod_pressure_signal failed ({symbol}): {e}")
            return None

    async def get_advanced_signal(
        self,
        symbol: str = "SPY",
        signal_name: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Return the most recent advanced signal from signal_component_scores."""
        query = """
            SELECT
                scs.underlying,
                scs.timestamp,
                scs.clamped_score,
                scs.weighted_score,
                scs.weight,
                scs.context_values,
                CASE
                    WHEN scs.clamped_score > 0 THEN 'bullish'
                    WHEN scs.clamped_score < 0 THEN 'bearish'
                    ELSE 'neutral'
                END AS direction
            FROM signal_component_scores scs
            WHERE scs.underlying = $1
              AND scs.component_name = $2
            ORDER BY scs.timestamp DESC
            LIMIT 1
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol, signal_name)
                if not row:
                    return None
                d = dict(row)
                raw = d.get("clamped_score") or 0.0
                d["score"] = round(float(raw) * 100.0, 2)
                ctx = d.get("context_values") or {}
                if isinstance(ctx, str):
                    ctx = json.loads(ctx)
                d["context_values"] = ctx
                d["score_history"] = await self._get_component_score_history(
                    conn,
                    symbol=symbol,
                    component_name=signal_name,
                    limit=SIGNAL_HISTORY_LIMIT,
                )
                return d
        except Exception as e:
            logger.error(f"get_advanced_signal failed ({symbol}, {signal_name}): {e}")
            return None

    async def get_basic_signal(
        self,
        symbol: str = "SPY",
        signal_name: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Return the most recent basic signal from signal_component_scores.

        Mirrors :meth:`get_advanced_signal` — the two groups share the same
        table, distinguished only by ``component_name``.
        """
        return await self.get_advanced_signal(symbol=symbol, signal_name=signal_name)

    async def get_signal_events(
        self,
        symbol: str = "SPY",
        signal_name: str = "",
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        """Return recent emitted signal_events rows with their realized outcomes."""
        query = """
            SELECT
                id,
                underlying,
                signal_name,
                emitted_at,
                direction,
                score,
                context_values,
                close_at_emit,
                close_30m,
                close_60m,
                close_120m,
                outcome_30m,
                outcome_60m,
                outcome_120m
            FROM signal_events
            WHERE underlying = $1
              AND ($2 = '' OR signal_name = $2)
            ORDER BY emitted_at DESC
            LIMIT $3
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, signal_name, limit)
                out: list[Dict[str, Any]] = []
                for row in rows:
                    d = dict(row)
                    ctx = d.get("context_values") or {}
                    if isinstance(ctx, str):
                        ctx = json.loads(ctx)
                    d["context_values"] = ctx
                    out.append(d)
                return out
        except Exception as e:
            logger.error(f"get_signal_events failed ({symbol}, {signal_name}): {e}")
            return []

    async def get_signal_hit_rate(
        self,
        symbol: str = "SPY",
        signal_name: str = "",
        horizon: str = "60m",
    ) -> Optional[Dict[str, Any]]:
        """Return realized hit-rate of a signal over a given outcome horizon.

        ``horizon`` must be one of "30m", "60m", "120m".
        """
        if horizon not in {"30m", "60m", "120m"}:
            return None
        col = f"outcome_{horizon}"
        query = f"""
            SELECT
                COUNT(*)::int AS total,
                COUNT(*) FILTER (WHERE {col} = 'win')::int AS wins,
                COUNT(*) FILTER (WHERE {col} = 'loss')::int AS losses,
                COUNT(*) FILTER (WHERE {col} IS NULL)::int AS pending,
                AVG(CASE WHEN close_at_emit > 0 AND close_{horizon} IS NOT NULL
                         THEN (close_{horizon} - close_at_emit) / close_at_emit * direction
                         ELSE NULL END) AS avg_signed_return
            FROM signal_events
            WHERE underlying = $1
              AND signal_name = $2
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol, signal_name)
                if not row:
                    return None
                d = dict(row)
                total = d.get("total") or 0
                resolved = total - (d.get("pending") or 0)
                d["resolved"] = resolved
                d["hit_rate"] = round(d["wins"] / resolved, 4) if resolved > 0 else None
                d["horizon"] = horizon
                d["signal_name"] = signal_name
                d["underlying"] = symbol
                d["avg_signed_return"] = (
                    float(d["avg_signed_return"])
                    if d.get("avg_signed_return") is not None
                    else None
                )
                return d
        except Exception as e:
            logger.error(f"get_signal_hit_rate failed ({symbol}, {signal_name}): {e}")
            return None

    async def get_latest_advanced_signals_bundle(
        self,
        symbol: str = "SPY",
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Return the latest row for each advanced signal for a symbol.

        Used by the confluence-matrix endpoint to score cross-signal agreement
        in a single DB round-trip.
        """
        query = """
            SELECT DISTINCT ON (component_name)
                component_name,
                timestamp,
                clamped_score,
                weighted_score,
                weight,
                context_values
            FROM signal_component_scores
            WHERE underlying = $1
              AND component_name IN (
                'vol_expansion','eod_pressure','squeeze_setup',
                'trap_detection','zero_dte_position_imbalance',
                'gamma_vwap_confluence'
              )
            ORDER BY component_name, timestamp DESC
        """
        out: Dict[str, Optional[Dict[str, Any]]] = {
            "vol_expansion": None,
            "eod_pressure": None,
            "squeeze_setup": None,
            "trap_detection": None,
            "zero_dte_position_imbalance": None,
            "gamma_vwap_confluence": None,
        }
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol)
                for row in rows:
                    d = dict(row)
                    ctx = d.get("context_values") or {}
                    if isinstance(ctx, str):
                        ctx = json.loads(ctx)
                    d["context_values"] = ctx
                    raw = d.get("clamped_score") or 0.0
                    d["score"] = round(float(raw) * 100.0, 2)
                    out[d["component_name"]] = d
                return out
        except Exception as e:
            logger.error(f"get_latest_advanced_signals_bundle failed ({symbol}): {e}")
            return out

    async def get_latest_basic_signals_bundle(
        self,
        symbol: str = "SPY",
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Return the latest row for each basic signal for a symbol.

        Basic signals share ``signal_component_scores`` with the MSI
        components and Advanced Signals, distinguished by ``component_name``.
        """
        query = """
            SELECT DISTINCT ON (component_name)
                component_name,
                timestamp,
                clamped_score,
                weighted_score,
                weight,
                context_values
            FROM signal_component_scores
            WHERE underlying = $1
              AND component_name IN (
                'tape_flow_bias','skew_delta','vanna_charm_flow',
                'dealer_delta_pressure','gex_gradient','positioning_trap'
              )
            ORDER BY component_name, timestamp DESC
        """
        out: Dict[str, Optional[Dict[str, Any]]] = {
            "tape_flow_bias": None,
            "skew_delta": None,
            "vanna_charm_flow": None,
            "dealer_delta_pressure": None,
            "gex_gradient": None,
            "positioning_trap": None,
        }
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol)
                for row in rows:
                    d = dict(row)
                    ctx = d.get("context_values") or {}
                    if isinstance(ctx, str):
                        ctx = json.loads(ctx)
                    d["context_values"] = ctx
                    raw = d.get("clamped_score") or 0.0
                    d["score"] = round(float(raw) * 100.0, 2)
                    out[d["component_name"]] = d
                return out
        except Exception as e:
            logger.error(f"get_latest_basic_signals_bundle failed ({symbol}): {e}")
            return out

    async def get_signal_component_events(
        self,
        symbol: str = "SPY",
        component_name: str = "",
        limit: int = 1000,
        horizon: str = "60m",
    ) -> list[Dict[str, Any]]:
        """Return per-component score time series with sign-flip events.

        Time range is bounded to the start of the older of the two most-recent
        trading sessions so every signal's Event Timeline covers a consistent
        window — the current session plus the previous one when live, or the
        two most-recent fully-elapsed sessions otherwise — regardless of how
        often each signal emits scores. ``limit`` is a safety cap on result
        size for unusually dense signals; the session cutoff is the primary
        bound.
        """
        horizon_interval = {"30m": "30 minutes", "60m": "60 minutes", "120m": "120 minutes"}.get(
            horizon, "60 minutes"
        )
        cutoff = _two_session_cutoff()
        query = f"""
            SELECT
                scs.underlying,
                scs.timestamp,
                scs.component_name,
                scs.clamped_score,
                scs.weighted_score,
                scs.weight,
                scs.context_values,
                q0.close AS close_at_timestamp,
                q1.close AS close_at_horizon
            FROM signal_component_scores scs
            LEFT JOIN LATERAL (
                SELECT close
                FROM underlying_quotes uq
                WHERE uq.symbol = scs.underlying
                  AND uq.timestamp <= scs.timestamp
                ORDER BY uq.timestamp DESC
                LIMIT 1
            ) q0 ON TRUE
            LEFT JOIN LATERAL (
                SELECT close
                FROM underlying_quotes uq
                WHERE uq.symbol = scs.underlying
                  AND uq.timestamp >= scs.timestamp + INTERVAL '{horizon_interval}'
                ORDER BY uq.timestamp ASC
                LIMIT 1
            ) q1 ON TRUE
            WHERE scs.underlying = $1
              AND scs.component_name = $2
              AND scs.timestamp >= $4
            ORDER BY scs.timestamp DESC
            LIMIT $3
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, component_name, limit, cutoff)
            # Compute sign-flips chronologically (oldest → newest), but
            # return newest → oldest to match the convention used by the
            # rest of the timeseries APIs.
            ordered = [dict(r) for r in reversed(rows)]
            out: list[Dict[str, Any]] = []
            prev_sign = 0
            flip_count = 0
            for row in ordered:
                ctx = row.get("context_values") or {}
                if isinstance(ctx, str):
                    ctx = json.loads(ctx)
                raw = float(row.get("clamped_score") or 0.0)
                score = round(raw * 100.0, 4)
                sign = 1 if raw > 0 else (-1 if raw < 0 else 0)
                sign_flip = prev_sign != 0 and sign != 0 and sign != prev_sign
                if sign_flip:
                    flip_count += 1
                out.append(
                    {
                        "underlying": row["underlying"],
                        "timestamp": row["timestamp"],
                        "component_name": row["component_name"],
                        "score": score,
                        "weighted_score": float(row.get("weighted_score") or 0.0),
                        "weight": float(row.get("weight") or 0.0),
                        "direction": (
                            "bullish" if sign > 0 else "bearish" if sign < 0 else "neutral"
                        ),
                        "direction_flip": sign_flip,
                        "inputs": ctx,
                        "horizon": horizon,
                        "close": (
                            float(row.get("close_at_timestamp"))
                            if row.get("close_at_timestamp") is not None
                            else None
                        ),
                        "horizon_close": (
                            float(row.get("close_at_horizon"))
                            if row.get("close_at_horizon") is not None
                            else None
                        ),
                        "realized_return": (
                            round(
                                (
                                    float(row.get("close_at_horizon"))
                                    - float(row.get("close_at_timestamp"))
                                )
                                / float(row.get("close_at_timestamp")),
                                6,
                            )
                            if row.get("close_at_timestamp")
                            and row.get("close_at_horizon") is not None
                            else None
                        ),
                    }
                )
                if sign != 0:
                    prev_sign = sign
            out.reverse()
            return out
        except Exception as e:
            logger.error(f"get_signal_component_events failed ({symbol}, {component_name}): {e}")
            return []

    async def get_signal_confluence_matrix(
        self,
        symbol: str,
        component_names: Optional[list[str]] = None,
        lookback: int = 240,
        neutral_epsilon: float = 0.02,
    ) -> Dict[str, Any]:
        """Return signal-by-signal agreement/disagreement over lookback (advanced signals only).

        Aggregates the agreement matrix entirely in SQL (rather than fetching
        every row and counting in Python) so only ~N²+N rows cross the wire
        per call.  Combined with the covering index
        ``idx_signal_component_scores_underlying_ts_comp_clamped_covering``
        defined in ``setup/database/schema.sql`` the read is an Index Only
        Scan with no heap fetches — the original CTE+LEFT-JOIN form pulled
        ``lookback × N`` rows back and re-counted them in Python, dwarfing
        the actual aggregation cost.  Result is also briefly cached
        (``_analytics_cache_ttl_seconds``) since the matrix only changes per
        scoring cycle.
        """
        if component_names is None:
            component_names = [
                "vol_expansion",
                "eod_pressure",
                "squeeze_setup",
                "trap_detection",
                "zero_dte_position_imbalance",
                "gamma_vwap_confluence",
            ]
        if not component_names:
            return {"component_order": [], "matrix": {}, "rows_analyzed": 0}

        cache_key = (
            "signal_confluence_matrix:"
            f"{symbol}:{lookback}:{neutral_epsilon}:"
            + ",".join(sorted(component_names))
        )
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        component_order = list(component_names)

        def _empty_pair() -> Dict[str, Any]:
            # Pre-filled zero cell — every (c1, c2) must appear in the output
            # even when no rows match, matching the prior behavior.
            return {
                "observations": 0,
                "active_observations": 0,
                "agreement_count": 0,
                "disagreement_count": 0,
                "neutral_count": 0,
                "agreement_ratio": None,
                "disagreement_ratio": None,
                "net_confluence": 0.0,
            }

        def _empty_regime() -> Dict[str, Any]:
            return {
                "observations": 0,
                "active_observations": 0,
                "agreement_count": 0,
                "disagreement_count": 0,
                "neutral_count": 0,
                "agreement_ratio": None,
                "disagreement_ratio": None,
            }

        matrix: Dict[str, Dict[str, Any]] = {
            c1: {c2: _empty_pair() for c2 in component_order} for c1 in component_order
        }
        component_vs_regime: Dict[str, Dict[str, Any]] = {
            c: _empty_regime() for c in component_order
        }

        # Single round-trip that does sign-bucketing + agreement counting
        # entirely in SQL.  Returns at most N² pair rows + N regime rows
        # + 1 meta row.
        #
        # ``recent`` drives a nested-loop join into ``signal_component_scores``
        # via the (underlying, timestamp, component_name) PK plus the new
        # ``INCLUDE (clamped_score)`` covering index — each of the ~lookback
        # outer rows becomes one Index Only Scan with no heap fetches.  The
        # old form returned every joined row to Python and re-counted them,
        # so the wire payload alone was orders of magnitude larger than the
        # final ~N²+N matrix needs.
        query = """
            WITH recent AS (
                SELECT timestamp, composite_score
                FROM signal_scores
                WHERE underlying = $1
                ORDER BY timestamp DESC
                LIMIT $2
            ),
            comp AS (
                SELECT
                    scs.timestamp,
                    scs.component_name,
                    CASE
                        WHEN scs.clamped_score >  $4 THEN 1
                        WHEN scs.clamped_score < -$4 THEN -1
                        ELSE 0
                    END AS sign
                FROM recent r
                JOIN signal_component_scores scs
                  ON scs.underlying = $1
                 AND scs.timestamp  = r.timestamp
                WHERE scs.component_name = ANY($3::text[])
            ),
            regime AS (
                SELECT
                    timestamp,
                    CASE
                        WHEN composite_score >  $4 THEN 1
                        WHEN composite_score < -$4 THEN -1
                        ELSE 0
                    END AS sign
                FROM recent
            ),
            pair_counts AS (
                SELECT
                    a.component_name AS c1,
                    b.component_name AS c2,
                    COUNT(*)::int                                                        AS observations,
                    COUNT(*) FILTER (WHERE a.sign = 0 OR b.sign = 0)::int                AS neutral_count,
                    COUNT(*) FILTER (WHERE a.sign <> 0 AND b.sign <> 0 AND a.sign  = b.sign)::int AS agreement_count,
                    COUNT(*) FILTER (WHERE a.sign <> 0 AND b.sign <> 0 AND a.sign <> b.sign)::int AS disagreement_count
                FROM comp a
                JOIN comp b USING (timestamp)
                GROUP BY a.component_name, b.component_name
            ),
            regime_counts AS (
                SELECT
                    c.component_name AS comp_name,
                    COUNT(*)::int                                                        AS observations,
                    COUNT(*) FILTER (WHERE c.sign = 0 OR r.sign = 0)::int                AS neutral_count,
                    COUNT(*) FILTER (WHERE c.sign <> 0 AND r.sign <> 0 AND c.sign  = r.sign)::int AS agreement_count,
                    COUNT(*) FILTER (WHERE c.sign <> 0 AND r.sign <> 0 AND c.sign <> r.sign)::int AS disagreement_count
                FROM comp c
                JOIN regime r USING (timestamp)
                GROUP BY c.component_name
            ),
            meta AS (
                SELECT MAX(timestamp) AS latest_ts, COUNT(*)::int AS sample_count
                FROM recent
            )
            SELECT 'pair' AS kind, c1 AS a, c2 AS b,
                   observations, neutral_count, agreement_count, disagreement_count,
                   NULL::timestamptz AS latest_ts, NULL::int AS sample_count
            FROM pair_counts
            UNION ALL
            SELECT 'regime', comp_name, NULL,
                   observations, neutral_count, agreement_count, disagreement_count,
                   NULL::timestamptz, NULL::int
            FROM regime_counts
            UNION ALL
            SELECT 'meta', NULL, NULL, NULL, NULL, NULL, NULL,
                   meta.latest_ts, meta.sample_count
            FROM meta
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(
                    query, symbol, lookback, component_names, neutral_epsilon
                )

            sample_count = 0
            latest_timestamp = None
            for row in rows:
                kind = row["kind"]
                if kind == "pair":
                    c1, c2 = row["a"], row["b"]
                    if c1 in matrix and c2 in matrix[c1]:
                        agree = row["agreement_count"] or 0
                        disagree = row["disagreement_count"] or 0
                        active = agree + disagree
                        matrix[c1][c2] = {
                            "observations": row["observations"] or 0,
                            "active_observations": active,
                            "agreement_count": agree,
                            "disagreement_count": disagree,
                            "neutral_count": row["neutral_count"] or 0,
                            "agreement_ratio": (
                                round(agree / active, 4) if active else None
                            ),
                            "disagreement_ratio": (
                                round(disagree / active, 4) if active else None
                            ),
                            "net_confluence": (
                                round((agree - disagree) / active, 4) if active else 0.0
                            ),
                        }
                elif kind == "regime":
                    comp = row["a"]
                    if comp in component_vs_regime:
                        agree = row["agreement_count"] or 0
                        disagree = row["disagreement_count"] or 0
                        active = agree + disagree
                        component_vs_regime[comp] = {
                            "observations": row["observations"] or 0,
                            "active_observations": active,
                            "agreement_count": agree,
                            "disagreement_count": disagree,
                            "neutral_count": row["neutral_count"] or 0,
                            "agreement_ratio": (
                                round(agree / active, 4) if active else None
                            ),
                            "disagreement_ratio": (
                                round(disagree / active, 4) if active else None
                            ),
                        }
                elif kind == "meta":
                    sample_count = row["sample_count"] or 0
                    latest_timestamp = row["latest_ts"]

            result = {
                "components": component_order,
                "matrix": matrix,
                "component_vs_regime": component_vs_regime,
                "sample_count": sample_count,
                "latest_timestamp": latest_timestamp,
            }
            self._cache_set(cache_key, result, self._confluence_matrix_cache_ttl_seconds)
            return result
        except Exception as e:
            logger.error(f"get_signal_confluence_matrix failed ({symbol}): {e}")
            return {
                "components": list(component_names),
                "matrix": {},
                "component_vs_regime": {},
                "sample_count": 0,
                "latest_timestamp": None,
            }

    async def get_position_optimizer_signal(
        self,
        symbol: str = "SPY",
    ) -> Optional[Dict[str, Any]]:
        """Return the most recent position optimizer signal for this symbol."""
        query = """
            SELECT
                underlying,
                timestamp,
                timestamp AS signal_timestamp,
                timeframe AS signal_timeframe,
                direction AS signal_direction,
                strength AS signal_strength,
                trade_type,
                current_price,
                composite_score,
                100 AS max_possible_score,
                normalized_score,
                top_strategy_type,
                (top_candidate::jsonb ->> 'expiry')::date AS top_expiry,
                COALESCE((top_candidate::jsonb ->> 'dte')::int, 0) AS top_dte,
                COALESCE(top_candidate::jsonb ->> 'strikes', '') AS top_strikes,
                COALESCE((top_candidate::jsonb ->> 'probability_of_profit')::numeric, 0) AS top_probability_of_profit,
                COALESCE((top_candidate::jsonb ->> 'expected_value')::numeric, 0) AS top_expected_value,
                COALESCE((top_candidate::jsonb ->> 'max_profit')::numeric, 0) AS top_max_profit,
                COALESCE((top_candidate::jsonb ->> 'max_loss')::numeric, 0) AS top_max_loss,
                COALESCE((top_candidate::jsonb ->> 'kelly_fraction')::numeric, 0) AS top_kelly_fraction,
                COALESCE((top_candidate::jsonb ->> 'sharpe_like_ratio')::numeric, 0) AS top_sharpe_like_ratio,
                COALESCE((top_candidate::jsonb ->> 'liquidity_score')::numeric, 0) AS top_liquidity_score,
                COALESCE((top_candidate::jsonb ->> 'market_structure_fit')::numeric, 0) AS top_market_structure_fit,
                '[]'::jsonb AS top_reasoning,
                jsonb_build_array(top_candidate::jsonb) AS candidates
            FROM consolidated_trade_signals
            WHERE underlying = $1
            ORDER BY timestamp DESC
            LIMIT 1
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol)
                if not row:
                    return None
                d = dict(row)
                for key in ("top_reasoning", "candidates"):
                    if isinstance(d.get(key), str):
                        d[key] = json.loads(d[key])
                return d
        except Exception as e:
            logger.error(f"get_position_optimizer_signal failed ({symbol}): {e}")
            return None

    async def get_position_optimizer_accuracy(
        self,
        symbol: str = "SPY",
        lookback_days: int = 30,
    ) -> Dict[str, Any]:
        """Return historical profitability / calibration stats for the position optimizer."""
        query = """
            SELECT
                signal_direction,
                strategy_type,
                SUM(total_signals)::int AS total,
                SUM(profitable_signals)::int AS profitable_signals,
                AVG(avg_realized_return_pct)::float AS avg_realized_return_pct,
                AVG(avg_expected_value)::float AS avg_expected_value,
                AVG(avg_predicted_pop)::float AS avg_predicted_pop,
                AVG(avg_realized_move_pct)::float AS avg_realized_move_pct
            FROM consolidated_position_accuracy
            WHERE underlying = $1
              AND trade_date >= CURRENT_DATE - ($2 * INTERVAL '1 day')
            GROUP BY signal_direction, strategy_type
            ORDER BY signal_direction, strategy_type
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, lookback_days)
            result: Dict[str, Any] = {}
            for row in rows:
                direction = row["signal_direction"]
                strategy = row["strategy_type"]
                total = row["total"] or 0
                profitable = row["profitable_signals"] or 0
                result.setdefault(direction, {})[strategy] = {
                    "total": total,
                    "profitable_signals": profitable,
                    "profitability_rate": round(profitable / total, 4) if total > 0 else None,
                    "avg_realized_return_pct": (
                        round(float(row["avg_realized_return_pct"]), 4)
                        if row["avg_realized_return_pct"] is not None
                        else None
                    ),
                    "avg_expected_value": (
                        round(float(row["avg_expected_value"]), 4)
                        if row["avg_expected_value"] is not None
                        else None
                    ),
                    "avg_predicted_pop": (
                        round(float(row["avg_predicted_pop"]), 4)
                        if row["avg_predicted_pop"] is not None
                        else None
                    ),
                    "avg_realized_move_pct": (
                        round(float(row["avg_realized_move_pct"]), 4)
                        if row["avg_realized_move_pct"] is not None
                        else None
                    ),
                }
            return result
        except Exception as e:
            logger.error(f"get_position_optimizer_accuracy failed: {e}")
            return {}

    async def get_signal_history(
        self, symbol: str = "SPY", limit: int = 100
    ) -> list[Dict[str, Any]]:
        """Return recent managed trade history with win/loss and realized P&L."""
        query = """
            SELECT
                id,
                underlying,
                timestamp,
                signal_timestamp,
                signal_timeframe,
                signal_direction,
                strategy_type,
                status,
                time_opened,
                time_closed,
                contracts,
                entry_price,
                current_mark,
                trade_cost,
                realized_pnl,
                unrealized_pnl,
                total_pnl,
                CASE WHEN total_pnl > 0 THEN 'win'
                     WHEN total_pnl < 0 THEN 'loss'
                     ELSE 'flat' END AS outcome,
                notes
            FROM signal_engine_trade_ideas
            WHERE underlying = $1
            ORDER BY timestamp DESC
            LIMIT $2
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"get_signal_history failed ({symbol}): {e}")
            return []

    async def get_current_signal_with_trades(
        self, symbol: str = "SPY", timeframe: str = "intraday"
    ) -> Optional[Dict[str, Any]]:
        """Return current consolidated signal plus active trade statuses."""
        signal_row = await self.get_trade_signal(symbol=symbol, timeframe=timeframe)
        if not signal_row:
            return None

        trades_query = """
            SELECT
                id,
                timestamp,
                status,
                time_opened,
                time_closed,
                signal_timeframe,
                signal_direction,
                strategy_type,
                strikes,
                contracts,
                entry_price,
                current_mark,
                stop_price,
                target_1,
                target_2,
                realized_pnl,
                unrealized_pnl,
                total_pnl,
                trade_cost
            FROM signal_engine_trade_ideas
            WHERE underlying = $1
              AND status IN ('position_open', 'partial_take_profit')
            ORDER BY timestamp DESC
        """

        try:
            async with self._acquire_connection() as conn:
                trades = await conn.fetch(trades_query, symbol)
            signal_row["active_trades"] = [dict(row) for row in trades]
            signal_row["has_active_trade"] = len(trades) > 0
            return signal_row
        except Exception as e:
            logger.error(f"get_current_signal_with_trades failed ({symbol}): {e}")
            signal_row["active_trades"] = []
            signal_row["has_active_trade"] = False
            return signal_row

    async def get_live_signal_trades(self) -> list[Dict[str, Any]]:
        query = """
            SELECT id, underlying, signal_timestamp, opened_at, updated_at,
                   status, direction, score_at_entry, score_latest,
                   option_symbol, option_type, expiration, strike,
                   entry_price, current_price, quantity_initial, quantity_open,
                   realized_pnl, unrealized_pnl, total_pnl, pnl_percent
            FROM signal_trades
            WHERE status = 'open'
            ORDER BY opened_at DESC
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query)
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"get_live_signal_trades failed: {e}")
            return []

    async def get_closed_signal_trades(self, limit: int = 500) -> list[Dict[str, Any]]:
        query = """
            SELECT id, underlying, signal_timestamp, opened_at, updated_at, closed_at,
                   status, direction, score_at_entry, score_latest,
                   option_symbol, option_type, expiration, strike,
                   entry_price, current_price, quantity_initial, quantity_open,
                   realized_pnl, unrealized_pnl, total_pnl, pnl_percent,
                   CASE WHEN total_pnl > 0 THEN 'win'
                        WHEN total_pnl < 0 THEN 'loss'
                        ELSE 'flat' END AS outcome
            FROM signal_trades
            WHERE status = 'closed'
            ORDER BY closed_at DESC NULLS LAST
            LIMIT $1
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, limit)
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"get_closed_signal_trades failed: {e}")
            return []

    async def get_latest_signal_score(self, symbol: str = "SPY") -> Optional[Dict[str, Any]]:
        query = """
            SELECT underlying, timestamp, composite_score, normalized_score, direction, components
            FROM signal_scores
            WHERE underlying = $1
            ORDER BY timestamp DESC
            LIMIT 1
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol)
                if not row:
                    return None
                d = dict(row)
                if isinstance(d.get("components"), str):
                    d["components"] = json.loads(d["components"])
                return d
        except Exception as e:
            logger.error(f"get_latest_signal_score failed ({symbol}): {e}")
            return None

    async def get_latest_signal_score_enriched(
        self, symbol: str = "SPY"
    ) -> Optional[Dict[str, Any]]:
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        ss.underlying,
                        ss.timestamp,
                        ss.composite_score,
                        ss.components
                    FROM signal_scores ss
                    WHERE ss.underlying = $1
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    symbol,
                )
                if not row:
                    return None

                row = dict(row)
                if isinstance(row.get("components"), str):
                    row["components"] = json.loads(row["components"])
                return row
        except Exception as e:
            logger.error(f"get_latest_signal_score_enriched failed ({symbol}): {e}")
            return None

    async def get_signal_score_history(
        self, symbol: str = "SPY", limit: int = SIGNAL_HISTORY_LIMIT
    ) -> list[Dict[str, Any]]:
        # Two-session lookback by calendar days plus a row cap. The composite
        # MSI is persisted every cycle so dense underlyings can produce many
        # thousands of rows over four days — LIMIT keeps the payload bounded.
        query = """
            SELECT
                ss.underlying,
                ss.timestamp,
                ss.composite_score,
                ss.components
            FROM signal_scores ss
            WHERE ss.underlying = $1
              AND ss.timestamp >= NOW() - make_interval(days => $3)
            ORDER BY ss.timestamp DESC
            LIMIT $2
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, limit, SIGNAL_HISTORY_LOOKBACK_DAYS)
                out = []
                for row in rows:
                    d = dict(row)
                    if isinstance(d.get("components"), str):
                        d["components"] = json.loads(d["components"])
                    out.append(d)
                return out
        except Exception as e:
            logger.error(f"get_signal_score_history failed ({symbol}): {e}")
            return []

    # ------------------------------------------------------------------
    # Playbook Action Cards (PR-3+)
    # ------------------------------------------------------------------

    async def get_signal_history(
        self,
        symbol: str,
        component_name: str,
        days_back: int = 21,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Return recent ``signal_component_scores`` rows for one signal.

        Used by the Playbook context-builder to populate
        ``SignalSnapshot.score_history`` for patterns that need multi-day
        aggregations (squeeze_setup 2-day sustained, vanna_charm_flow
        2-day sustained, skew_delta 20-day mean / new-low).

        Returns rows ordered oldest → newest with ``timestamp`` and
        ``clamped_score``.  Capped at ``limit`` rows to bound payload.
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT timestamp, clamped_score
                    FROM signal_component_scores
                    WHERE underlying = $1
                      AND component_name = $2
                      AND timestamp > NOW() - ($3::int * INTERVAL '1 day')
                    ORDER BY timestamp ASC
                    LIMIT $4
                    """,
                    symbol,
                    component_name,
                    int(days_back),
                    int(limit),
                )
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("get_signal_history failed (%s, %s): %s", symbol, component_name, exc)
            return []

    async def insert_action_card(self, card: Dict[str, Any]) -> None:
        """Persist a non-STAND_DOWN Action Card.

        Caller passes ``card.to_dict()`` from the Playbook engine.  Failures
        are logged but never raised — persistence is best-effort and should
        not break the API response path.
        """
        if not card or card.get("action") == "STAND_DOWN":
            return
        ts = card.get("timestamp")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                logger.warning("insert_action_card: bad timestamp %r", ts)
                return
        if ts is None:
            return
        try:
            async with self._acquire_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO signal_action_cards
                        (underlying, timestamp, pattern, action, tier,
                         direction, confidence, payload)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                    """,
                    card.get("underlying"),
                    ts,
                    card.get("pattern"),
                    card.get("action"),
                    card.get("tier") or "n/a",
                    card.get("direction") or "non_directional",
                    float(card.get("confidence") or 0.0),
                    json.dumps(card, default=str),
                )
        except Exception as exc:
            # Best-effort — don't surface persistence errors to API callers.
            logger.warning("insert_action_card failed (%s): %s", card.get("pattern"), exc)

    async def get_recent_action_cards(
        self, underlying: str, since_minutes: int = 90
    ) -> List[Dict[str, Any]]:
        """Return non-STAND_DOWN Cards emitted within the last ``since_minutes``.

        Used by the Playbook context-builder to populate ``recently_emitted``
        for hysteresis.  Returns the most recent row per pattern, newest first.
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT ON (pattern)
                        pattern, timestamp, action, tier, direction, confidence
                    FROM signal_action_cards
                    WHERE underlying = $1
                      AND timestamp > NOW() - ($2::int * INTERVAL '1 minute')
                    ORDER BY pattern, timestamp DESC
                    """,
                    underlying,
                    int(since_minutes),
                )
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("get_recent_action_cards failed (%s): %s", underlying, exc)
            return []
