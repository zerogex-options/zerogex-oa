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

import json
import logging
from datetime import datetime, timedelta, date, time
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    import asyncpg

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

    if TYPE_CHECKING:
        _acquire_connection: Callable[[], AbstractAsyncContextManager[Any]]
        _cache_get: Callable[[str], Optional[Any]]
        _cache_set: Callable[[str, Any, float], None]
        _confluence_matrix_cache_ttl_seconds: float

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
        # Map the validated horizon to fixed, hard-coded column identifiers.
        # Keeping the guard and the column names in one structure (rather than
        # interpolating f"outcome_{horizon}" 18 lines from the membership
        # check) means a future edit that adds a horizon cannot accidentally
        # introduce an injection path — the columns are literals here.
        _HORIZON_COLUMNS = {
            "30m": ("outcome_30m", "close_30m"),
            "60m": ("outcome_60m", "close_60m"),
            "120m": ("outcome_120m", "close_120m"),
        }
        cols = _HORIZON_COLUMNS.get(horizon)
        if cols is None:
            return None
        col, close_col = cols
        query = f"""
            SELECT
                COUNT(*)::int AS total,
                COUNT(*) FILTER (WHERE {col} = 'win')::int AS wins,
                COUNT(*) FILTER (WHERE {col} = 'loss')::int AS losses,
                COUNT(*) FILTER (WHERE {col} IS NULL)::int AS pending,
                AVG(CASE WHEN close_at_emit > 0 AND {close_col} IS NOT NULL
                         THEN ({close_col} - close_at_emit) / close_at_emit
                              * CASE direction
                                    WHEN 'bullish' THEN 1
                                    WHEN 'bearish' THEN -1
                                    ELSE 0
                                END
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
                # Hit rate is win share of *decided* events (win or loss).
                # Dividing by ``resolved`` would dilute the rate with any
                # non-win/non-loss outcome label (e.g. a flat/tie bucket).
                wins = d.get("wins") or 0
                losses = d.get("losses") or 0
                decided = wins + losses
                d["hit_rate"] = round(wins / decided, 4) if decided > 0 else None
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

        Perf: this is 6 LATERAL single-row lookups (one per component
        name), not a ``DISTINCT ON (component_name) ... ORDER BY
        component_name, timestamp DESC`` over the whole table.  The
        ``signal_component_scores`` table is written every signal-engine
        cycle (~1 row/s/component), so ``DISTINCT ON`` had to scan and
        sort every historical row for these 6 components and dedupe to
        6 — cost grew with the table all session (observed 0.6s → 6s →
        12s within one trading day).  Each LATERAL leg instead resolves
        to the same indexed ``component_name=… AND underlying=… ORDER BY
        timestamp DESC LIMIT 1`` lookup the per-signal endpoints use
        (idx_signal_component_scores_component_underlying_ts), so the
        bundle is constant-time regardless of table size.  A component
        with no rows yields no LATERAL row (CROSS JOIN drops it), which
        matches the old DISTINCT ON behavior — the caller pre-seeds all
        six keys to ``None``.
        """
        query = """
            SELECT
                c.component_name,
                s.timestamp,
                s.clamped_score,
                s.weighted_score,
                s.weight,
                s.context_values
            FROM (VALUES
                ('tape_flow_bias'),
                ('skew_delta'),
                ('vanna_charm_flow'),
                ('dealer_delta_pressure'),
                ('gex_gradient'),
                ('positioning_trap')
            ) AS c(component_name)
            CROSS JOIN LATERAL (
                SELECT
                    scs.timestamp,
                    scs.clamped_score,
                    scs.weighted_score,
                    scs.weight,
                    scs.context_values
                FROM signal_component_scores scs
                WHERE scs.underlying = $1
                  AND scs.component_name = c.component_name
                ORDER BY scs.timestamp DESC
                LIMIT 1
            ) s
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
                            float(row.get("close_at_timestamp"))  # type: ignore[arg-type]
                            if row.get("close_at_timestamp") is not None
                            else None
                        ),
                        "horizon_close": (
                            float(row.get("close_at_horizon"))  # type: ignore[arg-type]
                            if row.get("close_at_horizon") is not None
                            else None
                        ),
                        "realized_return": (
                            round(
                                (
                                    float(row.get("close_at_horizon"))  # type: ignore[arg-type]
                                    - float(row.get("close_at_timestamp"))  # type: ignore[arg-type]
                                )
                                / float(row.get("close_at_timestamp")),  # type: ignore[arg-type]
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
            f"{symbol}:{lookback}:{neutral_epsilon}:" + ",".join(sorted(component_names))
        )
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached  # type: ignore[no-any-return]

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
                -- Unordered distinct pairs only. Without this predicate the
                -- self-join also emits c1=c2 (a component always agrees
                -- with itself -> a fake 100% diagonal) and BOTH (X,Y) and
                -- (Y,X), so any aggregate over the matrix double-counts.
                WHERE a.component_name < b.component_name
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
                rows = await conn.fetch(query, symbol, lookback, component_names, neutral_epsilon)

            sample_count = 0
            latest_timestamp = None
            for row in rows:
                kind = row["kind"]
                if kind == "pair":
                    c1, c2 = row["a"], row["b"]
                    agree = row["agreement_count"] or 0
                    disagree = row["disagreement_count"] or 0
                    active = agree + disagree
                    cell = {
                        "observations": row["observations"] or 0,
                        "active_observations": active,
                        "agreement_count": agree,
                        "disagreement_count": disagree,
                        "neutral_count": row["neutral_count"] or 0,
                        "agreement_ratio": (round(agree / active, 4) if active else None),
                        "disagreement_ratio": (round(disagree / active, 4) if active else None),
                        "net_confluence": (
                            round((agree - disagree) / active, 4) if active else 0.0
                        ),
                    }
                    # SQL now returns each unordered pair once (c1 < c2).
                    # Confluence is symmetric, so mirror the cell into both
                    # off-diagonal positions. The diagonal stays at the
                    # pre-filled empty cell -- self-confluence is undefined,
                    # not a (misleading) perfect 100%.
                    if c1 in matrix and c2 in matrix[c1]:
                        matrix[c1][c2] = cell
                    if c2 in matrix and c1 in matrix[c2]:
                        matrix[c2][c1] = dict(cell)
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
                            "agreement_ratio": (round(agree / active, 4) if active else None),
                            "disagreement_ratio": (round(disagree / active, 4) if active else None),
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

    async def get_live_signal_trades(self) -> list[Dict[str, Any]]:
        query = """
            SELECT id, underlying, signal_timestamp, opened_at, updated_at,
                   status, direction, score_at_entry, score_latest,
                   option_symbol, option_type, expiration, strike,
                   entry_price, current_price, quantity_initial, quantity_open,
                   realized_pnl, unrealized_pnl, total_pnl, pnl_percent,
                   components_at_entry->'optimizer'->>'pricing_mode' AS pricing_mode
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
                   components_at_entry->'optimizer'->>'pricing_mode' AS pricing_mode,
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
        self,
        symbol: str = "SPY",
        limit: int = SIGNAL_HISTORY_LIMIT,
        lookback_days: int = SIGNAL_HISTORY_LOOKBACK_DAYS,
    ) -> list[Dict[str, Any]]:
        # Calendar-day lookback plus a row cap. The composite MSI is persisted
        # every cycle so dense underlyings can produce many thousands of rows
        # — LIMIT keeps the payload bounded.
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
                rows = await conn.fetch(query, symbol, limit, lookback_days)
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

    async def insert_action_card(self, card: Dict[str, Any]) -> Optional[int]:
        """Persist a non-STAND_DOWN Action Card.

        Caller passes ``card.to_dict()`` from the Playbook engine.  Failures
        are logged but never raised — persistence is best-effort and should
        not break the API response path.

        Returns the inserted row's ``id`` on a fresh write, or the existing
        row's ``id`` when the idempotency guard matches a recently-persisted
        duplicate (same underlying / pattern / timestamp).  Returns ``None``
        for STAND_DOWN cards, malformed payloads, or any DB error.
        """
        if not card or card.get("action") == "STAND_DOWN":
            return None
        ts = card.get("timestamp")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                logger.warning("insert_action_card: bad timestamp %r", ts)
                return None
        if ts is None:
            return None
        try:
            async with self._acquire_connection() as conn:
                inserted_id = await conn.fetchval(
                    """
                    INSERT INTO signal_action_cards
                        (underlying, timestamp, pattern, action, tier,
                         direction, confidence, payload)
                    SELECT $1::varchar, $2::timestamptz, $3::varchar,
                           $4, $5, $6, $7, $8::jsonb
                    WHERE NOT EXISTS (
                        -- Idempotency guard (no UNIQUE on the logical key):
                        -- skip an exact (underlying, pattern, timestamp)
                        -- duplicate from a restart / overlapping cycle that
                        -- defeats the dwell-window dedup. asyncpg allows
                        -- positional-parameter reuse, but only when each
                        -- parameter's type can be unambiguously deduced
                        -- from a single context.  ``$1``/``$2``/``$3``
                        -- appear in BOTH the INSERT-SELECT value position
                        -- (type inferred from the target column) AND in
                        -- the WHERE equality (type inferred from the LHS
                        -- column type); without explicit casts the two
                        -- deductions conflict and asyncpg rejects the
                        -- prepare with ``inconsistent types deduced for
                        -- parameter $N``, silently dropping every
                        -- action-card write.  The explicit casts above
                        -- pin each reused parameter to a single type so
                        -- the deduction is unambiguous.
                        SELECT 1 FROM signal_action_cards
                        WHERE underlying = $1::varchar
                          AND pattern = $3::varchar
                          AND timestamp = $2::timestamptz
                    )
                    RETURNING id
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
                if inserted_id is not None:
                    return int(inserted_id)
                # Idempotency guard matched — return the existing row's id so
                # the live API response can still expose a stable permalink for
                # the same logical card.
                existing_id = await conn.fetchval(
                    """
                    SELECT id FROM signal_action_cards
                    WHERE underlying = $1::varchar
                      AND pattern = $2::varchar
                      AND timestamp = $3::timestamptz
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    card.get("underlying"),
                    card.get("pattern"),
                    ts,
                )
                return int(existing_id) if existing_id is not None else None
        except Exception as exc:
            # Best-effort — don't surface persistence errors to API callers.
            logger.warning("insert_action_card failed (%s): %s", card.get("pattern"), exc)
            return None

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

    async def get_daily_scorecard(
        self,
        symbol: str,
        start_utc: datetime,
        end_utc: datetime,
        signal_names: List[str],
        horizon_minutes: int = 60,
    ) -> Dict[str, Any]:
        """Aggregate the day's signal-engine output for the public scorecard.

        Returns a single dict combining three independent reads:

        1. **Action Cards** persisted in the window — total count, breakdown
           by ``action`` enum, and the id of the first non-STAND_DOWN card
           emitted that day (used as the OG card's anchor permalink).
        2. **Per-signal flip events** with realized return at the requested
           horizon — for each of the 13 signal names, counts the number of
           direction-flip events in the window, the number that "won"
           (return same-sign as the flip direction), and the average
           directional return. Best/worst signal are picked from the names
           with at least two qualifying events.
        3. **Closing regime** — the most recent ``signal_scores`` row at or
           before ``end_utc``, used to label the day's MSI regime.

        Every fetch is best-effort: if one chunk fails the scorecard still
        renders with the remaining sections populated. ``start_utc`` is
        inclusive, ``end_utc`` is exclusive (matches the 1-min bucket
        boundary convention used elsewhere).
        """
        from collections import Counter

        out: Dict[str, Any] = {
            "symbol": symbol,
            "window_start_utc": start_utc,
            "window_end_utc": end_utc,
            "horizon_minutes": horizon_minutes,
            "cards": {
                "total": 0,
                "by_action": [],
                "first_card_id": None,
            },
            "signals": {
                "events": [],
                "best": None,
                "worst": None,
            },
            "regime": None,
        }

        # 1. Action Card aggregates for the day.
        try:
            async with self._acquire_connection() as conn:
                card_rows = await conn.fetch(
                    """
                    SELECT id, action
                    FROM signal_action_cards
                    WHERE underlying = $1
                      AND timestamp >= $2
                      AND timestamp < $3
                      AND action <> 'STAND_DOWN'
                    ORDER BY timestamp ASC, id ASC
                    """,
                    symbol,
                    start_utc,
                    end_utc,
                )
            if card_rows:
                out["cards"]["total"] = len(card_rows)
                out["cards"]["first_card_id"] = int(card_rows[0]["id"])
                counts = Counter(r["action"] for r in card_rows if r["action"])
                out["cards"]["by_action"] = [
                    {"action": action, "count": count}
                    for action, count in counts.most_common(5)
                ]
        except Exception as exc:
            logger.warning(
                "get_daily_scorecard: cards section failed (%s, %s..%s): %s",
                symbol, start_utc, end_utc, exc,
            )

        # 2. Per-signal flip events with realized return at horizon.
        if signal_names:
            horizon_interval = f"{int(horizon_minutes)} minutes"
            try:
                async with self._acquire_connection() as conn:
                    sig_rows = await conn.fetch(
                        f"""
                        WITH events AS (
                            SELECT
                                scs.component_name,
                                scs.timestamp,
                                scs.clamped_score,
                                SIGN(scs.clamped_score) AS sign_now,
                                LAG(SIGN(scs.clamped_score)) OVER (
                                    PARTITION BY scs.component_name
                                    ORDER BY scs.timestamp
                                ) AS sign_prev,
                                q0.close AS close_at_ts,
                                q1.close AS close_at_horizon
                            FROM signal_component_scores scs
                            LEFT JOIN LATERAL (
                                SELECT close FROM underlying_quotes uq
                                WHERE uq.symbol = scs.underlying
                                  AND uq.timestamp <= scs.timestamp
                                ORDER BY uq.timestamp DESC LIMIT 1
                            ) q0 ON TRUE
                            LEFT JOIN LATERAL (
                                SELECT close FROM underlying_quotes uq
                                WHERE uq.symbol = scs.underlying
                                  AND uq.timestamp >= scs.timestamp + INTERVAL '{horizon_interval}'
                                ORDER BY uq.timestamp ASC LIMIT 1
                            ) q1 ON TRUE
                            WHERE scs.underlying = $1
                              AND scs.component_name = ANY($2::varchar[])
                              AND scs.timestamp >= $3
                              AND scs.timestamp < $4
                        ),
                        flips AS (
                            -- A "flip" is a non-zero sign that differs from the
                            -- previous non-zero sign. The LAG above returns the
                            -- immediately-prior bar's sign, which may be 0; we
                            -- treat 0-prev as "not a flip" so we only count
                            -- transitions between live directional states.
                            SELECT
                                component_name,
                                sign_now,
                                close_at_ts,
                                close_at_horizon,
                                CASE
                                    WHEN close_at_ts IS NULL OR close_at_horizon IS NULL THEN NULL
                                    WHEN sign_now > 0 THEN (close_at_horizon - close_at_ts) / NULLIF(close_at_ts, 0)
                                    WHEN sign_now < 0 THEN (close_at_ts - close_at_horizon) / NULLIF(close_at_ts, 0)
                                    ELSE NULL
                                END AS directional_return
                            FROM events
                            WHERE sign_now <> 0
                              AND sign_prev IS NOT NULL
                              AND sign_prev <> 0
                              AND sign_now <> sign_prev
                        )
                        SELECT
                            component_name,
                            COUNT(*) AS flips,
                            COUNT(directional_return) AS scored,
                            SUM(CASE WHEN directional_return > 0 THEN 1 ELSE 0 END) AS wins,
                            SUM(CASE WHEN directional_return < 0 THEN 1 ELSE 0 END) AS losses,
                            AVG(directional_return) AS avg_directional_return
                        FROM flips
                        GROUP BY component_name
                        """,
                        symbol,
                        list(signal_names),
                        start_utc,
                        end_utc,
                    )
                events: List[Dict[str, Any]] = []
                for r in sig_rows:
                    avg = r["avg_directional_return"]
                    events.append(
                        {
                            "name": r["component_name"],
                            "flips": int(r["flips"] or 0),
                            "scored": int(r["scored"] or 0),
                            "wins": int(r["wins"] or 0),
                            "losses": int(r["losses"] or 0),
                            "avg_directional_return": float(avg) if avg is not None else None,
                        }
                    )
                out["signals"]["events"] = sorted(
                    events,
                    key=lambda e: (
                        e["avg_directional_return"]
                        if e["avg_directional_return"] is not None
                        else 0.0
                    ),
                    reverse=True,
                )
                # Pick best/worst from names with ≥ 2 scored events so a
                # one-print outlier doesn't crown a signal of the day.
                qualifying = [
                    e for e in events
                    if e["scored"] >= 2 and e["avg_directional_return"] is not None
                ]
                if qualifying:
                    out["signals"]["best"] = max(
                        qualifying, key=lambda e: e["avg_directional_return"]
                    )
                    out["signals"]["worst"] = min(
                        qualifying, key=lambda e: e["avg_directional_return"]
                    )
            except Exception as exc:
                logger.warning(
                    "get_daily_scorecard: signals section failed (%s): %s",
                    symbol, exc,
                )

        # 3. Closing regime — most recent signal_scores row in the window.
        try:
            async with self._acquire_connection() as conn:
                regime_row = await conn.fetchrow(
                    """
                    SELECT underlying, timestamp, composite_score, normalized_score, direction
                    FROM signal_scores
                    WHERE underlying = $1
                      AND timestamp >= $2
                      AND timestamp < $3
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    symbol,
                    start_utc,
                    end_utc,
                )
            if regime_row:
                out["regime"] = {
                    "timestamp": regime_row["timestamp"],
                    "composite_score": float(regime_row["composite_score"]) if regime_row["composite_score"] is not None else None,
                    "normalized_score": float(regime_row["normalized_score"]) if regime_row["normalized_score"] is not None else None,
                    "direction": regime_row["direction"],
                }
        except Exception as exc:
            logger.warning(
                "get_daily_scorecard: regime section failed (%s): %s",
                symbol, exc,
            )

        return out

    async def get_action_card_by_id(self, card_id: int) -> Optional[Dict[str, Any]]:
        """Return a single persisted Action Card by primary key.

        Returns the full ``payload`` (the Card's ``to_dict()`` form) plus the
        row's ``id`` and ``created_at`` so callers can build stable permalinks.
        Returns ``None`` if no row matches.
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, underlying, timestamp, pattern, action, tier,
                           direction, confidence, payload, created_at
                    FROM signal_action_cards
                    WHERE id = $1
                    """,
                    int(card_id),
                )
                if row is None:
                    return None
                payload = row["payload"]
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except json.JSONDecodeError:
                        payload = {}
                payload = dict(payload or {})
                payload["id"] = int(row["id"])
                payload["created_at"] = row["created_at"]
                # Normalize a few top-level fields in case the payload was
                # written by an older engine version that omitted them.
                payload.setdefault("underlying", row["underlying"])
                payload.setdefault("timestamp", row["timestamp"])
                payload.setdefault("pattern", row["pattern"])
                payload.setdefault("action", row["action"])
                payload.setdefault("tier", row["tier"])
                payload.setdefault("direction", row["direction"])
                payload.setdefault("confidence", float(row["confidence"]))
                return payload
        except Exception as exc:
            logger.warning("get_action_card_by_id failed (%s): %s", card_id, exc)
            return None

    async def get_action_cards_chronological(
        self,
        underlying: Optional[str] = None,
        limit: int = 50,
        since_hours: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Chronological list of persisted Action Cards for the public feed.

        Unlike ``get_recent_action_cards`` (which DISTINCT-ONs per pattern for
        hysteresis bookkeeping), this returns every row newest-first so the
        site can paginate the full history without collapsing duplicates.

        ``underlying`` filters to one symbol; ``since_hours`` restricts to a
        rolling window (default: no restriction, just the ``limit`` rows).
        """
        params: List[Any] = []
        where_parts: List[str] = []
        if underlying:
            params.append(underlying)
            where_parts.append(f"underlying = ${len(params)}")
        if since_hours is not None and since_hours > 0:
            params.append(int(since_hours))
            where_parts.append(
                f"timestamp > NOW() - (${len(params)}::int * INTERVAL '1 hour')"
            )
        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        params.append(int(limit))
        limit_placeholder = f"${len(params)}"
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT id, underlying, timestamp, pattern, action, tier,
                           direction, confidence, payload, created_at
                    FROM signal_action_cards
                    {where_clause}
                    ORDER BY timestamp DESC
                    LIMIT {limit_placeholder}
                    """,
                    *params,
                )
                out: List[Dict[str, Any]] = []
                for r in rows:
                    payload = r["payload"]
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except json.JSONDecodeError:
                            payload = {}
                    out.append(
                        {
                            "id": int(r["id"]),
                            "underlying": r["underlying"],
                            "timestamp": r["timestamp"],
                            "pattern": r["pattern"],
                            "action": r["action"],
                            "tier": r["tier"],
                            "direction": r["direction"],
                            "confidence": float(r["confidence"]),
                            "created_at": r["created_at"],
                            "rationale": (payload or {}).get("rationale"),
                        }
                    )
                return out
        except Exception as exc:
            logger.warning(
                "get_action_cards_chronological failed (underlying=%s): %s",
                underlying,
                exc,
            )
            return []

    # ------------------------------------------------------------------
    # Daily Forecast (Phase 3: Gamma Forecast Card + 4 PM Receipt)
    # ------------------------------------------------------------------

    async def insert_daily_forecast_morning(
        self, payload: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Persist the 7:00 AM ET morning snapshot for one trading day.

        Idempotent: the (symbol, date) primary key plus the morning-column
        immutability trigger guarantee that re-running the writer cannot
        overwrite a row already committed for the day. Returns the resulting
        row dict (whether freshly inserted or pre-existing) so the caller
        can confirm the content_hash matches what they computed.
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO daily_forecast (
                        symbol, date, open_ts, open_spot,
                        call_wall, put_wall, gamma_flip, open_msi,
                        regime, projected_low, projected_high, projected_close,
                        pin_strike, flagship_setup, range_model, content_hash
                    )
                    VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9, $10, $11, $12,
                        $13, $14::jsonb, $15, $16
                    )
                    ON CONFLICT (symbol, date) DO NOTHING
                    RETURNING symbol, date, open_ts, open_spot, call_wall,
                              put_wall, gamma_flip, open_msi, regime,
                              projected_low, projected_high, projected_close,
                              pin_strike, flagship_setup, range_model,
                              content_hash, created_at
                    """,
                    payload["symbol"],
                    payload["date"],
                    payload["open_ts"],
                    payload["open_spot"],
                    payload.get("call_wall"),
                    payload.get("put_wall"),
                    payload.get("gamma_flip"),
                    payload.get("open_msi"),
                    payload["regime"],
                    payload["projected_low"],
                    payload["projected_high"],
                    payload.get("projected_close"),
                    payload.get("pin_strike"),
                    json.dumps(payload.get("flagship_setup"), default=str)
                    if payload.get("flagship_setup") is not None
                    else None,
                    payload["range_model"],
                    payload["content_hash"],
                )
                if row is not None:
                    return dict(row)
                # ON CONFLICT no-op: return the existing row so callers can
                # log "already committed" rather than silently double-running.
                existing = await conn.fetchrow(
                    """
                    SELECT * FROM daily_forecast
                    WHERE symbol = $1 AND date = $2
                    """,
                    payload["symbol"], payload["date"],
                )
                return dict(existing) if existing else None
        except Exception as exc:
            logger.warning(
                "insert_daily_forecast_morning failed (%s, %s): %s",
                payload.get("symbol"), payload.get("date"), exc,
            )
            return None

    async def update_daily_forecast_receipt(
        self,
        symbol: str,
        forecast_date: date,
        receipt_ts: datetime,
        actual_low: float,
        actual_high: float,
        actual_close: float,
        setup_outcome: Optional[Dict[str, Any]] = None,
        pin_tolerance: float = 1.0,
    ) -> Optional[Dict[str, Any]]:
        """Write the 4:05 PM ET receipt for an existing morning row.

        Computes derived verdicts (``range_respected`` / ``pin_hit`` /
        ``regime_correct``) from the actual OHLC against the immutable
        morning columns. Skips silently and returns None if no morning row
        exists (the writer either never ran or failed earlier in the day);
        skips silently and returns the existing row if the receipt has
        already been written (the trigger enforces immutability).
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT projected_low, projected_high, pin_strike, regime,
                           open_spot, receipt_ts
                    FROM daily_forecast
                    WHERE symbol = $1 AND date = $2
                    """,
                    symbol, forecast_date,
                )
                if row is None:
                    logger.info(
                        "update_daily_forecast_receipt: no morning row for %s %s — skipping",
                        symbol, forecast_date.isoformat(),
                    )
                    return None
                if row["receipt_ts"] is not None:
                    logger.info(
                        "update_daily_forecast_receipt: receipt already written for %s %s — skipping",
                        symbol, forecast_date.isoformat(),
                    )
                    return dict(row)

                projected_low = float(row["projected_low"])
                projected_high = float(row["projected_high"])
                pin_strike = (
                    float(row["pin_strike"]) if row["pin_strike"] is not None else None
                )
                regime = row["regime"]
                open_spot = float(row["open_spot"])

                # Range respected = the day's traded high/low both sat
                # inside the band. Wicks count: the band is a no-touch
                # prediction, not a 90%-of-bars threshold.
                range_respected = (
                    actual_low >= projected_low and actual_high <= projected_high
                )
                pin_hit = (
                    pin_strike is not None
                    and abs(actual_close - pin_strike) <= pin_tolerance
                )
                # Regime correctness: long-gamma days should chop (close
                # within 0.5% of open); short-gamma days should trend
                # (close moved more than 0.5%). Transition days are
                # neutral — never marked wrong.
                move_pct = abs(actual_close - open_spot) / open_spot if open_spot else 0.0
                if regime == "long_gamma":
                    regime_correct = move_pct <= 0.005
                elif regime == "short_gamma":
                    regime_correct = move_pct > 0.005
                else:
                    regime_correct = None

                updated = await conn.fetchrow(
                    """
                    UPDATE daily_forecast
                    SET receipt_ts = $3,
                        actual_low = $4,
                        actual_high = $5,
                        actual_close = $6,
                        range_respected = $7,
                        pin_hit = $8,
                        regime_correct = $9,
                        setup_outcome = $10::jsonb
                    WHERE symbol = $1 AND date = $2
                    RETURNING *
                    """,
                    symbol, forecast_date, receipt_ts,
                    actual_low, actual_high, actual_close,
                    range_respected, pin_hit, regime_correct,
                    json.dumps(setup_outcome, default=str) if setup_outcome else None,
                )
                return dict(updated) if updated else None
        except Exception as exc:
            logger.warning(
                "update_daily_forecast_receipt failed (%s, %s): %s",
                symbol, forecast_date, exc,
            )
            return None

    async def get_daily_forecast(
        self, symbol: str, forecast_date: date
    ) -> Optional[Dict[str, Any]]:
        """Fetch one symbol/date forecast row, morning + receipt combined."""
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT * FROM daily_forecast
                    WHERE symbol = $1 AND date = $2
                    """,
                    symbol, forecast_date,
                )
                if row is None:
                    return None
                out = dict(row)
                for key in ("flagship_setup", "setup_outcome"):
                    val = out.get(key)
                    if isinstance(val, str):
                        try:
                            out[key] = json.loads(val)
                        except json.JSONDecodeError:
                            out[key] = None
                return out
        except Exception as exc:
            logger.warning(
                "get_daily_forecast failed (%s, %s): %s", symbol, forecast_date, exc,
            )
            return None

    async def get_daily_forecast_history(
        self, symbol: str, limit: int = 30
    ) -> List[Dict[str, Any]]:
        """Recent forecasts (newest first) — powers the stats endpoint and
        the website's rolling hit-rate strip."""
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT symbol, date, open_spot, projected_low, projected_high,
                           pin_strike, regime, range_respected, pin_hit,
                           regime_correct, actual_close, receipt_ts
                    FROM daily_forecast
                    WHERE symbol = $1
                    ORDER BY date DESC
                    LIMIT $2
                    """,
                    symbol, int(limit),
                )
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning(
                "get_daily_forecast_history failed (%s): %s", symbol, exc,
            )
            return []
