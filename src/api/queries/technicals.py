"""Technicals-feature query methods.

Extracted from ``src/api/database.py``.  Mixed into ``DatabaseManager``
as ``TechnicalsQueriesMixin``.  Methods rely on instance state
(``_acquire_connection``, ``_cache_get``/``_cache_set``) defined on
DatabaseManager.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from src.api.queries._sql_helpers import _bucket_expr, _interval_expr
from src.symbols import resolve_volume_proxy

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


class TechnicalsQueriesMixin:
    """Read-side methods for technicals endpoints.

    VWAP deviation, opening-range breakout, dealer hedging, unusual
    volume spikes, momentum divergence.
    """

    async def get_vwap_deviation(
        self, symbol: str = "SPY", timeframe: str = "1min", window_units: int = 20
    ) -> List[Dict[str, Any]]:
        """Get VWAP deviation for mean reversion signals by interval/window.

        Cash indices (SPX, NDX, RUT, DJX) carry no transactional volume of
        their own, so the standard ``underlying_vwap_deviation`` view
        returns NULL VWAP for them.  When a proxy ETF is configured for
        the symbol we route through ``_get_vwap_deviation_with_proxy``
        which applies the ETF's per-bar volume profile to the index's
        prices.  Equities/ETFs continue to use the canonical view.
        """
        window_units = max(1, min(window_units, 90))
        proxy = resolve_volume_proxy(symbol)
        if proxy:
            return await self._get_vwap_deviation_with_proxy(symbol, proxy, timeframe, window_units)
        step_interval = _interval_expr(timeframe)
        bucket = _bucket_expr(timeframe)
        query = f"""
            WITH latest AS (
                SELECT timestamp AS max_ts
                FROM underlying_vwap_deviation
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            bounds AS (
                SELECT
                    max_ts - ({step_interval} * ($2 - 1)) AS start_ts,
                    max_ts AS end_ts
                FROM latest
            ),
            base AS (
                SELECT
                    time_et,
                    timestamp,
                    symbol,
                    price,
                    vwap,
                    vwap_deviation_pct,
                    volume,
                    vwap_position,
                    {bucket} AS bucket_ts,
                    ROW_NUMBER() OVER (PARTITION BY {bucket} ORDER BY timestamp DESC) AS rn
                FROM underlying_vwap_deviation
                WHERE symbol = $1
                  AND timestamp BETWEEN (SELECT start_ts FROM bounds) AND (SELECT end_ts FROM bounds)
            )
            SELECT
                time_et,
                bucket_ts AS timestamp,
                symbol,
                price,
                vwap,
                vwap_deviation_pct,
                volume,
                vwap_position
            FROM base
            WHERE rn = 1
            ORDER BY timestamp DESC
            LIMIT $2
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, window_units)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching VWAP deviation: {e}", exc_info=True)
            raise

    async def _get_vwap_deviation_with_proxy(
        self,
        symbol: str,
        proxy: str,
        timeframe: str,
        window_units: int,
    ) -> List[Dict[str, Any]]:
        """VWAP deviation using a proxy ETF's per-bar volume.

        Joins the index's per-minute close to the proxy's per-minute
        ``up_volume + down_volume`` on the same timestamp, then runs the
        canonical session-cumulative VWAP formula over the joined series.
        Buckets/windows match the non-proxy path so the response shape is
        identical.
        """
        step_interval = _interval_expr(timeframe)
        bucket = _bucket_expr(timeframe)
        query = f"""
            WITH index_quotes AS (
                SELECT
                    timestamp,
                    symbol,
                    close AS price
                FROM underlying_quotes
                WHERE symbol = $1
            ),
            proxy_volume AS (
                SELECT
                    timestamp,
                    (up_volume + down_volume) AS volume
                FROM underlying_quotes
                WHERE symbol = $3
            ),
            joined AS (
                SELECT
                    iq.timestamp,
                    iq.symbol,
                    iq.price,
                    COALESCE(pv.volume, 0) AS volume
                FROM index_quotes iq
                LEFT JOIN proxy_volume pv ON pv.timestamp = iq.timestamp
            ),
            vwap_calc AS (
                SELECT
                    timestamp,
                    symbol,
                    price,
                    volume,
                    SUM(price * volume) OVER (
                        PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
                        ORDER BY timestamp
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cum_pv,
                    SUM(volume) OVER (
                        PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
                        ORDER BY timestamp
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cum_vol
                FROM joined
            ),
            with_vwap AS (
                SELECT
                    timestamp AT TIME ZONE 'America/New_York' AS time_et,
                    timestamp,
                    symbol,
                    price,
                    (cum_pv / NULLIF(cum_vol, 0))::numeric(12,4) AS vwap,
                    ROUND(
                        ((price - (cum_pv / NULLIF(cum_vol, 0)))
                         / NULLIF((cum_pv / NULLIF(cum_vol, 0)), 0) * 100)::numeric,
                        3
                    ) AS vwap_deviation_pct,
                    volume,
                    CASE
                        WHEN NULLIF(cum_vol, 0) IS NULL THEN NULL
                        WHEN price > (cum_pv / NULLIF(cum_vol, 0)) * 1.002 THEN '🔥 Extended Above VWAP'
                        WHEN price > (cum_pv / NULLIF(cum_vol, 0)) THEN '✅ Above VWAP'
                        WHEN price < (cum_pv / NULLIF(cum_vol, 0)) * 0.998 THEN '🔥 Extended Below VWAP'
                        ELSE '❌ Below VWAP'
                    END AS vwap_position
                FROM vwap_calc
            ),
            latest AS (
                SELECT timestamp AS max_ts
                FROM with_vwap
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            bounds AS (
                SELECT
                    max_ts - ({step_interval} * ($2 - 1)) AS start_ts,
                    max_ts AS end_ts
                FROM latest
            ),
            windowed AS (
                SELECT
                    time_et,
                    timestamp,
                    symbol,
                    price,
                    vwap,
                    vwap_deviation_pct,
                    volume,
                    vwap_position,
                    {bucket} AS bucket_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY {bucket} ORDER BY timestamp DESC
                    ) AS rn
                FROM with_vwap
                WHERE timestamp BETWEEN
                    (SELECT start_ts FROM bounds) AND (SELECT end_ts FROM bounds)
            )
            SELECT
                time_et,
                bucket_ts AS timestamp,
                symbol,
                price,
                vwap,
                vwap_deviation_pct,
                volume,
                vwap_position
            FROM windowed
            WHERE rn = 1
            ORDER BY timestamp DESC
            LIMIT $2
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, window_units, proxy)
                results = [dict(row) for row in rows]
                for row in results:
                    row["volume_proxy"] = proxy
                return results
        except Exception as e:
            logger.error(
                f"Error fetching VWAP deviation for {symbol} via proxy {proxy}: {e}",
                exc_info=True,
            )
            raise

    async def get_opening_range_breakout(
        self, symbol: str = "SPY", timeframe: str = "1min", window_units: int = 20
    ) -> List[Dict[str, Any]]:
        """Get opening range breakout status by interval/window."""
        window_units = max(1, min(window_units, 90))
        step_interval = _interval_expr(timeframe)
        bucket = _bucket_expr(timeframe)
        query = f"""
            WITH latest AS (
                SELECT timestamp AS max_ts
                FROM opening_range_breakout
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            bounds AS (
                SELECT
                    max_ts - ({step_interval} * ($2 - 1)) AS start_ts,
                    max_ts AS end_ts
                FROM latest
            ),
            base AS (
                SELECT
                    time_et,
                    timestamp,
                    symbol,
                    current_price,
                    orb_high,
                    orb_low,
                    orb_range,
                    distance_above_orb_high,
                    distance_below_orb_low,
                    orb_pct,
                    orb_status,
                    volume,
                    {bucket} AS bucket_ts,
                    ROW_NUMBER() OVER (PARTITION BY {bucket} ORDER BY timestamp DESC) AS rn
                FROM opening_range_breakout
                WHERE symbol = $1
                  AND timestamp BETWEEN (SELECT start_ts FROM bounds) AND (SELECT end_ts FROM bounds)
            )
            SELECT
                time_et,
                bucket_ts AS timestamp,
                symbol,
                current_price,
                orb_high,
                orb_low,
                orb_range,
                distance_above_orb_high,
                distance_below_orb_low,
                orb_pct,
                orb_status,
                volume
            FROM base
            WHERE rn = 1
            ORDER BY timestamp DESC
            LIMIT $2
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, window_units)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching ORB: {e}", exc_info=True)
            raise

    async def get_dealer_hedging_pressure(self, symbol: str = "SPY") -> List[Dict[str, Any]]:
        """Get dealer hedging pressure (point-in-time snapshot).

        The ``dealer_hedging_pressure`` view emits exactly one row per
        symbol — the current state aggregated across all option contracts —
        so this is intentionally not a timeseries.
        """
        query = """
            SELECT
                time_et,
                timestamp,
                symbol,
                current_price,
                price_change,
                expected_hedge_shares,
                hedge_pressure
            FROM dealer_hedging_pressure
            WHERE symbol = $1
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching dealer hedging: {e}", exc_info=True)
            raise

    async def get_unusual_volume_spikes(
        self, symbol: str = "SPY", limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get unusual volume spikes — filtered to Extreme (≥3σ) only.

        The ``unusual_volume_spikes`` view labels each row by sigma of
        the rolling 30-bar volume distribution: ``📈 Moderate Spike``
        (≥1σ), ``⚡ High Spike`` (≥2σ), ``🚨 Extreme Spike`` (≥3σ),
        ``⚪ Normal`` (<1σ).  We surface only Extreme readings — ≥1σ
        and ≥2σ fire on a large fraction of bars during the open and
        close auctions, so anything looser becomes routine noise.

        Cash indices (SPX, NDX, RUT, DJX) carry no transactional volume
        of their own, so the canonical view stops emitting fresh rows
        for them once TradeStation's synthetic index volume drops to
        zero.  When a proxy ETF is configured for the symbol we route
        through ``_get_unusual_volume_spikes_with_proxy`` which applies
        the ETF's per-bar volume profile to the index's prices.
        Equities/ETFs continue to use the canonical view.
        """
        proxy = resolve_volume_proxy(symbol)
        if proxy:
            return await self._get_unusual_volume_spikes_with_proxy(symbol, proxy, limit)

        query = """
            SELECT
                time_et,
                timestamp,
                symbol,
                price,
                up_volume,
                down_volume,
                current_volume,
                avg_volume,
                volume_sigma,
                volume_ratio,
                buying_pressure_pct,
                volume_class
            FROM unusual_volume_spikes
            WHERE symbol = $1
              AND volume_sigma >= 3.0
            ORDER BY timestamp DESC
            LIMIT $2
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching volume spikes: {e}", exc_info=True)
            raise

    async def _get_unusual_volume_spikes_with_proxy(
        self, symbol: str, proxy: str, limit: int
    ) -> List[Dict[str, Any]]:
        """Volume-spike detection using a proxy ETF's per-bar volume.

        Joins the index's per-minute close to the proxy ETF's per-minute
        ``up_volume + down_volume`` on the same timestamp, then runs the
        canonical ``unusual_volume_spikes`` rolling-window stats over the
        joined series — same 30-bar window, same sample sigma, same
        Moderate ≥1σ / High ≥2σ / Extreme ≥3σ labels.  Buying pressure
        uses the proxy's directional volume split.  The response
        includes a ``volume_proxy`` field so callers can see which ETF's
        volume profile was substituted.
        """
        query = """
            WITH index_quotes AS (
                SELECT timestamp, symbol, close AS price
                FROM underlying_quotes
                WHERE symbol = $1
            ),
            proxy_volume AS (
                SELECT
                    timestamp,
                    up_volume,
                    down_volume,
                    (up_volume + down_volume) AS volume
                FROM underlying_quotes
                WHERE symbol = $2
            ),
            joined AS (
                SELECT
                    iq.timestamp AT TIME ZONE 'America/New_York' AS time_et,
                    iq.timestamp,
                    iq.symbol,
                    iq.price,
                    COALESCE(pv.up_volume, 0) AS up_volume,
                    COALESCE(pv.down_volume, 0) AS down_volume,
                    COALESCE(pv.volume, 0) AS current_volume,
                    AVG(COALESCE(pv.volume, 0)) OVER (
                        PARTITION BY iq.symbol
                        ORDER BY iq.timestamp
                        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                    ) AS avg_volume,
                    STDDEV_SAMP(COALESCE(pv.volume, 0)) OVER (
                        PARTITION BY iq.symbol
                        ORDER BY iq.timestamp
                        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                    ) AS volume_stddev,
                    ROUND(
                        COALESCE(
                            COALESCE(pv.up_volume, 0)::numeric
                            / NULLIF(
                                (COALESCE(pv.up_volume, 0) + COALESCE(pv.down_volume, 0))::numeric,
                                0
                            ) * 100,
                            50
                        ),
                        2
                    ) AS buying_pressure_pct
                FROM index_quotes iq
                LEFT JOIN proxy_volume pv ON pv.timestamp = iq.timestamp
            )
            SELECT
                time_et,
                timestamp,
                symbol,
                price,
                up_volume,
                down_volume,
                current_volume,
                COALESCE(avg_volume, 0)::numeric(18,2) AS avg_volume,
                ROUND(
                    COALESCE(
                        (current_volume::numeric - avg_volume) / NULLIF(volume_stddev, 0),
                        0
                    ),
                    2
                ) AS volume_sigma,
                ROUND(
                    COALESCE(current_volume::numeric / NULLIF(avg_volume, 0), 1),
                    2
                ) AS volume_ratio,
                buying_pressure_pct,
                CASE
                    WHEN COALESCE(
                        (current_volume::numeric - avg_volume) / NULLIF(volume_stddev, 0),
                        0
                    ) >= 3 THEN '🚨 Extreme Spike'
                    WHEN COALESCE(
                        (current_volume::numeric - avg_volume) / NULLIF(volume_stddev, 0),
                        0
                    ) >= 2 THEN '⚡ High Spike'
                    WHEN COALESCE(
                        (current_volume::numeric - avg_volume) / NULLIF(volume_stddev, 0),
                        0
                    ) >= 1 THEN '📈 Moderate Spike'
                    ELSE '⚪ Normal'
                END AS volume_class
            FROM joined
            WHERE COALESCE(
                (current_volume::numeric - avg_volume) / NULLIF(volume_stddev, 0),
                0
            ) >= 3
            ORDER BY timestamp DESC
            LIMIT $3
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, proxy, limit)
                results = [dict(row) for row in rows]
                for row in results:
                    row["volume_proxy"] = proxy
                return results
        except Exception as e:
            logger.error(
                f"Error fetching volume spikes for {symbol} via proxy {proxy}: {e}",
                exc_info=True,
            )
            raise

    async def get_momentum_divergence(
        self, symbol: str = "SPY", timeframe: str = "1min", window_units: int = 20
    ) -> List[Dict[str, Any]]:
        """Get momentum divergence signals matching Makefile divergence shortcut semantics."""
        window_units = max(1, min(window_units, 90))
        query = """
            WITH option_flow AS (
                SELECT
                    timestamp,
                    symbol,
                    SUM(CASE WHEN option_type = 'C' THEN premium_delta ELSE -premium_delta END)::numeric AS net_option_flow
                FROM flow_contract_facts
                WHERE symbol = $1
                  AND timestamp >= NOW() - INTERVAL '2 days'
                GROUP BY timestamp, symbol
            ),
            base AS (
                SELECT
                    u.timestamp,
                    u.symbol,
                    u.close AS price,
                    u.close - LAG(u.close, 5) OVER (PARTITION BY u.symbol ORDER BY u.timestamp) AS price_change_5min,
                    (u.up_volume - u.down_volume)::bigint AS net_volume,
                    of.net_option_flow
                FROM underlying_quotes u
                LEFT JOIN option_flow of ON of.timestamp = u.timestamp AND of.symbol = u.symbol
                WHERE u.symbol = $1
                  AND u.timestamp >= NOW() - INTERVAL '2 days'
            )
            SELECT
                timestamp,
                symbol,
                ROUND(price, 2) AS price,
                ROUND(price_change_5min, 2) AS chg_5m,
                COALESCE(net_option_flow, 0)::numeric AS opt_flow,
                CASE
                    WHEN price_change_5min > 0 AND net_option_flow < -50000 THEN '🚨 Bearish Divergence (Price Up, Puts Buying)'
                    WHEN price_change_5min < 0 AND net_option_flow > 50000 THEN '🚨 Bullish Divergence (Price Down, Calls Buying)'
                    WHEN price_change_5min > 0 AND net_option_flow > 50000 THEN '🟢 Bullish Confirmation'
                    WHEN price_change_5min < 0 AND net_option_flow < -50000 THEN '🔴 Bearish Confirmation'
                    WHEN price_change_5min > 0 AND net_volume < 0 THEN '⚠️ Weak Rally (Selling Volume)'
                    WHEN price_change_5min < 0 AND net_volume > 0 THEN '⚠️ Weak Selloff (Buying Volume)'
                    ELSE '⚪ Neutral'
                END AS divergence_signal
            FROM base
            WHERE price_change_5min IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT $2
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, window_units)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching momentum divergence: {e}", exc_info=True)
            raise

    async def get_technicals_timeseries(
        self, symbol: str, intervals: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Per 5-minute bar timeseries combining VWAP deviation,
        opening-range breakout, unusual volume spikes (all classes),
        and momentum divergence — plus the underlying close — for the
        most recent session.

        Session window is decided by ``symbols.asset_type``:
            INDEX → 09:30–16:00 ET (cash session only)
            otherwise (ETF / EQUITY / unknown) → 04:00–20:00 ET (extended)

        Cash indices have no native volume, so VWAP and volume-spike
        rolling stats are computed against a proxy ETF's per-bar volume
        when one is configured (SPX→SPY, NDX→QQQ, RUT→IWM, DJX→DIA).

        Each bar represents a 5-minute bucket; ``timestamp`` is the
        START of the bucket (e.g. 10:30 → 10:30:00–10:34:59). The bar
        aggregates whichever 1-minute underlying bars have landed in
        the bucket: ``close`` is the latest 1-minute close, ``volume``
        is summed, ``high``/``low`` use max/min. While the bucket is
        active the bar updates as new 1-minute bars arrive; once the
        5-minute window closes the bar becomes immutable.

        ``bars`` is ordered newest → oldest so ``bars[0]`` is the most
        recent 5-minute bucket.

        ``intervals``: optional tail-window size in 5-minute bars (max
        192 = 16 hours). When provided, only the trailing N buckets are
        returned and the rolling-stats lookback shrinks from a full
        day to ~160 minutes — much faster for live polling.

        ORB anchor: opening-range high/low is computed from the most
        recent ET date that has cash-session data (>= 09:30 ET), not
        strictly ``session_date``. For ETFs this matters during pre-
        market — ``latest_ts`` (and therefore ``session_date``) advances
        to the new trading day at 04:00 ET, but the new day's ORB
        window doesn't start until 09:30 ET. Anchoring ORB on the most
        recent cash-session-active date surfaces the previous session's
        ORB through pre-market instead of returning NULL on every bar.
        INDEX symbols never carry pre-market data, so for them ORB and
        session always agree.

        Returns ``None`` when ``symbol`` isn't in the symbols table; an
        empty ``bars`` list when the symbol exists but has no data for
        the most recent session.
        """
        if intervals is not None:
            intervals = max(1, min(int(intervals), 192))

        cache_key = f"technicals_ts:{symbol}:{intervals}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        async with self._acquire_connection() as conn:
            row = await conn.fetchrow(
                "SELECT asset_type FROM symbols WHERE symbol = $1",
                symbol,
            )
            if row is None:
                return None
            asset_type = row["asset_type"]

            latest_ts = await conn.fetchval(
                "SELECT MAX(timestamp) FROM underlying_quotes WHERE symbol = $1",
                symbol,
            )
            if latest_ts is None:
                empty = {
                    "symbol": symbol,
                    "asset_type": asset_type,
                    "session_date": None,
                    "session_start_et": None,
                    "session_end_et": None,
                    "volume_proxy": resolve_volume_proxy(symbol),
                    "bars": [],
                }
                self._cache_set(cache_key, empty, self._analytics_cache_ttl_seconds)
                return empty

            session_date = latest_ts.astimezone(_ET).date()
            if asset_type == "INDEX":
                start_t, end_t = time(9, 30), time(16, 0)
            else:
                start_t, end_t = time(4, 0), time(20, 0)

            session_start = datetime.combine(session_date, start_t, tzinfo=_ET)
            session_end = datetime.combine(session_date, end_t, tzinfo=_ET)

            # ORB anchor: most recent ET date that has at least one bar
            # at/after 09:30 ET. For ETFs in pre-market this differs from
            # ``session_date`` — ``latest_ts`` has advanced to the new
            # trading day but its 09:30 ET ORB window hasn't started yet,
            # so anchoring ORB on session_date returns NULL for every bar.
            # Falling back to the most recent cash-session-active date
            # surfaces the previous session's ORB until today's ORB
            # actually has data. INDEX symbols never carry pre-market data
            # so orb_date == session_date for them.
            orb_date = await conn.fetchval(
                """
                SELECT (MAX(timestamp) AT TIME ZONE 'America/New_York')::date
                FROM underlying_quotes
                WHERE symbol = $1
                  AND (timestamp AT TIME ZONE 'America/New_York')::time >= '09:30'
                """,
                symbol,
            )
            if orb_date is None:
                # Symbol exists but has never had cash-session data.
                # Fall back to session_date so the query is well-formed;
                # orb_window will return NULL/NULL as expected.
                orb_date = session_date
            orb_start = datetime.combine(orb_date, time(9, 30), tzinfo=_ET)
            orb_end = datetime.combine(orb_date, time(9, 59, 59), tzinfo=_ET)

            # Bar window: full session by default, or the trailing
            # ``intervals`` 5-minute buckets when the caller asks for a
            # tail. Anchor on the most recent existing bar (clamped to
            # session_end) so live mid-session polls actually return
            # the trailing buckets — anchoring on session_end would put
            # the window in the future during a live session.
            if intervals is not None:
                anchor = min(latest_ts.astimezone(_ET), session_end)
                bar_window_start = max(
                    session_start,
                    anchor - timedelta(minutes=intervals * 5),
                )
            else:
                bar_window_start = session_start

            # Lookback needs to span 30 prior 5-minute buckets (vol-sigma
            # rolling window) plus 1 prior bucket (LAG-1 used for the
            # 5-minute price-change in divergence) — 31 × 5 = 155 minutes,
            # rounded up to 160. When the bar window starts at session
            # open we span the overnight gap with a 1-day lookback so
            # bar one of the session has stable rolling input.
            if bar_window_start > session_start + timedelta(minutes=160):
                lookback_start = bar_window_start - timedelta(minutes=160)
            else:
                lookback_start = session_start - timedelta(days=1)

            proxy = resolve_volume_proxy(symbol)
            volume_source = proxy or symbol

            query = """
                WITH bucketed_target AS (
                    SELECT
                        date_trunc('hour', timestamp)
                          + FLOOR(EXTRACT(MINUTE FROM timestamp) / 5)
                            * INTERVAL '5 minutes' AS bucket_ts,
                        MAX(high) AS high,
                        MIN(low) AS low,
                        -- close = latest 1-minute close in the bucket
                        (array_agg(close ORDER BY timestamp DESC))[1] AS close,
                        SUM(up_volume) AS native_up_volume,
                        SUM(down_volume) AS native_down_volume
                    FROM underlying_quotes
                    WHERE symbol = $1
                      AND timestamp BETWEEN $3 AND $5
                    GROUP BY 1
                ),
                bucketed_volume_source AS (
                    SELECT
                        date_trunc('hour', timestamp)
                          + FLOOR(EXTRACT(MINUTE FROM timestamp) / 5)
                            * INTERVAL '5 minutes' AS bucket_ts,
                        SUM(up_volume) AS up_volume,
                        SUM(down_volume) AS down_volume,
                        SUM(up_volume + down_volume) AS volume
                    FROM underlying_quotes
                    WHERE symbol = $2
                      AND timestamp BETWEEN $3 AND $5
                    GROUP BY 1
                ),
                joined AS (
                    SELECT
                        bt.bucket_ts AS timestamp,
                        bt.close,
                        bt.high,
                        bt.low,
                        bt.native_up_volume,
                        bt.native_down_volume,
                        COALESCE(bvs.up_volume, 0) AS up_volume,
                        COALESCE(bvs.down_volume, 0) AS down_volume,
                        COALESCE(bvs.volume, 0) AS volume
                    FROM bucketed_target bt
                    LEFT JOIN bucketed_volume_source bvs
                      ON bvs.bucket_ts = bt.bucket_ts
                ),
                combined AS (
                    SELECT
                        timestamp,
                        close,
                        volume,
                        up_volume,
                        down_volume,
                        native_up_volume,
                        native_down_volume,
                        SUM(close * volume) OVER (
                            PARTITION BY DATE(timestamp AT TIME ZONE 'America/New_York')
                            ORDER BY timestamp
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS cum_pv,
                        SUM(volume) OVER (
                            PARTITION BY DATE(timestamp AT TIME ZONE 'America/New_York')
                            ORDER BY timestamp
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS cum_vol,
                        AVG(volume) OVER (
                            ORDER BY timestamp
                            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                        ) AS avg_volume,
                        STDDEV_SAMP(volume) OVER (
                            ORDER BY timestamp
                            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                        ) AS volume_stddev,
                        -- 5-minute price change = LAG-1 over 5-min bars
                        close - LAG(close, 1) OVER (ORDER BY timestamp)
                            AS price_change_5min
                    FROM joined
                ),
                orb_window AS (
                    -- Query underlying_quotes directly so ORB lookup is
                    -- independent of ``lookback_start``. Anchored on
                    -- ``orb_date`` ($6/$7) which is the most recent date
                    -- that has cash-session data — see Python side for
                    -- the rationale (handles ETFs in pre-market).
                    SELECT
                        MAX(high) AS orb_high,
                        MIN(low) AS orb_low
                    FROM underlying_quotes
                    WHERE symbol = $1
                      AND timestamp >= $6
                      AND timestamp <= $7
                ),
                option_flow AS (
                    SELECT
                        date_trunc('hour', timestamp)
                          + FLOOR(EXTRACT(MINUTE FROM timestamp) / 5)
                            * INTERVAL '5 minutes' AS bucket_ts,
                        SUM(
                            CASE
                                WHEN option_type = 'C' THEN premium_delta
                                ELSE -premium_delta
                            END
                        )::numeric AS net_option_flow
                    FROM flow_contract_facts
                    WHERE symbol = $1
                      AND timestamp BETWEEN $4 AND $5
                    GROUP BY 1
                )
                SELECT
                    c.timestamp AT TIME ZONE 'America/New_York' AS time_et,
                    c.timestamp,
                    c.close,
                    c.volume,
                    c.up_volume,
                    c.down_volume,
                    (c.cum_pv / NULLIF(c.cum_vol, 0))::numeric(12,4) AS vwap,
                    ROUND(
                        ((c.close - (c.cum_pv / NULLIF(c.cum_vol, 0)))
                         / NULLIF((c.cum_pv / NULLIF(c.cum_vol, 0)), 0) * 100)::numeric,
                        3
                    ) AS vwap_deviation_pct,
                    CASE
                        WHEN NULLIF(c.cum_vol, 0) IS NULL THEN NULL
                        WHEN c.close > (c.cum_pv / NULLIF(c.cum_vol, 0)) * 1.002 THEN '🔥 Extended Above VWAP'
                        WHEN c.close > (c.cum_pv / NULLIF(c.cum_vol, 0)) THEN '✅ Above VWAP'
                        WHEN c.close < (c.cum_pv / NULLIF(c.cum_vol, 0)) * 0.998 THEN '🔥 Extended Below VWAP'
                        ELSE '❌ Below VWAP'
                    END AS vwap_position,
                    CASE WHEN c.timestamp >= $6 THEN ow.orb_high END AS orb_high,
                    CASE WHEN c.timestamp >= $6 THEN ow.orb_low END AS orb_low,
                    CASE WHEN c.timestamp >= $6
                         THEN (ow.orb_high - ow.orb_low) END AS orb_range,
                    CASE WHEN c.timestamp >= $6
                         THEN ROUND(c.close - ow.orb_high, 2) END
                        AS distance_above_orb_high,
                    CASE WHEN c.timestamp >= $6
                         THEN ROUND(ow.orb_low - c.close, 2) END
                        AS distance_below_orb_low,
                    CASE WHEN c.timestamp >= $6
                         THEN ROUND((c.close - ow.orb_low)
                                    / NULLIF(ow.orb_high - ow.orb_low, 0)
                                    * 100, 1) END AS orb_pct,
                    CASE
                        WHEN c.timestamp < $6 THEN NULL
                        WHEN c.close > ow.orb_high THEN '🚀 ORB Breakout (Long)'
                        WHEN c.close < ow.orb_low THEN '💥 ORB Breakdown (Short)'
                        WHEN c.close >= ow.orb_high * 0.998 THEN '⚡ Near ORB High'
                        WHEN c.close <= ow.orb_low * 1.002 THEN '⚡ Near ORB Low'
                        ELSE '⏸️ Inside ORB'
                    END AS orb_status,
                    c.volume AS current_volume,
                    COALESCE(c.avg_volume, 0)::numeric(18,2) AS avg_volume,
                    ROUND(
                        COALESCE(
                            (c.volume::numeric - c.avg_volume)
                            / NULLIF(c.volume_stddev, 0),
                            0
                        ),
                        2
                    ) AS volume_sigma,
                    ROUND(
                        COALESCE(
                            c.volume::numeric / NULLIF(c.avg_volume, 0),
                            1
                        ),
                        2
                    ) AS volume_ratio,
                    ROUND(
                        COALESCE(
                            c.up_volume::numeric
                            / NULLIF((c.up_volume + c.down_volume)::numeric, 0)
                            * 100,
                            50
                        ),
                        2
                    ) AS buying_pressure_pct,
                    CASE
                        WHEN COALESCE(
                            (c.volume::numeric - c.avg_volume)
                            / NULLIF(c.volume_stddev, 0),
                            0
                        ) >= 3 THEN '🚨 Extreme Spike'
                        WHEN COALESCE(
                            (c.volume::numeric - c.avg_volume)
                            / NULLIF(c.volume_stddev, 0),
                            0
                        ) >= 2 THEN '⚡ High Spike'
                        WHEN COALESCE(
                            (c.volume::numeric - c.avg_volume)
                            / NULLIF(c.volume_stddev, 0),
                            0
                        ) >= 1 THEN '📈 Moderate Spike'
                        ELSE '⚪ Normal'
                    END AS volume_class,
                    ROUND(c.price_change_5min, 2) AS chg_5m,
                    COALESCE(of.net_option_flow, 0)::numeric AS opt_flow,
                    CASE
                        WHEN c.price_change_5min IS NULL THEN NULL
                        WHEN c.price_change_5min > 0
                             AND COALESCE(of.net_option_flow, 0) < -50000
                            THEN '🚨 Bearish Divergence (Price Up, Puts Buying)'
                        WHEN c.price_change_5min < 0
                             AND COALESCE(of.net_option_flow, 0) > 50000
                            THEN '🚨 Bullish Divergence (Price Down, Calls Buying)'
                        WHEN c.price_change_5min > 0
                             AND COALESCE(of.net_option_flow, 0) > 50000
                            THEN '🟢 Bullish Confirmation'
                        WHEN c.price_change_5min < 0
                             AND COALESCE(of.net_option_flow, 0) < -50000
                            THEN '🔴 Bearish Confirmation'
                        WHEN c.price_change_5min > 0
                             AND (c.native_up_volume - c.native_down_volume) < 0
                            THEN '⚠️ Weak Rally (Selling Volume)'
                        WHEN c.price_change_5min < 0
                             AND (c.native_up_volume - c.native_down_volume) > 0
                            THEN '⚠️ Weak Selloff (Buying Volume)'
                        ELSE '⚪ Neutral'
                    END AS divergence_signal
                FROM combined c
                CROSS JOIN orb_window ow
                LEFT JOIN option_flow of ON of.bucket_ts = c.timestamp
                WHERE c.timestamp BETWEEN $4 AND $5
                ORDER BY c.timestamp DESC
            """

            try:
                rows = await conn.fetch(
                    query,
                    symbol,
                    volume_source,
                    lookback_start,
                    bar_window_start,
                    session_end,
                    orb_start,
                    orb_end,
                )
            except Exception as e:
                logger.error(
                    f"Error fetching technicals timeseries for {symbol}: {e}",
                    exc_info=True,
                )
                raise

        bars = [_format_technicals_bar(r) for r in rows]
        payload = {
            "symbol": symbol,
            "asset_type": asset_type,
            "session_date": session_date.isoformat(),
            "session_start_et": session_start.isoformat(),
            "session_end_et": session_end.isoformat(),
            "volume_proxy": proxy,
            "bars": bars,
        }
        self._cache_set(cache_key, payload, self._analytics_cache_ttl_seconds)
        return payload


def _format_technicals_bar(row: Any) -> Dict[str, Any]:
    """Coerce a raw asyncpg row into a JSON-friendly nested bar dict."""

    def f(value: Any) -> Optional[float]:
        return float(value) if value is not None else None

    def i(value: Any) -> Optional[int]:
        return int(value) if value is not None else None

    return {
        "time_et": row["time_et"].isoformat() if row["time_et"] else None,
        "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
        "close": f(row["close"]),
        "volume": i(row["volume"]),
        "vwap_deviation": {
            "vwap": f(row["vwap"]),
            "vwap_deviation_pct": f(row["vwap_deviation_pct"]),
            "vwap_position": row["vwap_position"],
        },
        "opening_range": {
            "orb_high": f(row["orb_high"]),
            "orb_low": f(row["orb_low"]),
            "orb_range": f(row["orb_range"]),
            "distance_above_orb_high": f(row["distance_above_orb_high"]),
            "distance_below_orb_low": f(row["distance_below_orb_low"]),
            "orb_pct": f(row["orb_pct"]),
            "orb_status": row["orb_status"],
        },
        "volume_spike": {
            "current_volume": i(row["current_volume"]),
            "up_volume": i(row["up_volume"]),
            "down_volume": i(row["down_volume"]),
            "avg_volume": f(row["avg_volume"]),
            "volume_sigma": f(row["volume_sigma"]),
            "volume_ratio": f(row["volume_ratio"]),
            "buying_pressure_pct": f(row["buying_pressure_pct"]),
            "volume_class": row["volume_class"],
        },
        "momentum_divergence": {
            "chg_5m": f(row["chg_5m"]),
            "opt_flow": f(row["opt_flow"]),
            "divergence_signal": row["divergence_signal"],
        },
    }
