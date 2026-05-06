"""Technicals-feature query methods.

Extracted from ``src/api/database.py``.  Mixed into ``DatabaseManager``
as ``TechnicalsQueriesMixin``.  Methods rely on instance state
(``_acquire_connection``, ``_cache_get``/``_cache_set``) defined on
DatabaseManager.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List

from src.api.queries._sql_helpers import _bucket_expr, _interval_expr
from src.symbols import resolve_volume_proxy

logger = logging.getLogger(__name__)


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
            return await self._get_vwap_deviation_with_proxy(
                symbol, proxy, timeframe, window_units
            )
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

    async def get_dealer_hedging_pressure(
        self, symbol: str = "SPY", limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get dealer hedging pressure"""
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
            ORDER BY timestamp DESC
            LIMIT $2
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching dealer hedging: {e}", exc_info=True)
            raise

    async def get_unusual_volume_spikes(
        self, symbol: str = "SPY", limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get unusual volume spikes — filtered to Moderate Spike or above.

        The ``unusual_volume_spikes`` view classifies each row by sigma of
        the rolling-window volume distribution; the conventional labels are
        ``Mild Spike`` (≥2σ), ``Moderate Spike`` (≥3σ), ``Strong Spike``
        (≥4σ), ``Extreme Spike`` (≥5σ).  We surface only Moderate or
        stronger so consumers don't have to filter out routine noise.
        """
        query = """
            SELECT
                time_et,
                timestamp,
                symbol,
                price,
                current_volume,
                avg_volume,
                volume_sigma,
                volume_ratio,
                buying_pressure_pct,
                volume_class
            FROM unusual_volume_spikes
            WHERE symbol = $1
              AND (
                  volume_class IN ('Moderate Spike', 'Strong Spike', 'Extreme Spike')
                  OR volume_sigma >= 3.0
              )
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
