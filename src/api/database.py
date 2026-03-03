"""
Database manager for API queries
Uses asyncpg for async PostgreSQL operations
"""

import asyncpg
import os
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)



def _normalize_timeframe(timeframe: str) -> str:
    normalized = (timeframe or '1min').lower()
    if normalized == '1hour':
        return '1hr'
    return normalized


def _bucket_expr(timeframe: str, column: str = 'timestamp') -> str:
    timeframe = _normalize_timeframe(timeframe)
    if timeframe == '1min':
        return f"date_trunc('minute', {column})"
    if timeframe == '5min':
        return (
            f"date_trunc('hour', {column}) + "
            f"FLOOR(EXTRACT(MINUTE FROM {column}) / 5) * INTERVAL '5 minutes'"
        )
    if timeframe == '15min':
        return (
            f"date_trunc('hour', {column}) + "
            f"FLOOR(EXTRACT(MINUTE FROM {column}) / 15) * INTERVAL '15 minutes'"
        )
    if timeframe == '1hr':
        return f"date_trunc('hour', {column})"
    if timeframe == '1day':
        return f"date_trunc('day', {column})"
    raise ValueError(f'Unsupported timeframe: {timeframe}')


def _interval_expr(timeframe: str) -> str:
    timeframe = _normalize_timeframe(timeframe)
    mapping = {
        '1min': "INTERVAL '1 minute'",
        '5min': "INTERVAL '5 minutes'",
        '15min': "INTERVAL '15 minutes'",
        '1hr': "INTERVAL '1 hour'",
        '1day': "INTERVAL '1 day'",
    }
    if timeframe not in mapping:
        raise ValueError(f'Unsupported timeframe: {timeframe}')
    return mapping[timeframe]

class DatabaseManager:
    """Manages database connections and queries"""

    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self._load_credentials()

    def _load_credentials(self):
        """Load database credentials from .pgpass or environment"""
        # Try .pgpass first (production)
        pgpass_file = Path.home() / '.pgpass'
        if pgpass_file.exists():
            with open(pgpass_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split(':')
                        if len(parts) >= 5:
                            self.host = parts[0]
                            self.port = parts[1]
                            self.database = parts[2]
                            self.user = parts[3]
                            self.password = parts[4]
                            return

        # Fallback to environment variables
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'zerogexdb')
        self.user = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')

    async def connect(self):
        """Create connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=2,
                max_size=10,
                command_timeout=30
            )
            logger.info(f"Database pool created: {self.database}@{self.host}")
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise

    async def disconnect(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")

    async def check_health(self) -> bool:
        """Check database connection health"""
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    # ========================================================================
    # GEX Queries
    # ========================================================================

    async def get_latest_gex_summary(self, symbol: str = 'SPY') -> Optional[Dict[str, Any]]:
        """Get latest GEX summary"""
        query = """
            SELECT 
                timestamp,
                underlying as symbol,
                max_gamma_strike as spot_price,
                total_call_oi::numeric as total_call_gex,
                total_put_oi::numeric as total_put_gex,
                total_net_gex as net_gex,
                gamma_flip_point as gamma_flip,
                max_pain,
                max_gamma_strike as call_wall,
                max_gamma_strike as put_wall,
                total_call_oi,
                total_put_oi,
                put_call_ratio
            FROM gex_summary
            WHERE underlying = $1
            ORDER BY timestamp DESC
            LIMIT 1
        """

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(query, symbol)
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error fetching GEX summary: {e}")
            raise

    async def get_gex_by_strike(
        self, 
        symbol: str = 'SPY',
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get latest GEX breakdown by strike"""
        query = """
            SELECT 
                timestamp,
                underlying as symbol,
                strike,
                call_oi,
                put_oi,
                call_volume,
                put_volume,
                call_gamma as call_gex,
                put_gamma as put_gex,
                net_gex,
                (SELECT close FROM underlying_quotes 
                 WHERE symbol = $1 
                 ORDER BY timestamp DESC LIMIT 1) as spot_price,
                strike - (SELECT close FROM underlying_quotes 
                          WHERE symbol = $1 
                          ORDER BY timestamp DESC LIMIT 1) as distance_from_spot
            FROM gex_by_strike
            WHERE underlying = $1
                AND timestamp = (
                    SELECT MAX(timestamp) 
                    FROM gex_by_strike 
                    WHERE underlying = $1
                )
            ORDER BY ABS(strike - (SELECT close FROM underlying_quotes 
                                   WHERE symbol = $1 
                                   ORDER BY timestamp DESC LIMIT 1)) ASC
            LIMIT $2
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching GEX by strike: {e}")
            raise

    async def get_historical_gex(
        self,
        symbol: str = 'SPY',
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 90,
        timeframe: str = '1min'
    ) -> List[Dict[str, Any]]:
        """Get historical GEX summary data aggregated by timeframe."""
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        query = f"""
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM gex_summary
                WHERE underlying = $1
            ),
            bounds AS (
                SELECT
                    COALESCE($2::timestamptz, max_ts - ({step_interval} * ($4 - 1))) AS start_ts,
                    COALESCE($3::timestamptz, max_ts) AS end_ts
                FROM latest
            ),
            base AS (
                SELECT
                    timestamp,
                    underlying as symbol,
                    max_gamma_strike as spot_price,
                    total_call_oi::numeric as total_call_gex,
                    total_put_oi::numeric as total_put_gex,
                    total_net_gex as net_gex,
                    gamma_flip_point as gamma_flip,
                    max_pain,
                    max_gamma_strike as call_wall,
                    max_gamma_strike as put_wall,
                    total_call_oi,
                    total_put_oi,
                    put_call_ratio,
                    {bucket} as bucket_ts,
                    ROW_NUMBER() OVER (PARTITION BY {bucket} ORDER BY timestamp DESC) as rn
                FROM gex_summary
                WHERE underlying = $1
                    AND timestamp BETWEEN (SELECT start_ts FROM bounds) AND (SELECT end_ts FROM bounds)
            )
            SELECT
                bucket_ts as timestamp,
                symbol,
                spot_price,
                total_call_gex,
                total_put_gex,
                net_gex,
                gamma_flip,
                max_pain,
                call_wall,
                put_wall,
                total_call_oi,
                total_put_oi,
                put_call_ratio
            FROM base
            WHERE rn = 1
            ORDER BY timestamp DESC
            LIMIT $4
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, start_date, end_date, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching historical GEX: {e}")
            raise

    # ========================================================================
    # Options Flow Queries (from views)
    # ========================================================================

    async def get_flow_by_type(
        self,
        symbol: str = 'SPY',
        window_minutes: int = 60
    ) -> List[Dict[str, Any]]:
        """Get option flow by type (calls vs puts)."""
        query = """
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM option_chains
                WHERE underlying = $1
            ),
            windowed AS (
                SELECT oc.*
                FROM option_chains oc
                CROSS JOIN latest l
                WHERE oc.underlying = $1
                    AND oc.timestamp BETWEEN l.max_ts - INTERVAL '1 minute' * $2 AND l.max_ts
            ),
            contract_agg AS (
                SELECT
                    option_symbol,
                    option_type,
                    GREATEST(MAX(volume) - MIN(volume), 0) AS flow,
                    (ARRAY_AGG(last ORDER BY timestamp DESC))[1] AS last_price,
                    MIN(timestamp) AS time_window_start,
                    MAX(timestamp) AS time_window_end
                FROM windowed
                GROUP BY option_symbol, option_type
            ),
            type_agg AS (
                SELECT
                    MIN(time_window_start) AS time_window_start,
                    MAX(time_window_end) AS time_window_end,
                    option_type,
                    SUM(flow)::bigint AS total_volume,
                    SUM(flow * COALESCE(last_price, 0) * 100)::numeric AS total_premium
                FROM contract_agg
                GROUP BY option_type
            )
            SELECT
                time_window_start,
                time_window_end,
                $1::varchar AS symbol,
                CASE WHEN option_type = 'C' THEN 'CALL' ELSE 'PUT' END AS option_type,
                total_volume,
                total_premium,
                NULL::numeric AS avg_iv,
                NULL::numeric AS net_delta,
                CASE
                    WHEN option_type = 'C' AND total_volume > COALESCE((SELECT total_volume FROM type_agg WHERE option_type = 'P'), 0) THEN 'bullish'
                    WHEN option_type = 'P' AND total_volume > COALESCE((SELECT total_volume FROM type_agg WHERE option_type = 'C'), 0) THEN 'bearish'
                    ELSE 'neutral'
                END AS sentiment
            FROM type_agg
            ORDER BY option_type
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, window_minutes)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching flow by type: {e}")
            raise

    async def get_flow_by_strike(
        self,
        symbol: str = 'SPY',
        window_minutes: int = 60,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get option flow by strike level."""
        query = """
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM option_chains
                WHERE underlying = $1
            ),
            windowed AS (
                SELECT oc.*
                FROM option_chains oc
                CROSS JOIN latest l
                WHERE oc.underlying = $1
                    AND oc.timestamp BETWEEN l.max_ts - INTERVAL '1 minute' * $2 AND l.max_ts
            ),
            contract_agg AS (
                SELECT
                    option_symbol,
                    strike,
                    option_type,
                    GREATEST(MAX(volume) - MIN(volume), 0) AS flow,
                    (ARRAY_AGG(last ORDER BY timestamp DESC))[1] AS last_price,
                    AVG(implied_volatility) AS avg_iv,
                    MIN(timestamp) AS time_window_start,
                    MAX(timestamp) AS time_window_end
                FROM windowed
                GROUP BY option_symbol, strike, option_type
            ),
            strike_agg AS (
                SELECT
                    MIN(time_window_start) AS time_window_start,
                    MAX(time_window_end) AS time_window_end,
                    strike,
                    SUM(flow)::bigint AS total_volume,
                    SUM(flow * COALESCE(last_price, 0) * 100)::numeric AS total_premium,
                    AVG(avg_iv)::numeric AS avg_iv,
                    SUM(CASE WHEN option_type = 'C' THEN flow ELSE -flow END)::numeric AS net_delta
                FROM contract_agg
                GROUP BY strike
            )
            SELECT
                time_window_start,
                time_window_end,
                $1::varchar AS symbol,
                strike,
                total_volume,
                total_premium,
                avg_iv,
                net_delta
            FROM strike_agg
            ORDER BY total_premium DESC NULLS LAST
            LIMIT $3
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, window_minutes, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching flow by strike: {e}")
            raise

    async def get_smart_money_flow(
        self,
        symbol: str = 'SPY',
        window_minutes: int = 60,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get unusual activity / smart money flow."""
        query = """
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM option_chains
                WHERE underlying = $1
            ),
            windowed AS (
                SELECT oc.*
                FROM option_chains oc
                CROSS JOIN latest l
                WHERE oc.underlying = $1
                    AND oc.timestamp BETWEEN l.max_ts - INTERVAL '1 minute' * $2 AND l.max_ts
            ),
            contract_agg AS (
                SELECT
                    option_symbol,
                    strike,
                    expiration,
                    option_type,
                    GREATEST(MAX(volume) - MIN(volume), 0) AS flow,
                    (ARRAY_AGG(last ORDER BY timestamp DESC))[1] AS last_price,
                    AVG(implied_volatility)::numeric AS iv,
                    AVG(delta)::numeric AS avg_delta,
                    MIN(timestamp) AS time_window_start,
                    MAX(timestamp) AS time_window_end
                FROM windowed
                GROUP BY option_symbol, strike, expiration, option_type
            )
            SELECT
                time_window_start,
                time_window_end,
                $1::varchar AS symbol,
                option_type,
                strike,
                flow::bigint AS total_volume,
                (flow * COALESCE(last_price, 0) * 100)::numeric AS total_premium,
                iv AS avg_iv,
                LEAST(10, GREATEST(0,
                    CASE WHEN flow >= 500 THEN 4 WHEN flow >= 200 THEN 3 WHEN flow >= 100 THEN 2 WHEN flow >= 50 THEN 1 ELSE 0 END +
                    CASE WHEN flow * COALESCE(last_price, 0) * 100 >= 500000 THEN 4 WHEN flow * COALESCE(last_price, 0) * 100 >= 250000 THEN 3 WHEN flow * COALESCE(last_price, 0) * 100 >= 100000 THEN 2 WHEN flow * COALESCE(last_price, 0) * 100 >= 50000 THEN 1 ELSE 0 END +
                    CASE WHEN iv > 1.0 THEN 2 WHEN iv > 0.6 THEN 1 ELSE 0 END
                ))::numeric AS unusual_activity_score,
                CASE WHEN flow >= 500 THEN '🔥 Massive Block' WHEN flow >= 200 THEN '📦 Large Block' WHEN flow >= 100 THEN '📊 Medium Block' ELSE '💼 Standard' END AS size_class,
                CASE WHEN flow * COALESCE(last_price, 0) * 100 >= 500000 THEN '💰 $500K+' WHEN flow * COALESCE(last_price, 0) * 100 >= 250000 THEN '💵 $250K+' WHEN flow * COALESCE(last_price, 0) * 100 >= 100000 THEN '💸 $100K+' WHEN flow * COALESCE(last_price, 0) * 100 >= 50000 THEN '💳 $50K+' ELSE '💴 <$50K' END AS notional_class,
                CASE WHEN ABS(COALESCE(avg_delta, 0)) < 0.15 THEN '💰 Deep OTM' WHEN ABS(COALESCE(avg_delta, 0)) < 0.35 THEN '🎯 OTM' WHEN ABS(COALESCE(avg_delta, 0)) < 0.65 THEN '⚖️ ATM' ELSE '💎 ITM' END AS moneyness
            FROM contract_agg
            WHERE flow > 0
            ORDER BY unusual_activity_score DESC, total_premium DESC
            LIMIT $3
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, window_minutes, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching smart money flow: {e}")
            raise

    async def get_vwap_deviation(
        self,
        symbol: str = 'SPY',
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get VWAP deviation for mean reversion signals"""
        query = """
            SELECT 
                time_et,
                timestamp,
                symbol,
                price,
                vwap,
                vwap_deviation_pct,
                volume,
                vwap_position
            FROM underlying_vwap_deviation
            WHERE symbol = $1
            ORDER BY timestamp DESC
            LIMIT $2
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching VWAP deviation: {e}")
            raise

    async def get_opening_range_breakout(
        self,
        symbol: str = 'SPY',
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get opening range breakout status"""
        query = """
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
                volume
            FROM opening_range_breakout
            WHERE symbol = $1
            ORDER BY timestamp DESC
            LIMIT $2
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching ORB: {e}")
            raise

    async def get_gamma_exposure_levels(
        self,
        symbol: str = 'SPY',
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get gamma exposure by strike (support/resistance)"""
        query = """
            SELECT 
                underlying as symbol,
                strike,
                net_gex,
                total_gex,
                call_gex,
                put_gex,
                num_contracts,
                total_oi,
                gex_level
            FROM gamma_exposure_levels
            WHERE underlying = $1
            ORDER BY ABS(net_gex) DESC
            LIMIT $2
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching gamma levels: {e}")
            raise

    async def get_dealer_hedging_pressure(
        self,
        symbol: str = 'SPY',
        limit: int = 20
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
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching dealer hedging: {e}")
            raise

    async def get_unusual_volume_spikes(
        self,
        symbol: str = 'SPY',
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get unusual volume spikes"""
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
            ORDER BY volume_sigma DESC
            LIMIT $2
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching volume spikes: {e}")
            raise

    async def get_momentum_divergence(
        self,
        symbol: str = 'SPY',
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get momentum divergence signals"""
        query = """
            SELECT 
                time_et,
                timestamp,
                symbol,
                price,
                price_change_5min,
                net_volume,
                net_option_flow,
                divergence_signal
            FROM momentum_divergence
            WHERE symbol = $1
            ORDER BY timestamp DESC
            LIMIT $2
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching momentum divergence: {e}")
            raise

    # ========================================================================
    # Market Data Queries
    # ========================================================================

    async def get_latest_quote(self, symbol: str = 'SPY') -> Optional[Dict[str, Any]]:
        """Get latest underlying quote"""
        query = """
            SELECT 
                timestamp,
                symbol,
                open,
                high,
                low,
                close,
                up_volume + down_volume as volume
            FROM underlying_quotes
            WHERE symbol = $1
            ORDER BY timestamp DESC
            LIMIT 1
        """

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(query, symbol)
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error fetching latest quote: {e}")
            raise

    async def get_previous_close(self, symbol: str = 'SPY') -> Optional[Dict[str, Any]]:
        """
        Get the most recent 4:00 PM ET close price (previous trading day's close).
        Works on any day including weekends and holidays.
        """
        query = """
            WITH market_close_time AS (
                -- Find the most recent 4:00 PM ET bar
                SELECT 
                    timestamp,
                    symbol,
                    close as previous_close
                FROM underlying_quotes
                WHERE symbol = $1
                    AND EXTRACT(HOUR FROM timestamp AT TIME ZONE 'America/New_York') = 16
                    AND EXTRACT(MINUTE FROM timestamp AT TIME ZONE 'America/New_York') = 0
                    AND EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') BETWEEN 1 AND 5
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            nearest_close AS (
                -- Fallback: find the bar closest to 4:00 PM on the most recent trading day
                SELECT 
                    q.timestamp,
                    q.symbol,
                    q.close as previous_close,
                    ABS(EXTRACT(EPOCH FROM (
                        (q.timestamp AT TIME ZONE 'America/New_York')::time - '16:00:00'::time
                    ))) as time_diff_seconds
                FROM underlying_quotes q
                CROSS JOIN (
                    -- Get the most recent trading day that has data
                    SELECT DISTINCT DATE(timestamp AT TIME ZONE 'America/New_York') as trade_date
                    FROM underlying_quotes
                    WHERE symbol = $1
                        AND EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') BETWEEN 1 AND 5
                    ORDER BY DATE(timestamp AT TIME ZONE 'America/New_York') DESC
                    LIMIT 1
                ) recent_day
                WHERE q.symbol = $1
                    AND DATE(q.timestamp AT TIME ZONE 'America/New_York') = recent_day.trade_date
                    AND EXTRACT(HOUR FROM q.timestamp AT TIME ZONE 'America/New_York') BETWEEN 15 AND 16
                ORDER BY time_diff_seconds ASC
                LIMIT 1
            )
            -- Return exact 4:00 PM close if found, otherwise nearest
            SELECT timestamp, symbol, previous_close
            FROM market_close_time
            WHERE previous_close IS NOT NULL

            UNION ALL

            SELECT timestamp, symbol, previous_close
            FROM nearest_close
            WHERE NOT EXISTS (SELECT 1 FROM market_close_time WHERE previous_close IS NOT NULL)

            LIMIT 1
        """

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(query, symbol)
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error fetching previous close: {e}")
            raise

    async def get_historical_quotes(
        self,
        symbol: str = 'SPY',
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 90,
        timeframe: str = '1min'
    ) -> List[Dict[str, Any]]:
        """Get historical quotes aggregated by timeframe."""
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        query = f"""
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM underlying_quotes
                WHERE symbol = $1
            ),
            bounds AS (
                SELECT
                    COALESCE($2::timestamptz, max_ts - ({step_interval} * ($4 - 1))) AS start_ts,
                    COALESCE($3::timestamptz, max_ts) AS end_ts
                FROM latest
            ),
            base AS (
                SELECT
                    {bucket} as bucket_ts,
                    symbol,
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    up_volume,
                    down_volume,
                    ROW_NUMBER() OVER (PARTITION BY {bucket} ORDER BY timestamp ASC) as rn_open,
                    ROW_NUMBER() OVER (PARTITION BY {bucket} ORDER BY timestamp DESC) as rn_close
                FROM underlying_quotes
                WHERE symbol = $1
                    AND timestamp BETWEEN (SELECT start_ts FROM bounds) AND (SELECT end_ts FROM bounds)
            )
            SELECT
                bucket_ts as timestamp,
                symbol,
                MAX(open) FILTER (WHERE rn_open = 1) as open,
                MAX(high) as high,
                MIN(low) as low,
                MAX(close) FILTER (WHERE rn_close = 1) as close,
                SUM(up_volume)::bigint as up_volume,
                SUM(down_volume)::bigint as down_volume,
                (SUM(up_volume) + SUM(down_volume))::bigint as volume
            FROM base
            GROUP BY bucket_ts, symbol
            ORDER BY timestamp DESC
            LIMIT $4
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, start_date, end_date, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching historical quotes: {e}")
            raise

    async def get_max_pain_timeseries(
        self,
        symbol: str = 'SPY',
        timeframe: str = '5min',
        limit: int = 90
    ) -> List[Dict[str, Any]]:
        """Get max pain timeseries aggregated to timeframe."""
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        query = f"""
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM gex_summary
                WHERE underlying = $1
            ),
            bounds AS (
                SELECT max_ts - ({step_interval} * ($2 - 1)) AS start_ts, max_ts AS end_ts
                FROM latest
            ),
            ranked AS (
                SELECT
                    {bucket} AS bucket_ts,
                    underlying AS symbol,
                    timestamp,
                    max_pain::numeric AS max_pain,
                    ROW_NUMBER() OVER (PARTITION BY {bucket} ORDER BY timestamp DESC) AS rn
                FROM gex_summary
                WHERE underlying = $1
                    AND timestamp BETWEEN (SELECT start_ts FROM bounds) AND (SELECT end_ts FROM bounds)
            )
            SELECT bucket_ts AS timestamp, symbol, max_pain
            FROM ranked
            WHERE rn = 1
            ORDER BY timestamp ASC
            LIMIT $2
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, symbol, limit)
            return [dict(row) for row in rows]

    async def get_max_pain_current(self, symbol: str = 'SPY', strike_limit: int = 200) -> Optional[Dict[str, Any]]:
        """Get current max pain and payout/notional grid by settlement strike."""
        query = """
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM option_chains
                WHERE underlying = $1
            ),
            contracts AS (
                SELECT strike, option_type, SUM(open_interest)::numeric AS oi
                FROM option_chains oc
                CROSS JOIN latest l
                WHERE oc.underlying = $1
                    AND oc.timestamp = l.max_ts
                GROUP BY strike, option_type
            ),
            strikes AS (
                SELECT DISTINCT strike
                FROM contracts
                ORDER BY strike
                LIMIT $2
            ),
            payout AS (
                SELECT
                    s.strike AS settlement_price,
                    SUM(CASE WHEN c.option_type = 'C' THEN GREATEST(s.strike - c.strike, 0) * c.oi * 100 ELSE 0 END)::numeric AS call_notional,
                    SUM(CASE WHEN c.option_type = 'P' THEN GREATEST(c.strike - s.strike, 0) * c.oi * 100 ELSE 0 END)::numeric AS put_notional
                FROM strikes s
                CROSS JOIN contracts c
                GROUP BY s.strike
            ),
            with_total AS (
                SELECT settlement_price, call_notional, put_notional,
                    (COALESCE(call_notional,0)+COALESCE(put_notional,0))::numeric AS total_notional
                FROM payout
            ),
            best AS (
                SELECT settlement_price AS max_pain
                FROM with_total
                ORDER BY total_notional ASC, settlement_price ASC
                LIMIT 1
            )
            SELECT
                (SELECT max_ts FROM latest) AS timestamp,
                $1::varchar AS symbol,
                (SELECT max_pain FROM best)::numeric AS max_pain,
                COALESCE(
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'settlement_price', settlement_price,
                            'call_notional', call_notional,
                            'put_notional', put_notional,
                            'total_notional', total_notional
                        ) ORDER BY settlement_price
                    ),
                    '[]'::json
                ) AS strikes
            FROM with_total
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, symbol, strike_limit)
            return dict(row) if row and row['timestamp'] else None

    # ========================================================================
    # Chart Data Queries
    # ========================================================================

    async def get_gex_heatmap(
        self,
        symbol: str = 'SPY',
        window_minutes: int = 60,
        interval_minutes: int = 5,
        timeframe: str = '5min'
    ) -> List[Dict[str, Any]]:
        """
        Get GEX data by strike over time for heatmap visualization
        Returns time-series data of GEX by strike aligned to underlying price timestamps
        """
        bucket = _bucket_expr(timeframe)
        query = f"""
            WITH latest_price_timestamp AS (
                -- Use latest underlying price timestamp as baseline
                SELECT MAX(timestamp) as max_ts
                FROM underlying_quotes
                WHERE symbol = $1
            ),
            time_window AS (
                SELECT 
                    max_ts - INTERVAL '1 minute' * $2 as start_time,
                    max_ts as end_time
                FROM latest_price_timestamp
            ),
            latest_price AS (
                SELECT close 
                FROM underlying_quotes 
                WHERE symbol = $1 
                ORDER BY timestamp DESC 
                LIMIT 1
            ),
            recent_data AS (
                SELECT 
                    {bucket} as timestamp,
                    strike,
                    AVG(net_gex) as net_gex
                FROM gex_by_strike
                WHERE underlying = $1
                    AND timestamp >= (SELECT start_time FROM time_window)
                    AND timestamp <= (SELECT end_time FROM time_window)
                GROUP BY 1, strike
            ),
            filtered_data AS (
                SELECT 
                    r.timestamp,
                    r.strike,
                    r.net_gex
                FROM recent_data r
                CROSS JOIN latest_price l
                WHERE ABS(r.strike - l.close) <= 50
            )
            SELECT 
                timestamp,
                strike,
                net_gex
            FROM filtered_data
            ORDER BY timestamp ASC, strike ASC
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, window_minutes)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching GEX heatmap: {e}")
            raise

    async def get_flow_timeseries(
        self,
        symbol: str = 'SPY',
        window_minutes: int = 60,
        interval_minutes: int = 5,
        timeframe: str = '5min'
    ) -> List[Dict[str, Any]]:
        """Get aggregated call/put notional flow over time for the last 90 intervals."""
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        query = f"""
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM option_chains_with_deltas
                WHERE underlying = $1
            ),
            bounds AS (
                SELECT max_ts - ({step_interval} * 89) AS start_ts, max_ts AS end_ts
                FROM latest
            ),
            agg AS (
                SELECT
                    {bucket} AS bucket_ts,
                    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'C' AND volume_delta > 0) AS call_notional,
                    SUM(volume_delta * last * 100) FILTER (WHERE option_type = 'P' AND volume_delta > 0) AS put_notional,
                    SUM(volume_delta) FILTER (WHERE option_type = 'C' AND volume_delta > 0) AS call_flow,
                    SUM(volume_delta) FILTER (WHERE option_type = 'P' AND volume_delta > 0) AS put_flow
                FROM option_chains_with_deltas
                WHERE underlying = $1
                    AND timestamp BETWEEN (SELECT start_ts FROM bounds) AND (SELECT end_ts FROM bounds)
                GROUP BY 1
            )
            SELECT
                bucket_ts AS timestamp,
                COALESCE(call_notional, 0)::numeric AS call_notional,
                COALESCE(put_notional, 0)::numeric AS put_notional,
                COALESCE(call_flow, 0)::numeric AS call_flow,
                COALESCE(put_flow, 0)::numeric AS put_flow,
                (COALESCE(call_notional, 0) - COALESCE(put_notional, 0))::numeric AS net_notional,
                (COALESCE(call_flow, 0) - COALESCE(put_flow, 0))::numeric AS net_flow
            FROM agg
            ORDER BY timestamp ASC
            LIMIT 90
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching flow timeseries: {e}")
            raise

    async def get_price_timeseries(
        self,
        symbol: str = 'SPY',
        window_minutes: int = 60,
        interval_minutes: int = 5,
        timeframe: str = '5min'
    ) -> List[Dict[str, Any]]:
        """Get underlying price time-series data for the last 90 intervals."""
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        query = f"""
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM underlying_quotes
                WHERE symbol = $1
            ),
            bounds AS (
                SELECT max_ts - ({step_interval} * 89) AS start_ts, max_ts AS end_ts
                FROM latest
            ),
            ranked AS (
                SELECT
                    {bucket} as bucket_ts,
                    timestamp,
                    close,
                    ROW_NUMBER() OVER (PARTITION BY {bucket} ORDER BY timestamp DESC) as rn
                FROM underlying_quotes
                WHERE symbol = $1
                    AND timestamp BETWEEN (SELECT start_ts FROM bounds) AND (SELECT end_ts FROM bounds)
            )
            SELECT
                bucket_ts as timestamp,
                close as price
            FROM ranked
            WHERE rn = 1
            ORDER BY timestamp ASC
            LIMIT 90
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching price timeseries: {e}")
            raise
