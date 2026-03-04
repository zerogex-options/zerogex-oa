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
import json

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

    async def _refresh_flow_cache(self, conn: asyncpg.Connection, symbol: str) -> None:
        """Refresh flow caches for only the latest minute snapshot for a symbol."""
        latest_ts = await conn.fetchval(
            """
            SELECT MAX(timestamp)
            FROM option_chains
            WHERE underlying = $1
            """,
            symbol,
        )
        if latest_ts is None:
            return

        type_exists = await conn.fetchval(
            """
            SELECT 1 FROM flow_cache_by_type_minute
            WHERE symbol = $1 AND timestamp = $2
            LIMIT 1
            """,
            symbol,
            latest_ts,
        )
        if not type_exists:
            await conn.execute(
                """
                WITH latest_rows AS (
                    SELECT oc.*
                    FROM option_chains oc
                    WHERE oc.underlying = $1
                      AND oc.timestamp = $2
                ),
                with_prev AS (
                    SELECT
                        l.timestamp,
                        l.option_symbol,
                        l.option_type,
                        l.strike,
                        l.expiration,
                        l.last,
                        l.implied_volatility,
                        l.delta,
                        CASE
                            WHEN p.prev_volume IS NULL THEN COALESCE(l.volume, 0)
                            WHEN (p.prev_ts AT TIME ZONE 'America/New_York')::date
                                = (l.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(l.volume, 0) - COALESCE(p.prev_volume, 0), 0)
                            ELSE COALESCE(l.volume, 0)
                        END::bigint AS volume_delta
                    FROM latest_rows l
                    LEFT JOIN LATERAL (
                        SELECT oc2.timestamp AS prev_ts, oc2.volume AS prev_volume
                        FROM option_chains oc2
                        WHERE oc2.option_symbol = l.option_symbol
                          AND oc2.timestamp < l.timestamp
                        ORDER BY oc2.timestamp DESC
                        LIMIT 1
                    ) p ON TRUE
                )
                INSERT INTO flow_cache_by_type_minute (
                    timestamp,
                    symbol,
                    option_type,
                    total_volume,
                    total_premium,
                    avg_iv,
                    net_delta
                )
                SELECT
                    timestamp,
                    $1::varchar,
                    option_type,
                    SUM(volume_delta)::bigint,
                    SUM(volume_delta * COALESCE(last, 0) * 100)::numeric,
                    AVG(implied_volatility)::numeric,
                    SUM(CASE WHEN option_type = 'C' THEN volume_delta ELSE -volume_delta END)::numeric
                FROM with_prev
                WHERE volume_delta > 0
                GROUP BY timestamp, option_type
                ON CONFLICT (timestamp, symbol, option_type)
                DO UPDATE SET
                    total_volume = EXCLUDED.total_volume,
                    total_premium = EXCLUDED.total_premium,
                    avg_iv = EXCLUDED.avg_iv,
                    net_delta = EXCLUDED.net_delta,
                    updated_at = NOW()
                """,
                symbol,
                latest_ts,
            )

        strike_exists = await conn.fetchval(
            """
            SELECT 1 FROM flow_cache_by_strike_minute
            WHERE symbol = $1 AND timestamp = $2
            LIMIT 1
            """,
            symbol,
            latest_ts,
        )
        if not strike_exists:
            await conn.execute(
                """
                WITH latest_rows AS (
                    SELECT oc.*
                    FROM option_chains oc
                    WHERE oc.underlying = $1
                      AND oc.timestamp = $2
                ),
                with_prev AS (
                    SELECT
                        l.timestamp,
                        l.option_type,
                        l.strike,
                        l.last,
                        l.implied_volatility,
                        CASE
                            WHEN p.prev_volume IS NULL THEN COALESCE(l.volume, 0)
                            WHEN (p.prev_ts AT TIME ZONE 'America/New_York')::date
                                = (l.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(l.volume, 0) - COALESCE(p.prev_volume, 0), 0)
                            ELSE COALESCE(l.volume, 0)
                        END::bigint AS volume_delta
                    FROM latest_rows l
                    LEFT JOIN LATERAL (
                        SELECT oc2.timestamp AS prev_ts, oc2.volume AS prev_volume
                        FROM option_chains oc2
                        WHERE oc2.option_symbol = l.option_symbol
                          AND oc2.timestamp < l.timestamp
                        ORDER BY oc2.timestamp DESC
                        LIMIT 1
                    ) p ON TRUE
                )
                INSERT INTO flow_cache_by_strike_minute (
                    timestamp,
                    symbol,
                    strike,
                    total_volume,
                    total_premium,
                    avg_iv,
                    net_delta
                )
                SELECT
                    timestamp,
                    $1::varchar,
                    strike,
                    SUM(volume_delta)::bigint,
                    SUM(volume_delta * COALESCE(last, 0) * 100)::numeric,
                    AVG(implied_volatility)::numeric,
                    SUM(CASE WHEN option_type = 'C' THEN volume_delta ELSE -volume_delta END)::numeric
                FROM with_prev
                WHERE volume_delta > 0
                GROUP BY timestamp, strike
                ON CONFLICT (timestamp, symbol, strike)
                DO UPDATE SET
                    total_volume = EXCLUDED.total_volume,
                    total_premium = EXCLUDED.total_premium,
                    avg_iv = EXCLUDED.avg_iv,
                    net_delta = EXCLUDED.net_delta,
                    updated_at = NOW()
                """,
                symbol,
                latest_ts,
            )

        smart_exists = await conn.fetchval(
            """
            SELECT 1 FROM flow_cache_smart_money_minute
            WHERE symbol = $1 AND timestamp = $2
            LIMIT 1
            """,
            symbol,
            latest_ts,
        )
        if not smart_exists:
            await conn.execute(
                """
                WITH latest_rows AS (
                    SELECT oc.*
                    FROM option_chains oc
                    WHERE oc.underlying = $1
                      AND oc.timestamp = $2
                ),
                with_prev AS (
                    SELECT
                        l.timestamp,
                        l.option_symbol,
                        l.option_type,
                        l.strike,
                        l.expiration,
                        l.last,
                        l.implied_volatility,
                        l.delta,
                        CASE
                            WHEN p.prev_volume IS NULL THEN COALESCE(l.volume, 0)
                            WHEN (p.prev_ts AT TIME ZONE 'America/New_York')::date
                                = (l.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(l.volume, 0) - COALESCE(p.prev_volume, 0), 0)
                            ELSE COALESCE(l.volume, 0)
                        END::bigint AS volume_delta
                    FROM latest_rows l
                    LEFT JOIN LATERAL (
                        SELECT oc2.timestamp AS prev_ts, oc2.volume AS prev_volume
                        FROM option_chains oc2
                        WHERE oc2.option_symbol = l.option_symbol
                          AND oc2.timestamp < l.timestamp
                        ORDER BY oc2.timestamp DESC
                        LIMIT 1
                    ) p ON TRUE
                )
                INSERT INTO flow_cache_smart_money_minute (
                    timestamp,
                    symbol,
                    option_symbol,
                    strike,
                    expiration,
                    option_type,
                    total_volume,
                    total_premium,
                    avg_iv,
                    avg_delta,
                    unusual_activity_score
                )
                SELECT
                    timestamp,
                    $1::varchar,
                    option_symbol,
                    strike,
                    expiration,
                    option_type,
                    volume_delta::bigint,
                    (volume_delta * COALESCE(last, 0) * 100)::numeric,
                    implied_volatility::numeric,
                    delta::numeric,
                    LEAST(10, GREATEST(0,
                        CASE WHEN volume_delta >= 500 THEN 4 WHEN volume_delta >= 200 THEN 3 WHEN volume_delta >= 100 THEN 2 WHEN volume_delta >= 50 THEN 1 ELSE 0 END +
                        CASE WHEN volume_delta * COALESCE(last, 0) * 100 >= 500000 THEN 4 WHEN volume_delta * COALESCE(last, 0) * 100 >= 250000 THEN 3 WHEN volume_delta * COALESCE(last, 0) * 100 >= 100000 THEN 2 WHEN volume_delta * COALESCE(last, 0) * 100 >= 50000 THEN 1 ELSE 0 END +
                        CASE WHEN implied_volatility > 1.0 THEN 2 WHEN implied_volatility > 0.6 THEN 1 ELSE 0 END
                    ))::numeric
                FROM with_prev
                WHERE volume_delta > 0
                ON CONFLICT (timestamp, symbol, option_symbol)
                DO UPDATE SET
                    strike = EXCLUDED.strike,
                    expiration = EXCLUDED.expiration,
                    option_type = EXCLUDED.option_type,
                    total_volume = EXCLUDED.total_volume,
                    total_premium = EXCLUDED.total_premium,
                    avg_iv = EXCLUDED.avg_iv,
                    avg_delta = EXCLUDED.avg_delta,
                    unusual_activity_score = EXCLUDED.unusual_activity_score,
                    updated_at = NOW()
                """,
                symbol,
                latest_ts,
            )

    async def _refresh_max_pain_snapshot(self, conn: asyncpg.Connection, symbol: str, strike_limit: int) -> None:
        """Refresh daily max pain OI snapshot for the symbol if latest chain timestamp changed."""
        strike_limit = max(10, min(strike_limit, 1000))
        query = """
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM option_chains
                WHERE underlying = $1
            ),
            existing AS (
                SELECT source_timestamp
                FROM max_pain_oi_snapshot
                WHERE symbol = $1
                  AND as_of_date = (
                      SELECT (max_ts AT TIME ZONE 'America/New_York')::date
                      FROM latest
                  )
            ),
            should_refresh AS (
                SELECT l.max_ts
                FROM latest l
                LEFT JOIN existing e ON TRUE
                WHERE l.max_ts IS NOT NULL
                  AND (e.source_timestamp IS NULL OR e.source_timestamp < l.max_ts)
            ),
            underlying AS (
                SELECT close::numeric AS underlying_price
                FROM underlying_quotes
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            contracts AS (
                SELECT
                    oc.expiration,
                    oc.strike,
                    oc.option_type,
                    SUM(oc.open_interest)::numeric AS oi
                FROM option_chains oc
                JOIN should_refresh r ON oc.timestamp = r.max_ts
                WHERE oc.underlying = $1
                  AND oc.open_interest > 0
                GROUP BY oc.expiration, oc.strike, oc.option_type
            ),
            ranked_strikes AS (
                SELECT
                    expiration,
                    strike,
                    ROW_NUMBER() OVER (PARTITION BY expiration ORDER BY strike) AS rn
                FROM (SELECT DISTINCT expiration, strike FROM contracts) s
            ),
            settlement_candidates AS (
                SELECT expiration, strike AS settlement_price
                FROM ranked_strikes
                WHERE rn <= $2
            ),
            payout AS (
                SELECT
                    s.expiration,
                    s.settlement_price,
                    SUM(CASE WHEN c.option_type = 'C' THEN GREATEST(s.settlement_price - c.strike, 0) * c.oi * 100 ELSE 0 END)::numeric AS call_notional,
                    SUM(CASE WHEN c.option_type = 'P' THEN GREATEST(c.strike - s.settlement_price, 0) * c.oi * 100 ELSE 0 END)::numeric AS put_notional
                FROM settlement_candidates s
                JOIN contracts c ON c.expiration = s.expiration
                GROUP BY s.expiration, s.settlement_price
            ),
            with_total AS (
                SELECT
                    expiration,
                    settlement_price,
                    call_notional,
                    put_notional,
                    (COALESCE(call_notional, 0) + COALESCE(put_notional, 0))::numeric AS total_notional
                FROM payout
            ),
            best_per_exp AS (
                SELECT DISTINCT ON (expiration)
                    expiration,
                    settlement_price AS max_pain
                FROM with_total
                ORDER BY expiration, total_notional ASC, settlement_price ASC
            ),
            expiration_payload AS (
                SELECT
                    b.expiration,
                    b.max_pain,
                    (b.max_pain - u.underlying_price)::numeric AS difference_from_underlying,
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'expiration', wt.expiration,
                            'settlement_price', wt.settlement_price,
                            'call_notional', wt.call_notional,
                            'put_notional', wt.put_notional,
                            'total_notional', wt.total_notional
                        ) ORDER BY wt.settlement_price
                    ) AS strikes
                FROM best_per_exp b
                JOIN with_total wt ON wt.expiration = b.expiration
                CROSS JOIN underlying u
                GROUP BY b.expiration, b.max_pain, u.underlying_price
            ),
            snapshot_payload AS (
                SELECT
                    (r.max_ts AT TIME ZONE 'America/New_York')::date AS as_of_date,
                    r.max_ts AS source_timestamp,
                    $1::varchar AS symbol,
                    u.underlying_price,
                    bp.max_pain,
                    (bp.max_pain - u.underlying_price)::numeric AS difference,
                    COALESCE(
                        JSON_AGG(
                            JSON_BUILD_OBJECT(
                                'expiration', ep.expiration,
                                'max_pain', ep.max_pain,
                                'difference_from_underlying', ep.difference_from_underlying,
                                'strikes', ep.strikes
                            ) ORDER BY ep.expiration
                        ),
                        '[]'::json
                    ) AS expirations
                FROM should_refresh r
                CROSS JOIN underlying u
                LEFT JOIN LATERAL (
                    SELECT max_pain
                    FROM best_per_exp
                    ORDER BY expiration
                    LIMIT 1
                ) bp ON TRUE
                LEFT JOIN expiration_payload ep ON TRUE
                GROUP BY r.max_ts, u.underlying_price, bp.max_pain
            )
            INSERT INTO max_pain_oi_snapshot (
                symbol,
                as_of_date,
                source_timestamp,
                underlying_price,
                max_pain,
                difference,
                expirations
            )
            SELECT
                symbol,
                as_of_date,
                source_timestamp,
                underlying_price,
                max_pain,
                difference,
                expirations::jsonb
            FROM snapshot_payload
            WHERE max_pain IS NOT NULL
            ON CONFLICT (symbol, as_of_date)
            DO UPDATE SET
                source_timestamp = EXCLUDED.source_timestamp,
                underlying_price = EXCLUDED.underlying_price,
                max_pain = EXCLUDED.max_pain,
                difference = EXCLUDED.difference,
                expirations = EXCLUDED.expirations,
                updated_at = NOW()
        """
        await conn.execute(query, symbol, strike_limit)

        sync_expirations_query = """
            WITH snap AS (
                SELECT symbol, as_of_date, source_timestamp, expirations
                FROM max_pain_oi_snapshot
                WHERE symbol = $1
                ORDER BY as_of_date DESC
                LIMIT 1
            ),
            parsed AS (
                SELECT
                    s.symbol,
                    s.as_of_date,
                    s.source_timestamp,
                    (e->>'expiration')::date AS expiration,
                    (e->>'max_pain')::numeric AS max_pain,
                    (e->>'difference_from_underlying')::numeric AS difference_from_underlying,
                    (e->'strikes')::jsonb AS strikes
                FROM snap s
                CROSS JOIN LATERAL jsonb_array_elements(s.expirations) e
            )
            INSERT INTO max_pain_oi_snapshot_expiration (
                symbol,
                as_of_date,
                source_timestamp,
                expiration,
                max_pain,
                difference_from_underlying,
                strikes
            )
            SELECT
                symbol,
                as_of_date,
                source_timestamp,
                expiration,
                max_pain,
                difference_from_underlying,
                strikes
            FROM parsed
            ON CONFLICT (symbol, as_of_date, expiration)
            DO UPDATE SET
                source_timestamp = EXCLUDED.source_timestamp,
                max_pain = EXCLUDED.max_pain,
                difference_from_underlying = EXCLUDED.difference_from_underlying,
                strikes = EXCLUDED.strikes,
                updated_at = NOW()
        """
        await conn.execute(sync_expirations_query, symbol)

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
        """Get option flow by type (calls vs puts) across the full selected interval."""
        query = """
            WITH latest AS (
                SELECT MAX(timestamp) AS max_ts
                FROM flow_cache_by_type_minute
                WHERE symbol = $1
            ),
            bounds AS (
                SELECT max_ts - INTERVAL '1 minute' * $2 AS time_window_start, max_ts AS time_window_end
                FROM latest
            )
            SELECT
                b.time_window_start,
                b.time_window_end,
                c.symbol,
                CASE WHEN c.option_type = 'C' THEN 'CALL' ELSE 'PUT' END AS option_type,
                c.total_volume,
                c.total_premium,
                c.avg_iv,
                c.net_delta,
                CASE
                    WHEN c.option_type = 'C' AND c.total_premium > COALESCE((
                        SELECT c2.total_premium FROM flow_cache_by_type_minute c2
                        WHERE c2.symbol = c.symbol
                          AND c2.timestamp = c.timestamp
                          AND c2.option_type = 'P'
                    ), 0) THEN 'bullish'
                    WHEN c.option_type = 'P' AND c.total_premium > COALESCE((
                        SELECT c2.total_premium FROM flow_cache_by_type_minute c2
                        WHERE c2.symbol = c.symbol
                          AND c2.timestamp = c.timestamp
                          AND c2.option_type = 'C'
                    ), 0) THEN 'bearish'
                    ELSE 'neutral'
                END AS sentiment
            FROM flow_cache_by_type_minute c
            CROSS JOIN bounds b
            WHERE c.symbol = $1
              AND c.timestamp BETWEEN b.time_window_start AND b.time_window_end
            ORDER BY c.timestamp DESC, option_type
        """

        try:
            async with self.pool.acquire() as conn:
                await self._refresh_flow_cache(conn, symbol)
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
                FROM flow_cache_by_strike_minute
                WHERE symbol = $1
            ),
            bounds AS (
                SELECT max_ts - INTERVAL '1 minute' * $2 AS time_window_start, max_ts AS time_window_end
                FROM latest
            ),
            strike_agg AS (
                SELECT
                    c.strike,
                    SUM(c.total_volume)::bigint AS total_volume,
                    SUM(c.total_premium)::numeric AS total_premium,
                    AVG(c.avg_iv)::numeric AS avg_iv,
                    SUM(c.net_delta)::numeric AS net_delta
                FROM flow_cache_by_strike_minute c
                CROSS JOIN bounds b
                WHERE c.symbol = $1
                  AND c.timestamp BETWEEN b.time_window_start AND b.time_window_end
                GROUP BY c.strike
            )
            SELECT
                b.time_window_start,
                b.time_window_end,
                $1::varchar AS symbol,
                s.strike,
                s.total_volume,
                s.total_premium,
                s.avg_iv,
                s.net_delta
            FROM strike_agg s
            CROSS JOIN bounds b
            ORDER BY s.total_premium DESC NULLS LAST
            LIMIT $3
        """

        try:
            async with self.pool.acquire() as conn:
                await self._refresh_flow_cache(conn, symbol)
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
                FROM flow_cache_smart_money_minute
                WHERE symbol = $1
            ),
            bounds AS (
                SELECT max_ts - INTERVAL '1 minute' * $2 AS time_window_start, max_ts AS time_window_end
                FROM latest
            )
            SELECT
                b.time_window_start,
                b.time_window_end,
                c.symbol,
                c.option_type,
                c.strike,
                c.total_volume,
                c.total_premium,
                c.avg_iv,
                c.unusual_activity_score
            FROM flow_cache_smart_money_minute c
            CROSS JOIN bounds b
            WHERE c.symbol = $1
              AND c.timestamp BETWEEN b.time_window_start AND b.time_window_end
            ORDER BY c.unusual_activity_score DESC, c.total_premium DESC
            LIMIT $3
        """

        try:
            async with self.pool.acquire() as conn:
                await self._refresh_flow_cache(conn, symbol)
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
        """Get current max pain from daily OI snapshot cache."""
        snapshot_query = """
            SELECT
                symbol,
                as_of_date,
                source_timestamp AS timestamp,
                underlying_price,
                max_pain,
                difference
            FROM max_pain_oi_snapshot
            WHERE symbol = $1
            ORDER BY as_of_date DESC
            LIMIT 1
        """
        expiration_query = """
            SELECT
                expiration,
                max_pain,
                difference_from_underlying,
                strikes
            FROM max_pain_oi_snapshot_expiration
            WHERE symbol = $1
              AND as_of_date = $2
            ORDER BY expiration
        """

        async with self.pool.acquire() as conn:
            await self._refresh_max_pain_snapshot(conn, symbol, strike_limit)
            snapshot = await conn.fetchrow(snapshot_query, symbol)
            if not snapshot:
                return None

            expiration_rows = await conn.fetch(expiration_query, symbol, snapshot['as_of_date'])
            expirations: List[Dict[str, Any]] = []
            for row in expiration_rows:
                strikes = row['strikes']
                if isinstance(strikes, str):
                    strikes = json.loads(strikes)
                expirations.append({
                    'expiration': row['expiration'],
                    'max_pain': row['max_pain'],
                    'difference_from_underlying': row['difference_from_underlying'],
                    'strikes': strikes or [],
                })

            return {
                'timestamp': snapshot['timestamp'],
                'symbol': snapshot['symbol'],
                'underlying_price': snapshot['underlying_price'],
                'max_pain': snapshot['max_pain'],
                'difference': snapshot['difference'],
                'expirations': expirations,
            }

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


