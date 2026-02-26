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
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get historical GEX summary data"""
        if not start_date:
            start_date = datetime.now() - timedelta(days=1)
        if not end_date:
            end_date = datetime.now()

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
                AND timestamp BETWEEN $2 AND $3
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
        """Get option flow by type (calls vs puts)"""
        query = """
            SELECT 
                time_et as time_window_start,
                timestamp as time_window_end,
                underlying as symbol,
                'CALL' as option_type,
                call_flow as total_volume,
                call_notional as total_premium,
                NULL::numeric as avg_iv,
                call_flow - put_flow as net_delta,
                CASE 
                    WHEN call_flow > put_flow THEN 'bullish'
                    WHEN put_flow > call_flow THEN 'bearish'
                    ELSE 'neutral'
                END as sentiment
            FROM option_flow_by_type
            WHERE underlying = $1
                AND timestamp >= $2

            UNION ALL

            SELECT 
                time_et as time_window_start,
                timestamp as time_window_end,
                underlying as symbol,
                'PUT' as option_type,
                put_flow as total_volume,
                put_notional as total_premium,
                NULL::numeric as avg_iv,
                put_flow - call_flow as net_delta,
                CASE 
                    WHEN put_flow > call_flow THEN 'bearish'
                    WHEN call_flow > put_flow THEN 'bullish'
                    ELSE 'neutral'
                END as sentiment
            FROM option_flow_by_type
            WHERE underlying = $1
                AND timestamp >= $2

            ORDER BY time_window_end DESC, option_type
        """

        try:
            async with self.pool.acquire() as conn:
                # Calculate cutoff time in Python
                cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
                rows = await conn.fetch(query, symbol, cutoff_time)
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
        """Get option flow by strike level"""
        query = """
            SELECT 
                time_et as time_window_start,
                timestamp as time_window_end,
                underlying as symbol,
                strike,
                total_flow as total_volume,
                total_notional as total_premium,
                avg_iv,
                net_flow as net_delta
            FROM option_flow_by_strike
            WHERE underlying = $1
                AND timestamp >= $2
            ORDER BY total_notional DESC
            LIMIT $3
        """

        try:
            async with self.pool.acquire() as conn:
                # Calculate cutoff time in Python
                cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
                rows = await conn.fetch(query, symbol, cutoff_time, limit)
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
        """Get unusual activity / smart money flow"""
        query = """
            SELECT 
                time_et as time_window_start,
                timestamp as time_window_end,
                underlying as symbol,
                option_type,
                strike,
                flow as total_volume,
                notional as total_premium,
                iv as avg_iv,
                unusual_score as unusual_activity_score,
                size_class,
                notional_class,
                moneyness
            FROM option_flow_smart_money
            WHERE underlying = $1
                AND timestamp >= $2
                AND unusual_score > 5
            ORDER BY unusual_score DESC, notional DESC
            LIMIT $3
        """

        try:
            async with self.pool.acquire() as conn:
                # Calculate cutoff time in Python
                cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
                rows = await conn.fetch(query, symbol, cutoff_time, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching smart money flow: {e}")
            raise

    # ========================================================================
    # Day Trading Views Queries
    # ========================================================================

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

    async def get_historical_quotes(
        self,
        symbol: str = 'SPY',
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get historical quotes"""
        if not start_date:
            start_date = datetime.now() - timedelta(days=1)
        if not end_date:
            end_date = datetime.now()

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
                AND timestamp BETWEEN $2 AND $3
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
