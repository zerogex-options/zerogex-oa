"""
Database manager for API queries
Uses asyncpg for async PostgreSQL operations
"""

import asyncio
import asyncpg
import os
import time as time_module
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta, date, time, timezone
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo
import logging
import json

from .signal_metrics import calibrate_signal, classify_regime

logger = logging.getLogger(__name__)


_ET = ZoneInfo('America/New_York')


def _get_session_bounds(session: str = 'current') -> tuple:
    """Return (start_ts, end_ts) as timezone-aware datetimes for the requested trading session.

    'current': today 09:30–now if market is open, else most recent session 09:30–16:15 ET.
    'prior':   the full trading session immediately before the current one.
    """
    now_et = datetime.now(_ET)
    today = now_et.date()
    market_open_time = time(9, 30)
    market_close_time = time(16, 15)

    def prev_trading_day(d):
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    def make_ts(d, t):
        return datetime(d.year, d.month, d.day, t.hour, t.minute, tzinfo=_ET)

    # Current session date: last/current weekday on or after market open
    is_weekday = today.weekday() < 5
    past_open = now_et.time() >= market_open_time
    current_session_date = today if (is_weekday and past_open) else prev_trading_day(today)

    market_is_open = (current_session_date == today and now_et.time() < market_close_time)

    if session == 'current':
        start = make_ts(current_session_date, market_open_time)
        end = now_et if market_is_open else make_ts(current_session_date, market_close_time)
    else:  # 'prior'
        prior_date = prev_trading_day(current_session_date)
        start = make_ts(prior_date, market_open_time)
        end = make_ts(prior_date, market_close_time)

    return start, end


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


def _timeframe_view_suffix(timeframe: str) -> str:
    timeframe = _normalize_timeframe(timeframe)
    mapping = {
        '1min': '1min',
        '5min': '5min',
        '15min': '15min',
        '1hr': '1hr',
        '1day': '1day',
    }
    if timeframe not in mapping:
        raise ValueError(f'Unsupported timeframe: {timeframe}')
    return mapping[timeframe]

class DatabaseManager:
    """Manages database connections and queries"""

    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self._pool_lock = asyncio.Lock()
        self._last_flow_refresh_by_symbol: Dict[str, float] = {}
        self._flow_refresh_min_seconds: float = float(
            os.getenv("FLOW_CACHE_REFRESH_MIN_SECONDS", "15")
        )
        self._latest_quote_cache_ttl_seconds: float = float(
            os.getenv("LATEST_QUOTE_CACHE_TTL_SECONDS", "1.5")
        )
        self._latest_gex_summary_cache_ttl_seconds: float = float(
            os.getenv("LATEST_GEX_SUMMARY_CACHE_TTL_SECONDS", "1.5")
        )
        # TTL for analytics-derived endpoints (gex_by_strike, gex_walls, etc.)
        # that only change on the analytics cycle (~60s). A moderate TTL
        # eliminates redundant DB round-trips from rapid frontend polling.
        self._analytics_cache_ttl_seconds: float = float(
            os.getenv("ANALYTICS_CACHE_TTL_SECONDS", "5.0")
        )
        # Flow endpoints are frequently polled by the frontend. A short TTL
        # dramatically cuts repeated heavy reads while keeping intraday charts
        # effectively real-time.
        self._flow_endpoint_cache_ttl_seconds: float = float(
            os.getenv("FLOW_ENDPOINT_CACHE_TTL_SECONDS", "3.0")
        )
        self._read_cache: Dict[str, Tuple[float, Any]] = {}
        self._load_credentials()

    def _cache_get(self, key: str) -> Optional[Any]:
        """Get a cached value if it has not expired."""
        cached = self._read_cache.get(key)
        if not cached:
            return None
        expires_at, payload = cached
        if time_module.monotonic() >= expires_at:
            self._read_cache.pop(key, None)
            return None
        return payload

    def _cache_set(self, key: str, payload: Any, ttl_seconds: float) -> None:
        """Store a value in the short-lived in-memory read cache."""
        if ttl_seconds <= 0:
            return
        self._read_cache[key] = (time_module.monotonic() + ttl_seconds, payload)

    async def _create_pool(self) -> asyncpg.Pool:
        """Create and return a fresh asyncpg pool instance."""
        connect_timeout = float(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "20"))
        # Keep defaults conservative to avoid exhausting RDS connections when
        # multiple services/workers run at once.
        min_size = int(os.getenv("DB_POOL_MIN", "1"))
        max_size = int(os.getenv("DB_POOL_MAX", "3"))
        statement_timeout_ms = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "30000"))
        ssl_mode = os.getenv("DB_SSLMODE", "").strip().lower()
        ssl = None
        if ssl_mode in {"require", "verify-ca", "verify-full"}:
            ssl = True
        if min_size > max_size:
            min_size = max_size
        logger.info(
            "Creating asyncpg pool (min=%d, max=%d, timeout=%.1fs)",
            min_size,
            max_size,
            connect_timeout,
        )
        return await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            min_size=min_size,
            max_size=max_size,
            command_timeout=30,
            max_inactive_connection_lifetime=120,
            timeout=connect_timeout,
            ssl=ssl,
            server_settings={
                "statement_timeout": str(statement_timeout_ms),
            },
        )

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
        self.database = os.getenv('DB_NAME', 'zerogex')
        self.user = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')

    async def connect(self):
        """Create connection pool"""
        retries = int(os.getenv("DB_CONNECT_RETRIES", "5"))
        retry_base_delay = float(os.getenv("DB_CONNECT_RETRY_DELAY_SECONDS", "1.5"))
        last_error: Optional[Exception] = None

        for attempt in range(1, retries + 1):
            try:
                async with self._pool_lock:
                    if not self._pool_is_usable(self.pool):
                        self.pool = await self._create_pool()
                logger.info(f"Database pool created: {self.database}@{self.host}")
                return
            except Exception as e:
                last_error = e
                logger.error(
                    "Failed to create database pool (attempt %d/%d): %r",
                    attempt,
                    retries,
                    e,
                    exc_info=True,
                )
                if attempt < retries:
                    delay = retry_base_delay * attempt
                    logger.warning("Retrying database pool creation in %.1fs...", delay)
                    await asyncio.sleep(delay)

        raise RuntimeError(f"Failed to create database pool after {retries} attempts: {last_error}")

    async def disconnect(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")

    @staticmethod
    def _pool_is_usable(pool: Optional[asyncpg.Pool]) -> bool:
        """True when pool exists and is not already closing."""
        if pool is None:
            return False
        try:
            return not pool.is_closing()
        except Exception:
            return False

    @staticmethod
    def _is_transient_db_error(error: Exception) -> bool:
        text = str(error).lower()
        return any(
            token in text
            for token in (
                "ssl handshake",
                "ssl syscall error",
                "eof detected",
                "connection reset",
                "connection refused",
                "connection is closed",
                "pool is closed",
                "pool is closing",
                "timeout",
            )
        ) or isinstance(error, (TimeoutError, ConnectionError, OSError))

    async def _reconnect_pool(self) -> None:
        """Reconnect DB pool once under lock."""
        async with self._pool_lock:
            old_pool = self.pool
            self.pool = await self._create_pool()
        if old_pool is not None:
            try:
                await old_pool.close()
            except Exception:
                logger.warning("Failed to close old pool during reconnect", exc_info=True)

    @asynccontextmanager
    async def _acquire_connection(self):
        """
        Acquire a DB connection from the existing pool.

        Fail fast when pool is unavailable/closing to avoid hidden retries and
        request-level latency amplification.
        """
        for attempt in range(2):
            pool = self.pool
            if not self._pool_is_usable(pool):
                raise RuntimeError("Database pool is unavailable or closing")
            try:
                async with pool.acquire() as conn:
                    yield conn
                    return
            except Exception as e:
                if attempt == 0 and self._is_transient_db_error(e):
                    logger.warning("Transient DB acquire error; reconnecting pool and retrying once", exc_info=True)
                    await self._reconnect_pool()
                    continue
                raise

    async def check_health(self) -> bool:
        """Check database connection health"""
        try:
            async with self._acquire_connection() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    @staticmethod
    def _decode_json_field(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    def _normalize_flow_payload(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row['total_volume'] = self._decode_json_field(row.get('total_volume'))
        row['total_premium'] = self._decode_json_field(row.get('total_premium'))
        return row

    async def _refresh_flow_cache(self, conn: asyncpg.Connection, symbol: str) -> None:
        """Refresh flow caches for the latest snapshot and any recent missing minutes.

        Failures here are non-fatal: the endpoint will serve whatever data
        already exists in the cache tables rather than returning a 500.
        """
        now = time_module.monotonic()
        last_refresh = self._last_flow_refresh_by_symbol.get(symbol, 0.0)
        if (now - last_refresh) < self._flow_refresh_min_seconds:
            return

        try:
            async with conn.transaction():
                await self._do_refresh_flow_cache(conn, symbol)
        except Exception as e:
            logger.warning(f"Flow cache refresh failed for {symbol} (non-fatal): {e}")
        finally:
            # Always update the throttle timestamp so we don't retry
            # a failing refresh on every request.
            self._last_flow_refresh_by_symbol[symbol] = time_module.monotonic()

    async def _do_refresh_flow_cache(self, conn: asyncpg.Connection, symbol: str) -> None:
        """Inner implementation of flow cache refresh."""
        canonical_only = os.getenv("FLOW_CANONICAL_ONLY", "true").lower() == "true"
        # Use ORDER BY + LIMIT 1 instead of MAX() to exploit the
        # (underlying, timestamp DESC) index as an index-only scan.
        latest_ts = await conn.fetchval(
            """
            SELECT timestamp
            FROM option_chains
            WHERE underlying = $1
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            symbol,
        )
        if latest_ts is None:
            return

        # Fetch underlying price at this timestamp as a fallback.
        underlying_price = await conn.fetchval(
            """
            SELECT close
            FROM underlying_quotes
            WHERE symbol = $1
              AND timestamp <= $2
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            symbol,
            latest_ts,
        )

        last_fact_ts = await conn.fetchval(
            """
            SELECT timestamp
            FROM flow_contract_facts
            WHERE symbol = $1
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            symbol,
        )

        # Backfill from the last known canonical flow row, bounded to a recent window
        # so one request can repair short outages without unbounded scan cost.
        try:
            canonical_backfill_minutes = max(
                1,
                int(os.getenv("FLOW_CANONICAL_BACKFILL_MINUTES", "90")),
            )
        except ValueError:
            canonical_backfill_minutes = 90

        backfill_start = (
            latest_ts - timedelta(minutes=canonical_backfill_minutes)
            if last_fact_ts is None
            else max(
                last_fact_ts - timedelta(minutes=1),
                latest_ts - timedelta(minutes=canonical_backfill_minutes),
            )
        )

        # Canonical per-contract fact table used as the source of truth for flow APIs.
        # Uses LAG() window function instead of LATERAL join for O(n) vs O(n²) perf.
        await conn.execute(
            """
            WITH window_rows AS (
                SELECT
                    oc.timestamp,
                    oc.underlying AS symbol,
                    oc.option_symbol,
                    oc.strike,
                    oc.expiration,
                    oc.option_type,
                    oc.volume,
                    oc.ask_volume,
                    oc.bid_volume,
                    oc.last,
                    oc.mid,
                    oc.bid,
                    oc.ask,
                    oc.implied_volatility,
                    oc.delta
                FROM option_chains oc
                WHERE oc.underlying = $1
                  AND oc.timestamp >= $2
                  AND oc.timestamp <= $3
            ),
            active_symbols AS (
                SELECT DISTINCT option_symbol
                FROM window_rows
            ),
            seed_rows AS (
                SELECT
                    oc.timestamp,
                    oc.underlying AS symbol,
                    oc.option_symbol,
                    oc.strike,
                    oc.expiration,
                    oc.option_type,
                    oc.volume,
                    oc.ask_volume,
                    oc.bid_volume,
                    oc.last,
                    oc.mid,
                    oc.bid,
                    oc.ask,
                    oc.implied_volatility,
                    oc.delta
                FROM active_symbols s
                JOIN LATERAL (
                    SELECT
                        oc.timestamp,
                        oc.underlying,
                        oc.option_symbol,
                        oc.strike,
                        oc.expiration,
                        oc.option_type,
                        oc.volume,
                        oc.ask_volume,
                        oc.bid_volume,
                        oc.last,
                        oc.mid,
                        oc.bid,
                        oc.ask,
                        oc.implied_volatility,
                        oc.delta
                    FROM option_chains oc
                    WHERE oc.underlying = $1
                      AND oc.option_symbol = s.option_symbol
                      AND oc.timestamp < $2
                    ORDER BY oc.timestamp DESC
                    LIMIT 1
                ) oc ON TRUE
            ),
            source_rows AS (
                SELECT * FROM seed_rows
                UNION ALL
                SELECT * FROM window_rows
            ),
            with_prev AS (
                SELECT
                    s.timestamp,
                    s.symbol,
                    s.option_symbol,
                    s.strike,
                    s.expiration,
                    s.option_type,
                    COALESCE(s.last, s.mid, (COALESCE(s.bid, 0) + COALESCE(s.ask, 0)) / 2.0, 0) AS trade_price,
                    s.implied_volatility,
                    s.delta,
                    CASE
                        WHEN LAG(s.volume) OVER w IS NULL THEN COALESCE(s.volume, 0)
                        WHEN (LAG(s.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                            = (s.timestamp AT TIME ZONE 'America/New_York')::date
                            THEN GREATEST(COALESCE(s.volume, 0) - COALESCE(LAG(s.volume) OVER w, 0), 0)
                        ELSE COALESCE(s.volume, 0)
                    END::bigint AS volume_delta,
                    CASE
                        WHEN LAG(s.ask_volume) OVER w IS NULL THEN COALESCE(s.ask_volume, 0)
                        WHEN (LAG(s.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                            = (s.timestamp AT TIME ZONE 'America/New_York')::date
                            THEN GREATEST(COALESCE(s.ask_volume, 0) - COALESCE(LAG(s.ask_volume) OVER w, 0), 0)
                        ELSE COALESCE(s.ask_volume, 0)
                    END::bigint AS ask_vol_delta,
                    CASE
                        WHEN LAG(s.bid_volume) OVER w IS NULL THEN COALESCE(s.bid_volume, 0)
                        WHEN (LAG(s.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                            = (s.timestamp AT TIME ZONE 'America/New_York')::date
                            THEN GREATEST(COALESCE(s.bid_volume, 0) - COALESCE(LAG(s.bid_volume) OVER w, 0), 0)
                        ELSE COALESCE(s.bid_volume, 0)
                    END::bigint AS bid_vol_delta
                FROM source_rows s
                WINDOW w AS (PARTITION BY s.option_symbol ORDER BY s.timestamp)
            )
            INSERT INTO flow_contract_facts (
                timestamp, symbol, option_symbol, strike, expiration, option_type,
                volume_delta, premium_delta, signed_volume, signed_premium,
                buy_volume, sell_volume, buy_premium, sell_premium,
                implied_volatility, delta, underlying_price
            )
            SELECT
                timestamp,
                symbol,
                option_symbol,
                strike,
                expiration,
                option_type,
                volume_delta,
                (volume_delta * trade_price * 100)::numeric AS premium_delta,
                (CASE WHEN option_type = 'C' THEN volume_delta ELSE -volume_delta END)::bigint AS signed_volume,
                (CASE WHEN option_type = 'C' THEN 1 ELSE -1 END * volume_delta * trade_price * 100)::numeric AS signed_premium,
                -- Scale buy/sell volumes to account for unclassified volume.
                -- The classified subset (ask + bid) provides the directional
                -- signal; extrapolate to the full volume_delta assuming the
                -- unclassified portion has the same buy/sell ratio.
                CASE WHEN (ask_vol_delta + bid_vol_delta) > 0
                     THEN (ask_vol_delta::numeric / (ask_vol_delta + bid_vol_delta) * volume_delta)::bigint
                     ELSE 0
                END AS buy_volume,
                CASE WHEN (ask_vol_delta + bid_vol_delta) > 0
                     THEN (bid_vol_delta::numeric / (ask_vol_delta + bid_vol_delta) * volume_delta)::bigint
                     ELSE 0
                END AS sell_volume,
                CASE WHEN (ask_vol_delta + bid_vol_delta) > 0
                     THEN (ask_vol_delta::numeric / (ask_vol_delta + bid_vol_delta)
                           * volume_delta * trade_price * 100)::numeric
                     ELSE 0
                END AS buy_premium,
                CASE WHEN (ask_vol_delta + bid_vol_delta) > 0
                     THEN (bid_vol_delta::numeric / (ask_vol_delta + bid_vol_delta)
                           * volume_delta * trade_price * 100)::numeric
                     ELSE 0
                END AS sell_premium,
                implied_volatility,
                delta,
                COALESCE(
                    (
                        SELECT uq.close::numeric
                        FROM underlying_quotes uq
                        WHERE uq.symbol = $1
                          AND uq.timestamp <= with_prev.timestamp
                        ORDER BY uq.timestamp DESC
                        LIMIT 1
                    ),
                    $4::numeric
                ) AS underlying_price
            FROM with_prev
            WHERE timestamp >= $2
              AND timestamp > $5
              AND volume_delta > 0
            ON CONFLICT (timestamp, symbol, option_symbol)
            DO UPDATE SET
                volume_delta = EXCLUDED.volume_delta,
                premium_delta = EXCLUDED.premium_delta,
                signed_volume = EXCLUDED.signed_volume,
                signed_premium = EXCLUDED.signed_premium,
                buy_volume = EXCLUDED.buy_volume,
                sell_volume = EXCLUDED.sell_volume,
                buy_premium = EXCLUDED.buy_premium,
                sell_premium = EXCLUDED.sell_premium,
                implied_volatility = EXCLUDED.implied_volatility,
                delta = EXCLUDED.delta,
                underlying_price = EXCLUDED.underlying_price,
                updated_at = NOW()
            """,
            symbol,
            backfill_start,
            latest_ts,
            underlying_price,
            last_fact_ts if last_fact_ts is not None else datetime(1970, 1, 1, tzinfo=timezone.utc),
        )

        if not canonical_only:
            # One-time bootstrap for the new expiration cache table so endpoint
            # can serve historical buckets immediately after deployment.
            expiration_seeded = await conn.fetchval(
                """
                SELECT 1
                FROM flow_by_expiration
                WHERE symbol = $1
                LIMIT 1
                """,
                symbol,
            )
            if not expiration_seeded:
                await conn.execute(
                    """
                    INSERT INTO flow_by_expiration (
                        timestamp,
                        symbol,
                        expiration,
                        total_volume,
                        total_premium
                    )
                    SELECT
                        timestamp,
                        underlying,
                        expiration,
                        SUM(volume_delta)::bigint,
                        SUM(volume_delta * COALESCE(last, 0) * 100)::numeric
                    FROM option_chains_with_deltas
                    WHERE underlying = $1
                      AND timestamp >= NOW() - INTERVAL '90 minutes'
                      AND volume_delta > 0
                    GROUP BY timestamp, underlying, expiration
                    ON CONFLICT (timestamp, symbol, expiration)
                    DO NOTHING
                    """,
                    symbol,
                )

        type_exists = True if canonical_only else await conn.fetchval(
            """
            SELECT 1 FROM flow_by_type
            WHERE symbol = $1 AND timestamp = $2
            LIMIT 1
            """,
            symbol,
            latest_ts,
        )
        if not type_exists:
            await conn.execute(
                """
                WITH with_prev AS (
                    SELECT
                        oc.timestamp,
                        oc.option_symbol,
                        oc.option_type,
                        oc.strike,
                        oc.expiration,
                        oc.last,
                        oc.implied_volatility,
                        oc.delta,
                        CASE
                            WHEN LAG(oc.volume) OVER w IS NULL THEN COALESCE(oc.volume, 0)
                            WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(oc.volume, 0) - COALESCE(LAG(oc.volume) OVER w, 0), 0)
                            ELSE COALESCE(oc.volume, 0)
                        END::bigint AS volume_delta,
                        CASE
                            WHEN LAG(oc.ask_volume) OVER w IS NULL THEN COALESCE(oc.ask_volume, 0)
                            WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(oc.ask_volume, 0) - COALESCE(LAG(oc.ask_volume) OVER w, 0), 0)
                            ELSE COALESCE(oc.ask_volume, 0)
                        END::bigint AS ask_vol_delta,
                        CASE
                            WHEN LAG(oc.bid_volume) OVER w IS NULL THEN COALESCE(oc.bid_volume, 0)
                            WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(oc.bid_volume, 0) - COALESCE(LAG(oc.bid_volume) OVER w, 0), 0)
                            ELSE COALESCE(oc.bid_volume, 0)
                        END::bigint AS bid_vol_delta
                    FROM option_chains oc
                    WHERE oc.underlying = $1
                      AND oc.timestamp >= $2::timestamptz - INTERVAL '2 minutes'
                      AND oc.timestamp <= $2
                    WINDOW w AS (PARTITION BY oc.option_symbol ORDER BY oc.timestamp)
                )
                INSERT INTO flow_by_type (
                    timestamp,
                    symbol,
                    option_type,
                    total_volume,
                    total_premium,
                    avg_iv,
                    net_delta,
                    buy_volume,
                    sell_volume,
                    buy_premium,
                    sell_premium,
                    underlying_price
                )
                SELECT
                    timestamp,
                    $1::varchar,
                    option_type,
                    SUM(volume_delta)::bigint,
                    SUM(volume_delta * COALESCE(last, 0) * 100)::numeric,
                    AVG(implied_volatility)::numeric,
                    SUM(CASE WHEN option_type = 'C' THEN volume_delta ELSE -volume_delta END)::numeric,
                    SUM(ask_vol_delta)::bigint,
                    SUM(bid_vol_delta)::bigint,
                    SUM(ask_vol_delta * COALESCE(last, 0) * 100)::numeric,
                    SUM(bid_vol_delta * COALESCE(last, 0) * 100)::numeric,
                    $3::numeric
                FROM with_prev
                WHERE timestamp = $2
                  AND volume_delta > 0
                GROUP BY timestamp, option_type
                ON CONFLICT (timestamp, symbol, option_type)
                DO UPDATE SET
                    total_volume = EXCLUDED.total_volume,
                    total_premium = EXCLUDED.total_premium,
                    avg_iv = EXCLUDED.avg_iv,
                    net_delta = EXCLUDED.net_delta,
                    buy_volume = EXCLUDED.buy_volume,
                    sell_volume = EXCLUDED.sell_volume,
                    buy_premium = EXCLUDED.buy_premium,
                    sell_premium = EXCLUDED.sell_premium,
                    underlying_price = EXCLUDED.underlying_price,
                    updated_at = NOW()
                """,
                symbol,
                latest_ts,
                underlying_price,
            )

        strike_exists = True if canonical_only else await conn.fetchval(
            """
            SELECT 1 FROM flow_by_strike
            WHERE symbol = $1 AND timestamp = $2
            LIMIT 1
            """,
            symbol,
            latest_ts,
        )
        if not strike_exists:
            await conn.execute(
                """
                WITH with_prev AS (
                    SELECT
                        oc.timestamp,
                        oc.option_type,
                        oc.strike,
                        oc.last,
                        oc.implied_volatility,
                        CASE
                            WHEN LAG(oc.volume) OVER w IS NULL THEN COALESCE(oc.volume, 0)
                            WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(oc.volume, 0) - COALESCE(LAG(oc.volume) OVER w, 0), 0)
                            ELSE COALESCE(oc.volume, 0)
                        END::bigint AS volume_delta,
                        CASE
                            WHEN LAG(oc.ask_volume) OVER w IS NULL THEN COALESCE(oc.ask_volume, 0)
                            WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(oc.ask_volume, 0) - COALESCE(LAG(oc.ask_volume) OVER w, 0), 0)
                            ELSE COALESCE(oc.ask_volume, 0)
                        END::bigint AS ask_vol_delta,
                        CASE
                            WHEN LAG(oc.bid_volume) OVER w IS NULL THEN COALESCE(oc.bid_volume, 0)
                            WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(oc.bid_volume, 0) - COALESCE(LAG(oc.bid_volume) OVER w, 0), 0)
                            ELSE COALESCE(oc.bid_volume, 0)
                        END::bigint AS bid_vol_delta
                    FROM option_chains oc
                    WHERE oc.underlying = $1
                      AND oc.timestamp >= $2::timestamptz - INTERVAL '2 minutes'
                      AND oc.timestamp <= $2
                    WINDOW w AS (PARTITION BY oc.option_symbol ORDER BY oc.timestamp)
                )
                INSERT INTO flow_by_strike (
                    timestamp,
                    symbol,
                    strike,
                    total_volume,
                    total_premium,
                    avg_iv,
                    net_delta,
                    buy_volume,
                    sell_volume,
                    buy_premium,
                    sell_premium,
                    underlying_price
                )
                SELECT
                    timestamp,
                    $1::varchar,
                    strike,
                    SUM(volume_delta)::bigint,
                    SUM(volume_delta * COALESCE(last, 0) * 100)::numeric,
                    AVG(implied_volatility)::numeric,
                    SUM(CASE WHEN option_type = 'C' THEN volume_delta ELSE -volume_delta END)::numeric,
                    SUM(ask_vol_delta)::bigint,
                    SUM(bid_vol_delta)::bigint,
                    SUM(ask_vol_delta * COALESCE(last, 0) * 100)::numeric,
                    SUM(bid_vol_delta * COALESCE(last, 0) * 100)::numeric,
                    $3::numeric
                FROM with_prev
                WHERE timestamp = $2
                  AND volume_delta > 0
                GROUP BY timestamp, strike
                ON CONFLICT (timestamp, symbol, strike)
                DO UPDATE SET
                    total_volume = EXCLUDED.total_volume,
                    total_premium = EXCLUDED.total_premium,
                    avg_iv = EXCLUDED.avg_iv,
                    net_delta = EXCLUDED.net_delta,
                    buy_volume = EXCLUDED.buy_volume,
                    sell_volume = EXCLUDED.sell_volume,
                    buy_premium = EXCLUDED.buy_premium,
                    sell_premium = EXCLUDED.sell_premium,
                    underlying_price = EXCLUDED.underlying_price,
                    updated_at = NOW()
                """,
                symbol,
                latest_ts,
                underlying_price,
            )

        expiration_exists = True if canonical_only else await conn.fetchval(
            """
            SELECT 1 FROM flow_by_expiration
            WHERE symbol = $1 AND timestamp = $2
            LIMIT 1
            """,
            symbol,
            latest_ts,
        )
        if not expiration_exists:
            await conn.execute(
                """
                WITH with_prev AS (
                    SELECT
                        oc.timestamp,
                        oc.expiration,
                        oc.last,
                        CASE
                            WHEN LAG(oc.volume) OVER w IS NULL THEN COALESCE(oc.volume, 0)
                            WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(oc.volume, 0) - COALESCE(LAG(oc.volume) OVER w, 0), 0)
                            ELSE COALESCE(oc.volume, 0)
                        END::bigint AS volume_delta,
                        CASE
                            WHEN LAG(oc.ask_volume) OVER w IS NULL THEN COALESCE(oc.ask_volume, 0)
                            WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(oc.ask_volume, 0) - COALESCE(LAG(oc.ask_volume) OVER w, 0), 0)
                            ELSE COALESCE(oc.ask_volume, 0)
                        END::bigint AS ask_vol_delta,
                        CASE
                            WHEN LAG(oc.bid_volume) OVER w IS NULL THEN COALESCE(oc.bid_volume, 0)
                            WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(oc.bid_volume, 0) - COALESCE(LAG(oc.bid_volume) OVER w, 0), 0)
                            ELSE COALESCE(oc.bid_volume, 0)
                        END::bigint AS bid_vol_delta
                    FROM option_chains oc
                    WHERE oc.underlying = $1
                      AND oc.timestamp >= $2::timestamptz - INTERVAL '2 minutes'
                      AND oc.timestamp <= $2
                    WINDOW w AS (PARTITION BY oc.option_symbol ORDER BY oc.timestamp)
                )
                INSERT INTO flow_by_expiration (
                    timestamp,
                    symbol,
                    expiration,
                    total_volume,
                    total_premium,
                    buy_volume,
                    sell_volume,
                    buy_premium,
                    sell_premium,
                    underlying_price
                )
                SELECT
                    timestamp,
                    $1::varchar,
                    expiration,
                    SUM(volume_delta)::bigint,
                    SUM(volume_delta * COALESCE(last, 0) * 100)::numeric,
                    SUM(ask_vol_delta)::bigint,
                    SUM(bid_vol_delta)::bigint,
                    SUM(ask_vol_delta * COALESCE(last, 0) * 100)::numeric,
                    SUM(bid_vol_delta * COALESCE(last, 0) * 100)::numeric,
                    $3::numeric
                FROM with_prev
                WHERE timestamp = $2
                  AND volume_delta > 0
                GROUP BY timestamp, expiration
                ON CONFLICT (timestamp, symbol, expiration)
                DO UPDATE SET
                    total_volume = EXCLUDED.total_volume,
                    total_premium = EXCLUDED.total_premium,
                    buy_volume = EXCLUDED.buy_volume,
                    sell_volume = EXCLUDED.sell_volume,
                    buy_premium = EXCLUDED.buy_premium,
                    sell_premium = EXCLUDED.sell_premium,
                    underlying_price = EXCLUDED.underlying_price,
                    updated_at = NOW()
                """,
                symbol,
                latest_ts,
                underlying_price,
            )

        smart_exists = await conn.fetchval(
            """
            SELECT 1 FROM flow_smart_money
            WHERE symbol = $1 AND timestamp = $2
            LIMIT 1
            """,
            symbol,
            latest_ts,
        )
        if not smart_exists:
            await conn.execute(
                """
                WITH with_prev AS (
                    SELECT
                        oc.timestamp,
                        oc.option_symbol,
                        oc.option_type,
                        oc.strike,
                        oc.expiration,
                        oc.last,
                        oc.implied_volatility,
                        oc.delta,
                        CASE
                            WHEN LAG(oc.volume) OVER w IS NULL THEN COALESCE(oc.volume, 0)
                            WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                THEN GREATEST(COALESCE(oc.volume, 0) - COALESCE(LAG(oc.volume) OVER w, 0), 0)
                            ELSE COALESCE(oc.volume, 0)
                        END::bigint AS volume_delta
                    FROM option_chains oc
                    WHERE oc.underlying = $1
                      AND oc.timestamp >= $2::timestamptz - INTERVAL '2 minutes'
                      AND oc.timestamp <= $2
                    WINDOW w AS (PARTITION BY oc.option_symbol ORDER BY oc.timestamp)
                )
                INSERT INTO flow_smart_money (
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
                    unusual_activity_score,
                    underlying_price
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
                    ))::numeric,
                    $3::numeric
                FROM with_prev
                WHERE timestamp = $2
                  AND volume_delta > 0
                  AND (
                    volume_delta >= 50
                    OR volume_delta * COALESCE(last, 0) * 100 >= 50000
                    OR (implied_volatility > 0.4 AND volume_delta >= 20)
                    OR (ABS(delta) < 0.15 AND volume_delta >= 20)
                  )
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
                    underlying_price = EXCLUDED.underlying_price,
                    updated_at = NOW()
                """,
                symbol,
                latest_ts,
                underlying_price,
            )

    async def _refresh_max_pain_snapshot(self, conn: asyncpg.Connection, symbol: str, strike_limit: int) -> None:
        """Refresh daily max pain OI snapshot for the symbol if latest chain timestamp changed."""
        strike_limit = max(10, min(strike_limit, 1000))
        query = """
            WITH latest AS (
                SELECT timestamp AS max_ts
                FROM option_chains
                WHERE underlying = $1
                ORDER BY timestamp DESC
                LIMIT 1
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
        symbol = symbol.upper()
        cache_key = f"latest_gex_summary:{symbol}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        query = """
            WITH latest_summary AS (
                SELECT
                    gs.timestamp,
                    gs.underlying,
                    gs.gamma_flip_point,
                    gs.max_pain,
                    gs.total_call_oi,
                    gs.total_put_oi,
                    gs.put_call_ratio,
                    gs.total_net_gex
                FROM gex_summary gs
                WHERE gs.underlying = $1
                ORDER BY gs.timestamp DESC
                LIMIT 1
            ),
            latest_quote AS (
                SELECT COALESCE(uq.close, 0)::numeric AS spot_price
                FROM underlying_quotes uq
                WHERE uq.symbol = $1
                ORDER BY uq.timestamp DESC
                LIMIT 1
            ),
            strike_exposures AS (
                SELECT
                    gbs.strike,
                    -- call_gamma / put_gamma are already OI-weighted in analytics
                    -- (gamma * open_interest). Do NOT multiply by OI again here.
                    (gbs.call_gamma * 100 * lq.spot_price)::numeric AS call_exposure,
                    (-1 * gbs.put_gamma * 100 * lq.spot_price)::numeric AS put_exposure
                FROM gex_by_strike gbs
                JOIN latest_summary ls
                  ON gbs.underlying = ls.underlying
                 AND gbs.timestamp = ls.timestamp
                JOIN latest_quote lq ON TRUE
            ),
            strike_totals AS (
                SELECT
                    COALESCE(SUM(se.call_exposure), 0)::numeric AS total_call_gex,
                    COALESCE(SUM(se.put_exposure), 0)::numeric AS total_put_gex
                FROM strike_exposures se
            ),
            call_wall AS (
                SELECT se.strike::numeric AS call_wall
                FROM strike_exposures se
                ORDER BY ABS(se.call_exposure) DESC, se.strike
                LIMIT 1
            ),
            put_wall AS (
                SELECT se.strike::numeric AS put_wall
                FROM strike_exposures se
                ORDER BY ABS(se.put_exposure) DESC, se.strike
                LIMIT 1
            )
            SELECT
                ls.timestamp,
                ls.underlying AS symbol,
                lq.spot_price,
                st.total_call_gex,
                st.total_put_gex,
                ls.total_net_gex AS net_gex,
                ls.gamma_flip_point AS gamma_flip,
                ls.max_pain,
                cw.call_wall,
                pw.put_wall,
                ls.total_call_oi,
                ls.total_put_oi,
                ls.put_call_ratio
            FROM latest_summary ls
            JOIN latest_quote lq ON TRUE
            JOIN strike_totals st ON TRUE
            LEFT JOIN call_wall cw ON TRUE
            LEFT JOIN put_wall pw ON TRUE
        """

        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol)
                payload = dict(row) if row else None
                self._cache_set(
                    cache_key,
                    payload,
                    self._latest_gex_summary_cache_ttl_seconds,
                )
                return payload
        except Exception as e:
            logger.error(f"Error fetching GEX summary: {e}", exc_info=True)
            raise

    async def get_gex_by_strike(
        self,
        symbol: str = 'SPY',
        limit: int = 50,
        sort_by: str = 'distance'  # 'distance' or 'impact'
    ) -> List[Dict[str, Any]]:
        """
        Get latest GEX breakdown by strike

        Args:
            symbol: Underlying symbol
            limit: Number of strikes to return
            sort_by: 'distance' (closest to spot) or 'impact' (highest absolute net GEX)
        """
        symbol = symbol.upper()
        cache_key = f"gex_by_strike:{symbol}:{limit}:{sort_by}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        # Choose sort order
        if sort_by == 'impact':
            order_clause = "ORDER BY ABS(g.net_gex) DESC"
        else:
            order_clause = "ORDER BY ABS(g.strike - spot.close) ASC"

        query = f"""
            WITH spot AS (
                SELECT close
                FROM underlying_quotes
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            )
            SELECT
                g.timestamp,
                g.underlying as symbol,
                g.strike,
                g.expiration,
                g.call_oi,
                g.put_oi,
                g.call_volume,
                g.put_volume,
                (g.call_gamma * 100 * COALESCE(spot.close, 0)) as call_gex,
                (-1 * g.put_gamma * 100 * COALESCE(spot.close, 0)) as put_gex,
                g.net_gex,
                g.vanna_exposure,
                g.charm_exposure,
                spot.close as spot_price,
                g.strike - spot.close as distance_from_spot
            FROM gex_by_strike g
            CROSS JOIN spot
            WHERE g.underlying = $1
                AND g.timestamp = (
                    SELECT timestamp
                    FROM gex_by_strike
                    WHERE underlying = $1
                    ORDER BY timestamp DESC
                    LIMIT 1
                )
            {order_clause}
            LIMIT $2
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, limit)
                result = [dict(row) for row in rows]
                self._cache_set(cache_key, result, self._analytics_cache_ttl_seconds)
                return result
        except Exception as e:
            logger.error(f"Error fetching GEX by strike: {e}", exc_info=True)
            raise

    async def get_gex_walls(
        self,
        symbol: str = "SPY",
    ) -> Optional[Dict[str, Any]]:
        """
        Get latest strongest call and put wall levels relative to spot.

        Wall levels are determined by the largest absolute directional gamma
        exposure aggregated by strike across expirations.
        """
        symbol = symbol.upper()
        cache_key = f"gex_walls:{symbol}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        query = """
            WITH latest AS (
                SELECT timestamp AS ts
                FROM gex_by_strike
                WHERE underlying = $1
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            spot AS (
                SELECT close::numeric AS spot_price
                FROM underlying_quotes
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            strike_agg AS (
                SELECT
                    g.timestamp,
                    g.strike,
                    SUM(g.call_gamma * 100 * s.spot_price)::numeric AS call_exposure,
                    SUM(-1 * g.put_gamma * 100 * s.spot_price)::numeric AS put_exposure,
                    s.spot_price
                FROM gex_by_strike g
                CROSS JOIN spot s
                WHERE g.underlying = $1
                  AND g.timestamp = (SELECT ts FROM latest)
                GROUP BY g.timestamp, g.strike, s.spot_price
            ),
            call_wall AS (
                SELECT *
                FROM strike_agg
                ORDER BY call_exposure DESC, strike
                LIMIT 1
            ),
            put_wall AS (
                SELECT *
                FROM strike_agg
                ORDER BY put_exposure ASC, strike
                LIMIT 1
            )
            SELECT
                c.timestamp,
                $1::varchar AS symbol,
                c.spot_price,
                c.strike AS call_wall_strike,
                c.call_exposure AS call_wall_exposure,
                (c.strike - c.spot_price)::numeric AS call_wall_distance,
                CASE
                    WHEN c.spot_price = 0 THEN 0::numeric
                    ELSE ((c.strike - c.spot_price) / c.spot_price * 100)::numeric
                END AS call_wall_pct_from_spot,
                p.strike AS put_wall_strike,
                p.put_exposure AS put_wall_exposure,
                (p.strike - p.spot_price)::numeric AS put_wall_distance,
                CASE
                    WHEN p.spot_price = 0 THEN 0::numeric
                    ELSE ((p.strike - p.spot_price) / p.spot_price * 100)::numeric
                END AS put_wall_pct_from_spot
            FROM call_wall c
            CROSS JOIN put_wall p
        """

        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol)
                if not row:
                    return None

                r = dict(row)
                result = {
                    "timestamp": r["timestamp"],
                    "symbol": r["symbol"],
                    "spot_price": r["spot_price"],
                    "call_wall": {
                        "strike": r["call_wall_strike"],
                        "exposure": r["call_wall_exposure"],
                        "distance_from_spot": r["call_wall_distance"],
                        "pct_from_spot": r["call_wall_pct_from_spot"],
                    },
                    "put_wall": {
                        "strike": r["put_wall_strike"],
                        "exposure": r["put_wall_exposure"],
                        "distance_from_spot": r["put_wall_distance"],
                        "pct_from_spot": r["put_wall_pct_from_spot"],
                    },
                }
                self._cache_set(cache_key, result, self._analytics_cache_ttl_seconds)
                return result
        except Exception as e:
            logger.error(f"Error fetching GEX walls: {e}", exc_info=True)
            raise

    async def get_historical_gex(
        self,
        symbol: str = 'SPY',
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        window_units: int = 90,
        timeframe: str = '1min'
    ) -> List[Dict[str, Any]]:
        """Get historical GEX summary data aggregated by timeframe."""
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        query = f"""
            WITH latest AS (
                SELECT timestamp AS max_ts
                FROM gex_summary
                WHERE underlying = $1
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            bounds AS (
                SELECT
                    COALESCE($2::timestamptz, max_ts - ({step_interval} * ($4 - 1))) AS start_ts,
                    COALESCE($3::timestamptz, max_ts) AS end_ts
                FROM latest
            ),
            spot AS (
                SELECT close::numeric AS spot_price
                FROM underlying_quotes
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            bucketed AS (
                SELECT
                    gs.timestamp,
                    gs.underlying as symbol,
                    gs.total_net_gex as net_gex,
                    gs.gamma_flip_point as gamma_flip,
                    gs.max_pain,
                    gs.total_call_oi,
                    gs.total_put_oi,
                    gs.put_call_ratio,
                    {bucket} as bucket_ts,
                    ROW_NUMBER() OVER (PARTITION BY {bucket} ORDER BY gs.timestamp DESC) as rn
                FROM gex_summary gs
                WHERE gs.underlying = $1
                    AND gs.timestamp BETWEEN (SELECT start_ts FROM bounds) AND (SELECT end_ts FROM bounds)
            ),
            base AS (
                SELECT *
                FROM bucketed
                WHERE rn = 1
            ),
            strike_agg AS (
                SELECT
                    gbs.timestamp,
                    COALESCE(SUM(gbs.call_gamma * 100 * s.spot_price), 0)::numeric AS total_call_gex,
                    COALESCE(SUM(-1 * gbs.put_gamma * 100 * s.spot_price), 0)::numeric AS total_put_gex
                FROM gex_by_strike gbs
                CROSS JOIN spot s
                WHERE gbs.underlying = $1
                  AND gbs.timestamp IN (SELECT timestamp FROM base)
                GROUP BY gbs.timestamp
            ),
            walls AS (
                SELECT DISTINCT ON (gbs.timestamp)
                    gbs.timestamp,
                    FIRST_VALUE(gbs.strike) OVER (
                        PARTITION BY gbs.timestamp
                        ORDER BY ABS(gbs.call_gamma) DESC, gbs.strike
                    )::numeric AS call_wall,
                    FIRST_VALUE(gbs.strike) OVER (
                        PARTITION BY gbs.timestamp
                        ORDER BY ABS(gbs.put_gamma) DESC, gbs.strike
                    )::numeric AS put_wall
                FROM gex_by_strike gbs
                WHERE gbs.underlying = $1
                  AND gbs.timestamp IN (SELECT timestamp FROM base)
            )
            SELECT
                b.bucket_ts as timestamp,
                b.symbol,
                s.spot_price,
                COALESCE(sa.total_call_gex, 0)::numeric AS total_call_gex,
                COALESCE(sa.total_put_gex, 0)::numeric AS total_put_gex,
                b.net_gex,
                b.gamma_flip,
                b.max_pain,
                w.call_wall,
                w.put_wall,
                b.total_call_oi,
                b.total_put_oi,
                b.put_call_ratio
            FROM base b
            CROSS JOIN spot s
            LEFT JOIN strike_agg sa ON sa.timestamp = b.timestamp
            LEFT JOIN walls w ON w.timestamp = b.timestamp
            ORDER BY timestamp DESC
            LIMIT $4
        """

        try:
            async with self._acquire_connection() as conn:
                window_units = max(1, min(window_units, 90))
                rows = await conn.fetch(query, symbol, start_date, end_date, window_units)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching historical GEX: {e}", exc_info=True)
            raise

    # ========================================================================
    # Options Flow Queries (from views)
    # ========================================================================

    async def get_flow_by_type(
        self,
        symbol: str = 'SPY',
        session: str = 'current'
    ) -> List[Dict[str, Any]]:
        """Get option flow by type from canonical flow_contract_facts."""
        symbol = symbol.upper()
        cache_key = f"flow_by_type:{symbol}:{session}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        session_start, session_end = _get_session_bounds(session)
        query = """
            WITH minutes AS (
                SELECT generate_series(
                    date_trunc('minute', $2::timestamptz),
                    date_trunc('minute', $3::timestamptz),
                    interval '1 minute'
                ) AS timestamp
            ),
            aggregated AS (
                SELECT
                    date_trunc('minute', timestamp) AS timestamp,
                    symbol,
                    SUM(CASE WHEN option_type = 'C' THEN volume_delta ELSE 0 END) AS call_volume,
                    SUM(CASE WHEN option_type = 'C' THEN premium_delta ELSE 0 END) AS call_premium,
                    SUM(CASE WHEN option_type = 'P' THEN volume_delta ELSE 0 END) AS put_volume,
                    SUM(CASE WHEN option_type = 'P' THEN premium_delta ELSE 0 END) AS put_premium,
                    SUM(CASE WHEN option_type = 'C' THEN buy_volume ELSE 0 END) AS call_buy_volume,
                    SUM(CASE WHEN option_type = 'C' THEN sell_volume ELSE 0 END) AS call_sell_volume,
                    SUM(CASE WHEN option_type = 'P' THEN buy_volume ELSE 0 END) AS put_buy_volume,
                    SUM(CASE WHEN option_type = 'P' THEN sell_volume ELSE 0 END) AS put_sell_volume,
                    SUM(CASE WHEN option_type = 'C' THEN buy_premium ELSE 0 END) AS call_buy_premium,
                    SUM(CASE WHEN option_type = 'C' THEN sell_premium ELSE 0 END) AS call_sell_premium,
                    SUM(CASE WHEN option_type = 'P' THEN buy_premium ELSE 0 END) AS put_buy_premium,
                    SUM(CASE WHEN option_type = 'P' THEN sell_premium ELSE 0 END) AS put_sell_premium,
                    MAX(underlying_price) AS underlying_price
                FROM flow_contract_facts
                WHERE symbol = $1
                  AND timestamp >= $2
                  AND timestamp <= $3
                GROUP BY date_trunc('minute', timestamp), symbol
            ),
            dense AS (
                SELECT
                    m.timestamp,
                    $1::text AS symbol,
                    COALESCE(a.call_volume, 0) AS call_volume,
                    COALESCE(a.call_premium, 0) AS call_premium,
                    COALESCE(a.put_volume, 0) AS put_volume,
                    COALESCE(a.put_premium, 0) AS put_premium,
                    COALESCE(a.call_buy_volume, 0) AS call_buy_volume,
                    COALESCE(a.call_sell_volume, 0) AS call_sell_volume,
                    COALESCE(a.put_buy_volume, 0) AS put_buy_volume,
                    COALESCE(a.put_sell_volume, 0) AS put_sell_volume,
                    COALESCE(a.call_buy_premium, 0) AS call_buy_premium,
                    COALESCE(a.call_sell_premium, 0) AS call_sell_premium,
                    COALESCE(a.put_buy_premium, 0) AS put_buy_premium,
                    COALESCE(a.put_sell_premium, 0) AS put_sell_premium,
                    COALESCE(
                        a.underlying_price,
                        (
                            SELECT a_prev.underlying_price
                            FROM aggregated a_prev
                            WHERE a_prev.timestamp <= m.timestamp
                            ORDER BY a_prev.timestamp DESC
                            LIMIT 1
                        )
                    ) AS underlying_price
                FROM minutes m
                LEFT JOIN aggregated a
                    ON a.timestamp = m.timestamp
            ),
            with_net AS (
                SELECT
                    timestamp,
                    symbol,
                    COALESCE(call_volume, 0)::bigint AS call_volume,
                    COALESCE(call_premium, 0)::numeric AS call_premium,
                    COALESCE(put_volume, 0)::bigint AS put_volume,
                    COALESCE(put_premium, 0)::numeric AS put_premium,
                    (COALESCE(call_volume, 0) - COALESCE(put_volume, 0))::bigint AS net_volume,
                    (
                        (COALESCE(call_buy_volume, 0) - COALESCE(call_sell_volume, 0))
                        - (COALESCE(put_buy_volume, 0) - COALESCE(put_sell_volume, 0))
                    )::bigint AS net_directional_volume,
                    -- Net Call Premium (NCP): buy pressure minus sell pressure on calls
                    (COALESCE(call_buy_premium, 0) - COALESCE(call_sell_premium, 0))::numeric AS ncp,
                    -- Net Put Premium (NPP): buy pressure minus sell pressure on puts (negated)
                    (-(COALESCE(put_buy_premium, 0) - COALESCE(put_sell_premium, 0)))::numeric AS npp,
                    (
                        (COALESCE(call_buy_premium, 0) - COALESCE(call_sell_premium, 0))
                        - (COALESCE(put_buy_premium, 0) - COALESCE(put_sell_premium, 0))
                    )::numeric AS net_premium,
                    underlying_price
                FROM dense
            )
            SELECT
                timestamp,
                symbol,
                call_volume,
                call_premium,
                ncp AS net_call_premium,
                put_volume,
                put_premium,
                npp AS net_put_premium,
                net_volume,
                net_directional_volume,
                net_premium,
                -- Cumulative NCP: running sum of net call buying (positive = net call buying)
                SUM(ncp) OVER (ORDER BY timestamp)::numeric AS cumulative_call_premium,
                -- Cumulative NPP: running sum of net put buying (negative = net put buying)
                SUM(npp) OVER (ORDER BY timestamp)::numeric AS cumulative_put_premium,
                SUM(call_volume + put_volume) OVER (ORDER BY timestamp)::bigint AS cumulative_volume,
                SUM(call_volume) OVER (ORDER BY timestamp)::bigint AS cumulative_call_volume,
                SUM(put_volume) OVER (ORDER BY timestamp)::bigint AS cumulative_put_volume,
                SUM(net_volume) OVER (ORDER BY timestamp)::bigint AS cumulative_net_volume,
                SUM(net_directional_volume) OVER (ORDER BY timestamp)::bigint AS cumulative_net_directional_volume,
                SUM(ncp + npp) OVER (ORDER BY timestamp)::numeric AS cumulative_net_premium,
                ROUND(
                    SUM(put_volume) OVER (ORDER BY timestamp)::numeric
                    / NULLIF(SUM(call_volume) OVER (ORDER BY timestamp), 0),
                    4
                ) AS running_put_call_ratio,
                CASE
                    WHEN net_volume > 500 THEN '🟢 Strong Calls'
                    WHEN net_volume > 0 THEN '✅ Calls'
                    WHEN net_volume < -500 THEN '🔴 Strong Puts'
                    WHEN net_volume < 0 THEN '❌ Puts'
                    ELSE '⚪ Neutral'
                END AS flow_bias,
                underlying_price
            FROM with_net
            ORDER BY timestamp DESC
        """

        try:
            async with self._acquire_connection() as conn:
                await self._refresh_flow_cache(conn, symbol)
                rows = await asyncio.wait_for(
                    conn.fetch(query, symbol, session_start, session_end),
                    timeout=15.0,
                )
                result = [dict(row) for row in rows]
                self._cache_set(cache_key, result, self._flow_endpoint_cache_ttl_seconds)
                return result
        except asyncio.TimeoutError:
            logger.warning(f"Flow by type query timed out for {symbol}, returning empty")
            return []
        except Exception as e:
            logger.warning(f"Flow by type query failed for {symbol} (returning empty): {e!r}")
            return []

    async def get_flow_by_strike(
        self,
        symbol: str = 'SPY',
        session: str = 'current',
    ) -> List[Dict[str, Any]]:
        """Get option flow by strike from canonical flow_contract_facts."""
        symbol = symbol.upper()
        cache_key = f"flow_by_strike:{symbol}:{session}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        session_start, session_end = _get_session_bounds(session)
        query = """
            WITH buckets AS (
                SELECT generate_series(
                    to_timestamp(floor(extract(epoch from $2::timestamptz) / 300) * 300),
                    to_timestamp(floor(extract(epoch from $3::timestamptz) / 300) * 300),
                    interval '5 minute'
                ) AS timestamp
            ),
            strikes AS (
                SELECT DISTINCT strike
                FROM flow_contract_facts
                WHERE symbol = $1
                  AND timestamp >= $2
                  AND timestamp <= $3
            ),
            bucketed AS (
                SELECT
                    to_timestamp(floor(extract(epoch from timestamp) / 300) * 300) AS bucket,
                    symbol,
                    strike,
                    volume_delta,
                    premium_delta,
                    signed_volume,
                    signed_premium,
                    buy_volume,
                    sell_volume,
                    buy_premium,
                    sell_premium,
                    option_type,
                    underlying_price
                FROM flow_contract_facts
                WHERE symbol = $1
                  AND timestamp >= $2
                  AND timestamp <= $3
            ),
            agg AS (
                SELECT
                    bucket AS timestamp,
                    symbol,
                    strike,
                    SUM(volume_delta)::bigint AS volume,
                    SUM(premium_delta)::numeric AS premium,
                    SUM(CASE WHEN option_type = 'C' THEN (buy_premium - sell_premium) ELSE 0 END)::numeric AS ncp,
                    SUM(CASE WHEN option_type = 'P' THEN -(buy_premium - sell_premium) ELSE 0 END)::numeric AS npp,
                    SUM(signed_volume)::bigint AS net_volume,
                    SUM(
                        CASE
                            WHEN option_type = 'C' THEN (buy_volume - sell_volume)
                            WHEN option_type = 'P' THEN -(buy_volume - sell_volume)
                            ELSE 0
                        END
                    )::bigint AS net_directional_volume,
                    SUM(signed_premium)::numeric AS net_premium,
                    MAX(underlying_price) AS underlying_price
                FROM bucketed
                GROUP BY bucket, symbol, strike
            ),
            underlying_by_bucket AS (
                SELECT
                    bucket AS timestamp,
                    MAX(underlying_price) AS underlying_price
                FROM bucketed
                GROUP BY bucket
            ),
            underlying_dense AS (
                SELECT
                    b.timestamp,
                    COALESCE(
                        ub.underlying_price,
                        (
                            SELECT ub_prev.underlying_price
                            FROM underlying_by_bucket ub_prev
                            WHERE ub_prev.timestamp <= b.timestamp
                            ORDER BY ub_prev.timestamp DESC
                            LIMIT 1
                        )
                    ) AS underlying_price
                FROM buckets b
                LEFT JOIN underlying_by_bucket ub
                  ON ub.timestamp = b.timestamp
            ),
            dense AS (
                SELECT
                    b.timestamp,
                    $1::text AS symbol,
                    s.strike,
                    COALESCE(a.volume, 0)::bigint AS volume,
                    COALESCE(a.premium, 0)::numeric AS premium,
                    COALESCE(a.ncp, 0)::numeric AS ncp,
                    COALESCE(a.npp, 0)::numeric AS npp,
                    COALESCE(a.net_volume, 0)::bigint AS net_volume,
                    COALESCE(a.net_directional_volume, 0)::bigint AS net_directional_volume,
                    COALESCE(a.net_premium, 0)::numeric AS net_premium,
                    ud.underlying_price AS underlying_price
                FROM buckets b
                CROSS JOIN strikes s
                LEFT JOIN agg a
                  ON a.timestamp = b.timestamp
                 AND a.strike = s.strike
                LEFT JOIN underlying_dense ud
                  ON ud.timestamp = b.timestamp
            )
            SELECT
                timestamp,
                symbol,
                strike,
                volume,
                premium,
                ncp AS net_call_premium,
                npp AS net_put_premium,
                net_volume,
                net_directional_volume,
                (ncp + npp)::numeric AS net_premium,
                SUM(volume) OVER (PARTITION BY strike ORDER BY timestamp)::bigint AS cumulative_volume,
                SUM(net_volume) OVER (PARTITION BY strike ORDER BY timestamp)::bigint AS cumulative_net_volume,
                SUM(net_directional_volume) OVER (PARTITION BY strike ORDER BY timestamp)::bigint AS cumulative_net_directional_volume,
                SUM(premium) OVER (PARTITION BY strike ORDER BY timestamp)::numeric AS cumulative_premium,
                SUM(ncp) OVER (PARTITION BY strike ORDER BY timestamp)::numeric AS cumulative_call_premium,
                SUM(npp) OVER (PARTITION BY strike ORDER BY timestamp)::numeric AS cumulative_put_premium,
                SUM(ncp + npp) OVER (PARTITION BY strike ORDER BY timestamp)::numeric AS cumulative_net_premium,
                CASE
                    WHEN net_volume > 100 THEN '🟢 Strong Calls'
                    WHEN net_volume > 0 THEN '✅ Calls'
                    WHEN net_volume < -100 THEN '🔴 Strong Puts'
                    WHEN net_volume < 0 THEN '❌ Puts'
                    ELSE '⚪ Neutral'
                END AS flow_bias,
                underlying_price
            FROM dense
            ORDER BY timestamp DESC, strike
        """

        try:
            async with self._acquire_connection() as conn:
                await self._refresh_flow_cache(conn, symbol)
                rows = await asyncio.wait_for(
                    conn.fetch(query, symbol, session_start, session_end),
                    timeout=15.0,
                )
                result = [dict(row) for row in rows]
                self._cache_set(cache_key, result, self._flow_endpoint_cache_ttl_seconds)
                return result
        except asyncio.TimeoutError:
            logger.warning(f"Flow by strike query timed out for {symbol}, returning empty")
            return []
        except Exception as e:
            logger.warning(f"Flow by strike query failed for {symbol} (returning empty): {e!r}")
            return []

    async def get_flow_by_expiration(
        self,
        symbol: str = 'SPY',
        session: str = 'current',
    ) -> List[Dict[str, Any]]:
        """Get option flow by expiration from canonical flow_contract_facts."""
        symbol = symbol.upper()
        cache_key = f"flow_by_expiration:{symbol}:{session}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        session_start, session_end = _get_session_bounds(session)
        query = """
            WITH buckets AS (
                SELECT generate_series(
                    to_timestamp(floor(extract(epoch from $2::timestamptz) / 300) * 300),
                    to_timestamp(floor(extract(epoch from $3::timestamptz) / 300) * 300),
                    interval '5 minute'
                ) AS timestamp
            ),
            expirations AS (
                SELECT DISTINCT expiration
                FROM flow_contract_facts
                WHERE symbol = $1
                  AND timestamp >= $2
                  AND timestamp <= $3
            ),
            bucketed AS (
                SELECT
                    to_timestamp(floor(extract(epoch from timestamp) / 300) * 300) AS bucket,
                    symbol,
                    expiration,
                    volume_delta,
                    premium_delta,
                    signed_volume,
                    signed_premium,
                    buy_volume,
                    sell_volume,
                    buy_premium,
                    sell_premium,
                    option_type,
                    underlying_price
                FROM flow_contract_facts
                WHERE symbol = $1
                  AND timestamp >= $2
                  AND timestamp <= $3
            ),
            agg AS (
                SELECT
                    bucket AS timestamp,
                    symbol,
                    expiration,
                    SUM(volume_delta)::bigint AS volume,
                    SUM(premium_delta)::numeric AS premium,
                    SUM(CASE WHEN option_type = 'C' THEN (buy_premium - sell_premium) ELSE 0 END)::numeric AS ncp,
                    SUM(CASE WHEN option_type = 'P' THEN -(buy_premium - sell_premium) ELSE 0 END)::numeric AS npp,
                    SUM(signed_volume)::bigint AS net_volume,
                    SUM(
                        CASE
                            WHEN option_type = 'C' THEN (buy_volume - sell_volume)
                            WHEN option_type = 'P' THEN -(buy_volume - sell_volume)
                            ELSE 0
                        END
                    )::bigint AS net_directional_volume,
                    SUM(signed_premium)::numeric AS net_premium,
                    MAX(underlying_price) AS underlying_price
                FROM bucketed
                GROUP BY bucket, symbol, expiration
            ),
            underlying_by_bucket AS (
                SELECT
                    bucket AS timestamp,
                    MAX(underlying_price) AS underlying_price
                FROM bucketed
                GROUP BY bucket
            ),
            underlying_dense AS (
                SELECT
                    b.timestamp,
                    COALESCE(
                        ub.underlying_price,
                        (
                            SELECT ub_prev.underlying_price
                            FROM underlying_by_bucket ub_prev
                            WHERE ub_prev.timestamp <= b.timestamp
                            ORDER BY ub_prev.timestamp DESC
                            LIMIT 1
                        )
                    ) AS underlying_price
                FROM buckets b
                LEFT JOIN underlying_by_bucket ub
                  ON ub.timestamp = b.timestamp
            ),
            dense AS (
                SELECT
                    b.timestamp,
                    $1::text AS symbol,
                    e.expiration,
                    COALESCE(a.volume, 0)::bigint AS volume,
                    COALESCE(a.premium, 0)::numeric AS premium,
                    COALESCE(a.ncp, 0)::numeric AS net_call_premium,
                    COALESCE(a.npp, 0)::numeric AS net_put_premium,
                    COALESCE(a.net_volume, 0)::bigint AS net_volume,
                    COALESCE(a.net_directional_volume, 0)::bigint AS net_directional_volume,
                    COALESCE(a.net_premium, 0)::numeric AS net_premium,
                    ud.underlying_price AS underlying_price
                FROM buckets b
                CROSS JOIN expirations e
                LEFT JOIN agg a
                  ON a.timestamp = b.timestamp
                 AND a.expiration = e.expiration
                LEFT JOIN underlying_dense ud
                  ON ud.timestamp = b.timestamp
            )
            SELECT
                timestamp,
                symbol,
                expiration,
                (expiration - CURRENT_DATE)::int AS dte,
                volume,
                premium,
                net_call_premium,
                net_put_premium,
                net_volume,
                net_directional_volume,
                (net_call_premium + net_put_premium)::numeric AS net_premium,
                SUM(volume) OVER (PARTITION BY expiration ORDER BY timestamp)::bigint AS cumulative_volume,
                SUM(net_volume) OVER (PARTITION BY expiration ORDER BY timestamp)::bigint AS cumulative_net_volume,
                SUM(net_directional_volume) OVER (PARTITION BY expiration ORDER BY timestamp)::bigint AS cumulative_net_directional_volume,
                SUM(premium) OVER (PARTITION BY expiration ORDER BY timestamp)::numeric AS cumulative_premium,
                SUM(net_call_premium) OVER (PARTITION BY expiration ORDER BY timestamp)::numeric AS cumulative_call_premium,
                SUM(net_put_premium) OVER (PARTITION BY expiration ORDER BY timestamp)::numeric AS cumulative_put_premium,
                SUM(net_call_premium + net_put_premium) OVER (PARTITION BY expiration ORDER BY timestamp)::numeric AS cumulative_net_premium,
                CASE
                    WHEN net_volume > 100 THEN '🟢 Strong Calls'
                    WHEN net_volume > 0 THEN '✅ Calls'
                    WHEN net_volume < -100 THEN '🔴 Strong Puts'
                    WHEN net_volume < 0 THEN '❌ Puts'
                    ELSE '⚪ Neutral'
                END AS flow_bias,
                underlying_price
            FROM dense
            ORDER BY timestamp DESC, expiration
        """

        try:
            async with self._acquire_connection() as conn:
                await self._refresh_flow_cache(conn, symbol)
                rows = await asyncio.wait_for(
                    conn.fetch(query, symbol, session_start, session_end),
                    timeout=15.0,
                )
                result = [dict(row) for row in rows]
                self._cache_set(cache_key, result, self._flow_endpoint_cache_ttl_seconds)
                return result
        except asyncio.TimeoutError:
            logger.warning(f"Flow by expiration query timed out for {symbol}, returning empty")
            return []
        except Exception as e:
            logger.warning(f"Flow by expiration query failed for {symbol} (returning empty): {e!r}")
            return []

    async def get_smart_money_flow(
        self,
        symbol: str = 'SPY',
        session: str = 'current',
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get smart-money events from canonical flow_contract_facts."""
        session_start, session_end = _get_session_bounds(session)
        limit = max(1, min(int(limit), 50))
        query = """
            WITH
            scored AS (
                SELECT
                    timestamp,
                    symbol,
                    option_symbol AS contract,
                    strike,
                    expiration,
                    (expiration - CURRENT_DATE)::int AS dte,
                    option_type,
                    volume_delta AS flow,
                    premium_delta::numeric AS notional,
                    CASE
                        WHEN buy_premium > sell_premium THEN 'BUY'
                        WHEN sell_premium > buy_premium THEN 'SELL'
                        ELSE 'NEUTRAL'
                    END AS trade_side,
                    delta,
                    LEAST(10, GREATEST(0,
                        CASE WHEN volume_delta >= 500 THEN 4 WHEN volume_delta >= 200 THEN 3 WHEN volume_delta >= 100 THEN 2 WHEN volume_delta >= 50 THEN 1 ELSE 0 END +
                        CASE WHEN premium_delta >= 500000 THEN 4 WHEN premium_delta >= 250000 THEN 3 WHEN premium_delta >= 100000 THEN 2 WHEN premium_delta >= 50000 THEN 1 ELSE 0 END +
                        CASE WHEN implied_volatility > 1.0 THEN 2 WHEN implied_volatility > 0.6 THEN 1 ELSE 0 END
                    ))::numeric AS score,
                    CASE
                        WHEN premium_delta >= 500000 THEN '💰 $500K+'
                        WHEN premium_delta >= 250000 THEN '💵 $250K+'
                        WHEN premium_delta >= 100000 THEN '💸 $100K+'
                        WHEN premium_delta >= 50000 THEN '💳 $50K+'
                        ELSE '💴 <$50K'
                    END AS notional_class,
                    CASE
                        WHEN volume_delta >= 500 THEN '🔥 Massive Block'
                        WHEN volume_delta >= 200 THEN '📦 Large Block'
                        WHEN volume_delta >= 100 THEN '📊 Medium Block'
                        ELSE '💼 Standard'
                    END AS size_class,
                    underlying_price
                FROM flow_contract_facts
                WHERE symbol = $1
                  AND timestamp >= $2
                  AND timestamp <= $3
                  AND volume_delta > 0
                  AND (
                    volume_delta >= 50
                    OR premium_delta >= 50000
                    OR (implied_volatility > 0.4 AND volume_delta >= 20)
                    OR (ABS(delta) < 0.15 AND volume_delta >= 20)
                  )
            )
            SELECT
                timestamp,
                symbol,
                contract,
                strike,
                expiration,
                dte,
                option_type,
                flow,
                notional,
                trade_side,
                delta,
                score,
                notional_class,
                size_class,
                underlying_price
            FROM scored
            ORDER BY ABS(notional) DESC, score DESC, timestamp DESC
            LIMIT $4
        """

        try:
            async with self._acquire_connection() as conn:
                await self._refresh_flow_cache(conn, symbol)
                rows = await asyncio.wait_for(
                    conn.fetch(query, symbol, session_start, session_end, limit),
                    timeout=15.0,
                )
                return [dict(row) for row in rows]
        except asyncio.TimeoutError:
            logger.warning(f"Smart money flow query timed out for {symbol}, returning empty")
            return []
        except Exception as e:
            logger.warning(f"Smart money flow query failed for {symbol} (returning empty): {e!r}")
            return []

    async def get_flow_buying_pressure(
        self,
        symbol: str = 'SPY',
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get underlying buying/selling pressure matching Makefile flow-buying-pressure."""
        query = """
            WITH quote_deltas AS (
                SELECT
                    timestamp,
                    symbol,
                    close,
                    up_volume,
                    down_volume,
                    COALESCE(
                        GREATEST(
                            up_volume - LAG(up_volume) OVER (
                                PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
                                ORDER BY timestamp
                            ),
                            0
                        ),
                        0
                    ) AS up_volume_delta,
                    COALESCE(
                        GREATEST(
                            down_volume - LAG(down_volume) OVER (
                                PARTITION BY symbol, DATE(timestamp AT TIME ZONE 'America/New_York')
                                ORDER BY timestamp
                            ),
                            0
                        ),
                        0
                    ) AS down_volume_delta
                FROM underlying_quotes
                WHERE symbol = $1
                  AND timestamp >= NOW() - INTERVAL '2 days'
            )
            SELECT
                timestamp,
                symbol,
                ROUND(close, 2) AS price,
                (up_volume_delta + down_volume_delta)::bigint AS volume,
                ROUND(
                    CASE
                        WHEN (up_volume + down_volume) > 0
                        THEN up_volume::numeric / (up_volume + down_volume) * 100
                        ELSE 50
                    END,
                    2
                ) AS buy_pct,
                ROUND(
                    CASE
                        WHEN (up_volume_delta + down_volume_delta) > 0
                        THEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) * 100
                        ELSE 50
                    END,
                    2
                ) AS period_buy_pct,
                ROUND(close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp), 2) AS price_chg,
                CASE
                    WHEN (up_volume_delta + down_volume_delta) = 0 THEN '⚪ Neutral'
                    WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) > 0.7 THEN '🟢 Strong Buying'
                    WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) > 0.55 THEN '✅ Buying'
                    WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) >= 0.45 THEN '⚪ Neutral'
                    WHEN up_volume_delta::numeric / (up_volume_delta + down_volume_delta) >= 0.3 THEN '❌ Selling'
                    ELSE '🔴 Strong Selling'
                END AS momentum
            FROM quote_deltas
            ORDER BY timestamp DESC
            LIMIT $2
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await asyncio.wait_for(
                    conn.fetch(query, symbol, limit),
                    timeout=15.0,
                )
                return [dict(row) for row in rows]
        except asyncio.TimeoutError:
            logger.warning(f"Buying pressure query timed out for {symbol}, returning empty")
            return []
        except Exception as e:
            logger.warning(f"Buying pressure query failed for {symbol} (returning empty): {e!r}")
            return []

    async def get_vwap_deviation(
        self,
        symbol: str = 'SPY',
        timeframe: str = '1min',
        window_units: int = 20
    ) -> List[Dict[str, Any]]:
        """Get VWAP deviation for mean reversion signals by interval/window."""
        window_units = max(1, min(window_units, 90))
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

    async def get_opening_range_breakout(
        self,
        symbol: str = 'SPY',
        timeframe: str = '1min',
        window_units: int = 20
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
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching dealer hedging: {e}", exc_info=True)
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
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching volume spikes: {e}", exc_info=True)
            raise

    async def get_momentum_divergence(
        self,
        symbol: str = 'SPY',
        timeframe: str = '1min',
        window_units: int = 20
    ) -> List[Dict[str, Any]]:
        """Get momentum divergence signals matching Makefile divergence shortcut semantics."""
        window_units = max(1, min(window_units, 90))
        query = """
            WITH option_flow AS (
                SELECT
                    timestamp,
                    symbol,
                    SUM(CASE WHEN option_type = 'C' THEN total_premium ELSE -total_premium END)::numeric AS net_option_flow
                FROM flow_by_type
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

    # ========================================================================
    # Trade Signal Queries
    # ========================================================================

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
                tf  = row["timeframe"]
                sb  = row["strength_bucket"]
                tot = row["total"] or 0
                cor = row["correct"] or 0
                result.setdefault(tf, {})[sb] = {
                    "total":   tot,
                    "correct": cor,
                    "win_pct": round(cor / tot, 4) if tot > 0 else None,
                }
            return result
        except Exception as e:
            logger.error(f"get_signal_accuracy failed: {e}")
            return {}


    async def get_vol_expansion_signal(
        self,
        symbol: str = "SPY",
    ) -> Optional[Dict[str, Any]]:
        """Return the most recent vol_expansion component score for this symbol.

        Reads from signal_component_scores (populated by VolExpansionComponent
        via ScoringEngine) and returns the raw score scaled to [0, 100].
        """
        query = """
            SELECT
                scs.underlying,
                scs.timestamp,
                scs.raw_score,
                scs.weighted_score,
                scs.weight,
                scs.context_values,
                CASE
                    WHEN scs.raw_score > 0 THEN 'bullish'
                    WHEN scs.raw_score < 0 THEN 'bearish'
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
                raw = d.get("raw_score") or 0.0
                d["score"] = round(float(raw) * 100.0, 2)
                ctx = d.get("context_values") or {}
                if isinstance(ctx, str):
                    ctx = json.loads(ctx)
                d["context_values"] = ctx
                return d
        except Exception as e:
            logger.error(f"get_vol_expansion_signal failed ({symbol}): {e}")
            return None

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
                    "avg_realized_return_pct": round(float(row["avg_realized_return_pct"]), 4) if row["avg_realized_return_pct"] is not None else None,
                    "avg_expected_value": round(float(row["avg_expected_value"]), 4) if row["avg_expected_value"] is not None else None,
                    "avg_predicted_pop": round(float(row["avg_predicted_pop"]), 4) if row["avg_predicted_pop"] is not None else None,
                    "avg_realized_move_pct": round(float(row["avg_realized_move_pct"]), 4) if row["avg_realized_move_pct"] is not None else None,
                }
            return result
        except Exception as e:
            logger.error(f"get_position_optimizer_accuracy failed: {e}")
            return {}


    async def get_latest_quote(self, symbol: str = 'SPY') -> Optional[Dict[str, Any]]:
        """Get latest underlying quote"""
        symbol = symbol.upper()
        cache_key = f"latest_quote:{symbol}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        query = """
            WITH latest_quote AS (
                SELECT
                    uq.timestamp,
                    uq.symbol,
                    uq.open,
                    uq.high,
                    uq.low,
                    uq.close
                FROM underlying_quotes uq
                WHERE uq.symbol = $1
                ORDER BY uq.timestamp DESC
                LIMIT 1
            )
            SELECT
                lq.timestamp,
                lq.symbol,
                lq.open,
                lq.high,
                lq.low,
                lq.close,
                COALESCE(udv.cumulative_daily_volume, 0)::bigint AS cumulative_daily_volume,
                s.asset_type
            FROM latest_quote lq
            LEFT JOIN underlying_daily_volume udv
              ON udv.symbol = lq.symbol
             AND udv.trade_date_et = (lq.timestamp AT TIME ZONE 'America/New_York')::date
            LEFT JOIN symbols s ON s.symbol = lq.symbol
        """

        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol)
                payload = dict(row) if row else None
                self._cache_set(
                    cache_key,
                    payload,
                    self._latest_quote_cache_ttl_seconds,
                )
                return payload
        except Exception as e:
            logger.error(f"Error fetching latest quote: {e!r}", exc_info=True)
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
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol)
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error fetching previous close: {e}", exc_info=True)
            raise

    async def get_session_closes(self, symbol: str = 'SPY') -> Optional[Dict[str, Any]]:
        """
        Get the two most recently completed regular session closes.

        current_session_close = the most recent completed cash session close
          (last bar <= 16:00 ET on the most recent day whose session has ended).
          Today's session is only included if the current time is at/after 16:00 ET.
        prior_session_close = the session close immediately before current.
        """
        query = """
            WITH session_closes AS (
                SELECT DISTINCT ON ((timestamp AT TIME ZONE 'America/New_York')::date)
                    timestamp,
                    close
                FROM underlying_quotes
                WHERE symbol = $1
                    AND EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') BETWEEN 1 AND 5
                    AND (timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '16:00'
                    AND timestamp <= NOW()
                    -- Exclude today's date if the session hasn't closed yet (before 16:00 ET)
                    AND (
                        (timestamp AT TIME ZONE 'America/New_York')::date
                        < (NOW() AT TIME ZONE 'America/New_York')::date
                        OR (NOW() AT TIME ZONE 'America/New_York')::time >= '16:00'
                    )
                ORDER BY (timestamp AT TIME ZONE 'America/New_York')::date DESC, timestamp DESC
                LIMIT 2
            ),
            ranked AS (
                SELECT
                    close,
                    timestamp,
                    ROW_NUMBER() OVER (ORDER BY timestamp DESC) AS rn
                FROM session_closes
            )
            SELECT
                MAX(CASE WHEN rn = 1 THEN close END)     AS current_session_close,
                MAX(CASE WHEN rn = 1 THEN timestamp END) AS current_session_close_ts,
                MAX(CASE WHEN rn = 2 THEN close END)     AS prior_session_close,
                MAX(CASE WHEN rn = 2 THEN timestamp END) AS prior_session_close_ts
            FROM ranked
        """

        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, symbol)

                current_close = row['current_session_close'] if row else None
                current_ts = row['current_session_close_ts'] if row else None
                prior_close = row['prior_session_close'] if row else None
                prior_ts = row['prior_session_close_ts'] if row else None

                # Fall back to the most recent quote price if a session close is missing
                if current_close is None or prior_close is None:
                    fallback = await conn.fetchrow(
                        """
                        SELECT close, timestamp
                        FROM underlying_quotes
                        WHERE symbol = $1
                        ORDER BY timestamp DESC
                        LIMIT 1
                        """,
                        symbol,
                    )
                    fallback_price = fallback['close'] if fallback else None
                    fallback_ts = fallback['timestamp'] if fallback else None

                    if current_close is None:
                        current_close = fallback_price
                        current_ts = fallback_ts

                    if prior_close is None:
                        prior_close = current_close
                        prior_ts = current_ts

                if current_close is None:
                    return None

                return {
                    'symbol': symbol,
                    'current_session_close': current_close,
                    'current_session_close_ts': current_ts,
                    'prior_session_close': prior_close,
                    'prior_session_close_ts': prior_ts,
                }
        except Exception as e:
            logger.error(f"Error fetching session closes: {e}", exc_info=True)
            raise

    async def get_historical_quotes(
        self,
        symbol: str = 'SPY',
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        window_units: int = 90,
        timeframe: str = '1min'
    ) -> List[Dict[str, Any]]:
        """Get historical quotes aggregated by timeframe."""
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        query = f"""
            WITH latest AS (
                SELECT timestamp AS max_ts
                FROM underlying_quotes
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
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
            async with self._acquire_connection() as conn:
                window_units = max(1, min(window_units, 90))
                rows = await conn.fetch(query, symbol, start_date, end_date, window_units)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching historical quotes: {e}", exc_info=True)
            raise

    async def get_max_pain_timeseries(
        self,
        symbol: str = 'SPY',
        timeframe: str = '5min',
        window_units: int = 90
    ) -> List[Dict[str, Any]]:
        """Get max pain timeseries aggregated to timeframe over window units."""
        window_units = max(1, min(window_units, 90))
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        query = f"""
            WITH latest AS (
                SELECT timestamp AS max_ts
                FROM gex_summary
                WHERE underlying = $1
                ORDER BY timestamp DESC
                LIMIT 1
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

        async with self._acquire_connection() as conn:
            rows = await conn.fetch(query, symbol, window_units)
            return [dict(row) for row in rows]

    async def get_max_pain_current(self, symbol: str = 'SPY', strike_limit: int = 200) -> Optional[Dict[str, Any]]:
        """Get current max pain from daily OI snapshot cache."""
        symbol = symbol.upper()
        cache_key = f"max_pain_current:{symbol}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

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

        async with self._acquire_connection() as conn:
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

            result = {
                'timestamp': snapshot['timestamp'],
                'symbol': snapshot['symbol'],
                'underlying_price': snapshot['underlying_price'],
                'max_pain': snapshot['max_pain'],
                'difference': snapshot['difference'],
                'expirations': expirations,
            }
            self._cache_set(cache_key, result, self._analytics_cache_ttl_seconds)
            return result

    # ========================================================================
    # Chart Data Queries
    # ========================================================================

    async def get_gex_heatmap(
        self,
        symbol: str = 'SPY',
        timeframe: str = '5min',
        window_units: int = 60
    ) -> List[Dict[str, Any]]:
        """
        Get GEX data by strike over time for heatmap visualization using interval + window units.
        """
        symbol = symbol.upper()
        window_units = max(1, min(window_units, 90))
        cache_key = f"gex_heatmap:{symbol}:{timeframe}:{window_units}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        query = f"""
            WITH latest_price_timestamp AS (
                SELECT timestamp as max_ts
                FROM underlying_quotes
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            time_window AS (
                SELECT
                    max_ts - ({step_interval} * ($2 - 1)) as start_time,
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
            async with self._acquire_connection() as conn:
                rows = await asyncio.wait_for(
                    conn.fetch(query, symbol, window_units),
                    timeout=15.0
                )
                result = [dict(row) for row in rows]
                self._cache_set(cache_key, result, self._analytics_cache_ttl_seconds)
                return result
        except asyncio.TimeoutError:
            logger.warning(f"GEX heatmap query timed out for {symbol} timeframe={timeframe} window={window_units}, returning empty")
            return []
        except Exception as e:
            logger.warning(f"GEX heatmap query failed for {symbol} (returning empty): {e!r}")
            return []

    async def get_option_quote(
        self,
        underlying: str,
        strike: Optional[float] = None,
        expiration: Optional[str] = None,
        option_type: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get the most recent quote matching the provided filters"""
        conditions = ["underlying = $1"]
        params: list = [underlying]

        if strike is not None:
            params.append(float(strike))
            conditions.append(f"strike = ${len(params)}")

        if expiration is not None:
            expiration_date = datetime.strptime(expiration, "%Y-%m-%d").date()
            params.append(expiration_date)
            conditions.append(f"expiration = ${len(params)}")

        if option_type is not None:
            params.append(option_type)
            conditions.append(f"option_type = ${len(params)}")

        query = f"""
            SELECT
                timestamp,
                underlying,
                strike,
                expiration,
                option_type,
                bid,
                ask,
                volume,
                open_interest
            FROM option_chains
            WHERE {" AND ".join(conditions)}
            ORDER BY timestamp DESC
            LIMIT 1
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(query, *params)
                return dict(row) if row else None
        except ValueError as e:
            logger.error(f"Invalid expiration format '{expiration}': {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching option quote: {e}", exc_info=True)
            raise

    async def get_option_contract_history(
        self,
        underlying: str,
        strike: float,
        expiration: str,
        option_type: str,
    ) -> List[Dict[str, Any]]:
        """Get all rows for a specific option contract.

        If the regular market session is currently open (weekday 09:30–16:00 ET)
        returns today's data; otherwise returns data for the most recent date
        that has rows for this contract in the database.
        """
        from datetime import time as _time
        expiration_date = datetime.strptime(expiration, "%Y-%m-%d").date()

        now_et = datetime.now(_ET)
        today = now_et.date()
        session_is_open = (
            today.weekday() < 5
            and _time(9, 30) <= now_et.time() < _time(16, 0)
        )

        if session_is_open:
            target_date = today
        else:
            date_query = """
                SELECT MAX(DATE(timestamp AT TIME ZONE 'America/New_York')) AS latest_date
                FROM option_chains
                WHERE underlying = $1
                  AND strike = $2
                  AND expiration = $3
                  AND option_type = $4
            """
            try:
                async with self._acquire_connection() as conn:
                    row = await conn.fetchrow(
                        date_query, underlying, float(strike), expiration_date, option_type
                    )
                    if not row or row["latest_date"] is None:
                        return []
                    target_date = row["latest_date"]
            except Exception as e:
                logger.error(f"Error finding most recent date for option contract: {e}")
                raise

        query = """
            WITH ranked AS (
                SELECT
                    *,
                    DATE_TRUNC('minute', timestamp)                          AS bar_ts,
                    MAX(volume)      OVER (PARTITION BY DATE_TRUNC('minute', timestamp)) AS bar_volume,
                    MAX(open_interest) OVER (PARTITION BY DATE_TRUNC('minute', timestamp)) AS bar_oi,
                    MAX(updated_at)  OVER (PARTITION BY DATE_TRUNC('minute', timestamp)) AS bar_updated_at,
                    ROW_NUMBER()     OVER (
                        PARTITION BY DATE_TRUNC('minute', timestamp)
                        ORDER BY timestamp DESC
                    ) AS rn
                FROM option_chains
                WHERE underlying = $1
                  AND strike = $2
                  AND expiration = $3
                  AND option_type = $4
                  AND DATE(timestamp AT TIME ZONE 'America/New_York') = $5
            )
            SELECT
                bar_ts             AS timestamp,
                underlying,
                strike,
                expiration,
                option_type,
                last,
                bid,
                ask,
                mid,
                bar_volume         AS volume,
                bar_oi             AS open_interest,
                ask_volume,
                mid_volume,
                bid_volume,
                implied_volatility,
                delta,
                gamma,
                theta,
                vega,
                bar_updated_at     AS updated_at,
                GREATEST(
                    COALESCE(bar_volume, 0)
                        - COALESCE(LAG(bar_volume) OVER (ORDER BY bar_ts), 0),
                    0
                )::bigint          AS volume_delta
            FROM ranked
            WHERE rn = 1
            ORDER BY timestamp ASC
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(
                    query, underlying, float(strike), expiration_date, option_type, target_date
                )
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching option contract history: {e}", exc_info=True)
            raise

    # ------------------------------------------------------------------
    # Vol Surface
    # ------------------------------------------------------------------

    async def get_vol_surface_data(
        self,
        symbol: str,
        dte_max: int,
        strike_count: int,
    ) -> Optional[Dict[str, Any]]:
        """Fetch latest option-chain snapshot for a vol surface.

        Returns spot price, snapshot timestamp, and rows of
        (strike, expiration, option_type, implied_volatility, delta,
        open_interest) filtered to the `strike_count` strikes nearest
        spot and expirations within `dte_max` days.
        """
        spot_query = """
            SELECT close, timestamp
            FROM underlying_quotes
            WHERE symbol = $1
            ORDER BY timestamp DESC
            LIMIT 1
        """

        chain_query = """
            WITH latest_ts AS (
                SELECT timestamp AS ts
                FROM option_chains
                WHERE underlying = $1
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            eligible_strikes AS (
                SELECT strike
                FROM (
                    SELECT DISTINCT strike
                    FROM option_chains, latest_ts
                    WHERE underlying = $1
                      AND timestamp = latest_ts.ts
                      AND expiration <= CURRENT_DATE + make_interval(days => $2)
                ) sub
                ORDER BY ABS(sub.strike - $3::numeric)
                LIMIT $4
            )
            SELECT
                oc.strike,
                oc.expiration,
                oc.option_type,
                oc.implied_volatility,
                oc.delta,
                oc.open_interest
            FROM option_chains oc
            CROSS JOIN latest_ts lt
            JOIN eligible_strikes es ON es.strike = oc.strike
            WHERE oc.underlying = $1
              AND oc.timestamp = lt.ts
              AND oc.expiration <= CURRENT_DATE + make_interval(days => $2)
            ORDER BY oc.expiration, oc.strike, oc.option_type
        """

        try:
            async with self._acquire_connection() as conn:
                spot_row = await conn.fetchrow(spot_query, symbol)
                if not spot_row:
                    return None

                spot_price = float(spot_row["close"])
                timestamp = spot_row["timestamp"]

                rows = await conn.fetch(
                    chain_query, symbol, dte_max, spot_price, strike_count
                )
                return {
                    "spot_price": spot_price,
                    "timestamp": timestamp,
                    "rows": [dict(r) for r in rows],
                }
        except Exception as e:
            logger.error(f"Error fetching vol surface data: {e}", exc_info=True)
            raise

    async def get_signal_history(self, symbol: str = "SPY", limit: int = 100) -> list[Dict[str, Any]]:
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

    async def get_current_signal_with_trades(self, symbol: str = "SPY", timeframe: str = "intraday") -> Optional[Dict[str, Any]]:
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

    async def _get_signal_calibration_history(
        self,
        conn: asyncpg.Connection,
        symbol: str,
        as_of: datetime,
        limit: int = 2000,
        horizon_minutes: int = 60,
    ) -> list[Dict[str, Any]]:
        query = """
            WITH raw AS (
                SELECT
                    ss.timestamp,
                    ss.composite_score,
                    CASE
                        WHEN COALESCE((ss.components -> 'gex_regime' ->> 'value')::double precision, 0.0) < 0 THEN 'short_gamma'
                        WHEN COALESCE((ss.components -> 'gex_regime' ->> 'value')::double precision, 0.0) > 0 THEN 'long_gamma'
                        ELSE 'neutral_gamma'
                    END AS regime,
                    q0.close AS close_at_signal,
                    q1.close AS close_forward
                FROM signal_scores ss
                JOIN LATERAL (
                    SELECT close
                    FROM underlying_quotes q
                    WHERE q.symbol = ss.underlying
                      AND q.timestamp <= ss.timestamp
                    ORDER BY q.timestamp DESC
                    LIMIT 1
                ) q0 ON TRUE
                JOIN LATERAL (
                    SELECT close
                    FROM underlying_quotes q
                    WHERE q.symbol = ss.underlying
                      AND q.timestamp >= ss.timestamp + make_interval(mins => $4::int)
                    ORDER BY q.timestamp ASC
                    LIMIT 1
                ) q1 ON TRUE
                WHERE ss.underlying = $1
                  AND ss.timestamp < $2
                ORDER BY ss.timestamp DESC
                LIMIT $3
            )
            SELECT
                composite_score,
                regime,
                CASE
                    WHEN close_at_signal > 0 THEN (close_forward / close_at_signal) - 1.0
                    ELSE NULL
                END AS fwd_return
            FROM raw
            WHERE close_at_signal > 0
              AND close_forward IS NOT NULL
        """
        rows = await conn.fetch(query, symbol, as_of, limit, horizon_minutes)
        return [dict(r) for r in rows]

    async def get_latest_signal_score_enriched(self, symbol: str = "SPY") -> Optional[Dict[str, Any]]:
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT underlying, timestamp, composite_score, normalized_score, direction, components
                    FROM signal_scores
                    WHERE underlying = $1
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
                regime = classify_regime(row.get("components"))
                row["regime"] = regime
                calibration_history = await self._get_signal_calibration_history(
                    conn,
                    symbol=symbol,
                    as_of=row["timestamp"],
                )
                row["analytics"] = calibrate_signal(
                    current_composite=float(row.get("composite_score") or 0.0),
                    current_normalized=float(row.get("normalized_score") or 0.0),
                    current_regime=regime,
                    history_rows=calibration_history,
                )
                return row
        except Exception as e:
            logger.error(f"get_latest_signal_score_enriched failed ({symbol}): {e}")
            return None

    async def get_signal_score_history(self, symbol: str = "SPY", limit: int = 100) -> list[Dict[str, Any]]:
        query = """
            SELECT underlying, timestamp, composite_score, normalized_score, direction, components
            FROM signal_scores
            WHERE underlying = $1
            ORDER BY timestamp DESC
            LIMIT $2
        """
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, symbol, limit)
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
