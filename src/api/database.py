"""
Database manager for API queries
Uses asyncpg for async PostgreSQL operations
"""

import asyncio
import asyncpg
import os
import time as time_module
import traceback
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta, date, time, timezone
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo
import logging
import json

from src.api.queries.signals import SignalsQueriesMixin
from src.api.queries.technicals import TechnicalsQueriesMixin

logger = logging.getLogger(__name__)


_ET = ZoneInfo("America/New_York")
# Default history depth for component score endpoints. Sized to span the two
# most recent trading sessions at the engine's heartbeat cadence (~one row
# per 5-min bucket, 24/5 engine = 12 * 24 hours per session * 2 sessions =
# ~576 rows; round up for headroom across long weekends and signals that
# flip more often than the heartbeat).
SIGNAL_HISTORY_LIMIT = 600
# Calendar lookback in days used as the time bound on score history reads.
# 4 days covers two sessions across a normal weekend (Mon@now ↦ Fri full
# session) and most US-holiday breaks; the row limit above caps the
# result-set size on dense signals.
SIGNAL_HISTORY_LOOKBACK_DAYS = 4


def _get_session_bounds(session: str = "current") -> tuple:
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

    market_is_open = current_session_date == today and now_et.time() < market_close_time

    if session == "current":
        start = make_ts(current_session_date, market_open_time)
        end = now_et if market_is_open else make_ts(current_session_date, market_close_time)
    else:  # 'prior'
        prior_date = prev_trading_day(current_session_date)
        start = make_ts(prior_date, market_open_time)
        end = make_ts(prior_date, market_close_time)

    return start, end


def _get_flow_session_bounds(session: str = "current") -> tuple:
    """Return (start_ts, end_ts) for flow endpoints, which run 09:30–16:15 ET.

    Aligned to TradeStation's regular trading hours so the per-contract
    cumulative counters in flow_by_contract reset at 09:30 ET with the
    underlying volume field.

    'current': today 09:30–now if session is open, else most recent session 09:30–16:15 ET.
    'prior':   the full session immediately before the current one.
    """
    now_et = datetime.now(_ET)
    today = now_et.date()
    session_open_time = time(9, 30)
    session_close_time = time(16, 15)

    def prev_trading_day(d):
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    def make_ts(d, t):
        return datetime(d.year, d.month, d.day, t.hour, t.minute, tzinfo=_ET)

    is_weekday = today.weekday() < 5
    past_open = now_et.time() >= session_open_time
    current_session_date = today if (is_weekday and past_open) else prev_trading_day(today)

    session_is_open = current_session_date == today and now_et.time() < session_close_time

    if session == "current":
        start = make_ts(current_session_date, session_open_time)
        end = now_et if session_is_open else make_ts(current_session_date, session_close_time)
    else:  # 'prior'
        prior_date = prev_trading_day(current_session_date)
        start = make_ts(prior_date, session_open_time)
        end = make_ts(prior_date, session_close_time)

    return start, end


# SQL fragment allowlists live in `_sql_helpers` so that both
# DatabaseManager and the query mixins it composes can import them
# without a circular import.
from src.api.queries._sql_helpers import (
    _bucket_expr,
    _gex_by_strike_order_clause,
    _interval_expr,
    _normalize_timeframe,
    _timeframe_view_suffix,
)

# option_chains rows are UPSERTed in 60-second buckets: every contract that
# ticks within a minute is rewritten to the same `timestamp`, and each write
# bumps `updated_at` to NOW(). The bucket for the current minute is therefore
# partially populated at any given moment, so `SELECT MAX(timestamp)` while
# ingestion is live can return a snapshot missing contracts that haven't had
# a tick yet — producing the sparse responses historically seen on
# /api/market/open-interest and /api/gex/vol_surface.
#
# STABLE_SNAPSHOT_CTE defines a `latest_ts(ts)` CTE that picks a snapshot
# guaranteed to be complete: if the most recent bucket has quiesced (no
# writes for at least STABLE_SNAPSHOT_QUIESCENCE_SECONDS) use it, otherwise
# fall back to the prior bucket, which must be complete since ingestion has
# already rolled over to a newer one. If only one bucket exists, use it.
# The only bound parameter referenced is `$1 = underlying`.
_STABLE_SNAPSHOT_QUIESCENCE_SECONDS = float(os.getenv("STABLE_SNAPSHOT_QUIESCENCE_SECONDS", "15"))

_STABLE_SNAPSHOT_CTE = f"""
    recent_ts AS (
        SELECT DISTINCT timestamp
        FROM option_chains
        WHERE underlying = $1
        ORDER BY timestamp DESC
        LIMIT 2
    ),
    snapshot_stats AS (
        SELECT rt.timestamp, MAX(oc.updated_at) AS last_write
        FROM recent_ts rt
        JOIN option_chains oc
          ON oc.underlying = $1 AND oc.timestamp = rt.timestamp
        GROUP BY rt.timestamp
    ),
    latest_ts AS (
        SELECT CASE
            WHEN (SELECT COUNT(*) FROM snapshot_stats) < 2
                 OR (SELECT last_write FROM snapshot_stats
                     ORDER BY timestamp DESC LIMIT 1)
                     < NOW() - make_interval(secs => {_STABLE_SNAPSHOT_QUIESCENCE_SECONDS})
            THEN (SELECT timestamp FROM snapshot_stats
                  ORDER BY timestamp DESC LIMIT 1)
            ELSE (SELECT timestamp FROM snapshot_stats
                  ORDER BY timestamp DESC OFFSET 1 LIMIT 1)
        END AS ts
    )
"""


class DatabaseManager(SignalsQueriesMixin, TechnicalsQueriesMixin):
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
        # Symbols whose max-pain snapshot is refreshed by a background task in
        # the FastAPI lifespan; for these, get_max_pain_current skips the heavy
        # inline _refresh_max_pain_snapshot call and just reads from the
        # snapshot tables.  Symbols not listed here keep the original
        # on-demand-recompute behavior.
        self._max_pain_background_refresh_enabled: bool = (
            os.getenv("MAX_PAIN_BACKGROUND_REFRESH_ENABLED", "true").lower() == "true"
        )
        self._max_pain_background_refresh_symbols: frozenset = frozenset(
            s.strip().upper()
            for s in os.getenv(
                "MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS", "SPY,SPX,QQQ"
            ).split(",")
            if s.strip()
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
        pgpass_file = Path.home() / ".pgpass"
        if pgpass_file.exists():
            with open(pgpass_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        parts = line.split(":")
                        if len(parts) >= 5:
                            self.host = parts[0]
                            self.port = parts[1]
                            self.database = parts[2]
                            self.user = parts[3]
                            self.password = parts[4]
                            return

        # Fallback to environment variables
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.database = os.getenv("DB_NAME", "zerogex")
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "")

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
        logger.warning(
            "DatabaseManager.disconnect() called — stack:\n%s",
            "".join(traceback.format_stack()),
        )
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
        # Only classify true *connection/pool*-level failures as transient.
        # In particular, do NOT match asyncpg's per-statement command_timeout
        # (a bare TimeoutError raised from inside conn.execute): that means a
        # specific query was slow, not that the pool is unhealthy. Treating
        # statement timeouts as transient triggered pool-wide reconnects on
        # every slow request and produced concurrent reconnect storms when
        # several heavy queries (e.g. /api/max-pain/current) timed out in
        # parallel. See the 2026-05-11 incident logs.
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
            )
        ) or isinstance(error, (ConnectionError, OSError))

    async def _reconnect_pool(self) -> None:
        """Reconnect DB pool once under lock."""
        async with self._pool_lock:
            old_pool = self.pool
            self.pool = await self._create_pool()
        if old_pool is not None:
            logger.warning(
                "DatabaseManager._reconnect_pool() closing old pool — stack:\n%s",
                "".join(traceback.format_stack()),
            )
            try:
                await old_pool.close()
            except Exception:
                logger.warning("Failed to close old pool during reconnect", exc_info=True)

    async def _acquire_with_retry(self) -> Tuple[asyncpg.Connection, asyncpg.Pool]:
        """Acquire one connection with retry-once on transient acquire-time errors.

        Returns ``(connection, pool)``.  The caller must release the
        connection back to the **same pool** it was acquired from rather
        than ``self.pool``: a concurrent ``_reconnect_pool`` could swap
        ``self.pool`` between acquire and release, and releasing to the
        wrong pool corrupts pool accounting.
        """
        for attempt in range(2):
            pool = self.pool
            if not self._pool_is_usable(pool):
                raise RuntimeError("Database pool is unavailable or closing")
            try:
                conn = await pool.acquire()
                return conn, pool
            except Exception as e:
                if attempt == 0 and self._is_transient_db_error(e):
                    logger.warning(
                        "Transient DB acquire error; reconnecting pool and retrying once",
                        exc_info=True,
                    )
                    await self._reconnect_pool()
                    continue
                raise
        raise RuntimeError("unreachable")  # for type checker; loop always returns or raises

    @asynccontextmanager
    async def _acquire_connection(self):
        """
        Acquire a DB connection from the existing pool.

        Retries once on *transient acquire-time* errors (e.g., a pooled
        connection that went stale across an RDS idle timeout): the pool
        is reconnected and the acquire is retried.  Errors raised inside
        the ``async with`` body are **not** retried — that would require
        re-yielding through the asynccontextmanager, which ``contextlib``
        does not support and which previously surfaced as the cryptic
        ``RuntimeError: generator didn't stop after athrow()`` on top of
        the original exception.  Callers that want their *use-time*
        errors retried should implement that at their own level.

        Fail fast when the pool is unavailable/closing.
        """
        conn, pool = await self._acquire_with_retry()
        try:
            yield conn
        finally:
            try:
                await pool.release(conn)
            except Exception:
                logger.warning("Failed to release DB connection", exc_info=True)

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
        row["total_volume"] = self._decode_json_field(row.get("total_volume"))
        row["total_premium"] = self._decode_json_field(row.get("total_premium"))
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

    async def _refresh_max_pain_snapshot(
        self, conn: asyncpg.Connection, symbol: str, strike_limit: int
    ) -> None:
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
            -- Find option_symbols that streamed any update in the last day.
            -- The (underlying, timestamp DESC, option_symbol) index covers
            -- this scan, which DISTINCTs ~3K symbols out of ~1M rows for SPY.
            -- Note: a wider window (e.g. 7 days) would cover the Friday →
            -- Monday gap and surface more strikes through weekends, but it
            -- pushes the DISTINCT scan to 10M+ rows and the refresh starts
            -- exceeding the statement_timeout for liquid underlyings. A
            -- proper weekend fix needs a separate ``active_contracts``
            -- materialized view (or similar) rather than widening this
            -- discovery scan; tracked as a follow-up.
            active_symbols AS (
                SELECT DISTINCT oc.option_symbol
                FROM option_chains oc
                CROSS JOIN should_refresh r
                WHERE oc.underlying = $1
                  AND oc.timestamp >= r.max_ts - INTERVAL '1 day'
                  AND oc.timestamp <= r.max_ts
            ),
            -- For each active symbol, fetch its latest row via the
            -- (option_symbol, timestamp DESC) primary key. OI is published
            -- once per day at settlement, so the latest row's OI is correct
            -- regardless of how recently the contract streamed. This avoids
            -- the original WHERE oc.timestamp = max_ts which collapsed the
            -- chain off-hours when only a handful of contracts streamed in
            -- the most-recent minute bucket.
            latest_per_contract AS (
                SELECT
                    latest.expiration,
                    latest.strike,
                    latest.option_type,
                    latest.open_interest
                FROM active_symbols s
                CROSS JOIN should_refresh r
                CROSS JOIN LATERAL (
                    SELECT expiration, strike, option_type, open_interest
                    FROM option_chains
                    WHERE option_symbol = s.option_symbol
                      AND timestamp <= r.max_ts
                    ORDER BY timestamp DESC
                    LIMIT 1
                ) latest
                WHERE latest.expiration >= (r.max_ts AT TIME ZONE 'America/New_York')::date
            ),
            contracts AS (
                SELECT
                    expiration,
                    strike,
                    option_type,
                    SUM(open_interest)::numeric AS oi
                FROM latest_per_contract
                WHERE open_interest > 0
                GROUP BY expiration, strike, option_type
            ),
            ranked_strikes AS (
                SELECT
                    s.expiration,
                    s.strike,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.expiration
                        ORDER BY ABS(s.strike - u.underlying_price), s.strike
                    ) AS rn
                FROM (SELECT DISTINCT expiration, strike FROM contracts) s
                CROSS JOIN underlying u
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

    async def refresh_max_pain_snapshots(
        self,
        symbols: List[str],
        strike_limit: int,
        statement_timeout_ms: int,
    ) -> None:
        """Refresh the max_pain_oi_snapshot table for the given symbols.

        Designed to run from a background task — not the request path — so the
        per-symbol recompute can take longer than the pool's default
        ``DB_STATEMENT_TIMEOUT_MS``.  Errors per symbol are logged but do not
        abort the loop.

        :param symbols: list of underlyings to refresh, e.g. ``["SPY", "SPX"]``.
        :param strike_limit: settlement-candidate cap (forwarded to
            :meth:`_refresh_max_pain_snapshot`).
        :param statement_timeout_ms: per-statement timeout override applied via
            ``SET LOCAL`` inside a transaction so the heavy CTE chain can run
            to completion.
        """
        for symbol in symbols:
            symbol_upper = symbol.upper()
            try:
                async with self._acquire_connection() as conn:
                    async with conn.transaction():
                        await conn.execute(
                            f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}"
                        )
                        await self._refresh_max_pain_snapshot(conn, symbol_upper, strike_limit)
                logger.info(
                    "max-pain background refresh: %s OK (strike_limit=%d)",
                    symbol_upper,
                    strike_limit,
                )
            except Exception:
                logger.warning(
                    "max-pain background refresh failed for %s (non-fatal, will retry)",
                    symbol_upper,
                    exc_info=True,
                )

    # ========================================================================
    # GEX Queries
    # ========================================================================

    async def get_latest_gex_summary(self, symbol: str = "SPY") -> Optional[Dict[str, Any]]:
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
                    gs.flip_distance,
                    gs.local_gex,
                    gs.convexity_risk,
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
                    -- Industry-standard dollar GEX per 1% move: γ × OI × 100 × S² × 0.01.
                    (gbs.call_gamma * 100 * lq.spot_price * lq.spot_price * 0.01)::numeric AS call_exposure,
                    (-1 * gbs.put_gamma * 100 * lq.spot_price * lq.spot_price * 0.01)::numeric AS put_exposure
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
                -- Re-derive net_gex from strike sums so the API output stays
                -- internally consistent (net_gex == call_gex + put_gex) and
                -- always uses the current formula, even when the stored
                -- summary row was written under an older convention.
                (st.total_call_gex + st.total_put_gex) AS net_gex,
                ls.gamma_flip_point AS gamma_flip,
                ls.flip_distance,
                ls.local_gex,
                ls.convexity_risk,
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
        symbol: str = "SPY",
        limit: int = 50,
        sort_by: str = "distance",  # 'distance' or 'impact'
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

        # `order_clause` is a validated literal from the allowlist; raises
        # ValueError on unknown sort_by. Runtime values use $1..$N.
        order_clause = _gex_by_strike_order_clause(sort_by)
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
                -- Industry-standard dollar GEX per 1% move: γ × OI × 100 × S² × 0.01.
                (g.call_gamma * 100 * COALESCE(spot.close, 0) * COALESCE(spot.close, 0) * 0.01) as call_gex,
                (-1 * g.put_gamma * 100 * COALESCE(spot.close, 0) * COALESCE(spot.close, 0) * 0.01) as put_gex,
                -- Re-derive net_gex from gamma using the current formula so the
                -- API output stays consistent regardless of when the row was
                -- written.  net_gex == call_gex + put_gex by construction.
                ((g.call_gamma - g.put_gamma) * 100 * COALESCE(spot.close, 0) * COALESCE(spot.close, 0) * 0.01) as net_gex,
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

    async def get_historical_gex(
        self,
        symbol: str = "SPY",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        window_units: int = 90,
        timeframe: str = "1min",
    ) -> List[Dict[str, Any]]:
        """Get historical GEX summary data aggregated by timeframe."""
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        # `bucket` and `step_interval` are validated allowlist literals.
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
                    -- Industry-standard dollar GEX per 1% move: γ × OI × 100 × S² × 0.01.
                    COALESCE(SUM(gbs.call_gamma * 100 * s.spot_price * s.spot_price * 0.01), 0)::numeric AS total_call_gex,
                    COALESCE(SUM(-1 * gbs.put_gamma * 100 * s.spot_price * s.spot_price * 0.01), 0)::numeric AS total_put_gex
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
                -- Re-derive net_gex from strike sums so historical buckets
                -- always reflect the current formula and stay consistent with
                -- (total_call_gex + total_put_gex), even for rows persisted
                -- under an older convention.
                (COALESCE(sa.total_call_gex, 0) + COALESCE(sa.total_put_gex, 0))::numeric AS net_gex,
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

    async def get_flow(
        self,
        symbol: str = "SPY",
        session: str = "current",
        intervals: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get unified option flow keyed by (type, strike, expiration) in 5-min buckets.

        Reads from the flow_by_contract rollup populated by the analytics
        engine and decorates each row with running cumulative totals plus a
        per (strike, expiration) running put/call ratio. When *intervals* is
        set, the query window is narrowed to the most recent N 5-minute
        buckets within the session; cumulative totals are then partial sums
        from the start of that window rather than session opens.
        """
        symbol = symbol.upper()
        intervals_key = intervals if intervals and intervals > 0 else "all"
        cache_key = f"flow:{symbol}:{session}:{intervals_key}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        session_start, session_end = _get_flow_session_bounds(session)

        # Align the query window to 5-minute bucket boundaries. Rows in
        # flow_by_contract are keyed by the bucket-start timestamp (e.g. 14:40
        # covers [14:40, 14:45)), so the most recent queryable bucket is the
        # one strictly before session_end. The (session_end − 1µs) floor makes
        # an exact boundary (e.g. 16:15:00 at session close) land on the
        # previous bucket rather than a bucket that would fall outside the
        # session window.
        bucket_seconds = 300
        end_bucket_epoch = int((session_end.timestamp() - 1e-6) // bucket_seconds) * bucket_seconds
        start_bucket_epoch = int(session_start.timestamp() // bucket_seconds) * bucket_seconds

        if intervals and intervals > 0:
            window_start_epoch = end_bucket_epoch - (intervals - 1) * bucket_seconds
            if window_start_epoch > start_bucket_epoch:
                start_bucket_epoch = window_start_epoch

        if end_bucket_epoch < start_bucket_epoch:
            self._cache_set(cache_key, [], self._flow_endpoint_cache_ttl_seconds)
            return []

        effective_start = datetime.fromtimestamp(start_bucket_epoch, tz=timezone.utc)
        effective_end = datetime.fromtimestamp(end_bucket_epoch, tz=timezone.utc)

        query = """
            SELECT
                timestamp,
                symbol,
                option_type,
                strike,
                expiration,
                (expiration - CURRENT_DATE)::int AS dte,
                raw_volume,
                raw_premium,
                net_volume,
                net_premium,
                underlying_price
            FROM flow_by_contract
            WHERE symbol = $1
              AND timestamp >= $2
              AND timestamp <= $3
            ORDER BY timestamp DESC, option_type, strike, expiration
        """

        try:
            async with self._acquire_connection() as conn:
                await self._refresh_flow_cache(conn, symbol)
                rows = await asyncio.wait_for(
                    conn.fetch(query, symbol, effective_start, effective_end),
                    timeout=15.0,
                )
                result = [dict(row) for row in rows]
                self._cache_set(cache_key, result, self._flow_endpoint_cache_ttl_seconds)
                return result
        except asyncio.TimeoutError:
            logger.warning(f"Flow query timed out for {symbol}, returning empty")
            return []
        except Exception as e:
            logger.warning(f"Flow query failed for {symbol} (returning empty): {e!r}")
            return []

    async def _resolve_flow_series_session(
        self,
        conn: asyncpg.Connection,
        symbol: str,
        session: str,
    ) -> Optional[Tuple[datetime, datetime, bool]]:
        """Resolve (session_start_utc, session_end_utc, symbol_has_any_data) for
        the data-driven session model used by /api/flow/series.

        Returns ``None`` when the symbol has no rows in flow_by_contract at
        all (spec: 404 unknown symbol). Returns ``(_, _, False)`` when the
        symbol exists but the requested session has no data — the endpoint
        surfaces this as ``200 + []`` (see T4 / "session=prior but no prior
        data"). Normal resolution returns ``(start, end, True)``.
        """
        exists = await conn.fetchval(
            "SELECT 1 FROM flow_by_contract WHERE symbol = $1 LIMIT 1",
            symbol,
        )
        if not exists:
            return None

        if session == "prior":
            # Resolve in two steps so the absence of a prior day is
            # distinguishable from the absence of any data at all.
            current_date = await conn.fetchval(
                """
                SELECT (MAX(timestamp) AT TIME ZONE 'America/New_York')::date
                FROM flow_by_contract
                WHERE symbol = $1
                """,
                symbol,
            )
            prior_date = await conn.fetchval(
                """
                SELECT (MAX(timestamp) AT TIME ZONE 'America/New_York')::date
                FROM flow_by_contract
                WHERE symbol = $1
                  AND (timestamp AT TIME ZONE 'America/New_York')::date < $2::date
                """,
                symbol,
                current_date,
            )
            if prior_date is None:
                # Symbol exists but has no prior session → 200 + [].
                return datetime.now(timezone.utc), datetime.now(timezone.utc), False
            session_start_et = datetime(
                prior_date.year, prior_date.month, prior_date.day, 9, 30, tzinfo=_ET
            )
            session_start_utc = session_start_et.astimezone(timezone.utc)
            # Prior session is always closed — cap at 16:15 ET (09:30 + 6h45m).
            session_end_utc = session_start_utc + timedelta(hours=6, minutes=45)
            return session_start_utc, session_end_utc, True

        # session == 'current': most recent ET day with data.
        current_date = await conn.fetchval(
            """
            SELECT (MAX(timestamp) AT TIME ZONE 'America/New_York')::date
            FROM flow_by_contract
            WHERE symbol = $1
            """,
            symbol,
        )
        session_start_et = datetime(
            current_date.year, current_date.month, current_date.day, 9, 30, tzinfo=_ET
        )
        session_start_utc = session_start_et.astimezone(timezone.utc)
        session_close_utc = session_start_utc + timedelta(hours=6, minutes=45)
        # Floor now() to the 5-minute bucket boundary so generate_series
        # lands on clean bar_start values (and stops before any
        # partially-populated bucket on the client's clock).
        now_utc = datetime.now(timezone.utc)
        now_floor_epoch = int(now_utc.timestamp() // 300) * 300
        now_floored = datetime.fromtimestamp(now_floor_epoch, tz=timezone.utc)
        session_end_utc = min(now_floored, session_close_utc)
        if session_end_utc < session_start_utc:
            # Shouldn't happen in practice — MAX(bar_start) >= session_start
            # implies now() is past open. Clamp defensively.
            session_end_utc = session_start_utc
        return session_start_utc, session_end_utc, True

    async def get_flow_series(
        self,
        symbol: str = "SPY",
        session: str = "current",
        strikes: Optional[List[float]] = None,
        expirations: Optional[List[date]] = None,
        intervals: Optional[int] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """Return pre-accumulated 5-minute flow series rows for a session.

        Returns ``None`` when the symbol has never appeared in
        flow_by_contract (caller surfaces 404). Otherwise returns a list of
        dicts — possibly empty — with one entry per 5-minute bar from 09:30 ET
        through the latest bar covered by the resolved session window.
        Carry-forward synthetic rows fill quiet bars; bars that haven't
        happened yet are excluded.

        Rows are returned newest-first so ``rows[0]`` is the most recent
        bar; ``intervals=N`` returns the leading N rows.
        """
        symbol = symbol.upper()

        # Cache only full-series fetches. Incremental (intervals=N) polls
        # bypass the cache so the tail row reflects the newest DB state.
        use_cache = intervals is None
        cache_key = None
        if use_cache:
            strikes_key = ",".join(f"{s:g}" for s in sorted(strikes)) if strikes else ""
            exps_key = ",".join(e.isoformat() for e in sorted(expirations)) if expirations else ""
            cache_key = f"flow_series:{symbol}:{session}:{strikes_key}:{exps_key}"
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached

        strikes_arg = [float(s) for s in strikes] if strikes else None
        expirations_arg = list(expirations) if expirations else None

        try:
            async with self._acquire_connection() as conn:
                await self._refresh_flow_cache(conn, symbol)
                resolved = await self._resolve_flow_series_session(conn, symbol, session)
                if resolved is None:
                    return None
                session_start, session_end, has_session_data = resolved
                if not has_session_data:
                    if use_cache:
                        self._cache_set(cache_key, [], self._flow_endpoint_cache_ttl_seconds)
                    return []

                query = """
                    WITH filtered AS (
                        SELECT
                            timestamp AS bar_start,
                            option_type,
                            strike,
                            expiration,
                            raw_volume,
                            net_volume,
                            net_premium
                        FROM flow_by_contract
                        WHERE symbol = $1
                          AND timestamp >= $2
                          AND timestamp <= $3
                          AND ($4::numeric[] IS NULL OR strike = ANY($4::numeric[]))
                          AND ($5::date[]    IS NULL OR expiration = ANY($5::date[]))
                    ),
                    contract_deltas AS (
                        SELECT
                            bar_start,
                            option_type,
                            strike,
                            expiration,
                            (raw_volume  - COALESCE(LAG(raw_volume)  OVER w, 0))::bigint  AS raw_volume_delta,
                            (net_volume  - COALESCE(LAG(net_volume)  OVER w, 0))::bigint  AS net_volume_delta,
                            (net_premium - COALESCE(LAG(net_premium) OVER w, 0))::numeric AS net_premium_delta
                        FROM filtered
                        WINDOW w AS (PARTITION BY option_type, strike, expiration ORDER BY bar_start)
                    ),
                    per_bar AS (
                        SELECT
                            bar_start,
                            SUM(CASE WHEN option_type='C' THEN net_premium_delta ELSE 0 END)::numeric AS call_premium_delta,
                            SUM(CASE WHEN option_type='P' THEN net_premium_delta ELSE 0 END)::numeric AS put_premium_delta,
                            SUM(CASE WHEN option_type='C' THEN raw_volume_delta  ELSE 0 END)::bigint  AS call_volume_delta,
                            SUM(CASE WHEN option_type='P' THEN raw_volume_delta  ELSE 0 END)::bigint  AS put_volume_delta,
                            SUM(net_volume_delta)::bigint                                             AS net_volume_delta,
                            SUM(raw_volume_delta)::bigint                                             AS raw_volume_delta,
                            SUM(CASE WHEN option_type='C' THEN net_volume_delta  ELSE 0 END)::bigint  AS call_position_delta,
                            SUM(CASE WHEN option_type='P' THEN net_volume_delta  ELSE 0 END)::bigint  AS put_position_delta,
                            COUNT(*)::int AS contract_count
                        FROM contract_deltas
                        GROUP BY bar_start
                    ),
                    -- Underlying price comes from the tape (underlying_quotes
                    -- OHLC), NOT from flow_by_contract.underlying_price. The
                    -- per-contract column captures each contract's last-trade
                    -- price, which is stale for contracts that didn't trade
                    -- in a given bar — aggregating it produces the stair-step
                    -- artifact where the price sticks for 20–30 minutes and
                    -- then jumps. Critically, this subquery does NOT see the
                    -- strike/expiration filters, so underlying_price stays
                    -- invariant across different filter combinations for the
                    -- same (symbol, bar_start).
                    underlying_by_bar AS (
                        SELECT
                            (date_trunc('hour', timestamp)
                             + FLOOR(EXTRACT(MINUTE FROM timestamp)::int / 5)
                               * INTERVAL '5 minutes') AS bar_start,
                            (ARRAY_AGG(close ORDER BY timestamp DESC))[1] AS underlying_price
                        FROM underlying_quotes
                        WHERE symbol = $1
                          AND timestamp >= $2
                          AND timestamp <  $3::timestamptz + INTERVAL '5 minutes'
                        GROUP BY 1
                    ),
                    timeline AS (
                        -- Gate the timeline on filtered having rows. An empty
                        -- filter match (T5) returns zero rows rather than 81
                        -- synthetic zero-cumulative bars.
                        SELECT g.bar_start
                        FROM generate_series($2::timestamptz, $3::timestamptz, INTERVAL '5 minutes') AS g(bar_start)
                        WHERE EXISTS (SELECT 1 FROM filtered)
                    ),
                    joined AS (
                        SELECT
                            t.bar_start,
                            COALESCE(pb.call_premium_delta, 0) AS call_premium_delta,
                            COALESCE(pb.put_premium_delta, 0)  AS put_premium_delta,
                            COALESCE(pb.call_volume_delta, 0)  AS call_volume_delta,
                            COALESCE(pb.put_volume_delta, 0)   AS put_volume_delta,
                            COALESCE(pb.net_volume_delta, 0)   AS net_volume_delta,
                            COALESCE(pb.raw_volume_delta, 0)   AS raw_volume_delta,
                            COALESCE(pb.call_position_delta, 0) AS call_position_delta,
                            COALESCE(pb.put_position_delta, 0)  AS put_position_delta,
                            ub.underlying_price,
                            COALESCE(pb.contract_count, 0) AS contract_count,
                            (pb.bar_start IS NULL) AS is_synthetic
                        FROM timeline t
                        LEFT JOIN per_bar           pb USING (bar_start)
                        LEFT JOIN underlying_by_bar ub USING (bar_start)
                    ),
                    carry AS (
                        -- FIRST_VALUE + partition-by-running-count emulates
                        -- LAST_VALUE(... IGNORE NULLS) portably (Postgres < 16
                        -- doesn't support IGNORE NULLS in LAST_VALUE).
                        SELECT
                            j.*,
                            COUNT(underlying_price) OVER (ORDER BY bar_start ROWS UNBOUNDED PRECEDING) AS up_grp
                        FROM joined j
                    )
                    SELECT
                        bar_start,
                        SUM(call_premium_delta)  OVER w_cum AS call_premium_cum,
                        SUM(put_premium_delta)   OVER w_cum AS put_premium_cum,
                        SUM(call_volume_delta)   OVER w_cum AS call_volume_cum,
                        SUM(put_volume_delta)    OVER w_cum AS put_volume_cum,
                        SUM(net_volume_delta)    OVER w_cum AS net_volume_cum,
                        SUM(raw_volume_delta)    OVER w_cum AS raw_volume_cum,
                        SUM(call_position_delta) OVER w_cum AS call_position_cum,
                        SUM(put_position_delta)  OVER w_cum AS put_position_cum,
                        (SUM(call_premium_delta) OVER w_cum
                         + SUM(put_premium_delta) OVER w_cum) AS net_premium_cum,
                        CASE
                            WHEN SUM(call_volume_delta) OVER w_cum > 0
                            THEN (SUM(put_volume_delta) OVER w_cum)::float8
                               / (SUM(call_volume_delta) OVER w_cum)::float8
                            ELSE NULL
                        END AS put_call_ratio,
                        FIRST_VALUE(underlying_price) OVER (
                            PARTITION BY up_grp ORDER BY bar_start
                        ) AS underlying_price,
                        contract_count,
                        is_synthetic
                    FROM carry
                    WINDOW w_cum AS (ORDER BY bar_start ROWS UNBOUNDED PRECEDING)
                    ORDER BY bar_start DESC
                """

                rows = await asyncio.wait_for(
                    conn.fetch(
                        query,
                        symbol,
                        session_start,
                        session_end,
                        strikes_arg,
                        expirations_arg,
                    ),
                    timeout=15.0,
                )
                result = [dict(row) for row in rows]
                if intervals is not None and intervals > 0 and len(result) > intervals:
                    # Result is newest-first; take the leading N rows for the
                    # most-recent N 5-minute buckets.
                    result = result[:intervals]
                if use_cache:
                    self._cache_set(cache_key, result, self._flow_endpoint_cache_ttl_seconds)
                return result
        except asyncio.TimeoutError:
            logger.warning(f"Flow series query timed out for {symbol}, returning empty")
            return []
        except Exception as e:
            logger.warning(f"Flow series query failed for {symbol} (returning empty): {e!r}")
            return []

    async def get_flow_contracts(
        self,
        symbol: str = "SPY",
        session: str = "current",
    ) -> Optional[Dict[str, List[Any]]]:
        """Return the distinct strikes and expirations that traded in a session.

        Returns ``None`` when the symbol has never appeared in flow_by_contract
        (caller surfaces 404). Otherwise a dict with ``strikes`` (ascending
        floats) and ``expirations`` (ascending ISO dates). Empty lists are
        returned when the resolved session has no data.
        """
        symbol = symbol.upper()
        cache_key = f"flow_contracts:{symbol}:{session}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        try:
            async with self._acquire_connection() as conn:
                await self._refresh_flow_cache(conn, symbol)
                resolved = await self._resolve_flow_series_session(conn, symbol, session)
                if resolved is None:
                    return None
                session_start, session_end, has_session_data = resolved
                if not has_session_data:
                    payload = {"strikes": [], "expirations": []}
                    self._cache_set(cache_key, payload, self._flow_endpoint_cache_ttl_seconds)
                    return payload

                query = """
                    SELECT
                        COALESCE(
                            ARRAY(
                                SELECT DISTINCT strike
                                FROM flow_by_contract
                                WHERE symbol = $1
                                  AND timestamp >= $2
                                  AND timestamp <= $3
                                ORDER BY strike
                            ),
                            ARRAY[]::numeric[]
                        ) AS strikes,
                        COALESCE(
                            ARRAY(
                                SELECT DISTINCT expiration
                                FROM flow_by_contract
                                WHERE symbol = $1
                                  AND timestamp >= $2
                                  AND timestamp <= $3
                                ORDER BY expiration
                            ),
                            ARRAY[]::date[]
                        ) AS expirations
                """
                row = await asyncio.wait_for(
                    conn.fetchrow(query, symbol, session_start, session_end),
                    timeout=10.0,
                )
                strikes = [float(s) for s in (row["strikes"] or [])]
                expirations = [d.isoformat() for d in (row["expirations"] or [])]
                payload = {"strikes": strikes, "expirations": expirations}
                self._cache_set(cache_key, payload, self._flow_endpoint_cache_ttl_seconds)
                return payload
        except asyncio.TimeoutError:
            logger.warning(f"Flow contracts query timed out for {symbol}, returning empty")
            return {"strikes": [], "expirations": []}
        except Exception as e:
            logger.warning(f"Flow contracts query failed for {symbol} (returning empty): {e!r}")
            return {"strikes": [], "expirations": []}

    async def get_smart_money_flow(
        self, symbol: str = "SPY", session: str = "current", limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get smart-money events from canonical flow_contract_facts."""
        session_start, session_end = _get_flow_session_bounds(session)
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
        self, symbol: str = "SPY", limit: int = 20
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

    # ========================================================================
    # Trade Signal Queries
    # ========================================================================

    async def get_latest_quote(self, symbol: str = "SPY") -> Optional[Dict[str, Any]]:
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

    async def get_previous_close(self, symbol: str = "SPY") -> Optional[Dict[str, Any]]:
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

    async def get_session_closes(self, symbol: str = "SPY") -> Optional[Dict[str, Any]]:
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

                current_close = row["current_session_close"] if row else None
                current_ts = row["current_session_close_ts"] if row else None
                prior_close = row["prior_session_close"] if row else None
                prior_ts = row["prior_session_close_ts"] if row else None

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
                    fallback_price = fallback["close"] if fallback else None
                    fallback_ts = fallback["timestamp"] if fallback else None

                    if current_close is None:
                        current_close = fallback_price
                        current_ts = fallback_ts

                    if prior_close is None:
                        prior_close = current_close
                        prior_ts = current_ts

                if current_close is None:
                    return None

                return {
                    "symbol": symbol,
                    "current_session_close": current_close,
                    "current_session_close_ts": current_ts,
                    "prior_session_close": prior_close,
                    "prior_session_close_ts": prior_ts,
                }
        except Exception as e:
            logger.error(f"Error fetching session closes: {e}", exc_info=True)
            raise

    async def get_historical_quotes(
        self,
        symbol: str = "SPY",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        window_units: int = 192,
        timeframe: str = "1min",
    ) -> List[Dict[str, Any]]:
        """Get historical quotes aggregated by timeframe."""
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        # `bucket` and `step_interval` are validated allowlist literals.
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
                window_units = max(1, min(window_units, 576))
                rows = await conn.fetch(query, symbol, start_date, end_date, window_units)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching historical quotes: {e}", exc_info=True)
            raise

    async def get_max_pain_timeseries(
        self, symbol: str = "SPY", timeframe: str = "5min", window_units: int = 90
    ) -> List[Dict[str, Any]]:
        """Get max pain timeseries aggregated to timeframe over window units."""
        window_units = max(1, min(window_units, 300))
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        # `bucket` and `step_interval` are validated allowlist literals.
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
            ORDER BY timestamp DESC
            LIMIT $2
        """

        async with self._acquire_connection() as conn:
            rows = await conn.fetch(query, symbol, window_units)
            return [dict(row) for row in rows]

    async def get_max_pain_current(
        self, symbol: str = "SPY", strike_limit: int = 200
    ) -> Optional[Dict[str, Any]]:
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

        skip_inline_refresh = (
            self._max_pain_background_refresh_enabled
            and symbol in self._max_pain_background_refresh_symbols
        )
        async with self._acquire_connection() as conn:
            if not skip_inline_refresh:
                await self._refresh_max_pain_snapshot(conn, symbol, strike_limit)
            snapshot = await conn.fetchrow(snapshot_query, symbol)
            if not snapshot:
                return None

            expiration_rows = await conn.fetch(expiration_query, symbol, snapshot["as_of_date"])
            expirations: List[Dict[str, Any]] = []
            for row in expiration_rows:
                strikes = row["strikes"]
                if isinstance(strikes, str):
                    strikes = json.loads(strikes)
                expirations.append(
                    {
                        "expiration": row["expiration"],
                        "max_pain": row["max_pain"],
                        "difference_from_underlying": row["difference_from_underlying"],
                        "strikes": strikes or [],
                    }
                )

            result = {
                "timestamp": snapshot["timestamp"],
                "symbol": snapshot["symbol"],
                "underlying_price": snapshot["underlying_price"],
                "max_pain": snapshot["max_pain"],
                "difference": snapshot["difference"],
                "expirations": expirations,
            }
            self._cache_set(cache_key, result, self._analytics_cache_ttl_seconds)
            return result

    # ========================================================================
    # Chart Data Queries
    # ========================================================================

    async def get_gex_heatmap(
        self, symbol: str = "SPY", timeframe: str = "5min", window_units: int = 60
    ) -> List[Dict[str, Any]]:
        """
        Get GEX data by strike over time for heatmap visualization using interval + window units.

        Rows are ordered newest → oldest by ``timestamp`` (and ascending by
        ``strike`` within each timestamp), so ``rows[0]`` is from the most
        recent bucket.
        """
        symbol = symbol.upper()
        window_units = max(1, min(window_units, 300))
        cache_key = f"gex_heatmap:{symbol}:{timeframe}:{window_units}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        bucket = _bucket_expr(timeframe)
        step_interval = _interval_expr(timeframe)
        # `bucket` and `step_interval` are validated allowlist literals.
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
            ORDER BY timestamp DESC, strike ASC
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await asyncio.wait_for(conn.fetch(query, symbol, window_units), timeout=15.0)
                result = [dict(row) for row in rows]
                self._cache_set(cache_key, result, self._analytics_cache_ttl_seconds)
                return result
        except asyncio.TimeoutError:
            logger.warning(
                f"GEX heatmap query timed out for {symbol} timeframe={timeframe} window={window_units}, returning empty"
            )
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

    async def get_open_interest(self, underlying: str) -> Optional[Dict[str, Any]]:
        """Get the most recent OI snapshot + per-contract directional dollar exposure.

        Uses the stable-snapshot CTE to avoid returning an in-flight minute bucket
        that ingestion is still populating; see STABLE_SNAPSHOT_CTE for details.
        Returns one row per (strike, expiration, option_type) combination from the
        chosen snapshot, ordered by expiration then strike then option_type.
        """
        underlying = underlying.upper()
        query = f"""
            WITH {_STABLE_SNAPSHOT_CTE},
            latest_spot AS (
                SELECT close::numeric AS spot_price
                FROM underlying_quotes
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            )
            SELECT
                oc.timestamp,
                oc.underlying,
                oc.strike,
                oc.expiration,
                oc.option_type,
                oc.open_interest,
                (
                    CASE
                        WHEN oc.option_type = 'P' THEN -1
                        ELSE 1
                    END
                    * COALESCE(oc.gamma, 0)
                    * COALESCE(oc.open_interest, 0)
                    * 100
                    * COALESCE(ls.spot_price, 0)
                )::numeric AS exposure,
                oc.updated_at
            FROM option_chains oc
            JOIN latest_ts lt ON oc.timestamp = lt.ts
            CROSS JOIN latest_spot ls
            WHERE oc.underlying = $1
              AND oc.open_interest IS NOT NULL
            ORDER BY oc.expiration, oc.strike, oc.option_type
        """
        try:
            async with self._acquire_connection() as conn:
                spot_row = await conn.fetchrow(
                    """
                    SELECT close::numeric AS spot_price
                    FROM underlying_quotes
                    WHERE symbol = $1
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    underlying,
                )
                if not spot_row:
                    return None
                rows = await conn.fetch(query, underlying)
                if not rows:
                    return None
                return {
                    "underlying": underlying.upper(),
                    "spot_price": spot_row["spot_price"],
                    "contracts": [dict(row) for row in rows],
                }
        except Exception as e:
            logger.error(f"Error fetching open interest for {underlying}: {e}", exc_info=True)
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

        Rows are ordered newest → oldest so ``rows[0]`` is the most recent
        1-minute bar.
        """
        from datetime import time as _time

        expiration_date = datetime.strptime(expiration, "%Y-%m-%d").date()

        now_et = datetime.now(_ET)
        today = now_et.date()
        session_is_open = today.weekday() < 5 and _time(9, 30) <= now_et.time() < _time(16, 0)

        # Resolve option_symbol once, then drive everything else off the
        # (option_symbol, timestamp) primary key. Filtering option_chains by
        # (underlying, strike, expiration, option_type) directly forces the
        # planner onto (underlying, timestamp DESC) and re-checks the other
        # three columns against every row for that underlying in the window —
        # millions of rows for SPX, which trips the 30s statement_timeout.
        #
        # With ORDER BY timestamp DESC LIMIT 1 and the timestamp lower bound,
        # the planner walks the (underlying, timestamp DESC) index backward
        # and stops at the first row whose strike/expiration/option_type match
        # — cheap for any contract that's been quoted recently. The 14-day
        # floor bounds the worst case where the contract doesn't exist.
        resolve_query = """
            SELECT option_symbol, timestamp AS latest_ts
            FROM option_chains
            WHERE underlying = $1
              AND strike = $2
              AND expiration = $3
              AND option_type = $4
              AND timestamp >= NOW() - INTERVAL '14 days'
            ORDER BY timestamp DESC
            LIMIT 1
        """
        try:
            async with self._acquire_connection() as conn:
                resolved = await conn.fetchrow(
                    resolve_query, underlying, float(strike), expiration_date, option_type
                )
                if not resolved or resolved["option_symbol"] is None:
                    return []
                option_symbol = resolved["option_symbol"]
                latest_ts = resolved["latest_ts"]

                target_date = today if session_is_open else latest_ts.astimezone(_ET).date()

                # Compute the ET calendar day as an explicit UTC timestamptz
                # range. Computing day_end_et from (target_date + 1 day) rather
                # than +timedelta(days=1) keeps it correct across DST shifts.
                day_start_et = datetime.combine(target_date, _time(0, 0), tzinfo=_ET)
                day_end_et = datetime.combine(
                    target_date + timedelta(days=1), _time(0, 0), tzinfo=_ET
                )

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
                        WHERE option_symbol = $1
                          AND timestamp >= $2
                          AND timestamp < $3
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
                        -- volume_delta uses an explicit chronological window
                        -- (ORDER BY bar_ts ASC) so each bar's delta is the
                        -- increment over the prior bar regardless of how the
                        -- outer SELECT orders the result set.
                        GREATEST(
                            COALESCE(bar_volume, 0)
                                - COALESCE(LAG(bar_volume) OVER (ORDER BY bar_ts), 0),
                            0
                        )::bigint          AS volume_delta
                    FROM ranked
                    WHERE rn = 1
                    ORDER BY timestamp DESC
                """
                rows = await conn.fetch(query, option_symbol, day_start_et, day_end_et)
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

        chain_query = f"""
            WITH {_STABLE_SNAPSHOT_CTE},
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

                rows = await conn.fetch(chain_query, symbol, dte_max, spot_price, strike_count)
                return {
                    "spot_price": spot_price,
                    "timestamp": timestamp,
                    "rows": [dict(r) for r in rows],
                }
        except Exception as e:
            logger.error(f"Error fetching vol surface data: {e}", exc_info=True)
            raise
