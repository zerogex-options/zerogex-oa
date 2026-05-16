"""
Database manager for API queries
Uses asyncpg for async PostgreSQL operations
"""

import asyncio
import asyncpg
import os
import time as time_module
import traceback
from collections import OrderedDict
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta, date, time, timezone
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo
import logging
import json

from src.api.queries.signals import SignalsQueriesMixin
from src.api.queries.technicals import TechnicalsQueriesMixin
from src.config import GEX_HEATMAP_STRIKE_BAND_PCT
from src.flow_series_sql import FLOW_SERIES_CTE_ASYNCPG, SNAPSHOT_SELECT_ASYNCPG
from src.market_calendar import NYSE_HOLIDAYS
from src.symbols import is_cash_index

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


def _expected_flow_series_bars(session_start: datetime, session_end: datetime) -> int:
    """Number of 5-minute bars the CTE's generate_series would emit for a
    resolved window — inclusive of both ends (the window is always a 5-min
    multiple). Used only to size the snapshot shortfall warning; it is not
    a hard gate (an empty session legitimately yields zero rows)."""
    span = (session_end - session_start).total_seconds()
    if span < 0:
        return 0
    return int(span // 300) + 1


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
        # Fraction of spot used as the /api/gex/heatmap strike half-band
        # (validated + bounded in config). Proportional so the heatmap
        # fills the frontend's price-cropped y-axis for any underlying.
        self._gex_heatmap_strike_band_pct: float = GEX_HEATMAP_STRIKE_BAND_PCT
        # Flow endpoints are frequently polled by the frontend. A short TTL
        # dramatically cuts repeated heavy reads while keeping intraday charts
        # effectively real-time.
        self._flow_endpoint_cache_ttl_seconds: float = float(
            os.getenv("FLOW_ENDPOINT_CACHE_TTL_SECONDS", "3.0")
        )
        # /api/flow/series gets its own, longer TTL. Its unfiltered read
        # is a flow_series_5min snapshot the Analytics Engine only
        # rewrites ~once per cycle (~60s), so the shared 3s flow TTL just
        # forces redundant snapshot reads; and the live tail is polled
        # via intervals=N, which bypasses this cache entirely (see
        # use_cache in get_flow_series), so a longer full-series TTL
        # never stales the updating number. It also amortises the
        # strike/expiration-filtered CTE (measured 6-26x the snapshot).
        # by-contract/contracts deliberately keep the shared TTL above.
        # <= 0 disables endpoint caching.
        self._flow_series_endpoint_cache_ttl_seconds: float = float(
            os.getenv("FLOW_SERIES_ENDPOINT_CACHE_TTL_SECONDS", "30.0")
        )
        # Phase-2 read switch for the flow_series_5min snapshot. When true,
        # unfiltered /api/flow/series reads the pre-aggregated snapshot
        # instead of running the 8-CTE pipeline; filtered calls always use
        # the CTE (the snapshot is keyed (symbol, bar_start) only). Default
        # off: ship schema + write path + backfill, verify a session's
        # rows match the live CTE, only then flip this on.
        self._flow_series_use_snapshot: bool = os.getenv(
            "FLOW_SERIES_USE_SNAPSHOT", "false"
        ).strip().lower() in {"true", "1", "yes", "on"}
        # Confluence-matrix is structurally an aggregate over the rolling
        # ``lookback`` window of signal_scores × signal_component_scores; the
        # underlying values only change on the scoring cycle.  The per-worker
        # in-memory cache is the only thing that keeps a multi-worker
        # uvicorn from re-hitting the DB on every poll, so a longer TTL is
        # worth the staleness — and 5 s (the analytics default) frequently
        # expires inside a single cold call's wall-clock and gives no benefit.
        self._confluence_matrix_cache_ttl_seconds: float = float(
            os.getenv("CONFLUENCE_MATRIX_CACHE_TTL_SECONDS", "60.0")
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
            for s in os.getenv("MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS", "SPY,SPX,QQQ").split(",")
            if s.strip()
        )
        # /api/max-pain/current returns a daily OI snapshot that only
        # changes every MAX_PAIN_BACKGROUND_REFRESH_INTERVAL_SECONDS (the
        # background loop for listed symbols; the heavy inline recompute
        # for non-listed ones).  Sharing the 5 s analytics TTL forced a
        # DB round-trip ~every 5 s for data that moves ~every 5 min, and
        # each of those reads competes for the small pool with the heavy
        # background recompute — the head-of-line stall behind that
        # recompute is what made the endpoint take ~9 s.  A dedicated,
        # longer TTL keeps the request path a pure in-process cache hit
        # between snapshot refreshes.  <= 0 disables endpoint caching.
        self._max_pain_current_cache_ttl_seconds: float = float(
            os.getenv("MAX_PAIN_CURRENT_CACHE_TTL_SECONDS", "120.0")
        )
        # Bounded LRU + TTL. Keys like option_symbol:* / flow_series:* have an
        # effectively unbounded keyspace (per strike-set/expiration-set query
        # string); a plain dict only evicted a key when that exact key was
        # re-requested after expiry, so cold keys accumulated forever and the
        # worker RSS grew without bound over a trading session. Capping size
        # and evicting oldest/expired keeps memory bounded — the cache is a
        # pure latency optimization so eviction can never affect correctness.
        self._read_cache_maxsize: int = max(
            64, int(os.getenv("READ_CACHE_MAXSIZE", "2048"))
        )
        self._read_cache: "OrderedDict[str, Tuple[float, Any]]" = OrderedDict()
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
        # Mark as most-recently-used for LRU eviction.
        self._read_cache.move_to_end(key)
        return payload

    def _cache_set(self, key: str, payload: Any, ttl_seconds: float) -> None:
        """Store a value in the short-lived, bounded in-memory read cache."""
        if ttl_seconds <= 0:
            return
        now = time_module.monotonic()
        self._read_cache[key] = (now + ttl_seconds, payload)
        self._read_cache.move_to_end(key)
        if len(self._read_cache) > self._read_cache_maxsize:
            # Opportunistically drop already-expired entries first; if still
            # over capacity, evict least-recently-used until within bound.
            for k in list(self._read_cache.keys()):
                if self._read_cache[k][0] <= now:
                    self._read_cache.pop(k, None)
            while len(self._read_cache) > self._read_cache_maxsize:
                self._read_cache.popitem(last=False)

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
        #
        # TimeoutError is a subclass of OSError on Python 3.10+, so the
        # OSError fallback below has to explicitly exclude it — otherwise
        # the bare-TimeoutError case from a statement timeout would be
        # reclassified as transient and trigger exactly the storm above.
        if isinstance(error, TimeoutError):
            return False
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

        Cache ownership: ``flow_by_contract`` and ``flow_smart_money`` are
        normally written by the analytics engine
        (``src/analytics/main_engine.py:_refresh_flow_caches``) on its
        cycle interval (default 60s).  This API-side path is a per-request
        backstop that fills in gaps when the analytics engine is
        catching up after a restart, or when a hot symbol is polled
        between analytics cycles.  The two refreshes are idempotent
        upserts on the same primary key, so concurrent runs are safe;
        the analytics engine remains the steady-state writer.

        Set ``ANALYTICS_FLOW_CACHE_REFRESH_ENABLED=false`` to disable
        the analytics-side write and let this path handle 100% of
        cache freshness (useful when a single API process serves all
        flow reads and the analytics engine doesn't need the cache).
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
        self,
        conn: asyncpg.Connection,
        symbol: str,
        strike_limit: int,
        timeout: Optional[float] = None,
    ) -> None:
        """Refresh daily max pain OI snapshot for the symbol if latest chain timestamp changed.

        ``timeout`` (seconds) overrides the asyncpg pool's default
        ``command_timeout`` for this refresh.  Required for the background
        path where the heavy multi-CTE recompute legitimately exceeds the
        pool's default 30 s client-side timeout — without an override the
        ``SET LOCAL statement_timeout`` set by the caller is masked by
        asyncpg cancelling the call client-side first.  ``None`` keeps
        the pool default (used on the inline / on-demand path).
        """
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
        await conn.execute(query, symbol, strike_limit, timeout=timeout)

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
        await conn.execute(sync_expirations_query, symbol, timeout=timeout)

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
        # asyncpg enforces a *client-side* command_timeout (default 30 s on
        # this pool — see _create_pool).  ``SET LOCAL statement_timeout``
        # only relaxes the server-side cancel, so without a matching
        # per-call ``timeout=`` argument asyncpg would still abort the
        # recompute at 30 s and the background refresh would fail for any
        # symbol whose CTE chain runs longer (e.g. SPX / QQQ during cash
        # session).  Pass the same budget down to both layers so they
        # agree.
        statement_timeout_s = max(1.0, statement_timeout_ms / 1000.0)
        for symbol in symbols:
            symbol_upper = symbol.upper()
            try:
                async with self._acquire_connection() as conn:
                    async with conn.transaction():
                        await conn.execute(
                            f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}"
                        )
                        await self._refresh_max_pain_snapshot(
                            conn,
                            symbol_upper,
                            strike_limit,
                            timeout=statement_timeout_s,
                        )
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

        # Call/Put Walls are persisted to ``gex_summary`` by the Analytics
        # Engine using the canonical definition in
        # :mod:`src.analytics.walls` — strike with maximum dollar gamma
        # exposure on the appropriate side of spot.  We read those columns
        # directly so this endpoint, ``/api/gex/history``, the unified signal
        # engine, and every playbook pattern all agree on the same values.
        #
        # ``gex_summary.call_wall`` / ``put_wall`` can be NULL on rows written
        # before the column backfill; the fallback CTE recomputes them via the
        # same canonical formula straight from ``gex_by_strike`` so historical
        # latest-summary lookups never lose the wall.
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
                    gs.total_net_gex,
                    gs.call_wall AS stored_call_wall,
                    gs.put_wall  AS stored_put_wall
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
            strike_totals AS (
                SELECT
                    -- Industry-standard dollar GEX per 1% move:
                    -- γ × OI × 100 × S² × 0.01.  Call/put gamma are already
                    -- OI-weighted in the analytics writer.
                    COALESCE(SUM(gbs.call_gamma * 100 * lq.spot_price * lq.spot_price * 0.01), 0)::numeric AS total_call_gex,
                    COALESCE(SUM(-1 * gbs.put_gamma * 100 * lq.spot_price * lq.spot_price * 0.01), 0)::numeric AS total_put_gex
                FROM gex_by_strike gbs
                JOIN latest_summary ls
                  ON gbs.underlying = ls.underlying
                 AND gbs.timestamp = ls.timestamp
                JOIN latest_quote lq ON TRUE
            ),
            -- Canonical Call Wall fallback: max call-gamma strike at-or-above
            -- spot, tiebreaker nearest-to-spot (lowest strike above spot).
            -- Only used when gex_summary.call_wall is NULL.
            fallback_call_wall AS (
                SELECT gbs.strike::numeric AS call_wall
                FROM gex_by_strike gbs
                JOIN latest_summary ls
                  ON gbs.underlying = ls.underlying
                 AND gbs.timestamp = ls.timestamp
                JOIN latest_quote lq ON TRUE
                WHERE gbs.strike >= lq.spot_price
                  AND COALESCE(gbs.call_gamma, 0) > 0
                ORDER BY gbs.call_gamma DESC, gbs.strike ASC
                LIMIT 1
            ),
            -- Canonical Put Wall fallback: max put-gamma strike at-or-below
            -- spot, tiebreaker nearest-to-spot (highest strike below spot).
            fallback_put_wall AS (
                SELECT gbs.strike::numeric AS put_wall
                FROM gex_by_strike gbs
                JOIN latest_summary ls
                  ON gbs.underlying = ls.underlying
                 AND gbs.timestamp = ls.timestamp
                JOIN latest_quote lq ON TRUE
                WHERE gbs.strike <= lq.spot_price
                  AND COALESCE(gbs.put_gamma, 0) > 0
                ORDER BY gbs.put_gamma DESC, gbs.strike DESC
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
                COALESCE(ls.stored_call_wall, fcw.call_wall) AS call_wall,
                COALESCE(ls.stored_put_wall,  fpw.put_wall)  AS put_wall,
                ls.total_call_oi,
                ls.total_put_oi,
                ls.put_call_ratio
            FROM latest_summary ls
            JOIN latest_quote lq ON TRUE
            JOIN strike_totals st ON TRUE
            LEFT JOIN fallback_call_wall fcw ON TRUE
            LEFT JOIN fallback_put_wall  fpw ON TRUE
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
                    gs.call_wall AS stored_call_wall,
                    gs.put_wall  AS stored_put_wall,
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
            -- Canonical Call/Put Wall fallback (matches src/analytics/walls.py):
            -- Call Wall = strike >= spot with max call_gamma, ties → lowest strike.
            -- Put  Wall = strike <= spot with max put_gamma,  ties → highest strike.
            -- Only used for buckets where ``gex_summary.call_wall`` /
            -- ``put_wall`` is NULL (i.e., rows persisted before the column
            -- backfill).  All new analytics runs write the canonical value
            -- straight into ``gex_summary``.
            call_walls AS (
                SELECT DISTINCT ON (gbs.timestamp)
                    gbs.timestamp,
                    gbs.strike::numeric AS call_wall
                FROM gex_by_strike gbs
                CROSS JOIN spot s
                WHERE gbs.underlying = $1
                  AND gbs.timestamp IN (SELECT timestamp FROM base)
                  AND gbs.strike >= s.spot_price
                  AND COALESCE(gbs.call_gamma, 0) > 0
                ORDER BY gbs.timestamp, gbs.call_gamma DESC, gbs.strike ASC
            ),
            put_walls AS (
                SELECT DISTINCT ON (gbs.timestamp)
                    gbs.timestamp,
                    gbs.strike::numeric AS put_wall
                FROM gex_by_strike gbs
                CROSS JOIN spot s
                WHERE gbs.underlying = $1
                  AND gbs.timestamp IN (SELECT timestamp FROM base)
                  AND gbs.strike <= s.spot_price
                  AND COALESCE(gbs.put_gamma, 0) > 0
                ORDER BY gbs.timestamp, gbs.put_gamma DESC, gbs.strike DESC
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
                COALESCE(b.stored_call_wall, cw.call_wall) AS call_wall,
                COALESCE(b.stored_put_wall,  pw.put_wall)  AS put_wall,
                b.total_call_oi,
                b.total_put_oi,
                b.put_call_ratio
            FROM base b
            CROSS JOIN spot s
            LEFT JOIN strike_agg sa ON sa.timestamp = b.timestamp
            LEFT JOIN call_walls cw ON cw.timestamp = b.timestamp
            LEFT JOIN put_walls  pw ON pw.timestamp = b.timestamp
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
                        self._cache_set(cache_key, [], self._flow_series_endpoint_cache_ttl_seconds)
                    return []

                if (
                    self._flow_series_use_snapshot
                    and strikes_arg is None
                    and expirations_arg is None
                ):
                    # Phase-2 snapshot read. The 8-CTE pipeline has been
                    # pre-materialised per (symbol, bar_start) by the
                    # Analytics Engine; closed bars are window-invariant
                    # (outer window is ROWS UNBOUNDED PRECEDING ORDER BY
                    # bar_start) so this is byte-identical to the CTE for
                    # the same resolved window. Filtered calls never reach
                    # here — the snapshot is keyed (symbol, bar_start) only
                    # and can't answer per-strike/expiration questions.
                    rows = await asyncio.wait_for(
                        conn.fetch(
                            SNAPSHOT_SELECT_ASYNCPG,
                            symbol,
                            session_start,
                            session_end,
                        ),
                        timeout=15.0,
                    )
                    expected = _expected_flow_series_bars(session_start, session_end)
                    # The API sizes `expected` off the wall clock
                    # (session_end = floor(now()/5min)), but the snapshot
                    # is only as fresh as the last Analytics Engine cycle
                    # (ANALYTICS_INTERVAL, ~60s). For up to one cycle after
                    # every 5-min boundary a live session is structurally
                    # one bar ahead of any snapshot the engine could have
                    # written — a guaranteed, self-healing 1-bar shortfall
                    # that is NOT an engine fault. Tolerate exactly that
                    # lag for live ('current') sessions; closed ('prior')
                    # sessions have no such race and stay strict so a real
                    # gap still alerts.
                    lag_tolerance = 1 if session == "current" else 0
                    if 0 < len(rows) < expected - lag_tolerance:
                        # A shortfall beyond the structural cadence lag
                        # means the Analytics Engine actually missed
                        # cycles or backfill hasn't run for this session.
                        # Engine-health alert, not an API fallback — a CTE
                        # fallback would reintroduce the heavy scan exactly
                        # when the system is degraded.
                        logger.warning(
                            "flow_series_5min shortfall for %s %s: %d rows, "
                            "expected up to %d for window [%s, %s]",
                            symbol,
                            session,
                            len(rows),
                            expected,
                            session_start.isoformat(),
                            session_end.isoformat(),
                        )
                else:
                    rows = await asyncio.wait_for(
                        conn.fetch(
                            FLOW_SERIES_CTE_ASYNCPG,
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
                    self._cache_set(cache_key, result, self._flow_series_endpoint_cache_ttl_seconds)
                return result
        except asyncio.TimeoutError:
            logger.warning(f"Flow series query timed out for {symbol}, returning empty")
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
            self._cache_set(cache_key, result, self._max_pain_current_cache_ttl_seconds)
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

        # Cash indices (SPX, NDX, RUT, …) have no underlying price/volume of
        # their own outside the regular cash session, yet their options
        # trade extended / global hours so the analytics engine writes
        # gex_summary / gex_by_strike around the clock.  Returning those
        # extended-hours and overnight buckets makes the heatmap plot
        # nonsensical 17:00–19:00 ET cells for an index and misaligns the
        # surface with the RTH-only candlesticks.  For cash indices,
        # restrict the per-bucket representatives to the regular session
        # (weekdays, 09:30–16:00 ET, excluding NYSE holidays) — the same
        # session definition get_session_closes uses.  ETFs / equities
        # (SPY, QQQ, …) genuinely trade extended hours, so they keep the
        # original query and params unchanged.
        session_filter = ""
        params: list = [symbol, window_units]
        if is_cash_index(symbol):
            session_filter = """
                    AND EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') BETWEEN 1 AND 5
                    AND (timestamp AT TIME ZONE 'America/New_York')::time
                        BETWEEN TIME '09:30' AND TIME '16:00'
                    AND (timestamp AT TIME ZONE 'America/New_York')::date <> ALL($3::date[])
            """
            params.append(sorted(NYSE_HOLIDAYS))
        # Strike half-band around spot, proportional for every underlying
        # so the colored surface fills the frontend's price-cropped y-axis
        # at any price level. A fixed ±50 was ≈±8.5% of a ~$585 SPY but
        # only ≈±0.7% of a ~$7400 index, which collapsed the index heatmap
        # into a thin strip. band_pct is a config-validated float bounded
        # to [0.005, 0.5] (GEX_HEATMAP_STRIKE_BAND_PCT) and formatted as a
        # plain decimal literal — no user input — so it is safe to
        # interpolate alongside the other validated fragments below.
        band_pct = self._gex_heatmap_strike_band_pct
        strike_band = f"(SELECT spot_close FROM latest_quote) * {band_pct:g}"
        # `bucket` and `step_interval` are validated allowlist literals.
        #
        # Perf: a true per-bucket AVG over raw gex_by_strike requires
        # reading every snapshot in the window.  gex_by_strike is the
        # highest-cardinality table (the analytics engine writes one row
        # per strike×expiration every ~60s), so for timeframe=1day,
        # window_units=N (an N-DAY window) that scan is tens of millions
        # of rows -> ~14s, regardless of how few strikes survive a
        # post-scan filter.
        #
        # Instead, pick ONE representative (latest) snapshot per bucket
        # from the lightweight gex_summary (one row per analytics cycle,
        # written in the SAME transaction as gex_by_strike), then read
        # gex_by_strike only AT those ~window_units timestamps.  This is
        # the same pattern get_historical_gex uses; it bounds the
        # expensive per-strike read to a handful of timestamps instead of
        # the whole window.  Heatmap cells are therefore the GEX-by-strike
        # surface at each bucket's close (point-in-time) rather than an
        # average across every snapshot in the bucket -- consistent with
        # the historical/summary endpoints.  Only the TIME aggregation
        # changed: the cross-expiration combination per (bucket, strike)
        # is still AVG, and the spot±50 band filter is unchanged, so cell
        # magnitudes stay comparable to before.
        query = f"""
            WITH latest_quote AS (
                SELECT timestamp AS max_ts, close AS spot_close
                FROM underlying_quotes
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            time_window AS (
                SELECT
                    max_ts - ({step_interval} * ($2 - 1)) as start_time,
                    max_ts as end_time
                FROM latest_quote
            ),
            bucket_reps AS (
                SELECT DISTINCT ON ({bucket})
                    {bucket} AS bucket_ts,
                    timestamp AS rep_ts,
                    gamma_flip_point AS gamma_flip
                FROM gex_summary
                WHERE underlying = $1
                    AND timestamp >= (SELECT start_time FROM time_window)
                    AND timestamp <= (SELECT end_time FROM time_window){session_filter}
                ORDER BY {bucket}, timestamp DESC
            )
            SELECT
                br.bucket_ts AS timestamp,
                g.strike,
                AVG(g.net_gex) AS net_gex,
                -- gamma_flip is one value per bucket (the bucket's
                -- representative gex_summary row), but rows here are
                -- per-strike.  MAX() collapses that constant, and the
                -- CASE emits it on only the lowest-strike row of each
                -- bucket (NULL elsewhere) so it isn't repeated across
                -- every strike.  The frontend keys the gamma-flip line
                -- by timestamp and skips NULLs, so one row per bucket is
                -- enough — and because it now rides the heatmap's own
                -- (RTH-filtered, over-fetched) timestamps the line spans
                -- the full surface instead of the short /api/gex/historical
                -- fallback window.
                CASE
                    WHEN g.strike = MIN(g.strike) OVER (PARTITION BY br.bucket_ts)
                    THEN MAX(br.gamma_flip)
                END AS gamma_flip
            FROM bucket_reps br
            JOIN gex_by_strike g
                ON g.underlying = $1
               AND g.timestamp = br.rep_ts
            WHERE ABS(g.strike - (SELECT spot_close FROM latest_quote)) <= {strike_band}
            GROUP BY br.bucket_ts, g.strike
            ORDER BY br.bucket_ts DESC, g.strike ASC
        """

        try:
            async with self._acquire_connection() as conn:
                rows = await asyncio.wait_for(conn.fetch(query, *params), timeout=15.0)
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
        # exposure_ts: the most recent snapshot at or before the stable
        # `latest_ts` that actually has Greeks populated (gamma IS NOT NULL).
        # The stable CTE picks the absolute latest minute-bucket; over
        # weekends/holidays the feed winds down and the *terminal* bucket
        # carries strikes + open_interest but NULL gamma (the underlying
        # price aged out, so Greeks were skipped). Anchoring on that bucket
        # makes COALESCE(gamma, 0) zero every contract's exposure. Falling
        # back to the last gamma-bearing snapshot is an index-only probe of
        # idx_option_chains_underlying_ts_gamma (partial, gamma IS NOT NULL),
        # so it stays O(1) and cannot reintroduce the statement_timeout that
        # the reverted 7-day-DISTINCT retention rewrite caused. If gamma is
        # NULL across *all* of history the COALESCE degrades to the stable
        # `latest_ts` so open interest still renders (exposure 0) — never
        # less than the pre-fix behavior.
        query = f"""
            WITH {_STABLE_SNAPSHOT_CTE},
            exposure_ts AS (
                SELECT COALESCE(
                    (
                        SELECT oc.timestamp
                        FROM option_chains oc
                        CROSS JOIN latest_ts lt
                        WHERE oc.underlying = $1
                          AND oc.timestamp <= lt.ts
                          AND oc.gamma IS NOT NULL
                        ORDER BY oc.timestamp DESC
                        LIMIT 1
                    ),
                    (SELECT ts FROM latest_ts)
                ) AS ts
            ),
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
            JOIN exposure_ts et ON oc.timestamp = et.ts
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

        On a trading day at or after 09:30 ET (a weekday that is not a
        configured NYSE holiday) returns the current day's session data.
        Otherwise — before the 09:30 ET open, or on a non-trading day
        (weekend / NYSE holiday) — returns data for the most recent cash
        session prior to now (the nearest earlier trading day, by the
        calendar — not the latest timestamp in the DB, which during 24x5
        ingestion would be the current day's thin pre-market rows).

        Rows are ordered newest → oldest so ``rows[0]`` is the most recent
        1-minute bar.

        Caching strategy: the two-stage (resolve symbol + fetch bars) design
        is intentional, so the remaining win is caching the immutable parts.
        Closed minute bars never change once their minute boundary passes, and
        whole-day bar lists for any past session are immutable until ingestion
        rewrites them (which doesn't happen). We therefore cache:

        1. The (underlying, strike, expiration, option_type) → option_symbol
           mapping, which is stable for the life of the contract.
        2. For non-live targets (after-hours or prior session): the entire
           bar list, keyed by (option_symbol, target_date), with a long TTL.
        3. For the live session: only the bars strictly before the current
           minute. Each call re-fetches just the window starting at the last
           cached minute (kept as a LAG seed so volume_delta stays correct
           on the first newly-fetched bar) and merges with cached closed bars.
        """
        from datetime import time as _time

        expiration_date = datetime.strptime(expiration, "%Y-%m-%d").date()

        now_et = datetime.now(_ET)
        today = now_et.date()
        # A trading day is a weekday that is not a configured NYSE holiday.
        is_trading_day = today.weekday() < 5 and today not in NYSE_HOLIDAYS
        # Serve the current session once it has opened (09:30 ET) and for
        # the rest of that calendar day. Before the open, or on a
        # non-trading day, fall back to the most recent cash session prior
        # to now (resolved from the calendar below).
        use_current_session = is_trading_day and now_et.time() >= _time(9, 30)
        # Bars are only still being written while the regular session is
        # live; after 16:00 ET the day's data is immutable and can use the
        # whole-day cache path below.
        market_live = is_trading_day and _time(9, 30) <= now_et.time() < _time(16, 0)

        # Stage 1 — resolve option_symbol (cached). The mapping
        # (underlying, strike, expiration, option_type) → option_symbol is
        # deterministic per OCC contract spec, so a long TTL is safe.
        # Negative results (no contract quoted in the last 14 days) are NOT
        # cached so a contract that starts trading mid-session resolves on the
        # next call.
        symbol_cache_key = (
            f"option_symbol:{underlying}:{float(strike):.4f}"
            f":{expiration_date.isoformat()}:{option_type}"
        )
        resolved_cached = self._cache_get(symbol_cache_key)
        if resolved_cached is not None:
            option_symbol = resolved_cached
        else:
            # Resolve option_symbol once, then drive everything else off the
            # (option_symbol, timestamp) primary key. Filtering option_chains
            # by (underlying, strike, expiration, option_type) directly forces
            # the planner onto (underlying, timestamp DESC) and re-checks the
            # other three columns against every row for that underlying in
            # the window — millions of rows for SPX, which trips the 30s
            # statement_timeout.
            #
            # With ORDER BY timestamp DESC LIMIT 1 and the timestamp lower
            # bound, the planner walks the (underlying, timestamp DESC)
            # index backward and stops at the first row whose
            # strike/expiration/option_type match — cheap for any contract
            # that's been quoted recently. The 14-day floor bounds the worst
            # case where the contract doesn't exist.
            resolve_query = """
                SELECT option_symbol
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
                        resolve_query,
                        underlying,
                        float(strike),
                        expiration_date,
                        option_type,
                    )
            except Exception as e:
                logger.error(f"Error fetching option contract history: {e}", exc_info=True)
                raise

            if not resolved or resolved["option_symbol"] is None:
                return []
            option_symbol = resolved["option_symbol"]
            self._cache_set(symbol_cache_key, option_symbol, 3600.0)

        if use_current_session:
            target_date = today
        else:
            # Most recent cash session prior to now: walk back from today
            # to the nearest earlier weekday that isn't a configured NYSE
            # holiday. This branch is only reached before the 09:30 ET open
            # or on a non-trading day, so today's own session is never the
            # answer — the prior trading day always is.
            prior = today - timedelta(days=1)
            while prior.weekday() >= 5 or prior in NYSE_HOLIDAYS:
                prior -= timedelta(days=1)
            target_date = prior

        # Compute the ET calendar day as an explicit UTC timestamptz range.
        # Computing day_end_et from (target_date + 1 day) rather than
        # +timedelta(days=1) keeps it correct across DST shifts.
        day_start_et = datetime.combine(target_date, _time(0, 0), tzinfo=_ET)
        day_end_et = datetime.combine(target_date + timedelta(days=1), _time(0, 0), tzinfo=_ET)

        is_live = market_live and target_date == today

        # Non-live path: the whole bar list is immutable for the duration of
        # the cache entry, so cache it wholesale.
        if not is_live:
            full_cache_key = f"option_contract_full:{option_symbol}:{target_date.isoformat()}"
            cached_full = self._cache_get(full_cache_key)
            if cached_full is not None:
                return cached_full
            rows = await self._fetch_option_contract_bars(option_symbol, day_start_et, day_end_et)
            self._cache_set(full_cache_key, rows, 3600.0)
            return rows

        # Live path: cache closed bars and only re-fetch from the last cached
        # minute forward.
        closed_cache_key = f"option_contract_closed:{option_symbol}:{target_date.isoformat()}"
        cached_closed = self._cache_get(closed_cache_key)
        if cached_closed is not None:
            cached_bars_asc = cached_closed["bars"]
            last_cached_minute = cached_closed["last_minute"]
        else:
            cached_bars_asc = []
            last_cached_minute = None

        # Lower bound: include last_cached_minute so the SQL LAG() has the
        # prior bar's volume when computing volume_delta for the first
        # newly-fetched bar. The seed row is then dropped from the response.
        live_lower_bound = last_cached_minute if last_cached_minute is not None else day_start_et
        fetched = await self._fetch_option_contract_bars(
            option_symbol, live_lower_bound, day_end_et
        )

        if last_cached_minute is not None:
            fresh = [b for b in fetched if b["timestamp"] > last_cached_minute]
        else:
            fresh = fetched

        # Promote bars whose minute has already closed into the cache. Use
        # the current UTC minute boundary; bar timestamps come back as
        # tz-aware UTC datetimes from asyncpg.
        current_minute_floor = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        newly_closed_asc = sorted(
            (b for b in fresh if b["timestamp"] < current_minute_floor),
            key=lambda b: b["timestamp"],
        )
        if newly_closed_asc:
            updated_bars_asc = cached_bars_asc + newly_closed_asc
            self._cache_set(
                closed_cache_key,
                {
                    "bars": updated_bars_asc,
                    "last_minute": updated_bars_asc[-1]["timestamp"],
                },
                3600.0,
            )

        # Response is newest-first: freshly-fetched window (already DESC)
        # followed by the cached closed bars in reverse (oldest stored asc).
        return fresh + list(reversed(cached_bars_asc))

    async def _fetch_option_contract_bars(
        self,
        option_symbol: str,
        window_start: datetime,
        window_end: datetime,
    ) -> List[Dict[str, Any]]:
        """Run the 1-minute bar aggregation CTE for a single option contract.

        Returned rows are newest-first. ``volume_delta`` is computed via
        SQL LAG() over ``bar_ts ASC`` within the supplied window, so callers
        that wish to incrementally extend a prior result must include the
        last already-known bar as the lower bound to seed LAG correctly.
        """
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
        try:
            async with self._acquire_connection() as conn:
                rows = await conn.fetch(query, option_symbol, window_start, window_end)
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

        # iv_ts: the most recent snapshot at or before the stable
        # `latest_ts` that actually has implied_volatility populated. The
        # stable CTE picks the absolute latest minute-bucket; over
        # weekends/holidays the feed winds down and the *terminal* bucket
        # carries strikes + open_interest but NULL implied_volatility
        # (closing quotes collapse, so the IV solver has no bid/ask/last to
        # work from). Anchoring on that bucket yields an all-NULL surface
        # ("API returned strikes, but all IV values are null"). Falling
        # back to the last IV-bearing snapshot walks `latest_ts` backwards
        # over only the few degraded trailing buckets via
        # idx_option_chains_underlying_timestamp, so it stays bounded and
        # cannot reintroduce the statement_timeout that the reverted
        # 7-day-DISTINCT retention rewrite caused. If implied_volatility is
        # NULL across *all* of history the COALESCE degrades to the stable
        # `latest_ts` so the endpoint still returns strikes (the frontend's
        # "all IV null" notice) instead of a hard 404 — never less than the
        # pre-fix behavior.
        chain_query = f"""
            WITH {_STABLE_SNAPSHOT_CTE},
            iv_ts AS (
                SELECT COALESCE(
                    (
                        SELECT oc.timestamp
                        FROM option_chains oc
                        CROSS JOIN latest_ts lt
                        WHERE oc.underlying = $1
                          AND oc.timestamp <= lt.ts
                          AND oc.implied_volatility IS NOT NULL
                        ORDER BY oc.timestamp DESC
                        LIMIT 1
                    ),
                    (SELECT ts FROM latest_ts)
                ) AS ts
            ),
            eligible_strikes AS (
                SELECT strike
                FROM (
                    SELECT DISTINCT strike
                    FROM option_chains, iv_ts
                    WHERE underlying = $1
                      AND timestamp = iv_ts.ts
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
            CROSS JOIN iv_ts
            JOIN eligible_strikes es ON es.strike = oc.strike
            WHERE oc.underlying = $1
              AND oc.timestamp = iv_ts.ts
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
