"""
ZeroGEX Main Ingestion Engine

This engine:
1. Streams real-time data using StreamManager
2. Handles 1-minute aggregation
3. Calculates Greeks for options (if enabled)
4. Stores data in PostgreSQL/TimescaleDB
5. Monitors data quality and pipeline health
"""

import os
import signal
import sys
import hashlib
import json
import threading
import time
import time as _time
from multiprocessing import Process
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import defaultdict
import pytz
from psycopg2.extras import execute_values

from src.ingestion.tradestation_client import TradeStationClient
from src.ingestion.stream_manager import StreamManager
from src.ingestion.greeks_calculator import GreeksCalculator
from src.database import db_connection, close_connection_pool
from src.utils import get_logger
from src.validation import bucket_timestamp, is_engine_run_window, seconds_until_engine_run_window
from src.symbols import parse_underlyings, get_canonical_symbol
from src.config import (
    AGGREGATION_BUCKET_SECONDS,
    MAX_BUFFER_SIZE,
    BUFFER_FLUSH_INTERVAL,
    GREEKS_ENABLED,
    INGEST_PARITY_GUARD_ENABLED,
    OPTION_BUCKET_WRITE_MIN_SECONDS,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


def _to_db_float(value: Any) -> Optional[float]:
    """Convert numeric-like values (including numpy scalars) to plain float for DB writes."""
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:  # NaN check
        return None
    return parsed


class IngestionEngine:
    """
    Main ingestion engine - forward-only streaming with storage

    StreamManager fetches data, IngestionEngine stores it.
    """

    def __init__(
        self,
        client: TradeStationClient,
        underlying: str = "SPY",
        num_expirations: int = 3,
        num_strikes: int = 10,
    ):
        """Initialize main ingestion engine"""
        self.client = client
        self.underlying = underlying.upper()         # TradeStation API symbol (e.g. "$SPX.X")
        self.db_symbol = get_canonical_symbol(self.underlying)  # canonical alias for DB (e.g. "SPX")
        self.num_expirations = num_expirations
        self.num_strikes = num_strikes

        self.running = False

        # Buffering for options only (underlying writes every update)
        self.underlying_buffer: List[Dict[str, Any]] = []
        self.options_buffer: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        # Per-contract cumulative-volume baseline used to turn TradeStation's
        # monotonically-increasing Volume field into per-bucket deltas.
        # Entries are (baseline_value, monotonic_timestamp). The monotonic
        # timestamp drives a TTL so the cache self-heals after any external
        # DB write (data-retention sweeps, backfills, failed writes that
        # left the cache ahead of the DB, etc.).
        self._option_volume_baseline: Dict[str, tuple[int, float]] = {}
        self._option_volume_baseline_lock = threading.Lock()
        self._option_volume_baseline_ttl = float(
            os.getenv("OPTION_VOLUME_BASELINE_TTL_SECONDS", "1800")
        )

        # Track latest underlying price for Greeks calculation
        self.latest_underlying_price: Optional[float] = None

        # Greeks calculator (initialize if enabled)
        self.greeks_calculator = None
        if GREEKS_ENABLED:
            self.greeks_calculator = GreeksCalculator()
            logger.info("✅ Greeks calculation ENABLED")
            logger.info("   Note: Will use mid-price for IV calculation if API doesn't provide IV")
        else:
            logger.info("⚠️  Greeks calculation DISABLED (set GREEKS_ENABLED=true to enable)")

        # Metrics
        self.underlying_bars_stored = 0
        self.option_quotes_stored = 0
        self.greeks_calculated = 0
        self.last_flush_time = datetime.now(ET)
        self.errors_count = 0

        # Observability: write-path performance counters (reset on log).
        self._obs_batches_written = 0
        self._obs_rows_written = 0
        self._obs_write_time_ms = 0.0
        self._obs_last_log = _time.monotonic()

        # Circuit breaker: stop hammering a dead database.
        self._db_consecutive_failures = 0
        self._db_backoff_until = 0.0  # monotonic timestamp
        self._last_underlying_signature: Optional[str] = None
        self._option_bucket_last_write: Dict[tuple[str, datetime], float] = {}

        logger.info(f"Initialized IngestionEngine for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, {num_strikes} strikes each side")

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Initialize database
        self._initialize_database()
        self._ensure_symbol_exists()

    def _infer_asset_type(self, symbol: str) -> str:
        """Infer a sensible asset type for symbols table bootstrap."""
        if symbol.startswith("$"):
            return "INDEX"
        if symbol in {"SPY", "QQQ", "IWM", "DIA"}:
            return "ETF"
        return "EQUITY"

    def _ensure_symbol_exists(self):
        """Ensure underlying exists in symbols table (required by FK on underlying_quotes)."""
        try:
            symbol_payload = {
                "symbol": self.db_symbol,
                "name": self.db_symbol,
                "asset_type": self._infer_asset_type(self.underlying),
                "is_active": True,
            }
            self._log_parity_signature("symbols", symbol_payload)

            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO symbols (symbol, name, asset_type, is_active)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (symbol) DO UPDATE SET
                        is_active = TRUE,
                        updated_at = NOW()
                    """,
                    (
                        symbol_payload["symbol"],
                        symbol_payload["name"],
                        symbol_payload["asset_type"],  # ts_symbol has $ prefix for indexes
                    ),
                )
                conn.commit()
            logger.info(f"✅ Ensured symbols row exists for {self.db_symbol}")
        except Exception as e:
            logger.error(f"Error ensuring symbols row for {self.db_symbol}: {e}", exc_info=True)

    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals gracefully.

        Signal handlers run on the main thread between bytecodes, so we must
        not touch the ingestion buffers or DB pool here — the main loop may be
        mid-append/mid-iterate, which would corrupt state or raise
        ``RuntimeError: dictionary changed size during iteration``.

        Just flip ``running`` so the main loop exits cleanly; its ``finally``
        block handles the flush and pool close.
        """
        logger.info(f"\n⚠️  Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _initialize_database(self):
        """Initialize database tables if needed"""
        try:
            with db_connection() as conn:
                cursor = conn.cursor()

                # Check if tables exist
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('underlying_quotes', 'option_chains')
                """)

                existing_tables = [row[0] for row in cursor.fetchall()]

                if len(existing_tables) < 2:
                    logger.warning("Database tables not found. Please run sql/schema.sql")
                    logger.warning("Attempting to continue, but storage will fail...")
                else:
                    logger.info(f"✅ Database initialized: {existing_tables}")

        except Exception as e:
            logger.error(f"Error checking database: {e}", exc_info=True)

    def _store_underlying(self, data: Dict[str, Any]):
        """Store latest 1-minute underlying bar snapshot with upsert semantics."""
        # The stream delivers the current 1-minute bar continuously.
        # Persist each update immediately and overwrite the in-progress minute.
        timestamp = data["timestamp"]
        bucket = bucket_timestamp(timestamp, AGGREGATION_BUCKET_SECONDS)

        payload = {
            "symbol": self.db_symbol,
            "timestamp": bucket,
            "open": data["open"],
            "high": data["high"],
            "low": data["low"],
            "close": data["close"],
            "up_volume": data.get("up_volume", 0),
            "down_volume": data.get("down_volume", 0),
        }

        payload_sig = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
        if payload_sig == self._last_underlying_signature:
            # Stream can emit many duplicate updates for the same minute bucket.
            # Skip redundant upserts to reduce DB load.
            return

        self._log_parity_signature("underlying_quotes", payload)

        self._upsert_underlying_quote(payload)
        self._last_underlying_signature = payload_sig

        # Track latest underlying price for Greeks calculation
        old_price = self.latest_underlying_price
        if "close" in data and data["close"] > 0:
            self.latest_underlying_price = data["close"]

            # Log when we first get underlying price (important for Greeks)
            if old_price is None:
                logger.info(f"🎯 First underlying price received: ${self.latest_underlying_price:.2f}")
                logger.info("   Greeks calculation can now proceed for options")
            elif self.underlying_bars_stored % 10 == 0:  # Log every 10 bars
                logger.debug(f"Underlying price updated: ${self.latest_underlying_price:.2f}")

    def _upsert_underlying_quote(self, quote: Dict[str, Any]):
        """Upsert one underlying quote row for the current minute bucket."""
        # Share circuit breaker with option writes — if DB is down, skip.
        if _time.monotonic() < self._db_backoff_until:
            return
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO underlying_quotes
                    (symbol, timestamp, open, high, low, close, up_volume, down_volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        up_volume = EXCLUDED.up_volume,
                        down_volume = EXCLUDED.down_volume,
                        updated_at = NOW()
                """, (
                    quote["symbol"],
                    quote["timestamp"],
                    quote["open"],
                    quote["high"],
                    quote["low"],
                    quote["close"],
                    quote["up_volume"],
                    quote["down_volume"],
                ))
                conn.commit()
                # Reset breaker on success (underlying writes confirm DB is alive).
                self._db_consecutive_failures = 0
                self._db_backoff_until = 0.0

            self.underlying_bars_stored += 1
            self.last_flush_time = datetime.now(ET)

        except Exception as e:
            self._db_consecutive_failures += 1
            self.errors_count += 1
            backoff = min(2 ** self._db_consecutive_failures, 60)
            self._db_backoff_until = _time.monotonic() + backoff
            logger.error(
                f"[CIRCUIT-BREAKER] Underlying upsert failed "
                f"(attempt #{self._db_consecutive_failures}, backoff {backoff}s): {e}",
                exc_info=True,
            )

    def _enrich_with_greeks(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply Greeks calculation to option data, returning enriched copy."""
        if data is None:
            return None

        if self.greeks_calculator and self.latest_underlying_price:
            try:
                if self.greeks_calculated == 0:
                    logger.info(f"Starting Greeks calculation with underlying price: ${self.latest_underlying_price:.2f}")
                    logger.debug(f"Sample option data before Greeks: {data}")

                enriched_data = self.greeks_calculator.enrich_option_data(
                    data,
                    self.latest_underlying_price
                )

                if enriched_data is None:
                    logger.error(f"Greeks calculator returned None for {data.get('option_symbol', 'unknown')}, using original data")
                    data["delta"] = data["gamma"] = data["theta"] = data["vega"] = None
                else:
                    data = enriched_data
                    self.greeks_calculated += 1
                    if self.greeks_calculated % 100 == 0:
                        logger.info(f"Calculated Greeks for {self.greeks_calculated} options")
                    if self.greeks_calculated == 1:
                        logger.info(f"✅ First Greek calculated successfully: delta={data.get('delta')}, gamma={data.get('gamma')}")

            except Exception as e:
                logger.error(f"Error calculating Greeks for {data.get('option_symbol', 'unknown')}: {e}", exc_info=True)
                data["delta"] = data["gamma"] = data["theta"] = data["vega"] = None
        elif self.greeks_calculator and not self.latest_underlying_price:
            if self.greeks_calculated == 0:
                logger.warning("⚠️  Skipping Greeks calculation - no underlying price available yet")
            data["delta"] = data["gamma"] = data["theta"] = data["vega"] = None
        else:
            data["delta"] = data["gamma"] = data["theta"] = data["vega"] = None

        return data

    def _store_option(self, data: Dict[str, Any]):
        """Store a single option quote (delegates to batch method)."""
        self._store_option_batch([data])

    def _store_option_batch(self, batch: List[Dict[str, Any]]):
        """
        Process a batch of option quotes with batched DB writes.

        Each quote is enriched with Greeks and buffered into per-symbol
        1-minute buckets.  All pending aggregations are then flushed to
        the database in a single transaction — one commit for the entire
        batch rather than one commit per contract.
        """
        if not batch:
            return

        rows_to_write: List[Dict[str, Any]] = []

        for data in batch:
            if data is None:
                continue

            pre_symbol = data.get("option_symbol", "unknown")
            data = self._enrich_with_greeks(data)
            if data is None:
                logger.warning(
                    "Dropping option quote after Greeks enrichment returned None: %s",
                    pre_symbol,
                )
                continue

            timestamp = data.get("timestamp")
            if timestamp is None:
                logger.error(f"Option data missing timestamp: {data.get('option_symbol', 'unknown')}")
                continue

            bucket = bucket_timestamp(timestamp, AGGREGATION_BUCKET_SECONDS)

            option_symbol = data.get("option_symbol")
            if option_symbol is None:
                logger.error("Option data missing option_symbol")
                continue

            # If this symbol crossed into a new time bucket, aggregate the previous one.
            existing = self.options_buffer.get(option_symbol)
            if existing:
                prev_timestamp = existing[-1].get("timestamp")
                if prev_timestamp is not None:
                    prev_bucket = bucket_timestamp(prev_timestamp, AGGREGATION_BUCKET_SECONDS)
                    if prev_bucket != bucket:
                        prev_snapshot = existing[-1]
                        agg = self._prepare_option_agg(option_symbol, prev_bucket, keep_last_snapshot=False)
                        if agg:
                            rows_to_write.append(agg)
                        # Seed the new bucket with the previous snapshot for volume delta.
                        self.options_buffer[option_symbol] = [prev_snapshot]

            self.options_buffer[option_symbol].append(data)

            # Prepare aggregation for the current bucket, but throttle
            # in-minute writes to reduce UPDATE churn/dead tuples.
            if self._should_write_option_bucket(option_symbol, bucket):
                agg = self._prepare_option_agg(option_symbol, bucket, keep_last_snapshot=True)
                if agg:
                    rows_to_write.append(agg)

        # Write all aggregated rows in a single DB transaction.
        if rows_to_write:
            self._write_option_rows(rows_to_write)

        # Safety valve: flush everything if total buffer exceeds limit.
        # Use each symbol's latest buffered timestamp so data lands in the
        # correct time bucket (not forced into "now").
        total_buffered = sum(len(v) for v in self.options_buffer.values())
        if total_buffered >= MAX_BUFFER_SIZE:
            logger.warning(f"Option buffer limit reached ({total_buffered} items), flushing all option buffers")
            overflow_rows = []
            for sym in list(self.options_buffer.keys()):
                buf = self.options_buffer.get(sym)
                if buf:
                    last_ts = buf[-1].get("timestamp")
                    sym_bucket = bucket_timestamp(
                        last_ts if last_ts else datetime.now(ET),
                        AGGREGATION_BUCKET_SECONDS,
                    )
                    agg = self._prepare_option_agg(sym, sym_bucket)
                    if agg:
                        overflow_rows.append(agg)
            if overflow_rows:
                self._write_option_rows(overflow_rows)

    def _log_parity_signature(self, stream_name: str, payload: Dict[str, Any]):
        """
        Emit a stable payload signature for runtime parity checks.

        This is feature-flagged and does not alter DB writes.
        """
        if not INGEST_PARITY_GUARD_ENABLED:
            return

        try:
            canonical = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
            digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
            logger.info(f"[PARITY] {stream_name} sig={digest} payload={canonical}")
        except Exception as e:
            logger.warning(f"Failed to emit parity signature for {stream_name}: {e}")

    def _should_write_option_bucket(
        self,
        option_symbol: str,
        bucket: datetime,
        *,
        force: bool = False,
    ) -> bool:
        """Rate-limit writes for the same option contract and time bucket."""
        key = (option_symbol, bucket)
        now_mono = _time.monotonic()

        if force or OPTION_BUCKET_WRITE_MIN_SECONDS <= 0:
            self._option_bucket_last_write[key] = now_mono
            return True

        last_write = self._option_bucket_last_write.get(key)
        if last_write is not None and (now_mono - last_write) < OPTION_BUCKET_WRITE_MIN_SECONDS:
            return False

        self._option_bucket_last_write[key] = now_mono
        return True

    def _classify_volume_chunk(self, volume_delta: int, last: Optional[float], bid: Optional[float], ask: Optional[float], mid: Optional[float]) -> tuple:
        """
        Classify a volume chunk into ask_volume, mid_volume, or bid_volume
        based on how close the last traded price is to each level.

        Returns (ask_vol, mid_vol, bid_vol) tuple where exactly one is non-zero.
        """
        if volume_delta <= 0:
            return (0, 0, 0)

        # Need last price and at least bid/ask to classify
        if last is None or last <= 0:
            return (0, volume_delta, 0)  # Default to mid if we can't determine

        # Compute mid if not provided
        effective_mid = mid
        if effective_mid is None:
            if bid is not None and ask is not None:
                effective_mid = (bid + ask) / 2.0
            else:
                return (0, volume_delta, 0)  # Can't classify without bid/ask

        dist_to_ask = abs(last - ask) if ask is not None else float("inf")
        dist_to_mid = abs(last - effective_mid)
        dist_to_bid = abs(last - bid) if bid is not None else float("inf")

        min_dist = min(dist_to_ask, dist_to_mid, dist_to_bid)

        if dist_to_ask == min_dist:
            return (volume_delta, 0, 0)
        elif dist_to_bid == min_dist:
            return (0, 0, volume_delta)
        else:
            return (0, volume_delta, 0)

    def _get_option_volume_baseline(self, option_symbol: str, bucket: datetime) -> int:
        """Get latest persisted cumulative volume before current bucket for a contract.

        Cached in-memory; entries older than
        ``OPTION_VOLUME_BASELINE_TTL_SECONDS`` are refreshed from the DB
        so the cache self-heals after any external write (retention
        sweeps, backfills) or failed write that left the cache ahead of
        the persisted row.
        """
        now = _time.monotonic()
        with self._option_volume_baseline_lock:
            cached = self._option_volume_baseline.get(option_symbol)
        if cached is not None:
            value, cached_at = cached
            if (now - cached_at) < self._option_volume_baseline_ttl:
                return value

        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT volume
                    FROM option_chains
                    WHERE option_symbol = %s
                      AND timestamp < %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (option_symbol, bucket),
                )
                row = cursor.fetchone()
                baseline = int(row[0]) if row and row[0] is not None else 0
                with self._option_volume_baseline_lock:
                    self._option_volume_baseline[option_symbol] = (baseline, now)
                return baseline
        except Exception as e:
            logger.warning(f"Failed loading volume baseline for {option_symbol}: {e}")
            # On DB failure, prefer the stale cached value (if any) over
            # zero — zero would overcount volume on the next flush.
            if cached is not None:
                return cached[0]
            return 0

    def _invalidate_option_volume_baseline(self, option_symbol: str) -> None:
        """Drop a contract's cached baseline so the next read hits the DB."""
        with self._option_volume_baseline_lock:
            self._option_volume_baseline.pop(option_symbol, None)

    def _prepare_option_agg(self, option_symbol: str, bucket: datetime, keep_last_snapshot: bool = False) -> Optional[Dict[str, Any]]:
        """Aggregate a per-symbol option buffer into a single row dict.

        Handles volume delta classification and buffer cleanup.
        Returns the aggregated dict ready for DB write, or None if the
        buffer is empty.
        """
        buffer = self.options_buffer.get(option_symbol, [])
        if not buffer:
            return None

        try:
            last = buffer[-1]

            delta = _to_db_float(last.get("delta"))
            gamma = _to_db_float(last.get("gamma"))
            theta = _to_db_float(last.get("theta"))
            vega = _to_db_float(last.get("vega"))
            implied_volatility = _to_db_float(last.get("implied_volatility"))

            # Volume delta classification
            ask_volume = 0
            mid_volume = 0
            bid_volume = 0
            if len(buffer) == 1:
                curr = buffer[0]
                curr_vol = curr.get("volume") or 0
                baseline = self._get_option_volume_baseline(option_symbol, bucket)
                vol_delta = max(curr_vol - baseline, 0)
                if vol_delta > 0:
                    av, mv, bv = self._classify_volume_chunk(
                        vol_delta,
                        curr.get("last"), curr.get("bid"),
                        curr.get("ask"), curr.get("mid"),
                    )
                    ask_volume += av
                    mid_volume += mv
                    bid_volume += bv
            else:
                for i in range(1, len(buffer)):
                    prev_vol = buffer[i - 1].get("volume") or 0
                    curr = buffer[i]
                    curr_vol = curr.get("volume") or 0
                    vol_delta = max(curr_vol - prev_vol, 0)
                    if vol_delta > 0:
                        av, mv, bv = self._classify_volume_chunk(
                            vol_delta,
                            curr.get("last"), curr.get("bid"),
                            curr.get("ask"), curr.get("mid"),
                        )
                        ask_volume += av
                        mid_volume += mv
                        bid_volume += bv

            # Use the best available bid/ask/last from any snapshot in
            # the buffer — fall back through the buffer so a single delta
            # that omits price fields doesn't wipe previously-seen values.
            best_last = next((b["last"] for b in reversed(buffer) if b.get("last") is not None), None)
            best_bid = next((b["bid"] for b in reversed(buffer) if b.get("bid") is not None), None)
            best_ask = next((b["ask"] for b in reversed(buffer) if b.get("ask") is not None), None)
            best_mid = next((b["mid"] for b in reversed(buffer) if b.get("mid") is not None), None)
            if best_mid is None and best_bid is not None and best_ask is not None:
                best_mid = (best_bid + best_ask) / 2.0

            agg = {
                "option_symbol": last["option_symbol"],
                "timestamp": bucket,
                "underlying": last["underlying"],
                "strike": last["strike"],
                "expiration": last["expiration"],
                "option_type": last["option_type"],
                "last": best_last,
                "bid": best_bid,
                "ask": best_ask,
                "mid": best_mid,
                "volume": max((b.get("volume") or 0) for b in buffer),
                "open_interest": max((b.get("open_interest") or 0) for b in buffer),
                "implied_volatility": implied_volatility,
                "ask_volume": ask_volume,
                "mid_volume": mid_volume,
                "bid_volume": bid_volume,
                "delta": delta,
                "gamma": gamma,
                "theta": theta,
                "vega": vega,
            }

            self._log_parity_signature("option_chains", agg)

            # Update volume baseline cache with the freshly-aggregated value.
            with self._option_volume_baseline_lock:
                self._option_volume_baseline[option_symbol] = (
                    int(agg["volume"] or 0),
                    _time.monotonic(),
                )

            # Trim buffer.
            if keep_last_snapshot and buffer:
                self.options_buffer[option_symbol] = [buffer[-1]]
            else:
                self.options_buffer[option_symbol] = []
                stale_keys = [
                    key for key in self._option_bucket_last_write
                    if key[0] == option_symbol and key[1] <= bucket
                ]
                for key in stale_keys:
                    self._option_bucket_last_write.pop(key, None)

            return agg

        except Exception as e:
            logger.error(f"Error preparing option agg for {option_symbol}: {e}", exc_info=True)
            self.errors_count += 1
            return None

    # SQL template shared by single and batch writes.
    _OPTION_UPSERT_SQL = """
        INSERT INTO option_chains
        (option_symbol, timestamp, underlying, strike, expiration, option_type,
         last, bid, ask, mid, volume, open_interest, implied_volatility,
         ask_volume, mid_volume, bid_volume,
         delta, gamma, theta, vega)
        VALUES %s
        ON CONFLICT (option_symbol, timestamp) DO UPDATE SET
            last = COALESCE(EXCLUDED.last, option_chains.last),
            bid = COALESCE(EXCLUDED.bid, option_chains.bid),
            ask = COALESCE(EXCLUDED.ask, option_chains.ask),
            mid = COALESCE(EXCLUDED.mid, option_chains.mid),
            volume = GREATEST(option_chains.volume, EXCLUDED.volume),
            open_interest = GREATEST(option_chains.open_interest, EXCLUDED.open_interest),
            implied_volatility = COALESCE(EXCLUDED.implied_volatility, option_chains.implied_volatility),
            ask_volume = option_chains.ask_volume + EXCLUDED.ask_volume,
            mid_volume = option_chains.mid_volume + EXCLUDED.mid_volume,
            bid_volume = option_chains.bid_volume + EXCLUDED.bid_volume,
            delta = EXCLUDED.delta,
            gamma = EXCLUDED.gamma,
            theta = EXCLUDED.theta,
            vega = EXCLUDED.vega,
            updated_at = NOW()
        WHERE
            COALESCE(EXCLUDED.last, option_chains.last) IS DISTINCT FROM option_chains.last
            OR COALESCE(EXCLUDED.bid, option_chains.bid) IS DISTINCT FROM option_chains.bid
            OR COALESCE(EXCLUDED.ask, option_chains.ask) IS DISTINCT FROM option_chains.ask
            OR COALESCE(EXCLUDED.mid, option_chains.mid) IS DISTINCT FROM option_chains.mid
            OR GREATEST(option_chains.volume, EXCLUDED.volume) IS DISTINCT FROM option_chains.volume
            OR GREATEST(option_chains.open_interest, EXCLUDED.open_interest) IS DISTINCT FROM option_chains.open_interest
            OR COALESCE(EXCLUDED.implied_volatility, option_chains.implied_volatility) IS DISTINCT FROM option_chains.implied_volatility
            OR (option_chains.ask_volume + EXCLUDED.ask_volume) IS DISTINCT FROM option_chains.ask_volume
            OR (option_chains.mid_volume + EXCLUDED.mid_volume) IS DISTINCT FROM option_chains.mid_volume
            OR (option_chains.bid_volume + EXCLUDED.bid_volume) IS DISTINCT FROM option_chains.bid_volume
            OR EXCLUDED.delta IS DISTINCT FROM option_chains.delta
            OR EXCLUDED.gamma IS DISTINCT FROM option_chains.gamma
            OR EXCLUDED.theta IS DISTINCT FROM option_chains.theta
            OR EXCLUDED.vega IS DISTINCT FROM option_chains.vega
    """

    def _coalesce_option_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Collapse duplicate (option_symbol, timestamp) rows before DB writes."""
        coalesced: Dict[tuple, Dict[str, Any]] = {}

        for row in rows:
            key = (row["option_symbol"], row["timestamp"])
            existing = coalesced.get(key)
            if existing is None:
                coalesced[key] = dict(row)
                continue

            # Preserve latest non-null quote fields.
            for field in ("last", "bid", "ask", "mid", "implied_volatility", "delta", "gamma", "theta", "vega"):
                if row.get(field) is not None:
                    existing[field] = row[field]

            # Preserve monotonic fields.
            existing["volume"] = max(existing.get("volume") or 0, row.get("volume") or 0)
            existing["open_interest"] = max(existing.get("open_interest") or 0, row.get("open_interest") or 0)

            # Preserve additive flow fields.
            existing["ask_volume"] = (existing.get("ask_volume") or 0) + (row.get("ask_volume") or 0)
            existing["mid_volume"] = (existing.get("mid_volume") or 0) + (row.get("mid_volume") or 0)
            existing["bid_volume"] = (existing.get("bid_volume") or 0) + (row.get("bid_volume") or 0)

        return list(coalesced.values())

    def _write_option_rows(self, rows: List[Dict[str, Any]]):
        """Write multiple aggregated option rows in a single DB transaction.

        Includes a circuit breaker: after consecutive failures the engine
        backs off exponentially (2s, 4s, 8s … capped at 60s) so we don't
        hammer a dead database.  On recovery the breaker resets immediately.
        """
        if not rows:
            return

        # Many stream iterations can generate repeated updates for the same
        # option/timestamp key. Coalesce them before touching the DB.
        rows = self._coalesce_option_rows(rows)

        # Circuit breaker: skip write if still in backoff window.
        now_mono = _time.monotonic()
        if now_mono < self._db_backoff_until:
            logger.warning(
                f"[CIRCUIT-BREAKER] Skipping write of {len(rows)} rows — "
                f"DB backoff for {self._db_backoff_until - now_mono:.1f}s more"
            )
            return

        t0 = _time.monotonic()
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                values = [
                    (
                        agg["option_symbol"],
                        agg["timestamp"],
                        agg["underlying"],
                        agg["strike"],
                        agg["expiration"],
                        agg["option_type"],
                        agg["last"],
                        agg["bid"],
                        agg["ask"],
                        agg["mid"],
                        agg["volume"],
                        agg["open_interest"],
                        agg["implied_volatility"],
                        agg["ask_volume"],
                        agg["mid_volume"],
                        agg["bid_volume"],
                        agg["delta"],
                        agg["gamma"],
                        agg["theta"],
                        agg["vega"],
                    )
                    for agg in rows
                ]
                execute_values(
                    cursor,
                    self._OPTION_UPSERT_SQL,
                    values,
                    page_size=500,
                )
                conn.commit()

            elapsed_ms = (_time.monotonic() - t0) * 1000
            self.option_quotes_stored += len(rows)
            self.last_flush_time = datetime.now(ET)

            # Reset circuit breaker on success.
            if self._db_consecutive_failures > 0:
                logger.info(
                    f"[CIRCUIT-BREAKER] DB recovered after "
                    f"{self._db_consecutive_failures} consecutive failures"
                )
            self._db_consecutive_failures = 0
            self._db_backoff_until = 0.0

            # Observability accumulators.
            self._obs_batches_written += 1
            self._obs_rows_written += len(rows)
            self._obs_write_time_ms += elapsed_ms

            # Log first few with Greeks.
            if self.option_quotes_stored <= len(rows) + 3:
                for agg in rows[:3]:
                    d = agg.get("delta")
                    if d is not None:
                        logger.info(
                            f"✅ Stored option with Greeks: {agg['option_symbol']} "
                            f"delta={d:.4f} gamma={agg.get('gamma', 0):.6f}"
                        )

            logger.debug(
                f"Wrote {len(rows)} option rows in single transaction "
                f"({elapsed_ms:.1f}ms)"
            )

            # Periodic observability summary (every 60s).
            now = _time.monotonic()
            if now - self._obs_last_log >= 60:
                avg_ms = (
                    self._obs_write_time_ms / self._obs_batches_written
                    if self._obs_batches_written
                    else 0
                )
                logger.info(
                    f"[DB-METRICS] last 60s: "
                    f"batches={self._obs_batches_written} "
                    f"rows={self._obs_rows_written} "
                    f"avg_write_ms={avg_ms:.1f} "
                    f"total_stored={self.option_quotes_stored} "
                    f"errors={self.errors_count}"
                )
                self._obs_batches_written = 0
                self._obs_rows_written = 0
                self._obs_write_time_ms = 0.0
                self._obs_last_log = now

        except Exception as e:
            self._db_consecutive_failures += 1
            self.errors_count += 1
            # Exponential backoff: 2s, 4s, 8s, 16s, 32s, 60s cap
            backoff = min(2 ** self._db_consecutive_failures, 60)
            self._db_backoff_until = _time.monotonic() + backoff

            # The baseline cache was optimistically advanced in
            # _prepare_option_agg; invalidate entries for failed rows so the
            # next flush re-queries the DB and computes volume deltas against
            # what was actually persisted.
            for row in rows:
                self._invalidate_option_volume_baseline(row["option_symbol"])

            # Include affected-symbol counts, unique underlyings, and the full
            # timestamp range. Without this, root cause analysis is impossible
            # when a single bad row triggers a whole batch rollback — a
            # 5-symbol sample buries the outlier that caused the failure.
            unique_symbols = {r["option_symbol"] for r in rows}
            unique_underlyings = sorted({r.get("underlying") for r in rows if r.get("underlying")})
            timestamps = [r.get("timestamp") for r in rows if r.get("timestamp") is not None]
            ts_min = min(timestamps) if timestamps else None
            ts_max = max(timestamps) if timestamps else None
            logger.error(
                "[CIRCUIT-BREAKER] DB write failed (%d rows, %d unique symbols, "
                "underlyings=%s, attempt #%d, backoff %ds): %s\n"
                "  first_symbol=%s last_symbol=%s\n"
                "  timestamp range: %s .. %s",
                len(rows),
                len(unique_symbols),
                unique_underlyings,
                self._db_consecutive_failures,
                backoff,
                e,
                rows[0].get("option_symbol") if rows else None,
                rows[-1].get("option_symbol") if rows else None,
                ts_min,
                ts_max,
                exc_info=True,
            )

    def _flush_option_bucket(self, option_symbol: str, bucket: datetime, keep_last_snapshot: bool = False):
        """Flush a single option bucket (used by _flush_all_buffers)."""
        agg = self._prepare_option_agg(option_symbol, bucket, keep_last_snapshot)
        if agg:
            self._write_option_rows([agg])

    def _flush_all_buffers(self):
        """Flush all pending buffers"""
        logger.info(f"Flushing all buffers... (Underlying: {len(self.underlying_buffer)}, Options: {sum(len(v) for v in self.options_buffer.values())} across {len(self.options_buffer)} symbols)")

        # Flush all options
        current_time = datetime.now(ET)
        bucket = bucket_timestamp(current_time, AGGREGATION_BUCKET_SECONDS)

        options_flushed = 0
        for option_symbol in list(self.options_buffer.keys()):
            if self.options_buffer[option_symbol]:  # Only flush if buffer has data
                self._flush_option_bucket(option_symbol, bucket)
                options_flushed += 1

        logger.info(f"✅ Flushed buffers: {options_flushed} option symbols")
        self.last_flush_time = current_time

    def _check_buffer_flush_timeout(self):
        """Check if buffers should be flushed due to timeout"""
        now = datetime.now(ET)

        if (now - self.last_flush_time).total_seconds() > BUFFER_FLUSH_INTERVAL:
            logger.debug("Buffer flush timeout reached, flushing all buffers...")
            self._flush_all_buffers()

    def run_streaming(self):
        """Run streaming phase"""
        if not is_engine_run_window():
            logger.info("Skipping stream start outside configured run window")
            return True
        logger.info("="*80)
        logger.info("STREAMING PHASE")
        logger.info("="*80)

        stream_manager = StreamManager(
            client=self.client,
            underlying=self.underlying,
            db_underlying=self.db_symbol,
            num_expirations=self.num_expirations,
            num_strikes=self.num_strikes,
        )

        if not stream_manager.initialize():
            logger.error("Failed to initialize streaming")
            return

        logger.info("✅ Streaming initialized")
        logger.info("Press Ctrl+C to stop\n")

        self.running = True

        window_closed = False
        try:
            for item in stream_manager.stream(max_iterations=None):
                if not self.running:
                    break
                if not is_engine_run_window():
                    logger.info("Run window closed; stopping active streams until next run window")
                    window_closed = True
                    self.running = False
                    break

                if item["type"] == "underlying":
                    self._store_underlying(item["data"])
                elif item["type"] == "option_batch":
                    self._store_option_batch(item["data"])
                elif item["type"] == "option":
                    self._store_option(item["data"])

                # Check for flush timeout
                self._check_buffer_flush_timeout()

        except KeyboardInterrupt:
            logger.info("\n⚠️  Stream interrupted by user")
        except Exception as e:
            logger.error(f"Streaming error: {e}", exc_info=True)
        finally:
            self._flush_all_buffers()
            try:
                self.client.close_all_streams()
            except Exception as e:
                logger.warning(f"Error closing TradeStation streams: {e}")
            logger.info("Streaming stopped")
        return window_closed

    def run(self):
        """Run forward-only ingestion pipeline"""
        logger.info("\n" + "="*80)
        logger.info("ZEROGEX MAIN INGESTION ENGINE - FORWARD ONLY")
        logger.info("="*80)
        logger.info(f"Underlying: {self.underlying}")
        logger.info(f"Expirations: {self.num_expirations}")
        logger.info(f"Strikes Each Side: {self.num_strikes}")
        logger.info(f"Greeks: {'ENABLED' if GREEKS_ENABLED else 'DISABLED'}")
        logger.info("")
        logger.info("NOTE: This engine streams forward-looking data.")
        logger.info("="*80 + "\n")

        self.running = True
        try:
            while self.running:
                if not is_engine_run_window():
                    sleep_for = seconds_until_engine_run_window()
                    logger.info(
                        "IngestionEngine [%s] paused outside run window (24x5: weekdays, non-holidays); sleeping %ss",
                        self.underlying,
                        sleep_for,
                    )
                    time.sleep(max(1, sleep_for))
                    continue

                window_closed = self.run_streaming()
                if not self.running:
                    if window_closed:
                        # run_streaming intentionally sets running=False when window closes;
                        # restore loop sentinel so scheduler can sleep and resume next window.
                        self.running = True
                    else:
                        break

        except Exception as e:
            logger.error(f"Fatal error in main engine: {e}", exc_info=True)
            sys.exit(1)
        finally:
            # Print final stats
            logger.info("\n" + "="*80)
            logger.info("SESSION SUMMARY")
            logger.info("="*80)
            logger.info(f"Underlying bars stored: {self.underlying_bars_stored}")
            logger.info(f"Option quotes stored: {self.option_quotes_stored}")
            if GREEKS_ENABLED:
                logger.info(f"Greeks calculated: {self.greeks_calculated}")
            logger.info(f"Errors encountered: {self.errors_count}")
            logger.info("="*80 + "\n")

            close_connection_pool()


def main():
    """Main entry point"""
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="ZeroGEX Main Ingestion Engine")
    parser.add_argument("--underlying", default=None,
                       help="Single underlying symbol (backward compatible)")
    parser.add_argument(
        "--underlyings",
        default=os.getenv("INGEST_UNDERLYINGS", os.getenv("INGEST_UNDERLYING", "SPY")),
        help="Comma-separated underlying symbols or aliases (default: SPY)",
    )
    parser.add_argument("--expirations", type=int,
                       default=int(os.getenv("INGEST_EXPIRATIONS", "3")),
                       help="Number of expirations (default: 3)")
    parser.add_argument("--num-strikes", type=int,
                       default=int(os.getenv("INGEST_STRIKE_COUNT", "10")),
                       help="Number of strikes to track on each side of current price (default: 10)")
    parser.add_argument("--session-template", 
                       default=os.getenv("SESSION_TEMPLATE", "Default"),
                       choices=["Default", "USEQPre", "USEQ24Hour"],
                       help="Session template (default: Default)")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging")

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        from src.utils import set_log_level
        set_log_level("DEBUG")

    raw_underlyings = args.underlying if args.underlying else args.underlyings
    symbols = parse_underlyings(raw_underlyings)

    if not symbols:
        logger.error("No valid underlyings provided")
        sys.exit(1)

    def run_for_symbol(symbol: str):
        from src.ingestion.api_call_tracker import attach_db_writer

        client = TradeStationClient(
            os.getenv("TRADESTATION_CLIENT_ID"),
            os.getenv("TRADESTATION_CLIENT_SECRET"),
            os.getenv("TRADESTATION_REFRESH_TOKEN"),
            sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true"
        )
        attach_db_writer(client)
        engine = IngestionEngine(
            client=client,
            underlying=symbol,
            num_expirations=args.expirations,
            num_strikes=args.num_strikes,
        )
        engine.run()

    def run_vix_ingester():
        from src.ingestion.vix_ingester import main as vix_main
        vix_main()

    # Always run the VIX ingester alongside the per-symbol engines so that
    # /api/market/vix can read from `vix_bars` without hitting TradeStation.
    vix_enabled = os.getenv("INGEST_VIX_ENABLED", "true").lower() != "false"

    if len(symbols) == 1 and not vix_enabled:
        run_for_symbol(symbols[0])
        return

    logger.info(f"Starting ingestion engines for symbols: {', '.join(symbols)}")
    if vix_enabled:
        logger.info("Starting VIX ingester alongside symbol engines")
    processes: List[Process] = []

    for symbol in symbols:
        process = Process(target=run_for_symbol, args=(symbol,), name=f"ingest-{symbol}")
        process.start()
        processes.append(process)

    if vix_enabled:
        vix_process = Process(target=run_vix_ingester, name="ingest-vix")
        vix_process.start()
        processes.append(vix_process)

    def shutdown_children(signum, frame):
        logger.info(f"Received signal {signum}, terminating ingestion workers...")
        for proc in processes:
            if proc.is_alive():
                proc.terminate()

    signal.signal(signal.SIGINT, shutdown_children)
    signal.signal(signal.SIGTERM, shutdown_children)

    exit_code = 0
    for proc in processes:
        proc.join()
        if proc.exitcode not in (0, None):
            exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
