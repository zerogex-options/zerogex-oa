"""
ZeroGEX Main Ingestion Engine

This engine:
1. Streams real-time data using StreamManager (no backfill)
2. Handles 1-minute aggregation
3. Calculates Greeks for options (if enabled)
4. Stores data in PostgreSQL/TimescaleDB
5. Monitors data quality and pipeline health

For historical data backfilling, use backfill_manager.py independently.
"""

import os
import signal
import sys
from multiprocessing import Process
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from collections import defaultdict
import pytz

from src.ingestion.tradestation_client import TradeStationClient
from src.ingestion.stream_manager import StreamManager
from src.ingestion.greeks_calculator import GreeksCalculator
from src.database import db_connection, close_connection_pool
from src.utils import get_logger
from src.validation import bucket_timestamp
from src.symbols import parse_underlyings, get_canonical_symbol
from src.config import (
    AGGREGATION_BUCKET_SECONDS,
    MAX_BUFFER_SIZE,
    BUFFER_FLUSH_INTERVAL,
    GREEKS_ENABLED,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


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
        strike_distance: float = 10.0,
    ):
        """Initialize main ingestion engine"""
        self.client = client
        self.underlying = underlying.upper()         # TradeStation API symbol (e.g. "$SPX.X")
        self.db_symbol = get_canonical_symbol(self.underlying)  # canonical alias for DB (e.g. "SPX")
        self.num_expirations = num_expirations
        self.strike_distance = strike_distance

        self.running = False

        # Buffering for options only (underlying writes every update)
        self.underlying_buffer: List[Dict[str, Any]] = []
        self.options_buffer: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

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

        logger.info(f"Initialized IngestionEngine for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, ±${strike_distance} strikes")

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
                        self.db_symbol,
                        self.db_symbol,
                        self._infer_asset_type(self.underlying),  # ts_symbol has $ prefix for indexes
                    ),
                )
                conn.commit()
            logger.info(f"✅ Ensured symbols row exists for {self.db_symbol}")
        except Exception as e:
            logger.error(f"Error ensuring symbols row for {self.db_symbol}: {e}", exc_info=True)

    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals gracefully

        Args:
            signum: Signal number
            frame: Current stack frame
        """
        logger.info(f"\n⚠️  Received signal {signum}, shutting down gracefully...")
        self.running = False
        self._flush_all_buffers()
        close_connection_pool()

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

        self._upsert_underlying_quote(payload)

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

            self.underlying_bars_stored += 1
            self.last_flush_time = datetime.now(ET)

        except Exception as e:
            logger.error(f"Error upserting underlying quote: {e}", exc_info=True)
            self.errors_count += 1

    def _flush_underlying_bucket(self):
        """No-op: underlying bars are written immediately per update."""
        return

    def _store_option(self, data: Dict[str, Any]):
        """
        Buffer option quote for 1-minute aggregation

        Calculates Greeks before buffering if enabled.

        Args:
            data: Option quote data
        """
        # Validate data is not None
        if data is None:
            logger.error("Received None data in _store_option, skipping")
            return

        # Calculate Greeks if enabled and we have underlying price
        if self.greeks_calculator and self.latest_underlying_price:
            try:
                # Log what we're working with
                if self.greeks_calculated == 0:
                    logger.info(f"Starting Greeks calculation with underlying price: ${self.latest_underlying_price:.2f}")
                    logger.debug(f"Sample option data before Greeks: {data}")

                enriched_data = self.greeks_calculator.enrich_option_data(
                    data, 
                    self.latest_underlying_price
                )

                # Check if enrichment returned None
                if enriched_data is None:
                    logger.error(f"Greeks calculator returned None for {data.get('option_symbol', 'unknown')}, using original data")
                    # Add zero Greeks to original data
                    data["delta"] = None
                    data["gamma"] = None
                    data["theta"] = None
                    data["vega"] = None
                else:
                    data = enriched_data  # Use enriched data
                    self.greeks_calculated += 1

                    if self.greeks_calculated % 100 == 0:
                        logger.info(f"Calculated Greeks for {self.greeks_calculated} options")

                    # Log first successful Greek calculation
                    if self.greeks_calculated == 1:
                        logger.info(f"✅ First Greek calculated successfully: delta={data.get('delta')}, gamma={data.get('gamma')}")

            except Exception as e:
                logger.error(f"Error calculating Greeks for {data.get('option_symbol', 'unknown')}: {e}", exc_info=True)
                # Add zero Greeks as fallback
                data["delta"] = None
                data["gamma"] = None
                data["theta"] = None
                data["vega"] = None
        elif self.greeks_calculator and not self.latest_underlying_price:
            if self.greeks_calculated == 0:  # Only warn once
                logger.warning("⚠️  Skipping Greeks calculation - no underlying price available yet")
            # Add zero Greeks
            data["delta"] = None
            data["gamma"] = None
            data["theta"] = None
            data["vega"] = None
        elif not self.greeks_calculator:
            # Greeks not enabled
            data["delta"] = None
            data["gamma"] = None
            data["theta"] = None
            data["vega"] = None

        # Get timestamp and bucket it
        timestamp = data.get("timestamp")
        if timestamp is None:
            logger.error(f"Option data missing timestamp: {data.get('option_symbol', 'unknown')}")
            return

        bucket = bucket_timestamp(timestamp, AGGREGATION_BUCKET_SECONDS)

        # Buffer by option symbol
        option_symbol = data.get("option_symbol")
        if option_symbol is None:
            logger.error(f"Option data missing option_symbol")
            return

        self.options_buffer[option_symbol].append(data)

        # Check TOTAL buffer size across all options, not per-symbol
        total_buffered = sum(len(v) for v in self.options_buffer.values())

        # Flush all buffers if total exceeds limit
        if total_buffered >= MAX_BUFFER_SIZE:
            logger.debug(f"Option buffer limit reached ({total_buffered} items), flushing all option buffers")
            current_time = datetime.now(ET)
            flush_bucket = bucket_timestamp(current_time, AGGREGATION_BUCKET_SECONDS)
            for sym in list(self.options_buffer.keys()):
                if self.options_buffer[sym]:
                    self._flush_option_bucket(sym, flush_bucket)

    def _flush_option_bucket(self, option_symbol: str, bucket: datetime):
        """Aggregate and store 1-minute option quote"""
        buffer = self.options_buffer.get(option_symbol, [])

        if not buffer:
            return

        try:
            # Aggregate: last of each field
            last = buffer[-1]

            # Convert numpy types to Python native types for PostgreSQL
            delta = float(last.get("delta")) if last.get("delta") is not None else None
            gamma = float(last.get("gamma")) if last.get("gamma") is not None else None
            theta = float(last.get("theta")) if last.get("theta") is not None else None
            vega = float(last.get("vega")) if last.get("vega") is not None else None

            agg = {
                "option_symbol": last["option_symbol"],
                "timestamp": bucket,
                "underlying": last["underlying"],
                "strike": last["strike"],
                "expiration": last["expiration"],
                "option_type": last["option_type"],
                "last": last.get("last", 0),
                "bid": last.get("bid", 0),
                "ask": last.get("ask", 0),
                "volume": last.get("volume", 0),
                "open_interest": last.get("open_interest", 0),
                "delta": delta,
                "gamma": gamma,
                "theta": theta,
                "vega": vega,
            }

            # Store in database
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO option_chains 
                    (option_symbol, timestamp, underlying, strike, expiration, option_type,
                     last, bid, ask, volume, open_interest, delta, gamma, theta, vega)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (option_symbol, timestamp) DO UPDATE SET
                        last = EXCLUDED.last,
                        bid = EXCLUDED.bid,
                        ask = EXCLUDED.ask,
                        volume = EXCLUDED.volume,
                        open_interest = EXCLUDED.open_interest,
                        delta = EXCLUDED.delta,
                        gamma = EXCLUDED.gamma,
                        theta = EXCLUDED.theta,
                        vega = EXCLUDED.vega,
                        updated_at = NOW()
                """, (
                    agg["option_symbol"],
                    agg["timestamp"],
                    agg["underlying"],
                    agg["strike"],
                    agg["expiration"],
                    agg["option_type"],
                    agg["last"],
                    agg["bid"],
                    agg["ask"],
                    agg["volume"],
                    agg["open_interest"],
                    agg["delta"],
                    agg["gamma"],
                    agg["theta"],
                    agg["vega"]
                ))
                conn.commit()

            self.option_quotes_stored += 1

            # Log first few with Greeks to confirm storage
            if self.option_quotes_stored <= 3 and delta is not None:
                logger.info(f"✅ Stored option with Greeks: {agg['option_symbol']} "
                          f"delta={delta:.4f} gamma={gamma:.6f}")

            logger.debug(f"Stored option: {agg['option_symbol']} @ {agg['timestamp']} "
                        f"Last=${agg['last']:.2f}")

            # Clear buffer
            self.options_buffer[option_symbol] = []

        except Exception as e:
            logger.error(f"Error flushing option bucket for {option_symbol}: {e}", exc_info=True)
            self.errors_count += 1

    def _flush_all_buffers(self):
        """Flush all pending buffers"""
        logger.info(f"Flushing all buffers... (Underlying: {len(self.underlying_buffer)}, Options: {sum(len(v) for v in self.options_buffer.values())} across {len(self.options_buffer)} symbols)")

        # Flush underlying
        if self.underlying_buffer:
            self._flush_underlying_bucket()

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
        logger.info("="*80)
        logger.info("STREAMING PHASE")
        logger.info("="*80)

        stream_manager = StreamManager(
            client=self.client,
            underlying=self.underlying,
            db_underlying=self.db_symbol,
            num_expirations=self.num_expirations,
            strike_distance=self.strike_distance,
        )

        if not stream_manager.initialize():
            logger.error("Failed to initialize streaming")
            return

        logger.info("✅ Streaming initialized")
        logger.info("Press Ctrl+C to stop\n")

        self.running = True

        try:
            for item in stream_manager.stream(max_iterations=None):
                if not self.running:
                    break

                if item["type"] == "underlying":
                    self._store_underlying(item["data"])
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
            logger.info("Streaming stopped")

    def run(self):
        """Run forward-only ingestion pipeline"""
        logger.info("\n" + "="*80)
        logger.info("ZEROGEX MAIN INGESTION ENGINE - FORWARD ONLY")
        logger.info("="*80)
        logger.info(f"Underlying: {self.underlying}")
        logger.info(f"Expirations: {self.num_expirations}")
        logger.info(f"Strike Distance: ±${self.strike_distance}")
        logger.info(f"Greeks: {'ENABLED' if GREEKS_ENABLED else 'DISABLED'}")
        logger.info("")
        logger.info("NOTE: This engine only streams forward-looking data.")
        logger.info("      For historical backfill, run backfill_manager.py independently.")
        logger.info("="*80 + "\n")

        try:
            # Only run streaming (no backfill)
            self.run_streaming()

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
    parser.add_argument("--strike-distance", type=float,
                       default=float(os.getenv("INGEST_STRIKE_DISTANCE", "10.0")),
                       help="Strike distance (default: 10.0)")
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
        client = TradeStationClient(
            os.getenv("TRADESTATION_CLIENT_ID"),
            os.getenv("TRADESTATION_CLIENT_SECRET"),
            os.getenv("TRADESTATION_REFRESH_TOKEN"),
            sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true"
        )
        engine = IngestionEngine(
            client=client,
            underlying=symbol,
            num_expirations=args.expirations,
            strike_distance=args.strike_distance,
        )
        engine.run()

    if len(symbols) == 1:
        run_for_symbol(symbols[0])
        return

    logger.info(f"Starting ingestion engines for symbols: {', '.join(symbols)}")
    processes: List[Process] = []

    for symbol in symbols:
        process = Process(target=run_for_symbol, args=(symbol,), name=f"ingest-{symbol}")
        process.start()
        processes.append(process)

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
