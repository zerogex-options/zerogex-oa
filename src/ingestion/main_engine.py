"""
ZeroGEX Main Ingestion Engine - Orchestrates backfill, streaming, and storage

This engine:
1. Delegates data fetching to BackfillManager and StreamManager
2. Handles 1-minute aggregation
3. Calculates Greeks for options (if enabled)
4. Stores data in PostgreSQL/TimescaleDB
5. Monitors data quality and pipeline health
"""

import os
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from collections import defaultdict
import pytz

from src.ingestion.tradestation_client import TradeStationClient
from src.ingestion.backfill_manager import BackfillManager
from src.ingestion.stream_manager import StreamManager
from src.ingestion.greeks_calculator import GreeksCalculator
from src.database import db_connection, close_connection_pool
from src.utils import get_logger
from src.validation import bucket_timestamp
from src.config import (
    AGGREGATION_BUCKET_SECONDS,
    MAX_BUFFER_SIZE,
    BUFFER_FLUSH_INTERVAL,
    BACKFILL_ON_STARTUP,
    MAX_GAP_MINUTES,
    GREEKS_ENABLED,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


class MainEngine:
    """
    Main ingestion engine - orchestrates backfill, streaming, and storage

    Managers fetch data, MainEngine stores it.
    """

    def __init__(
        self,
        client: TradeStationClient,
        underlying: str = "SPY",
        num_expirations: int = 3,
        strike_distance: float = 10.0,
        lookback_days: int = 7,
    ):
        """Initialize main ingestion engine"""
        self.client = client
        self.underlying = underlying.upper()
        self.num_expirations = num_expirations
        self.strike_distance = strike_distance
        self.lookback_days = lookback_days

        self.running = False

        # Aggregation buffers
        self.current_bucket: Optional[datetime] = None
        self.underlying_buffer: List[Dict[str, Any]] = []
        self.options_buffer: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        # Track latest underlying price for Greeks calculation
        self.latest_underlying_price: Optional[float] = None

        # Greeks calculator (initialize if enabled)
        self.greeks_calculator = None
        if GREEKS_ENABLED:
            self.greeks_calculator = GreeksCalculator()
            logger.info("✅ Greeks calculation ENABLED")
        else:
            logger.info("⚠️  Greeks calculation DISABLED (set GREEKS_ENABLED=true to enable)")

        # Metrics
        self.underlying_bars_stored = 0
        self.option_quotes_stored = 0
        self.greeks_calculated = 0
        self.last_flush_time = datetime.now(ET)
        self.errors_count = 0

        logger.info(f"Initialized MainEngine for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, ±${strike_distance} strikes, "
                   f"{lookback_days} days lookback")

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Initialize database
        self._initialize_database()

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

    def _detect_gaps(self) -> List[Dict[str, Any]]:
        """
        Detect gaps in data that need backfilling

        Returns:
            List of gap dictionaries with start/end times
        """
        gaps = []

        try:
            with db_connection() as conn:
                cursor = conn.cursor()

                # Find gaps in underlying_quotes
                cursor.execute("""
                    WITH time_series AS (
                        SELECT timestamp, 
                               LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                        FROM underlying_quotes
                        WHERE symbol = %s
                        AND timestamp > NOW() - INTERVAL '7 days'
                        ORDER BY timestamp
                    )
                    SELECT prev_timestamp, timestamp,
                           EXTRACT(EPOCH FROM (timestamp - prev_timestamp))/60 as gap_minutes
                    FROM time_series
                    WHERE EXTRACT(EPOCH FROM (timestamp - prev_timestamp))/60 > %s
                """, (self.underlying, MAX_GAP_MINUTES))

                results = cursor.fetchall()

                for row in results:
                    prev_time, curr_time, gap_minutes = row
                    gaps.append({
                        "start": prev_time,
                        "end": curr_time,
                        "gap_minutes": gap_minutes
                    })
                    logger.warning(f"Detected gap: {prev_time} to {curr_time} "
                                 f"({gap_minutes:.0f} minutes)")

        except Exception as e:
            logger.error(f"Error detecting gaps: {e}", exc_info=True)

        return gaps

    def _store_underlying(self, data: Dict[str, Any]):
        """
        Buffer underlying quote for 1-minute aggregation

        Args:
            data: Underlying quote data
        """
        # Get timestamp and bucket it
        timestamp = data["timestamp"]
        bucket = bucket_timestamp(timestamp, AGGREGATION_BUCKET_SECONDS)

        # If new bucket, flush previous
        if self.current_bucket and bucket > self.current_bucket:
            self._flush_underlying_bucket()

        self.current_bucket = bucket
        self.underlying_buffer.append(data)

        # Track latest underlying price for Greeks calculation
        if "last" in data and data["last"] > 0:
            self.latest_underlying_price = data["last"]
        elif "close" in data and data["close"] > 0:
            self.latest_underlying_price = data["close"]

        # Flush if buffer too large
        if len(self.underlying_buffer) >= MAX_BUFFER_SIZE:
            self._flush_underlying_bucket()

    def _flush_underlying_bucket(self):
        """Aggregate and store 1-minute underlying bar"""
        if not self.underlying_buffer:
            return

        try:
            # Aggregate: first open, max high, min low, last close, sum volume
            first = self.underlying_buffer[0]
            last = self.underlying_buffer[-1]

            # Handle both streaming (last/bid/ask) and backfill (open/high/low/close)
            if "open" in first:
                # Backfill data with OHLC
                agg = {
                    "symbol": first["symbol"],
                    "timestamp": self.current_bucket,
                    "open": first["open"],
                    "high": max(d["high"] for d in self.underlying_buffer),
                    "low": min(d["low"] for d in self.underlying_buffer),
                    "close": last["close"],
                    "volume": sum(d.get("volume", 0) for d in self.underlying_buffer),
                }
            else:
                # Streaming data - construct OHLC from last prices
                prices = [d["last"] for d in self.underlying_buffer if d.get("last", 0) > 0]

                if not prices:
                    logger.warning("No valid prices in buffer, skipping flush")
                    self.underlying_buffer = []
                    return

                agg = {
                    "symbol": first["symbol"],
                    "timestamp": self.current_bucket,
                    "open": prices[0],
                    "high": max(prices),
                    "low": min(prices),
                    "close": prices[-1],
                    "volume": sum(d.get("volume", 0) for d in self.underlying_buffer),
                }

            # Store in database
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
                    agg["symbol"], 
                    agg["timestamp"],
                    agg["open"],
                    agg["high"],
                    agg["low"],
                    agg["close"],
                    agg["volume"],
                    0  # down_volume - not available from TradeStation
                ))
                conn.commit()

            self.underlying_bars_stored += 1
            logger.info(f"✅ Stored 1-min bar: {agg['symbol']} @ {agg['timestamp']} "
                       f"O=${agg['open']:.2f} H=${agg['high']:.2f} "
                       f"L=${agg['low']:.2f} C=${agg['close']:.2f}")

            self.underlying_buffer = []
            self.last_flush_time = datetime.now(ET)

        except Exception as e:
            logger.error(f"Error flushing underlying bucket: {e}", exc_info=True)
            self.errors_count += 1

    def _store_option(self, data: Dict[str, Any]):
        """
        Buffer option quote for 1-minute aggregation

        Calculates Greeks before buffering if enabled.

        Args:
            data: Option quote data
        """
        # Calculate Greeks if enabled and we have underlying price
        if self.greeks_calculator and self.latest_underlying_price:
            try:
                data = self.greeks_calculator.enrich_option_data(
                    data, 
                    self.latest_underlying_price
                )
                self.greeks_calculated += 1

                if self.greeks_calculated % 100 == 0:
                    logger.debug(f"Calculated Greeks for {self.greeks_calculated} options")

            except Exception as e:
                logger.error(f"Error calculating Greeks for {data.get('option_symbol')}: {e}")
                # Add zero Greeks as fallback
                data["delta"] = 0.0
                data["gamma"] = 0.0
                data["theta"] = 0.0
                data["vega"] = 0.0
        elif self.greeks_calculator and not self.latest_underlying_price:
            logger.debug("Skipping Greeks calculation - no underlying price available yet")
            # Add zero Greeks
            data["delta"] = 0.0
            data["gamma"] = 0.0
            data["theta"] = 0.0
            data["vega"] = 0.0

        # Get timestamp and bucket it
        timestamp = data["timestamp"]
        bucket = bucket_timestamp(timestamp, AGGREGATION_BUCKET_SECONDS)

        # Buffer by option symbol
        option_symbol = data["option_symbol"]
        self.options_buffer[option_symbol].append(data)

        # Flush if buffer too large
        if len(self.options_buffer[option_symbol]) >= MAX_BUFFER_SIZE:
            self._flush_option_bucket(option_symbol, bucket)

    def _flush_option_bucket(self, option_symbol: str, bucket: datetime):
        """Aggregate and store 1-minute option quote"""
        buffer = self.options_buffer.get(option_symbol, [])

        if not buffer:
            return

        try:
            # Aggregate: last of each field
            last = buffer[-1]

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
                "delta": last.get("delta"),
                "gamma": last.get("gamma"),
                "theta": last.get("theta"),
                "vega": last.get("vega"),
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
            logger.debug(f"Stored option: {agg['option_symbol']} @ {agg['timestamp']} "
                        f"Last=${agg['last']:.2f}")

            # Clear buffer
            self.options_buffer[option_symbol] = []

        except Exception as e:
            logger.error(f"Error flushing option bucket for {option_symbol}: {e}", exc_info=True)
            self.errors_count += 1

    def _flush_all_buffers(self):
        """Flush all pending buffers"""
        logger.info("Flushing all buffers...")

        # Flush underlying
        self._flush_underlying_bucket()

        # Flush all options
        current_time = datetime.now(ET)
        bucket = bucket_timestamp(current_time, AGGREGATION_BUCKET_SECONDS)

        for option_symbol in list(self.options_buffer.keys()):
            self._flush_option_bucket(option_symbol, bucket)

        logger.info("✅ All buffers flushed")

    def _check_buffer_flush_timeout(self):
        """Check if buffers should be flushed due to timeout"""
        now = datetime.now(ET)

        if (now - self.last_flush_time).total_seconds() > BUFFER_FLUSH_INTERVAL:
            logger.debug("Buffer flush timeout reached, flushing...")
            self._flush_all_buffers()

    def run_backfill(self):
        """Run backfill phase"""
        logger.info("="*80)
        logger.info("BACKFILL PHASE")
        logger.info("="*80)

        # Check for gaps if enabled
        if BACKFILL_ON_STARTUP:
            gaps = self._detect_gaps()

            if gaps:
                logger.info(f"Found {len(gaps)} gaps to backfill")
                # Backfill gaps
                for gap in gaps:
                    logger.info(f"Backfilling gap: {gap['start']} to {gap['end']}")
                    # TODO: Implement targeted gap backfill
            else:
                logger.info("No gaps detected")

        # Regular backfill
        logger.info(f"Backfilling {self.lookback_days} days...")

        backfill = BackfillManager(
            client=self.client,
            underlying=self.underlying,
            num_expirations=self.num_expirations,
            strike_distance=self.strike_distance
        )

        try:
            for item in backfill.backfill(
                lookback_days=self.lookback_days,
                interval=1,
                unit="Minute",
                sample_every_n_bars=1
            ):
                if item["type"] == "underlying":
                    self._store_underlying(item["data"])
                elif item["type"] == "option":
                    self._store_option(item["data"])

                # Check for flush timeout
                self._check_buffer_flush_timeout()

        except Exception as e:
            logger.error(f"Backfill error: {e}", exc_info=True)
            self._flush_all_buffers()
            raise

        # Final flush
        self._flush_all_buffers()

        logger.info(f"\n✅ Backfill complete:")
        logger.info(f"   Underlying bars: {self.underlying_bars_stored}")
        logger.info(f"   Option quotes: {self.option_quotes_stored}")
        if GREEKS_ENABLED:
            logger.info(f"   Greeks calculated: {self.greeks_calculated}")

    def run_streaming(self):
        """Run streaming phase"""
        logger.info("="*80)
        logger.info("STREAMING PHASE")
        logger.info("="*80)

        stream_manager = StreamManager(
            client=self.client,
            underlying=self.underlying,
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
        """Run full ingestion pipeline: backfill → streaming"""
        logger.info("\n" + "="*80)
        logger.info("ZEROGEX MAIN INGESTION ENGINE")
        logger.info("="*80)
        logger.info(f"Underlying: {self.underlying}")
        logger.info(f"Expirations: {self.num_expirations}")
        logger.info(f"Strike Distance: ±${self.strike_distance}")
        logger.info(f"Lookback: {self.lookback_days} days")
        logger.info(f"Greeks: {'ENABLED' if GREEKS_ENABLED else 'DISABLED'}")
        logger.info("="*80 + "\n")

        try:
            # Phase 1: Backfill
            self.run_backfill()

            # Phase 2: Stream
            self.run_streaming()

        except Exception as e:
            logger.error(f"Fatal error in main engine: {e}", exc_info=True)
            sys.exit(1)
        finally:
            close_connection_pool()


def main():
    """Main entry point"""
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="ZeroGEX Main Ingestion Engine")
    parser.add_argument("--underlying", default=os.getenv("INGEST_UNDERLYING", "SPY"),
                       help="Underlying symbol (default: SPY)")
    parser.add_argument("--lookback-days", type=int,
                       default=int(os.getenv("INGEST_LOOKBACK_DAYS", "7")),
                       help="Days to backfill (default: 7)")
    parser.add_argument("--expirations", type=int,
                       default=int(os.getenv("INGEST_EXPIRATIONS", "3")),
                       help="Number of expirations (default: 3)")
    parser.add_argument("--strike-distance", type=float,
                       default=float(os.getenv("INGEST_STRIKE_DISTANCE", "10.0")),
                       help="Strike distance (default: 10.0)")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging")

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        from src.utils import set_log_level
        set_log_level("DEBUG")

    # Initialize client
    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID"),
        os.getenv("TRADESTATION_CLIENT_SECRET"),
        os.getenv("TRADESTATION_REFRESH_TOKEN"),
        sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true"
    )

    # Initialize and run engine
    engine = MainEngine(
        client=client,
        underlying=args.underlying,
        num_expirations=args.expirations,
        strike_distance=args.strike_distance,
        lookback_days=args.lookback_days
    )

    engine.run()


if __name__ == "__main__":
    main()
