"""
Backfill Manager - Independent Historical Data Backfilling

This manager fetches historical data and stores it directly in the database.
Updated to use Stream Bars API for proper UpVolume/DownVolume tracking.

Run independently when historical data is needed:
    python -m src.ingestion.backfill_manager --lookback-days 7
"""

import os
import time
from datetime import datetime, date, timedelta, timezone
from typing import List, Dict, Any, Optional
import pytz

from src.ingestion.tradestation_client import TradeStationClient
from src.ingestion.greeks_calculator import GreeksCalculator
from src.database import db_connection, close_connection_pool
from src.utils import get_logger
from src.validation import safe_float, safe_int, safe_datetime, validate_bar_data
from src.config import (
    OPTION_BATCH_SIZE,
    DELAY_BETWEEN_BATCHES,
    DELAY_BETWEEN_BARS,
    GREEKS_ENABLED,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


class BackfillManager:
    """Manages fetching and storing of historical underlying and options data"""

    def __init__(
        self,
        client: TradeStationClient,
        underlying: str = "SPY",
        num_expirations: int = 3,
        strike_distance: float = 10.0
    ):
        """Initialize backfill manager"""
        self.client = client
        self.underlying = underlying.upper()
        self.num_expirations = num_expirations
        self.strike_distance = strike_distance

        # Greeks calculator (initialize if enabled)
        self.greeks_calculator = None
        if GREEKS_ENABLED:
            self.greeks_calculator = GreeksCalculator()
            logger.info("✅ Greeks calculation ENABLED")
        else:
            logger.info("⚠️  Greeks calculation DISABLED")

        # Track latest underlying price for Greeks
        self.latest_underlying_price: Optional[float] = None

        # Metrics
        self.underlying_bars_stored = 0
        self.option_quotes_stored = 0
        self.greeks_calculated = 0

        logger.info(f"Initialized BackfillManager for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, ±${strike_distance} strikes")

    def _calculate_market_minutes(self, start_date: datetime, end_date: datetime) -> int:
        """
        Calculate number of 1-minute market hours between two dates

        Only counts minutes during market hours (including extended hours):
        - Monday-Friday: 4:00 AM - 8:00 PM ET
        - Excludes weekends

        Args:
            start_date: Start datetime
            end_date: End datetime

        Returns:
            Number of market minutes
        """
        if start_date >= end_date:
            return 0

        # Ensure datetimes are in ET
        if start_date.tzinfo != ET:
            start_date = start_date.astimezone(ET)
        if end_date.tzinfo != ET:
            end_date = end_date.astimezone(ET)

        total_minutes = 0
        current = start_date.replace(second=0, microsecond=0)
        end = end_date.replace(second=0, microsecond=0)

        # Market hours: 4:00 AM - 8:00 PM ET
        market_open = datetime.strptime("04:00:00", "%H:%M:%S").time()
        market_close = datetime.strptime("20:00:00", "%H:%M:%S").time()

        while current <= end:
            # Skip weekends
            if current.weekday() < 5:
                current_time = current.time()
                if market_open <= current_time <= market_close:
                    total_minutes += 1

            current += timedelta(minutes=1)

        return total_minutes

    def _get_underlying_bars(
        self,
        start_date: datetime,
        end_date: datetime,
        interval: int = 1,
        unit: str = "Minute"
    ) -> List[Dict[str, Any]]:
        """
        Fetch underlying price bars using regular Bars API

        Note: Regular bars API includes UpVolume and DownVolume,
        so we use get_bars() instead of get_stream_bars()
        """
        try:
            # For historical data, we may need to make multiple requests
            # TradeStation has limits on historical data retrieval

            logger.info(f"Fetching {interval}{unit} bars from {start_date} to {end_date}")

            # Calculate days between dates
            days_diff = (end_date - start_date).days

            all_bars = []

            # If requesting more than 30 days of 1-minute data, chunk it
            if unit == "Minute" and days_diff > 30:
                logger.info(f"Large date range ({days_diff} days), fetching in chunks...")

                current_start = start_date
                chunk_size = timedelta(days=30)

                while current_start < end_date:
                    current_end = min(current_start + chunk_size, end_date)

                    logger.info(f"Fetching chunk: {current_start} to {current_end}")

                    chunk_bars = self._fetch_bars_chunk(
                        current_start, current_end, interval, unit
                    )

                    all_bars.extend(chunk_bars)
                    current_start = current_end

                    # Rate limiting between chunks
                    if current_start < end_date:
                        time.sleep(2.0)
            else:
                # Single request for smaller ranges
                all_bars = self._fetch_bars_chunk(start_date, end_date, interval, unit)

            # Sort chronologically (oldest to newest)
            all_bars.sort(key=lambda x: x.get("TimeStamp", ""))

            logger.info(f"✅ Retrieved {len(all_bars)} bars for {self.underlying} (sorted oldest→newest)")

            return all_bars

        except Exception as e:
            logger.error(f"Error fetching underlying bars: {e}", exc_info=True)
            return []

    def _fetch_bars_chunk(
        self,
        start_date: datetime,
        end_date: datetime,
        interval: int,
        unit: str
    ) -> List[Dict[str, Any]]:
        """Fetch a single chunk of bars using regular bars API"""
        try:
            start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Use regular bars API - it includes UpVolume/DownVolume
            bars_data = self.client.get_bars(
                symbol=self.underlying,
                interval=interval,
                unit=unit,
                firstdate=start_str,
                lastdate=end_str,
                sessiontemplate="USEQ24Hour",
                warn_if_closed=False
            )

            if "Bars" not in bars_data or len(bars_data["Bars"]) == 0:
                logger.warning(f"No bar data returned for range {start_str} to {end_str}")
                return []

            return bars_data["Bars"]

        except Exception as e:
            logger.error(f"Error fetching bars chunk: {e}", exc_info=True)
            return []

    def _get_expirations_for_date(self, as_of_date: date) -> List[date]:
        """Get expirations available on a given date"""
        try:
            all_expirations = self.client.get_option_expirations(self.underlying)

            if not all_expirations:
                logger.warning(f"No expirations found for {self.underlying}")
                return []

            # Filter to expirations >= as_of_date
            future_exps = [exp for exp in all_expirations if exp >= as_of_date]

            # Take first N
            target_exps = future_exps[:self.num_expirations]

            logger.debug(f"Expirations for {as_of_date}: {[str(e) for e in target_exps]}")
            return target_exps

        except Exception as e:
            logger.error(f"Error fetching expirations: {e}", exc_info=True)
            return []

    def _get_strikes_near_price(self, expiration: date, price: float) -> List[float]:
        """Get strikes within configured distance of price"""
        try:
            exp_str = expiration.strftime("%m-%d-%Y")
            all_strikes = self.client.get_option_strikes(self.underlying, expiration=exp_str)

            if not all_strikes:
                logger.warning(f"No strikes found for exp {exp_str}")
                return []

            min_strike = price - self.strike_distance
            max_strike = price + self.strike_distance

            nearby_strikes = [
                strike for strike in all_strikes
                if min_strike <= strike <= max_strike
            ]

            return sorted(nearby_strikes)

        except Exception as e:
            logger.error(f"Error fetching strikes: {e}", exc_info=True)
            return []

    def _build_option_symbols_for_bar(self, bar_date: date, bar_price: float) -> List[str]:
        """Build option symbols for a specific bar"""
        expirations = self._get_expirations_for_date(bar_date)
        option_symbols = []

        for expiration in expirations:
            strikes = self._get_strikes_near_price(expiration, bar_price)

            for strike in strikes:
                call_symbol = self.client.build_option_symbol(
                    self.underlying, expiration, "C", strike
                )
                put_symbol = self.client.build_option_symbol(
                    self.underlying, expiration, "P", strike
                )

                option_symbols.append(call_symbol)
                option_symbols.append(put_symbol)

        return option_symbols

    def _store_underlying_bar(self, bar_data: Dict[str, Any]):
        """Store underlying bar directly in database"""
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
                    bar_data["symbol"],
                    bar_data["timestamp"],
                    bar_data["open"],
                    bar_data["high"],
                    bar_data["low"],
                    bar_data["close"],
                    bar_data["up_volume"],
                    bar_data["down_volume"]
                ))
                conn.commit()

            self.underlying_bars_stored += 1

        except Exception as e:
            logger.error(f"Error storing underlying bar: {e}", exc_info=True)

    def _store_option_quote(self, option_data: Dict[str, Any]):
        """Store option quote directly in database"""
        try:
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
                    option_data["option_symbol"],
                    option_data["timestamp"],
                    option_data["underlying"],
                    option_data["strike"],
                    option_data["expiration"],
                    option_data["option_type"],
                    option_data["last"],
                    option_data["bid"],
                    option_data["ask"],
                    option_data["volume"],
                    option_data["open_interest"],
                    option_data.get("delta"),
                    option_data.get("gamma"),
                    option_data.get("theta"),
                    option_data.get("vega")
                ))
                conn.commit()

            self.option_quotes_stored += 1

        except Exception as e:
            logger.error(f"Error storing option quote: {e}", exc_info=True)

    def backfill(
        self,
        lookback_days: int = 1,
        interval: int = 1,
        unit: str = "Minute",
        sample_every_n_bars: int = 1
    ):
        """
        Fetch historical data and store directly in database

        Args:
            lookback_days: Number of days to look back
            interval: Bar interval
            unit: Time unit
            sample_every_n_bars: Sample options every N bars
        """
        logger.info(f"Starting backfill for {self.underlying}")
        logger.info(f"Lookback: {lookback_days} days, Interval: {interval}{unit}")
        logger.info(f"Sampling options every {sample_every_n_bars} bar(s)")

        # Calculate date range
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=lookback_days)

        # Calculate total market minutes for progress tracking
        total_market_minutes = self._calculate_market_minutes(start_date, end_date)
        logger.info(f"Total market minutes in range: {total_market_minutes:,}")

        # Fetch underlying bars
        bars = self._get_underlying_bars(start_date, end_date, interval, unit)

        if not bars:
            logger.error("No bars retrieved, aborting backfill")
            return

        logger.info(f"Processing {len(bars)} bars in chronological order (oldest→newest)...")

        # Track progress
        minutes_processed = 0
        last_progress_pct = 0

        # Process each bar
        for i, bar in enumerate(bars):
            try:
                # Validate bar data
                if not validate_bar_data(bar):
                    logger.warning(f"Bar {i}: Invalid data, skipping")
                    continue

                # Parse bar timestamp
                timestamp_str = bar.get("TimeStamp", "")
                bar_dt = safe_datetime(timestamp_str, field_name="TimeStamp")

                if not bar_dt:
                    logger.warning(f"Bar {i}: Invalid timestamp, skipping")
                    continue

                bar_date = bar_dt.date()

                # Parse OHLCV with volume breakdown
                open_price = safe_float(bar.get("Open"), field_name="Open")
                high_price = safe_float(bar.get("High"), field_name="High")
                low_price = safe_float(bar.get("Low"), field_name="Low")
                close_price = safe_float(bar.get("Close"), field_name="Close")
                up_volume = safe_int(bar.get("UpVolume"), field_name="UpVolume")
                down_volume = safe_int(bar.get("DownVolume"), field_name="DownVolume")
                total_volume = safe_int(bar.get("TotalVolume"), field_name="TotalVolume")

                if close_price == 0:
                    logger.warning(f"Bar {i}: Zero close price, skipping")
                    continue

                # Update progress tracking
                minutes_processed = self._calculate_market_minutes(start_date, bar_dt)
                progress_pct = (minutes_processed / total_market_minutes * 100) if total_market_minutes > 0 else 0

                # Log progress every 5% increment
                if int(progress_pct / 5) > int(last_progress_pct / 5):
                    logger.info(f"Progress: {progress_pct:.1f}% [{minutes_processed:,}/{total_market_minutes:,} market minutes] - "
                               f"Processing {bar_dt.strftime('%Y-%m-%d %H:%M ET')}")
                    last_progress_pct = progress_pct

                # Store underlying bar
                underlying_data = {
                    "symbol": self.underlying,
                    "timestamp": bar_dt,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "up_volume": up_volume,
                    "down_volume": down_volume,
                }

                self._store_underlying_bar(underlying_data)

                # Update latest price for Greeks
                self.latest_underlying_price = close_price

                logger.debug(f"Bar {i+1}/{len(bars)}: {self.underlying} @ {bar_dt} "
                           f"C=${close_price:.2f} UpVol={up_volume:,} DownVol={down_volume:,}")

                # Check if we should sample options for this bar
                if i % sample_every_n_bars != 0:
                    logger.debug(f"Skipping options (sampling every {sample_every_n_bars} bars)")
                    continue

                # Build option symbols
                logger.debug(f"Fetching options chain at ${close_price:.2f}...")
                option_symbols = self._build_option_symbols_for_bar(bar_date, close_price)

                if not option_symbols:
                    logger.debug("No option symbols to fetch")
                    continue

                logger.debug(f"Fetching quotes for {len(option_symbols)} options...")

                # Fetch options quotes in batches
                for j in range(0, len(option_symbols), OPTION_BATCH_SIZE):
                    batch = option_symbols[j:j + OPTION_BATCH_SIZE]

                    try:
                        options_data = self.client.get_option_quotes(batch)

                        if "Quotes" in options_data:
                            logger.debug(f"Batch {j//OPTION_BATCH_SIZE + 1}: "
                                       f"{len(options_data['Quotes'])} quotes")

                            for opt_quote in options_data["Quotes"]:
                                # Parse option quote
                                option_symbol = opt_quote.get("Symbol", "")

                                # Parse option symbol
                                parts = option_symbol.split()
                                if len(parts) < 2:
                                    logger.warning(f"Invalid option symbol: {option_symbol}")
                                    continue

                                option_part = parts[1]
                                option_type = "C" if "C" in option_part else "P"

                                exp_str = option_part[:6]
                                try:
                                    expiration = datetime.strptime(exp_str, "%y%m%d").date()
                                except ValueError:
                                    logger.warning(f"Invalid expiration in {option_symbol}")
                                    continue

                                strike_str = option_part.split(option_type)[1]
                                strike = safe_float(strike_str, field_name="strike")

                                # Parse quote data
                                last = safe_float(opt_quote.get("Last"), field_name="Last")
                                bid = safe_float(opt_quote.get("Bid"), field_name="Bid")
                                ask = safe_float(opt_quote.get("Ask"), field_name="Ask")
                                volume = safe_int(opt_quote.get("Volume"), field_name="Volume")
                                open_interest = safe_int(opt_quote.get("OpenInterest"),
                                                        field_name="OpenInterest")

                                # Build option data
                                option_data = {
                                    "option_symbol": option_symbol,
                                    "timestamp": bar_dt,
                                    "underlying": self.underlying,
                                    "strike": strike,
                                    "expiration": expiration,
                                    "option_type": option_type,
                                    "last": last,
                                    "bid": bid,
                                    "ask": ask,
                                    "volume": volume,
                                    "open_interest": open_interest,
                                }

                                # Calculate Greeks if enabled
                                if self.greeks_calculator and self.latest_underlying_price:
                                    try:
                                        option_data = self.greeks_calculator.enrich_option_data(
                                            option_data,
                                            self.latest_underlying_price
                                        )
                                        self.greeks_calculated += 1
                                    except Exception as e:
                                        logger.error(f"Error calculating Greeks: {e}")
                                        option_data["delta"] = None
                                        option_data["gamma"] = None
                                        option_data["theta"] = None
                                        option_data["vega"] = None

                                # Store option quote
                                self._store_option_quote(option_data)

                        # Rate limiting between batches
                        time.sleep(DELAY_BETWEEN_BATCHES)

                    except Exception as e:
                        logger.error(f"Error fetching options batch: {e}")

                # Rate limiting between bars
                if i < len(bars) - 1:
                    time.sleep(DELAY_BETWEEN_BARS)

            except Exception as e:
                logger.error(f"Error processing bar {i}: {e}", exc_info=True)
                continue

        # Final progress update
        logger.info(f"Progress: 100.0% [{total_market_minutes:,}/{total_market_minutes:,} market minutes] - Complete!")
        logger.info("Backfill complete - all data stored in database")


def main():
    """Standalone backfill"""
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="Backfill historical options data")
    parser.add_argument("--underlying", default=os.getenv("BACKFILL_UNDERLYING", "SPY"),
                       help="Underlying symbol (default: SPY)")
    parser.add_argument("--lookback-days", type=int, 
                       default=int(os.getenv("BACKFILL_LOOKBACK_DAYS", "1")),
                       help="Days to backfill (default: 1)")
    parser.add_argument("--interval", type=int,
                       default=int(os.getenv("BACKFILL_INTERVAL", "1")),
                       help="Bar interval (default: 1)")
    parser.add_argument("--unit", default=os.getenv("BACKFILL_UNIT", "Minute"),
                       choices=["Minute", "Daily", "Weekly", "Monthly"],
                       help="Bar unit (default: Minute)")
    parser.add_argument("--expirations", type=int,
                       default=int(os.getenv("BACKFILL_EXPIRATIONS", "3")),
                       help="Number of expirations to track (default: 3)")
    parser.add_argument("--strike-distance", type=float,
                       default=float(os.getenv("BACKFILL_STRIKE_DISTANCE", "10.0")),
                       help="Strike distance from price (default: 10.0)")
    parser.add_argument("--sample-every", type=int,
                       default=int(os.getenv("BACKFILL_SAMPLE_EVERY", "1")),
                       help="Sample options every N bars (default: 1)")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging")

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        from src.utils import set_log_level
        set_log_level("DEBUG")

    print("\n" + "="*80)
    print("BACKFILL MANAGER - INDEPENDENT HISTORICAL DATA BACKFILL")
    print("="*80)
    print(f"Underlying: {args.underlying}")
    print(f"Lookback: {args.lookback_days} days")
    print(f"Interval: {args.interval}{args.unit}")
    print(f"Expirations: {args.expirations}")
    print(f"Strike Distance: ±${args.strike_distance}")
    print(f"Sample Every: {args.sample_every} bar(s)")
    print(f"Greeks: {'ENABLED' if GREEKS_ENABLED else 'DISABLED'}")
    print("="*80 + "\n")

    # Initialize client
    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID"),
        os.getenv("TRADESTATION_CLIENT_SECRET"),
        os.getenv("TRADESTATION_REFRESH_TOKEN"),
        sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true"
    )

    # Initialize backfill manager
    manager = BackfillManager(
        client=client,
        underlying=args.underlying,
        num_expirations=args.expirations,
        strike_distance=args.strike_distance
    )

    try:
        # Run backfill
        manager.backfill(
            lookback_days=args.lookback_days,
            interval=args.interval,
            unit=args.unit,
            sample_every_n_bars=args.sample_every
        )

        print("\n" + "="*80)
        print("BACKFILL COMPLETE")
        print("="*80)
        print(f"✅ Underlying bars stored: {manager.underlying_bars_stored}")
        print(f"✅ Option quotes stored: {manager.option_quotes_stored}")
        if GREEKS_ENABLED:
            print(f"✅ Greeks calculated: {manager.greeks_calculated}")
        print("="*80 + "\n")

    except KeyboardInterrupt:
        print("\n\n⚠️  Backfill interrupted by user")
        print(f"Partial results: {manager.underlying_bars_stored} underlying, "
              f"{manager.option_quotes_stored} options stored")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        logger.error(f"Backfill failed: {e}", exc_info=True)
        import sys
        sys.exit(1)
    finally:
        close_connection_pool()


if __name__ == "__main__":
    main()
