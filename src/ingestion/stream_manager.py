"""
Stream Manager - Streams real-time data and yields to IngestionEngine

Updated to use TradeStation Stream Bars API for underlying quotes,
which provides proper UpVolume and DownVolume tracking.

This manager ONLY fetches data from TradeStation API.
Storage is handled by IngestionEngine.
"""

import os
import time
from datetime import datetime, date
from typing import Generator, List, Dict, Any, Optional, Set
import pytz

from src.ingestion.tradestation_client import TradeStationClient
from src.utils import get_logger
from src.validation import (
    safe_float, safe_int, safe_datetime,
    get_market_session
)
from src.config import (
    OPTION_BATCH_SIZE,
    MARKET_HOURS_POLL_INTERVAL,
    EXTENDED_HOURS_POLL_INTERVAL,
    CLOSED_HOURS_POLL_INTERVAL,
    STRIKE_RECALC_INTERVAL,
    PRICE_MOVE_THRESHOLD,
    STRIKE_CLEANUP_INTERVAL,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


class StreamManager:
    """Manages streaming of real-time underlying and options data"""

    def __init__(
        self,
        client: TradeStationClient,
        underlying: str = "SPY",
        num_expirations: int = 3,
        strike_distance: float = 10.0,
    ):
        """Initialize stream manager"""
        self.client = client
        self.underlying = underlying.upper()
        self.num_expirations = num_expirations
        self.strike_distance = strike_distance

        # Track state
        self.current_price: Optional[float] = None
        self.target_expirations: List[date] = []
        self.tracked_strikes: Set[float] = set()
        self.tracked_option_symbols: List[str] = []

        # Track expired strikes for cleanup
        self.all_tracked_strikes: Dict[date, Set[float]] = {}

        # Track last expiration refresh time
        self.last_expiration_refresh: Optional[datetime] = None

        logger.info(f"Initialized StreamManager for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, ±${strike_distance} strikes")

    def _fetch_underlying_bar(self) -> Optional[Dict[str, Any]]:
        """
        Fetch latest underlying quote and normalize to in-progress minute bar.

        This keeps the latest row in underlying_quotes updating throughout the minute,
        instead of waiting for the next completed minute bar.
        """
        try:
            quote_data = self.client.get_quote(self.underlying, warn_if_closed=False)
            quotes = quote_data.get("Quotes", [])
            if not quotes:
                logger.debug(f"No quote data returned for {self.underlying}")
                return None

            quote = quotes[0]
            last_price = safe_float(quote.get("Last"), field_name="Last")
            if last_price <= 0:
                logger.warning(f"Invalid live last price for {self.underlying}: {last_price}")
                return None

            quote_ts = safe_datetime(
                quote.get("TradeTime")
                or quote.get("TimeStamp")
                or quote.get("CloseTime")
                or "",
                default=datetime.now(ET),
                field_name="TradeTime"
            )
            if not quote_ts:
                quote_ts = datetime.now(ET)

            minute_ts = quote_ts.replace(second=0, microsecond=0)

            raw_total_volume = quote.get("TotalVolume") or quote.get("Volume") or quote.get("DailyVolume")
            total_volume = safe_int(raw_total_volume, field_name="TotalVolume") if raw_total_volume is not None else None

            raw_up_volume = quote.get("UpVolume") or quote.get("DailyUpVolume")
            up_volume = safe_int(raw_up_volume, field_name="UpVolume") if raw_up_volume is not None else None

            raw_down_volume = quote.get("DownVolume") or quote.get("DailyDownVolume")
            down_volume = safe_int(raw_down_volume, field_name="DownVolume") if raw_down_volume is not None else None

            underlying_data = {
                "symbol": self.underlying,
                "timestamp": minute_ts,
                "open": last_price,
                "high": last_price,
                "low": last_price,
                "close": last_price,
                "up_volume": up_volume,
                "down_volume": down_volume,
                "volume": total_volume,
            }

            logger.debug(
                f"Live quote normalized as bar: {self.underlying} @ {quote_ts} "
                f"(bucket={minute_ts}) C=${underlying_data['close']:.2f} Vol={underlying_data['volume'] if underlying_data['volume'] is not None else 'n/a'} "
                f"UpVol={underlying_data['up_volume'] if underlying_data['up_volume'] is not None else 'n/a'} "
                f"DownVol={underlying_data['down_volume'] if underlying_data['down_volume'] is not None else 'n/a'}"
            )
            return underlying_data

        except Exception as e:
            logger.error(f"Error fetching underlying quote: {e}", exc_info=True)
            return None

    def _get_underlying_price(self) -> Optional[float]:
        """
        Fetch current underlying price

        Used only for initialization and strike recalculation
        For streaming data, use _fetch_underlying_bar()
        """
        try:
            # Fetch latest bar and extract close price
            bar_data = self._fetch_underlying_bar()

            if bar_data:
                price = bar_data["close"]
                logger.debug(f"Current {self.underlying} price: ${price:.2f}")
                return price

            return None

        except Exception as e:
            logger.error(f"Error fetching underlying price: {e}", exc_info=True)
            return None

    def _should_refresh_expirations(self) -> bool:
        """
        Check if expirations need to be refreshed

        Refresh conditions:
        1. First time (never refreshed)
        2. Market close occurred since last refresh (4:00 PM ET)
        3. Any tracked expiration has expired (is in the past)

        Returns:
            True if expirations should be refreshed
        """
        now_et = datetime.now(ET)
        today = now_et.date()

        # First time - always refresh
        if self.last_expiration_refresh is None:
            logger.info("First expiration refresh needed")
            return True

        # Check if any tracked expiration has expired
        if self.target_expirations:
            earliest_exp = min(self.target_expirations)
            if earliest_exp < today:
                logger.info(f"Expiration {earliest_exp} has passed, refresh needed")
                return True

        # Check if we've crossed 4:00 PM ET since last refresh
        last_refresh_et = self.last_expiration_refresh.astimezone(ET)
        market_close_time = datetime.strptime("16:00:00", "%H:%M:%S").time()

        # If last refresh was before today's 4:00 PM and now is after 4:00 PM
        if (last_refresh_et.date() < now_et.date() or 
            (last_refresh_et.date() == now_et.date() and 
             last_refresh_et.time() < market_close_time and 
             now_et.time() >= market_close_time)):
            logger.info("Market close occurred since last refresh, expirations may need update")
            return True

        return False

    def _refresh_expirations(self) -> bool:
        """
        Refresh target expirations and rebuild option symbols

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Refreshing target expirations...")

            # Get fresh expirations
            new_expirations = self._get_target_expirations()

            if not new_expirations:
                logger.error("Failed to get new expirations")
                return False

            # Check if expirations actually changed
            if new_expirations == self.target_expirations:
                logger.info("Expirations unchanged, skipping rebuild")
                self.last_expiration_refresh = datetime.now(ET)
                return True

            # Log the change
            logger.info(f"Expirations changed:")
            logger.info(f"  Old: {[str(exp) for exp in self.target_expirations]}")
            logger.info(f"  New: {[str(exp) for exp in new_expirations]}")

            # Update expirations
            self.target_expirations = new_expirations

            # Rebuild option symbols with new expirations
            if self.current_price:
                self.tracked_option_symbols = self._build_option_symbols()
                logger.info(f"Rebuilt {len(self.tracked_option_symbols)} option symbols with new expirations")

            # Update refresh timestamp
            self.last_expiration_refresh = datetime.now(ET)

            return True

        except Exception as e:
            logger.error(f"Error refreshing expirations: {e}", exc_info=True)
            return False

    def _get_target_expirations(self) -> List[date]:
        """Get target expiration dates"""
        try:
            all_expirations = self.client.get_option_expirations(self.underlying)

            if not all_expirations:
                logger.warning(f"No expirations found for {self.underlying}")
                return []

            # Filter to future expirations
            today = date.today()
            future_expirations = [exp for exp in all_expirations if exp >= today]

            if not future_expirations:
                logger.warning("No future expirations available")
                return []

            # Take first N
            target_exps = future_expirations[:self.num_expirations]

            logger.info(f"Target expirations: {[str(exp) for exp in target_exps]}")
            return target_exps

        except Exception as e:
            logger.error(f"Error fetching expirations: {e}", exc_info=True)
            return []

    def _get_strikes_near_price(self, expiration: date, current_price: float) -> List[float]:
        """Get strikes within configured distance"""
        try:
            exp_str = expiration.strftime("%m-%d-%Y")
            all_strikes = self.client.get_option_strikes(self.underlying, expiration=exp_str)

            if not all_strikes:
                logger.warning(f"No strikes found for exp {exp_str}")
                return []

            min_strike = current_price - self.strike_distance
            max_strike = current_price + self.strike_distance

            nearby_strikes = [
                strike for strike in all_strikes
                if min_strike <= strike <= max_strike
            ]

            logger.debug(f"Exp {exp_str}: {len(nearby_strikes)} strikes in range "
                        f"[${min_strike:.2f}, ${max_strike:.2f}]")

            return sorted(nearby_strikes)

        except Exception as e:
            logger.error(f"Error fetching strikes for {expiration}: {e}", exc_info=True)
            return []

    def _build_option_symbols(self) -> List[str]:
        """Build list of option symbols to track"""
        if not self.current_price:
            logger.warning("No current price, cannot build option symbols")
            return []

        option_symbols = []
        self.tracked_strikes = set()
        self.all_tracked_strikes = {}

        for expiration in self.target_expirations:
            strikes = self._get_strikes_near_price(expiration, self.current_price)
            self.all_tracked_strikes[expiration] = set(strikes)

            for strike in strikes:
                call_symbol = self.client.build_option_symbol(
                    self.underlying, expiration, "C", strike
                )
                put_symbol = self.client.build_option_symbol(
                    self.underlying, expiration, "P", strike
                )

                option_symbols.append(call_symbol)
                option_symbols.append(put_symbol)
                self.tracked_strikes.add(strike)

        logger.info(f"Built {len(option_symbols)} option symbols to track")
        return option_symbols

    def _cleanup_expired_strikes(self):
        """Remove strikes for expired expirations to prevent memory leak"""
        today = date.today()
        expired = [exp for exp in self.all_tracked_strikes.keys() if exp < today]

        for exp in expired:
            del self.all_tracked_strikes[exp]
            logger.debug(f"Cleaned up strikes for expired expiration: {exp}")

    def initialize(self) -> bool:
        """Initialize stream"""
        logger.info(f"Initializing stream for {self.underlying}...")

        # Get current price
        self.current_price = self._get_underlying_price()
        if not self.current_price:
            logger.error("Failed to get underlying price")
            return False

        # Get target expirations
        self.target_expirations = self._get_target_expirations()
        if not self.target_expirations:
            logger.error("Failed to get target expirations")
            return False

        # Build option symbols
        self.tracked_option_symbols = self._build_option_symbols()
        if not self.tracked_option_symbols:
            logger.error("Failed to build option symbols")
            return False

        # Set initial refresh timestamp (NEW)
        self.last_expiration_refresh = datetime.now(ET)

        logger.info(f"✅ Initialization complete:")
        logger.info(f"   Price: ${self.current_price:.2f}")
        logger.info(f"   Tracking {len(self.target_expirations)} expirations")
        logger.info(f"   Tracking {len(self.tracked_option_symbols)} option contracts")

        return True

    def stream(
        self,
        max_iterations: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Stream real-time data and yield to caller

        Yields dictionaries with:
            {
                'type': 'underlying' | 'option',
                'data': {...}
            }
        """
        if not self.tracked_option_symbols:
            logger.error("Not initialized. Call initialize() first.")
            return

        logger.info("Starting stream loop...")
        logger.info("Press Ctrl+C to stop")

        iteration = 0

        while True:
            iteration += 1

            # Get current market session for dynamic polling
            session = get_market_session()

            # Determine poll interval based on session
            if session == "regular":
                poll_interval = MARKET_HOURS_POLL_INTERVAL
            elif session in ["pre-market", "after-hours"]:
                poll_interval = EXTENDED_HOURS_POLL_INTERVAL
            else:  # closed
                poll_interval = CLOSED_HOURS_POLL_INTERVAL

            logger.info(f"Iteration {iteration} - {datetime.now(ET).strftime('%Y-%m-%d %H:%M:%S ET')} "
                       f"[{session}]")

            # Check if expirations need refresh (NEW)
            if self._should_refresh_expirations():
                logger.info("Refreshing expirations...")
                if self._refresh_expirations():
                    logger.info("✅ Expirations refreshed successfully")
                else:
                    logger.warning("⚠️  Expiration refresh failed, continuing with current expirations")

            # Track option count for debugging
            option_count = 0

            try:
                # Fetch underlying bar using Stream Bars API
                underlying_data = self._fetch_underlying_bar()

                if underlying_data:
                    # Update current price for strike calculations
                    self.current_price = underlying_data["close"]

                    # Yield underlying data
                    yield {"type": "underlying", "data": underlying_data}

                # Fetch options quotes in batches
                for i in range(0, len(self.tracked_option_symbols), OPTION_BATCH_SIZE):
                    batch = self.tracked_option_symbols[i:i + OPTION_BATCH_SIZE]

                    try:
                        options_data = self.client.get_option_quotes(batch)

                        if "Quotes" in options_data:
                            for opt_quote in options_data["Quotes"]:
                                # Parse option symbol
                                option_symbol = opt_quote.get("Symbol", "")
                                parts = option_symbol.split()

                                if len(parts) < 2:
                                    continue

                                option_part = parts[1]
                                option_type = "C" if "C" in option_part else "P"

                                exp_str = option_part[:6]
                                try:
                                    expiration = datetime.strptime(exp_str, "%y%m%d").date()
                                except ValueError:
                                    continue

                                strike_str = option_part.split(option_type)[1]
                                strike = safe_float(strike_str, field_name="strike")

                                # Parse timestamp
                                timestamp_str = opt_quote.get("TimeStamp", "")
                                timestamp = safe_datetime(timestamp_str, field_name="TimeStamp")

                                if not timestamp:
                                    timestamp = datetime.now(ET)

                                # Parse quote data
                                last = safe_float(opt_quote.get("Last"), field_name="Last")
                                bid = safe_float(opt_quote.get("Bid"), field_name="Bid")
                                ask = safe_float(opt_quote.get("Ask"), field_name="Ask")
                                volume = safe_int(opt_quote.get("Volume"), field_name="Volume")
                                open_interest = safe_int(opt_quote.get("DailyOpenInterest"), field_name="DailyOpenInterest")

                                # Try multiple field names for implied volatility
                                # TradeStation may use different field names
                                implied_volatility = None
                                for iv_field in ["ImpliedVolatility", "IV", "Volatility", "IVol"]:
                                    iv_value = safe_float(opt_quote.get(iv_field), field_name=iv_field)
                                    if iv_value and iv_value > 0:
                                        implied_volatility = iv_value
                                        break

                                # Log what we're getting from API (only first option for debugging)
                                if option_count == 0:
                                    logger.debug(f"Sample option quote from API: {opt_quote}")
                                    logger.debug(f"  Available fields: {list(opt_quote.keys())}")
                                    logger.debug(f"  OpenInterest: {opt_quote.get('OpenInterest')}")
                                    logger.debug(f"  DailyOpenInterest: {opt_quote.get('DailyOpenInterest')}")
                                    logger.debug(f"  ImpliedVolatility found: {implied_volatility}")
                                    # Check for any field containing 'vol' or 'IV'
                                    vol_fields = {k: v for k, v in opt_quote.items() 
                                                 if 'vol' in k.lower() or 'iv' in k.lower()}
                                    if vol_fields:
                                        logger.debug(f"  Fields containing 'vol' or 'iv': {vol_fields}")

                                # Yield option data
                                option_data = {
                                    "option_symbol": option_symbol,
                                    "timestamp": timestamp,
                                    "underlying": self.underlying,
                                    "strike": strike,
                                    "expiration": expiration,
                                    "option_type": option_type,
                                    "last": last,
                                    "bid": bid,
                                    "ask": ask,
                                    "volume": volume,
                                    "open_interest": open_interest,
                                    "implied_volatility": implied_volatility if implied_volatility else None,
                                }

                                yield {"type": "option", "data": option_data}
                                option_count += 1

                    except Exception as e:
                        logger.error(f"Error fetching options batch: {e}")

                # Check if we should recalculate strikes
                if iteration % STRIKE_RECALC_INTERVAL == 0:
                    if self.current_price:
                        # Check if we have a previous price to compare
                        # If first time, just log current price
                        if iteration == STRIKE_RECALC_INTERVAL:
                            logger.debug(f"Current price: ${self.current_price:.2f}")
                        else:
                            # Get fresh price data
                            new_price = self._get_underlying_price()
                            if new_price and abs(new_price - self.current_price) > PRICE_MOVE_THRESHOLD:
                                logger.info(f"Price moved from ${self.current_price:.2f} to ${new_price:.2f}")
                                logger.info("Recalculating tracked strikes...")
                                self.current_price = new_price
                                self.tracked_option_symbols = self._build_option_symbols()

                # Cleanup expired strikes periodically
                if iteration % STRIKE_CLEANUP_INTERVAL == 0:
                    self._cleanup_expired_strikes()

                # Check max iterations
                if max_iterations and iteration >= max_iterations:
                    logger.info(f"Reached max iterations ({max_iterations})")
                    break

                # Sleep with dynamic interval
                logger.debug(f"Sleeping for {poll_interval}s...")
                time.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Stream iteration error: {e}", exc_info=True)
                time.sleep(poll_interval)

        logger.info("Stream stopped")


def main():
    """Standalone streaming for testing"""
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="Stream real-time options data")
    parser.add_argument("--underlying", default=os.getenv("STREAM_UNDERLYING", "SPY"),
                       help="Underlying symbol (default: SPY)")
    parser.add_argument("--expirations", type=int,
                       default=int(os.getenv("STREAM_EXPIRATIONS", "3")),
                       help="Number of expirations to track (default: 3)")
    parser.add_argument("--strike-distance", type=float,
                       default=float(os.getenv("STREAM_STRIKE_DISTANCE", "10.0")),
                       help="Strike distance from price (default: 10.0)")
    parser.add_argument("--max-iterations", type=int,
                       help="Maximum iterations (default: unlimited)")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging")

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        from src.utils import set_log_level
        set_log_level("DEBUG")

    print("\n" + "="*80)
    print("STREAM MANAGER - STANDALONE TEST")
    print("="*80)
    print(f"Underlying: {args.underlying}")
    print(f"Expirations: {args.expirations}")
    print(f"Strike Distance: ±${args.strike_distance}")
    if args.max_iterations:
        print(f"Max Iterations: {args.max_iterations}")
    else:
        print("Max Iterations: Unlimited (press Ctrl+C to stop)")
    print("="*80 + "\n")

    # Initialize client
    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID"),
        os.getenv("TRADESTATION_CLIENT_SECRET"),
        os.getenv("TRADESTATION_REFRESH_TOKEN"),
        sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true"
    )

    # Initialize stream manager
    manager = StreamManager(
        client=client,
        underlying=args.underlying,
        num_expirations=args.expirations,
        strike_distance=args.strike_distance
    )

    # Initialize
    if not manager.initialize():
        print("❌ Failed to initialize stream manager")
        import sys
        sys.exit(1)

    print()

    # Track counts
    underlying_count = 0
    option_count = 0

    try:
        # Run stream and count yielded items
        for item in manager.stream(max_iterations=args.max_iterations):
            if item["type"] == "underlying":
                underlying_count += 1
                if underlying_count % 10 == 0:
                    data = item["data"]
                    up = data.get('up_volume')
                    down = data.get('down_volume')
                    print(f"Underlying bars: {underlying_count} - Latest: "
                          f"${data['close']:.2f} "
                          f"(Up: {up if up is not None else 'n/a'}, Down: {down if down is not None else 'n/a'})")
            elif item["type"] == "option":
                option_count += 1
                if option_count % 100 == 0:
                    print(f"Option quotes: {option_count}")

        print("\n" + "="*80)
        print("STREAM COMPLETE")
        print("="*80)
        print(f"✅ Underlying bars yielded: {underlying_count}")
        print(f"✅ Option quotes yielded: {option_count}")
        print("="*80 + "\n")
        print("NOTE: This standalone test only YIELDS data, it does NOT store it.")
        print("Use 'python run.py ingest' to stream AND store data in database.")
        print()

    except KeyboardInterrupt:
        print("\n\n⚠️  Stream interrupted by user")
        print(f"Results: {underlying_count} underlying, {option_count} options yielded")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        logger.error(f"Stream failed: {e}", exc_info=True)
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()
