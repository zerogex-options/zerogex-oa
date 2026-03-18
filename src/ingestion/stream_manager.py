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
    validate_bar_data, get_market_session
)
from src.symbols import resolve_option_root, get_weekly_option_roots
from src.config import (
    OPTION_BATCH_SIZE,
    MARKET_HOURS_POLL_INTERVAL,
    EXTENDED_HOURS_POLL_INTERVAL,
    CLOSED_HOURS_POLL_INTERVAL,
    STRIKE_RECALC_INTERVAL,
    STRIKE_CLEANUP_INTERVAL,
    SESSION_TEMPLATE,
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
        db_underlying: str = None,
        num_expirations: int = 3,
        num_strikes: int = 10,
    ):
        """Initialize stream manager"""
        self.client = client
        self.underlying = underlying.upper()           # TradeStation API symbol for underlying (e.g. "$SPX.X")
        self.db_underlying = (db_underlying or underlying).upper()  # canonical alias for DB (e.g. "SPX")
        self.option_root = resolve_option_root(self.underlying)  # option root for expirations/chains (e.g. "SPXW")
        self.num_expirations = num_expirations
        self.num_strikes = num_strikes  # number of strikes to track on each side of current price

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
        logger.info(f"Config: {num_expirations} expirations, {num_strikes} strikes each side")

    def _fetch_underlying_bar(self) -> Optional[Dict[str, Any]]:
        """
        Fetch latest underlying bar with volume breakdown using regular Bars API

        Note: The regular bars endpoint includes UpVolume/DownVolume,
        so we don't need the stream/barcharts endpoint.

        Returns bar data with OHLC + UpVolume/DownVolume
        """
        try:
            # Use regular bars API with barsback=1 to get latest completed bar
            # This works reliably and includes UpVolume/DownVolume
            bars_data = self.client.get_bars(
                symbol=self.underlying,
                interval=1,
                unit="Minute",
                barsback=1,
                sessiontemplate=SESSION_TEMPLATE,
                warn_if_closed=False
            )

            if "Bars" not in bars_data or len(bars_data["Bars"]) == 0:
                logger.debug(f"No bar data returned for {self.underlying} - likely between bars or market just opened")
                return None

            bar = bars_data["Bars"][0]

            # Validate bar data
            if not validate_bar_data(bar):
                logger.warning("Invalid bar data, skipping")
                return None

            # Parse bar timestamp
            timestamp_str = bar.get("TimeStamp", "")
            timestamp = safe_datetime(timestamp_str, field_name="TimeStamp")

            if not timestamp:
                timestamp = datetime.now(ET)

            # Parse OHLCV with volume breakdown
            underlying_data = {
                "symbol": self.db_underlying,
                "timestamp": timestamp,
                "open": safe_float(bar.get("Open"), field_name="Open"),
                "high": safe_float(bar.get("High"), field_name="High"),
                "low": safe_float(bar.get("Low"), field_name="Low"),
                "close": safe_float(bar.get("Close"), field_name="Close"),
                "up_volume": safe_int(bar.get("UpVolume"), field_name="UpVolume"),
                "down_volume": safe_int(bar.get("DownVolume"), field_name="DownVolume"),
                "volume": safe_int(bar.get("TotalVolume"), field_name="TotalVolume"),
            }

            logger.debug(f"Bar: {self.underlying} @ {timestamp} "
                        f"C=${underlying_data['close']:.2f} "
                        f"UpVol={underlying_data['up_volume']:,} "
                        f"DownVol={underlying_data['down_volume']:,}")

            return underlying_data

        except Exception as e:
            logger.error(f"Error fetching underlying bar: {e}", exc_info=True)
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
        """Get target expiration dates.

        Always queries get_option_expirations(self.underlying) — e.g. "$SPX.X" — since
        that is the symbol TradeStation uses for expiration/strike structure lookups.
        self.option_root (e.g. "SPXW") is only used later inside build_option_symbol()
        when constructing the actual option chain symbols for get_option_quotes().

        If self.option_root is listed in OPTION_WEEKLY_ROOTS, the returned dates are
        filtered to Mon/Wed/Fri only, because building a "SPXW ..." symbol for a
        non-weekly expiration would be rejected by the TradeStation API.
        """
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

            # If the option root is weekly-only (e.g. SPXW), keep only Mon/Wed/Fri dates
            if self.option_root in get_weekly_option_roots():
                future_expirations = [exp for exp in future_expirations if exp.weekday() in (0, 2, 4)]
                logger.info(f"Filtered to weekly expirations for {self.option_root} (Mon/Wed/Fri)")

            # Take first N
            target_exps = future_expirations[:self.num_expirations]

            logger.info(f"Target expirations: {[str(exp) for exp in target_exps]}")
            return target_exps

        except Exception as e:
            logger.error(f"Error fetching expirations: {e}", exc_info=True)
            return []

    def _get_strikes_near_price(self, expiration: date, current_price: float) -> List[float]:
        """Get the N nearest strikes on each side of the current price."""
        try:
            exp_str = expiration.strftime("%m-%d-%Y")
            all_strikes = self.client.get_option_strikes(self.underlying, expiration=exp_str)

            if not all_strikes:
                logger.warning(f"No strikes found for exp {exp_str}")
                return []

            below = sorted([s for s in all_strikes if s <= current_price], reverse=True)[:self.num_strikes]
            above = sorted([s for s in all_strikes if s > current_price])[:self.num_strikes]
            nearby_strikes = sorted(below + above)

            logger.debug(f"Exp {exp_str}: {len(nearby_strikes)} strikes "
                        f"({len(below)} below, {len(above)} above ${current_price:.2f})")

            return nearby_strikes

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

    def _validate_option_quote_symbol(self) -> bool:
        """Validate at least one built option symbol returns a quote without API symbol errors."""
        if not self.tracked_option_symbols:
            logger.error("No option symbols available for validation")
            return False

        test_symbol = self.tracked_option_symbols[0]
        logger.info(f"Validating option quote symbol: {test_symbol}")

        try:
            result = self.client.get_option_quotes([test_symbol])

            errors = result.get("Errors", []) if isinstance(result, dict) else []
            if errors:
                logger.error(f"Option quote validation failed for {test_symbol}: {errors[0]}")
                logger.error(
                    "This usually means the option symbol format is not accepted by TradeStation quotes endpoint "
                    "for this underlying."
                )
                return False

            quotes = result.get("Quotes", []) if isinstance(result, dict) else []
            if not quotes:
                logger.warning(
                    f"Option quote validation returned no quotes for {test_symbol}; continuing "
                    "because endpoint did not report INVALID SYMBOL"
                )
                return True

            logger.info(f"✅ Option quote validation passed for {test_symbol}")
            return True

        except Exception as e:
            logger.error(f"Error validating option quote symbol {test_symbol}: {e}", exc_info=True)
            return False

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

        # Validate at least one option quote symbol actually resolves on quote endpoint
        if not self._validate_option_quote_symbol():
            logger.error("Failed option quote validation during initialization")
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
                                mid = safe_float(opt_quote.get("Mid"), field_name="Mid")
                                # Fall back to computed mid if TradeStation doesn't provide it
                                if mid is None and bid is not None and ask is not None:
                                    mid = (bid + ask) / 2.0
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
                                    "underlying": self.db_underlying,
                                    "strike": strike,
                                    "expiration": expiration,
                                    "option_type": option_type,
                                    "last": last,
                                    "bid": bid,
                                    "ask": ask,
                                    "mid": mid,
                                    "volume": volume,
                                    "open_interest": open_interest,
                                    "implied_volatility": implied_volatility if implied_volatility else None,
                                }

                                yield {"type": "option", "data": option_data}
                                option_count += 1

                    except Exception as e:
                        logger.error(f"Error fetching options batch: {e}")

                # Recalibrate strike range periodically — re-centers N strikes around
                # the latest price unconditionally, so the tracked window always stays current.
                if iteration % STRIKE_RECALC_INTERVAL == 0 and iteration > 0:
                    if self.current_price:
                        new_price = self._get_underlying_price()
                        if new_price:
                            self.current_price = new_price
                            self.tracked_option_symbols = self._build_option_symbols()
                            logger.info(f"Recalibrated strikes around ${self.current_price:.2f} "
                                       f"(±{self.num_strikes} strikes each side)")

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
    parser.add_argument("--underlying", default=os.getenv("INGEST_UNDERLYING", "SPY"),
                       help="Underlying symbol or alias (default: SPY)")
    parser.add_argument("--expirations", type=int,
                       default=int(os.getenv("INGEST_EXPIRATIONS", "3")),
                       help="Number of expirations to track (default: 3)")
    parser.add_argument("--num-strikes", type=int,
                       default=int(os.getenv("INGEST_STRIKE_COUNT", "10")),
                       help="Number of strikes to track on each side of current price (default: 10)")
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
    print(f"Strikes Each Side: {args.num_strikes}")
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
        num_strikes=args.num_strikes,
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
                    print(f"Underlying bars: {underlying_count} - Latest: "
                          f"${data['close']:.2f} "
                          f"(Up: {data['up_volume']:,}, Down: {data['down_volume']:,})")
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
