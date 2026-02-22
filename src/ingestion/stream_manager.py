"""
Underlying Quote and Options Chain Streaming Manager

Streams real-time underlying quotes and options chain data for a given symbol.
Configurable by number of expirations and strike distance from current price.
"""

import os
import time
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Set
from src.ingestion.tradestation_client import TradeStationClient
from src.utils import get_logger

logger = get_logger(__name__)


class StreamManager:
    """Manages streaming of underlying quotes and options chain data"""

    def __init__(
        self,
        client: TradeStationClient,
        underlying: str = "SPY",
        num_expirations: int = 3,
        strike_distance: float = 10.0,
        poll_interval: int = 5
    ):
        """
        Initialize options stream manager

        Args:
            client: TradeStationClient instance
            underlying: Underlying symbol to track (default: SPY)
            num_expirations: Number of expiration dates to track from today (default: 3)
            strike_distance: Strike distance from current price (default: 10.0)
            poll_interval: Seconds between polls (default: 5)
        """
        self.client = client
        self.underlying = underlying.upper()
        self.num_expirations = num_expirations
        self.strike_distance = strike_distance
        self.poll_interval = poll_interval

        # Track state
        self.current_price: Optional[float] = None
        self.target_expirations: List[date] = []
        self.tracked_strikes: Set[float] = set()
        self.tracked_option_symbols: List[str] = []

        logger.info(f"Initialized StreamManager for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, ±${strike_distance} strikes, {poll_interval}s interval")

    def _get_underlying_price(self) -> Optional[float]:
        """
        Fetch current underlying price

        Returns:
            Current price or None if unavailable
        """
        try:
            quote_data = self.client.get_quote(self.underlying, warn_if_closed=False)

            if 'Quotes' not in quote_data or len(quote_data['Quotes']) == 0:
                logger.warning(f"No quote data returned for {self.underlying}")
                return None

            quote = quote_data['Quotes'][0]
            price = float(quote.get('Last', 0))

            if price == 0:
                logger.warning(f"Price is 0 for {self.underlying}, trying Close or Bid")
                price = float(quote.get('Close', quote.get('Bid', 0)))

            logger.debug(f"Current {self.underlying} price: ${price:.2f}")
            return price

        except Exception as e:
            logger.error(f"Error fetching underlying price: {e}", exc_info=True)
            return None

    def _get_target_expirations(self) -> List[date]:
        """
        Get target expiration dates based on configuration

        Returns:
            List of expiration dates (sorted)
        """
        try:
            all_expirations = self.client.get_option_expirations(self.underlying)

            if not all_expirations:
                logger.warning(f"No expirations found for {self.underlying}")
                return []

            # Filter to expirations >= today
            today = date.today()
            future_expirations = [exp for exp in all_expirations if exp >= today]

            if not future_expirations:
                logger.warning("No future expirations available")
                return []

            # Take first N expirations
            target_exps = future_expirations[:self.num_expirations]

            logger.info(f"Target expirations: {[str(exp) for exp in target_exps]}")
            return target_exps

        except Exception as e:
            logger.error(f"Error fetching expirations: {e}", exc_info=True)
            return []

    def _get_strikes_near_price(self, expiration: date, current_price: float) -> List[float]:
        """
        Get strikes within configured distance of current price

        Args:
            expiration: Expiration date
            current_price: Current underlying price

        Returns:
            List of strikes within range
        """
        try:
            exp_str = expiration.strftime('%m-%d-%Y')
            all_strikes = self.client.get_option_strikes(self.underlying, expiration=exp_str)

            if not all_strikes:
                logger.warning(f"No strikes found for {self.underlying} exp {exp_str}")
                return []

            # Filter strikes within distance
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
        """
        Build list of option symbols to track based on current configuration

        Returns:
            List of option symbols in TradeStation format
        """
        if not self.current_price:
            logger.warning("No current price available, cannot build option symbols")
            return []

        option_symbols = []

        for expiration in self.target_expirations:
            strikes = self._get_strikes_near_price(expiration, self.current_price)

            for strike in strikes:
                # Build both call and put symbols
                call_symbol = self.client.build_option_symbol(
                    self.underlying, expiration, 'C', strike
                )
                put_symbol = self.client.build_option_symbol(
                    self.underlying, expiration, 'P', strike
                )

                option_symbols.append(call_symbol)
                option_symbols.append(put_symbol)
                self.tracked_strikes.add(strike)

        logger.info(f"Built {len(option_symbols)} option symbols to track")
        logger.debug(f"Tracking {len(self.tracked_strikes)} unique strikes: "
                    f"{sorted(self.tracked_strikes)}")

        return option_symbols

    def _fetch_underlying_quote(self) -> None:
        """Fetch and log underlying quote"""
        try:
            quote_data = self.client.get_quote(self.underlying, warn_if_closed=False)

            if 'Quotes' in quote_data and len(quote_data['Quotes']) > 0:
                quote = quote_data['Quotes'][0]

                logger.debug("="*80)
                logger.debug(f"UNDERLYING QUOTE: {self.underlying}")
                logger.debug("-"*80)
                logger.debug(f"  Last:      ${quote.get('Last', 'N/A')}")
                logger.debug(f"  Bid:       ${quote.get('Bid', 'N/A')} x {quote.get('BidSize', 0)}")
                logger.debug(f"  Ask:       ${quote.get('Ask', 'N/A')} x {quote.get('AskSize', 0)}")
                logger.debug(f"  Volume:    {quote.get('Volume', 0):,}")
                logger.debug(f"  Timestamp: {quote.get('TimeStamp', 'N/A')}")
                logger.debug("="*80)

        except Exception as e:
            logger.error(f"Error fetching underlying quote: {e}", exc_info=True)

    def _fetch_options_quotes(self) -> None:
        """Fetch and log options chain quotes"""
        if not self.tracked_option_symbols:
            logger.warning("No option symbols to track")
            return

        try:
            # TradeStation API has a 500 symbol limit per request
            # Split into batches if needed
            batch_size = 100

            for i in range(0, len(self.tracked_option_symbols), batch_size):
                batch = self.tracked_option_symbols[i:i + batch_size]

                logger.debug(f"Fetching quotes for {len(batch)} options...")
                options_data = self.client.get_option_quotes(batch)

                if 'Quotes' in options_data:
                    logger.debug("="*80)
                    logger.debug(f"OPTIONS CHAIN QUOTES (batch {i//batch_size + 1})")
                    logger.debug("-"*80)

                    for opt_quote in options_data['Quotes']:
                        symbol = opt_quote.get('Symbol', 'N/A')

                        # Safely convert numeric values
                        try:
                            volume = int(opt_quote.get('Volume', 0)) if opt_quote.get('Volume') else 0
                            open_interest = int(opt_quote.get('OpenInterest', 0)) if opt_quote.get('OpenInterest') else 0
                        except (ValueError, TypeError):
                            volume = 0
                            open_interest = 0

                        logger.debug(f"  {symbol}")
                        logger.debug(f"    Last: ${opt_quote.get('Last', 'N/A')} | "
                                   f"Bid: ${opt_quote.get('Bid', 'N/A')} x {opt_quote.get('BidSize', 0)} | "
                                   f"Ask: ${opt_quote.get('Ask', 'N/A')} x {opt_quote.get('AskSize', 0)}")
                        logger.debug(f"    Volume: {volume:,} | "
                                   f"OpenInterest: {open_interest:,}")

                    logger.debug("="*80)

                # Small delay between batches
                if i + batch_size < len(self.tracked_option_symbols):
                    time.sleep(0.5)

        except Exception as e:
            logger.error(f"Error fetching options quotes: {e}", exc_info=True)

    def initialize(self) -> bool:
        """
        Initialize stream by fetching expirations and building symbol list

        Returns:
            True if successful, False otherwise
        """
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

        # Build option symbols list
        self.tracked_option_symbols = self._build_option_symbols()
        if not self.tracked_option_symbols:
            logger.error("Failed to build option symbols list")
            return False

        logger.info(f"✅ Initialization complete:")
        logger.info(f"   Underlying price: ${self.current_price:.2f}")
        logger.info(f"   Tracking {len(self.target_expirations)} expirations")
        logger.info(f"   Tracking {len(self.tracked_option_symbols)} option contracts")

        return True

    def stream(self, max_iterations: Optional[int] = None) -> None:
        """
        Start streaming loop

        Args:
            max_iterations: Maximum iterations (None for infinite)
        """
        if not self.tracked_option_symbols:
            logger.error("Not initialized. Call initialize() first.")
            return

        logger.info(f"Starting stream loop (poll interval: {self.poll_interval}s)...")
        logger.info("Press Ctrl+C to stop")

        iteration = 0

        try:
            while True:
                iteration += 1
                logger.info(f"\n{'='*80}")
                logger.info(f"ITERATION {iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'='*80}")

                # Fetch underlying quote
                self._fetch_underlying_quote()

                # Fetch options quotes
                self._fetch_options_quotes()

                # Check if we should update price and recalculate strikes
                # (every 10 iterations, check if price moved significantly)
                if iteration % 10 == 0:
                    new_price = self._get_underlying_price()
                    if new_price and abs(new_price - self.current_price) > 1.0:
                        logger.info(f"Price moved from ${self.current_price:.2f} to ${new_price:.2f}")
                        logger.info("Recalculating tracked strikes...")
                        self.current_price = new_price
                        self.tracked_option_symbols = self._build_option_symbols()

                # Check max iterations
                if max_iterations and iteration >= max_iterations:
                    logger.info(f"Reached max iterations ({max_iterations})")
                    break

                # Sleep until next poll
                logger.debug(f"Sleeping for {self.poll_interval} seconds...")
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            logger.info("\n\n⚠️  Stream interrupted by user")
        except Exception as e:
            logger.error(f"Stream error: {e}", exc_info=True)
        finally:
            logger.info("Stream stopped")


def main():
    """Main entry point with argument parsing"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Stream real-time underlying and options chain data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Stream SPY with defaults (3 expirations, ±$10 strikes, 5s interval)
  python -m src.ingestion.options_stream

  # Stream AAPL with custom configuration
  python -m src.ingestion.options_stream --underlying AAPL --expirations 5 --strike-distance 20

  # Test with just 10 iterations
  python -m src.ingestion.options_stream --max-iterations 10

  # Fast polling with debug logging
  python -m src.ingestion.options_stream --interval 2 --debug

Environment Variables (.env):
  TRADESTATION_CLIENT_ID           Required: Your API client ID
  TRADESTATION_CLIENT_SECRET       Required: Your API client secret
  TRADESTATION_REFRESH_TOKEN       Required: Your refresh token
  TRADESTATION_USE_SANDBOX=false   Optional: Use sandbox environment
  LOG_LEVEL=INFO                   Optional: Logging level

  Stream Configuration (optional):
  STREAM_UNDERLYING=SPY            Underlying symbol (default: SPY)
  STREAM_EXPIRATIONS=3             Number of expirations (default: 3)
  STREAM_STRIKE_DISTANCE=10.0      Strike distance from price (default: 10.0)
  STREAM_POLL_INTERVAL=5           Seconds between polls (default: 5)
        '''
    )

    parser.add_argument('--underlying', type=str,
                       help='Underlying symbol (default: SPY, env: STREAM_UNDERLYING)')
    parser.add_argument('--expirations', type=int,
                       help='Number of expirations to track (default: 3, env: STREAM_EXPIRATIONS)')
    parser.add_argument('--strike-distance', type=float,
                       help='Strike distance from current price (default: 10.0, env: STREAM_STRIKE_DISTANCE)')
    parser.add_argument('--interval', type=int,
                       help='Poll interval in seconds (default: 5, env: STREAM_POLL_INTERVAL)')
    parser.add_argument('--max-iterations', type=int,
                       help='Maximum iterations (default: infinite)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')

    args = parser.parse_args()

    # Load from env with CLI override
    underlying = args.underlying or os.getenv('STREAM_UNDERLYING', 'SPY')
    num_expirations = args.expirations or int(os.getenv('STREAM_EXPIRATIONS', '3'))
    strike_distance = args.strike_distance or float(os.getenv('STREAM_STRIKE_DISTANCE', '10.0'))
    poll_interval = args.interval or int(os.getenv('STREAM_POLL_INTERVAL', '5'))

    # Set logging
    if args.debug or os.getenv('LOG_LEVEL', '').upper() == 'DEBUG':
        from src.utils import set_log_level
        set_log_level('DEBUG')

    print("\n" + "="*80)
    print("Options Chain Streaming")
    print("="*80)
    print(f"Underlying:       {underlying}")
    print(f"Expirations:      {num_expirations}")
    print(f"Strike Distance:  ±${strike_distance}")
    print(f"Poll Interval:    {poll_interval}s")
    print("="*80 + "\n")

    # Initialize client
    try:
        client = TradeStationClient(
            os.getenv('TRADESTATION_CLIENT_ID'),
            os.getenv('TRADESTATION_CLIENT_SECRET'),
            os.getenv('TRADESTATION_REFRESH_TOKEN'),
            sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
        )
    except Exception as e:
        logger.error(f"Failed to initialize TradeStation client: {e}")
        return

    # Create stream manager
    stream_manager = StreamManager(
        client=client,
        underlying=underlying,
        num_expirations=num_expirations,
        strike_distance=strike_distance,
        poll_interval=poll_interval
    )

    # Initialize
    if not stream_manager.initialize():
        logger.error("Failed to initialize stream")
        return

    # Start streaming
    stream_manager.stream(max_iterations=args.max_iterations)


if __name__ == '__main__':
    main()
