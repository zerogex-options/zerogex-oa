"""
Underlying Quote and Options Chain Backfill Module

Backfills historical underlying quotes and options chain data for a specified time period.
"""

import os
import time
import json
from datetime import datetime, date, timedelta, timezone
from typing import List, Dict, Any, Optional, Set
from src.ingestion.tradestation_client import TradeStationClient
from src.utils import get_logger

logger = get_logger(__name__)


class OptionsBackfillManager:
    """Manages backfilling of historical underlying and options chain data"""

    def __init__(
        self,
        client: TradeStationClient,
        underlying: str = "SPY",
        num_expirations: int = 3,
        strike_distance: float = 10.0
    ):
        """
        Initialize backfill manager

        Args:
            client: TradeStationClient instance
            underlying: Underlying symbol to backfill (default: SPY)
            num_expirations: Number of expiration dates to include (default: 3)
            strike_distance: Strike distance from price at each point (default: 10.0)
        """
        self.client = client
        self.underlying = underlying.upper()
        self.num_expirations = num_expirations
        self.strike_distance = strike_distance

        logger.info(f"Initialized OptionsBackfillManager for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, ±${strike_distance} strikes")

    def _get_underlying_bars(
        self,
        start_date: datetime,
        end_date: datetime,
        interval: int = 5,
        unit: str = 'Minute'
    ) -> List[Dict[str, Any]]:
        """
        Fetch underlying price bars for date range

        Args:
            start_date: Start datetime
            end_date: End datetime
            interval: Bar interval (default: 5)
            unit: Time unit (default: Minute)

        Returns:
            List of bar data
        """
        try:
            # Convert to ISO format for API
            start_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

            logger.info(f"Fetching {interval}{unit} bars from {start_str} to {end_str}")

            bars_data = self.client.get_bars(
                symbol=self.underlying,
                interval=interval,
                unit=unit,
                firstdate=start_str,
                lastdate=end_str,
                sessiontemplate='USEQ24Hour',
                warn_if_closed=False
            )

            if 'Bars' not in bars_data or len(bars_data['Bars']) == 0:
                logger.warning(f"No bar data returned for {self.underlying}")
                return []

            bars = bars_data['Bars']
            logger.info(f"✅ Retrieved {len(bars)} bars for {self.underlying}")

            return bars

        except Exception as e:
            logger.error(f"Error fetching underlying bars: {e}", exc_info=True)
            return []

    def _get_expirations_for_date(self, as_of_date: date) -> List[date]:
        """
        Get expirations that would have been available on a given date

        Args:
            as_of_date: Date to check expirations for

        Returns:
            List of expiration dates
        """
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

    def _get_strikes_near_price(
        self,
        expiration: date,
        price: float
    ) -> List[float]:
        """
        Get strikes within configured distance of a price

        Args:
            expiration: Expiration date
            price: Price to center strikes around

        Returns:
            List of strikes
        """
        try:
            exp_str = expiration.strftime('%m-%d-%Y')
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

    def _build_option_symbols_for_bar(
        self,
        bar_date: date,
        bar_price: float
    ) -> List[str]:
        """
        Build option symbols for a specific bar/timestamp

        Args:
            bar_date: Date of the bar
            bar_price: Price at the bar

        Returns:
            List of option symbols
        """
        expirations = self._get_expirations_for_date(bar_date)
        option_symbols = []

        for expiration in expirations:
            strikes = self._get_strikes_near_price(expiration, bar_price)

            for strike in strikes:
                call_symbol = self.client.build_option_symbol(
                    self.underlying, expiration, 'C', strike
                )
                put_symbol = self.client.build_option_symbol(
                    self.underlying, expiration, 'P', strike
                )

                option_symbols.append(call_symbol)
                option_symbols.append(put_symbol)

        return option_symbols

    def backfill(
        self,
        lookback_days: int = 1,
        interval: int = 5,
        unit: str = 'Minute',
        sample_every_n_bars: int = 1
    ) -> None:
        """
        Perform backfill of underlying and options data

        Args:
            lookback_days: Number of days back to fetch (default: 1)
            interval: Bar interval (default: 5)
            unit: Time unit - Minute, Daily, etc. (default: Minute)
            sample_every_n_bars: Sample options chain every N bars (default: 1 = every bar)
        """
        logger.info(f"Starting backfill for {self.underlying}")
        logger.info(f"Lookback: {lookback_days} days, Interval: {interval}{unit}")
        logger.info(f"Sampling options every {sample_every_n_bars} bar(s)")

        # Calculate date range
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=lookback_days)

        # Fetch underlying bars
        bars = self._get_underlying_bars(start_date, end_date, interval, unit)

        if not bars:
            logger.error("No bars retrieved, aborting backfill")
            return

        logger.info(f"Processing {len(bars)} bars...")

        # Process each bar
        bars_processed = 0
        options_fetched = 0

        for i, bar in enumerate(bars):
            try:
                # Parse bar data
                timestamp_str = bar.get('TimeStamp', '')
                close_price = float(bar.get('Close', 0))

                if close_price == 0:
                    logger.warning(f"Bar {i}: Invalid price, skipping")
                    continue

                # Parse timestamp
                bar_dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
                bar_date = bar_dt.date()

                # Safely convert all numeric values
                try:
                    open_price = float(bar.get('Open', 0))
                    high_price = float(bar.get('High', 0))
                    low_price = float(bar.get('Low', 0))
                    total_volume = int(bar.get('TotalVolume', 0)) if bar.get('TotalVolume') else 0
                except (ValueError, TypeError):
                    open_price = high_price = low_price = 0.0
                    total_volume = 0

                # Log underlying bar
                logger.debug("="*80)
                logger.debug(f"BAR {i+1}/{len(bars)}: {timestamp_str}")
                logger.debug("-"*80)
                logger.debug(f"  {self.underlying}: O=${open_price:.2f} "
                           f"H=${high_price:.2f} "
                           f"L=${low_price:.2f} "
                           f"C=${close_price:.2f}")
                logger.debug(f"  Volume: {total_volume:,}")

                bars_processed += 1

                # Check if we should sample options for this bar
                if i % sample_every_n_bars != 0:
                    logger.debug(f"  Skipping options (sampling every {sample_every_n_bars} bars)")
                    continue

                # Build option symbols for this point in time
                logger.debug(f"  Fetching options chain at price ${close_price:.2f}...")
                option_symbols = self._build_option_symbols_for_bar(bar_date, close_price)

                if not option_symbols:
                    logger.debug("  No option symbols to fetch")
                    continue

                logger.debug(f"  Fetching quotes for {len(option_symbols)} options...")

                # Fetch options quotes in batches
                batch_size = 100
                for j in range(0, len(option_symbols), batch_size):
                    batch = option_symbols[j:j + batch_size]

                    try:
                        options_data = self.client.get_option_quotes(batch)

                        if 'Quotes' in options_data:
                            logger.debug(f"    Batch {j//batch_size + 1}: {len(options_data['Quotes'])} quotes")

                            for opt_quote in options_data['Quotes']:
                                symbol = opt_quote.get('Symbol', 'N/A')

                                # Safely convert numeric values
                                try:
                                    volume = int(opt_quote.get('Volume', 0)) if opt_quote.get('Volume') else 0
                                except (ValueError, TypeError):
                                    volume = 0

                                logger.debug(f"      {symbol}: "
                                           f"Last=${opt_quote.get('Last', 'N/A')} "
                                           f"Bid=${opt_quote.get('Bid', 'N/A')} "
                                           f"Ask=${opt_quote.get('Ask', 'N/A')} "
                                           f"Vol={volume:,}")

                            options_fetched += len(options_data['Quotes'])

                        # Rate limiting
                        time.sleep(0.5)

                    except Exception as e:
                        logger.error(f"    Error fetching options batch: {e}")

                logger.debug("="*80)

                # Rate limiting between bars
                if i < len(bars) - 1:
                    time.sleep(1)

            except Exception as e:
                logger.error(f"Error processing bar {i}: {e}", exc_info=True)
                continue

        # Summary
        logger.info("")
        logger.info("="*80)
        logger.info("BACKFILL COMPLETE")
        logger.info("="*80)
        logger.info(f"Underlying bars processed: {bars_processed}")
        logger.info(f"Options quotes fetched:    {options_fetched}")
        logger.info(f"Time range:                {start_date.strftime('%Y-%m-%d %H:%M')} to "
                   f"{end_date.strftime('%Y-%m-%d %H:%M')}")
        logger.info("="*80)


def main():
    """Main entry point with argument parsing"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Backfill historical underlying and options chain data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Backfill last 1 day of 5-minute data for SPY
  python -m src.ingestion.options_backfill

  # Backfill last 3 days with custom configuration
  python -m src.ingestion.options_backfill --lookback-days 3 --expirations 5

  # Backfill with daily bars
  python -m src.ingestion.options_backfill --unit Daily --lookback-days 30

  # Sample options every 10 bars (for faster backfill)
  python -m src.ingestion.options_backfill --sample-every 10

  # Debug mode to see all data
  python -m src.ingestion.options_backfill --debug

Environment Variables (.env):
  TRADESTATION_CLIENT_ID           Required: Your API client ID
  TRADESTATION_CLIENT_SECRET       Required: Your API client secret
  TRADESTATION_REFRESH_TOKEN       Required: Your refresh token
  TRADESTATION_USE_SANDBOX=false   Optional: Use sandbox environment
  LOG_LEVEL=INFO                   Optional: Logging level

  Backfill Configuration (optional):
  BACKFILL_UNDERLYING=SPY          Underlying symbol (default: SPY)
  BACKFILL_LOOKBACK_DAYS=1         Days to look back (default: 1)
  BACKFILL_INTERVAL=5              Bar interval (default: 5)
  BACKFILL_UNIT=Minute             Time unit (default: Minute)
  BACKFILL_EXPIRATIONS=3           Number of expirations (default: 3)
  BACKFILL_STRIKE_DISTANCE=10.0    Strike distance (default: 10.0)
  BACKFILL_SAMPLE_EVERY=1          Sample every N bars (default: 1)
        '''
    )

    parser.add_argument('--underlying', type=str,
                       help='Underlying symbol (default: SPY, env: BACKFILL_UNDERLYING)')
    parser.add_argument('--lookback-days', type=int,
                       help='Days to look back (default: 1, env: BACKFILL_LOOKBACK_DAYS)')
    parser.add_argument('--interval', type=int,
                       help='Bar interval (default: 5, env: BACKFILL_INTERVAL)')
    parser.add_argument('--unit', type=str, choices=['Minute', 'Daily', 'Weekly', 'Monthly'],
                       help='Time unit (default: Minute, env: BACKFILL_UNIT)')
    parser.add_argument('--expirations', type=int,
                       help='Number of expirations (default: 3, env: BACKFILL_EXPIRATIONS)')
    parser.add_argument('--strike-distance', type=float,
                       help='Strike distance (default: 10.0, env: BACKFILL_STRIKE_DISTANCE)')
    parser.add_argument('--sample-every', type=int,
                       help='Sample options every N bars (default: 1, env: BACKFILL_SAMPLE_EVERY)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')

    args = parser.parse_args()

    # Load from env with CLI override
    underlying = args.underlying or os.getenv('BACKFILL_UNDERLYING', 'SPY')
    lookback_days = args.lookback_days or int(os.getenv('BACKFILL_LOOKBACK_DAYS', '1'))
    interval = args.interval or int(os.getenv('BACKFILL_INTERVAL', '5'))
    unit = args.unit or os.getenv('BACKFILL_UNIT', 'Minute')
    num_expirations = args.expirations or int(os.getenv('BACKFILL_EXPIRATIONS', '3'))
    strike_distance = args.strike_distance or float(os.getenv('BACKFILL_STRIKE_DISTANCE', '10.0'))
    sample_every = args.sample_every or int(os.getenv('BACKFILL_SAMPLE_EVERY', '1'))

    # Set logging
    if args.debug or os.getenv('LOG_LEVEL', '').upper() == 'DEBUG':
        from src.utils import set_log_level
        set_log_level('DEBUG')

    print("\n" + "="*80)
    print("Options Chain Backfill")
    print("="*80)
    print(f"Underlying:       {underlying}")
    print(f"Lookback:         {lookback_days} days")
    print(f"Interval:         {interval}{unit}")
    print(f"Expirations:      {num_expirations}")
    print(f"Strike Distance:  ±${strike_distance}")
    print(f"Sample Every:     {sample_every} bar(s)")
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

    # Create backfill manager
    backfill_manager = OptionsBackfillManager(
        client=client,
        underlying=underlying,
        num_expirations=num_expirations,
        strike_distance=strike_distance
    )

    # Run backfill
    backfill_manager.backfill(
        lookback_days=lookback_days,
        interval=interval,
        unit=unit,
        sample_every_n_bars=sample_every
    )


if __name__ == '__main__':
    main()
