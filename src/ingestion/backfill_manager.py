"""
Backfill Manager - Fetches historical data and yields to MainEngine

This manager ONLY fetches data from TradeStation API.
Storage is handled by MainEngine.
"""

import os
import time
from datetime import datetime, date, timedelta, timezone
from typing import Generator, List, Dict, Any, Optional
import pytz

from src.ingestion.tradestation_client import TradeStationClient
from src.utils import get_logger
from src.validation import safe_float, safe_int, safe_datetime, validate_bar_data
from src.config import (
    OPTION_BATCH_SIZE,
    DELAY_BETWEEN_BATCHES,
    DELAY_BETWEEN_BARS,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


class BackfillManager:
    """Manages fetching of historical underlying and options data"""

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

        logger.info(f"Initialized BackfillManager for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, ±${strike_distance} strikes")

    def _get_underlying_bars(
        self,
        start_date: datetime,
        end_date: datetime,
        interval: int = 5,
        unit: str = "Minute"
    ) -> List[Dict[str, Any]]:
        """Fetch underlying price bars for date range"""
        try:
            start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

            logger.info(f"Fetching {interval}{unit} bars from {start_str} to {end_str}")

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
                logger.warning(f"No bar data returned for {self.underlying}")
                return []

            bars = bars_data["Bars"]
            logger.info(f"✅ Retrieved {len(bars)} bars for {self.underlying}")

            return bars

        except Exception as e:
            logger.error(f"Error fetching underlying bars: {e}", exc_info=True)
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

    def backfill(
        self,
        lookback_days: int = 1,
        interval: int = 5,
        unit: str = "Minute",
        sample_every_n_bars: int = 1
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch historical data and yield to caller

        Yields dictionaries with:
            {
                'type': 'underlying' | 'option',
                'data': {...}
            }
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

                # Parse OHLCV with validation
                open_price = safe_float(bar.get("Open"), field_name="Open")
                high_price = safe_float(bar.get("High"), field_name="High")
                low_price = safe_float(bar.get("Low"), field_name="Low")
                close_price = safe_float(bar.get("Close"), field_name="Close")
                total_volume = safe_int(bar.get("TotalVolume"), field_name="TotalVolume")

                if close_price == 0:
                    logger.warning(f"Bar {i}: Zero close price, skipping")
                    continue

                # Yield underlying bar data
                underlying_data = {
                    "symbol": self.underlying,
                    "timestamp": bar_dt,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": total_volume,
                }

                logger.debug(f"Bar {i+1}/{len(bars)}: {self.underlying} @ {bar_dt} "
                           f"O=${open_price:.2f} H=${high_price:.2f} "
                           f"L=${low_price:.2f} C=${close_price:.2f} V={total_volume:,}")

                yield {"type": "underlying", "data": underlying_data}

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

                                # Parse option symbol to extract components
                                # Format: "SPY 260221C450"
                                parts = option_symbol.split()
                                if len(parts) < 2:
                                    logger.warning(f"Invalid option symbol: {option_symbol}")
                                    continue

                                option_part = parts[1]

                                # Extract option type (C or P)
                                option_type = "C" if "C" in option_part else "P"

                                # Extract expiration (YYMMDD)
                                exp_str = option_part[:6]
                                try:
                                    expiration = datetime.strptime(exp_str, "%y%m%d").date()
                                except ValueError:
                                    logger.warning(f"Invalid expiration in {option_symbol}")
                                    continue

                                # Extract strike
                                strike_str = option_part.split(option_type)[1]
                                strike = safe_float(strike_str, field_name="strike")

                                # Parse quote data
                                last = safe_float(opt_quote.get("Last"), field_name="Last")
                                bid = safe_float(opt_quote.get("Bid"), field_name="Bid")
                                ask = safe_float(opt_quote.get("Ask"), field_name="Ask")
                                volume = safe_int(opt_quote.get("Volume"), field_name="Volume")
                                open_interest = safe_int(opt_quote.get("OpenInterest"),
                                                        field_name="OpenInterest")

                                # Yield option data
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

                                yield {"type": "option", "data": option_data}

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

        logger.info("Backfill complete - all data yielded")
