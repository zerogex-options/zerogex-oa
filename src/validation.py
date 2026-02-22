"""
Data validation utilities for API responses and data quality
"""

from typing import Any, Optional, Union
from datetime import datetime, date
import pytz
from src.utils import get_logger

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


def safe_float(value: Any, default: float = 0.0, field_name: str = "value") -> float:
    """
    Safely convert value to float with validation

    Args:
        value: Value to convert
        default: Default value if conversion fails
        field_name: Name of field for logging

    Returns:
        Float value or default
    """
    if value in (None, "", "N/A"):
        return default

    try:
        result = float(value)

        # Validate reasonable ranges for prices/volumes
        if result < 0:
            logger.warning(f"{field_name} is negative: {result}, using default {default}")
            return default

        return result

    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to convert {field_name}='{value}' to float: {e}, using default {default}")
        return default


def safe_int(value: Any, default: int = 0, field_name: str = "value") -> int:
    """
    Safely convert value to int with validation

    Args:
        value: Value to convert
        default: Default value if conversion fails
        field_name: Name of field for logging

    Returns:
        Int value or default
    """
    if value in (None, "", "N/A"):
        return default

    try:
        result = int(value)

        # Validate reasonable ranges
        if result < 0:
            logger.warning(f"{field_name} is negative: {result}, using default {default}")
            return default

        return result

    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to convert {field_name}='{value}' to int: {e}, using default {default}")
        return default


def safe_datetime(value: str, default: Optional[datetime] = None, field_name: str = "timestamp") -> Optional[datetime]:
    """
    Safely parse ISO datetime string to timezone-aware datetime

    Args:
        value: ISO format datetime string (e.g., '2026-02-22T14:30:00Z')
        default: Default value if parsing fails
        field_name: Name of field for logging

    Returns:
        Timezone-aware datetime in ET or default
    """
    if not value:
        return default

    try:
        # Parse UTC timestamp
        if value.endswith("Z"):
            dt_utc = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
            dt_utc = pytz.UTC.localize(dt_utc)
        else:
            # Assume already has timezone info
            dt_utc = datetime.fromisoformat(value.replace("Z", "+00:00"))

        # Convert to ET
        dt_et = dt_utc.astimezone(ET)
        return dt_et

    except (ValueError, AttributeError) as e:
        logger.warning(f"Failed to parse {field_name}='{value}': {e}, using default")
        return default


def validate_quote_data(quote: dict) -> bool:
    """
    Validate quote data has required fields and reasonable values

    Args:
        quote: Quote dictionary from API

    Returns:
        True if valid, False otherwise
    """
    required_fields = ["Symbol", "Last"]

    # Check required fields exist
    for field in required_fields:
        if field not in quote:
            logger.warning(f"Quote missing required field: {field}")
            return False

    # Validate price is reasonable
    last_price = safe_float(quote.get("Last"), field_name="Last")
    if last_price <= 0:
        logger.warning(f"Invalid Last price for {quote.get('Symbol')}: {last_price}")
        return False

    # Validate bid/ask spread is reasonable
    bid = safe_float(quote.get("Bid"), field_name="Bid")
    ask = safe_float(quote.get("Ask"), field_name="Ask")

    if bid > 0 and ask > 0:
        spread_pct = (ask - bid) / last_price
        if spread_pct > 0.5:  # 50% spread is suspicious
            logger.warning(f"Suspicious spread for {quote.get('Symbol')}: {spread_pct:.1%}")
            # Don't reject, just warn

    return True


def validate_bar_data(bar: dict) -> bool:
    """
    Validate bar data has required fields and OHLC consistency

    Args:
        bar: Bar dictionary from API

    Returns:
        True if valid, False otherwise
    """
    required_fields = ["TimeStamp", "Open", "High", "Low", "Close"]

    # Check required fields
    for field in required_fields:
        if field not in bar:
            logger.warning(f"Bar missing required field: {field}")
            return False

    # Validate OHLC relationship: High >= Open, Close, Low
    open_price = safe_float(bar.get("Open"), field_name="Open")
    high_price = safe_float(bar.get("High"), field_name="High")
    low_price = safe_float(bar.get("Low"), field_name="Low")
    close_price = safe_float(bar.get("Close"), field_name="Close")

    if not (low_price <= open_price <= high_price and 
            low_price <= close_price <= high_price):
        logger.warning(f"Invalid OHLC relationship: O={open_price} H={high_price} L={low_price} C={close_price}")
        return False

    return True


def validate_option_symbol(symbol: str) -> bool:
    """
    Validate option symbol format

    Expected format: 'SPY 260221C450' (UNDERLYING YYMMDD C/P STRIKE)

    Args:
        symbol: Option symbol string

    Returns:
        True if valid format, False otherwise
    """
    if not symbol:
        return False

    parts = symbol.split()

    # Should have at least 2 parts: underlying and option details
    if len(parts) < 2:
        logger.warning(f"Invalid option symbol format: {symbol}")
        return False

    option_part = parts[1]

    # Check for call/put indicator
    if "C" not in option_part and "P" not in option_part:
        logger.warning(f"No call/put indicator in option symbol: {symbol}")
        return False

    return True


def bucket_timestamp(dt: datetime, bucket_seconds: int = 60) -> datetime:
    """
    Round datetime down to nearest bucket (default: 1 minute)

    Args:
        dt: Datetime to bucket
        bucket_seconds: Bucket size in seconds

    Returns:
        Bucketed datetime
    """
    # Calculate seconds since epoch
    timestamp = dt.timestamp()

    # Round down to nearest bucket
    bucketed_timestamp = (timestamp // bucket_seconds) * bucket_seconds

    # Convert back to datetime, preserving timezone
    return datetime.fromtimestamp(bucketed_timestamp, tz=dt.tzinfo)


def is_market_hours(dt: Optional[datetime] = None, check_extended: bool = False) -> bool:
    """
    Check if given datetime is during market hours

    Args:
        dt: Datetime to check (default: now)
        check_extended: Include extended hours (default: regular only)

    Returns:
        True if during market hours, False otherwise
    """
    if dt is None:
        dt = datetime.now(ET)
    elif dt.tzinfo is None:
        # Assume UTC, convert to ET
        dt = pytz.UTC.localize(dt).astimezone(ET)
    else:
        # Convert to ET
        dt = dt.astimezone(ET)

    # Check if weekday (Monday=0, Sunday=6)
    if dt.weekday() > 4:
        return False

    current_time = dt.time()

    if check_extended:
        # Extended hours: 4:00 AM - 8:00 PM ET
        market_open = datetime.strptime("04:00:00", "%H:%M:%S").time()
        market_close = datetime.strptime("20:00:00", "%H:%M:%S").time()
    else:
        # Regular hours: 9:30 AM - 4:00 PM ET
        market_open = datetime.strptime("09:30:00", "%H:%M:%S").time()
        market_close = datetime.strptime("16:00:00", "%H:%M:%S").time()

    return market_open <= current_time <= market_close


def get_market_session(dt: Optional[datetime] = None) -> str:
    """
    Get current market session

    Args:
        dt: Datetime to check (default: now)

    Returns:
        Session string: 'pre-market', 'regular', 'after-hours', 'closed'
    """
    if dt is None:
        dt = datetime.now(ET)
    elif dt.tzinfo is None:
        dt = pytz.UTC.localize(dt).astimezone(ET)
    else:
        dt = dt.astimezone(ET)

    # Weekend
    if dt.weekday() > 4:
        return "closed"

    current_time = dt.time()

    # Define market hours
    pre_market_start = datetime.strptime("04:00:00", "%H:%M:%S").time()
    regular_open = datetime.strptime("09:30:00", "%H:%M:%S").time()
    regular_close = datetime.strptime("16:00:00", "%H:%M:%S").time()
    after_hours_end = datetime.strptime("20:00:00", "%H:%M:%S").time()

    if current_time < pre_market_start:
        return "closed"
    elif current_time < regular_open:
        return "pre-market"
    elif current_time < regular_close:
        return "regular"
    elif current_time < after_hours_end:
        return "after-hours"
    else:
        return "closed"
