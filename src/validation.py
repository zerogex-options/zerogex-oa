"""
Data validation utilities for API responses and data quality.

Market-calendar helpers (``ET``, ``NYSE_HOLIDAYS``, ``is_market_hours``,
``get_market_session``, ``is_engine_run_window``,
``seconds_until_engine_run_window``) live in ``src.market_calendar`` and
are re-exported here for backwards compatibility with existing call
sites.  New code should import them from ``src.market_calendar``
directly.
"""

from typing import Any, Optional
from datetime import datetime
import pytz
from src.utils import get_logger

# Re-export the calendar helpers so ``from src.validation import ...``
# keeps working for callers that predate the split.
from src.market_calendar import (  # noqa: F401 — re-exported for back-compat
    ET,
    NYSE_HOLIDAYS,
    calculate_time_to_expiration,
    get_market_session,
    is_engine_run_window,
    is_market_hours,
    load_nyse_holidays as _load_nyse_holidays,
    seconds_until_engine_run_window,
)

logger = get_logger(__name__)


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

