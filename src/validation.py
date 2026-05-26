"""
Data validation utilities for API responses and data quality.

Market-calendar helpers (``ET``, ``NYSE_HOLIDAYS``, ``is_market_hours``,
``get_market_session``, ``is_engine_run_window``,
``seconds_until_engine_run_window``) live in ``src.market_calendar`` and
are re-exported here for backwards compatibility with existing call
sites.  New code should import them from ``src.market_calendar``
directly.
"""

from typing import Any, Optional, overload
from datetime import date as _date_type, datetime, timedelta as _timedelta
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
    underlying_feed_expected,
)

logger = get_logger(__name__)


@overload
def safe_float(value: Any, default: float = 0.0, field_name: str = "value") -> float: ...


@overload
def safe_float(value: Any, default: None, field_name: str = "value") -> Optional[float]: ...


def safe_float(
    value: Any, default: Optional[float] = 0.0, field_name: str = "value"
) -> Optional[float]:
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
        logger.warning(
            f"Failed to convert {field_name}='{value}' to float: {e}, using default {default}"
        )
        return default


@overload
def safe_int(value: Any, default: int = 0, field_name: str = "value") -> int: ...


@overload
def safe_int(value: Any, default: None, field_name: str = "value") -> Optional[int]: ...


def safe_int(value: Any, default: Optional[int] = 0, field_name: str = "value") -> Optional[int]:
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
        logger.warning(
            f"Failed to convert {field_name}='{value}' to int: {e}, using default {default}"
        )
        return default


def safe_datetime(
    value: str, default: Optional[datetime] = None, field_name: str = "timestamp"
) -> Optional[datetime]:
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

    if not (low_price <= open_price <= high_price and low_price <= close_price <= high_price):
        logger.warning(
            "Invalid OHLC relationship: O=%s H=%s L=%s C=%s",
            open_price,
            high_price,
            low_price,
            close_price,
        )
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
    Round datetime down to nearest bucket (default: 1 minute).

    Naive datetimes are canonicalized to UTC (every other helper in the
    codebase treats naive == UTC).  Previously ``dt.timestamp()`` on a
    naive value silently reinterpreted it through the process's local
    timezone, producing a bucket boundary off by the local UTC offset
    and a naive return value with mixed TZ semantics.

    Args:
        dt: Datetime to bucket (naive treated as UTC)
        bucket_seconds: Bucket size in seconds

    Returns:
        Bucketed datetime preserving the input's tz (UTC when input was naive).
    """
    from datetime import timezone as _tz

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_tz.utc)
    timestamp = dt.timestamp()
    bucketed_timestamp = (timestamp // bucket_seconds) * bucket_seconds
    return datetime.fromtimestamp(bucketed_timestamp, tz=dt.tzinfo)


# ---------------------------------------------------------------------------
# Cash-session date helpers (preparation for Item 8 of the volume-tracking
# review).  TradeStation resets option cumulative volume at 09:30 ET cash
# open, but the current ``_FlowAccumulator`` session key and the
# ``flow_contract_facts`` LAG partitions use the calendar ET date —
# a mismatch that forces load-bearing patches at the boundary.  Switching
# to cash-session-date keying obsoletes those patches.
#
# These two helpers are SCAFFOLDING ONLY in this commit: they are not yet
# consumed by any production code path.  Consumers will be migrated under
# a feature flag in a subsequent commit so the new behavior is
# observable / reversible before becoming default.
# ---------------------------------------------------------------------------

_ET_CASH_OPEN_HOUR = 9
_ET_CASH_OPEN_MINUTE = 30
_ET_TZ = pytz.timezone("US/Eastern")


def cash_session_date(ts: datetime) -> _date_type:
    """Return the ET cash-session date that ``ts`` belongs to.

    Convention:
      * ts >= 09:30:00 ET on day D  -> D
      * ts <  09:30:00 ET on day D  -> D - 1 (pre-cash-open hours belong
        to the prior cash session for purposes of TradeStation's option-
        cumulative reset semantic)

    Naive timestamps are treated as UTC (matches the rest of the
    validation/bucket helpers).  Weekend / holiday handling is OUT OF
    SCOPE: a Saturday 02:00 ET input returns Friday's calendar date,
    which is the correct "session this volume belongs to" answer for
    Friday-overnight extended-hours flow but does NOT inspect the NYSE
    calendar.  If the caller cares about that distinction it must
    layer ``market_calendar`` on top.

    The 09:30 ET boundary is wall-clock based.  DST transitions are
    handled correctly because ``pytz.localize`` / ``astimezone``
    resolve the local time including the offset, so 09:30 ET means
    09:30 ET on both EDT and EST days.
    """
    from datetime import date as _local_date
    from datetime import timezone as _local_tz

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=_local_tz.utc)
    ts_et = ts.astimezone(_ET_TZ)
    # Strict ">= 09:30:00" boundary: 09:29:59.999999 ET still belongs to
    # the prior session.  Comparing the full (hour, minute, second,
    # microsecond) tuple rather than .time() to avoid any ambiguity
    # introduced by pytz's exotic offset arithmetic.
    minutes_since_midnight = ts_et.hour * 60 + ts_et.minute
    cash_open_minutes = _ET_CASH_OPEN_HOUR * 60 + _ET_CASH_OPEN_MINUTE
    if minutes_since_midnight < cash_open_minutes:
        prior = ts_et.date() - _timedelta(days=1)
        return _local_date(prior.year, prior.month, prior.day)
    return _local_date(ts_et.year, ts_et.month, ts_et.day)


def cash_session_start_utc(session_date: _date_type) -> datetime:
    """UTC timestamp of the 09:30 ET cash-open on ``session_date``.

    The companion to :func:`cash_session_date`: starting from a
    session-date label, return the absolute UTC instant the session
    began.  Useful for building LAG-CASE partitions or hydration
    queries that need to span "this whole cash session" without
    caring whether the date crosses DST.

    Args:
        session_date: The cash-session date as returned by
            :func:`cash_session_date` (a plain ``date``).

    Returns:
        Timezone-aware datetime in UTC at 09:30 ET on ``session_date``.
    """
    from datetime import timezone as _local_tz

    open_et = _ET_TZ.localize(
        datetime(
            session_date.year,
            session_date.month,
            session_date.day,
            _ET_CASH_OPEN_HOUR,
            _ET_CASH_OPEN_MINUTE,
        )
    )
    return open_et.astimezone(_local_tz.utc)
