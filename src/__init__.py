"""
ZeroGEX Options Analytics Platform

Real-time gamma exposure (GEX) calculations for SPY/SPX options.
"""

__version__ = "0.2.0"
__author__ = "ZeroGEX, LLC"
__email__ = "zerogexoptions@gmail.com"

# Import key modules for easy access
from src.config import get_all_config
from src.validation import (
    safe_float,
    safe_int,
    safe_datetime,
    validate_quote_data,
    validate_bar_data,
    is_market_hours,
    get_market_session,
)

__all__ = [
    "__version__",
    "__author__",
    "__email__",
    "get_all_config",
    "safe_float",
    "safe_int",
    "safe_datetime",
    "validate_quote_data",
    "validate_bar_data",
    "is_market_hours",
    "get_market_session",
]
