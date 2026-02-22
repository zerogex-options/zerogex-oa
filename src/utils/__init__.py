"""
Utility modules for ZeroGEX platform

Components:
- Logging configuration with environment-based levels
- Common utility functions

Usage:
    from src.utils import get_logger, set_log_level

    logger = get_logger(__name__)
    logger.info("Application started")

    # Change log level at runtime
    set_log_level('DEBUG')
"""

from src.utils.logging import get_logger, set_log_level, logger

__all__ = [
    "get_logger",
    "set_log_level",
    "logger",
]
