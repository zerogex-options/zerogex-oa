"""
Centralized logging configuration module.

This module provides a consistent logging setup across all project components.
It reads the LOG_LEVEL from environment variables and configures logging accordingly.

Usage:
    from logging_config import get_logger
    
    logger = get_logger(__name__)
    logger.info("Application started")
"""

import logging
import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables once at module level
load_dotenv()

# Valid logging levels mapping
VALID_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

# Track if logging has been configured
_logging_configured = False


def _configure_logging() -> int:
    """
    Configure the root logger with settings from environment variables.
    This is called automatically on first logger creation.
    
    Returns:
        int: The configured logging level
    """
    global _logging_configured
    
    if _logging_configured:
        return logging.getLogger().level
    
    # Get and validate logging level from environment
    log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
    
    if log_level_str in VALID_LEVELS:
        log_level = VALID_LEVELS[log_level_str]
    else:
        log_level = logging.INFO
        print(
            f"Warning: Invalid LOG_LEVEL '{log_level_str}', defaulting to INFO. "
            f"Valid options: {', '.join(VALID_LEVELS.keys())}"
        )
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True  # Override any existing configuration
    )
    
    _logging_configured = True
    return log_level


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name, typically __name__ from the calling module.
              If None, returns the root logger.
    
    Returns:
        logging.Logger: Configured logger instance
    
    Example:
        >>> from logging_config import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Starting process")
    """
    # Ensure logging is configured
    _configure_logging()
    
    # Return logger with specified name
    return logging.getLogger(name)


def set_log_level(level: str) -> None:
    """
    Dynamically change the logging level at runtime.
    
    Args:
        level: Logging level as string (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Raises:
        ValueError: If level is not valid
    
    Example:
        >>> from logging_config import set_log_level
        >>> set_log_level('DEBUG')
    """
    level_upper = level.upper()
    
    if level_upper not in VALID_LEVELS:
        raise ValueError(
            f"Invalid log level '{level}'. "
            f"Valid options: {', '.join(VALID_LEVELS.keys())}"
        )
    
    logging.getLogger().setLevel(VALID_LEVELS[level_upper])


# For backward compatibility, provide a default logger
logger = get_logger(__name__)
