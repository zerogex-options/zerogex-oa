"""
Centralized configuration constants for ZeroGEX platform

All configurable constants in one place for easy tuning.
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# CRITICAL: Load environment variables FIRST before any config is read
load_dotenv()

# =============================================================================
# API Configuration
# =============================================================================

# Rate Limiting & Delays
API_REQUEST_TIMEOUT = int(os.getenv("API_REQUEST_TIMEOUT", "30"))  # seconds
API_RETRY_ATTEMPTS = int(os.getenv("API_RETRY_ATTEMPTS", "3"))
API_RETRY_DELAY = float(os.getenv("API_RETRY_DELAY", "1.0"))  # seconds
API_RETRY_BACKOFF = float(os.getenv("API_RETRY_BACKOFF", "2.0"))  # multiplier

# Batch Sizes
QUOTE_BATCH_SIZE = int(os.getenv("QUOTE_BATCH_SIZE", "100"))  # TradeStation supports up to 500
OPTION_BATCH_SIZE = int(os.getenv("OPTION_BATCH_SIZE", "100"))

# Delays Between Requests
DELAY_BETWEEN_BATCHES = float(os.getenv("DELAY_BETWEEN_BATCHES", "0.5"))  # seconds
DELAY_BETWEEN_BARS = float(os.getenv("DELAY_BETWEEN_BARS", "1.0"))  # seconds

# =============================================================================
# Database Configuration
# =============================================================================

# Connection Pool
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))

# Data Retention
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "90"))  # days to keep data

# =============================================================================
# Streaming Configuration
# =============================================================================

# Poll Intervals
MARKET_HOURS_POLL_INTERVAL = int(os.getenv("MARKET_HOURS_POLL_INTERVAL", "5"))  # seconds
EXTENDED_HOURS_POLL_INTERVAL = int(os.getenv("EXTENDED_HOURS_POLL_INTERVAL", "30"))  # seconds
CLOSED_HOURS_POLL_INTERVAL = int(os.getenv("CLOSED_HOURS_POLL_INTERVAL", "300"))  # 5 minutes

# Strike Recalculation
STRIKE_RECALC_INTERVAL = int(os.getenv("STRIKE_RECALC_INTERVAL", "10"))  # iterations
PRICE_MOVE_THRESHOLD = float(os.getenv("PRICE_MOVE_THRESHOLD", "1.0"))  # dollars

# Memory Management
STRIKE_CLEANUP_INTERVAL = int(os.getenv("STRIKE_CLEANUP_INTERVAL", "100"))  # iterations

# =============================================================================
# Aggregation Configuration
# =============================================================================

# Time Bucket Size
AGGREGATION_BUCKET_SECONDS = int(os.getenv("AGGREGATION_BUCKET_SECONDS", "60"))  # 1 minute

# Buffer Flush Settings
MAX_BUFFER_SIZE = int(os.getenv("MAX_BUFFER_SIZE", "1000"))  # flush if buffer exceeds
BUFFER_FLUSH_INTERVAL = int(os.getenv("BUFFER_FLUSH_INTERVAL", "60"))  # seconds

# =============================================================================
# Monitoring & Alerting
# =============================================================================

# Health Check Intervals
HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", "60"))  # seconds

# Stale Data Thresholds
UNDERLYING_STALE_THRESHOLD = int(os.getenv("UNDERLYING_STALE_THRESHOLD", "300"))  # 5 minutes
OPTION_STALE_THRESHOLD = int(os.getenv("OPTION_STALE_THRESHOLD", "600"))  # 10 minutes

# Alert Thresholds
CONSECUTIVE_FAILURES_ALERT = int(os.getenv("CONSECUTIVE_FAILURES_ALERT", "5"))
ERROR_RATE_ALERT_THRESHOLD = float(os.getenv("ERROR_RATE_ALERT_THRESHOLD", "0.1"))  # 10%

# =============================================================================
# WebSocket Configuration
# =============================================================================

WEBSOCKET_ENABLED = os.getenv("WEBSOCKET_ENABLED", "false").lower() == "true"
WEBSOCKET_RECONNECT_DELAY = float(os.getenv("WEBSOCKET_RECONNECT_DELAY", "5.0"))  # seconds
WEBSOCKET_PING_INTERVAL = int(os.getenv("WEBSOCKET_PING_INTERVAL", "30"))  # seconds
WEBSOCKET_MAX_RECONNECT_ATTEMPTS = int(os.getenv("WEBSOCKET_MAX_RECONNECT_ATTEMPTS", "10"))

# =============================================================================
# GEX Calculation Configuration
# =============================================================================

GEX_ENABLED = os.getenv("GEX_ENABLED", "false").lower() == "true"
GEX_CALCULATION_INTERVAL = int(os.getenv("GEX_CALCULATION_INTERVAL", "60"))  # seconds
GEX_STRIKE_RANGE = float(os.getenv("GEX_STRIKE_RANGE", "50.0"))  # dollars from current price

# =============================================================================
# Greeks & IV Calculation Configuration
# =============================================================================

# Greeks Calculation
GREEKS_ENABLED = os.getenv("GREEKS_ENABLED", "true").lower() == "true"
RISK_FREE_RATE = float(os.getenv("RISK_FREE_RATE", "0.05"))  # 5%
IMPLIED_VOLATILITY_DEFAULT = float(os.getenv("IMPLIED_VOLATILITY_DEFAULT", "0.20"))  # 20%

# IV Calculation
IV_CALCULATION_ENABLED = os.getenv("IV_CALCULATION_ENABLED", "true").lower() == "true"
IV_MAX_ITERATIONS = int(os.getenv("IV_MAX_ITERATIONS", "100"))
IV_TOLERANCE = float(os.getenv("IV_TOLERANCE", "0.00001"))
IV_MIN = float(os.getenv("IV_MIN", "0.01"))
IV_MAX = float(os.getenv("IV_MAX", "5.0"))

# =============================================================================
# Backfill Configuration
# =============================================================================

# Gap Detection
BACKFILL_ON_STARTUP = os.getenv("BACKFILL_ON_STARTUP", "true").lower() == "true"
MAX_GAP_MINUTES = int(os.getenv("MAX_GAP_MINUTES", "60"))  # auto-backfill if gap > 1 hour
BACKFILL_CHUNK_SIZE = int(os.getenv("BACKFILL_CHUNK_SIZE", "1000"))  # bars per chunk

# =============================================================================
# Grafana/Metrics Configuration
# =============================================================================

METRICS_ENABLED = os.getenv("METRICS_ENABLED", "false").lower() == "true"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9090"))
GRAFANA_DASHBOARD_ENABLED = os.getenv("GRAFANA_DASHBOARD_ENABLED", "false").lower() == "true"

# =============================================================================
# Helper Functions
# =============================================================================

def get_all_config() -> Dict[str, Any]:
    """Get all configuration as dictionary for logging/debugging"""
    return {
        "api": {
            "request_timeout": API_REQUEST_TIMEOUT,
            "retry_attempts": API_RETRY_ATTEMPTS,
            "retry_delay": API_RETRY_DELAY,
            "quote_batch_size": QUOTE_BATCH_SIZE,
            "option_batch_size": OPTION_BATCH_SIZE,
        },
        "database": {
            "pool_min": DB_POOL_MIN,
            "pool_max": DB_POOL_MAX,
            "retention_days": DATA_RETENTION_DAYS,
        },
        "streaming": {
            "market_hours_poll": MARKET_HOURS_POLL_INTERVAL,
            "extended_hours_poll": EXTENDED_HOURS_POLL_INTERVAL,
            "closed_hours_poll": CLOSED_HOURS_POLL_INTERVAL,
        },
        "monitoring": {
            "health_check_interval": HEALTH_CHECK_INTERVAL,
            "underlying_stale_threshold": UNDERLYING_STALE_THRESHOLD,
            "option_stale_threshold": OPTION_STALE_THRESHOLD,
        },
        "features": {
            "websocket_enabled": WEBSOCKET_ENABLED,
            "gex_enabled": GEX_ENABLED,
            "greeks_enabled": GREEKS_ENABLED,
            "metrics_enabled": METRICS_ENABLED,
            "backfill_on_startup": BACKFILL_ON_STARTUP,
        },
    }


def print_config():
    """Pretty print configuration for debugging"""
    config = get_all_config()
    print("\n" + "=" * 80)
    print("ZeroGEX Configuration")
    print("=" * 80)
    for section, values in config.items():
        print(f"\n{section.upper()}:")
        for key, value in values.items():
            print(f"  {key}: {value}")
    print("=" * 80 + "\n")
