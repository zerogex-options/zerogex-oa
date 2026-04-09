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

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# CORS
# Comma-separated list consumed by src.api.main._parse_cors_origins().
CORS_ALLOW_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS")

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

# Connectivity settings
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "zerogex")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_PASSWORD_PROVIDER = os.getenv("DB_PASSWORD_PROVIDER", "pgpass")
DB_SECRET_NAME = os.getenv("DB_SECRET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DB_SSLMODE = os.getenv("DB_SSLMODE", "").strip()

# Connection Pool
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "4"))
DB_CONNECT_TIMEOUT_SECONDS = float(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "20"))
DB_CONNECT_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "5"))
DB_CONNECT_RETRY_DELAY_SECONDS = float(os.getenv("DB_CONNECT_RETRY_DELAY_SECONDS", "1.5"))
DB_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "30000"))
DB_KEEPALIVES_IDLE_SECONDS = int(os.getenv("DB_KEEPALIVES_IDLE_SECONDS", "30"))
DB_KEEPALIVES_INTERVAL_SECONDS = int(os.getenv("DB_KEEPALIVES_INTERVAL_SECONDS", "10"))
DB_KEEPALIVES_COUNT = int(os.getenv("DB_KEEPALIVES_COUNT", "5"))

# Data Retention
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "90"))  # days to keep data

# =============================================================================
# Streaming Configuration
# =============================================================================

# Maximum wait between drain cycles.  The main loop wakes *immediately*
# when a stream accumulator receives data; these values only cap the
# longest the loop will block when the streams are quiet.
MARKET_HOURS_POLL_INTERVAL = int(os.getenv("MARKET_HOURS_POLL_INTERVAL", "5"))  # seconds
EXTENDED_HOURS_POLL_INTERVAL = int(os.getenv("EXTENDED_HOURS_POLL_INTERVAL", "30"))  # seconds
CLOSED_HOURS_POLL_INTERVAL = int(os.getenv("CLOSED_HOURS_POLL_INTERVAL", "300"))  # 5 minutes

# Strike Recalculation
# One iteration = one poll cycle (MARKET_HOURS_POLL_INTERVAL seconds during market hours).
# At the default 5s poll interval, STRIKE_RECALC_INTERVAL=12 recalibrates every ~1 minute.
STRIKE_RECALC_INTERVAL = int(os.getenv("STRIKE_RECALC_INTERVAL", "12"))  # iterations

# Memory Management
STRIKE_CLEANUP_INTERVAL = int(os.getenv("STRIKE_CLEANUP_INTERVAL", "100"))  # iterations

# Session template for market data
# Options: "Default" (9:30-16:00), "USEQPre" (4:00-9:30), "USEQ24Hour" (4:00-20:00)
SESSION_TEMPLATE = os.getenv("SESSION_TEMPLATE", "Default")
TS_STREAM_READ_TIMEOUT = int(os.getenv("TS_STREAM_READ_TIMEOUT", "300"))
TS_STREAM_REUSE_CONNECTIONS = (
    os.getenv("TS_STREAM_REUSE_CONNECTIONS", "false").lower() == "true"
)
TS_STREAM_REUSE_QUOTES = os.getenv("TS_STREAM_REUSE_QUOTES", "false").lower() == "true"
TS_WARN_MARKET_HOURS = os.getenv("TS_WARN_MARKET_HOURS", "true").lower() != "false"
OPTION_OI_COVERAGE_ALERT_THRESHOLD = float(
    os.getenv("OPTION_OI_COVERAGE_ALERT_THRESHOLD", "0.35")
)
OPTION_VOLUME_COVERAGE_ALERT_THRESHOLD = float(
    os.getenv("OPTION_VOLUME_COVERAGE_ALERT_THRESHOLD", "0.35")
)
OPTION_REST_SEED_ON_RECALC = (
    os.getenv("OPTION_REST_SEED_ON_RECALC", "false").lower() == "true"
)
FLOW_CACHE_REFRESH_MIN_SECONDS = float(
    os.getenv("FLOW_CACHE_REFRESH_MIN_SECONDS", "15")
)
FLOW_CANONICAL_ONLY = os.getenv("FLOW_CANONICAL_ONLY", "true").lower() == "true"
ANALYTICS_FLOW_CACHE_REFRESH_ENABLED = (
    os.getenv("ANALYTICS_FLOW_CACHE_REFRESH_ENABLED", "true").lower() == "true"
)
TS_REFRESH_BUFFER_SECONDS = int(os.getenv("TS_REFRESH_BUFFER_SECONDS", "30"))
TS_MIN_FORCE_REFRESH_INTERVAL_SECONDS = int(
    os.getenv("TS_MIN_FORCE_REFRESH_INTERVAL_SECONDS", "60")
)
LATEST_QUOTE_CACHE_TTL_SECONDS = float(os.getenv("LATEST_QUOTE_CACHE_TTL_SECONDS", "1.5"))
LATEST_GEX_SUMMARY_CACHE_TTL_SECONDS = float(
    os.getenv("LATEST_GEX_SUMMARY_CACHE_TTL_SECONDS", "1.5")
)
ANALYTICS_CACHE_TTL_SECONDS = float(os.getenv("ANALYTICS_CACHE_TTL_SECONDS", "5.0"))
FLOW_ENDPOINT_CACHE_TTL_SECONDS = float(os.getenv("FLOW_ENDPOINT_CACHE_TTL_SECONDS", "3.0"))

# =============================================================================
# Aggregation Configuration
# =============================================================================

# Time Bucket Size
AGGREGATION_BUCKET_SECONDS = int(os.getenv("AGGREGATION_BUCKET_SECONDS", "60"))  # 1 minute

# Buffer Flush Settings
MAX_BUFFER_SIZE = int(os.getenv("MAX_BUFFER_SIZE", "1000"))  # flush if buffer exceeds
BUFFER_FLUSH_INTERVAL = int(os.getenv("BUFFER_FLUSH_INTERVAL", "60"))  # seconds
# Throttle in-minute option upserts per contract/bucket to reduce UPDATE churn.
OPTION_BUCKET_WRITE_MIN_SECONDS = float(
    os.getenv("OPTION_BUCKET_WRITE_MIN_SECONDS", "5")
)

# =============================================================================
# Symbol Mapping Configuration
# =============================================================================

SYMBOL_ALIASES = os.getenv("SYMBOL_ALIASES", "")
OPTION_ROOT_ALIASES = os.getenv("OPTION_ROOT_ALIASES", "")
OPTION_WEEKLY_ROOTS = os.getenv("OPTION_WEEKLY_ROOTS", "")

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
# Analytics Signal Configuration
# =============================================================================
SIGNAL_SMART_MONEY_DOMINANCE_RATIO = float(
    os.getenv("SIGNAL_SMART_MONEY_DOMINANCE_RATIO", "1.2")
)
SIGNAL_VWAP_DEV_BULL_THRESHOLD_PCT = float(
    os.getenv("SIGNAL_VWAP_DEV_BULL_THRESHOLD_PCT", "0.2")
)
SIGNAL_VWAP_DEV_BEAR_THRESHOLD_PCT = float(
    os.getenv("SIGNAL_VWAP_DEV_BEAR_THRESHOLD_PCT", "-0.2")
)
SIGNAL_PCR_BULLISH_THRESHOLD = float(
    os.getenv("SIGNAL_PCR_BULLISH_THRESHOLD", "0.7")
)
SIGNAL_PCR_BEARISH_THRESHOLD = float(
    os.getenv("SIGNAL_PCR_BEARISH_THRESHOLD", "1.3")
)
SIGNAL_AUTO_TUNE_ENABLED = os.getenv("SIGNAL_AUTO_TUNE_ENABLED", "true").lower() == "true"
SIGNAL_AUTO_TUNE_LOOKBACK_DAYS = max(
    5, int(os.getenv("SIGNAL_AUTO_TUNE_LOOKBACK_DAYS", "20"))
)
SIGNAL_AUTO_TUNE_MIN_SAMPLES = max(
    50, int(os.getenv("SIGNAL_AUTO_TUNE_MIN_SAMPLES", "250"))
)
SIGNAL_IV_RANK_ENABLED = os.getenv("SIGNAL_IV_RANK_ENABLED", "false").lower() == "true"

# =============================================================================
# Volatility Expansion Configuration
# =============================================================================
VOL_SMART_MONEY_DOMINANCE_RATIO = float(
    os.getenv("VOL_SMART_MONEY_DOMINANCE_RATIO", "1.2")
)
VOL_GAMMA_DEEP_NEGATIVE = float(os.getenv("VOL_GAMMA_DEEP_NEGATIVE", "-5000000000"))
VOL_GAMMA_NEGATIVE = float(os.getenv("VOL_GAMMA_NEGATIVE", "-3000000000"))
VOL_GAMMA_FLIP_NEAR_PCT = float(os.getenv("VOL_GAMMA_FLIP_NEAR_PCT", "0.003"))
VOL_PCR_HIGH = float(os.getenv("VOL_PCR_HIGH", "1.8"))
VOL_PCR_LOW = float(os.getenv("VOL_PCR_LOW", "0.4"))
VOL_AUTO_TUNE_ENABLED = os.getenv("VOL_AUTO_TUNE_ENABLED", "true").lower() == "true"
VOL_AUTO_TUNE_LOOKBACK_DAYS = max(
    5, int(os.getenv("VOL_AUTO_TUNE_LOOKBACK_DAYS", "30"))
)
VOL_AUTO_TUNE_MIN_SAMPLES = max(
    50, int(os.getenv("VOL_AUTO_TUNE_MIN_SAMPLES", "250"))
)


# =============================================================================
# Signals Engine Configuration
# =============================================================================

SIGNALS_UNDERLYINGS = os.getenv("SIGNALS_UNDERLYINGS", "SPY")
SIGNALS_PORTFOLIO_SIZE = float(os.getenv("SIGNALS_PORTFOLIO_SIZE", "1000000"))
POSITION_OPTIMIZER_VERBOSE_DIAGNOSTICS = (
    os.getenv("POSITION_OPTIMIZER_VERBOSE_DIAGNOSTICS", "false").lower() == "true"
)

# Aggregate exposure limits — prevent the engine from piling into the same
# direction without regard for what is already on the books.
SIGNALS_MAX_OPEN_TRADES = int(os.getenv("SIGNALS_MAX_OPEN_TRADES", "3"))
SIGNALS_MAX_PORTFOLIO_HEAT_PCT = float(os.getenv("SIGNALS_MAX_PORTFOLIO_HEAT_PCT", "0.06"))
SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES = int(
    os.getenv("SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES", "30")
)

# Stop-loss as a fraction of trade outlay (entry_price * quantity * 100).
# Default -0.25 means the trade is stopped out when it loses 25% of the
# initial premium paid (debit trades) or 25% of max-risk (credit trades).
SIGNALS_STOP_LOSS_PCT = float(os.getenv("SIGNALS_STOP_LOSS_PCT", "-0.25"))

# =============================================================================
# Ingestion/Analytics CLI Defaults
# =============================================================================

INGEST_UNDERLYING = os.getenv("INGEST_UNDERLYING", "SPY")
INGEST_UNDERLYINGS = os.getenv("INGEST_UNDERLYINGS", "")
INGEST_EXPIRATIONS = int(os.getenv("INGEST_EXPIRATIONS", "3"))
INGEST_STRIKE_COUNT = int(os.getenv("INGEST_STRIKE_COUNT", "10"))
ANALYTICS_UNDERLYING = os.getenv("ANALYTICS_UNDERLYING", "SPY")
ANALYTICS_UNDERLYINGS = os.getenv("ANALYTICS_UNDERLYINGS", "")
ANALYTICS_INTERVAL = int(os.getenv("ANALYTICS_INTERVAL", "60"))
ANALYTICS_SNAPSHOT_LOOKBACK_MINUTES = max(
    1, int(os.getenv("ANALYTICS_SNAPSHOT_LOOKBACK_MINUTES", "5"))
)
ANALYTICS_SNAPSHOT_FRESHNESS_SECONDS = max(
    30, int(os.getenv("ANALYTICS_SNAPSHOT_FRESHNESS_SECONDS", "180"))
)
ANALYTICS_MIN_OI_COVERAGE_PCT_ALERT = float(
    os.getenv("ANALYTICS_MIN_OI_COVERAGE_PCT_ALERT", "0.35")
)

# TradeStation credential variables (used by service startup and helper scripts).
TRADESTATION_CLIENT_ID = os.getenv("TRADESTATION_CLIENT_ID")
TRADESTATION_CLIENT_SECRET = os.getenv("TRADESTATION_CLIENT_SECRET")
TRADESTATION_REFRESH_TOKEN = os.getenv("TRADESTATION_REFRESH_TOKEN")
TRADESTATION_USE_SANDBOX = os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true"

# TradeStation CLI test defaults.
TS_TEST = os.getenv("TS_TEST", "all")
TS_SYMBOL = os.getenv("TS_SYMBOL", "SPY")
TS_BARS_BACK = int(os.getenv("TS_BARS_BACK", "5"))
TS_INTERVAL = int(os.getenv("TS_INTERVAL", "1"))
TS_UNIT = os.getenv("TS_UNIT", "Daily")
TS_QUERY = os.getenv("TS_QUERY", "Apple")

# Calendar overrides.
NYSE_HOLIDAYS = os.getenv("NYSE_HOLIDAYS", "")

# =============================================================================
# Ingestion Parity Guard
# =============================================================================

# Emits deterministic payload signatures before DB writes so stream-vs-rest
# ingestion parity can be validated in production without schema changes.
INGEST_PARITY_GUARD_ENABLED = os.getenv("INGEST_PARITY_GUARD_ENABLED", "false").lower() == "true"

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
            "connect_timeout_seconds": DB_CONNECT_TIMEOUT_SECONDS,
            "retention_days": DATA_RETENTION_DAYS,
        },
        "streaming": {
            "market_hours_max_wait": MARKET_HOURS_POLL_INTERVAL,
            "extended_hours_max_wait": EXTENDED_HOURS_POLL_INTERVAL,
            "closed_hours_max_wait": CLOSED_HOURS_POLL_INTERVAL,
            "stream_read_timeout": TS_STREAM_READ_TIMEOUT,
            "option_oi_coverage_alert_threshold": OPTION_OI_COVERAGE_ALERT_THRESHOLD,
            "option_volume_coverage_alert_threshold": OPTION_VOLUME_COVERAGE_ALERT_THRESHOLD,
            "option_rest_seed_on_recalc": OPTION_REST_SEED_ON_RECALC,
            "flow_cache_refresh_min_seconds": FLOW_CACHE_REFRESH_MIN_SECONDS,
            "analytics_flow_cache_refresh_enabled": ANALYTICS_FLOW_CACHE_REFRESH_ENABLED,
            "option_bucket_write_min_seconds": OPTION_BUCKET_WRITE_MIN_SECONDS,
        },
        "features": {
            "greeks_enabled": GREEKS_ENABLED,
            "ingest_parity_guard_enabled": INGEST_PARITY_GUARD_ENABLED,
            "flow_canonical_only": FLOW_CANONICAL_ONLY,
            "analytics_flow_cache_refresh_enabled": ANALYTICS_FLOW_CACHE_REFRESH_ENABLED,
        },
        "auth": {
            "refresh_buffer_seconds": TS_REFRESH_BUFFER_SECONDS,
            "min_force_refresh_interval_seconds": TS_MIN_FORCE_REFRESH_INTERVAL_SECONDS,
        },
        "signals": {
            "underlyings": SIGNALS_UNDERLYINGS,
            "portfolio_size": SIGNALS_PORTFOLIO_SIZE,
            "max_open_trades": SIGNALS_MAX_OPEN_TRADES,
            "max_portfolio_heat_pct": SIGNALS_MAX_PORTFOLIO_HEAT_PCT,
            "same_direction_cooldown_minutes": SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES,
            "stop_loss_pct": SIGNALS_STOP_LOSS_PCT,
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
