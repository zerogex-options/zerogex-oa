"""
Centralized configuration constants for ZeroGEX platform

All configurable constants in one place for easy tuning.
"""

import json
import logging
import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# CRITICAL: Load environment variables FIRST before any config is read
load_dotenv()

_cfg_logger = logging.getLogger(__name__)


def _getenv_int(
    name: str, default: int, *, min: Optional[int] = None, max: Optional[int] = None
) -> int:
    """Fetch an int env var with a clear error on parse failure and optional clamping.

    ``min`` and ``max`` are inclusive bounds.  Values outside the bounds are
    clamped and logged at WARNING so a misconfigured env var can't silently
    drive an unreasonable parameter.
    """
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = int(raw.strip())
        except (TypeError, ValueError):
            _cfg_logger.error(
                "Invalid int for env var %s=%r — falling back to default %d",
                name,
                raw,
                default,
            )
            value = default
    if min is not None and value < min:
        _cfg_logger.warning("%s=%d below minimum %d; clamping", name, value, min)
        value = min
    if max is not None and value > max:
        _cfg_logger.warning("%s=%d above maximum %d; clamping", name, value, max)
        value = max
    return value


def _getenv_float(
    name: str, default: float, *, min: Optional[float] = None, max: Optional[float] = None
) -> float:
    """Fetch a float env var with a clear error on parse failure and optional clamping."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = float(raw.strip())
        except (TypeError, ValueError):
            _cfg_logger.error(
                "Invalid float for env var %s=%r — falling back to default %s",
                name,
                raw,
                default,
            )
            value = default
    if min is not None and value < min:
        _cfg_logger.warning("%s=%s below minimum %s; clamping", name, value, min)
        value = min
    if max is not None and value > max:
        _cfg_logger.warning("%s=%s above maximum %s; clamping", name, value, max)
        value = max
    return value


def _getenv_bool(name: str, default: bool) -> bool:
    """Fetch a boolean env var.  Accepts (case-insensitive) true/false/1/0/yes/no."""
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off", ""}:
        return False
    _cfg_logger.error(
        "Invalid bool for env var %s=%r — falling back to default %s",
        name,
        raw,
        default,
    )
    return default


# =============================================================================
# API Configuration
# =============================================================================

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# CORS
# Comma-separated list consumed by src.api.main._parse_cors_origins().
CORS_ALLOW_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS")

# Deployment environment name.  Enables prod-only guardrails
# (e.g. refuse to start with CORS "*" when ENVIRONMENT=production).
ENVIRONMENT = os.getenv("ENVIRONMENT", "development").strip().lower()

# Rate Limiting & Delays
API_REQUEST_TIMEOUT = _getenv_int("API_REQUEST_TIMEOUT", 30, min=1, max=600)  # seconds
API_RETRY_ATTEMPTS = _getenv_int("API_RETRY_ATTEMPTS", 3, min=0, max=20)
API_RETRY_DELAY = _getenv_float("API_RETRY_DELAY", 1.0, min=0.0, max=60.0)  # seconds
API_RETRY_BACKOFF = _getenv_float("API_RETRY_BACKOFF", 2.0, min=1.0, max=10.0)  # multiplier

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
TS_STREAM_REUSE_CONNECTIONS = os.getenv("TS_STREAM_REUSE_CONNECTIONS", "false").lower() == "true"
TS_STREAM_REUSE_QUOTES = os.getenv("TS_STREAM_REUSE_QUOTES", "false").lower() == "true"
TS_WARN_MARKET_HOURS = os.getenv("TS_WARN_MARKET_HOURS", "true").lower() != "false"
OPTION_OI_COVERAGE_ALERT_THRESHOLD = float(os.getenv("OPTION_OI_COVERAGE_ALERT_THRESHOLD", "0.35"))
OPTION_VOLUME_COVERAGE_ALERT_THRESHOLD = float(
    os.getenv("OPTION_VOLUME_COVERAGE_ALERT_THRESHOLD", "0.35")
)
OPTION_VOLUME_WARMUP_MINUTES = int(os.getenv("OPTION_VOLUME_WARMUP_MINUTES", "30"))
OPTION_REST_SEED_ON_RECALC = os.getenv("OPTION_REST_SEED_ON_RECALC", "false").lower() == "true"
FLOW_CACHE_REFRESH_MIN_SECONDS = float(os.getenv("FLOW_CACHE_REFRESH_MIN_SECONDS", "15"))
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

# /api/max-pain/current background refresh.
#
# The max-pain snapshot is computed by a heavy multi-CTE recompute over
# option_chains that exceeds the per-statement timeout (~30s) for our active
# underlyings during market hours.  Running it inline on every request triggers
# 500s and (pre-PR-#77) cascaded into pool-reconnect storms.  Instead, refresh
# the snapshot off the request path on a fixed cadence; the endpoint then
# becomes a pure cache read of max_pain_oi_snapshot.
#
# Symbols not in MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS fall back to the original
# on-demand recompute (still vulnerable to the 30s timeout, but only if anyone
# polls for them).
MAX_PAIN_BACKGROUND_REFRESH_ENABLED = (
    os.getenv("MAX_PAIN_BACKGROUND_REFRESH_ENABLED", "true").lower() == "true"
)
MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS", "SPY,SPX,QQQ").split(",")
    if s.strip()
]
MAX_PAIN_BACKGROUND_REFRESH_INTERVAL_SECONDS = max(
    30, int(os.getenv("MAX_PAIN_BACKGROUND_REFRESH_INTERVAL_SECONDS", "300"))
)
MAX_PAIN_BACKGROUND_REFRESH_STRIKE_LIMIT = max(
    10, min(1000, int(os.getenv("MAX_PAIN_BACKGROUND_REFRESH_STRIKE_LIMIT", "500")))
)
# Per-statement timeout override applied to the background refresh only,
# allowing the heavy recompute to run beyond the pool's default
# DB_STATEMENT_TIMEOUT_MS (~30s).  Applied via SET LOCAL inside a transaction.
MAX_PAIN_BACKGROUND_REFRESH_STATEMENT_TIMEOUT_MS = max(
    1000, int(os.getenv("MAX_PAIN_BACKGROUND_REFRESH_STATEMENT_TIMEOUT_MS", "120000"))
)

# =============================================================================
# Aggregation Configuration
# =============================================================================

# Time Bucket Size
AGGREGATION_BUCKET_SECONDS = int(os.getenv("AGGREGATION_BUCKET_SECONDS", "60"))  # 1 minute

# Buffer Flush Settings
MAX_BUFFER_SIZE = int(os.getenv("MAX_BUFFER_SIZE", "1000"))  # flush if buffer exceeds
BUFFER_FLUSH_INTERVAL = int(os.getenv("BUFFER_FLUSH_INTERVAL", "60"))  # seconds
# Throttle in-minute option upserts per contract/bucket to reduce UPDATE churn.
OPTION_BUCKET_WRITE_MIN_SECONDS = float(os.getenv("OPTION_BUCKET_WRITE_MIN_SECONDS", "5"))

# =============================================================================
# Flow Classification Configuration
# =============================================================================
# Fraction of each half-spread that's classified as mid_volume:
#   0.0  = pure Lee-Ready (any print above mid is ask, below is bid)
#   0.5  ≈ legacy nearest-neighbor behavior
#   1.0  = only at-or-beyond-quote prints count as ask/bid; everything in
#          between is mid
# Default 0.70 widens the mid zone past nearest-neighbor so borderline
# fills (e.g. a print at 5.57 with bid 5.53 / ask 5.58) land in mid_volume
# rather than being credited as full ask volume.
FLOW_CLASSIFY_MID_BAND_PCT = float(os.getenv("FLOW_CLASSIFY_MID_BAND_PCT", "0.70"))
# Route the opening-auction bucket (09:30 ET) to mid_volume instead of running
# Lee-Ready against post-open quotes that don't reflect the auction cross.
FLOW_CLASSIFY_SKIP_OPEN_AUCTION = (
    os.getenv("FLOW_CLASSIFY_SKIP_OPEN_AUCTION", "true").lower() == "true"
)

# =============================================================================
# Symbol Mapping Configuration
# =============================================================================

SYMBOL_ALIASES = os.getenv("SYMBOL_ALIASES", "")
OPTION_ROOT_ALIASES = os.getenv("OPTION_ROOT_ALIASES", "")

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
SIGNAL_SMART_MONEY_DOMINANCE_RATIO = float(os.getenv("SIGNAL_SMART_MONEY_DOMINANCE_RATIO", "1.2"))
SIGNAL_VWAP_DEV_BULL_THRESHOLD_PCT = float(os.getenv("SIGNAL_VWAP_DEV_BULL_THRESHOLD_PCT", "0.2"))
SIGNAL_VWAP_DEV_BEAR_THRESHOLD_PCT = float(os.getenv("SIGNAL_VWAP_DEV_BEAR_THRESHOLD_PCT", "-0.2"))
SIGNAL_PCR_BULLISH_THRESHOLD = float(os.getenv("SIGNAL_PCR_BULLISH_THRESHOLD", "0.7"))
SIGNAL_PCR_BEARISH_THRESHOLD = float(os.getenv("SIGNAL_PCR_BEARISH_THRESHOLD", "1.3"))
SIGNAL_AUTO_TUNE_ENABLED = os.getenv("SIGNAL_AUTO_TUNE_ENABLED", "true").lower() == "true"
SIGNAL_AUTO_TUNE_LOOKBACK_DAYS = max(5, int(os.getenv("SIGNAL_AUTO_TUNE_LOOKBACK_DAYS", "20")))
SIGNAL_AUTO_TUNE_MIN_SAMPLES = max(50, int(os.getenv("SIGNAL_AUTO_TUNE_MIN_SAMPLES", "250")))
SIGNAL_IV_RANK_ENABLED = os.getenv("SIGNAL_IV_RANK_ENABLED", "false").lower() == "true"

# =============================================================================
# Volatility Expansion Configuration
# =============================================================================
VOL_SMART_MONEY_DOMINANCE_RATIO = float(os.getenv("VOL_SMART_MONEY_DOMINANCE_RATIO", "1.2"))
# Calibrated for the industry-standard "dollar gamma per 1% move" GEX
# convention (γ × OI × 100 × S² × 0.01).  Pre-fix values (-5e9 / -3e9) were
# in the share-equivalent share scale; multiplied by ≈7 for SPY-magnitude
# underlyings to keep the same regime classification.
VOL_GAMMA_DEEP_NEGATIVE = float(os.getenv("VOL_GAMMA_DEEP_NEGATIVE", "-35000000000"))
VOL_GAMMA_NEGATIVE = float(os.getenv("VOL_GAMMA_NEGATIVE", "-21000000000"))
VOL_GAMMA_FLIP_NEAR_PCT = float(os.getenv("VOL_GAMMA_FLIP_NEAR_PCT", "0.003"))
VOL_PCR_HIGH = float(os.getenv("VOL_PCR_HIGH", "1.8"))
VOL_PCR_LOW = float(os.getenv("VOL_PCR_LOW", "0.4"))
VOL_AUTO_TUNE_ENABLED = os.getenv("VOL_AUTO_TUNE_ENABLED", "true").lower() == "true"
VOL_AUTO_TUNE_LOOKBACK_DAYS = max(5, int(os.getenv("VOL_AUTO_TUNE_LOOKBACK_DAYS", "30")))
VOL_AUTO_TUNE_MIN_SAMPLES = max(50, int(os.getenv("VOL_AUTO_TUNE_MIN_SAMPLES", "250")))


# =============================================================================
# Signals Engine Configuration
# =============================================================================

SIGNALS_UNDERLYINGS = os.getenv("SIGNALS_UNDERLYINGS", "SPY")
SIGNALS_INTERVAL = max(1, int(os.getenv("SIGNALS_INTERVAL", "1")))
SIGNALS_PORTFOLIO_SIZE = float(os.getenv("SIGNALS_PORTFOLIO_SIZE", "1000000"))

# GEX normalization scale used to map net_gex into [-1, 1] for multiple
# signal components (vol_expansion, strategy_builder, position optimizer).
# Calibrated for the industry-standard "dollar gamma per 1% move" GEX
# convention (γ × OI × 100 × S² × 0.01); the prior 300M default was on the
# share-equivalent scale and is multiplied by ≈7 for SPY-magnitude
# underlyings.  Override via env var if your universe's typical GEX
# magnitude differs.
SIGNAL_GEX_NORMALIZATION = _getenv_float("SIGNAL_GEX_NORMALIZATION", 2_100_000_000.0, min=1.0)
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
SIGNALS_TRIGGER_THRESHOLD = _getenv_float("SIGNALS_TRIGGER_THRESHOLD", 0.50, min=0.0, max=1.0)
# Asymmetric exit: once a position is open in a direction, conviction must
# decay below this floor (lower than the entry trigger) before we close.
# Entry-vs-exit hysteresis cuts whipsaws when MSI oscillates around the trigger.
SIGNALS_EXIT_THRESHOLD = _getenv_float("SIGNALS_EXIT_THRESHOLD", 0.40, min=0.0, max=1.0)
# Combined gate: kelly_fraction × conviction must clear this floor or the
# trade is rejected before sizing. Kills "fire on technically-positive but
# microscopic edge" cases that produce 50/50 scalps.
SIGNALS_CONVICTION_FLOOR = _getenv_float("SIGNALS_CONVICTION_FLOOR", 0.05, min=0.0, max=1.0)
# Trend-direction guard: how big a multi-bar move (in basis points of price)
# is required before _msi_trend_direction commits to bullish/bearish.  The
# previous implicit 5bps threshold flipped on single-tick noise on QQQ/SPY,
# producing the bull→bear→bull whipsaws in trade history.  15bps requires a
# real, measurable trend before a directional debit fires.
SIGNALS_TREND_THRESHOLD_BPS = _getenv_float(
    "SIGNALS_TREND_THRESHOLD_BPS", 15.0, min=0.0, max=10000.0
)
SIGNALS_TREND_LOOKBACK_BARS = _getenv_int("SIGNALS_TREND_LOOKBACK_BARS", 5, min=2, max=120)
SIGNALS_TREND_CONFIRMATION_BARS = max(0, int(os.getenv("SIGNALS_TREND_CONFIRMATION_BARS", "3")))
SIGNALS_TREND_CONFIRMATION_MIN_MATCH = max(
    0, int(os.getenv("SIGNALS_TREND_CONFIRMATION_MIN_MATCH", "1"))
)

# -----------------------------------------------------------------------------
# Position lifecycle controls (Phase 1: stops/targets + min-hold)
# -----------------------------------------------------------------------------
# Minimum seconds a freshly-opened position is protected from reconcile-driven
# closes. Stops/targets/time-stops still fire inside this window; this only
# blocks close-because-the-target-portfolio-flipped churn.
SIGNALS_MIN_HOLD_SECONDS = _getenv_int("SIGNALS_MIN_HOLD_SECONDS", 90, min=0, max=86400)
# Per-trade profit target as a fraction of entry premium. 0.50 = +50% on
# entry debit (a 1.5R take-profit). Set to 0 to disable target-driven exits.
SIGNALS_TARGET_PCT = _getenv_float("SIGNALS_TARGET_PCT", 0.50, min=0.0)
# Time stop: close any open trade older than this many minutes. Prevents
# stale positions from drifting through the close on a quiet afternoon.
# Set to 0 to disable.
SIGNALS_TIME_STOP_MINUTES = _getenv_int("SIGNALS_TIME_STOP_MINUTES", 60, min=0, max=1440)

# Optional timeframe-specific lifecycle overrides (fallback to global values above):
# - scalp: fast turnover
# - intraday: same-session holds
# - swing: multi-hour/multi-session intent
SIGNALS_SCALP_MIN_HOLD_SECONDS = _getenv_int(
    "SIGNALS_SCALP_MIN_HOLD_SECONDS", SIGNALS_MIN_HOLD_SECONDS, min=0, max=86400
)
SIGNALS_INTRADAY_MIN_HOLD_SECONDS = _getenv_int(
    "SIGNALS_INTRADAY_MIN_HOLD_SECONDS", SIGNALS_MIN_HOLD_SECONDS, min=0, max=86400
)
SIGNALS_SWING_MIN_HOLD_SECONDS = _getenv_int(
    "SIGNALS_SWING_MIN_HOLD_SECONDS", SIGNALS_MIN_HOLD_SECONDS, min=0, max=86400 * 14
)

SIGNALS_SCALP_TIME_STOP_MINUTES = _getenv_int(
    "SIGNALS_SCALP_TIME_STOP_MINUTES", SIGNALS_TIME_STOP_MINUTES, min=0, max=1440
)
SIGNALS_INTRADAY_TIME_STOP_MINUTES = _getenv_int(
    "SIGNALS_INTRADAY_TIME_STOP_MINUTES", SIGNALS_TIME_STOP_MINUTES, min=0, max=1440
)
SIGNALS_SWING_TIME_STOP_MINUTES = _getenv_int(
    "SIGNALS_SWING_TIME_STOP_MINUTES", SIGNALS_TIME_STOP_MINUTES, min=0, max=1440 * 14
)

# Entry dedupe + reconcile writer lock controls.
SIGNALS_ENTRY_DEDUPE_WINDOW_SECONDS = _getenv_int(
    "SIGNALS_ENTRY_DEDUPE_WINDOW_SECONDS", 60, min=0, max=3600
)
SIGNALS_RECONCILE_LOCK_ENABLED = _getenv_bool("SIGNALS_RECONCILE_LOCK_ENABLED", True)

# Kelly damping factor (multiplier on the raw kelly fraction).  0.50 = half
# Kelly, the practitioner standard.  Was hard-coded at 0.25 (quarter-Kelly)
# in position_optimizer_engine.py which combined with conviction + tier %
# damping produced microscopic position sizes.
SIGNALS_KELLY_FRACTION = _getenv_float("SIGNALS_KELLY_FRACTION", 0.50, min=0.0, max=1.0)

# -----------------------------------------------------------------------------
# Regime filter (Phase 2: time-of-day + event suppression)
# -----------------------------------------------------------------------------
# Skip *new* entries during low-edge windows: lunch chop, last-N-minutes
# before close, and a ±buffer around scheduled macro events.  Same-direction
# holds and stop-driven exits are unaffected.
SIGNALS_TIME_FILTER_ENABLED = _getenv_bool("SIGNALS_TIME_FILTER_ENABLED", True)
SIGNALS_LUNCH_START_ET = os.getenv("SIGNALS_LUNCH_START_ET", "11:30").strip()
SIGNALS_LUNCH_END_ET = os.getenv("SIGNALS_LUNCH_END_ET", "13:30").strip()
# Conviction (direction-aware MSI 0..1) required to trade *through* the
# lunch chop window.  A genuine reversal or trend-day breakout can clear
# this; routine chop can't.  Lowered from 0.75 so the conviction at the
# kink of an intraday reversal — typically 0.55-0.70 — can still fire
# instead of being held off until the lunch window closes.  Set to 1.0
# to make the lunch block hard.
SIGNALS_LUNCH_MSI_OVERRIDE = _getenv_float("SIGNALS_LUNCH_MSI_OVERRIDE", 0.60, min=0.0, max=1.0)
# Comma-separated signal_source prefixes that bypass the lunch chop
# block entirely.  Advanced signals and Playbook Action Cards each run
# their own setup-specific filters before reaching the regime gate, so
# the time-of-day chop suppressor would otherwise double-gate already
# vetted entries.  Empty string disables the carve-out and reverts to
# the conviction-only override.
SIGNALS_LUNCH_BYPASS_SOURCES = [
    item.strip().lower()
    for item in os.getenv(
        "SIGNALS_LUNCH_BYPASS_SOURCES",
        "advanced:,card:",
    ).split(",")
    if item.strip()
]
# Last-N-minutes before close: only eod_pressure-sourced entries are allowed
# (close-pinning + dealer-flow plays). 0 disables.  The previous 10-minute
# default left 15:50-16:00 covered but 15:30-15:50 wide open for noise-driven
# scalps that systematically lose on theta + spread.  30 minutes pushes the
# lockdown to 15:30, the empirical edge cliff for late-day directional debits.
SIGNALS_LATE_CLOSE_LOCKDOWN_MINUTES = _getenv_int(
    "SIGNALS_LATE_CLOSE_LOCKDOWN_MINUTES", 30, min=0, max=120
)
# Buffer (minutes) around each scheduled event below.  0 disables event filter.
SIGNALS_EVENT_BUFFER_MINUTES = _getenv_int("SIGNALS_EVENT_BUFFER_MINUTES", 15, min=0, max=240)
# Comma-separated list of ISO ET timestamps for FOMC/CPI/NFP/etc.
# Example: "2026-05-07T08:30,2026-05-21T14:00"  (ET, no offset implied).
# When a timestamp has no offset it is interpreted in America/New_York.
SIGNALS_EVENT_CALENDAR = [
    item.strip() for item in os.getenv("SIGNALS_EVENT_CALENDAR", "").split(",") if item.strip()
]

# -----------------------------------------------------------------------------
# Multi-source confirmation for advanced-signal entries (Phase 2.3)
# -----------------------------------------------------------------------------
# When a single advanced signal triggers, require at least one independent
# confirmation in the same direction before opening a position.  Confirmations
# are: another advanced signal triggered, a basic signal score above the
# basic-confirmation cutoff, or the MSI conviction above the MSI-confirmation
# cutoff.
# Default loosened so a single strong advanced trigger can fire on its
# own.  At the kink of an intraday reversal a second confirming source
# rarely arrives in the same minute, and the prior True default
# systematically suppressed the trades the user most wants to catch.
# Set back to True to restore the conservative two-source policy.
SIGNALS_ADVANCED_REQUIRE_CONFIRMATION = _getenv_bool("SIGNALS_ADVANCED_REQUIRE_CONFIRMATION", False)
SIGNALS_ADVANCED_MIN_BASIC_CONFIRM = _getenv_float(
    "SIGNALS_ADVANCED_MIN_BASIC_CONFIRM", 0.30, min=0.0, max=1.0
)
SIGNALS_ADVANCED_MIN_MSI_CONFIRM = _getenv_float(
    "SIGNALS_ADVANCED_MIN_MSI_CONFIRM", 0.50, min=0.0, max=1.0
)

# -----------------------------------------------------------------------------
# Adaptive expiry selection (Phase 3.3)
# -----------------------------------------------------------------------------
# In the first N minutes after the open, 0DTE pricing is dominated by
# overnight risk-premium repricing and gamma flips around dealer hedging
# orders — a coin-flip window.  Bump optimizer dte_min from 0 to 1 inside
# this window so we reach for 1-2 DTE structures with theta protection.
# Set to 0 to disable.
SIGNALS_NO_0DTE_MORNING_MINUTES = _getenv_int("SIGNALS_NO_0DTE_MORNING_MINUTES", 90, min=0, max=390)
# Symmetric afternoon guard: in the last N minutes of the session 0DTE pricing
# is dominated by gamma pin compression + charm decay, and any directional
# debit is racing the clock against fading delta.  Bumping dte_min from 0 to 1
# in this window forces the optimizer to reach for 1-2 DTE structures with
# theta protection.  Set to 0 to disable.
SIGNALS_NO_0DTE_AFTERNOON_MINUTES = _getenv_int(
    "SIGNALS_NO_0DTE_AFTERNOON_MINUTES", 90, min=0, max=390
)

# -----------------------------------------------------------------------------
# Chop-regime directional gate (Phase 4.4)
# -----------------------------------------------------------------------------
# When MSI is in the chop_range band (20-40), directional debits are
# statistically a coin flip minus spread + theta.  Even when an advanced
# signal triggers, require conviction at least this high before allowing a
# directional bull/bear debit to fire from chop.  Below this threshold the
# engine stays in cash regardless of signal_driven flags.
SIGNALS_CHOP_DIRECTIONAL_MIN_CONVICTION = _getenv_float(
    "SIGNALS_CHOP_DIRECTIONAL_MIN_CONVICTION", 0.30, min=0.0, max=1.0
)
# When chop-regime conviction (direction-aware MSI) clears this floor,
# the scalp-tier size cap lifts from the conservative 0.6× to
# ``SIGNALS_CHOP_HIGH_CONVICTION_SIZE``.  This keeps routine chop trades
# small while letting a genuine reversal entry that happens to land in
# chop_range (low MSI + clear directional close path) size up.
SIGNALS_CHOP_HIGH_CONVICTION_THRESHOLD = _getenv_float(
    "SIGNALS_CHOP_HIGH_CONVICTION_THRESHOLD", 0.55, min=0.0, max=1.0
)
SIGNALS_CHOP_HIGH_CONVICTION_SIZE = _getenv_float(
    "SIGNALS_CHOP_HIGH_CONVICTION_SIZE", 0.85, min=0.0, max=1.0
)

# -----------------------------------------------------------------------------
# Daily-loss kill switch (Phase 4.4)
# -----------------------------------------------------------------------------
# Hard ceiling on a single trading day's realized loss.  Once today's summed
# realized PnL drops below -SIGNALS_DAILY_LOSS_KILL_PCT × SIGNALS_PORTFOLIO_SIZE,
# every subsequent compute_target returns a cash target and existing positions
# are managed by their own stop/target plans.  Trading resumes the next session.
# Set to 0 to disable.  Independent from the rolling-N-trade drawdown breaker
# (which only down-sizes); this fully halts new entries.
SIGNALS_DAILY_LOSS_KILL_ENABLED = _getenv_bool("SIGNALS_DAILY_LOSS_KILL_ENABLED", True)
SIGNALS_DAILY_LOSS_KILL_PCT = _getenv_float("SIGNALS_DAILY_LOSS_KILL_PCT", 0.01, min=0.0, max=1.0)

# -----------------------------------------------------------------------------
# Inflection-point sizing boost (Phase 3.2)
# -----------------------------------------------------------------------------
# When the entry source is a "directional expansion" advanced signal
# (range_break_imminence, squeeze_setup), apply a sizing multiplier and
# widen the take-profit target relative to the standard SIGNALS_TARGET_PCT.
# These signals predict directional expansion, so they deserve more risk
# capital and more room to run before the take-profit fires.
SIGNALS_BREAKOUT_SIZE_MULTIPLIER = _getenv_float(
    "SIGNALS_BREAKOUT_SIZE_MULTIPLIER", 1.50, min=0.0, max=4.0
)
SIGNALS_BREAKOUT_TARGET_PCT = _getenv_float("SIGNALS_BREAKOUT_TARGET_PCT", 1.00, min=0.0)
# Comma-separated list of advanced-signal names eligible for the boost.
SIGNALS_BREAKOUT_SIGNAL_SOURCES = [
    item.strip().lower()
    for item in os.getenv(
        "SIGNALS_BREAKOUT_SIGNAL_SOURCES",
        "range_break_imminence,squeeze_setup",
    ).split(",")
    if item.strip()
]

# -----------------------------------------------------------------------------
# Drawdown-aware sizing (Phase 4.3)
# -----------------------------------------------------------------------------
# Rolling-PnL circuit breaker.  After every cycle the engine sums the
# realized PnL of the last SIGNALS_DRAWDOWN_LOOKBACK_TRADES closed trades
# (per symbol).  If that sum drops below
# -SIGNALS_DRAWDOWN_TRIGGER_PCT × SIGNALS_PORTFOLIO_SIZE the new-entry
# sizing is multiplied by SIGNALS_DRAWDOWN_SIZE_MULTIPLIER until the
# rolling sum recovers.  Standard Kelly assumes independent trials; this
# accounts for the fact that consecutive losses usually mean we're in a
# regime the model is reading wrong.
SIGNALS_DRAWDOWN_AWARE_SIZING_ENABLED = _getenv_bool("SIGNALS_DRAWDOWN_AWARE_SIZING_ENABLED", True)
SIGNALS_DRAWDOWN_LOOKBACK_TRADES = _getenv_int(
    "SIGNALS_DRAWDOWN_LOOKBACK_TRADES", 20, min=1, max=500
)
# Trigger as a fraction of portfolio size.  0.02 = 2% of equity.
SIGNALS_DRAWDOWN_TRIGGER_PCT = _getenv_float("SIGNALS_DRAWDOWN_TRIGGER_PCT", 0.02, min=0.0, max=1.0)
# Sizing multiplier applied while the breaker is engaged.  0.50 = half-size.
SIGNALS_DRAWDOWN_SIZE_MULTIPLIER = _getenv_float(
    "SIGNALS_DRAWDOWN_SIZE_MULTIPLIER", 0.50, min=0.0, max=1.0
)

# -----------------------------------------------------------------------------
# Look-ahead audit: GEX stale-buffer (Phase 2.5)
# -----------------------------------------------------------------------------
# The analytics engine aggregates option quotes into 1-minute buckets stamped
# with the bucket's start time.  A row stamped 14:00 was therefore computed
# from quotes that arrived up to 14:00:59 — a bounded but real look-ahead
# window for any backtest replay at second-resolution.  Live trading is
# unaffected (each cycle naturally consumes the most recent data).
#
# When set > 0, this buffer is subtracted from the underlying-quote anchor
# timestamp in both the gex_summary lateral join and the gex_by_strike read,
# effectively saying "only consider GEX rows whose aggregation window
# closed at least N seconds ago".  Default 0 preserves live behavior; set
# to ~60 (one bucket width) when running backtests.
SIGNALS_GEX_STALE_BUFFER_SECONDS = _getenv_int(
    "SIGNALS_GEX_STALE_BUFFER_SECONDS", 0, min=0, max=3600
)
SIGNALS_DRS_HARD_GATES_ENABLED = (
    os.getenv("SIGNALS_DRS_HARD_GATES_ENABLED", "true").lower() == "true"
)
SIGNALS_DRS_CALL_ENTRY_MIN = float(os.getenv("SIGNALS_DRS_CALL_ENTRY_MIN", "0.40"))
SIGNALS_DRS_PUT_ENTRY_MAX = float(os.getenv("SIGNALS_DRS_PUT_ENTRY_MAX", "0.20"))

# -----------------------------------------------------------------------------
# Conviction aggregation -- fights dilution from abstaining components
# -----------------------------------------------------------------------------
# When enabled, the ScoringEngine renormalizes the composite against *only*
# the active (non-abstaining) components, then applies an agreement and an
# extremity amplifier so that 8 components all screaming the same direction
# isn't averaged down to 0.2 by 6 quiet ones.
SIGNALS_CONVICTION_AGGREGATION_ENABLED = (
    os.getenv("SIGNALS_CONVICTION_AGGREGATION_ENABLED", "true").lower() == "true"
)
# Absolute-score cutoff below which a component is treated as abstaining
# (removed from the active-weight denominator). 0.02 keeps legitimately
# near-zero directional reads in the pool while dropping hard zeros.
SIGNALS_CONVICTION_ABSTAIN_EPSILON = float(os.getenv("SIGNALS_CONVICTION_ABSTAIN_EPSILON", "0.02"))
# Maximum multiplier applied when all active components agree in direction.
SIGNALS_CONVICTION_AGREEMENT_MAX_MULT = float(
    os.getenv("SIGNALS_CONVICTION_AGREEMENT_MAX_MULT", "1.75")
)
# Extremity amplifier: extra boost when the loudest active component is
# screaming. Applied multiplicatively on top of agreement.
SIGNALS_CONVICTION_EXTREMITY_MAX_MULT = float(
    os.getenv("SIGNALS_CONVICTION_EXTREMITY_MAX_MULT", "1.30")
)

# -----------------------------------------------------------------------------
# Scalp-tier trigger -- second, lower threshold for reduced-size trades
# -----------------------------------------------------------------------------
# When the composite clears SIGNALS_SCALP_TRIGGER_THRESHOLD but not the main
# SIGNALS_TRIGGER_THRESHOLD, the engine opens a smaller ("scalp") position.
# This captures the "split-second technical opportunities" use case without
# requiring conviction-trade strength.
SIGNALS_SCALP_TRIGGER_ENABLED = os.getenv("SIGNALS_SCALP_TRIGGER_ENABLED", "true").lower() == "true"
SIGNALS_SCALP_TRIGGER_THRESHOLD = float(os.getenv("SIGNALS_SCALP_TRIGGER_THRESHOLD", "0.36"))
# Fraction of normal Kelly-based contracts used for scalp-tier trades.
SIGNALS_SCALP_SIZE_MULTIPLIER = float(os.getenv("SIGNALS_SCALP_SIZE_MULTIPLIER", "0.40"))

# -----------------------------------------------------------------------------
# Strong-conviction DRS override -- lets high-conviction reversals through
# even when the DRS hard gates would block them (e.g. bearish entry on a day
# already below the gamma flip, where the "fresh cross" rule fires once).
# -----------------------------------------------------------------------------
SIGNALS_DRS_OVERRIDE_ENABLED = os.getenv("SIGNALS_DRS_OVERRIDE_ENABLED", "true").lower() == "true"
SIGNALS_DRS_OVERRIDE_THRESHOLD = float(os.getenv("SIGNALS_DRS_OVERRIDE_THRESHOLD", "0.70"))

# Conviction uplift for a fresh gamma-flip cross in the signaled direction.
# Previously "fresh cross" was a hard bearish-entry requirement; symmetrizing
# the DRS gates moved it to an additive sizing boost applied after the gate
# passes. 0.0 disables the boost.
SIGNALS_DRS_FRESH_CROSS_BOOST = max(0.0, float(os.getenv("SIGNALS_DRS_FRESH_CROSS_BOOST", "0.20")))

# -----------------------------------------------------------------------------
# Independent signal trigger risk controls
# -----------------------------------------------------------------------------
_INDEPENDENT_RISK_PROFILE_VALUES = {"conservative", "balanced", "aggressive"}


def _independent_risk_profile(name: str, default: str = "balanced") -> str:
    value = os.getenv(name, default).strip().lower()
    return value if value in _INDEPENDENT_RISK_PROFILE_VALUES else default


def _clamped_threshold(raw: float) -> float:
    return max(0.0, min(1.0, float(raw)))


def _parse_symbol_minutes_map(name: str) -> Dict[str, int]:
    """Parse symbol->minutes map from JSON env var."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}

    out: Dict[str, int] = {}
    for k, v in parsed.items():
        symbol = str(k or "").strip().upper()
        if not symbol:
            continue
        try:
            minutes = int(v)
        except (TypeError, ValueError):
            continue
        out[symbol] = max(15, min(390, minutes))
    return out


def _parse_signal_phase_threshold_overrides(name: str) -> Dict[str, Dict[str, float]]:
    """Parse nested {signal: {phase: threshold}} override map from JSON env var."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}

    out: Dict[str, Dict[str, float]] = {}
    valid_phases = {"scalp", "intraday", "swing"}
    for signal_name, phase_map in parsed.items():
        signal_key = str(signal_name or "").strip().lower()
        if not signal_key or not isinstance(phase_map, dict):
            continue
        clean_phase_map: Dict[str, float] = {}
        for phase, threshold in phase_map.items():
            phase_key = str(phase or "").strip().lower()
            if phase_key not in valid_phases:
                continue
            try:
                clean_phase_map[phase_key] = _clamped_threshold(float(threshold))
            except (TypeError, ValueError):
                continue
        if clean_phase_map:
            out[signal_key] = clean_phase_map
    return out


def _jsonable_copy(value):
    """Return a JSON-safe representation for config debug output."""
    if isinstance(value, dict):
        return {str(k): _jsonable_copy(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable_copy(v) for v in value]
    return value


# Session-phase segmentation for independent-trigger gating.
SIGNALS_INDEPENDENT_PHASE_SCALP_MINUTES_FROM_OPEN = max(
    15, int(os.getenv("SIGNALS_INDEPENDENT_PHASE_SCALP_MINUTES_FROM_OPEN", "75"))
)
SIGNALS_INDEPENDENT_PHASE_SWING_MINUTES_TO_CLOSE = max(
    15, int(os.getenv("SIGNALS_INDEPENDENT_PHASE_SWING_MINUTES_TO_CLOSE", "90"))
)
# Optional per-symbol overrides:
#   {"SPY": 60, "QQQ": 45}
SIGNALS_INDEPENDENT_PHASE_SCALP_MINUTES_BY_SYMBOL = _parse_symbol_minutes_map(
    "SIGNALS_INDEPENDENT_PHASE_SCALP_MINUTES_BY_SYMBOL"
)
SIGNALS_INDEPENDENT_PHASE_SWING_MINUTES_TO_CLOSE_BY_SYMBOL = _parse_symbol_minutes_map(
    "SIGNALS_INDEPENDENT_PHASE_SWING_MINUTES_TO_CLOSE_BY_SYMBOL"
)

# Base threshold by phase (higher = stricter; all values clamped in [0,1]).
SIGNALS_INDEPENDENT_THRESHOLD_SCALP = max(
    0.0, min(1.0, float(os.getenv("SIGNALS_INDEPENDENT_THRESHOLD_SCALP", "0.38")))
)
SIGNALS_INDEPENDENT_THRESHOLD_INTRADAY = max(
    0.0, min(1.0, float(os.getenv("SIGNALS_INDEPENDENT_THRESHOLD_INTRADAY", "0.30")))
)
SIGNALS_INDEPENDENT_THRESHOLD_SWING = max(
    0.0, min(1.0, float(os.getenv("SIGNALS_INDEPENDENT_THRESHOLD_SWING", "0.34")))
)
# Optional per-signal/per-phase overrides:
# {"squeeze_setup":{"scalp":0.40,"intraday":0.33},"eod_pressure":{"swing":0.42}}
SIGNALS_INDEPENDENT_PHASE_THRESHOLD_OVERRIDES = _parse_signal_phase_threshold_overrides(
    "SIGNALS_INDEPENDENT_PHASE_THRESHOLD_OVERRIDES"
)

# Risk-profile multipliers applied to phase thresholds.
SIGNALS_INDEPENDENT_RISK_MULT_CONSERVATIVE = max(
    0.5,
    min(
        2.0,
        float(os.getenv("SIGNALS_INDEPENDENT_RISK_MULT_CONSERVATIVE", "1.15")),
    ),
)
SIGNALS_INDEPENDENT_RISK_MULT_BALANCED = max(
    0.5,
    min(2.0, float(os.getenv("SIGNALS_INDEPENDENT_RISK_MULT_BALANCED", "1.00"))),
)
SIGNALS_INDEPENDENT_RISK_MULT_AGGRESSIVE = max(
    0.5,
    min(2.0, float(os.getenv("SIGNALS_INDEPENDENT_RISK_MULT_AGGRESSIVE", "0.90"))),
)

# Hard floors to avoid over-loose thresholds even in aggressive profiles.
SIGNALS_INDEPENDENT_MIN_THRESHOLD_SQUEEZE_SETUP = max(
    0.0,
    min(1.0, float(os.getenv("SIGNALS_INDEPENDENT_MIN_THRESHOLD_SQUEEZE_SETUP", "0.25"))),
)
SIGNALS_INDEPENDENT_MIN_THRESHOLD_TRAP_DETECTION = max(
    0.0,
    min(1.0, float(os.getenv("SIGNALS_INDEPENDENT_MIN_THRESHOLD_TRAP_DETECTION", "0.25"))),
)
SIGNALS_INDEPENDENT_MIN_THRESHOLD_ZERO_DTE_POSITION_IMBALANCE = max(
    0.0,
    min(
        1.0,
        float(
            os.getenv(
                "SIGNALS_INDEPENDENT_MIN_THRESHOLD_ZERO_DTE_POSITION_IMBALANCE",
                "0.25",
            )
        ),
    ),
)
SIGNALS_INDEPENDENT_MIN_THRESHOLD_GAMMA_VWAP_CONFLUENCE = max(
    0.0,
    min(
        1.0,
        float(os.getenv("SIGNALS_INDEPENDENT_MIN_THRESHOLD_GAMMA_VWAP_CONFLUENCE", "0.20")),
    ),
)
SIGNALS_INDEPENDENT_MIN_THRESHOLD_VOL_EXPANSION = max(
    0.0,
    min(1.0, float(os.getenv("SIGNALS_INDEPENDENT_MIN_THRESHOLD_VOL_EXPANSION", "0.25"))),
)
SIGNALS_INDEPENDENT_MIN_THRESHOLD_EOD_PRESSURE = max(
    0.0,
    min(1.0, float(os.getenv("SIGNALS_INDEPENDENT_MIN_THRESHOLD_EOD_PRESSURE", "0.20"))),
)

# Per-signal risk profile knobs.
SIGNALS_INDEPENDENT_RISK_PROFILE_SQUEEZE_SETUP = _independent_risk_profile(
    "SIGNALS_INDEPENDENT_RISK_PROFILE_SQUEEZE_SETUP",
    "balanced",
)
SIGNALS_INDEPENDENT_RISK_PROFILE_TRAP_DETECTION = _independent_risk_profile(
    "SIGNALS_INDEPENDENT_RISK_PROFILE_TRAP_DETECTION",
    "conservative",
)
SIGNALS_INDEPENDENT_RISK_PROFILE_ZERO_DTE_POSITION_IMBALANCE = _independent_risk_profile(
    "SIGNALS_INDEPENDENT_RISK_PROFILE_ZERO_DTE_POSITION_IMBALANCE",
    "balanced",
)
SIGNALS_INDEPENDENT_RISK_PROFILE_GAMMA_VWAP_CONFLUENCE = _independent_risk_profile(
    "SIGNALS_INDEPENDENT_RISK_PROFILE_GAMMA_VWAP_CONFLUENCE",
    "conservative",
)
SIGNALS_INDEPENDENT_RISK_PROFILE_VOL_EXPANSION = _independent_risk_profile(
    "SIGNALS_INDEPENDENT_RISK_PROFILE_VOL_EXPANSION",
    "conservative",
)
SIGNALS_INDEPENDENT_RISK_PROFILE_EOD_PRESSURE = _independent_risk_profile(
    "SIGNALS_INDEPENDENT_RISK_PROFILE_EOD_PRESSURE",
    "balanced",
)

# -----------------------------------------------------------------------------
# Contrarian direction override
# -----------------------------------------------------------------------------
# A strong consensus from the three contrarian components (exhaustion,
# skew_delta, positioning_trap) pointing *against* the trend-driven composite
# is a classic setup for a flush/squeeze. When that consensus is big enough
# and the composite has enough magnitude to have picked a clear direction,
# flip the composite sign so the portfolio engine routes a counter-trend
# trade instead of doubling down on the exhausted move.
SIGNALS_CONTRARIAN_OVERRIDE_ENABLED = (
    os.getenv("SIGNALS_CONTRARIAN_OVERRIDE_ENABLED", "true").lower() == "true"
)
SIGNALS_CONTRARIAN_REWEIGHT_ENABLED = (
    os.getenv("SIGNALS_CONTRARIAN_REWEIGHT_ENABLED", "true").lower() == "true"
)
SIGNALS_CONTRARIAN_REWEIGHT_MULT = max(
    1.0, float(os.getenv("SIGNALS_CONTRARIAN_REWEIGHT_MULT", "1.45"))
)
SIGNALS_CONTRARIAN_OVERRIDE_THRESHOLD = max(
    0.0, min(1.0, float(os.getenv("SIGNALS_CONTRARIAN_OVERRIDE_THRESHOLD", "0.60")))
)
# Minimum composite magnitude before the override can fire. Prevents flipping
# near-zero composites where the trend signal isn't really pointing anywhere.
SIGNALS_CONTRARIAN_OVERRIDE_MIN_COMPOSITE = max(
    0.0, float(os.getenv("SIGNALS_CONTRARIAN_OVERRIDE_MIN_COMPOSITE", "0.20"))
)

# -----------------------------------------------------------------------------
# Signal confluence trade trigger
# -----------------------------------------------------------------------------
# Cross-signal agreement across Basic + Advanced signal families.  When a
# single advanced signal doesn't clear its individual trigger threshold but
# several signals still agree on direction, a strong confluence is its own
# entry signal.  These knobs gate when that fallback fires.
#
# SIGNALS_CONFLUENCE_ENABLED: master switch.  When false, the only signal
#   entry path is the strongest-single-advanced-signal logic.
# SIGNALS_CONFLUENCE_MIN_OPINIONATED: a signal only counts if |score| is at
#   least this cut-off (weaker reads are treated as abstaining).
# SIGNALS_CONFLUENCE_MIN_AGREE: minimum number of signals that must agree on
#   direction before confluence can trigger.
# SIGNALS_CONFLUENCE_MIN_NET_RATIO: (agree - disagree) / opinionated must be
#   at least this much.  Filters out 3-vs-2 near-splits.
# SIGNALS_CONFLUENCE_MIN_STRENGTH: sum of |score| of agreeing signals.  Makes
#   sure we don't fire on a crowd of whispers.
# SIGNALS_CONFLUENCE_ADVANCED_WEIGHT: multiplier applied to advanced-signal
#   |score| contributions in the agreement sum (basic signals are weight 1.0).
SIGNALS_CONFLUENCE_ENABLED = _getenv_bool("SIGNALS_CONFLUENCE_ENABLED", True)
SIGNALS_CONFLUENCE_MIN_OPINIONATED = _getenv_float(
    "SIGNALS_CONFLUENCE_MIN_OPINIONATED", 0.15, min=0.0, max=1.0
)
SIGNALS_CONFLUENCE_MIN_AGREE = _getenv_int("SIGNALS_CONFLUENCE_MIN_AGREE", 3, min=2, max=20)
SIGNALS_CONFLUENCE_MIN_NET_RATIO = _getenv_float(
    "SIGNALS_CONFLUENCE_MIN_NET_RATIO", 0.50, min=0.0, max=1.0
)
SIGNALS_CONFLUENCE_MIN_STRENGTH = _getenv_float("SIGNALS_CONFLUENCE_MIN_STRENGTH", 1.00, min=0.0)
SIGNALS_CONFLUENCE_ADVANCED_WEIGHT = _getenv_float(
    "SIGNALS_CONFLUENCE_ADVANCED_WEIGHT", 1.25, min=0.0
)

# Stop-loss as a fraction of trade outlay (entry_price * quantity * 100).
# Default -0.25 means the trade is stopped out when it loses 25% of the
# initial premium paid (debit trades) or 25% of max-risk (credit trades).
SIGNALS_STOP_LOSS_PCT = float(os.getenv("SIGNALS_STOP_LOSS_PCT", "-0.25"))

# Optional timeframe-specific stop/target overrides (fallback: global stop/target).
SIGNALS_SCALP_STOP_LOSS_PCT = _getenv_float("SIGNALS_SCALP_STOP_LOSS_PCT", SIGNALS_STOP_LOSS_PCT)
SIGNALS_INTRADAY_STOP_LOSS_PCT = _getenv_float(
    "SIGNALS_INTRADAY_STOP_LOSS_PCT", SIGNALS_STOP_LOSS_PCT
)
SIGNALS_SWING_STOP_LOSS_PCT = _getenv_float("SIGNALS_SWING_STOP_LOSS_PCT", SIGNALS_STOP_LOSS_PCT)

SIGNALS_SCALP_TARGET_PCT = _getenv_float("SIGNALS_SCALP_TARGET_PCT", SIGNALS_TARGET_PCT, min=0.0)
SIGNALS_INTRADAY_TARGET_PCT = _getenv_float(
    "SIGNALS_INTRADAY_TARGET_PCT", SIGNALS_TARGET_PCT, min=0.0
)
SIGNALS_SWING_TARGET_PCT = _getenv_float("SIGNALS_SWING_TARGET_PCT", SIGNALS_TARGET_PCT, min=0.0)

# -----------------------------------------------------------------------------
# Execution model -- realistic entry/exit fills
# -----------------------------------------------------------------------------
# When pricing a candidate spread, long legs fill at ask and short legs fill
# at bid (the opposite on exit).  This parameter widens each side by the given
# fraction to model slippage / adverse-selection on top of the quoted spread:
#   buyer pays  = ask * (1 + slippage)
#   seller gets = bid * (1 - slippage)
# Default 0.0 preserves historical behavior (pure bid/ask fill).  Typical
# live-trading values are 0.01-0.03 (1-3%).
SIGNALS_EXECUTION_SLIPPAGE_PCT = max(0.0, float(os.getenv("SIGNALS_EXECUTION_SLIPPAGE_PCT", "0.0")))

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
            "option_volume_warmup_minutes": OPTION_VOLUME_WARMUP_MINUTES,
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
            "independent_phase_scalp_minutes_from_open": SIGNALS_INDEPENDENT_PHASE_SCALP_MINUTES_FROM_OPEN,
            "independent_phase_swing_minutes_to_close": SIGNALS_INDEPENDENT_PHASE_SWING_MINUTES_TO_CLOSE,
            "independent_phase_scalp_minutes_by_symbol": _jsonable_copy(
                SIGNALS_INDEPENDENT_PHASE_SCALP_MINUTES_BY_SYMBOL
            ),
            "independent_phase_swing_minutes_to_close_by_symbol": _jsonable_copy(
                SIGNALS_INDEPENDENT_PHASE_SWING_MINUTES_TO_CLOSE_BY_SYMBOL
            ),
            "independent_threshold_scalp": SIGNALS_INDEPENDENT_THRESHOLD_SCALP,
            "independent_threshold_intraday": SIGNALS_INDEPENDENT_THRESHOLD_INTRADAY,
            "independent_threshold_swing": SIGNALS_INDEPENDENT_THRESHOLD_SWING,
            "independent_phase_threshold_overrides": _jsonable_copy(
                SIGNALS_INDEPENDENT_PHASE_THRESHOLD_OVERRIDES
            ),
            "independent_risk_profiles": {
                "squeeze_setup": SIGNALS_INDEPENDENT_RISK_PROFILE_SQUEEZE_SETUP,
                "trap_detection": SIGNALS_INDEPENDENT_RISK_PROFILE_TRAP_DETECTION,
                "zero_dte_position_imbalance": SIGNALS_INDEPENDENT_RISK_PROFILE_ZERO_DTE_POSITION_IMBALANCE,
                "gamma_vwap_confluence": SIGNALS_INDEPENDENT_RISK_PROFILE_GAMMA_VWAP_CONFLUENCE,
                "vol_expansion": SIGNALS_INDEPENDENT_RISK_PROFILE_VOL_EXPANSION,
                "eod_pressure": SIGNALS_INDEPENDENT_RISK_PROFILE_EOD_PRESSURE,
            },
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
