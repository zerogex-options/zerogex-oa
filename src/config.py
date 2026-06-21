"""
Centralized configuration constants for ZeroGEX platform

All configurable constants in one place for easy tuning.
"""

import json
import logging
import os
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# CRITICAL: Load environment variables FIRST before any config is read
load_dotenv()

_cfg_logger = logging.getLogger(__name__)


def _strip_env_value(raw: Optional[str]) -> Optional[str]:
    """Normalize a raw env-var value before numeric parsing.

    Strips leading/trailing whitespace AND any inline ``# comment`` tail.
    python-dotenv preserves everything after ``=`` literally (including
    inline ``# ...`` annotations), so a ``.env`` file with a line like
    ``KEY=1  # was 2`` would otherwise crash int()/float() with a
    confusing ValueError on service startup.  ``#`` is never valid in a
    numeric value, so dropping at the first ``#`` is safe for the
    helpers that call this.
    """
    if raw is None:
        return None
    stripped = raw.split("#", 1)[0].strip()
    return stripped


def _getenv_int(
    name: str, default: int, *, min: Optional[int] = None, max: Optional[int] = None
) -> int:
    """Fetch an int env var with a clear error on parse failure and optional clamping.

    ``min`` and ``max`` are inclusive bounds.  Values outside the bounds are
    clamped and logged at WARNING so a misconfigured env var can't silently
    drive an unreasonable parameter.
    """
    raw = os.getenv(name)
    cleaned = _strip_env_value(raw)
    if cleaned is None or cleaned == "":
        value = default
    else:
        try:
            value = int(cleaned)
        except (TypeError, ValueError):
            _cfg_logger.error(
                "Invalid int for env var %s=%r (cleaned=%r) — falling back to default %d",
                name,
                raw,
                cleaned,
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
    cleaned = _strip_env_value(raw)
    if cleaned is None or cleaned == "":
        value = default
    else:
        try:
            value = float(cleaned)
        except (TypeError, ValueError):
            _cfg_logger.error(
                "Invalid float for env var %s=%r (cleaned=%r) — falling back to default %s",
                name,
                raw,
                cleaned,
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


def _parse_symbol_float_map(name: str, *, min: float, max: float) -> Dict[str, float]:
    """Parse a ``{symbol: float}`` JSON env map with canonical-upper keys.

    Mirrors :func:`_parse_symbol_minutes_map` but for float values, clamped to
    ``[min, max]``. Used for per-symbol overrides like ``DIVIDEND_YIELD_BY_SYMBOL``.
    Returns ``{}`` on empty/invalid input (never raises at import time).
    """
    # Strip an inline ``# comment`` tail before JSON parsing so a
    # ``{...}  # note`` .env line parses instead of silently falling back.
    raw = _strip_env_value(os.getenv(name)) or ""
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        _cfg_logger.error("Invalid JSON for env var %s=%r — ignoring", name, raw)
        return {}
    if not isinstance(parsed, dict):
        return {}

    out: Dict[str, float] = {}
    for k, v in parsed.items():
        symbol = str(k or "").strip().upper()
        if not symbol:
            continue
        try:
            value = float(v)
        except (TypeError, ValueError):
            continue
        if value < min:
            value = min
        if value > max:
            value = max
        out[symbol] = value
    return out


def _getenv_float_list(
    name: str,
    default: List[float],
    *,
    min_item: Optional[float] = None,
    max_item: Optional[float] = None,
    ascending: bool = False,
) -> List[float]:
    """Fetch a comma-separated list of floats.  Empty / parse failure /
    a result with no usable items falls back to ``default``.

    ``ascending``: when True, drop entries that are not strictly greater
    than the running max (keeps the list ascending without raising).
    """
    # Strip an inline ``# comment`` tail so ``0.2,0.35,0.5  # ladder`` parses
    # instead of silently falling back to the default on the last token.
    raw = _strip_env_value(os.getenv(name))
    if raw is None or raw.strip() == "":
        values: List[float] = list(default)
    else:
        parsed: List[float] = []
        ok = True
        for tok in raw.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                parsed.append(float(tok))
            except (TypeError, ValueError):
                ok = False
                break
        if not ok or not parsed:
            _cfg_logger.error(
                "Invalid float list for env var %s=%r — falling back to default %s",
                name,
                raw,
                default,
            )
            values = list(default)
        else:
            values = parsed
    if min_item is not None:
        values = [v if v >= min_item else min_item for v in values]
    if max_item is not None:
        values = [v if v <= max_item else max_item for v in values]
    if ascending:
        cleaned: List[float] = []
        for v in values:
            if not cleaned or v > cleaned[-1]:
                cleaned.append(v)
        values = cleaned or list(default)
    return values


def _getenv_bool(name: str, default: bool) -> bool:
    """Fetch a boolean env var.  Accepts (case-insensitive) true/false/1/0/yes/no.

    Inline ``# comment`` tails are stripped so an operator with a
    ``KEY=true  # explanation`` line in .env still gets True (python-dotenv
    preserves everything after ``=`` literally, otherwise that value would
    silently fall back to the default + log an error every startup).
    """
    raw = os.getenv(name)
    cleaned = _strip_env_value(raw)
    if cleaned is None:
        return default
    normalized = cleaned.lower()
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


def _getenv_str(name: str, default: str) -> str:
    """Fetch a string env var, stripping an inline ``# comment`` tail.

    Only for values that can never legitimately contain ``#`` (symbols,
    session templates, log level, environment name). Do NOT use for secrets,
    DSNs, CORS lists, or tokens, where ``#`` may be meaningful. Empty after
    stripping falls back to ``default``.
    """
    cleaned = _strip_env_value(os.getenv(name))
    if cleaned is None or cleaned == "":
        return default
    return cleaned


# =============================================================================
# API Configuration
# =============================================================================

# Logging
LOG_LEVEL = _getenv_str("LOG_LEVEL", "INFO").upper()

# CORS
# Comma-separated list consumed by src.api.main._parse_cors_origins().
CORS_ALLOW_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS")

# Deployment environment name.  Enables prod-only guardrails
# (e.g. refuse to start with CORS "*" when ENVIRONMENT=production).
ENVIRONMENT = _getenv_str("ENVIRONMENT", "development").lower()

# Rate Limiting & Delays
API_REQUEST_TIMEOUT = _getenv_int("API_REQUEST_TIMEOUT", 30, min=1, max=600)  # seconds
API_RETRY_ATTEMPTS = _getenv_int("API_RETRY_ATTEMPTS", 3, min=0, max=20)
API_RETRY_DELAY = _getenv_float("API_RETRY_DELAY", 1.0, min=0.0, max=60.0)  # seconds
API_RETRY_BACKOFF = _getenv_float("API_RETRY_BACKOFF", 2.0, min=1.0, max=10.0)  # multiplier

# /api/gex/heatmap scopes returned strikes to spot ± this fraction of
# spot, for every underlying. Sized to the frontend's maximum y-axis
# margin (0.02 base × 4.0 max zoom-out = 0.08) so the colored surface
# fills the price-cropped chart at every zoom level regardless of the
# underlying's price level — a high-priced index (SPX ≈ $7400) and an
# ETF (SPY ≈ $585) now get proportionally equivalent coverage. Replaces
# a hard-coded ±50 absolute band that was ≈±8.5% of SPY but only
# ≈±0.7% of SPX, collapsing the index heatmap into a thin strip.
GEX_HEATMAP_STRIKE_BAND_PCT = _getenv_float("GEX_HEATMAP_STRIKE_BAND_PCT", 0.08, min=0.005, max=0.5)

# Gamma-flip / net-GEX-at-spot are derived from the SpotGamma-style
# spot-shift dealer gamma-exposure profile: option gammas are re-priced
# across a grid of hypothetical spots spanning spot ± SPAN_PCT, stepped
# by STEP_PCT of spot.
#
# The gamma flip is resolved by *adaptive bracket-and-verify*: scan an
# ascending ladder of spans (SPAN_PCT, then GAMMA_PROFILE_EXPANSION_RUNGS)
# and accept the first rung at which a candidate sign change is both
# INTERIOR (well away from the grid edges, where every contract's BS
# gamma has decayed near zero and the profile enters its noise floor)
# AND STRUCTURAL (the |profile| magnitude near the candidate is
# meaningfully non-zero relative to the global peak — i.e. the profile
# is REALLY crossing zero, not just drifting through it).  When even
# the widest rung yields nothing, the flip is persisted NULL+WARN:
# "actionable flip is beyond ±MAX% from spot, or chain is degraded" —
# an honest signal, not a fabricated edge value.
#
# The asymptotic structure of dealer dollar gamma guarantees a
# zero crossing exists somewhere in (0, ∞): f(S)→0− as S→0
# (puts-only, dealer net short under this codebase's sign convention)
# and f(S)→0+ as S→∞ (calls-only, dealer net long).  Our job is to
# RESOLVE it in a window where the signal is strong enough to trust.
#
# ──────────────────────────────────────────────────────────────────
# KNOB BUNDLE: GAMMA_FLIP_PROFILE
# ──────────────────────────────────────────────────────────────────
# The eleven individual knobs below have non-obvious interactions, so
# direct tuning of any one of them in isolation is rarely the right
# answer.  GAMMA_FLIP_PROFILE selects a vetted bundle of all of them
# at once:
#
#   default — current production tuning (these are the values the
#             codebase has been validated against; pick this unless
#             you have a specific operational reason to deviate).
#   strict  — tighter gates: rejects more candidate crossings, more
#             NULLs, higher signal quality on the resolved flips.
#             Use when downstream consumers are sensitive to false
#             positives (e.g. an automated playbook in a degraded
#             chain regime).
#   lenient — looser gates: accepts more candidates, fewer NULLs,
#             more noise.  Use when downstream consumers can tolerate
#             a wider, more frequent signal (e.g. exploratory analysis).
#
# Per-knob env vars below STILL override the bundle for ops emergencies;
# the bundle just supplies the default if no per-knob override is set.
_FLIP_PROFILES: Dict[str, Dict[str, Any]] = {
    "default": {
        "span_pct": 0.20,
        "step_pct": 0.0025,
        "expansion_rungs": [0.35, 0.50],
        "interior_margin": 0.10,
        "structural_min_frac": 0.02,
        "structural_window_pct": 0.01,
        "structural_reference_percentile": 90.0,
        "structural_reference_span_pct": 0.15,
        "structural_active_distance_pct": 0.01,
        "max_flip_distance_pct": 0.08,
        "dte_ref_days": 5.0,
    },
    "strict": {
        # Narrower max distance, higher floor fraction, tighter
        # structural window — accepts only well-anchored, near-spot flips.
        "span_pct": 0.20,
        "step_pct": 0.0025,
        "expansion_rungs": [0.35, 0.50],
        "interior_margin": 0.15,
        "structural_min_frac": 0.05,
        "structural_window_pct": 0.005,
        "structural_reference_percentile": 90.0,
        "structural_reference_span_pct": 0.15,
        "structural_active_distance_pct": 0.005,
        "max_flip_distance_pct": 0.05,
        "dte_ref_days": 5.0,
    },
    "lenient": {
        # Wider max distance, lower floor fraction, looser interior
        # margin — accepts more candidates, including marginal ones.
        "span_pct": 0.20,
        "step_pct": 0.0025,
        "expansion_rungs": [0.35, 0.50],
        "interior_margin": 0.05,
        "structural_min_frac": 0.01,
        "structural_window_pct": 0.02,
        "structural_reference_percentile": 90.0,
        "structural_reference_span_pct": 0.15,
        "structural_active_distance_pct": 0.015,
        "max_flip_distance_pct": 0.12,
        "dte_ref_days": 5.0,
    },
}


def _selected_flip_profile() -> Dict[str, Any]:
    name = os.getenv("GAMMA_FLIP_PROFILE", "default").strip().lower()
    if name not in _FLIP_PROFILES:
        # Unknown bundle: degrade to default rather than fail import.
        # This is the config layer; a bad value here shouldn't crash
        # the engine at startup before we can log the diagnostic.
        return _FLIP_PROFILES["default"]
    return _FLIP_PROFILES[name]


_FP = _selected_flip_profile()

GAMMA_PROFILE_SPAN_PCT = _getenv_float("GAMMA_PROFILE_SPAN_PCT", _FP["span_pct"], min=0.02, max=1.0)
GAMMA_PROFILE_STEP_PCT = _getenv_float(
    "GAMMA_PROFILE_STEP_PCT", _FP["step_pct"], min=0.0001, max=0.05
)

# Adaptive expansion rungs BEYOND the initial GAMMA_PROFILE_SPAN_PCT,
# in ascending order, each an absolute fraction of spot.  Tried only
# when the smaller rung does not yield a structural interior crossing.
# Bound to 1.0 (= ±100% of spot) — beyond that the grid would include
# negative prices, and a flip that far from spot is not actionable on
# any trading horizon.  The largest rung is the implicit "we tried hard
# enough" threshold: anything beyond it is persisted NULL+WARN.
GAMMA_PROFILE_EXPANSION_RUNGS = _getenv_float_list(
    "GAMMA_PROFILE_EXPANSION_RUNGS",
    _FP["expansion_rungs"],
    min_item=0.02,
    max_item=1.0,
    ascending=True,
)
# Composed ladder: initial span + any expansion rungs strictly greater
# than it, ascending.  This is the runtime artifact the analytics engine
# walks; the two env vars above are the user-facing knobs.
GAMMA_PROFILE_SPAN_LADDER: List[float] = [
    GAMMA_PROFILE_SPAN_PCT,
    *[s for s in GAMMA_PROFILE_EXPANSION_RUNGS if s > GAMMA_PROFILE_SPAN_PCT],
]

# Interior gate: a candidate sign change between adjacent profile points
# is rejected unless its linearly-interpolated crossing position sits at
# least this fraction of the grid span away from EITHER edge.  Forces
# the resolver to EXPAND the grid rather than accept a brittle near-edge
# value (the 2026-05-19 QQQ pathology: $839 / $802 flips stuck at the
# ±20% grid boundary).  Range [0.0, 0.49] — 0.49 is the largest value
# that still permits a single qualifying crossing (the grid center).
GAMMA_PROFILE_INTERIOR_MARGIN = _getenv_float(
    "GAMMA_PROFILE_INTERIOR_MARGIN", _FP["interior_margin"], min=0.0, max=0.49
)
# Structural gate: a candidate sign change is rejected unless the peak
# |profile| value within ±STRUCTURAL_WINDOW_PCT × candidate_price of the
# crossing is at least STRUCTURAL_MIN_FRAC × (the chain's robust
# high-magnitude reference — the STRUCTURAL_REFERENCE_PERCENTILE'th
# percentile of |profile| across the whole grid).  Filters noise-floor
# sign changes (profile drifting through zero in a region where every
# contract's gamma has decayed — the morning-open / extended-hours
# artifact where IVs spike, gammas collapse, and the entire grid's
# profile slumps into a low-signal regime where any imbalance can flip
# sign spuriously).
#
# The reference is a robust percentile, NOT the global max: a single
# colossal spike (e.g., a low-IV / OI-concentrated ATM wall on SPX with
# peak |GEX| ≫ p90) used to dominate the global max so every "ordinary"
# crossing in the rest of the chain was rejected as noise relative to
# the spike — the 2026-05-20 SPX/QQQ pathology where the diagnostic
# logged peak=7.5B vs median≈0, so the 2%-of-peak floor swallowed every
# legitimate interior crossing.  A robust percentile is stable to a
# small number of outlier peaks while still matching the global max in
# a truly uniformly-noisy chain (where p90 ≈ max), so the noise-floor
# rejection it was originally designed for is preserved.
GAMMA_PROFILE_STRUCTURAL_MIN_FRAC = _getenv_float(
    "GAMMA_PROFILE_STRUCTURAL_MIN_FRAC", _FP["structural_min_frac"], min=0.0, max=1.0
)
GAMMA_PROFILE_STRUCTURAL_WINDOW_PCT = _getenv_float(
    "GAMMA_PROFILE_STRUCTURAL_WINDOW_PCT", _FP["structural_window_pct"], min=0.001, max=0.10
)
GAMMA_PROFILE_STRUCTURAL_REFERENCE_PERCENTILE = _getenv_float(
    "GAMMA_PROFILE_STRUCTURAL_REFERENCE_PERCENTILE",
    _FP["structural_reference_percentile"],
    min=50.0,
    max=100.0,
)
# Span over which the structural-reference profile is built before the
# active-strike filter is applied.  Held constant across every ladder
# rung so the significance test for a crossing depends only on the
# chain, not on how wide the resolver happens to be scanning.  Without
# this, widening the grid from ±20% to ±35%/±50% diluted p90 with
# deep-OTM near-zero values and lowered the floor enough for the SAME
# marginal crossing to pass at the expansion rung after failing at the
# default — the 2026-05-20 SPX/QQQ pathology where flips clustered
# just inside the 8% distance gate and the chart line walked off the
# visible band.  Anchored at ±15% so the active-strike filter (see
# below) has enough profile points to work with even on chains whose
# OI is geometrically concentrated.
GAMMA_PROFILE_STRUCTURAL_REFERENCE_SPAN_PCT = _getenv_float(
    "GAMMA_PROFILE_STRUCTURAL_REFERENCE_SPAN_PCT",
    _FP["structural_reference_span_pct"],
    min=0.02,
    max=1.0,
)
# Active-strike filter for the structural reference.  After the
# canonical-band profile is built, each grid point is INCLUDED in the
# p90 reference only when its nearest option strike with non-zero open
# interest sits within this fraction of spot.  Anchors the noise floor
# to the actual book rather than to grid points that happen to live in
# OI dead zones (extended-hours degraded chains where most strikes
# have zero OI; long-dated tails where strike spacing widens out
# beyond any meaningful contribution).  Default 1% of spot is roughly
# four grid steps at the default GAMMA_PROFILE_STEP_PCT=0.25%; tighten
# this for symbols with dense OI, loosen for thin chains.
GAMMA_PROFILE_STRUCTURAL_ACTIVE_DISTANCE_PCT = _getenv_float(
    "GAMMA_PROFILE_STRUCTURAL_ACTIVE_DISTANCE_PCT",
    _FP["structural_active_distance_pct"],
    min=0.001,
    max=0.10,
)
# Distance gate: a structurally valid interior crossing is rejected when
# it sits further than this fraction of spot from the current underlying
# price.  A flip that far from spot is not actionable on any reasonable
# trading horizon, and is often a morning-open / IV-spike artifact that
# slips past the structural gate (the SPX 2026-05-20 pathology, where
# the resolved flip descended from ~spot toward the grid floor over the
# first trading hour and ultimately fell off the chart while the
# dashboard's latest-summary endpoint went NULL — same source column,
# diverging displays).  Set to 1.0 to disable (= unbounded, prior
# behavior).  Range [0.01, 1.0].
GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT = _getenv_float(
    "GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT", _FP["max_flip_distance_pct"], min=0.01, max=1.0
)

# The gamma flip is a *multi-day* regime level, but a same-day 0DTE wall
# carries a colossal re-greeked Black-Scholes gamma spike (ATM gamma ∝
# 1/√T) that can pin it to a strike irrelevant for any multi-day horizon
# (the original 751.82-vs-spot pathology).  When enabled, each contract's
# contribution to the spot-shift profile is scaled by a linear
# horizon-occupancy ramp min(1, DTE / DTE_REF_DAYS): each expiry is
# weighted by the fraction of the reference horizon over which the
# contract still exists, so a 0DTE (gone by today's close) is
# down-weighted out of contention for the regime level, while contracts
# living at least the full reference horizon are unaffected (weight 1.0)
# — longer-dated regime structure and all behavior away from near-dated
# are unchanged.  Applied inside the single shared profile, so the flip
# and net-GEX-at-spot stay sign-consistent by construction.
GAMMA_PROFILE_DTE_WEIGHTING = _getenv_bool("GAMMA_PROFILE_DTE_WEIGHTING", True)
GAMMA_PROFILE_DTE_REF_DAYS = _getenv_float(
    "GAMMA_PROFILE_DTE_REF_DAYS", _FP["dte_ref_days"], min=0.5, max=60.0
)
# DTE weight curve shape.  All three shapes cancel the BS 1/√T near-expiry
# gamma spike (linear and exp via w(T) → 0 as T → 0, sqrt via the constant
# w(T)/√T limit), but redistribute weight across the near-dated bucket
# differently.  Tunes how aggressively near-dated (< DTE_REF_DAYS) contracts
# count in the spot-shift profile:
#   * linear (default, preserves prior behavior): w = min(1, DTE/ref).
#       Horizon-occupancy interpretation — the fraction of the reference
#       horizon over which the contract still exists.  Hard saturation
#       at DTE = ref.  Cleanest semantics, one knob.
#   * sqrt:                                       w = sqrt(min(1, DTE/ref)).
#       More aggressive on near-dated — 1DTE under sqrt at ref=2 is 0.71
#       vs linear's 0.50.  Per-OI dollar gamma contribution is CONSTANT
#       (≈ 1/√ref) for all DTE < ref (the 1/√T cancellation goes to a
#       flat shelf instead of a √T ramp).  Hard saturation at DTE = ref.
#   * exp:                                        w = 1 - exp(-DTE/ref).
#       Smoothest curve — no corner at DTE=ref, asymptotic saturation
#       (~0.63 at DTE=ref, ~0.95 at DTE=3*ref).  Same near-zero
#       asymptotics as linear (both ~ T/ref near T=0).  Use when you want
#       to avoid the sharp ON/OFF transition at the saturation point.
# Invalid values fall back to "linear" with a WARN; the same env-var name
# is the operator-facing knob.
GAMMA_PROFILE_DTE_WEIGHT_SHAPE = (
    os.getenv("GAMMA_PROFILE_DTE_WEIGHT_SHAPE", "linear").strip().lower()
)
if GAMMA_PROFILE_DTE_WEIGHT_SHAPE not in ("linear", "sqrt", "exp"):
    import logging as _logging  # local to avoid disturbing module-load order

    _logging.getLogger(__name__).warning(
        "GAMMA_PROFILE_DTE_WEIGHT_SHAPE=%r is invalid; falling back to 'linear'. "
        "Allowed: linear | sqrt | exp.",
        GAMMA_PROFILE_DTE_WEIGHT_SHAPE,
    )
    GAMMA_PROFILE_DTE_WEIGHT_SHAPE = "linear"

# Batch Sizes
QUOTE_BATCH_SIZE = _getenv_int("QUOTE_BATCH_SIZE", 100)  # TradeStation supports up to 500
OPTION_BATCH_SIZE = _getenv_int("OPTION_BATCH_SIZE", 100)

# Delays Between Requests
DELAY_BETWEEN_BATCHES = _getenv_float("DELAY_BETWEEN_BATCHES", 0.5)  # seconds
DELAY_BETWEEN_BARS = _getenv_float("DELAY_BETWEEN_BARS", 1.0)  # seconds

# =============================================================================
# Database Configuration
# =============================================================================

# Connectivity settings
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = _getenv_int("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "zerogex")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_PASSWORD_PROVIDER = os.getenv("DB_PASSWORD_PROVIDER", "pgpass")
DB_SECRET_NAME = os.getenv("DB_SECRET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DB_SSLMODE = os.getenv("DB_SSLMODE", "").strip()

# Connection Pool
DB_POOL_MIN = _getenv_int("DB_POOL_MIN", 1)
DB_POOL_MAX = _getenv_int("DB_POOL_MAX", 4)
DB_CONNECT_TIMEOUT_SECONDS = _getenv_float("DB_CONNECT_TIMEOUT_SECONDS", 20)
DB_CONNECT_RETRIES = _getenv_int("DB_CONNECT_RETRIES", 5)
DB_CONNECT_RETRY_DELAY_SECONDS = _getenv_float("DB_CONNECT_RETRY_DELAY_SECONDS", 1.5)
DB_STATEMENT_TIMEOUT_MS = _getenv_int("DB_STATEMENT_TIMEOUT_MS", 30000)
DB_KEEPALIVES_IDLE_SECONDS = _getenv_int("DB_KEEPALIVES_IDLE_SECONDS", 30)
DB_KEEPALIVES_INTERVAL_SECONDS = _getenv_int("DB_KEEPALIVES_INTERVAL_SECONDS", 10)
DB_KEEPALIVES_COUNT = _getenv_int("DB_KEEPALIVES_COUNT", 5)

# Data Retention
DATA_RETENTION_DAYS = _getenv_int("DATA_RETENTION_DAYS", 90)  # days to keep data

# =============================================================================
# Streaming Configuration
# =============================================================================

# Maximum wait between drain cycles.  The main loop wakes *immediately*
# when a stream accumulator receives data; these values only cap the
# longest the loop will block when the streams are quiet.
MARKET_HOURS_POLL_INTERVAL = _getenv_int("MARKET_HOURS_POLL_INTERVAL", 5)  # seconds
EXTENDED_HOURS_POLL_INTERVAL = _getenv_int("EXTENDED_HOURS_POLL_INTERVAL", 30)  # seconds
CLOSED_HOURS_POLL_INTERVAL = _getenv_int("CLOSED_HOURS_POLL_INTERVAL", 300)  # 5 minutes

# Strike Recalculation
# One iteration = one poll cycle (MARKET_HOURS_POLL_INTERVAL seconds during market hours).
# At the default 5s poll interval, STRIKE_RECALC_INTERVAL=12 recalibrates every ~1 minute.
STRIKE_RECALC_INTERVAL = _getenv_int("STRIKE_RECALC_INTERVAL", 12)  # iterations

# Memory Management
STRIKE_CLEANUP_INTERVAL = _getenv_int("STRIKE_CLEANUP_INTERVAL", 100)  # iterations

# Session template for market data
# Options: "Default" (9:30-16:00), "USEQPre" (4:00-9:30), "USEQ24Hour" (4:00-20:00)
SESSION_TEMPLATE = _getenv_str("SESSION_TEMPLATE", "Default")
TS_STREAM_READ_TIMEOUT = _getenv_int("TS_STREAM_READ_TIMEOUT", 300)
TS_STREAM_REUSE_CONNECTIONS = _getenv_bool("TS_STREAM_REUSE_CONNECTIONS", False)

# TradeStation API rate-limit governor.
# Caps combined API call volume across all ingestion processes in each
# 5-minute UTC window (the same bucket used by ``tradestation_api_calls``).
# When the cross-process estimate meets the cap, callers sleep until the
# next 5-minute boundary instead of issuing further requests that would
# return 429 and burn the per-process retry budget.
#
# ``TS_RATE_LIMIT_PER_5MIN``: hard cap.  Leaves headroom below the observed
# TradeStation ceiling (~1000/5min on the production account); set 0 to
# disable the governor entirely.
# ``TS_RATE_LIMIT_SYNC_INTERVAL``: how often (seconds) each process pushes
# its in-flight partial count to the DB AND refreshes its read of the
# cross-process total.  Shorter = tighter coordination, more DB chatter.
TS_RATE_LIMIT_PER_5MIN = _getenv_int("TS_RATE_LIMIT_PER_5MIN", 900, min=0)
TS_RATE_LIMIT_SYNC_INTERVAL = _getenv_int("TS_RATE_LIMIT_SYNC_INTERVAL", 5, min=1)

# Strikes endpoint cache TTL (seconds).
# Strike chains barely change intraday for liquid underlyings, so re-fetching
# them every STRIKE_RECALC_INTERVAL cycle wastes API calls.  Cache the full
# strike list per (underlying, expiration) for this many seconds.  Set 0
# to disable caching entirely (fetch every call).
TS_STRIKES_CACHE_TTL = _getenv_int("TS_STRIKES_CACHE_TTL", 3600, min=0)

# TradeStation response-header rate-limit gate (preferred path).
# TradeStation exposes ``X-RateLimit-Limit/Period/Remaining/Reset/Resource``
# on every response.  When set, the client parses these headers, tracks
# per-resource state in memory, and gates subsequent requests against the
# observed remaining quota -- replacing blind retry-on-429 with deterministic
# sleep-until-reset.  The static cap above (TS_RATE_LIMIT_PER_5MIN) remains
# as a defense-in-depth fallback for first requests after process start
# when no header has been observed yet.
#
# ``TS_RATE_LIMIT_HEADER_GATE_ENABLED``: master switch.  Set False to disable
# the header-driven gate and rely solely on the static cap + retry path
# (the behaviour shipped at 7e7e56d).  Default True.
# ``TS_RATE_LIMIT_HEADER_MIN_REMAINING``: pre-emptive sleep threshold.  When
# a resource has fewer than this many requests remaining, the client sleeps
# the proportional fraction of the period instead of consuming the last
# request and risking a 429.  Set 0 to only sleep on outright remaining=0.
# Default 1 (sleep only when budget is fully exhausted).
# ``TS_RATE_LIMIT_HEADER_STALE_SECONDS``: discard a resource's cached state
# after this many seconds with no fresh observation, so we don't gate on
# stale data after a long quiet period.  Default 600 (10 minutes -- well
# longer than any normal TradeStation period of 60-300s).
TS_RATE_LIMIT_HEADER_GATE_ENABLED = _getenv_bool("TS_RATE_LIMIT_HEADER_GATE_ENABLED", True)
TS_RATE_LIMIT_HEADER_MIN_REMAINING = _getenv_int(
    "TS_RATE_LIMIT_HEADER_MIN_REMAINING", 1, min=0
)
TS_RATE_LIMIT_HEADER_STALE_SECONDS = _getenv_int(
    "TS_RATE_LIMIT_HEADER_STALE_SECONDS", 600, min=1
)

# Streaming quotes endpoint encodes the symbol list in the URL path. With
# ~1000+ option contracts tracked, a single-connection URL exceeds ~25KB
# and triggers a 414 Request-URI Too Large from TradeStation's gateway.
# Split tracked symbols across multiple stream connections so each URL
# stays well under that gateway limit.
#
# Sizing — two constraints, both per ingestion *account* (NOT per process):
#
#   1. URL length per stream (TradeStation gateway).
#        url_bytes ≈ chunk_size × 25
#        Hard ceiling: ~25KB triggers 414.  Aim ≤ ~20KB (chunk_size ≤ 800).
#
#   2. Concurrent stream count (TradeStation account cap, nominally 10).
#        total_streams = N_underlyings × (1 + ceil(symbols_per_underlying / chunk_size))
#        i.e. one underlying bar stream + one stream per option-symbol chunk,
#        multiplied by the number of ingestion processes (one per underlying
#        symbol — see ``main()`` in ``ingestion/main_engine.py``).
#        A short-lived (<30s) connection is the only on-the-wire fingerprint
#        of cap exhaustion — no 414, no 429 — and is surfaced by the
#        cap-exhaustion WARNING in ``OptionStreamAccumulator._read_stream``
#        and ``UnderlyingBarAccumulator._read_stream``.
#
# Worked examples at the 800-symbol default:
#
#     | symbols/underlying | chunks/process | 1 underlying | 3 underlyings |
#     | ------------------ | -------------- | ------------ | ------------- |
#     |              1,200 |              2 |   3 streams  |   9 streams   |
#     |              1,600 |              2 |   3 streams  |   9 streams   |
#     |              2,000 |              3 |   4 streams  |  10 streams   |
#     |              2,400 |              3 |   4 streams  |  10 streams   |
#
# Operational guidance when the cap-exhaustion WARN fires:
#
#   ``STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION`` is already at the URL
#   ceiling — RAISING IT IS NOT THE FIX.  At ~25 bytes/symbol after URL-
#   encoding, 800 symbols ≈ 20KB per URL, with the 414 cliff at ~25KB;
#   going higher trades cap-exhaustion warnings for hard 414 errors.
#
#   The real lever is ``symbols_per_underlying``.  Trim, in order:
#
#     1. ``INGEST_EXPIRATIONS`` — least disruptive.  No downstream signal
#        filters by the 8+ DTE bucket; ``_gamma_exposure_profile`` reads
#        all contracts but the furthest-dated LEAPS carry full DTE-weight
#        (1.0), so dropping them shifts the gamma flip output (worth
#        measuring against a baseline session before going further).
#     2. ``INGEST_STRIKE_COUNT_MAX`` — graceful degradation.  Only binds
#        on dense-chain underlyings (typically SPX with $5 strikes);
#        SPY/QQQ usually sit below the cap already.  Vol surface and GEX-
#        gradient signals soft-clamp on lower density.
#     3. ``INGEST_STRIKE_PCT_RANGE`` — DO NOT CUT BELOW 4.0.  The wing
#        GEX signal (``src/signals/basic/gex_gradient.py`` ``_WING_WINDOW_PCT
#        = 0.04``) hard-depends on streamed strikes reaching ±4%; below
#        that the wing-fraction confidence collapses to zero.
#
# Historical context: ``bb4a78b`` introduced chunking with a 200-symbol
# default sized for a single-underlying deployment (1100 symbols → 6
# chunks → 7 streams, "within the 10-stream cap").  That sizing implicitly
# assumed N_underlyings = 1; running three underlyings at 200/chunk
# pushed the account to 15+ streams and the underlying bar stream was the
# most frequent casualty (it started last in ``_start_accumulators`` so it
# lost the race for the remaining slot first).  The 500 default + the
# start-order / no-recalc-thrash fixes in ``adaa9bc`` kept a 3×2000
# deployment functioning even though it formally over-subscribed.  Live
# 3-underlying + VIX runs in 2026-06 still tripped the cap during a
# reconnect storm (a 1000-symbol process at 500/chunk = 2 option chunks,
# a 1500-symbol process = 3; account total 12-16 streams), so the default
# moved to 800 — the upper bound that keeps URLs under 414.  At 800/chunk
# the 1500-symbol process collapses to 2 chunks, dropping the account
# total to ~12 with VIX, ~8-9 without.  Set lower via the env var if a
# given deployment's per-chunk symbol mix produces over-long URIs.
STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION = _getenv_int(
    "STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION", 800
)

# Underlying-stream data-staleness watchdog. The TradeStation bar stream
# can stay socket-alive (heartbeats flowing) while delivering zero bars —
# a "connected but data-starved" gap that the socket read timeout and the
# thread-liveness check both structurally miss. When no underlying bar has
# arrived for this many seconds during (extended) market hours, the
# supervisor force-reconnects the underlying accumulator (only — the
# options stream is left untouched). Cooldown prevents thrash; after the
# max consecutive ineffective reconnects it is treated as an upstream
# outage (ERROR, backed-off) rather than restarted in a tight loop.
# Warn (observability only, no action) once the underlying feed has been
# silent this long. Must sit ABOVE the bar cadence (1-minute bars => ~60s
# between bars is normal) so healthy operation never trips it, and BELOW
# UNDERLYING_STREAM_STALE_RESTART_SECONDS so operators get a heads-up
# before the supervisor force-reconnects. Gauged in wall-clock seconds,
# NOT empty-drain count: the poll loop wakes sub-second on every option
# tick, so the drain count races far ahead of real time.
UNDERLYING_STREAM_STALE_WARN_SECONDS = _getenv_int("UNDERLYING_STREAM_STALE_WARN_SECONDS", 75)
UNDERLYING_STREAM_STALE_RESTART_SECONDS = _getenv_int(
    "UNDERLYING_STREAM_STALE_RESTART_SECONDS", 120
)
# The thresholds above are tuned for the dense regular cash session where a
# 1-minute bar prints roughly every ~60s. In pre-market / after-hours an
# equity/ETF (SPY, QQQ — cash indices are clamped out of these windows) trades
# thinly and a 1-minute bar stream legitimately goes minutes between bars, so
# the regular-session thresholds produce false STALE/restart storms. Extended
# hours therefore get their own, much wider thresholds.
UNDERLYING_STREAM_STALE_WARN_SECONDS_EXTENDED = _getenv_int(
    "UNDERLYING_STREAM_STALE_WARN_SECONDS_EXTENDED", 300
)
UNDERLYING_STREAM_STALE_RESTART_SECONDS_EXTENDED = _getenv_int(
    "UNDERLYING_STREAM_STALE_RESTART_SECONDS_EXTENDED", 600
)
UNDERLYING_STREAM_RESTART_COOLDOWN_SECONDS = _getenv_int(
    "UNDERLYING_STREAM_RESTART_COOLDOWN_SECONDS", 90
)
UNDERLYING_STREAM_MAX_RESTART_ATTEMPTS = _getenv_int("UNDERLYING_STREAM_MAX_RESTART_ATTEMPTS", 5)
# After the fast retry budget is exhausted the supervisor enters a
# backed-off state — previously terminal until the process restarted.
# A 2026-06 prod incident sat in that state for 17 hours, accumulating
# 1.1M Greeks rejects, because no further reconnect was ever attempted.
# Now: after this many seconds in the backed-off state, allow ONE more
# reconnect, then drop back to backed-off until the interval elapses
# again. Resets the fast-retry counter so a subsequent transient gap
# gets the same full budget. 10 min is long enough to avoid pestering
# a genuinely-down upstream and short enough that a recovered upstream
# is rediscovered within a single market half-hour.
UNDERLYING_STREAM_BACKOFF_RETRY_INTERVAL_SECONDS = _getenv_int(
    "UNDERLYING_STREAM_BACKOFF_RETRY_INTERVAL_SECONDS", 600
)
TS_STREAM_REUSE_QUOTES = _getenv_bool("TS_STREAM_REUSE_QUOTES", False)
TS_WARN_MARKET_HOURS = _getenv_bool("TS_WARN_MARKET_HOURS", True)
OPTION_OI_COVERAGE_ALERT_THRESHOLD = _getenv_float("OPTION_OI_COVERAGE_ALERT_THRESHOLD", 0.35)
OPTION_VOLUME_COVERAGE_ALERT_THRESHOLD = _getenv_float(
    "OPTION_VOLUME_COVERAGE_ALERT_THRESHOLD", 0.35
)
OPTION_VOLUME_WARMUP_MINUTES = _getenv_int("OPTION_VOLUME_WARMUP_MINUTES", 30)
OPTION_OI_WARMUP_MINUTES = _getenv_int("OPTION_OI_WARMUP_MINUTES", 5)
OPTION_REST_SEED_ON_RECALC = _getenv_bool("OPTION_REST_SEED_ON_RECALC", False)
FLOW_CACHE_REFRESH_MIN_SECONDS = _getenv_float("FLOW_CACHE_REFRESH_MIN_SECONDS", 15)
FLOW_CANONICAL_ONLY = _getenv_bool("FLOW_CANONICAL_ONLY", True)
ANALYTICS_FLOW_CACHE_REFRESH_ENABLED = _getenv_bool("ANALYTICS_FLOW_CACHE_REFRESH_ENABLED", True)
TS_REFRESH_BUFFER_SECONDS = _getenv_int("TS_REFRESH_BUFFER_SECONDS", 30)
TS_MIN_FORCE_REFRESH_INTERVAL_SECONDS = _getenv_int("TS_MIN_FORCE_REFRESH_INTERVAL_SECONDS", 60)
LATEST_QUOTE_CACHE_TTL_SECONDS = _getenv_float("LATEST_QUOTE_CACHE_TTL_SECONDS", 1.5)
LATEST_GEX_SUMMARY_CACHE_TTL_SECONDS = _getenv_float("LATEST_GEX_SUMMARY_CACHE_TTL_SECONDS", 1.5)
ANALYTICS_CACHE_TTL_SECONDS = _getenv_float("ANALYTICS_CACHE_TTL_SECONDS", 5.0)
FLOW_ENDPOINT_CACHE_TTL_SECONDS = _getenv_float("FLOW_ENDPOINT_CACHE_TTL_SECONDS", 3.0)

# Max-pain daily snapshot refresh (scheduled, off-process).
#
# Max pain is a daily figure — open interest only changes at settlement.
# The recompute is a heavy multi-CTE scan over option_chains; running it
# every 5 min in-process (old background loop) or inline on the request
# path (old on-demand fallback) hammered option_chains during the cash
# session and starved the Analytics engine.  It now runs ONCE per day,
# pre-market, as src.tools.max_pain_refresh driven by
# zerogex-oa-max-pain-refresh.timer; /api/max-pain/current is a pure
# cache read of max_pain_oi_snapshot.  These constants configure that
# job (env var names kept stable so operator .env files don't churn).
MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS", "SPY,SPX,QQQ").split(",")
    if s.strip()
]
MAX_PAIN_BACKGROUND_REFRESH_STRIKE_LIMIT = max(
    10, min(1000, _getenv_int("MAX_PAIN_BACKGROUND_REFRESH_STRIKE_LIMIT", 500))
)
# Per-statement timeout for the recompute, applied via SET LOCAL on the
# server and forwarded as asyncpg ``timeout=`` so the client-side
# command_timeout doesn't fire first.  300s covers SPX during/after the
# cash session (the active_symbols + LATERAL latest-per-contract +
# settlement-candidate CTE chain exceeded 120s on a 2026-05-14 prod
# observation).  The job runs off-hours with the box idle, so a long
# budget here no longer contends with anything; if it still trips,
# optimize the query (start by dropping the strike limit from 500) rather
# than bumping this further.
MAX_PAIN_BACKGROUND_REFRESH_STATEMENT_TIMEOUT_MS = max(
    1000, _getenv_int("MAX_PAIN_BACKGROUND_REFRESH_STATEMENT_TIMEOUT_MS", 300000)
)

# =============================================================================
# Aggregation Configuration
# =============================================================================

# Time Bucket Size
AGGREGATION_BUCKET_SECONDS = _getenv_int("AGGREGATION_BUCKET_SECONDS", 60)  # 1 minute

# Buffer Flush Settings
MAX_BUFFER_SIZE = _getenv_int("MAX_BUFFER_SIZE", 1000)  # flush if buffer exceeds
BUFFER_FLUSH_INTERVAL = _getenv_int("BUFFER_FLUSH_INTERVAL", 60)  # seconds
# Throttle in-minute option upserts per contract/bucket to reduce UPDATE churn.
OPTION_BUCKET_WRITE_MIN_SECONDS = _getenv_float("OPTION_BUCKET_WRITE_MIN_SECONDS", 5)

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
FLOW_CLASSIFY_MID_BAND_PCT = _getenv_float("FLOW_CLASSIFY_MID_BAND_PCT", 0.70)
# Route the opening-auction bucket (09:30 ET) to mid_volume instead of running
# Lee-Ready against post-open quotes that don't reflect the auction cross.
FLOW_CLASSIFY_SKIP_OPEN_AUCTION = _getenv_bool("FLOW_CLASSIFY_SKIP_OPEN_AUCTION", True)
# Staleness guard for the Lee-Ready prior-tick quote.  Classification
# normally compares a trade against the NBBO that prevailed *before* it
# (the prior tick), which avoids crediting a marketable order against the
# post-trade NBBO it just moved.  But the prior tick is only a valid
# pre-trade proxy when it is RECENT.  On a contract that goes quiet and
# then the price moves, the last NBBO we recorded can be many seconds (or
# a full minute) old; comparing a fresh print against that stale quote
# degrades the quote-test into a bar-over-bar tick-test and inverts the
# side (a bid-hitting sell into a fast up-move reads as a lift -> "buy").
# When the prior tick is older than this many seconds relative to the
# trade being classified, fall back to the snapshot's own
# (contemporaneous) NBBO, which is sampled together with the trade.
# 0 disables the guard (always prior-tick; legacy behavior).
FLOW_CLASSIFY_PRIOR_TICK_MAX_AGE_SECONDS = _getenv_float(
    "FLOW_CLASSIFY_PRIOR_TICK_MAX_AGE_SECONDS", 10.0
)

# =============================================================================
# Symbol Mapping Configuration
# =============================================================================

SYMBOL_ALIASES = os.getenv("SYMBOL_ALIASES", "")
OPTION_ROOT_ALIASES = os.getenv("OPTION_ROOT_ALIASES", "")
# Per-underlying TS symbol used to query the MONTHLY chain. Same alias
# format as SYMBOL_ALIASES. See INGEST_MONTHLY_EXPIRATIONS below for the
# rationale; resolved by src.symbols.resolve_monthly_underlying().
INGEST_MONTHLY_UNDERLYING_ALIASES = os.getenv("INGEST_MONTHLY_UNDERLYING_ALIASES", "")

# =============================================================================
# Greeks & IV Calculation Configuration
# =============================================================================

# Greeks Calculation
GREEKS_ENABLED = _getenv_bool("GREEKS_ENABLED", True)
RISK_FREE_RATE = _getenv_float("RISK_FREE_RATE", 0.05)  # 5%
# Continuous dividend yield (q) for the Black-Scholes-Merton model. Default
# 0.0 keeps every Greek/price byte-identical to the prior dividend-free model
# so deploying is a no-op; operators can set it per underlying basket (e.g.
# ~0.013 for SPY, ~0.015 for the SPX constituents) to remove the
# systematic call/put-delta and solved-IV bias on dividend-paying names.
#
# Parsed via _getenv_float (NOT bare float()) so an inline ``# comment`` tail
# or stray whitespace in the .env value can't crash service startup —
# python-dotenv preserves everything after ``=`` literally.
DIVIDEND_YIELD = _getenv_float("DIVIDEND_YIELD", 0.0, min=0.0, max=0.2)
# Per-symbol dividend-yield overrides, keyed by CANONICAL symbol (e.g. "SPY",
# "SPX" — what get_canonical_symbol returns, NOT "$SPX.X"). JSON map, e.g.
# DIVIDEND_YIELD_BY_SYMBOL='{"SPY": 0.013, "QQQ": 0.006, "SPX": 0.015}'.
# Any symbol not in the map falls back to the scalar DIVIDEND_YIELD above.
# Note: $SPX.X is a cash index that pays no dividend itself, but its BSM
# forward should still discount the constituents' yield (~1.5%), so a non-zero
# q for "SPX" is correct.
DIVIDEND_YIELD_BY_SYMBOL = _parse_symbol_float_map("DIVIDEND_YIELD_BY_SYMBOL", min=0.0, max=0.2)


def resolve_dividend_yield(symbol: str) -> float:
    """Dividend yield q for ``symbol`` (canonical), or the scalar fallback.

    Each ingestion/analytics worker runs for a single symbol and resolves its
    q once at construction, so the per-symbol map is consulted exactly here.
    """
    return DIVIDEND_YIELD_BY_SYMBOL.get((symbol or "").upper(), DIVIDEND_YIELD)


IMPLIED_VOLATILITY_DEFAULT = _getenv_float("IMPLIED_VOLATILITY_DEFAULT", 0.20)  # 20%

# IV Calculation
IV_CALCULATION_ENABLED = _getenv_bool("IV_CALCULATION_ENABLED", True)
IV_MAX_ITERATIONS = _getenv_int("IV_MAX_ITERATIONS", 100)
IV_TOLERANCE = _getenv_float("IV_TOLERANCE", 0.00001)
IV_MIN = _getenv_float("IV_MIN", 0.01)
IV_MAX = _getenv_float("IV_MAX", 5.0)
# Windowed clamp telemetry for the IV solver. Every Newton iterate is
# clamped into [IV_MIN, IV_MAX]; deep-OTM / near-expiry strikes saturate
# the bounds as a matter of course, so a raw per-hit count is noise.
# Instead the solver reports the *fraction of solves* that saturated over
# a rolling window: INFO normally, WARNING only when that fraction is high
# enough to suggest IV_MIN/IV_MAX are miscalibrated for the regime.
IV_CLAMP_REPORT_INTERVAL_SECONDS = _getenv_float("IV_CLAMP_REPORT_INTERVAL_SECONDS", 300)
IV_CLAMP_WARN_FRACTION = _getenv_float("IV_CLAMP_WARN_FRACTION", 0.25)

# =============================================================================
# Analytics Signal Configuration
# =============================================================================
SIGNAL_SMART_MONEY_DOMINANCE_RATIO = _getenv_float("SIGNAL_SMART_MONEY_DOMINANCE_RATIO", 1.2)
SIGNAL_VWAP_DEV_BULL_THRESHOLD_PCT = _getenv_float("SIGNAL_VWAP_DEV_BULL_THRESHOLD_PCT", 0.2)
SIGNAL_VWAP_DEV_BEAR_THRESHOLD_PCT = _getenv_float("SIGNAL_VWAP_DEV_BEAR_THRESHOLD_PCT", -0.2)
SIGNAL_PCR_BULLISH_THRESHOLD = _getenv_float("SIGNAL_PCR_BULLISH_THRESHOLD", 0.7)
SIGNAL_PCR_BEARISH_THRESHOLD = _getenv_float("SIGNAL_PCR_BEARISH_THRESHOLD", 1.3)
SIGNAL_AUTO_TUNE_ENABLED = _getenv_bool("SIGNAL_AUTO_TUNE_ENABLED", True)
SIGNAL_AUTO_TUNE_LOOKBACK_DAYS = max(5, _getenv_int("SIGNAL_AUTO_TUNE_LOOKBACK_DAYS", 20))
SIGNAL_AUTO_TUNE_MIN_SAMPLES = max(50, _getenv_int("SIGNAL_AUTO_TUNE_MIN_SAMPLES", 250))
SIGNAL_IV_RANK_ENABLED = _getenv_bool("SIGNAL_IV_RANK_ENABLED", False)

# =============================================================================
# Volatility Expansion Configuration
# =============================================================================
VOL_SMART_MONEY_DOMINANCE_RATIO = _getenv_float("VOL_SMART_MONEY_DOMINANCE_RATIO", 1.2)
# Calibrated for the industry-standard "dollar gamma per 1% move" GEX
# convention (γ × OI × 100 × S² × 0.01).  Pre-fix values (-5e9 / -3e9) were
# in the share-equivalent share scale; multiplied by ≈7 for SPY-magnitude
# underlyings to keep the same regime classification.
VOL_GAMMA_DEEP_NEGATIVE = _getenv_float("VOL_GAMMA_DEEP_NEGATIVE", -35000000000)
VOL_GAMMA_NEGATIVE = _getenv_float("VOL_GAMMA_NEGATIVE", -21000000000)
VOL_GAMMA_FLIP_NEAR_PCT = _getenv_float("VOL_GAMMA_FLIP_NEAR_PCT", 0.003)
VOL_PCR_HIGH = _getenv_float("VOL_PCR_HIGH", 1.8)
VOL_PCR_LOW = _getenv_float("VOL_PCR_LOW", 0.4)
VOL_AUTO_TUNE_ENABLED = _getenv_bool("VOL_AUTO_TUNE_ENABLED", True)
VOL_AUTO_TUNE_LOOKBACK_DAYS = max(5, _getenv_int("VOL_AUTO_TUNE_LOOKBACK_DAYS", 30))
VOL_AUTO_TUNE_MIN_SAMPLES = max(50, _getenv_int("VOL_AUTO_TUNE_MIN_SAMPLES", 250))


# =============================================================================
# Signals Engine Configuration
# =============================================================================

SIGNALS_UNDERLYINGS = os.getenv("SIGNALS_UNDERLYINGS", "SPY")
SIGNALS_INTERVAL = max(1, _getenv_int("SIGNALS_INTERVAL", 1))
SIGNALS_PORTFOLIO_SIZE = _getenv_float("SIGNALS_PORTFOLIO_SIZE", 1000000)

# ---------------------------------------------------------------------------
# Playbook pattern calibration (empirical-base feedback loop)
#
# When enabled, the live PlaybookEngine replaces each pattern's hand-set
# ``pattern_base`` prior with the empirical win rate measured by the playbook
# backtest harness (``playbook_pattern_stats.proposed_base``), so live Action
# Card confidence reflects what a pattern actually did rather than a guess.
# OFF by default — turning it on changes live trade-signal confidence, so it
# is an explicit operator decision. See docs/design/pattern-calibration.md.
# ---------------------------------------------------------------------------
SIGNALS_PATTERN_CALIBRATION_ENABLED = _getenv_bool(
    "SIGNALS_PATTERN_CALIBRATION_ENABLED", False
)
# Minimum resolved trades in a (pattern, underlying) window before its measured
# base is trusted. Below this the hand-set prior is kept.
SIGNALS_PATTERN_CALIBRATION_MIN_SAMPLES = _getenv_int(
    "SIGNALS_PATTERN_CALIBRATION_MIN_SAMPLES", 20, min=1
)
# Ignore stats windows whose end date is older than this — a pattern's edge
# decays, so stale measurements should fall back to the prior.
SIGNALS_PATTERN_CALIBRATION_MAX_AGE_DAYS = _getenv_int(
    "SIGNALS_PATTERN_CALIBRATION_MAX_AGE_DAYS", 45, min=1
)
# Clamp band for the calibrated base. Mirrors the catalog's [0.40, 0.85]
# pattern_base band so a single unlucky window can't zero out a pattern or a
# lucky one can't inflate it past the engine's design envelope.
SIGNALS_PATTERN_CALIBRATION_FLOOR = _getenv_float(
    "SIGNALS_PATTERN_CALIBRATION_FLOOR", 0.40, min=0.0, max=1.0
)
SIGNALS_PATTERN_CALIBRATION_CEIL = _getenv_float(
    "SIGNALS_PATTERN_CALIBRATION_CEIL", 0.85, min=0.0, max=1.0
)
# How often the long-running signals process reloads the calibration store
# from the stats table (seconds). Cheap no-op between reloads.
SIGNALS_PATTERN_CALIBRATION_REFRESH_SECONDS = _getenv_int(
    "SIGNALS_PATTERN_CALIBRATION_REFRESH_SECONDS", 21600, min=60  # 6h
)
# Number of days of history each nightly calibration backtest scans.
SIGNALS_PATTERN_CALIBRATION_LOOKBACK_DAYS = _getenv_int(
    "SIGNALS_PATTERN_CALIBRATION_LOOKBACK_DAYS", 60, min=5
)

# ---------------------------------------------------------------------------
# Backtesting platform — signal cooldown / dedup
#
# The live engine emits an Action Card nearly every cycle, so a naive backtest
# would "take" thousands of near-identical signals per day. The cooldown
# collapses that continuous stream into discrete entries: at most one new entry
# per (pattern) per this many minutes. 0 disables (price every card). This is
# both a realism fix (you don't re-enter the same setup every minute) and a
# performance fix (cards inside a cooldown are skipped BEFORE pricing).
# Per-request overridable via BacktestSpec.cooldown_minutes.
# ---------------------------------------------------------------------------
BACKTEST_SIGNAL_COOLDOWN_MINUTES = _getenv_int(
    "BACKTEST_SIGNAL_COOLDOWN_MINUTES", 30, min=0, max=1440
)
# Absolute cap on contracts per simulated trade — keeps a near-zero-debit
# spread (tiny per-contract risk) from being sized into an unrealistic position
# whose commission alone would dominate. Realistic retail-scale default.
BACKTEST_MAX_CONTRACTS_PER_TRADE = _getenv_int(
    "BACKTEST_MAX_CONTRACTS_PER_TRADE", 100, min=1
)

# Dedicated worker (Phase 4): when enabled, the API only ENQUEUES runs and a
# standalone `python -m src.backtesting.worker` process drains them, so long
# backtests don't tie up an API thread and survive API restarts. OFF by default
# ⇒ the API executes runs in-process via BackgroundTasks (single-host default).
# Enabling without a running worker leaves runs queued, so flip this on only
# together with the worker service.
BACKTEST_WORKER_ENABLED = _getenv_bool("BACKTEST_WORKER_ENABLED", False)
BACKTEST_WORKER_POLL_SECONDS = _getenv_float("BACKTEST_WORKER_POLL_SECONDS", 2.0, min=0.2)
# Runs stuck 'running' longer than this (e.g. orphaned by a crash) are requeued.
BACKTEST_WORKER_STALE_MINUTES = _getenv_int("BACKTEST_WORKER_STALE_MINUTES", 30, min=1)

# GEX normalization scale used to map net_gex into [-1, 1] for multiple
# signal components (vol_expansion, strategy_builder, position optimizer).
# Calibrated for the industry-standard "dollar gamma per 1% move" GEX
# convention (γ × OI × 100 × S² × 0.01); the prior 300M default was on the
# share-equivalent scale and is multiplied by ≈7 for SPY-magnitude
# underlyings.  Override via env var if your universe's typical GEX
# magnitude differs.
SIGNAL_GEX_NORMALIZATION = _getenv_float("SIGNAL_GEX_NORMALIZATION", 2_100_000_000.0, min=1.0)

# Saturation scale for the scale-invariant GEX readiness formula
# (see VolExpansionSignal._gex_readiness).  The signal computes
# ``ratio = net_gex / (S² × total_oi × 100 × 0.01)`` — a dimensionless
# balance measure that's symbol-agnostic by construction (the
# denominator already absorbs the scale differences between SPX, SPY,
# and QQQ).  This constant multiplies the raw ratio before the
# [-1, +1] clamp, replacing the old global ``SIGNAL_GEX_NORMALIZATION``
# as the single tuning knob.  Default 100 puts SPY's typical
# stable-regime ratio (~0.01) at the saturation boundary, mapping
# heavily long-gamma chains to the "Low" floor while leaving room for
# Medium / High classifications as net_gex moves toward zero / negative.
# Raise it to widen the dynamic range (more cycles read as Medium);
# lower it to compress (more cycles saturate at the extremes).
GEX_SCALE_INVARIANT_SATURATION = _getenv_float("GEX_SCALE_INVARIANT_SATURATION", 100.0, min=1.0)
POSITION_OPTIMIZER_VERBOSE_DIAGNOSTICS = _getenv_bool(
    "POSITION_OPTIMIZER_VERBOSE_DIAGNOSTICS", False
)

# Aggregate exposure limits — prevent the engine from piling into the same
# direction without regard for what is already on the books.
SIGNALS_MAX_OPEN_TRADES = _getenv_int("SIGNALS_MAX_OPEN_TRADES", 3)
SIGNALS_MAX_PORTFOLIO_HEAT_PCT = _getenv_float("SIGNALS_MAX_PORTFOLIO_HEAT_PCT", 0.06)
SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES = _getenv_int("SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES", 30)
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
SIGNALS_TREND_CONFIRMATION_BARS = max(0, _getenv_int("SIGNALS_TREND_CONFIRMATION_BARS", 3))
SIGNALS_TREND_CONFIRMATION_MIN_MATCH = max(
    0, _getenv_int("SIGNALS_TREND_CONFIRMATION_MIN_MATCH", 1)
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
# High-confidence Playbook pattern override.  When a card-driven entry
# arrives with confidence above the threshold, lift the regime size cap
# to the configured size — bypassing chop's blanket 0.4× scalp multiplier
# for structural setups like gamma_flip_bounce / put_wall_bounce in their
# preferred regimes.  Threshold default 0.65 matches the pattern_base of
# the highest-conviction structural patterns; tune lower to broaden the
# override, higher to restrict it.
SIGNALS_HIGH_CONFIDENCE_PATTERN_THRESHOLD = _getenv_float(
    "SIGNALS_HIGH_CONFIDENCE_PATTERN_THRESHOLD", 0.65, min=0.0, max=1.0
)
SIGNALS_HIGH_CONFIDENCE_PATTERN_SIZE = _getenv_float(
    "SIGNALS_HIGH_CONFIDENCE_PATTERN_SIZE", 1.0, min=0.0, max=1.0
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
SIGNALS_DRS_HARD_GATES_ENABLED = _getenv_bool("SIGNALS_DRS_HARD_GATES_ENABLED", True)
SIGNALS_DRS_CALL_ENTRY_MIN = _getenv_float("SIGNALS_DRS_CALL_ENTRY_MIN", 0.40)
SIGNALS_DRS_PUT_ENTRY_MAX = _getenv_float("SIGNALS_DRS_PUT_ENTRY_MAX", 0.20)

# -----------------------------------------------------------------------------
# Conviction aggregation -- fights dilution from abstaining components
# -----------------------------------------------------------------------------
# When enabled, the ScoringEngine renormalizes the composite against *only*
# the active (non-abstaining) components, then applies an agreement and an
# extremity amplifier so that 8 components all screaming the same direction
# isn't averaged down to 0.2 by 6 quiet ones.
SIGNALS_CONVICTION_AGGREGATION_ENABLED = _getenv_bool(
    "SIGNALS_CONVICTION_AGGREGATION_ENABLED", True
)
# Absolute-score cutoff below which a component is treated as abstaining
# (removed from the active-weight denominator). 0.02 keeps legitimately
# near-zero directional reads in the pool while dropping hard zeros.
SIGNALS_CONVICTION_ABSTAIN_EPSILON = _getenv_float("SIGNALS_CONVICTION_ABSTAIN_EPSILON", 0.02)
# Maximum multiplier applied when all active components agree in direction.
SIGNALS_CONVICTION_AGREEMENT_MAX_MULT = _getenv_float("SIGNALS_CONVICTION_AGREEMENT_MAX_MULT", 1.75)
# Extremity amplifier: extra boost when the loudest active component is
# screaming. Applied multiplicatively on top of agreement.
SIGNALS_CONVICTION_EXTREMITY_MAX_MULT = _getenv_float("SIGNALS_CONVICTION_EXTREMITY_MAX_MULT", 1.30)

# -----------------------------------------------------------------------------
# Scalp-tier trigger -- second, lower threshold for reduced-size trades
# -----------------------------------------------------------------------------
# When the composite clears SIGNALS_SCALP_TRIGGER_THRESHOLD but not the main
# SIGNALS_TRIGGER_THRESHOLD, the engine opens a smaller ("scalp") position.
# This captures the "split-second technical opportunities" use case without
# requiring conviction-trade strength.
SIGNALS_SCALP_TRIGGER_ENABLED = _getenv_bool("SIGNALS_SCALP_TRIGGER_ENABLED", True)
SIGNALS_SCALP_TRIGGER_THRESHOLD = _getenv_float("SIGNALS_SCALP_TRIGGER_THRESHOLD", 0.36)
# Fraction of normal Kelly-based contracts used for scalp-tier trades.
SIGNALS_SCALP_SIZE_MULTIPLIER = _getenv_float("SIGNALS_SCALP_SIZE_MULTIPLIER", 0.40)

# -----------------------------------------------------------------------------
# Strong-conviction DRS override -- lets high-conviction reversals through
# even when the DRS hard gates would block them (e.g. bearish entry on a day
# already below the gamma flip, where the "fresh cross" rule fires once).
# -----------------------------------------------------------------------------
SIGNALS_DRS_OVERRIDE_ENABLED = _getenv_bool("SIGNALS_DRS_OVERRIDE_ENABLED", True)
SIGNALS_DRS_OVERRIDE_THRESHOLD = _getenv_float("SIGNALS_DRS_OVERRIDE_THRESHOLD", 0.70)

# Conviction uplift for a fresh gamma-flip cross in the signaled direction.
# Previously "fresh cross" was a hard bearish-entry requirement; symmetrizing
# the DRS gates moved it to an additive sizing boost applied after the gate
# passes. 0.0 disables the boost.
SIGNALS_DRS_FRESH_CROSS_BOOST = max(0.0, _getenv_float("SIGNALS_DRS_FRESH_CROSS_BOOST", 0.20))

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
    # Strip an inline ``# comment`` tail before JSON parsing so a
    # ``{...}  # note`` .env line parses instead of silently falling back.
    raw = _strip_env_value(os.getenv(name)) or ""
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
    # Strip an inline ``# comment`` tail before JSON parsing so a
    # ``{...}  # note`` .env line parses instead of silently falling back.
    raw = _strip_env_value(os.getenv(name)) or ""
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
    15, _getenv_int("SIGNALS_INDEPENDENT_PHASE_SCALP_MINUTES_FROM_OPEN", 75)
)
SIGNALS_INDEPENDENT_PHASE_SWING_MINUTES_TO_CLOSE = max(
    15, _getenv_int("SIGNALS_INDEPENDENT_PHASE_SWING_MINUTES_TO_CLOSE", 90)
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
    0.0, min(1.0, _getenv_float("SIGNALS_INDEPENDENT_THRESHOLD_SCALP", 0.38))
)
SIGNALS_INDEPENDENT_THRESHOLD_INTRADAY = max(
    0.0, min(1.0, _getenv_float("SIGNALS_INDEPENDENT_THRESHOLD_INTRADAY", 0.30))
)
SIGNALS_INDEPENDENT_THRESHOLD_SWING = max(
    0.0, min(1.0, _getenv_float("SIGNALS_INDEPENDENT_THRESHOLD_SWING", 0.34))
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
        _getenv_float("SIGNALS_INDEPENDENT_RISK_MULT_CONSERVATIVE", 1.15),
    ),
)
SIGNALS_INDEPENDENT_RISK_MULT_BALANCED = max(
    0.5,
    min(2.0, _getenv_float("SIGNALS_INDEPENDENT_RISK_MULT_BALANCED", 1.00)),
)
SIGNALS_INDEPENDENT_RISK_MULT_AGGRESSIVE = max(
    0.5,
    min(2.0, _getenv_float("SIGNALS_INDEPENDENT_RISK_MULT_AGGRESSIVE", 0.90)),
)

# Hard floors to avoid over-loose thresholds even in aggressive profiles.
SIGNALS_INDEPENDENT_MIN_THRESHOLD_SQUEEZE_SETUP = max(
    0.0,
    min(1.0, _getenv_float("SIGNALS_INDEPENDENT_MIN_THRESHOLD_SQUEEZE_SETUP", 0.25)),
)
SIGNALS_INDEPENDENT_MIN_THRESHOLD_TRAP_DETECTION = max(
    0.0,
    min(1.0, _getenv_float("SIGNALS_INDEPENDENT_MIN_THRESHOLD_TRAP_DETECTION", 0.25)),
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
        _getenv_float("SIGNALS_INDEPENDENT_MIN_THRESHOLD_GAMMA_VWAP_CONFLUENCE", 0.20),
    ),
)
SIGNALS_INDEPENDENT_MIN_THRESHOLD_VOL_EXPANSION = max(
    0.0,
    min(1.0, _getenv_float("SIGNALS_INDEPENDENT_MIN_THRESHOLD_VOL_EXPANSION", 0.25)),
)
SIGNALS_INDEPENDENT_MIN_THRESHOLD_EOD_PRESSURE = max(
    0.0,
    min(1.0, _getenv_float("SIGNALS_INDEPENDENT_MIN_THRESHOLD_EOD_PRESSURE", 0.20)),
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
SIGNALS_CONTRARIAN_OVERRIDE_ENABLED = _getenv_bool("SIGNALS_CONTRARIAN_OVERRIDE_ENABLED", True)
SIGNALS_CONTRARIAN_REWEIGHT_ENABLED = _getenv_bool("SIGNALS_CONTRARIAN_REWEIGHT_ENABLED", True)
SIGNALS_CONTRARIAN_REWEIGHT_MULT = max(1.0, _getenv_float("SIGNALS_CONTRARIAN_REWEIGHT_MULT", 1.45))
SIGNALS_CONTRARIAN_OVERRIDE_THRESHOLD = max(
    0.0, min(1.0, _getenv_float("SIGNALS_CONTRARIAN_OVERRIDE_THRESHOLD", 0.60))
)
# Minimum composite magnitude before the override can fire. Prevents flipping
# near-zero composites where the trend signal isn't really pointing anywhere.
SIGNALS_CONTRARIAN_OVERRIDE_MIN_COMPOSITE = max(
    0.0, _getenv_float("SIGNALS_CONTRARIAN_OVERRIDE_MIN_COMPOSITE", 0.20)
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
SIGNALS_STOP_LOSS_PCT = _getenv_float("SIGNALS_STOP_LOSS_PCT", -0.25)

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
SIGNALS_EXECUTION_SLIPPAGE_PCT = max(0.0, _getenv_float("SIGNALS_EXECUTION_SLIPPAGE_PCT", 0.0))

# Reject option_chains quote rows older than this when marking an open trade
# for stop/take-profit evaluation. Without this floor a quote-ingest stall
# (or the first cycle after a weekend / holiday) would price the position
# against a multi-day-stale mark and could fire a fictitious stop fill.
# Set to 0 to disable the check (NOT recommended in live trading).
SIGNALS_OPTION_QUOTE_MAX_AGE_SECONDS = _getenv_int(
    "SIGNALS_OPTION_QUOTE_MAX_AGE_SECONDS", 900, min=0, max=86400
)

# =============================================================================
# Ingestion/Analytics CLI Defaults
# =============================================================================

INGEST_UNDERLYING = _getenv_str("INGEST_UNDERLYING", "SPY")
INGEST_UNDERLYINGS = _getenv_str("INGEST_UNDERLYINGS", "")
INGEST_EXPIRATIONS = _getenv_int("INGEST_EXPIRATIONS", 3)
# Additional expirations pulled from the monthly chain mapped via
# INGEST_MONTHLY_UNDERLYING_ALIASES. These layer ON TOP OF the primary
# (weekly) window selected by INGEST_EXPIRATIONS — useful for index
# underlyings where the AM-settled monthlies live under a separate TS
# chain root (e.g. SPX vs SPXW) and would otherwise never be ingested.
# Default 0 keeps prior behavior; per-process knob — each ingestion
# worker fetches its own monthly window from its mapped chain symbol.
INGEST_MONTHLY_EXPIRATIONS = _getenv_int("INGEST_MONTHLY_EXPIRATIONS", 0)
INGEST_STRIKE_COUNT_MAX = _getenv_int("INGEST_STRIKE_COUNT_MAX", 40)
INGEST_STRIKE_PCT_RANGE = _getenv_float("INGEST_STRIKE_PCT_RANGE", 3.0)
ANALYTICS_UNDERLYING = _getenv_str("ANALYTICS_UNDERLYING", "SPY")
ANALYTICS_UNDERLYINGS = _getenv_str("ANALYTICS_UNDERLYINGS", "")
ANALYTICS_INTERVAL = _getenv_int("ANALYTICS_INTERVAL", 60)
ANALYTICS_SNAPSHOT_LOOKBACK_MINUTES = max(1, _getenv_int("ANALYTICS_SNAPSHOT_LOOKBACK_MINUTES", 5))
ANALYTICS_SNAPSHOT_FRESHNESS_SECONDS = max(
    30, _getenv_int("ANALYTICS_SNAPSHOT_FRESHNESS_SECONDS", 180)
)
ANALYTICS_MIN_OI_COVERAGE_PCT_ALERT = _getenv_float("ANALYTICS_MIN_OI_COVERAGE_PCT_ALERT", 0.35)

# TradeStation credential variables (used by service startup and helper scripts).
TRADESTATION_CLIENT_ID = os.getenv("TRADESTATION_CLIENT_ID")
TRADESTATION_CLIENT_SECRET = os.getenv("TRADESTATION_CLIENT_SECRET")
TRADESTATION_REFRESH_TOKEN = os.getenv("TRADESTATION_REFRESH_TOKEN")
TRADESTATION_USE_SANDBOX = _getenv_bool("TRADESTATION_USE_SANDBOX", False)

# TradeStation CLI test defaults.
TS_TEST = os.getenv("TS_TEST", "all")
TS_SYMBOL = os.getenv("TS_SYMBOL", "SPY")
TS_BARS_BACK = _getenv_int("TS_BARS_BACK", 5)
TS_INTERVAL = _getenv_int("TS_INTERVAL", 1)
TS_UNIT = os.getenv("TS_UNIT", "Daily")
TS_QUERY = os.getenv("TS_QUERY", "Apple")

# Calendar overrides.
NYSE_HOLIDAYS = os.getenv("NYSE_HOLIDAYS", "")

# =============================================================================
# Ingestion Parity Guard
# =============================================================================

# Emits deterministic payload signatures before DB writes so stream-vs-rest
# ingestion parity can be validated in production without schema changes.
INGEST_PARITY_GUARD_ENABLED = _getenv_bool("INGEST_PARITY_GUARD_ENABLED", False)

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
            "gex_heatmap_strike_band_pct": GEX_HEATMAP_STRIKE_BAND_PCT,
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
            "quotes_max_symbols_per_connection": STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION,
            "option_oi_coverage_alert_threshold": OPTION_OI_COVERAGE_ALERT_THRESHOLD,
            "option_volume_coverage_alert_threshold": OPTION_VOLUME_COVERAGE_ALERT_THRESHOLD,
            "option_volume_warmup_minutes": OPTION_VOLUME_WARMUP_MINUTES,
            "option_oi_warmup_minutes": OPTION_OI_WARMUP_MINUTES,
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
            "independent_phase_scalp_minutes_from_open": (
                SIGNALS_INDEPENDENT_PHASE_SCALP_MINUTES_FROM_OPEN
            ),
            "independent_phase_swing_minutes_to_close": (
                SIGNALS_INDEPENDENT_PHASE_SWING_MINUTES_TO_CLOSE
            ),
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
                "zero_dte_position_imbalance": (
                    SIGNALS_INDEPENDENT_RISK_PROFILE_ZERO_DTE_POSITION_IMBALANCE
                ),
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
