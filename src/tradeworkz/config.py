"""TradeWorkz environment configuration.

Every ``TRADEWORKZ_*`` env var read by the engine, bots, calibrator, or API
router lives here. Import from this module; do not read ``os.getenv``
directly elsewhere in the package.
"""

from __future__ import annotations

from src.config import _getenv_bool, _getenv_float, _getenv_int, _getenv_str

# ---------------------------------------------------------------------------
# Fleet capital
# ---------------------------------------------------------------------------
# Sliced evenly across every enabled bot at provisioning time. Admins can
# subsequently override individual sleeves in ``tw_bot_capital``; the fleet
# figure here is only the initial allocation baseline.
FLEET_CAPITAL: float = _getenv_float("TRADEWORKZ_FLEET_CAPITAL", 1_000_000.0, min=1.0)

# ---------------------------------------------------------------------------
# Engine loop
# ---------------------------------------------------------------------------
ENGINE_ENABLED: bool = _getenv_bool("TRADEWORKZ_ENGINE_ENABLED", True)
ENGINE_INTERVAL_SECONDS: int = _getenv_int("TRADEWORKZ_ENGINE_INTERVAL_SECONDS", 5, min=1, max=3600)
# Comma-separated list of underlyings the fleet trades against. A bot
# whose ``universe`` column is the wildcard ``"*"`` runs against every
# ticker in this list every tick (one signal per (bot, underlying) pair,
# each with its own snapshot). Legacy per-bot single-ticker rows are
# still supported — a bot pinned to ``universe='SPY'`` will only see the
# SPY snapshot even if the fleet universe is wider.
UNIVERSE: str = _getenv_str("TRADEWORKZ_UNIVERSE", "SPY,QQQ,IWM")


def fleet_universes() -> tuple[str, ...]:
    """Parse ``TRADEWORKZ_UNIVERSE`` into a tuple of upper-cased tickers."""
    return tuple(sym.strip().upper() for sym in UNIVERSE.split(",") if sym.strip())


RECONCILE_LOCK_ENABLED: bool = _getenv_bool("TRADEWORKZ_RECONCILE_LOCK_ENABLED", True)

# ---------------------------------------------------------------------------
# Per-bot risk defaults
# ---------------------------------------------------------------------------
DEFAULT_MAX_HEAT_PCT: float = _getenv_float(
    "TRADEWORKZ_DEFAULT_MAX_HEAT_PCT", 0.06, min=0.0, max=1.0
)
DEFAULT_KELLY_FRACTION: float = _getenv_float(
    "TRADEWORKZ_DEFAULT_KELLY_FRACTION", 0.50, min=0.0, max=1.0
)
DEFAULT_DAILY_KILL_PCT: float = _getenv_float(
    "TRADEWORKZ_DEFAULT_DAILY_KILL_PCT", 0.02, min=0.0, max=1.0
)

# ---------------------------------------------------------------------------
# Position lifecycle
# ---------------------------------------------------------------------------
MIN_HOLD_SECONDS: int = _getenv_int("TRADEWORKZ_MIN_HOLD_SECONDS", 90, min=0, max=86400)
OPTION_QUOTE_MAX_AGE_SECONDS: int = _getenv_int(
    "TRADEWORKZ_OPTION_QUOTE_MAX_AGE_SECONDS", 900, min=1, max=86400
)
ENTRY_DEDUPE_WINDOW_SECONDS: int = _getenv_int(
    "TRADEWORKZ_ENTRY_DEDUPE_WINDOW_SECONDS", 60, min=0, max=3600
)
EXECUTION_SLIPPAGE_PCT: float = _getenv_float(
    "TRADEWORKZ_EXECUTION_SLIPPAGE_PCT", 0.02, min=0.0, max=1.0
)
# Premium-loss damage-control stop. Bots set structural spot-level
# stops that invalidate the thesis, but on 0DTE ATM debits a 0.3-1%
# adverse spot move can eat 60-80% of premium before spot reaches
# that structural level. This is a hard second stop keyed off option
# premium: if the mark drops below (1 - MAX_PREMIUM_LOSS_PCT) × entry,
# the reconciler closes the position on the next tick with
# reason='premium_stop'. Set to 0 to disable. Per-bot override via
# params['max_premium_loss_pct'] takes precedence over this default.
# Reverted 0.25 -> 0.40 after live data showed the tighter cap stopping cheap
# 0DTE structures on the entry-tick bid/ask + slippage gap alone: a $0.62 debit
# spread marks ~$0.42 the instant it opens (~-32%), tripping a 25% stop at 0.0
# min before any thesis can play out. 0.40 clears that microstructure noise; the
# proper fix (min-premium floor + a premium-stop grace window) is tracked
# separately so the stop protects against real adverse moves, not the spread.
MAX_PREMIUM_LOSS_PCT: float = _getenv_float(
    "TRADEWORKZ_MAX_PREMIUM_LOSS_PCT", 0.40, min=0.0, max=1.0
)
# Grace window (seconds after open) during which the premium stop does NOT
# fire. A freshly opened position is first marked at the CLOSE side (bid −
# slippage), so on a wide-market 0DTE structure the very first mark can show a
# large phantom loss from the bid/ask + slippage round-trip alone (a $0.62
# spread marking ~$0.42, ~-32%) — not a real adverse move. This lets that
# entry-tick gap settle before the premium stop can act; a genuine adverse move
# persists past the window and still stops. Set 0 to disable. Kept well under
# MIN_HOLD_SECONDS so it never delays a real risk exit by much.
PREMIUM_STOP_GRACE_SECONDS: int = _getenv_int(
    "TRADEWORKZ_PREMIUM_STOP_GRACE_SECONDS", 45, min=0, max=3600
)
# Minimum per-share entry premium to open a position. Below this, the bid/ask
# width tends to dominate the premium and every trade round-trips at a loss.
# A coarse proxy for "spread too wide relative to premium." Default 0 = no
# floor (unchanged); raise it (e.g. 0.30) per fleet or per-bot after reviewing
# tradeworkz-review, since it will filter otherwise-valid cheap 0DTE structures.
MIN_ENTRY_PREMIUM: float = _getenv_float("TRADEWORKZ_MIN_ENTRY_PREMIUM", 0.0, min=0.0, max=1000.0)

# ---------------------------------------------------------------------------
# Scale-out ladder (profit-harvesting on positions in profit)
# ---------------------------------------------------------------------------
# See docs/design/tradeworkz-exit-strategy.md. When a directional position
# goes into profit, harvest it in tranches instead of a single all-or-nothing
# target: take T1_TAKE_FRACTION at T1 (entry + T1_TRIGGER_FRACTION*R), then
# T2_TAKE_FRACTION of the remainder at T2 (entry + T2_TARGET_FRACTION*R), and
# ride the final piece under an S2 floor (entry + S2_STOP_FRACTION*R) plus a
# premium give-back trail (RUNNER_TRAIL_GIVEBACK_PCT off the runner's mark
# high-water mark). R = target_price - entry_spot. Every knob is per-bot
# overridable via params[...] (same pattern as max_premium_loss_pct). Set
# SCALE_OUT_ENABLED=false (or size below MIN_SCALE_CONTRACTS, or a
# non-directional / target-less structure) to fall back to the single-target
# exit. The existing stop stack (premium stop, structural stop, wall-break,
# time-stop) stays in force throughout — the ladder only governs profit-taking.
SCALE_OUT_ENABLED: bool = _getenv_bool("TRADEWORKZ_SCALE_OUT_ENABLED", True)
# Below this initial contract count, don't scale (splitting tiny positions into
# 1-contract tranches just triples per-fill slippage for no diversification).
MIN_SCALE_CONTRACTS: int = _getenv_int("TRADEWORKZ_MIN_SCALE_CONTRACTS", 4, min=1, max=100_000)
# T1 take-profit trigger, as a fraction of R. 0.90 fires just BEFORE the
# structural target — walls/max-pain/VWAP act as resistance and price often
# reverses on the last tick, so taking at 0.90*R fills far more often.
T1_TRIGGER_FRACTION: float = _getenv_float("TRADEWORKZ_T1_TRIGGER_FRACTION", 0.90, min=0.0, max=1.0)
# S2 runner floor, as a fraction of R. 0.75 sits below T1 to give the runner
# wiggle room while still locking a small gain (S2 is above entry).
S2_STOP_FRACTION: float = _getenv_float("TRADEWORKZ_S2_STOP_FRACTION", 0.75, min=0.0, max=1.0)
# T2 second take, as a fraction of R. 1.5 is half an R past the structural
# target — a realistic 0DTE stretch that still rewards the runner.
T2_TARGET_FRACTION: float = _getenv_float("TRADEWORKZ_T2_TARGET_FRACTION", 1.5, min=0.0, max=10.0)
# Fraction of the ORIGINAL size taken at T1.
T1_TAKE_FRACTION: float = _getenv_float("TRADEWORKZ_T1_TAKE_FRACTION", 0.5, min=0.0, max=1.0)
# Fraction of the REMAINDER taken at T2.
T2_TAKE_FRACTION: float = _getenv_float("TRADEWORKZ_T2_TAKE_FRACTION", 0.5, min=0.0, max=1.0)
# Premium give-back off the runner's high-water mark that trails it out. On
# 0DTE the runner's enemy is theta; a premium (not spot) trail captures that.
RUNNER_TRAIL_GIVEBACK_PCT: float = _getenv_float(
    "TRADEWORKZ_RUNNER_TRAIL_GIVEBACK_PCT", 0.30, min=0.0, max=1.0
)
# Cap the ladder's effective target to a reachable envelope: the geometry uses
# min(structural_target, entry*(1+this)) for bullish (mirror for bearish). Some
# bots target a far gamma wall a 0DTE won't reach intraday, which would anchor
# T1 out of reach and leave the ladder inert on a real premium winner. 0.8%
# keeps T1/S2/T2 reachable; a target already inside the envelope is unchanged.
# Set to 0 to disable the cap (use the raw structural target). Tuned for 0DTE —
# 1DTE/swing bots that legitimately target a multi-day move should raise this
# via params['ladder_max_move_pct'].
LADDER_MAX_MOVE_PCT: float = _getenv_float(
    "TRADEWORKZ_LADDER_MAX_MOVE_PCT", 0.008, min=0.0, max=1.0
)

# ---------------------------------------------------------------------------
# Reversion trend filter (don't fade a strong directional tape)
# ---------------------------------------------------------------------------
# Mean-reversion bots (max_pain / wall-bouncer / VWAP scalper) fade extension —
# they go short when price is stretched above a level and long when below.
# That is the WRONG side of a strongly trending day: on a gamma-positive but
# up-TRENDING session the fleet went net-short and got run over (puts -50% to
# -70%). This filter vetoes a fade whose direction opposes a strong recent move
# in snap.recent_closes (1-minute closes): a bearish fade is blocked when the
# last TREND_VETO_LOOKBACK_BARS closes are up >= TREND_VETO_PCT, a bullish fade
# when they are down that much. Momentum/breakout bots do NOT apply it (they
# trade WITH the trend). Per-bot overridable; set TREND_VETO_PCT=0 to disable.
# NOTE: defaults are a reasonable first cut — tune against tradeworkz-review.
TREND_VETO_PCT: float = _getenv_float("TRADEWORKZ_TREND_VETO_PCT", 0.002, min=0.0, max=1.0)
TREND_VETO_LOOKBACK_BARS: int = _getenv_int(
    "TRADEWORKZ_TREND_VETO_LOOKBACK_BARS", 10, min=2, max=390
)

# ---------------------------------------------------------------------------
# Regular-trading-hours (RTH) gate on NEW opens
# ---------------------------------------------------------------------------
# Every fleet bot trades same-day (0DTE) debits, and the reconciler
# hard-caps a 0DTE's time_stop at 15:55 ET (reconciler._EXPIRATION_CLOSE_HHMM).
# A position opened OUTSIDE the cash session therefore has a time_stop that is
# already in the past, so it dies on the very next tick for a spread/slippage
# loss — the after-hours churn that bled the fleet on 2026-07-16 (bots gate on
# ``minutes_since_open >= 30``, a floor with no ceiling, so at 20:00 ET
# minutes_since_open=630 sailed through and they opened 0DTEs that instantly
# time-stopped). When RTH_ONLY_OPENS is on, the engine refuses to open a new
# position outside the ET cash session (weekends, NYSE holidays, pre-open,
# after-hours) OR at/after RTH_NO_NEW_OPENS_AFTER_ET, so a position can never
# be opened already past its own hard time_stop. Marks and EXITS on existing
# positions are never gated — only new opens. Set RTH_ONLY_OPENS=false to
# disable (e.g. a future extended-hours strategy). Keep
# RTH_NO_NEW_OPENS_AFTER_ET aligned with the reconciler's 0DTE time_stop cap
# (15:55 ET) — an "HH:MM" ET wall-clock string; a malformed value falls back
# to 15:55.
RTH_ONLY_OPENS: bool = _getenv_bool("TRADEWORKZ_RTH_ONLY_OPENS", True)
RTH_NO_NEW_OPENS_AFTER_ET: str = _getenv_str("TRADEWORKZ_RTH_NO_NEW_OPENS_AFTER_ET", "15:55")

# ---------------------------------------------------------------------------
# ML calibrator
# ---------------------------------------------------------------------------
CALIBRATION_ENABLED: bool = _getenv_bool("TRADEWORKZ_CALIBRATION_ENABLED", True)
CALIBRATION_LOOKBACK_DAYS: int = _getenv_int(
    "TRADEWORKZ_CALIBRATION_LOOKBACK_DAYS", 60, min=1, max=730
)
CALIBRATION_MIN_SAMPLES: int = _getenv_int(
    "TRADEWORKZ_CALIBRATION_MIN_SAMPLES", 20, min=1, max=100_000
)
CALIBRATION_FLOOR: float = _getenv_float("TRADEWORKZ_CALIBRATION_FLOOR", 0.40, min=0.0, max=1.0)
CALIBRATION_CEIL: float = _getenv_float("TRADEWORKZ_CALIBRATION_CEIL", 0.85, min=0.0, max=1.0)
CALIBRATION_LEARNING_RATE: float = _getenv_float(
    "TRADEWORKZ_CALIBRATION_LEARNING_RATE", 0.05, min=1e-6, max=1.0
)
# Cadence of the in-process calibration sweep (the "nightly worker"): the
# scheduler recalibrates every enabled bot this often, in addition to the
# defensive recompute on each trade close. Runs once on scheduler start so a
# fresh deploy calibrates immediately. 0 disables the periodic sweep (the
# per-close path still runs).
CALIBRATION_SWEEP_INTERVAL_HOURS: float = _getenv_float(
    "TRADEWORKZ_CALIBRATION_SWEEP_INTERVAL_HOURS", 24.0, min=0.0, max=8760.0
)

# Adaptive entry-conviction gate. recalibrate_bot sets each bot's
# confidence_threshold to clamp(1 - win_30d + OFFSET, FLOOR, CEIL) — a bot
# whose recent win rate sags gets a HIGHER bar to enter. CEIL is the critical
# knob: a bot's conviction (base.compute_conviction) tops out around 0.76 even
# on a flawless setup, so a CEIL at/above that silently locks the WHOLE fleet
# out of trading — every signal scores below the gate and nothing opens. It
# must stay below the reachable conviction ceiling so a clean setup can still
# clear it. The default 0.60 lets high-quality (quality >= ~0.73) setups
# through while still filtering weak ones — "more rigor," not "no trades."
# (An earlier hard-coded 0.75 ceiling is exactly what froze the fleet after a
# losing streak dragged every bot's win_30d down to the cap.)
ADAPTIVE_THRESHOLD_FLOOR: float = _getenv_float(
    "TRADEWORKZ_ADAPTIVE_THRESHOLD_FLOOR", 0.45, min=0.0, max=1.0
)
ADAPTIVE_THRESHOLD_CEIL: float = _getenv_float(
    "TRADEWORKZ_ADAPTIVE_THRESHOLD_CEIL", 0.60, min=0.0, max=1.0
)
ADAPTIVE_THRESHOLD_OFFSET: float = _getenv_float(
    "TRADEWORKZ_ADAPTIVE_THRESHOLD_OFFSET", 0.30, min=-1.0, max=1.0
)

# ---------------------------------------------------------------------------
# Bot governance: auto-disable a persistently losing bot
# ---------------------------------------------------------------------------
# Calibration flips ``tw_bots.enabled = false`` for any bot with at least
# AUTO_DISABLE_MIN_TRADES closed trades in the calibration lookback whose hit
# rate is below AUTO_DISABLE_MAX_HIT_RATE — a circuit breaker so a broken bot
# stops bleeding at full size instead of only being throttled to 0.5x. This is
# NOT permanent retirement: remove a bot from the roster (registry.py) to
# retire it for good. provision_defaults no longer force-re-enables on
# restart, so an auto-disable sticks until an operator re-enables it.
AUTO_DISABLE_ENABLED: bool = _getenv_bool("TRADEWORKZ_AUTO_DISABLE_ENABLED", True)
AUTO_DISABLE_MIN_TRADES: int = _getenv_int(
    "TRADEWORKZ_AUTO_DISABLE_MIN_TRADES", 20, min=1, max=100_000
)
AUTO_DISABLE_MAX_HIT_RATE: float = _getenv_float(
    "TRADEWORKZ_AUTO_DISABLE_MAX_HIT_RATE", 0.35, min=0.0, max=1.0
)

# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------
NOTIFY_ENABLED: bool = _getenv_bool("TRADEWORKZ_NOTIFY_ENABLED", True)
NOTIFY_CHANNELS_DEFAULT: str = _getenv_str("TRADEWORKZ_NOTIFY_CHANNELS_DEFAULT", "in_app")
NOTIFY_MIN_CONFIDENCE: float = _getenv_float(
    "TRADEWORKZ_NOTIFY_MIN_CONFIDENCE", 0.0, min=0.0, max=1.0
)
NOTIFY_COOLDOWN_SECONDS: int = _getenv_int(
    "TRADEWORKZ_NOTIFY_COOLDOWN_SECONDS", 300, min=0, max=86400
)
# Dust filter for the EMAIL channel only. Exit-event emails whose
# ``|payload.realized_pnl| < EMAIL_DUST_THRESHOLD`` are suppressed on
# the email channel — the in_app row still writes so the bell / feed
# stays complete. Risk-off exits (``reason IN ('stop', 'wall_break')``)
# never dust-suppress even at $0.01 — the operator wants to know the
# stop fired even if the size was tiny. Set to 0 to disable the filter.
EMAIL_DUST_THRESHOLD: float = _getenv_float(
    "TRADEWORKZ_EMAIL_DUST_THRESHOLD", 10.0, min=0.0, max=10_000.0
)

# ---------------------------------------------------------------------------
# Admin scope for the FastAPI dependency guard on the admin sub-router.
# ---------------------------------------------------------------------------
ADMIN_SCOPE_NAME: str = _getenv_str("TRADEWORKZ_ADMIN_SCOPE_NAME", "tradeworkz:admin")
