"""Wall-alert environment configuration.

Every ``WALL_ALERTS_*`` env var read by the detector service or the API
router lives here. Import from this module; do not read ``os.getenv``
directly elsewhere in the package.
"""

from __future__ import annotations

from typing import Dict, Tuple

from src.config import _getenv_bool, _getenv_float, _getenv_int, _getenv_str

# ---------------------------------------------------------------------------
# Service loop
# ---------------------------------------------------------------------------
# Kill switch for the detector service. Off: the service stays up (so systemd
# does not flap it) but evaluates nothing and writes nothing. Member rules and
# the API are unaffected, so turning it back on resumes alerting with no
# migration.
ENABLED: bool = _getenv_bool("WALL_ALERTS_ENABLED", True)

# How often the service re-reads the tape and the walls. The in-progress
# minute bar is upserted continuously by ingestion, so a hit is seen within
# about this many seconds of the print that made it.
POLL_SECONDS: float = _getenv_float("WALL_ALERTS_POLL_SECONDS", 5.0, min=1.0, max=60.0)

# The symbols alerts can be set on. Each needs its own option chain (walls in
# gex_summary) and its own tape (underlying_quotes). ES / NQ are deliberately
# absent: their walls are the SPX / NDX walls carried onto the futures axis at
# fair value, so an ES "hit" would be a statement about the carry model as much
# as about the wall. Members trading futures set the alert on the index.
SYMBOLS_RAW: str = _getenv_str("WALL_ALERTS_SYMBOLS", "SPY,QQQ,SPX,NDX")


def symbols() -> Tuple[str, ...]:
    """``WALL_ALERTS_SYMBOLS`` as an ordered tuple of upper-cased tickers."""
    seen: Dict[str, None] = {}
    for raw in SYMBOLS_RAW.split(","):
        sym = raw.strip().upper()
        if sym:
            seen.setdefault(sym, None)
    return tuple(seen)


# Freshness guards. An alert computed from a stalled feed is worse than no
# alert: a tape that stopped updating ten minutes ago can still "touch" a wall
# that has since moved. Past these ages the symbol is skipped for the tick.
MAX_BAR_AGE_SECONDS: int = _getenv_int("WALL_ALERTS_MAX_BAR_AGE_SECONDS", 180, min=30, max=3600)
MAX_WALLS_AGE_SECONDS: int = _getenv_int("WALL_ALERTS_MAX_WALLS_AGE_SECONDS", 600, min=60, max=7200)

# An event found on a bar older than this is applied to the detector's state
# but not delivered. It happens when the service catches up after a restart
# or a feed gap, and a "the wall was hit" message about something eight
# minutes ago is noise, not an alert.
MAX_EVENT_AGE_SECONDS: int = _getenv_int("WALL_ALERTS_MAX_EVENT_AGE_SECONDS", 150, min=30, max=3600)

# How far back the service replays the tape when it has no saved state for
# today (first start of the day, or a lost state row): long enough to rebuild
# arming and any open test, short enough to stay cheap.
WARMUP_MINUTES: int = _getenv_int("WALL_ALERTS_WARMUP_MINUTES", 90, min=0, max=390)

# Touch tolerances a member may choose instead of their noise filter's own
# touch band, in basis points of the wall (1 = 0.01%). Each one runs as its own
# detector stream beside the profiles' defaults (see wall_detector.stream_
# profiles), so the list is also what the service computes every tick: keep it
# short. The website reads it from /api/alerts/walls/config.
TOUCH_OPTIONS_BP: Tuple[int, ...] = (1, 5, 10, 25)

# ---------------------------------------------------------------------------
# Member limits
# ---------------------------------------------------------------------------
MAX_RULES_PER_USER: int = _getenv_int("WALL_ALERTS_MAX_RULES_PER_USER", 25, min=1, max=500)

# Hard ceiling on alerts one member receives in a session, across all of
# their rules. The detector's own filters keep a normal day far below it; the
# cap exists for the abnormal one (a feed glitch, a pathological tape), so a
# member is never flooded no matter what goes wrong upstream.
DAILY_CAP_PER_USER: int = _getenv_int("WALL_ALERTS_DAILY_CAP_PER_USER", 60, min=1, max=10000)

# ---------------------------------------------------------------------------
# Delivery channels
# ---------------------------------------------------------------------------
# Channel -> how a delivery row starts life.
#   instant: written as 'sent' at fan-out (in-app: the feed IS the delivery).
#   queued:  written as 'queued' for an out-of-process worker to drain through
#            /api/alerts/walls/internal/queued, then marked via .../mark.
# Adding a channel is one line here plus a worker; nothing in the detector or
# the fan-out changes. SMS is registered now so rules, the outbox and the
# delivery endpoints already speak it — see SMS_ENABLED.
CHANNELS: Dict[str, str] = {
    "in_app": "instant",
    "email": "queued",
    "sms": "queued",
}

# Members may only switch on a channel that something actually delivers. SMS
# stays refused by the API until a phone-verification flow and an SMS worker
# exist (docs/design/wall-alerts.md, "Wiring SMS"); flipping this on is the
# last step of that rollout, never the first.
SMS_ENABLED: bool = _getenv_bool("WALL_ALERTS_SMS_ENABLED", False)

# How long fired events (and, by cascade, their delivery rows) are kept.
RETENTION_DAYS: int = _getenv_int("WALL_ALERTS_RETENTION_DAYS", 180, min=7, max=3650)

# ---------------------------------------------------------------------------
# Context attached to each alert
# ---------------------------------------------------------------------------
# Horizons (minutes) of the published wall_break_stats curve quoted on an
# alert, when that row is reportable. Kept to the horizons the nightly
# refresh writes; a missing or unreportable row is simply left off.
ODDS_HORIZONS: Tuple[int, ...] = (30, 60)
