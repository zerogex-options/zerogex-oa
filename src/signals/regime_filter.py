"""Time-of-day and scheduled-event filters for signal entries.

Suppresses *new* entries (no opposing position yet) during low-edge
windows: lunch chop, the final minutes before close, and a configurable
buffer around scheduled macro events (FOMC, CPI, NFP, etc.).

The filter never forces closes — same-direction holds and stop-driven exits
are unaffected.  Strong-conviction MSI snapshots can override the lunch
filter so we don't miss true trend-day moves that happen to occur midday.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Iterable, Optional

import pytz

from src.config import (
    SIGNALS_EVENT_BUFFER_MINUTES,
    SIGNALS_EVENT_CALENDAR,
    SIGNALS_LATE_CLOSE_LOCKDOWN_MINUTES,
    SIGNALS_LUNCH_BYPASS_SOURCES,
    SIGNALS_LUNCH_END_ET,
    SIGNALS_LUNCH_MSI_OVERRIDE,
    SIGNALS_LUNCH_START_ET,
    SIGNALS_TIME_FILTER_ENABLED,
)

ET = pytz.timezone("US/Eastern")
_MARKET_CLOSE = time(16, 0)


@dataclass(frozen=True)
class FilterDecision:
    """Outcome of regime-filter evaluation.

    ``skip`` is True when the filter wants to suppress a new entry.
    ``reason`` is a short human-readable explanation included in the
    portfolio rationale so an operator can see why a window was blocked.
    ``allow_eod_signal`` is True only inside the late-close lockdown — the
    portfolio engine uses it to permit eod_pressure-sourced entries while
    every other signal source stays muted.
    """

    skip: bool
    reason: str = ""
    allow_eod_signal: bool = False


def _parse_hhmm(value: str, default: time) -> time:
    """Parse "HH:MM" into a time, falling back to ``default`` on any error."""
    try:
        hours, minutes = (int(x) for x in str(value).strip().split(":", 1))
        return time(hour=hours, minute=minutes)
    except (TypeError, ValueError):
        return default


def _to_et(ts: datetime) -> datetime:
    """Coerce any datetime to America/New_York for time-of-day comparisons."""
    if ts.tzinfo is None:
        return ET.localize(ts)
    return ts.astimezone(ET)


def _parse_event_calendar(values: Iterable[str]) -> list[datetime]:
    """Convert env-supplied ISO timestamps to tz-aware ET datetimes."""
    parsed: list[datetime] = []
    for raw in values:
        if not raw:
            continue
        try:
            event = datetime.fromisoformat(str(raw).strip())
        except (TypeError, ValueError):
            continue
        if event.tzinfo is None:
            event = ET.localize(event)
        parsed.append(event.astimezone(ET))
    return parsed


_EVENT_CACHE: list[datetime] = _parse_event_calendar(SIGNALS_EVENT_CALENDAR)


def _within_lunch_window(ts_et: datetime) -> bool:
    start = _parse_hhmm(SIGNALS_LUNCH_START_ET, time(11, 30))
    end = _parse_hhmm(SIGNALS_LUNCH_END_ET, time(13, 30))
    if start >= end:
        return False
    current = ts_et.time()
    return start <= current < end


def _within_late_close_lockdown(ts_et: datetime) -> bool:
    minutes = int(SIGNALS_LATE_CLOSE_LOCKDOWN_MINUTES or 0)
    if minutes <= 0:
        return False
    cutoff_dt = datetime.combine(ts_et.date(), _MARKET_CLOSE, tzinfo=ts_et.tzinfo)
    return cutoff_dt - timedelta(minutes=minutes) <= ts_et < cutoff_dt


def _within_event_window(ts_et: datetime) -> Optional[datetime]:
    """Return the matched event timestamp when ts_et lands in any buffer."""
    buffer_minutes = int(SIGNALS_EVENT_BUFFER_MINUTES or 0)
    if buffer_minutes <= 0 or not _EVENT_CACHE:
        return None
    delta = timedelta(minutes=buffer_minutes)
    for event in _EVENT_CACHE:
        if event - delta <= ts_et <= event + delta:
            return event
    return None


def evaluate(
    *,
    timestamp: datetime,
    msi_conviction: float,
    signal_source: str,
) -> FilterDecision:
    """Decide whether a new entry should be suppressed at ``timestamp``.

    ``msi_conviction`` is the normalized MSI in [0, 1].  ``signal_source``
    is the broad family driving the entry — "advanced:eod_pressure" can
    still fire inside the late-close lockdown; everything else is muted.
    """
    if not SIGNALS_TIME_FILTER_ENABLED:
        return FilterDecision(skip=False)

    ts_et = _to_et(timestamp)

    event = _within_event_window(ts_et)
    if event is not None:
        return FilterDecision(
            skip=True,
            reason=f"Event buffer ±{int(SIGNALS_EVENT_BUFFER_MINUTES)}m around "
            f"{event.isoformat()}",
        )

    if _within_late_close_lockdown(ts_et):
        if signal_source == "advanced:eod_pressure":
            return FilterDecision(skip=False, allow_eod_signal=True)
        return FilterDecision(
            skip=True,
            reason=f"Late-close lockdown (last {int(SIGNALS_LATE_CLOSE_LOCKDOWN_MINUTES)}m); "
            "only eod_pressure entries allowed",
        )

    if _within_lunch_window(ts_et):
        # Source-based carve-out: advanced signals and Playbook Action
        # Cards each run setup-specific filters before reaching the
        # regime gate (e.g. trap_detection requires structural break +
        # decelerating flow), so the time-of-day chop suppressor would
        # double-gate.  When the entry source matches any configured
        # prefix, bypass the lunch override entirely.
        source_lower = (signal_source or "").lower()
        for prefix in SIGNALS_LUNCH_BYPASS_SOURCES:
            if source_lower.startswith(prefix):
                return FilterDecision(skip=False)
        # Override allows truly strong conviction (e.g., trend-day breakout)
        # to trade through the chop window.
        threshold = float(SIGNALS_LUNCH_MSI_OVERRIDE)
        if msi_conviction < threshold:
            return FilterDecision(
                skip=True,
                reason=f"Lunch chop {SIGNALS_LUNCH_START_ET}-{SIGNALS_LUNCH_END_ET} ET; "
                f"conviction {msi_conviction:.2f} below override {threshold:.2f}",
            )

    return FilterDecision(skip=False)
