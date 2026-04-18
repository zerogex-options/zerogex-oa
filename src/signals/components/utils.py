"""Shared helpers for scoring components.

Extracted to remove near-identical copies of 5-bar momentum math and
hardcoded cash-session time bounds across components.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

# US cash session in UTC minutes. These are the same values several
# components were hardcoding independently; centralize so a DST policy
# or holiday-hours change only has one place to land.
SESSION_OPEN_MIN_UTC = 13 * 60 + 30   # 13:30 UTC
SESSION_CLOSE_MIN_UTC = 20 * 60       # 20:00 UTC
SESSION_LENGTH_MIN = SESSION_CLOSE_MIN_UTC - SESSION_OPEN_MIN_UTC


def minute_of_day(ts: Optional[datetime]) -> Optional[int]:
    """Return the minute-of-day (UTC) for ``ts``, or None if missing."""
    if ts is None:
        return None
    return ts.hour * 60 + ts.minute


def pct_change_n_bar(closes: list[float], n: int = 5) -> float:
    """Return ``(closes[-1] - closes[-n]) / closes[-n]`` with edge-case safety.

    Returns 0.0 if there are fewer than ``n`` bars or the anchor bar is
    non-positive. Used by several components as a compact momentum proxy.
    """
    if not closes or len(closes) < n or closes[-n] <= 0:
        return 0.0
    return (closes[-1] - closes[-n]) / closes[-n]
