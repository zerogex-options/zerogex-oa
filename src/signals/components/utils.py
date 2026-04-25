"""Shared helpers for scoring components.

Extracted to remove near-identical copies of 5-bar momentum math and
session time math across components.  Also centralizes ET-aware session
windowing (DST correct) and a couple of reusable numeric helpers.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Iterable, Optional

import pytz

# US cash session in UTC minutes. Kept for legacy call sites that still pass
# UTC minutes; prefer the ET-native helpers below for new code.
SESSION_OPEN_MIN_UTC = 13 * 60 + 30  # 13:30 UTC (EDT open only)
SESSION_CLOSE_MIN_UTC = 20 * 60  # 20:00 UTC (EDT close only)
SESSION_LENGTH_MIN = SESSION_CLOSE_MIN_UTC - SESSION_OPEN_MIN_UTC

# ET-native session minutes (DST-correct).
SESSION_OPEN_MIN_ET = 9 * 60 + 30  # 09:30 ET
SESSION_CLOSE_MIN_ET = 16 * 60  # 16:00 ET
SESSION_LENGTH_MIN_ET = SESSION_CLOSE_MIN_ET - SESSION_OPEN_MIN_ET

ET = pytz.timezone("US/Eastern")


def minute_of_day(ts: Optional[datetime]) -> Optional[int]:
    """Return the minute-of-day (UTC) for ``ts``, or None if missing.

    Legacy UTC-only helper. New code should use :func:`minute_of_day_et`.
    """
    if ts is None:
        return None
    return ts.hour * 60 + ts.minute


def minute_of_day_et(ts: Optional[datetime]) -> Optional[int]:
    """Return the minute-of-day in *US/Eastern*, DST-correct.

    Assumes ``ts`` is tz-aware (psycopg2 returns tz-aware rows for
    TIMESTAMPTZ columns).  If ``ts`` is naive, it is treated as UTC
    to match the DB's native storage convention.
    """
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = pytz.UTC.localize(ts)
    et = ts.astimezone(ET)
    return et.hour * 60 + et.minute


def pct_change_n_bar(closes: list[float], n: int = 5) -> float:
    """Return ``(closes[-1] - closes[-n]) / closes[-n]`` with edge-case safety.

    Returns 0.0 if there are fewer than ``n`` bars or the anchor bar is
    non-positive. Used by several components as a compact momentum proxy.
    """
    if not closes or len(closes) < n or closes[-n] <= 0:
        return 0.0
    return (closes[-1] - closes[-n]) / closes[-n]


def realized_sigma(closes: Iterable[float], window: int = 60) -> float:
    """Bar-to-bar realized sigma of log returns over the last ``window`` bars.

    Returns 0.0 when there are fewer than 5 usable bars.  The result is
    per-bar (not annualized): multiply by ``sqrt(n)`` to project n-bar
    variance.
    """
    closes_l = [c for c in closes if c and c > 0]
    if len(closes_l) < 5:
        return 0.0
    tail = closes_l[-window:] if len(closes_l) > window else closes_l
    rets: list[float] = []
    for i in range(1, len(tail)):
        prev = tail[i - 1]
        cur = tail[i]
        if prev > 0 and cur > 0:
            rets.append(math.log(cur / prev))
    if len(rets) < 4:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
    return math.sqrt(max(var, 0.0))


def vol_normalized_momentum(
    closes: list[float],
    n: int = 5,
    vol_window: int = 60,
    clip: float = 3.0,
) -> tuple[float, float]:
    """Return ``(pct_change, z_score)`` where ``z_score`` is clipped to ±clip.

    ``z_score`` is the n-bar return divided by the n-bar projected sigma
    (per-bar realized sigma * sqrt(n)).  Use this in place of the raw
    ``pct_change / fixed_threshold`` pattern for a vol-regime-aware
    momentum signal.
    """
    pct = pct_change_n_bar(closes, n)
    sigma = realized_sigma(closes, vol_window)
    if sigma <= 0:
        return pct, 0.0
    projected = sigma * math.sqrt(n)
    if projected <= 0:
        return pct, 0.0
    z = pct / projected
    z = max(-clip, min(clip, z))
    return pct, z
