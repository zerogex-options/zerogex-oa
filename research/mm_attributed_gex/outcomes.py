"""Forward market outcomes each gamma reading is scored against.

Separated from the tests themselves so the same outcome definitions serve
every comparison.  If the existing methodology and the MM-attributed one were
scored against even slightly different outcome definitions, any difference
between them would be uninterpretable.

Outcomes are computed from ZeroGEX's own ``underlying_quotes`` minute bars.
All of them are strictly forward-looking from the reading's timestamp — the bar
*at* the timestamp is the entry reference and never part of the outcome window,
so a reading can never be scored partly on information it already contained.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Optional, Sequence

import numpy as np

__all__ = [
    "Bar",
    "BarSeries",
    "ForwardOutcome",
    "DEFAULT_HORIZONS",
    "compute_outcomes",
]

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    import pytz

    ET = pytz.timezone("US/Eastern")  # type: ignore[assignment]

#: Forward horizons in minutes, matching requirement 9.
DEFAULT_HORIZONS: tuple[int, ...] = (5, 15, 30, 60)


@dataclass(frozen=True, slots=True)
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


class BarSeries:
    """Indexed minute bars with the lookups the outcome math needs.

    Timestamps are held in a parallel sorted list so every lookup is a binary
    search: an ``O(log n)`` probe per reading instead of an ``O(n)`` scan, which
    is what keeps a multi-month study from becoming quadratic.
    """

    __slots__ = ("bars", "_stamps", "_session_start_idx", "_session_end_idx", "_vwap")

    def __init__(self, bars: Sequence[Bar]) -> None:
        self.bars = sorted(bars, key=lambda b: b.ts)
        self._stamps = [b.ts for b in self.bars]
        self._session_start_idx: dict[date, int] = {}
        self._session_end_idx: dict[date, int] = {}
        for i, bar in enumerate(self.bars):
            d = bar.ts.astimezone(ET).date()
            self._session_start_idx.setdefault(d, i)
            self._session_end_idx[d] = i
        self._vwap: Optional[list[float]] = None

    def __len__(self) -> int:
        return len(self.bars)

    def index_at_or_before(self, ts: datetime) -> Optional[int]:
        i = bisect.bisect_right(self._stamps, ts) - 1
        return i if i >= 0 else None

    def index_at_or_after(self, ts: datetime) -> Optional[int]:
        i = bisect.bisect_left(self._stamps, ts)
        return i if i < len(self._stamps) else None

    def close_at_or_before(self, ts: datetime) -> Optional[float]:
        i = self.index_at_or_before(ts)
        return self.bars[i].close if i is not None else None

    def session_end_index(self, ts: datetime) -> Optional[int]:
        return self._session_end_idx.get(ts.astimezone(ET).date())

    def next_session_end_index(self, ts: datetime) -> Optional[int]:
        d = ts.astimezone(ET).date()
        later = [k for k in self._session_end_idx if k > d]
        if not later:
            return None
        return self._session_end_idx[min(later)]

    def slice(self, start: int, end: int) -> list[Bar]:
        """Bars in ``(start, end]`` — the entry bar itself is excluded."""
        lo = max(0, start + 1)
        hi = min(len(self.bars), end + 1)
        return self.bars[lo:hi] if hi > lo else []

    def session_vwap(self, idx: int) -> Optional[float]:
        """Typical-price VWAP from the session open through ``idx``.

        ``underlying_quotes`` carries no per-bar traded volume for a cash index
        (its ``up_volume``/``down_volume`` describe tick direction), so this is
        an unweighted running mean of ``(H+L+C)/3`` — the standard cash-index
        substitute.  Named VWAP because that is the level traders watch; the
        approximation is stated here so nobody reads it as volume-weighted.
        """
        if idx < 0 or idx >= len(self.bars):
            return None
        start = self._session_start_idx.get(self.bars[idx].ts.astimezone(ET).date())
        if start is None or idx < start:
            return None
        window = self.bars[start : idx + 1]
        if not window:
            return None
        return float(np.mean([(b.high + b.low + b.close) / 3.0 for b in window]))


@dataclass
class ForwardOutcome:
    """Everything measured forward of one reading."""

    timestamp: datetime
    entry: float
    # Per-horizon forward measures, keyed by horizon minutes.
    ret_bps: dict[int, Optional[float]] = field(default_factory=dict)
    abs_ret_bps: dict[int, Optional[float]] = field(default_factory=dict)
    realized_vol_bps: dict[int, Optional[float]] = field(default_factory=dict)
    range_bps: dict[int, Optional[float]] = field(default_factory=dict)
    max_up_bps: dict[int, Optional[float]] = field(default_factory=dict)
    max_down_bps: dict[int, Optional[float]] = field(default_factory=dict)
    continuation: dict[int, Optional[bool]] = field(default_factory=dict)
    mean_reversion: dict[int, Optional[float]] = field(default_factory=dict)
    large_move: dict[int, Optional[bool]] = field(default_factory=dict)
    # Session-scale measures.
    ret_to_close_bps: Optional[float] = None
    abs_ret_to_close_bps: Optional[float] = None
    range_to_close_bps: Optional[float] = None
    realized_vol_to_close_bps: Optional[float] = None
    close_to_close_bps: Optional[float] = None
    # VWAP behavior.
    vwap_distance_bps: Optional[float] = None
    vwap_reversion: dict[int, Optional[float]] = field(default_factory=dict)

    def flat(self, horizons: Sequence[int]) -> dict[str, Any]:
        """Flatten to scalar columns for the regression/grouping code."""
        out: dict[str, Any] = {
            "entry": self.entry,
            "ret_to_close_bps": self.ret_to_close_bps,
            "abs_ret_to_close_bps": self.abs_ret_to_close_bps,
            "range_to_close_bps": self.range_to_close_bps,
            "realized_vol_to_close_bps": self.realized_vol_to_close_bps,
            "close_to_close_bps": self.close_to_close_bps,
            "vwap_distance_bps": self.vwap_distance_bps,
        }
        for h in horizons:
            out[f"ret_{h}m_bps"] = self.ret_bps.get(h)
            out[f"abs_ret_{h}m_bps"] = self.abs_ret_bps.get(h)
            out[f"rvol_{h}m_bps"] = self.realized_vol_bps.get(h)
            out[f"range_{h}m_bps"] = self.range_bps.get(h)
            out[f"max_up_{h}m_bps"] = self.max_up_bps.get(h)
            out[f"max_down_{h}m_bps"] = self.max_down_bps.get(h)
            out[f"continuation_{h}m"] = self.continuation.get(h)
            out[f"mean_reversion_{h}m"] = self.mean_reversion.get(h)
            out[f"large_move_{h}m"] = self.large_move.get(h)
            out[f"vwap_reversion_{h}m_bps"] = self.vwap_reversion.get(h)
        return out


def _bps(numerator: float, base: float) -> Optional[float]:
    if base <= 0:
        return None
    return 10_000.0 * numerator / base


def compute_outcomes(
    series: BarSeries,
    timestamp: datetime,
    *,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
    large_move_bps: float = 50.0,
    lookback_minutes: int = 15,
) -> Optional[ForwardOutcome]:
    """Forward outcomes for one reading, or ``None`` when bars are missing.

    ``large_move_bps`` is the threshold for the "large directional move"
    indicator.  ``lookback_minutes`` sets the prior move whose *continuation*
    is being tested — trend persistence needs a prior direction to persist in,
    and taking it from before the reading keeps the measure causal.
    """
    entry_idx = series.index_at_or_before(timestamp)
    if entry_idx is None:
        return None
    entry_bar = series.bars[entry_idx]
    # Guard against an entry bar that is hours stale (an off-session reading).
    if abs((timestamp - entry_bar.ts).total_seconds()) > 15 * 60:
        return None
    entry = entry_bar.close
    if entry <= 0:
        return None

    out = ForwardOutcome(timestamp=timestamp, entry=entry)

    prior_idx = series.index_at_or_before(timestamp - timedelta(minutes=lookback_minutes))
    prior_move = None
    if prior_idx is not None and prior_idx < entry_idx:
        prior_move = entry - series.bars[prior_idx].close

    vwap = series.session_vwap(entry_idx)
    if vwap:
        out.vwap_distance_bps = _bps(entry - vwap, entry)

    for h in horizons:
        end_idx = series.index_at_or_before(timestamp + timedelta(minutes=h))
        window = series.slice(entry_idx, end_idx) if end_idx is not None else []
        if not window:
            out.ret_bps[h] = None
            continue
        end_bar = window[-1]
        ret = _bps(end_bar.close - entry, entry)
        out.ret_bps[h] = ret
        out.abs_ret_bps[h] = abs(ret) if ret is not None else None
        highs = max(b.high for b in window)
        lows = min(b.low for b in window)
        out.range_bps[h] = _bps(highs - lows, entry)
        out.max_up_bps[h] = _bps(highs - entry, entry)
        out.max_down_bps[h] = _bps(entry - lows, entry)

        closes = np.array([entry] + [b.close for b in window], dtype=float)
        if closes.size >= 3:
            rets = np.diff(np.log(closes))
            out.realized_vol_bps[h] = float(10_000.0 * np.sqrt(np.sum(rets**2)))
        else:
            out.realized_vol_bps[h] = None

        if prior_move is not None and prior_move != 0 and ret is not None:
            out.continuation[h] = (prior_move > 0 and ret > 0) or (prior_move < 0 and ret < 0)
        else:
            out.continuation[h] = None

        # Mean reversion: how much of the window's peak excursion was given
        # back by its end.  1.0 = fully retraced, 0.0 = closed at the extreme.
        excursion = max(highs - entry, entry - lows)
        if excursion > 0 and ret is not None:
            retained = abs(end_bar.close - entry)
            out.mean_reversion[h] = max(0.0, 1.0 - retained / excursion)
        else:
            out.mean_reversion[h] = None

        out.large_move[h] = (abs(ret) >= large_move_bps) if ret is not None else None

        if vwap:
            end_vwap = series.session_vwap(end_idx) if end_idx is not None else None
            if end_vwap:
                # Did price move toward its own VWAP over the window?
                # Positive = closed the gap.
                before = abs(entry - vwap)
                after = abs(end_bar.close - end_vwap)
                out.vwap_reversion[h] = _bps(before - after, entry)

    # Remainder of session.
    session_end = series.session_end_index(timestamp)
    if session_end is not None and session_end > entry_idx:
        window = series.slice(entry_idx, session_end)
        if window:
            end_bar = window[-1]
            out.ret_to_close_bps = _bps(end_bar.close - entry, entry)
            out.abs_ret_to_close_bps = (
                abs(out.ret_to_close_bps) if out.ret_to_close_bps is not None else None
            )
            highs = max(b.high for b in window)
            lows = min(b.low for b in window)
            out.range_to_close_bps = _bps(highs - lows, entry)
            closes = np.array([entry] + [b.close for b in window], dtype=float)
            if closes.size >= 3:
                rets = np.diff(np.log(closes))
                out.realized_vol_to_close_bps = float(10_000.0 * np.sqrt(np.sum(rets**2)))

    # Close-to-close: this session's close to the next session's close.
    if session_end is not None:
        next_end = series.next_session_end_index(timestamp)
        if next_end is not None:
            today_close = series.bars[session_end].close
            if today_close > 0:
                out.close_to_close_bps = _bps(
                    series.bars[next_end].close - today_close, today_close
                )
    return out
