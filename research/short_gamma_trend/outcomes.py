"""What price did after one reading, from cash-session minute bars.

Entry is the close of the reading's own minute bar. Every outcome window is
half-open ``(entry, end]`` -- the entry bar is never part of it -- so a reading
is never scored on price it had already seen. Only cash-session bars
(09:30-15:59 ET) are used, so no window reaches into SPY's extended hours or
across an overnight gap, and a fixed horizon is measured only when its whole
window fits before the close (otherwise the last minutes of every session
would enter as artificially small moves).

The momentum variant's prior move reads the bar 30 minutes before entry, and
only within the same session, for the same reason: the overnight gap is not
a move the session made.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Iterable, Optional, Union

from research.msi_regime_excursion.excursion import ET, Bar, BarSeries

HORIZONS = (15, 30, 60)
REST = "rest"
Horizon = Union[int, str]

RTH_OPEN = time(9, 30)
RTH_CLOSE = time(16, 0)

#: The momentum variant's look-back and the move below which there is none.
#: 5 bps matches FLAT_PCT in frontend/core/impliedDirection.ts.
MOMENTUM_LOOKBACK_MIN = 30
MOMENTUM_FLAT_BPS = 5.0

#: A reading whose nearest earlier bar is older than this is not scored.
MAX_ENTRY_STALENESS = timedelta(minutes=2)


def cash_session_bars(bars: Iterable[Bar]) -> list[Bar]:
    """Bars stamped 09:30-15:59 ET on weekdays."""
    out = []
    for bar in bars:
        local = bar.ts.astimezone(ET)
        if local.weekday() < 5 and RTH_OPEN <= local.time() < RTH_CLOSE:
            out.append(bar)
    return out


class SessionBars:
    """A :class:`BarSeries` of cash-session bars plus per-session suffix
    extremes, so the rest-of-session outcome is a lookup rather than a scan."""

    def __init__(self, bars: Iterable[Bar]) -> None:
        self.series = BarSeries(cash_session_bars(bars))
        n = len(self.series.bars)
        self._suffix_high = [0.0] * n
        self._suffix_low = [0.0] * n
        bars_ = self.series.bars
        for i in range(n - 1, -1, -1):
            same_next = (
                i + 1 < n
                and bars_[i + 1].ts.astimezone(ET).date() == bars_[i].ts.astimezone(ET).date()
            )
            hi, lo = bars_[i].high, bars_[i].low
            if same_next:
                hi = max(hi, self._suffix_high[i + 1])
                lo = min(lo, self._suffix_low[i + 1])
            self._suffix_high[i] = hi
            self._suffix_low[i] = lo

    def __len__(self) -> int:
        return len(self.series)

    def suffix_extremes(self, start: int) -> tuple[float, float]:
        """``(max high, min low)`` from bar ``start`` to its session's end."""
        return self._suffix_high[start], self._suffix_low[start]


@dataclass
class Outcome:
    entry_ts: datetime
    entry: float
    #: Sign of the prior-30-minute move in the same session; 0 below 5 bps.
    prior_move: int
    #: All in basis points of the entry price. ``None`` when not measurable.
    ret: dict[Horizon, Optional[float]] = field(default_factory=dict)
    up: dict[Horizon, Optional[float]] = field(default_factory=dict)
    down: dict[Horizon, Optional[float]] = field(default_factory=dict)
    range: dict[Horizon, Optional[float]] = field(default_factory=dict)


def _bps(delta: float, base: float) -> float:
    return 10_000.0 * delta / base


def _fill(out: Outcome, key: Horizon, hi: float, lo: float, last: float) -> None:
    entry = out.entry
    out.ret[key] = _bps(last - entry, entry)
    # Excursions are floored at zero: never trading above entry is an
    # upside excursion of 0, not a negative one.
    out.up[key] = _bps(max(0.0, hi - entry), entry)
    out.down[key] = _bps(max(0.0, entry - lo), entry)
    out.range[key] = _bps(hi - lo, entry)


def _blank(out: Outcome, key: Horizon) -> None:
    out.ret[key] = out.up[key] = out.down[key] = out.range[key] = None


def _prior_move(series: BarSeries, idx: int) -> int:
    entry_bar = series.bars[idx]
    prior_idx = series.index_at_or_before(entry_bar.ts - timedelta(minutes=MOMENTUM_LOOKBACK_MIN))
    if prior_idx is None or prior_idx >= idx:
        return 0
    prior = series.bars[prior_idx]
    if prior.ts.astimezone(ET).date() != entry_bar.ts.astimezone(ET).date() or prior.close <= 0:
        return 0
    move = _bps(entry_bar.close - prior.close, prior.close)
    if abs(move) < MOMENTUM_FLAT_BPS:
        return 0
    return 1 if move > 0 else -1


def measure(
    bars: SessionBars, ts: datetime, horizons: Iterable[int] = HORIZONS
) -> Optional[Outcome]:
    """Everything forward of a reading at ``ts``; ``None`` without an entry bar."""
    series = bars.series
    idx = series.index_at_or_before(ts)
    if idx is None:
        return None
    entry_bar = series.bars[idx]
    if entry_bar.close <= 0 or ts - entry_bar.ts > MAX_ENTRY_STALENESS:
        return None
    if entry_bar.ts.astimezone(ET).date() != ts.astimezone(ET).date():
        return None

    out = Outcome(entry_ts=entry_bar.ts, entry=entry_bar.close, prior_move=_prior_move(series, idx))
    last_idx = series.session_last_index(entry_bar.ts)
    session_end = series.bars[last_idx].ts if last_idx is not None else None

    for h in horizons:
        end_ts = entry_bar.ts + timedelta(minutes=h)
        window = series.window(idx, end_ts)
        if session_end is None or end_ts > session_end or not window:
            _blank(out, h)
            continue
        _fill(
            out,
            h,
            max(b.high for b in window),
            min(b.low for b in window),
            window[-1].close,
        )

    if last_idx is None or last_idx <= idx:
        _blank(out, REST)
    else:
        hi, lo = bars.suffix_extremes(idx + 1)
        _fill(out, REST, hi, lo, series.bars[last_idx].close)
    return out
