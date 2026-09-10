"""Realized forward price excursion, measured from minute bars.

Every measure here is strictly forward-looking from a reading's timestamp. The
bar *at* the timestamp supplies the entry reference and is never part of the
outcome window, so a reading can never be scored partly on price action it had
already seen. Windows are half-open ``(entry, end]``.

Three families of measure, because the product's copy makes three different
kinds of claim and they are not interchangeable:

* **Directionless magnitude** -- ``max_up`` / ``max_down`` / ``range`` /
  ``abs_ret``. How far price travelled, ignoring which way. This is what
  "trends can run" versus "range-bound" is a claim about.
* **Bias-conditioned excursion** -- ``mfe`` / ``mae`` against the prevailing
  bias. "Favor trades in the prevailing bias" is a claim that a trade taken in
  the direction of the existing move does better in this band, so it has to be
  scored against a direction, and the direction has to come from information
  available at the reading (the prior-``N``-minute trend, which is how
  ``frontend/core/impliedDirection.ts`` defines "prevailing bias").
* **Point targets** -- ``P(max_up >= T)`` and ``P(max_down >= T)`` for targets
  in the instrument's own price units. A scalper does not trade bps; the
  question "does the regime read change the odds of a 4-point run in ES" is
  the question in the form it was actually asked.

Bars are ZeroGEX's own: ``underlying_quotes`` for cash symbols,
``futures_quotes`` for ES / NQ. Stdlib only -- no numpy -- so this runs on a
production host with nothing installed beyond what the services already need.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Iterable, Optional, Sequence

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - only on a stdlib without zoneinfo
    import pytz

    ET = pytz.timezone("US/Eastern")  # type: ignore[assignment]

__all__ = [
    "Bar",
    "BarSeries",
    "ForwardExcursion",
    "DEFAULT_HORIZONS",
    "DEFAULT_BIAS_LOOKBACK_MIN",
    "REST_OF_SESSION",
    "compute_excursion",
]

#: Forward horizons in minutes. 5/15/30 are scalp-scale (the horizons the
#: churned trader actually works on); 60 bridges to the session claim; the
#: rest-of-session measure carries the session-scale claim.
DEFAULT_HORIZONS: tuple[int, ...] = (5, 15, 30, 60)

#: Window used to read the "prevailing bias" at a reading. Matches
#: ``TREND_LOOKBACK_MS`` in ``frontend/core/impliedDirection.ts``.
DEFAULT_BIAS_LOOKBACK_MIN = 30

#: Sentinel horizon key for the rest-of-session window.
REST_OF_SESSION = "rest_of_session"

#: A prior move smaller than this reads as "no prevailing bias". Matches
#: ``FLAT_PCT`` (0.05%) in ``frontend/core/impliedDirection.ts``.
DEFAULT_FLAT_BPS = 5.0

#: Cash-session close, ET. The rest-of-session window ends at the last bar at
#: or before this time on the reading's own ET date -- for futures too, so the
#: session-scale measure means the same thing on both axes.
SESSION_CLOSE = time(16, 0)


@dataclass(frozen=True)
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


class BarSeries:
    """Sorted minute bars with the lookups the excursion math needs.

    Timestamps live in a parallel sorted list so each lookup is a binary
    search: an ``O(log n)`` probe per reading rather than an ``O(n)`` scan,
    which is what keeps a 90-day study over four instruments from going
    quadratic.
    """

    __slots__ = ("bars", "_stamps", "_session_last_idx")

    def __init__(self, bars: Iterable[Bar]) -> None:
        self.bars: list[Bar] = sorted(bars, key=lambda b: b.ts)
        self._stamps: list[datetime] = [b.ts for b in self.bars]
        # Last bar index at or before SESSION_CLOSE for each ET date.
        self._session_last_idx: dict[date, int] = {}
        for i, bar in enumerate(self.bars):
            local = bar.ts.astimezone(ET)
            if local.time() <= SESSION_CLOSE:
                self._session_last_idx[local.date()] = i

    def __len__(self) -> int:
        return len(self.bars)

    def index_at_or_before(self, ts: datetime) -> Optional[int]:
        i = bisect.bisect_right(self._stamps, ts) - 1
        return i if i >= 0 else None

    def index_at_or_after(self, ts: datetime) -> Optional[int]:
        i = bisect.bisect_left(self._stamps, ts)
        return i if i < len(self._stamps) else None

    def session_last_index(self, ts: datetime) -> Optional[int]:
        """Index of the last bar at or before 16:00 ET on ``ts``'s ET date."""
        return self._session_last_idx.get(ts.astimezone(ET).date())

    def window(self, start_idx: int, end_ts: datetime) -> list[Bar]:
        """Bars in ``(start_idx, end_ts]`` -- the entry bar itself excluded."""
        lo = start_idx + 1
        hi = bisect.bisect_right(self._stamps, end_ts)
        return self.bars[lo:hi] if hi > lo else []

    def window_to_index(self, start_idx: int, end_idx: int) -> list[Bar]:
        """Bars in ``(start_idx, end_idx]`` -- the entry bar itself excluded."""
        lo = start_idx + 1
        hi = end_idx + 1
        return self.bars[lo:hi] if hi > lo else []


@dataclass
class ForwardExcursion:
    """Everything measured forward of one reading, for one instrument."""

    timestamp: datetime
    entry: float
    #: +1 / -1 / 0, read from the prior-``bias_lookback_min`` trend.
    bias: int
    #: Bars actually available in each window (a short window near the close
    #: is kept, and reported, rather than silently scored as a full one).
    bars_in_window: dict[object, int] = field(default_factory=dict)

    # Directionless magnitude, basis points of the entry price.
    max_up_bps: dict[object, Optional[float]] = field(default_factory=dict)
    max_down_bps: dict[object, Optional[float]] = field(default_factory=dict)
    range_bps: dict[object, Optional[float]] = field(default_factory=dict)
    ret_bps: dict[object, Optional[float]] = field(default_factory=dict)
    abs_ret_bps: dict[object, Optional[float]] = field(default_factory=dict)

    # Bias-conditioned, basis points. None when there is no prevailing bias.
    mfe_bps: dict[object, Optional[float]] = field(default_factory=dict)
    mae_bps: dict[object, Optional[float]] = field(default_factory=dict)

    # Same excursions in the instrument's own price units, for point targets.
    max_up_pts: dict[object, Optional[float]] = field(default_factory=dict)
    max_down_pts: dict[object, Optional[float]] = field(default_factory=dict)

    def horizons(self) -> list[object]:
        return list(self.bars_in_window.keys())


def _bps(numerator: float, base: float) -> Optional[float]:
    if base <= 0:
        return None
    return 10_000.0 * numerator / base


def _read_bias(
    series: BarSeries,
    entry_idx: int,
    lookback_min: int,
    flat_bps: float,
) -> int:
    """Prevailing bias at the reading: sign of the prior-``lookback_min`` move.

    Strictly backward-looking -- it reads the bar at or before
    ``entry_ts - lookback`` and compares it to the entry close, so it uses only
    information the reading itself already had.
    """
    entry_bar = series.bars[entry_idx]
    prior_idx = series.index_at_or_before(entry_bar.ts - timedelta(minutes=lookback_min))
    if prior_idx is None or prior_idx >= entry_idx:
        return 0
    prior = series.bars[prior_idx]
    if prior.close <= 0:
        return 0
    move = _bps(entry_bar.close - prior.close, prior.close)
    if move is None or abs(move) < flat_bps:
        return 0
    return 1 if move > 0 else -1


def _measure(
    window: Sequence[Bar],
    entry: float,
    bias: int,
    out: ForwardExcursion,
    key: object,
) -> None:
    """Fill every measure for one window. ``window`` excludes the entry bar."""
    out.bars_in_window[key] = len(window)
    if not window or entry <= 0:
        for slot in (
            out.max_up_bps, out.max_down_bps, out.range_bps, out.ret_bps,
            out.abs_ret_bps, out.mfe_bps, out.mae_bps, out.max_up_pts,
            out.max_down_pts,
        ):
            slot[key] = None
        return

    hi = max(b.high for b in window)
    lo = min(b.low for b in window)
    last = window[-1].close

    # Excursions are floored at zero: if price never traded above the entry,
    # the upside excursion is 0, not a negative number. Anything else makes
    # "how far did it run up" mean two different things depending on sign.
    up_pts = max(0.0, hi - entry)
    down_pts = max(0.0, entry - lo)

    out.max_up_pts[key] = up_pts
    out.max_down_pts[key] = down_pts
    out.max_up_bps[key] = _bps(up_pts, entry)
    out.max_down_bps[key] = _bps(down_pts, entry)
    out.range_bps[key] = _bps(hi - lo, entry)
    ret = _bps(last - entry, entry)
    out.ret_bps[key] = ret
    out.abs_ret_bps[key] = abs(ret) if ret is not None else None

    if bias > 0:
        out.mfe_bps[key] = out.max_up_bps[key]
        out.mae_bps[key] = out.max_down_bps[key]
    elif bias < 0:
        out.mfe_bps[key] = out.max_down_bps[key]
        out.mae_bps[key] = out.max_up_bps[key]
    else:
        out.mfe_bps[key] = None
        out.mae_bps[key] = None


def compute_excursion(
    series: BarSeries,
    ts: datetime,
    *,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
    bias_lookback_min: int = DEFAULT_BIAS_LOOKBACK_MIN,
    flat_bps: float = DEFAULT_FLAT_BPS,
    include_rest_of_session: bool = True,
    require_full_window: bool = True,
) -> Optional[ForwardExcursion]:
    """Measure forward excursion from ``ts``. ``None`` if there is no entry bar.

    ``require_full_window`` drops a horizon whose window is cut short by the
    end of the archive -- otherwise the last hour of every extract would enter
    the sample as an artificially small excursion and bias every mean down.
    A window cut short by the *session* close is a different thing and is kept,
    since that is a real property of the horizon, not of the archive.
    """
    entry_idx = series.index_at_or_before(ts)
    if entry_idx is None:
        return None
    entry_bar = series.bars[entry_idx]
    entry = entry_bar.close
    if entry <= 0:
        return None

    bias = _read_bias(series, entry_idx, bias_lookback_min, flat_bps)
    out = ForwardExcursion(timestamp=ts, entry=entry, bias=bias)

    last_ts = series.bars[-1].ts
    for h in horizons:
        end_ts = entry_bar.ts + timedelta(minutes=h)
        if require_full_window and end_ts > last_ts:
            # The archive ends before this window does: not measurable.
            out.bars_in_window[h] = 0
            _measure([], entry, bias, out, h)
            continue
        _measure(series.window(entry_idx, end_ts), entry, bias, out, h)

    if include_rest_of_session:
        end_idx = series.session_last_index(entry_bar.ts)
        if end_idx is None or end_idx <= entry_idx:
            # Reading is at or after the session's last bar: nothing forward.
            _measure([], entry, bias, out, REST_OF_SESSION)
        else:
            _measure(
                series.window_to_index(entry_idx, end_idx), entry, bias, out,
                REST_OF_SESSION,
            )

    return out
