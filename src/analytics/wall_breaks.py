"""Gamma-wall test events and how long a tested wall survives.

Production home of the labelling and survival math behind the wall-break
statistics the product publishes.  Pure computation: no database, no network,
no configuration.  ``src/tools/wall_break_stats_refresh.py`` feeds it rows and
persists the result; ``research/wall_break_odds`` re-exports from here so the
exploratory study and the shipped number can never diverge.

Why the definitions are what they are
-------------------------------------
**A pierce is not a break.**  ZeroGEX's own published research on failed
breakouts notes they "often hold for the first ten or fifteen minutes before
unwinding".  A label that fires on the first tick through a level counts every
one of those as a break, so a break here must *confirm*: price closes beyond
the wall, by a buffer, for :attr:`EventConfig.confirm_minutes` consecutive
minutes.

**One grind at a wall is one observation.**  Forty minutes pressed against a
wall is a single decision a trader faces, not forty.  Events are de-duplicated
via a re-arm cooldown, and a wall that breaks is spent for the session.

**Held and censored are the same kind of observation.**  A test that held for
the full hour and one that held fifteen minutes before the closing bell both
say "had not broken by the time we stopped watching"; they differ only in when
watching stopped.  Both carry ``observed_minutes`` and both feed
:func:`kaplan_meier`, which is why the published curve uses the whole sample
rather than only the tests with room to resolve.

**The answer is a curve.**  "Does the wall break" is not well posed without a
clock — a longer watch gives price more chances to go.  Measured on SPX, the
same tests read 15.3% at a thirty-minute horizon and 34.4% at sixty, on
non-overlapping intervals.  Quote ``P(break within t)``, never a bare rate.

See ``docs/design/wall-break-odds.md`` for the full methodology and the
measured results.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Iterable, Optional, Sequence
from zoneinfo import ZoneInfo

__all__ = [
    # events
    "ET",
    "SESSION_START",
    "SESSION_END",
    "SESSION_MINUTES",
    "EventConfig",
    "PriceBar",
    "WallFrame",
    "WallTest",
    "StepSeries",
    "extract_wall_tests",
    # survival
    "Observation",
    "SurvivalPoint",
    "LogRank",
    "kaplan_meier",
    "break_probability_at",
    "by_group",
    "logrank",
]

ET = ZoneInfo("America/New_York")

SESSION_START = time(9, 30)
SESSION_END = time(16, 0)

#: Minutes in a regular cash session — the denominator for "how much of the
#: day is left", used by the feature layer and by censoring arithmetic here.
SESSION_MINUTES = 390


@dataclass(frozen=True)
class EventConfig:
    """Every threshold the labelling depends on, in one auditable place.

    Defaults are inherited from production where production has an opinion, so
    a result here is not quietly measuring a different level than the product
    draws.  ``touch_pct`` is ``src/jobs/level_history.TOUCH_PCT`` (5 bp) and
    ``confirm_minutes`` is its ``MIN_HOLD_MINUTES`` (10).
    """

    #: How close price must come for the wall to count as TESTED, as a
    #: fraction of the wall.  5 bp ≈ 39c on a 775 SPY strike.
    touch_pct: float = 0.0005
    #: Floor under the touch band so a low-priced underlying keeps a band
    #: wider than one tick.
    touch_min: float = 0.01
    #: How far beyond the wall price must close for the move to count toward
    #: a break.  Same size as the touch band: a break has to clear the zone
    #: that counted as a test, or "tested" and "broke" would overlap.
    break_buffer_pct: float = 0.0005
    #: Consecutive minutes of closes beyond the buffer required to confirm.
    confirm_minutes: int = 10
    #: How long a test is given to resolve before it counts as held.
    resolution_minutes: int = 60
    #: Quiet period after a resolved test before the same wall may re-arm.
    #: Without it, price oscillating in the touch band emits a new event
    #: every minute after the cooldown of the last one.
    rearm_minutes: int = 15
    #: Sessions with fewer usable GEX frames than this are dropped whole. A
    #: handful of scattered frames cannot distinguish "the wall sat there all
    #: day" from "we sampled it twice"; mirrors level_history.MIN_FRAMES in
    #: intent, at the resolution a per-minute study needs.
    min_session_frames: int = 30

    def tolerance(self, level: float) -> float:
        """The touch band around ``level``."""
        return max(abs(level) * self.touch_pct, self.touch_min)

    def buffer(self, level: float) -> float:
        """The break buffer around ``level``."""
        return max(abs(level) * self.break_buffer_pct, self.touch_min)


@dataclass(frozen=True)
class PriceBar:
    """One minute of the underlying."""

    ts: datetime
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class WallFrame:
    """One published ``gex_summary`` row, reduced to what labelling needs."""

    ts: datetime
    call_wall: Optional[float]
    put_wall: Optional[float]


class StepSeries:
    """A right-continuous step function over timestamps.

    Wall values are published on a cadence and hold until the next publish, so
    "the wall in force at 11:07:30" is the last frame at or before it — never
    an interpolation, and never the next one (which would be lookahead).
    """

    __slots__ = ("_stamps", "_values")

    def __init__(self, points: Iterable[tuple[datetime, Any]]) -> None:
        clean = sorted((ts, v) for ts, v in points if ts is not None and v is not None)
        self._stamps = [ts for ts, _ in clean]
        self._values = [v for _, v in clean]

    def __len__(self) -> int:
        return len(self._stamps)

    def at(self, ts: datetime) -> Optional[Any]:
        """The value in force at ``ts``; None if nothing was published yet."""
        idx = bisect.bisect_right(self._stamps, ts) - 1
        return self._values[idx] if idx >= 0 else None

    def first_stamp(self) -> Optional[datetime]:
        return self._stamps[0] if self._stamps else None

    def age_minutes(self, ts: datetime) -> Optional[float]:
        """How long the value in force at ``ts`` has been unchanged, in minutes.

        Walks back over equal values, so a wall republished identically for an
        hour reports 60 rather than the publish cadence.
        """
        idx = bisect.bisect_right(self._stamps, ts) - 1
        if idx < 0:
            return None
        value = self._values[idx]
        start = idx
        while start > 0 and self._values[start - 1] == value:
            start -= 1
        return max((ts - self._stamps[start]).total_seconds() / 60.0, 0.0)


@dataclass(frozen=True)
class WallTest:
    """One labelled decision point: price arrived at a wall, and then what."""

    symbol: str
    session: date
    side: str  # "call" | "put"
    tested_at: datetime
    wall: float
    spot_at_test: float
    outcome: str  # "broke" | "held" | "censored"
    resolved_at: Optional[datetime]
    minutes_to_resolve: Optional[float]
    #: 1 for the first test of this wall value today, 2 for the next, ...
    test_ordinal: int
    #: Furthest price got beyond the wall during the window, as a signed % of
    #: the wall and oriented toward the break (positive = past the wall).
    #: Diagnostic only — it is an OUTCOME, never a feature.
    excursion_pct: Optional[float] = None
    #: Minutes this test was actually WATCHED — to confirmation for a break,
    #: otherwise to the last bar examined (the horizon, or the bell, whichever
    #: came first). This is what makes ``held`` and ``censored`` the same kind
    #: of observation for survival analysis: both are right-censored, they
    #: merely differ in when the watching stopped. See :mod:`survival`.
    observed_minutes: Optional[float] = None

    @property
    def broke(self) -> bool:
        return self.outcome == "broke"

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "session": self.session.isoformat(),
            "side": self.side,
            "tested_at": self.tested_at.isoformat(),
            "wall": self.wall,
            "spot_at_test": self.spot_at_test,
            "outcome": self.outcome,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "minutes_to_resolve": self.minutes_to_resolve,
            "test_ordinal": self.test_ordinal,
            "excursion_pct": self.excursion_pct,
            "observed_minutes": self.observed_minutes,
        }


def _session_end_dt(session: date) -> datetime:
    return datetime.combine(session, SESSION_END, tzinfo=ET)


def _beyond(side: str, price: float, wall: float, buf: float) -> bool:
    """Is ``price`` decisively past ``wall`` on the breaking side?"""
    return price > wall + buf if side == "call" else price < wall - buf


def _tests(side: str, bar: PriceBar, wall: float, tol: float) -> bool:
    """Did this bar reach the touch band around the wall?"""
    return bar.high >= wall - tol if side == "call" else bar.low <= wall + tol


def _resolve(
    side: str,
    wall: float,
    start_idx: int,
    bars: Sequence[PriceBar],
    cfg: EventConfig,
    session_end: datetime,
) -> tuple[str, Optional[datetime], Optional[float], float]:
    """Scan forward from ``start_idx`` and decide the outcome.

    Returns ``(outcome, resolved_at, excursion_pct, observed_minutes)``.  A
    break is only declared on ``confirm_minutes`` consecutive closes beyond
    the buffer; the run resets the moment one close falls back inside, which
    is exactly the failed-breakout shape the label has to exclude.

    ``observed_minutes`` is how far the scan actually got — to confirmation
    for a break, otherwise to the last bar inside the horizon and the session.
    A ``held`` result is therefore "did not break in the time we watched",
    which is the same statement a ``censored`` result makes with a shorter
    watch. The survival layer treats them identically and the base-rate layer
    does not, which is the whole reason both are recorded.
    """
    buf = cfg.buffer(wall)
    deadline = bars[start_idx].ts + timedelta(minutes=cfg.resolution_minutes)
    # The horizon must fit inside the session. If it does not, the outcome is
    # only observable when a break confirms before the bell; otherwise it is
    # censored rather than assumed to have held.
    truncated = deadline > session_end
    run = 0
    best: Optional[float] = None
    start_ts = bars[start_idx].ts
    watched = 0.0
    for bar in bars[start_idx:]:
        if bar.ts > deadline or bar.ts > session_end:
            break
        watched = (bar.ts - start_ts).total_seconds() / 60.0
        extreme = bar.high if side == "call" else bar.low
        signed = (extreme - wall) / wall * (1.0 if side == "call" else -1.0) * 100.0
        best = signed if best is None else max(best, signed)
        if _beyond(side, bar.close, wall, buf):
            run += 1
            if run >= cfg.confirm_minutes:
                return "broke", bar.ts, best, watched
        else:
            run = 0
    if truncated:
        return "censored", None, best, watched
    return "held", None, best, watched


def extract_wall_tests(
    symbol: str,
    session: date,
    frames: Sequence[WallFrame],
    bars: Sequence[PriceBar],
    cfg: Optional[EventConfig] = None,
) -> list[WallTest]:
    """Every labelled wall test in one session, both sides, chronological.

    ``frames`` and ``bars`` may arrive in any order and may be sparse; both are
    sorted and the wall is read as a step function so a bar is always scored
    against the wall value that was actually published to a trader at that
    minute.

    Returns ``[]`` — never a partial or invented result — when the session has
    too few frames to characterise, which is what keeps a thin data day from
    entering the sample as if it were a clean one.
    """
    cfg = cfg or EventConfig()
    in_session = [
        f
        for f in frames
        if f.ts.astimezone(ET).date() == session
        and SESSION_START <= f.ts.astimezone(ET).time() <= SESSION_END
    ]
    if len(in_session) < cfg.min_session_frames:
        return []
    ordered_bars = sorted(
        (
            b
            for b in bars
            if b.ts.astimezone(ET).date() == session
            and SESSION_START <= b.ts.astimezone(ET).time() <= SESSION_END
        ),
        key=lambda b: b.ts,
    )
    if not ordered_bars:
        return []

    session_end = _session_end_dt(session)
    out: list[WallTest] = []
    for side in ("call", "put"):
        key = "call_wall" if side == "call" else "put_wall"
        series = StepSeries((f.ts, getattr(f, key)) for f in in_session)
        if not len(series):
            continue
        # Per-wall-value state: how many times it has been tested today, and
        # whether it is spent (broken once — a broken wall is not the same
        # object on a re-approach, so it emits no further tests).
        ordinals: dict[float, int] = {}
        spent: set[float] = set()
        armed_after: Optional[datetime] = None
        i = 0
        while i < len(ordered_bars):
            bar = ordered_bars[i]
            wall = series.at(bar.ts)
            if wall is None or wall in spent:
                i += 1
                continue
            if armed_after is not None and bar.ts < armed_after:
                i += 1
                continue
            if not _tests(side, bar, float(wall), cfg.tolerance(float(wall))):
                i += 1
                continue
            wall = float(wall)
            ordinal = ordinals.get(wall, 0) + 1
            ordinals[wall] = ordinal
            outcome, resolved_at, excursion, watched = _resolve(
                side, wall, i, ordered_bars, cfg, session_end
            )
            out.append(
                WallTest(
                    symbol=symbol,
                    session=session,
                    side=side,
                    tested_at=bar.ts,
                    wall=wall,
                    spot_at_test=bar.close,
                    outcome=outcome,
                    resolved_at=resolved_at,
                    minutes_to_resolve=(
                        (resolved_at - bar.ts).total_seconds() / 60.0 if resolved_at else None
                    ),
                    test_ordinal=ordinal,
                    excursion_pct=excursion,
                    observed_minutes=watched,
                )
            )
            if outcome == "broke":
                spent.add(wall)
                armed_after = resolved_at
            else:
                # Re-arm a cooldown past the END of the window we just spent,
                # so a grind at the wall yields spaced observations rather than
                # one per minute.
                end = resolved_at or (bar.ts + timedelta(minutes=cfg.resolution_minutes))
                armed_after = end + timedelta(minutes=cfg.rearm_minutes)
            # Continue scanning from the bar after the test; the arming guard
            # above is what enforces spacing, not the loop index.
            i += 1
    out.sort(key=lambda e: (e.tested_at, e.side))
    return out


@dataclass(frozen=True)
class Observation:
    """One test: how long it was watched, and whether it broke in that time."""

    minutes: float
    broke: bool


@dataclass(frozen=True)
class SurvivalPoint:
    """The estimate at one event time."""

    minutes: float
    at_risk: int
    breaks: int
    survival: float
    #: P(break by this time) = 1 - survival, with log-log bounds.
    break_prob: float
    break_lo: float
    break_hi: float


def kaplan_meier(observations: Iterable[Observation], z: float = 1.96) -> list[SurvivalPoint]:
    """Product-limit estimate of the break curve.

    Returns one point per distinct time at which a break was observed. An
    empty input, or one with no breaks at all, returns ``[]`` — a curve of
    "nothing ever broke" is reported as no curve rather than as a flat line
    that invites reading a zero as a measurement.
    """
    obs = [o for o in observations if o.minutes is not None and math.isfinite(o.minutes)]
    if not obs:
        return []
    times = sorted({o.minutes for o in obs if o.broke})
    if not times:
        return []

    survival = 1.0
    greenwood = 0.0
    out: list[SurvivalPoint] = []
    for t in times:
        # At risk = still being watched at t (censored exactly at t counts as
        # at risk; breaks at t are the events).
        at_risk = sum(1 for o in obs if o.minutes >= t)
        breaks = sum(1 for o in obs if o.broke and o.minutes == t)
        if at_risk <= 0:
            continue
        survival *= 1.0 - breaks / at_risk
        if at_risk > breaks:
            greenwood += breaks / (at_risk * (at_risk - breaks))
        lo, hi = _loglog_bounds(survival, greenwood, z)
        out.append(
            SurvivalPoint(
                minutes=t,
                at_risk=at_risk,
                breaks=breaks,
                survival=survival,
                break_prob=1.0 - survival,
                # Bounds flip when converting survival -> break probability.
                break_lo=1.0 - hi,
                break_hi=1.0 - lo,
            )
        )
    return out


def _loglog_bounds(survival: float, greenwood: float, z: float) -> tuple[float, float]:
    """Kalbfleisch-Prentice bounds on S(t); degenerate cases clamp to [0, 1]."""
    if survival <= 0.0 or survival >= 1.0 or greenwood <= 0.0:
        return (max(min(survival, 1.0), 0.0), max(min(survival, 1.0), 0.0))
    log_s = math.log(survival)
    se = math.sqrt(greenwood) / abs(log_s)
    lo = survival ** math.exp(z * se)
    hi = survival ** math.exp(-z * se)
    return (max(lo, 0.0), min(hi, 1.0))


def break_probability_at(curve: Sequence[SurvivalPoint], minutes: float) -> Optional[SurvivalPoint]:
    """The curve's value at ``minutes`` — the last event at or before it.

    None when the curve has not started by then, which is a real answer: no
    break was observed that early, so no estimate is made.
    """
    latest: Optional[SurvivalPoint] = None
    for point in curve:
        if point.minutes <= minutes:
            latest = point
        else:
            break
    return latest


def by_group(
    rows: Iterable[Any],
    key: Callable[[Any], Optional[str]],
    to_obs: Callable[[Any], Optional[Observation]],
    *,
    min_n: int = 30,
) -> dict[str, list[SurvivalPoint]]:
    """Curves split by a grouping key, skipping groups under ``min_n``.

    The intended use is checking the censoring assumption: split by session
    half and see whether the morning and afternoon curves look like the same
    process. If they do not, the pooled curve is an average over two regimes
    and should be reported as such.
    """
    buckets: dict[str, list[Observation]] = {}
    for row in rows:
        group = key(row)
        obs = to_obs(row)
        if group is None or obs is None:
            continue
        buckets.setdefault(group, []).append(obs)
    return {g: kaplan_meier(o) for g, o in buckets.items() if len(o) >= min_n}


@dataclass(frozen=True)
class LogRank:
    """Comparison of two survival curves."""

    observed_a: int
    expected_a: float
    chi2: float
    p_value: float
    n_a: int
    n_b: int


def logrank(a: Sequence[Observation], b: Sequence[Observation]) -> Optional[LogRank]:
    """Log-rank test that two groups share one hazard.

    Needed because eyeballing two curves is exactly the over-reading this
    study exists to avoid: "35.9% vs 25.3%" at n=78 and n=100 is not a result
    until something says whether it survives the noise.

    At each time a break occurs anywhere, the breaks in group A are compared
    against what the pooled risk sets predict, with the hypergeometric
    variance. The statistic is chi-square on 1 degree of freedom, so its
    p-value is the standard normal two-sided tail of its square root.

    Returns None when either group is empty or no break is observed — with no
    events there is nothing to compare, which is a different statement from
    "the curves agree".
    """
    obs_a = [o for o in a if o.minutes is not None and math.isfinite(o.minutes)]
    obs_b = [o for o in b if o.minutes is not None and math.isfinite(o.minutes)]
    if not obs_a or not obs_b:
        return None
    times = sorted({o.minutes for o in obs_a + obs_b if o.broke})
    if not times:
        return None

    observed = expected = variance = 0.0
    for t in times:
        n_a = sum(1 for o in obs_a if o.minutes >= t)
        n_b = sum(1 for o in obs_b if o.minutes >= t)
        n = n_a + n_b
        if n <= 1:
            continue
        d_a = sum(1 for o in obs_a if o.broke and o.minutes == t)
        d = d_a + sum(1 for o in obs_b if o.broke and o.minutes == t)
        if d == 0:
            continue
        share = n_a / n
        observed += d_a
        expected += d * share
        variance += d * share * (1.0 - share) * (n - d) / (n - 1)
    if variance <= 0:
        return None
    chi2 = (observed - expected) ** 2 / variance
    p_value = math.erfc(math.sqrt(chi2 / 2.0))
    return LogRank(
        observed_a=int(observed),
        expected_a=expected,
        chi2=chi2,
        p_value=p_value,
        n_a=len(obs_a),
        n_b=len(obs_b),
    )
