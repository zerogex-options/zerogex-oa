"""How long a tested wall survives — Kaplan-Meier over wall tests.

Why this replaces the headline number
-------------------------------------
``P(break | tested)`` is not a single number.  The first real run made that
unavoidable: sweeping only the resolution horizon moved the estimate from
15.3% (30 min) to 29.7% (45 min) to 34.4% (60 min), on intervals that do not
overlap.  Nothing about the market changed between those runs — a longer watch
simply gives price more chances to go, so the "base rate" was substantially a
restatement of a parameter we chose.

Two further problems with the point estimate, both fixed here:

* **It threw away a quarter of the data.**  Events whose horizon ran past
  16:00 were censored and dropped — 47 of 178 on the first run.  But "held for
  15 minutes and then the bell rang" is not missing data; it is the
  observation that the wall had not broken at 15 minutes.  Discarding those
  biases the sample toward tests with room to resolve, i.e. away from the
  late-session tape where 0DTE gamma is largest.
* **The event sets were not comparable across horizons.**  A longer window
  occupies the wall for longer, so the re-arm logic emits fewer events (234 at
  30 min vs 178 at 60).  Comparing rates across horizons compares different
  populations, not the same tests scored differently.

The survival formulation dissolves all three.  Every test contributes the time
it was actually watched (``WallTest.observed_minutes``) plus whether it broke.
``held`` and ``censored`` become the same kind of observation — right-censored,
differing only in when watching stopped — and the estimate is a CURVE:

    P(break within t minutes of the test), for t up to the horizon.

That is also a better answer to the question people actually ask, which is
never "does it break" but "does it break before I need to be out".

Method
------
Kaplan-Meier product-limit estimator with Greenwood's variance, and log-log
(Kalbfleisch-Prentice) confidence bounds rather than the plain-normal ones —
the plain interval misbehaves near 0 and 1, which is exactly where a
break-probability curve lives in its first fifteen minutes.

The one assumption worth stating: censoring must be independent of the
outcome, given the covariates.  Here censoring is "the session ended", which
is not obviously independent of break behaviour — late-day tape has its own
character.  So this fixes the *discarding* problem, not the underlying
time-of-day question, and :func:`by_group` exists so the curve can be split by
session half and the assumption inspected rather than assumed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Sequence

__all__ = [
    "Observation",
    "SurvivalPoint",
    "kaplan_meier",
    "break_probability_at",
    "by_group",
]


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
