"""Base rates -- is a classified state telling you anything, or just common?

A state's durability is easy to misread. If STABLE_BID is on screen 60% of the
session and any state that is up tends to stay up for a bar or two, then "when
we say stable bid, it holds 30 minutes 71% of the time" sounds like skill and
is very nearly arithmetic. The number that matters is not the hit rate, it is
the hit rate NEXT TO the rate the same question gets with the label ignored.
This module computes that comparison and nothing else: it takes a session's
worth of state labels and answers "how often did this hold, versus how often
anything held".

The precedent is :mod:`src.analytics.forced_flow`, which had to make the same
argument about a structurally-constant sign: a rule that degenerates to
"always guess the majority" has a hit rate equal to the naive baseline and an
edge of ~0 by construction, however good the hit rate looks. The Gamma Weather
vocabulary is exposed to exactly that failure, so it gets the same treatment
before anyone quotes a number from it.

--------------------------------------------------------------------------
The three ways a durability number lies, and what is done about each
--------------------------------------------------------------------------
1. **Right censoring.** A state still running at 16:15 did not last twelve
   bars; it lasted AT LEAST twelve bars. Counting the close as an ending
   drags every run length down, and it does so hardest for the states that
   last longest, which is backwards. :func:`held` therefore returns ``None``
   -- unknown, excluded -- when the session ends before the horizon without
   the state having broken, and ``False`` only where a break was actually
   observed. A break is observable at the moment it happens, so a run that
   dies at bar 3 of a 6-bar horizon still counts as a failure even though the
   horizon ran past the data.

2. **Overlap.** Anchoring at every bar makes bar i and bar i+1 near-duplicate
   observations: if a state survives 30 minutes from one, it almost survives
   30 minutes from the next. The sample size is real, the INDEPENDENCE is
   not, and a p-value computed as though it were will call noise significant.
   Rather than print a number and warn against reading it, comparisons carry
   ``independent`` and simply withhold the p-value when anchors overlap. The
   rates are still shown, labelled DESCRIPTIVE.

3. **A reference that contains the thing being measured.** Comparing a state
   against the pooled rate of all states compares it partly against itself,
   which shrinks the apparent lift of whichever state dominates the sample --
   again hardest on the states that matter most. The headline reference is
   therefore leave-one-out: every anchor NOT carrying this label. The pooled
   rate is reported too, because it is the number a reader would otherwise
   compute by hand and the gap between the two is worth seeing.

None of this is stored anywhere. Weather states are derived on read (see
:mod:`src.analytics.gamma_weather`), which is what makes an honest base-rate
pass possible at all: retuning a threshold re-labels the whole archive, so
this tool always measures the rule that is live rather than a fossil of the
rule that was live when the row was written.

Pure functions throughout -- no DB, no clock, no I/O. The caller assembles
sessions; :mod:`src.tools.gamma_weather_base_rates` is the one that talks to
Postgres.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

# One implementation of each, not two. These live in forced_flow because that
# is where the first track record needed them; duplicating a Wilson interval
# so the import has no underscore would be the drift that
# :mod:`src.hedging_flow_sql` exists to argue against.
from src.analytics.forced_flow import _normal_cdf, _wilson_interval

# --------------------------------------------------------------------------- #
# Tunables.
# --------------------------------------------------------------------------- #

#: Below this many resolved observations a cell is reported but never graded.
#: A normal approximation on a handful of runs lies, and the cells most likely
#: to be thin are the rare states whose durability claim is least supported.
#: Matches the bar forced_flow sets for the same reason.
MIN_GRADED_TRIALS = 30

#: Two-sided significance for the "is this different from the rest" test.
SIGNIFICANCE_P = 0.05

#: How far the lift must sit from 1.0 before a significant difference is worth
#: a word. Statistical significance on a large sample can attach to a
#: difference too small to change a decision.
MIN_MATERIAL_LIFT = 0.10

VERDICT_MORE_DURABLE = "MORE_DURABLE"
VERDICT_LESS_DURABLE = "LESS_DURABLE"
VERDICT_NO_EDGE = "NO_EDGE"
VERDICT_DESCRIPTIVE = "DESCRIPTIVE"
VERDICT_INSUFFICIENT = "INSUFFICIENT"


# --------------------------------------------------------------------------- #
# Proportions.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Proportion:
    """``hits`` out of ``n`` resolved observations, with a Wilson interval.

    ``unresolved`` counts observations that were dropped as censored. It is
    carried rather than discarded because a cell resting on 8 resolved and 40
    censored observations is a different object from one resting on 48, and a
    reader cannot tell them apart from the rate alone.
    """

    n: int = 0
    hits: int = 0
    unresolved: int = 0

    @property
    def rate(self) -> Optional[float]:
        return self.hits / self.n if self.n else None

    @property
    def interval(self) -> Tuple[float, float]:
        return _wilson_interval(self.hits, self.n)

    def as_dict(self) -> dict:
        low, high = self.interval
        return {
            "n": self.n,
            "hits": self.hits,
            "unresolved": self.unresolved,
            "rate": round(self.rate, 4) if self.rate is not None else None,
            "ci_low": round(low, 4),
            "ci_high": round(high, 4),
        }


def tally(outcomes: Sequence[Optional[bool]]) -> Proportion:
    """Fold a run of outcomes into a proportion, ``None`` meaning unresolved."""
    resolved = [o for o in outcomes if o is not None]
    return Proportion(
        n=len(resolved),
        hits=sum(1 for o in resolved if o),
        unresolved=len(outcomes) - len(resolved),
    )


def two_proportion_p(a: Proportion, b: Proportion) -> Optional[float]:
    """Two-sided p-value that ``a`` and ``b`` come from the same rate.

    Pooled-variance z-test on the difference of two proportions. Two-sided on
    purpose: a state that persists LESS than the rest is as much a finding as
    one that persists more, and the weather vocabulary claims both directions
    (fragile rally is supposed to be fragile). Returns ``None`` when the test
    cannot be formed -- an empty cell, or a pooled rate of exactly 0 or 1
    where the standard error collapses.

    Assumes independent observations. Callers holding overlapping anchors must
    not form this; :func:`compare` enforces that rather than trusting them to.
    """
    if a.n <= 0 or b.n <= 0:
        return None
    pooled = (a.hits + b.hits) / (a.n + b.n)
    if pooled <= 0.0 or pooled >= 1.0:
        return None
    se = math.sqrt(pooled * (1.0 - pooled) * (1.0 / a.n + 1.0 / b.n))
    if se == 0.0:
        return None
    z = ((a.hits / a.n) - (b.hits / b.n)) / se
    return 2.0 * (1.0 - _normal_cdf(abs(z)))


@dataclass(frozen=True)
class Comparison:
    """One group's rate against the rest, plus the verdict that follows.

    ``lift`` is the group rate over the LEAVE-ONE-OUT rate, so 1.35 reads "this
    label holds 35% more often than a bar without it". ``pooled`` is the same
    quantity measured against everything including this group, which is what a
    reader computes by hand; the two diverge exactly where the group is large
    enough to move the average it is being judged against.
    """

    group: str
    group_p: Proportion
    other_p: Proportion
    pooled_p: Proportion
    lift: Optional[float]
    pooled_lift: Optional[float]
    p_value: Optional[float]
    verdict: str

    def as_dict(self) -> dict:
        return {
            "group": self.group,
            "observed": self.group_p.as_dict(),
            "others": self.other_p.as_dict(),
            "pooled": self.pooled_p.as_dict(),
            "lift_vs_others": round(self.lift, 3) if self.lift is not None else None,
            "lift_vs_pooled": round(self.pooled_lift, 3) if self.pooled_lift is not None else None,
            "p_value": round(self.p_value, 4) if self.p_value is not None else None,
            "verdict": self.verdict,
        }


def _lift(group: Proportion, reference: Proportion) -> Optional[float]:
    if group.rate is None or reference.rate is None or reference.rate <= 0.0:
        return None
    return group.rate / reference.rate


def compare(
    group: str,
    group_p: Proportion,
    other_p: Proportion,
    independent: bool,
    min_trials: int = MIN_GRADED_TRIALS,
) -> Comparison:
    """Grade one group against the pooled rest.

    ``independent`` is the caller's statement about its own anchors, and it is
    load-bearing: when it is False no p-value is formed and the verdict is
    DESCRIPTIVE, because overlapping windows make the significance test
    confidently wrong rather than merely imprecise.
    """
    pooled_p = Proportion(
        n=group_p.n + other_p.n,
        hits=group_p.hits + other_p.hits,
        unresolved=group_p.unresolved + other_p.unresolved,
    )
    lift = _lift(group_p, other_p)
    pooled_lift = _lift(group_p, pooled_p)

    if not independent:
        verdict = VERDICT_DESCRIPTIVE
        p_value = None
    elif group_p.n < min_trials or other_p.n < min_trials:
        # No p-value either. A normal approximation on a dozen runs produces
        # small numbers readily, and printing one next to a refusal to grade
        # invites exactly the reading the refusal exists to prevent.
        verdict = VERDICT_INSUFFICIENT
        p_value = None
    else:
        p_value = two_proportion_p(group_p, other_p)
        material = lift is not None and abs(lift - 1.0) >= MIN_MATERIAL_LIFT
        if p_value is not None and p_value < SIGNIFICANCE_P and material:
            verdict = VERDICT_MORE_DURABLE if lift > 1.0 else VERDICT_LESS_DURABLE
        else:
            verdict = VERDICT_NO_EDGE

    return Comparison(
        group=group,
        group_p=group_p,
        other_p=other_p,
        pooled_p=pooled_p,
        lift=lift,
        pooled_lift=pooled_lift,
        p_value=p_value,
        verdict=verdict,
    )


def lift_table(
    trials: Dict[str, List[Optional[bool]]],
    independent: bool,
    min_trials: int = MIN_GRADED_TRIALS,
    order: Optional[Sequence[str]] = None,
) -> List[Comparison]:
    """Every group against the pooled rest, ordered by sample size.

    ``trials`` maps a group label to that group's outcomes, ``None`` for
    unresolved. Groups are compared leave-one-out, so each row's reference is
    the other rows combined.

    ``order`` overrides the sort for groups that have a natural sequence -- an
    age ladder read out of order hides the only thing it is there to show.
    """
    tallies = {group: tally(outcomes) for group, outcomes in trials.items()}
    out: List[Comparison] = []
    for group, group_p in tallies.items():
        rest = [p for g, p in tallies.items() if g != group]
        other_p = Proportion(
            n=sum(p.n for p in rest),
            hits=sum(p.hits for p in rest),
            unresolved=sum(p.unresolved for p in rest),
        )
        out.append(compare(group, group_p, other_p, independent, min_trials))
    if order is not None:
        rank = {group: i for i, group in enumerate(order)}
        out.sort(key=lambda c: (rank.get(c.group, len(rank)), c.group))
    else:
        out.sort(key=lambda c: (-c.group_p.n, c.group))
    return out


# --------------------------------------------------------------------------- #
# Sessions, runs and survival.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Session:
    """One trading day's classified bars, chronological.

    ``warmup`` is the count of leading bars that may not be used as anchors.
    They stay in ``states`` because a run that began during warmup and is
    still going afterwards is a real run that later bars have to be measured
    against; they are barred from STARTING a measurement because the rolling
    structure window has not filled yet, which forces
    :func:`~src.analytics.gamma_weather.classify_structure` to read FLAT and
    makes an ACCELERATIVE state unreachable for those bars. Measuring
    durability from a bar whose state was not yet computable would be
    measuring the warmup, not the weather.
    """

    label: str
    bar_starts: Sequence[datetime]
    states: Sequence[str]
    warnings: Sequence[bool] = field(default_factory=tuple)
    ages: Sequence[int] = field(default_factory=tuple)
    warmup: int = 0

    def __post_init__(self) -> None:
        if len(self.bar_starts) != len(self.states):
            raise ValueError("bar_starts and states must be the same length")

    def __len__(self) -> int:
        return len(self.states)


@dataclass(frozen=True)
class StateRun:
    """A maximal stretch of one state within a session.

    ``truncated`` marks a run still in progress at the session's last bar. Its
    length is a lower bound, never an observation, and every consumer here
    treats it that way.
    """

    state: str
    start: int
    length: int
    truncated: bool


def runs(states: Sequence[str]) -> List[StateRun]:
    """Split a session into maximal same-state stretches."""
    out: List[StateRun] = []
    if not states:
        return out
    start = 0
    for i in range(1, len(states) + 1):
        if i == len(states) or states[i] != states[start]:
            out.append(
                StateRun(
                    state=states[start],
                    start=start,
                    length=i - start,
                    truncated=(i == len(states)),
                )
            )
            start = i
    return out


def held(states: Sequence[str], anchor: int, horizon_bars: int) -> Optional[bool]:
    """Did the state at ``anchor`` survive unbroken for ``horizon_bars`` more?

    Returns ``True`` when it held the whole way, ``False`` when a break was
    observed, and ``None`` when the session ran out first without a break --
    unknown, not a failure. That asymmetry is the whole point: a break is
    visible the moment it happens, so a run that dies two bars into a six-bar
    horizon is a resolved failure even though the horizon extends past the
    data. Only survival needs the full window to be confirmed.

    "Held" means continuously. A state that leaves and comes back was not
    intact, which is the sense the spec's checkpoint question uses.
    """
    if anchor < 0 or anchor >= len(states) or horizon_bars < 0:
        return None
    target = states[anchor]
    last = len(states) - 1
    limit = min(anchor + horizon_bars, last)
    for i in range(anchor + 1, limit + 1):
        if states[i] != target:
            return False
    return True if anchor + horizon_bars <= last else None


def every_bar_anchors(session: Session) -> List[int]:
    """Every usable bar. Overlapping, so descriptive only."""
    return list(range(session.warmup, len(session)))


def onset_anchors(session: Session) -> List[int]:
    """The first bar of each run -- one observation per run, non-overlapping.

    The decision-relevant unit: the panel changes to a state and the question
    is whether it sticks. Runs beginning inside warmup are skipped rather than
    shifted forward, because their first bar is where the state actually
    formed and re-anchoring later would quietly measure a partly-elapsed run
    as a fresh one.
    """
    return [r.start for r in runs(session.states) if r.start >= session.warmup]


def checkpoint_anchors(
    session: Session,
    checkpoints: Sequence[time],
    tz,
    max_stale: timedelta = timedelta(minutes=5),
) -> List[int]:
    """The bar COVERING each wall-clock checkpoint, in ``tz``.

    The spec's validation grid is 10:00 / 12:00 / 14:30 ET. Picks the last bar
    starting at or before each checkpoint, so a checkpoint is answered by the
    reading that was on screen at the time rather than by one that had not
    printed yet.

    ``max_stale`` is why this is not simply "the last bar at or before": with
    no bound, a session that ends at noon would answer the 14:30 checkpoint
    with its 11:55 reading, and a gap in the middle of the day would answer
    12:30 with whatever printed before the gap. Either would report a stale
    bar as a checkpoint observation, which is worse than reporting none. One
    bar of staleness is the grid's own resolution; anything beyond it is a
    hole, and a checkpoint that falls in a hole is skipped.
    """
    out: List[int] = []
    for checkpoint in checkpoints:
        chosen: Optional[int] = None
        chosen_at: Optional[datetime] = None
        for i in range(session.warmup, len(session)):
            stamp = session.bar_starts[i]
            local = stamp.astimezone(tz) if stamp.tzinfo is not None else stamp
            if local.time() > checkpoint:
                break
            chosen, chosen_at = i, local
        if chosen is None or chosen_at is None:
            continue
        gap = datetime.combine(chosen_at.date(), checkpoint) - chosen_at.replace(tzinfo=None)
        if gap < max_stale:
            out.append(chosen)
    return out


def survival_trials(
    sessions: Sequence[Session],
    horizon_bars: int,
    anchors,
) -> Dict[str, List[Optional[bool]]]:
    """Group anchored survival outcomes by the state being anchored on.

    ``anchors`` is a callable taking a :class:`Session` and returning the
    indices to measure from -- :func:`onset_anchors`, :func:`every_bar_anchors`
    or a checkpoint selector. Sessions are kept apart throughout: a horizon
    never reaches across an overnight gap, where "the state held" would mean
    nothing.
    """
    out: Dict[str, List[Optional[bool]]] = {}
    for session in sessions:
        for anchor in anchors(session):
            state = session.states[anchor]
            out.setdefault(state, []).append(held(session.states, anchor, horizon_bars))
    return out


def age_band_trials(
    sessions: Sequence[Session],
    horizon_bars: int,
    bands: Sequence[Tuple[str, int, Optional[int]]],
) -> Dict[str, List[Optional[bool]]]:
    """Survival grouped by how long the state had ALREADY held.

    The sharpest test of the age ladder, and the one that can embarrass it: if
    "confirmed" is worth more than "developing", a state that has already run
    30 minutes must survive the next half hour more often than one that has
    run five. If the bands come back flat, the clock on the panel is
    decoration and should be labelled as such rather than quoted.

    ``bands`` are ``(label, min_bars, max_bars_exclusive)``; ``None`` for the
    upper bound means open-ended. Anchors are every usable bar, so the result
    is DESCRIPTIVE -- the bands are read against each other, not tested.
    """
    out: Dict[str, List[Optional[bool]]] = {label: [] for label, _, _ in bands}
    for session in sessions:
        ages = list(session.ages) if session.ages else _ages_from_states(session.states)
        for anchor in every_bar_anchors(session):
            age = ages[anchor]
            for label, low, high in bands:
                if age >= low and (high is None or age < high):
                    out[label].append(held(session.states, anchor, horizon_bars))
                    break
    return out


def _ages_from_states(states: Sequence[str]) -> List[int]:
    """Bars the state at each index has held, counting itself as the first."""
    ages: List[int] = []
    for i, state in enumerate(states):
        ages.append(1 if i == 0 or states[i - 1] != state else ages[i - 1] + 1)
    return ages


def warning_trials(
    sessions: Sequence[Session],
    horizon_bars: int,
) -> Dict[str, List[Optional[bool]]]:
    """Did a change follow the warning, and did it follow a quiet bar less?

    The spec's third validation question. The outcome is inverted relative to
    the survival tables -- a hit here is the state CHANGING within the horizon
    -- because a transition warning that is never followed by a transition is
    the failure mode worth catching. ``None`` still means unresolved, and it
    means it for the same reason: a session that ends with the state intact
    cannot rule out a change that was about to happen.

    Anchors are every bar and therefore overlap, so this is descriptive. A
    warning that fires on three consecutive bars before one change is three
    observations of one event, which is exactly the shape that would make a
    naive p-value here look spectacular.
    """
    out: Dict[str, List[Optional[bool]]] = {"WARNED": [], "QUIET": []}
    for session in sessions:
        warnings = list(session.warnings) if session.warnings else [False] * len(session)
        for anchor in every_bar_anchors(session):
            outcome = held(session.states, anchor, horizon_bars)
            changed = None if outcome is None else (not outcome)
            out["WARNED" if warnings[anchor] else "QUIET"].append(changed)
    return out


@dataclass(frozen=True)
class RunLengths:
    """Observed durations of one state, with the censored ones kept apart."""

    state: str
    complete: List[int] = field(default_factory=list)
    censored: List[int] = field(default_factory=list)

    @property
    def n_runs(self) -> int:
        return len(self.complete) + len(self.censored)

    def survival_at(self, horizon_bars: int) -> Proportion:
        """Share of runs that lasted MORE than ``horizon_bars`` bars.

        A censored run resolves the question only when what was already
        observed settles it: a run cut off at 9 bars has certainly outlived a
        6-bar horizon, while one cut off at 4 bars has not answered a 6-bar
        question either way and is excluded rather than counted as a failure.
        """
        hits = 0
        n = 0
        unresolved = 0
        for length in self.complete:
            n += 1
            if length > horizon_bars:
                hits += 1
        for length in self.censored:
            if length > horizon_bars:
                n += 1
                hits += 1
            else:
                unresolved += 1
        return Proportion(n=n, hits=hits, unresolved=unresolved)

    def as_dict(self) -> dict:
        ordered = sorted(self.complete)
        median = ordered[len(ordered) // 2] if ordered else None
        return {
            "state": self.state,
            "runs": self.n_runs,
            "complete": len(self.complete),
            "censored": len(self.censored),
            "median_complete_bars": median,
            "longest_observed_bars": max(self.complete + self.censored, default=None),
        }


def run_lengths(sessions: Sequence[Session]) -> Dict[str, RunLengths]:
    """Collect per-state run durations across sessions.

    Runs starting inside warmup are dropped, matching :func:`onset_anchors`:
    their length would be measured from a bar whose state was an artifact of
    an unfilled rolling window.
    """
    out: Dict[str, RunLengths] = {}
    for session in sessions:
        for run in runs(session.states):
            if run.start < session.warmup:
                continue
            bucket = out.setdefault(run.state, RunLengths(state=run.state))
            if run.truncated:
                bucket.censored.append(run.length)
            else:
                bucket.complete.append(run.length)
    return out


def state_share(sessions: Sequence[Session]) -> Dict[str, Proportion]:
    """How much of the usable session each state occupied.

    The first thing to read, and the reason the rest of the module exists: a
    state holding 70% of the tape will look durable under any measurement that
    forgets to ask how often it is on screen.
    """
    counts: Dict[str, int] = {}
    total = 0
    for session in sessions:
        for i in every_bar_anchors(session):
            counts[session.states[i]] = counts.get(session.states[i], 0) + 1
            total += 1
    return {state: Proportion(n=total, hits=n) for state, n in sorted(counts.items())}
