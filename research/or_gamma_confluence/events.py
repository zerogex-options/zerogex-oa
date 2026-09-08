"""First-touch detection on the extension ladder, and outcome labelling.

The dependent variable, stated once: **when price first reaches extension X,
does the PREVIOUS extension trade before the NEXT one?**  Reversion is the
previous rung; continuation is the next one.  Nothing here looks at gamma —
this module answers "what did price do", and :mod:`.levels` answers
independently "what was already there".  Keeping them apart is what makes it
possible to measure the second's effect on the first.

Four rules, each of which changes the answer:

**Events start when the range freezes.**  No rung can fire before
``OpeningRange.end``.  Extensions are defined in terms of ``R``, and ``R`` is
not known until the OR window closes; a touch labelled against a
partially-formed range is a look-ahead.

**The touch bar is not part of the outcome.**  The forward scan starts at the
NEXT bar, matching ``research/msi_regime_excursion/excursion.py`` ("the bar at
the timestamp supplies the entry reference and is never part of the outcome
window").  The one thing the touch bar IS asked is whether it already ran past
the next rung — a bar that slices from below the rung to beyond the next one
never paused, and that is recorded as its own outcome
(``continuation_same_bar``) rather than being resolved from bars that came
after the move was already over.

**Both times are recorded, not just the winner.**  The brief asks for
``time_to_previous_extension`` AND ``time_to_next_extension``; a scan that
stopped at the first of the two could not report the other.  So the scan runs
to the bell and records both, and the outcome is derived by comparing them.

**One grind at a level is one observation.**  A rung is spent after its first
touch.  Without that, price oscillating in the touch band for forty minutes
contributes forty rows and every standard error in the study is a fiction.
Re-arming is available (``rearm_minutes`` + ``rearm_distance_r``) so the choice
can be tested, but the default — and the brief's reading — is first touch only.

Bars are 1-minute OHLC, so intra-bar ORDER is unknowable.  Where both the
previous and the next rung are reached inside one bar the event is labelled
``ambiguous`` and excluded from the headline rate rather than resolved by a
convention that would be indistinguishable from a coin flip.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional, Sequence

from research.msi_regime_excursion.excursion import Bar
from research.or_gamma_confluence.config import ResearchConfig
from research.or_gamma_confluence.ranges import (
    SIDE_DOWN,
    SIDE_UP,
    ExtensionLadder,
    OpeningRange,
    Rung,
)

__all__ = [
    "OUTCOME_REVERSAL",
    "OUTCOME_CONTINUATION",
    "OUTCOME_CONTINUATION_SAME_BAR",
    "OUTCOME_AMBIGUOUS",
    "OUTCOME_CENSORED",
    "RESOLVED_OUTCOMES",
    "CONTINUATION_OUTCOMES",
    "TouchEvent",
    "extract_touch_events",
]

#: The previous (inner) rung traded first — the reversion outcome.
OUTCOME_REVERSAL = "reversal_first"
#: The next (outer) rung traded first — the continuation outcome.
OUTCOME_CONTINUATION = "continuation_first"
#: The touch bar itself already ran past the next rung: price did not pause at
#: this level at all.  A continuation, but a distinct kind of one, so the
#: "levels are being sliced through" hypothesis can be read directly.
OUTCOME_CONTINUATION_SAME_BAR = "continuation_same_bar"
#: Both rungs reached inside one 1-minute bar; order unknowable.
OUTCOME_AMBIGUOUS = "ambiguous"
#: Neither reached before the bell.
OUTCOME_CENSORED = "censored"

#: Outcomes that carry a reversion-vs-continuation verdict.  ``ambiguous`` and
#: ``censored`` are deliberately absent: they are reported, never folded into
#: the denominator.
RESOLVED_OUTCOMES: frozenset[str] = frozenset(
    {OUTCOME_REVERSAL, OUTCOME_CONTINUATION, OUTCOME_CONTINUATION_SAME_BAR}
)

CONTINUATION_OUTCOMES: frozenset[str] = frozenset(
    {OUTCOME_CONTINUATION, OUTCOME_CONTINUATION_SAME_BAR}
)


@dataclass(frozen=True)
class TouchEvent:
    """One labelled decision point: price reached a rung, and then what."""

    symbol: str
    session: date
    rung_index: int
    rung_label: str
    rung_k: float
    side: str
    level_price: float
    touched_at: datetime
    #: Index of the touch bar within the session's bar list — the anchor every
    #: forward measurement starts from.
    touch_bar_index: int
    #: The touch bar's extreme on the approach side (its high for an up rung).
    #: This is how far price actually got, which can overshoot the level.
    touch_price: float
    #: The touch bar's close — the entry reference for excursion measurement.
    #: Never the extreme: nothing can fill at a wick.
    spot_at_touch: float
    prev_price: float
    next_price: Optional[float]
    outcome: str
    resolved_at: Optional[datetime]
    minutes_to_resolve: Optional[float]
    #: Independent of the outcome: when each rung was first reached, if ever.
    minutes_to_prev: Optional[float]
    minutes_to_next: Optional[float]
    #: 1 for the first touch of this rung today, 2 for the next (re-arm only).
    touch_ordinal: int
    #: False for the outermost rung, where continuation is unobservable.
    next_exists: bool
    #: How many rungs this same bar fired.  >1 means a bar blew through
    #: several levels at once, which is information about the tape, not noise.
    same_bar_touches: int
    #: Minutes from the opening-range close to the touch.
    minutes_since_or: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "session": self.session.isoformat(),
            "rung_index": self.rung_index,
            "rung_label": self.rung_label,
            "rung_k": self.rung_k,
            "side": self.side,
            "level_price": self.level_price,
            "touched_at": self.touched_at.isoformat(),
            "touch_price": self.touch_price,
            "spot_at_touch": self.spot_at_touch,
            "prev_price": self.prev_price,
            "next_price": self.next_price,
            "outcome": self.outcome,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "minutes_to_resolve": self.minutes_to_resolve,
            "minutes_to_prev": self.minutes_to_prev,
            "minutes_to_next": self.minutes_to_next,
            "touch_ordinal": self.touch_ordinal,
            "next_exists": self.next_exists,
            "same_bar_touches": self.same_bar_touches,
            "minutes_since_or": self.minutes_since_or,
        }


def _reaches(side: str, bar: Bar, level: float, tol: float) -> bool:
    """Did ``bar`` reach ``level`` coming from the anchor side?"""
    if side == SIDE_UP:
        return bar.high >= level - tol
    return bar.low <= level + tol


def _reaches_back(side: str, bar: Bar, level: float, tol: float) -> bool:
    """Did ``bar`` reach ``level`` coming BACK toward the anchor?

    The mirror of :func:`_reaches`: for an up rung the previous rung is below,
    so reverting to it means the bar's LOW got there.
    """
    if side == SIDE_UP:
        return bar.low <= level + tol
    return bar.high >= level - tol


def _touch_extreme(side: str, bar: Bar) -> float:
    return bar.high if side == SIDE_UP else bar.low


def _minutes(a: datetime, b: datetime) -> float:
    return (b - a).total_seconds() / 60.0


def _resolve(
    rung: Rung,
    ladder: ExtensionLadder,
    bars: Sequence[Bar],
    touch_idx: int,
    cfg: ResearchConfig,
    tick: Optional[float],
) -> tuple[str, Optional[datetime], Optional[float], Optional[float], Optional[float]]:
    """Label one touch.

    Returns ``(outcome, resolved_at, minutes_to_resolve, minutes_to_prev,
    minutes_to_next)``.  ``minutes_to_prev`` / ``minutes_to_next`` are recorded
    for the whole rest of the session regardless of which decided the outcome,
    because the brief asks for both and a scan that stopped early could not
    supply the loser.
    """
    touch_bar = bars[touch_idx]
    prev_price = ladder.previous_price(rung)
    next_price = ladder.next_price(rung)
    prev_tol = cfg.tolerance(prev_price, tick)
    next_tol = cfg.tolerance(next_price, tick) if next_price is not None else 0.0

    # The touch bar is asked one question only: did it already run past the
    # next rung?  If so price never paused here and the outcome is settled by
    # the same bar that created the event — so no forward scan runs and
    # ``minutes_to_prev`` is None (unmeasured, not "never reached").
    if next_price is not None and _reaches(rung.side, touch_bar, next_price, next_tol):
        return OUTCOME_CONTINUATION_SAME_BAR, touch_bar.ts, 0.0, None, 0.0

    first_prev: Optional[datetime] = None
    first_next: Optional[datetime] = None
    for bar in bars[touch_idx + 1 :]:
        hit_prev = first_prev is None and _reaches_back(rung.side, bar, prev_price, prev_tol)
        hit_next = (
            first_next is None
            and next_price is not None
            and _reaches(rung.side, bar, next_price, next_tol)
        )
        if hit_prev and hit_next:
            # Both inside one minute — order unknowable.  Report both times
            # (they are the same instant) but refuse to pick a winner.
            elapsed = _minutes(touch_bar.ts, bar.ts)
            return OUTCOME_AMBIGUOUS, bar.ts, elapsed, elapsed, elapsed
        if hit_prev:
            first_prev = bar.ts
        if hit_next:
            first_next = bar.ts
        if first_prev is not None and first_next is not None:
            break

    to_prev = _minutes(touch_bar.ts, first_prev) if first_prev is not None else None
    to_next = _minutes(touch_bar.ts, first_next) if first_next is not None else None

    if first_prev is None and first_next is None:
        return OUTCOME_CENSORED, None, None, None, None
    if first_next is None or (first_prev is not None and first_prev < first_next):
        return OUTCOME_REVERSAL, first_prev, to_prev, to_prev, to_next
    return OUTCOME_CONTINUATION, first_next, to_next, to_prev, to_next


def extract_touch_events(
    symbol: str,
    orange: OpeningRange,
    ladder: ExtensionLadder,
    bars: Sequence[Bar],
    cfg: ResearchConfig,
    *,
    tick: Optional[float] = None,
) -> list[TouchEvent]:
    """Every first-touch event for one session, in chronological order.

    ``bars`` must be the session's bars ascending; only those at or after
    ``orange.end`` can produce an event.  Rungs touched by the same bar are
    emitted nearest-to-anchor first, which is the order price must have passed
    through them.
    """
    session_bars = [b for b in bars if b.ts >= orange.end]
    if not session_bars:
        return []
    # Map back to indices in the session-bar list so forward scans are cheap.
    stamps = [b.ts for b in session_bars]

    #: rung index -> (ordinal already fired, timestamp it was spent at)
    fired: dict[int, int] = {}
    spent_at: dict[int, datetime] = {}
    events: list[TouchEvent] = []

    for i, bar in enumerate(session_bars):
        hits: list[Rung] = []
        for rung in ladder.rungs:
            tol = cfg.tolerance(rung.price, tick)
            if not _reaches(rung.side, bar, rung.price, tol):
                continue
            ordinal = fired.get(rung.index, 0)
            if ordinal:
                if cfg.first_touch_only:
                    continue
                last = spent_at.get(rung.index)
                if last is None:
                    continue
                cooled = _minutes(last, bar.ts) >= float(cfg.rearm_minutes or 0)
                # Price must also have LEFT the level, or a slow grind
                # re-fires on the cooldown clock alone.
                travelled = _left_band(rung, session_bars, stamps, last, bar.ts, orange.width, cfg)
                if not (cooled and travelled):
                    continue
            hits.append(rung)

        if not hits:
            continue
        # Nearest the anchor first: that is the order price passed them.
        hits.sort(key=lambda r: abs(r.k))
        for rung in hits:
            ordinal = fired.get(rung.index, 0) + 1
            fired[rung.index] = ordinal
            spent_at[rung.index] = bar.ts
            outcome, resolved_at, mins, to_prev, to_next = _resolve(
                rung, ladder, session_bars, i, cfg, tick
            )
            events.append(
                TouchEvent(
                    symbol=symbol,
                    session=orange.session,
                    rung_index=rung.index,
                    rung_label=rung.label,
                    rung_k=rung.k,
                    side=rung.side,
                    level_price=rung.price,
                    touched_at=bar.ts,
                    touch_bar_index=i,
                    touch_price=_touch_extreme(rung.side, bar),
                    spot_at_touch=bar.close,
                    prev_price=ladder.previous_price(rung),
                    next_price=ladder.next_price(rung),
                    outcome=outcome,
                    resolved_at=resolved_at,
                    minutes_to_resolve=mins,
                    minutes_to_prev=to_prev,
                    minutes_to_next=to_next,
                    touch_ordinal=ordinal,
                    next_exists=ladder.next_price(rung) is not None,
                    same_bar_touches=len(hits),
                    minutes_since_or=_minutes(orange.end, bar.ts),
                )
            )
    return events


def _left_band(
    rung: Rung,
    bars: Sequence[Bar],
    stamps: Sequence[datetime],
    since: datetime,
    until: datetime,
    r_width: float,
    cfg: ResearchConfig,
) -> bool:
    """Did price travel ``rearm_distance_r * R`` away from ``rung`` in between?

    The distance half of the re-arm rule.  A rung that price has been sitting
    on has not become a fresh decision just because a cooldown elapsed.
    """
    threshold = cfg.rearm_distance_r * r_width
    if threshold <= 0:
        return True
    lo = bisect.bisect_right(stamps, since)
    hi = bisect.bisect_left(stamps, until)
    for bar in bars[lo:hi]:
        if rung.side == SIDE_UP and rung.price - bar.low >= threshold:
            return True
        if rung.side == SIDE_DOWN and bar.high - rung.price >= threshold:
            return True
    return False
