"""Pin stability — what the Pin Strike has *done* during a session.

``pin_strike`` answers "where is the pin now" and ``pin_confidence`` answers
"how dominant is it among the viable candidates".  Neither answers the third
question the reading order actually asks: **has this pin been holding still,
or has it been migrating?**

Those are different trades wearing the same label.  A pin that has sat on one
strike since the open is a level dealers keep re-anchoring to.  A pin that has
walked thirty points down with the tape is the model tracking a book that is
repricing — which is information, not failure, but it is not the same signal.
Without it a trader watching a pin drift away reasonably concludes the level
"did not work", when what actually happened is that it moved with them.

This module turns the day's per-minute pin path into that account:

  * ``current_*`` — where the pin is right now, and since when.
  * ``held_*`` — the most recent value that has actually SETTLED, plus
    ``session_open_pin`` / ``net_migration``: where the pin started and how
    far, net, it has travelled between settled levels.
  * ``distinct_values`` — how many strikes it has genuinely occupied.
  * ``current_established`` — whether those two levels are the same value, i.e.
    whether the pin standing right now has held long enough to count as a move.

Flicker is pruned the way :mod:`src.jobs.level_history` prunes it for the
walls, and for the same reason: a pin ticking to a neighbouring strike for a
minute and back is a near-tie in the scoring, not a migration.  Reporting it
would bury the one or two moves that mattered.  A 713 pin that printed 712
twice has not "migrated to 712" — that distinction is the whole point of the
metric, so single-sample excursions are dropped before anything is counted,
including one that lands on the very last sample of the session.

Design notes
------------
* **Pure and independently testable**, exactly like :mod:`pin_strike`: nothing
  here touches the DB, the clock, or ``market_calendar``.  The caller fetches
  the session's rows and passes them in, so the functions are deterministic
  given their inputs.
* **Quiet minutes are not a reset.**  ``pin_strike`` is NULL whenever no
  candidate clears the score floor, and that happens mid-session, not just at
  the end.  Runs are therefore built over the non-null samples only: a pin
  that goes quiet for two minutes and comes back at the same strike has held
  that strike throughout, and saying otherwise would invent a migration.  The
  quiet minutes are counted separately so a caller can still surface them.
* **Nullable, never fabricated.**  A session with no pin at all yields
  ``None`` rather than a zeroed record — the same hide-don't-zero rule the
  levels themselves follow.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

# A pin must hold at least this many samples to count as a genuine occupancy
# rather than a scoring near-tie.  Matches the walls' MIN_HOLD_MINUTES in
# src/jobs/level_history.py: both levels live in strike space and both migrate
# in whole-strike jumps, so the same noise floor applies.  The CURRENT run is
# always kept however new it is -- it is the level standing right now.
MIN_HOLD_SAMPLES = 10

# Strike equality tolerance.  Strikes are discrete, so this only has to absorb
# float/Decimal round-tripping, not genuine nearness.
_EPS = 1e-6


@dataclass
class PinRun:
    """One unbroken occupancy of a single strike."""

    value: float
    start: datetime
    end: datetime
    samples: int = 1


@dataclass
class PinStability:
    """The session's pin path, reduced to what a trader needs to read.

    Two levels, deliberately:

    * ``current_*`` is where the pin is RIGHT NOW, however new that value is.
      A caller asking "where is the pin" must never be told about a previous
      one.
    * ``held_*`` is the most recent value that has actually SETTLED -- cleared
      :data:`MIN_HOLD_SAMPLES`. Migration is measured between settled levels,
      so a one-sample tick at the bell is not reported as a move.

    ``current_established`` says whether those two are the same value. When it
    is False the current pin is provisional: real, but not yet evidence of a
    migration.
    """

    current_pin: float
    current_since: datetime
    current_samples: int
    current_established: bool
    held_pin: float
    held_since: datetime
    held_samples: int
    session_open_pin: float
    net_migration: float
    distinct_values: int
    quiet_samples: int
    total_samples: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "current_pin": self.current_pin,
            "current_since": self.current_since.isoformat(),
            "current_samples": self.current_samples,
            "current_established": self.current_established,
            "held_pin": self.held_pin,
            "held_since": self.held_since.isoformat(),
            "held_samples": self.held_samples,
            "session_open_pin": self.session_open_pin,
            "net_migration": self.net_migration,
            "distinct_values": self.distinct_values,
            "quiet_samples": self.quiet_samples,
            "total_samples": self.total_samples,
        }


def _to_float(value: Any) -> Optional[float]:
    """Coerce a Decimal/str/number to float; None (or unparseable) -> None."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    # A non-positive strike is never a real pin (same rule the clients apply).
    return out if out > 0 else None


def _raw_runs(frames: Sequence[Dict[str, Any]]) -> List[PinRun]:
    """Collapse the per-minute path into runs of an unchanged pin value."""
    runs: List[PinRun] = []
    for row in frames:
        value = _to_float(row.get("pin_strike"))
        if value is None:
            continue
        ts = row.get("timestamp")
        if not isinstance(ts, datetime):
            continue
        if runs and abs(runs[-1].value - value) < _EPS:
            runs[-1].end = ts
            runs[-1].samples += 1
        else:
            runs.append(PinRun(value=value, start=ts, end=ts))
    return runs


def _merge_adjacent(runs: List[PinRun]) -> List[PinRun]:
    """Fuse neighbouring runs carrying the same value (post-pruning)."""
    merged: List[PinRun] = []
    for run in runs:
        if merged and abs(merged[-1].value - run.value) < _EPS:
            merged[-1].end = run.end
            merged[-1].samples += run.samples
        else:
            merged.append(run)
    return merged


def _established_runs(runs: List[PinRun]) -> List[PinRun]:
    """The runs that actually SETTLED, at :data:`MIN_HOLD_SAMPLES` or more.

    Unlike the walls' pruning in :mod:`src.jobs.level_history`, the final run
    gets NO exemption here.  That exemption is what let a one-sample tick at
    the bell be promoted to a migration: mid-session such a blip is pruned, but
    at the session edge it survived as "the current run" and the reported
    net move jumped with it -- precisely the flicker-is-not-a-migration rule
    this module exists to enforce, defeated by its own edge case.

    Where the pin IS is answered separately (``current_*``), so nothing is lost
    by holding this series to one consistent standard.
    """
    return _merge_adjacent([r for r in runs if r.samples >= MIN_HOLD_SAMPLES])


def build_pin_stability(frames: Sequence[Dict[str, Any]]) -> Optional[PinStability]:
    """Reduce a session's ``gex_summary`` rows to a :class:`PinStability`.

    ``frames`` are chronological dicts carrying at least ``timestamp`` (a
    ``datetime``) and ``pin_strike``.  Returns ``None`` when the session
    carries no active pin at any point -- there is nothing to say, and saying
    it with zeros would be worse than saying nothing.
    """
    if not frames:
        return None

    runs = _merge_adjacent(_raw_runs(frames))
    if not runs:
        return None

    current = runs[-1]
    settled = _established_runs(runs)

    if settled:
        held = settled[-1]
        opening = settled[0].value
        established = abs(held.value - current.value) < _EPS
    else:
        # Nothing has settled yet -- an opening still inside the noise floor.
        # The honest reading is "no migration established", not a move
        # measured between two levels neither of which has held.
        held = current
        opening = current.value
        established = False

    total = len(frames)
    # Quiet minutes are the frames the engine declined to name a pin for -- a
    # real and frequent answer, not missing data, so it is reported rather
    # than silently folded into any held count.
    quiet = sum(1 for row in frames if _to_float(row.get("pin_strike")) is None)

    return PinStability(
        current_pin=current.value,
        current_since=current.start,
        current_samples=current.samples,
        current_established=established,
        held_pin=held.value,
        held_since=held.start,
        held_samples=held.samples,
        session_open_pin=opening,
        net_migration=held.value - opening,
        distinct_values=len(settled) if settled else 1,
        quiet_samples=quiet,
        total_samples=total,
    )
