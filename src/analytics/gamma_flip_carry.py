"""Per-bar gamma flip for ``gamma_regime_5min``, carried across missing rows.

The flip level a 5-minute bar reports comes from ``gex_summary``, which is
written on its own cadence by the analytics cycle rather than on the bar grid.
The original per-bar lookup asked for the newest ``gex_summary`` row inside the
bar's own five minutes and took ``None`` for an answer:

    SELECT gamma_flip_point FROM gex_summary
    WHERE underlying = %(symbol)s
      AND timestamp >= %(bar_start)s AND timestamp < %(bar_end)s
    ORDER BY timestamp DESC LIMIT 1

That is correct only while the cycle never falls below one write per five
minutes. A slow cycle, a restart or an upstream stall empties one window, the
bar is written with ``gamma_flip`` NULL, and NULL here is NOT a gap marker --
:mod:`src.analytics.flip_cushion` reads it as ``STATE_NO_FLIP``, the Gamma
Weather panel says "there is no gamma flip in the profile", and
``src.tools.gamma_weather_base_rates`` scores the session as having had no
cushion. A missing row and a one-signed gamma profile become the same reading,
and nothing says which one happened.

The two cases, and why only one of them may carry
------------------------------------------------
* **No ``gex_summary`` row in the window.** Nothing was measured. The profile
  did not change because the writer was late; the last level measured is still
  the best available answer, so it is carried forward.
* **A row exists and its ``gamma_flip_point`` is NULL.** Something WAS
  measured: the dealer-gamma profile is one-signed and has no crossing at all.
  Carrying a stale level over that would assert a boundary the profile does not
  have, so the NULL is written through unchanged.

``... ORDER BY timestamp DESC LIMIT 1`` cannot tell them apart -- it returns no
row in the first case and a NULL in the second, and ``cursor.fetchone()``
flattens both to ``None``. This module keeps them apart by grouping rather than
limiting: :data:`GAMMA_FLIP_OBSERVATIONS_SQL` emits one row per bar that HAS a
source row, so PRESENCE in the result set is the measurement and the VALUE
(possibly NULL) is what was measured.

Scope and causality
-------------------
Carry never crosses a session boundary: both the bar grid and the source query
are bounded by the session window the caller resolves, so the first bars of a
session have nothing behind them to carry and stay NULL -- correctly, since
yesterday's flip is not a reading about today.

:func:`resolve_session_flips` walks the grid forward and each bar sees only
bars at or before it. A bar therefore resolves to the same value whether it is
written live at its own moment or backfilled hours later by the gap-fill loop,
which is what makes a cold-started session identical to one written bar by bar.
:func:`summarize` is likewise computed from the resolved bars alone.

Why the carry is resolved here and not in the query
---------------------------------------------------
:mod:`src.hedging_flow_sql` does this shape in SQL, emulating
``LAST_VALUE(... IGNORE NULLS)`` with
``FIRST_VALUE(...) OVER (PARTITION BY <running count> ORDER BY ...)``, and the
same construction would work here over ``COUNT(observed) OVER (...)``. It is
not used, for one reason: the unit suite has no Postgres (the parity harnesses
that do are ``@pytest.mark.integration`` and skipped by default), so a carry
expressed in window functions could only be asserted as query TEXT. The
missing-row-versus-NULL distinction is the whole point of this module and it
has to be pinned by tests that actually run. The query stays the part SQL is
uniquely good at -- one grouped scan of the session -- and the distinction
lives where it can be executed.

That module's other lesson does hold: there is ONE query text here, shared by
the writer and by ``src.tools.gamma_flip_carry_healthcheck``, rather than a
second transcription that can drift.

Why carry-forward is not stored on the row
------------------------------------------
``gamma_regime_5min`` stores components and derives labels on read -- the
schema comment on ``gamma_flip`` says so, and it is why a retuned cushion
threshold reclassifies all of history instead of leaving every stored session
labelled by whatever rule was deployed that day. Provenance fits that rule
rather than breaking it: whether a bar had its own ``gex_summary`` row is a
fact still recorded in ``gex_summary``, which has been retention-EXEMPT since
2026-08-25, so "was this bar measured or carried?" is answerable from stored
components for as long as the bar itself exists. A boolean column would add
nothing a join cannot say, would be NULL across all history written before it,
and would need its own backfill to become readable -- while the derivation
reads the archive correctly today. Hence
``src.tools.gamma_flip_carry_healthcheck``, which does the join.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

#: A bar that has only just opened has not yet had time for a ``gex_summary``
#: row of its own, so a carry ONE bar deep is the ordinary state of the newest
#: bar on the first cycle after it opens and is not worth reporting. Two bars
#: deep means the previous bar's five minutes also passed with no row.
CARRY_NOTE_BARS = 2

#: Past this, a carry has stopped being a late write and become a stall worth
#: waking someone for: three bars is fifteen minutes of ``gex_summary``
#: silence, an order of magnitude past the cycle's cadence of roughly one row
#: per minute.
CARRY_WARN_BARS = 3

#: Newest ``gex_summary`` row per 5-minute bar of a session, for the bars that
#: HAVE one. Bars with no row are simply absent, which is the distinction the
#: old ``LIMIT 1`` lookup could not express: a bar present here with a NULL
#: ``gamma_flip_point`` measured a one-signed profile, and a bar missing here
#: measured nothing at all.
#:
#: The bucket expression, and the ``(ARRAY_AGG(... ORDER BY timestamp DESC))[1]``
#: pick of the newest row inside a bar, are both taken from
#: ``src.hedging_flow_sql`` / ``src.flow_series_sql``, so all three 5-minute
#: pipelines land on the same grid and resolve an in-bar tie the same way.
#:
#: That pick is load-bearing here in a way it is not there: it reports the
#: newest row's value INCLUDING when that value is NULL. An aggregate that
#: skipped NULLs -- ``MAX(gamma_flip_point)``, or any ``IGNORE NULLS``-flavoured
#: pick -- would resurrect an earlier crossing from the same bar and turn a
#: measured "the profile is one-signed" back into a level.
#:
#: The upper bound is ``session_end`` plus one bar because ``session_end`` is
#: itself the last bar's START, so its own five minutes must be in scope.
GAMMA_FLIP_OBSERVATIONS_SQL = """
    SELECT
        (date_trunc('hour', timestamp)
         + FLOOR(EXTRACT(MINUTE FROM timestamp)::int / 5)
           * INTERVAL '5 minutes') AS bar_start,
        (ARRAY_AGG(gamma_flip_point ORDER BY timestamp DESC))[1] AS gamma_flip_point
    FROM gex_summary
    WHERE underlying = %(symbol)s
      AND timestamp >= %(session_start)s
      AND timestamp <  %(session_end)s::timestamptz + INTERVAL '5 minutes'
    GROUP BY 1
    ORDER BY 1
"""


@dataclass(frozen=True)
class BarFlip:
    """One bar's resolved flip level, and where the level came from.

    ``flip`` is what the writer stores. ``measured`` says the bar had its own
    ``gex_summary`` row, in which case ``flip`` is that row's
    ``gamma_flip_point`` verbatim -- INCLUDING when it is ``None``, which is
    the profile genuinely having no crossing.

    When ``measured`` is false, ``carried_from`` names the bar whose reading is
    standing in and ``stale_bars`` is how far back that is. Both are ``None``
    and ``0`` for a bar with nothing behind it in the session: before the first
    ``gex_summary`` row of the day there is no level to carry, and reaching
    back past the open for one would import yesterday's market.
    """

    bar_start: datetime
    flip: Optional[float]
    measured: bool
    carried_from: Optional[datetime] = None
    stale_bars: int = 0

    @property
    def carried(self) -> bool:
        """The level is standing in from an earlier bar."""
        return self.carried_from is not None

    @property
    def unresolved(self) -> bool:
        """No reading at or before this bar in the session -- NULL is forced."""
        return not self.measured and self.carried_from is None


@dataclass(frozen=True)
class CarrySummary:
    """How much of a set of bars was measured, carried, or neither.

    ``max_stale_bars`` is the deepest carry in the set. Over a contiguous run
    of bars it is also the longest unbroken stretch of carried bars, since
    ``stale_bars`` counts up inside a stretch and resets at every measurement
    -- which is why one number serves both the live path (one bar per cycle,
    where a growing depth is the only sign of a stall) and a whole session.
    """

    bars: int
    measured: int
    carried: int
    unresolved: int
    max_stale_bars: int

    @property
    def degraded(self) -> bool:
        """Any bar that did not get its own reading."""
        return bool(self.carried or self.unresolved)

    @property
    def notable(self) -> bool:
        """Past the ordinary lag of a bar that has only just opened.

        ``unresolved`` is measured against the same depth: one or two bars of
        it at the top of a session is ``gex_summary`` not having written its
        first row yet, which is a warmup and not a fault.
        """
        return self.max_stale_bars >= CARRY_NOTE_BARS or self.unresolved >= CARRY_NOTE_BARS

    @property
    def sustained(self) -> bool:
        """Deep enough to be a stall rather than one late write."""
        return self.max_stale_bars >= CARRY_WARN_BARS or self.unresolved >= CARRY_WARN_BARS


def resolve_session_flips(
    bars: Sequence[datetime],
    observations: Iterable[Tuple[datetime, Optional[float]]],
) -> Dict[datetime, BarFlip]:
    """Resolve every bar of a session to a flip level and its provenance.

    ``bars`` is the session's 5-minute grid in chronological order.
    ``observations`` is ``(bar_start, gamma_flip_point)`` for the bars that had
    a ``gex_summary`` row, exactly as :data:`GAMMA_FLIP_OBSERVATIONS_SQL`
    emits them; a pair whose value is ``None`` is a measured absence of any
    crossing and is carried into the result as ``None`` rather than skipped.

    Strictly backward-looking. Bar *i* is decided by observations at bars
    ``<= i`` only, so resolving a prefix of the grid gives every bar in that
    prefix the same answer as resolving the whole session -- which is what
    makes a gap-filled bar identical to the live write it stands in for.
    Observations off the grid (a ``gex_summary`` row outside the session
    window) are ignored rather than snapped to a neighbouring bar.
    """
    observed = dict(observations)

    resolved: Dict[datetime, BarFlip] = {}
    held: Optional[Tuple[datetime, Optional[float]]] = None
    held_index: Optional[int] = None

    for index, bar_start in enumerate(bars):
        if bar_start in observed:
            raw = observed[bar_start]
            flip = float(raw) if raw is not None else None
            held = (bar_start, flip)
            held_index = index
            resolved[bar_start] = BarFlip(bar_start=bar_start, flip=flip, measured=True)
        elif held is not None and held_index is not None:
            source, flip = held
            resolved[bar_start] = BarFlip(
                bar_start=bar_start,
                flip=flip,
                measured=False,
                carried_from=source,
                stale_bars=index - held_index,
            )
        else:
            resolved[bar_start] = BarFlip(bar_start=bar_start, flip=None, measured=False)

    return resolved


def summarize(bars: Iterable[BarFlip]) -> CarrySummary:
    """Count a set of resolved bars by where their level came from."""
    total = measured = carried = unresolved = 0
    max_stale = 0

    for bar in bars:
        total += 1
        if bar.measured:
            measured += 1
        elif bar.carried:
            carried += 1
            max_stale = max(max_stale, bar.stale_bars)
        else:
            unresolved += 1

    return CarrySummary(
        bars=total,
        measured=measured,
        carried=carried,
        unresolved=unresolved,
        max_stale_bars=max_stale,
    )


def describe(summary: CarrySummary, bar_minutes: int = 5) -> str:
    """One line naming what was degraded and how deep it went.

    Written for a log record and for the healthcheck's per-session line, so
    the two report a degraded session in the same words.
    """
    parts: List[str] = [f"{summary.measured}/{summary.bars} bar(s) measured"]
    if summary.carried:
        parts.append(
            f"{summary.carried} carried forward "
            f"(deepest {summary.max_stale_bars} bar(s) = "
            f"{summary.max_stale_bars * bar_minutes}m)"
        )
    if summary.unresolved:
        parts.append(f"{summary.unresolved} with no earlier reading in the session")
    return ", ".join(parts)
