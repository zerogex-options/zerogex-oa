"""Promotion and retirement policy for the strategy catalog.

Two gates, deliberately asymmetric.

**Promotion** is cheap to attempt and easy to reverse: a strategy earns a live
capital sleeve by clearing a measurable bar on a screen. Unchanged from the
gate the fleet already ran under.

**Retirement is expensive and near-irreversible**, because a retired strategy
stops being refined and the thesis stops being tested. So the bar is set where
a wrong call is affordable: five years of history, repeated independent
retuning, and a conclusive absence of edge every time. Anything short of that
leaves the strategy in RESEARCH, where it keeps getting worked on.

The practical consequence today is that **nothing is retirement-eligible**.
``option_chains`` is pruned at ``DATA_RETENTION_DAYS`` (60-90 days) and the
durable archive only began accumulating in spring 2026, so the deepest screen
any strategy has is 90 days — about 5% of the evidence this policy requires.
That is the policy working, not a bug: the 2026-08-09 fleet shelving rested on
a 45-day window, which is nowhere near enough to write a thesis off. Reaching
the bar needs the deep-history backfill costed in
``docs/design/historical-options-data-vendors.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from src.strategies.models import Stage, StrategyEntry, Verdict

# ── Promotion gate (RESEARCH/CANDIDATE -> VALIDATED) ───────────────────
#: Profit factor a screen must clear.
PROMOTION_MIN_PROFIT_FACTOR = 1.1
#: Round-trips a screen must produce. Below this, a good number is noise.
PROMOTION_MIN_TRADES = 20

# ── Retirement gate (-> RETIRED) ───────────────────────────────────────
#: Calendar days of history the deepest screen must cover. Five years is the
#: ask: long enough to span a genuine variety of regimes (a COVID-style vol
#: shock, a trending bear, a low-vol grind) rather than one quarter's weather.
RETIREMENT_MIN_HISTORY_DAYS = 5 * 365
#: Distinct tuning generations that must each have concluded NO_EDGE. One
#: failure is a parameter choice; three independent re-parameterisations
#: failing is a statement about the thesis.
RETIREMENT_MIN_TUNING_GENERATIONS = 3
#: Round-trips accumulated across all conclusive screens. A five-year window
#: that only ever produced 40 trades has not tested the mechanism.
RETIREMENT_MIN_TRADES = 200


@dataclass(frozen=True)
class Eligibility:
    """Whether a stage transition is allowed, and what is still missing."""

    allowed: bool
    #: Human-readable reasons the gate is not met. Empty when ``allowed``.
    blockers: Tuple[str, ...] = ()

    def __bool__(self) -> bool:  # pragma: no cover - convenience
        return self.allowed

    @property
    def reason(self) -> str:
        return "; ".join(self.blockers) if self.blockers else "eligible"


def can_promote(entry: StrategyEntry) -> Eligibility:
    """Whether ``entry`` has a screen that clears the promotion gate.

    Reads the strategy's own research log rather than a passed-in number, so
    the decision is auditable after the fact: the run that justified promotion
    stays in the catalog.
    """
    blockers: List[str] = []
    if entry.stage in (Stage.RETIRED, Stage.SUPERSEDED):
        blockers.append(f"stage is {entry.stage.value}")

    qualifying = [
        r
        for r in entry.research
        if r.verdict is Verdict.EDGE
        and r.trades >= PROMOTION_MIN_TRADES
        and (r.profit_factor or 0.0) >= PROMOTION_MIN_PROFIT_FACTOR
        and (r.expectancy is None or r.expectancy > 0)
    ]
    if not qualifying:
        best = max((r.profit_factor or 0.0) for r in entry.research) if entry.research else 0.0
        most = max((r.trades for r in entry.research), default=0)
        blockers.append(
            f"no screen with PF >= {PROMOTION_MIN_PROFIT_FACTOR} and "
            f">= {PROMOTION_MIN_TRADES} trades (best PF {best:.2f}, most trades {most})"
        )
    return Eligibility(not blockers, tuple(blockers))


def can_retire(entry: StrategyEntry) -> Eligibility:
    """Whether ``entry`` has exhausted its thesis under the retirement policy.

    Requires, in order of how often each actually bites today:

    1. Five years of history behind the deepest screen.
    2. At least three distinct tuning generations, each concluding NO_EDGE.
    3. Enough accumulated trades for those screens to mean something.
    4. No screen that ever found an edge.

    UNDERPOWERED and INSUFFICIENT runs are ignored throughout — a screen whose
    gates never opened, or that produced five trades, has not tested anything.
    """
    blockers: List[str] = []

    depth = entry.deepest_window_days
    if depth < RETIREMENT_MIN_HISTORY_DAYS:
        blockers.append(
            f"deepest screen covers {depth}d, needs {RETIREMENT_MIN_HISTORY_DAYS}d "
            f"({RETIREMENT_MIN_HISTORY_DAYS / 365:.0f}y)"
        )

    generations = entry.conclusive_tuning_generations
    if len(generations) < RETIREMENT_MIN_TUNING_GENERATIONS:
        blockers.append(
            f"{len(generations)} conclusive tuning generation(s), "
            f"needs {RETIREMENT_MIN_TUNING_GENERATIONS}"
        )

    conclusive_trades = sum(r.trades for r in entry.research if r.is_conclusive_against)
    if conclusive_trades < RETIREMENT_MIN_TRADES:
        blockers.append(
            f"{conclusive_trades} trades across conclusive screens, "
            f"needs {RETIREMENT_MIN_TRADES}"
        )

    if entry.has_edge_evidence:
        blockers.append("a prior screen measured an edge — refine, do not retire")

    return Eligibility(not blockers, tuple(blockers))


def retirement_shortfall(entry: StrategyEntry) -> float:
    """How far along the history requirement ``entry`` is, in [0, 1].

    Surfaced by the audit so "not eligible" carries a sense of scale — 0.05
    reads very differently from 0.9.
    """
    if RETIREMENT_MIN_HISTORY_DAYS <= 0:  # pragma: no cover - defensive
        return 1.0
    return min(1.0, entry.deepest_window_days / RETIREMENT_MIN_HISTORY_DAYS)


def validate_stage(entry: StrategyEntry) -> Optional[str]:
    """Check a catalog entry's stage against its own evidence.

    Returns an error string when the declared stage is not supportable, else
    None. Run over the whole catalog by the integrity test, so the catalog
    cannot quietly drift into claiming a strategy is validated or retired
    without the evidence to back it.
    """
    if entry.stage is Stage.VALIDATED and not can_promote(entry).allowed:
        return f"{entry.id}: stage=validated but {can_promote(entry).reason}"
    if entry.stage is Stage.RETIRED:
        if entry.retirement is None:
            return f"{entry.id}: stage=retired without a Retirement record"
        verdict = can_retire(entry)
        if not verdict.allowed:
            return f"{entry.id}: stage=retired but {verdict.reason}"
    if entry.stage is Stage.SUPERSEDED and not entry.superseded_by:
        return f"{entry.id}: stage=superseded without superseded_by"
    if entry.retirement is not None and entry.stage is not Stage.RETIRED:
        return f"{entry.id}: carries a Retirement record but stage={entry.stage.value}"
    return None
