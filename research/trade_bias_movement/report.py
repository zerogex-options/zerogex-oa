"""Plain-text report: the four verdicts first, then only what explains them."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from research.msi_regime_excursion.excursion import ET
from research.short_gamma_trend.outcomes import REST
from research.short_gamma_trend.study import ALL_HORIZONS, BuildCounts, Estimate
from research.trade_bias_movement.study import (
    ALPHA,
    BY_TIME_OF_DAY,
    BY_SYMBOL,
    COMPARABLE,
    ITERATIONS,
    MATERIAL,
    MIN_SESSIONS,
    PRIMARY_HORIZON,
    RatioCell,
    StudyResult,
    matches_copy,
)


def _h(h: object) -> str:
    return "rest" if h == REST else f"{h}m"


def _ci(e: Estimate) -> str:
    if e.value is None:
        return "n/a"
    if e.lo is None or e.hi is None:
        return f"{e.value:.2f}"
    return f"{e.value:.2f} [{e.lo:.2f},{e.hi:.2f}]"


def _cell(c: RatioCell) -> str:
    """A context cell: its interval and minutes, or only its value and day
    count when its minutes span too few sessions for an interval to mean
    anything."""
    if c.sessions < MIN_SESSIONS:
        value = "n/a" if c.ratio.value is None else f"{c.ratio.value:.2f}"
        days = f"{c.sessions} day" + ("" if c.sessions == 1 else "s")
        return f"{value} ({c.minutes:,} min, {days})"
    return f"{_ci(c.ratio)} ({c.minutes:,})"


def render(
    result: StudyResult,
    counts: BuildCounts,
    spans: dict[str, tuple[Optional[datetime], Optional[datetime], int]],
    *,
    iterations: int = ITERATIONS,
) -> str:
    lines: list[str] = []
    add = lines.append
    add("TRADE BIAS STATES: DO THEY SAY HOW FAR PRICE WILL MOVE?")
    add(
        f"Symbols {', '.join(result.symbols)} · {result.n_dates} sessions · "
        f"{iterations:,} date-block resamples"
    )
    add("")
    add("Archive (trade_bias_scores, swing tenor)")
    for sym, (first, last, n) in spans.items():
        span = (
            f"{first.astimezone(ET):%Y-%m-%d} -> {last.astimezone(ET):%Y-%m-%d}"
            if first and last
            else "empty"
        )
        add(f"  {sym:<4} {span}  {n:,} readings (all hours)")
    add(
        f"Scored cash-session minutes {counts.rows:,} of {counts.readings:,} · "
        f"no inputs {counts.no_inputs:,} · no entry bar {counts.no_entry_bar:,}"
    )
    if counts.stored_compared:
        agree = counts.stored_agree / counts.stored_compared
        add(
            f"Plumbing: replayed state matches the stored one for {agree:.2%} "
            f"of {counts.stored_compared:,} minutes"
        )
        top = sorted(counts.mismatches.items(), key=lambda kv: -kv[1])[:4]
        if top:
            add("  mismatches (stored -> replayed): " + ", ".join(f"{k} {v}" for k, v in top))
    add("")

    groups = result.groups
    width = max(len(g.title) for g in groups)
    add("VERDICTS")
    add(
        f"  How far price typically travelled over the next {PRIMARY_HORIZON} minutes (high to "
        "low), as a multiple"
    )
    add("  of other minutes with the same symbol, the same time of day and the same movement")
    add("  over the last 5 minutes, the last 30 minutes and the session so far. SPY + SPX pooled,")
    add(
        f"  95% interval. p is judged with Holm's correction at {ALPHA:.0%} across the four; "
        f"MOVES MORE / LESS needs {MATERIAL:.0%}."
    )
    add(
        f"  {'state':<{width}}  {'copy says':<10} {'minutes':>8} {'days':>4}  "
        f"{'ratio':<20}{'p':<9}{'verdict':<20}matches the copy?"
    )
    for g in groups:
        c = g.primary()
        p = f"{g.p_primary:.4f}" if g.p_primary is not None else "-"
        decision = g.verdict.decision if g.verdict else ""
        add(
            f"  {g.title:<{width}}  {g.claim or '-':<10} {c.minutes:>8,} {c.sessions:>4}  "
            f"{_ci(c.ratio):<20}{p:<9}{decision:<20}{matches_copy(g.claim, decision)}"
        )
    add("")
    add("Everything below explains a verdict and moves none. None of it is corrected for")
    add("multiple comparisons: about 1 interval in 20 excludes 1.00 by chance alone. A cell whose")
    add(
        f"minutes span fewer than {MIN_SESSIONS} sessions shows its day count instead of an "
        "interval."
    )
    add("")

    add(
        f"How much of it is the clock and the tape? {PRIMARY_HORIZON}-minute range, pooled, "
        "against the other"
    )
    add("states' minutes")
    add(
        f"  {'':<{width}}  {'same symbol':<28}{'+ same time of day':<28}"
        "+ same recent movement (verdict)"
    )
    for g in groups:
        add(
            f"  {g.title:<{width}}  "
            + "".join(
                f"{_cell(g.cells[(basis, PRIMARY_HORIZON, 'pooled')]):<28}"
                for basis in (BY_SYMBOL, BY_TIME_OF_DAY)
            )
            + _cell(g.cells[(COMPARABLE, PRIMARY_HORIZON, "pooled")])
        )
    add("")

    add("Every horizon: range vs comparable minutes (pooled)")
    add(f"  {'':<{width}}  " + "".join(f"{_h(h):<26}" for h in ALL_HORIZONS))
    for g in groups:
        add(
            f"  {g.title:<{width}}  "
            + "".join(f"{_cell(g.cells[(COMPARABLE, h, 'pooled')]):<26}" for h in ALL_HORIZONS)
        )
    add("")

    add(f"Per symbol: {PRIMARY_HORIZON}-minute range vs comparable minutes")
    add(f"  {'':<{width}}  " + "".join(f"{sym:<26}" for sym in result.symbols))
    for g in groups:
        add(
            f"  {g.title:<{width}}  "
            + "".join(
                f"{_cell(g.cells[(COMPARABLE, PRIMARY_HORIZON, sym)]):<26}"
                for sym in result.symbols
            )
        )
    add("")

    context_width = max(
        width, *(len(r.title) for r in (*result.states, *result.regimes, *result.recent))
    )
    for title, rows in (
        ("Each state on its own", result.states),
        ("The panel's gamma regime (production's definition)", result.regimes),
    ):
        add(f"{title}: {PRIMARY_HORIZON}-minute range against the other minutes")
        add(f"  {'':<{context_width}}  {'same symbol':<26}same time of day and recent movement")
        for r in rows:
            add(
                f"  {r.title:<{context_width}}  {_cell(r.cells[BY_SYMBOL]):<26}"
                f"{_cell(r.cells[COMPARABLE])}"
            )
        add("")

    add(
        f"Recent movement alone: {PRIMARY_HORIZON}-minute range. The first half hour against "
        "the rest of"
    )
    add("the day; each fifth of the last 30 minutes' range against the other fifths at the same")
    add("half hour. The size of what the adjustment removes.")
    for r in result.recent:
        basis = BY_SYMBOL if BY_SYMBOL in r.cells else BY_TIME_OF_DAY
        add(f"  {r.title:<{context_width}}  {_cell(r.cells[basis])}")
    add("")

    add("How often each state is on the panel: share of scored minutes")
    add(f"  {'':<{width}}  " + "".join(f"{sym:<10}" for sym in result.symbols))
    for g in groups:
        add(
            f"  {g.title:<{width}}  "
            + "".join(f"{g.share.get(sym, 0.0):<10.1%}" for sym in result.symbols)
        )
    return "\n".join(line.rstrip() for line in lines)
