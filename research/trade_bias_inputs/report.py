"""Plain-text report: the nine verdicts first, then only what explains them."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from research.msi_regime_excursion.excursion import ET
from research.short_gamma_trend.outcomes import REST
from research.short_gamma_trend.study import ALL_HORIZONS, BuildCounts, Cell, Estimate
from research.trade_bias_inputs.study import (
    ALPHA,
    ITERATIONS,
    MIN_SESSIONS,
    PRIMARY_HORIZON,
    PRIOR_MOVE_SPLITS,
    REGIMES,
    StudyResult,
)

MOMENTUM_TITLE = "price momentum alone"


def _h(h: object) -> str:
    return "rest" if h == REST else f"{h}m"


def _ci(e: Estimate, fmt: str = "+.1f") -> str:
    if e.value is None:
        return "n/a"
    if e.lo is None or e.hi is None:
        return format(e.value, fmt)
    return f"{e.value:{fmt}} [{e.lo:{fmt}},{e.hi:{fmt}}]"


def _cell_n(c: Cell) -> str:
    """A context cell: its interval and minutes, or, when its minutes span
    fewer sessions than a verdict needs, only the value and the day count --
    a date-block resample of a handful of days measures almost nothing, and
    its interval would look precise."""
    if c.n_sessions < MIN_SESSIONS:
        v = c.excess_bps.value
        value = "n/a" if v is None else f"{v:+.1f}"
        days = f"{c.n_sessions} day" + ("" if c.n_sessions == 1 else "s")
        return f"{value} ({c.n_calls:,} min, {days})"
    return f"{_ci(c.excess_bps)} ({c.n_calls:,})"


def render(
    result: StudyResult,
    counts: BuildCounts,
    spans: dict[str, tuple[Optional[datetime], Optional[datetime], int]],
    *,
    iterations: int = ITERATIONS,
) -> str:
    lines: list[str] = []
    add = lines.append
    add("TRADE BIAS INPUTS: WHICH ONES PREDICT DIRECTION")
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

    width = max(len(MOMENTUM_TITLE), *(len(r.title) for r in result.inputs))
    rule_width = max(len(r.rule) for r in result.inputs)
    add("VERDICTS")
    add(
        f"  {PRIMARY_HORIZON}-minute excess over drift, SPY + SPX pooled, bps, 95% interval. "
        f"p is judged with Holm's correction at {ALPHA:.0%} across the nine."
    )
    add(
        f"  {'input':<{width}}  {'call':<{rule_width}}  {'calls':>7} {'days':>4}  "
        f"{'excess':<20}{'p':<9}verdict"
    )
    for r in result.inputs:
        c = r.primary()
        p = f"{r.p_primary:.4f}" if r.p_primary is not None else "-"
        add(
            f"  {r.title:<{width}}  {r.rule:<{rule_width}}  {c.n_calls:>7,} {c.n_sessions:>4}  "
            f"{_ci(c.excess_bps):<20}{p:<9}{r.verdict.decision if r.verdict else ''}"
        )
    m = result.momentum[(PRIMARY_HORIZON, "pooled")]
    add(
        f"  {MOMENTUM_TITLE:<{width}}  {'prior 30m move of 5+ bps':<{rule_width}}  "
        f"{m.n_calls:>7,} {m.n_sessions:>4}  {_ci(m.excess_bps):<20}"
        "reference only, not one of the nine"
    )
    add("")

    add(
        "Everything below explains a verdict and moves none. None of it is corrected for "
        "multiple comparisons:"
    )
    add("about 1 interval in 20 excludes zero by chance alone. A cell whose minutes span fewer")
    add(f"than {MIN_SESSIONS} sessions shows its day count instead of an interval.")
    add("")
    add("Every horizon: excess over drift, bps, 95% interval (pooled)")
    add(f"  {'':<{width}}  " + "".join(f"{_h(h):<20}" for h in ALL_HORIZONS))
    for r in result.inputs:
        row = "".join(f"{_ci(r.cells[(h, 'pooled')].excess_bps):<20}" for h in ALL_HORIZONS)
        add(f"  {r.title:<{width}}  {row}")
    row = "".join(f"{_ci(result.momentum[(h, 'pooled')].excess_bps):<20}" for h in ALL_HORIZONS)
    add(f"  {MOMENTUM_TITLE:<{width}}  {row}")
    add("")

    add(f"At {PRIMARY_HORIZON} minutes")
    add(
        f"  {'':<{width}}  {'hit rate':<22}"
        + "".join(f"{sym + ' excess':<20}" for sym in result.symbols)
        + "minus momentum"
    )
    for r in result.inputs:
        c = r.primary()
        add(
            f"  {r.title:<{width}}  {_ci(c.hit_rate, '.1%'):<22}"
            + "".join(
                f"{_ci(r.cells[(PRIMARY_HORIZON, sym)].excess_bps):<20}" for sym in result.symbols
            )
            + _ci(r.minus_momentum)
        )
    add(
        f"  {MOMENTUM_TITLE:<{width}}  {_ci(m.hit_rate, '.1%'):<22}"
        + "".join(
            f"{_ci(result.momentum[(PRIMARY_HORIZON, sym)].excess_bps):<20}"
            for sym in result.symbols
        )
    )
    add("")

    add(
        f"By reading: {PRIMARY_HORIZON}-minute return minus drift, bps, 95% interval (minutes), "
        "pooled. Not a call: every minute in the band counts"
    )
    band_width = max(len(b.label) for r in result.inputs for b in r.bands)
    for r in result.inputs:
        add(f"  {r.title}")
        for b in r.bands:
            add(f"    {b.label:<{band_width}}  {_cell_n(b.cell)}")
    add("")

    add(
        f"By gamma regime: {PRIMARY_HORIZON}-minute excess over that regime's own drift, bps "
        "(calls)"
    )
    add(f"  {'':<{width}}  " + "".join(f"{name:<28}" for name in REGIMES))
    for r in result.inputs:
        if not r.regimes:
            add(f"  {r.title:<{width}}  n/a: it is the regime")
            continue
        add(f"  {r.title:<{width}}  " + "".join(f"{_cell_n(r.regimes[n]):<28}" for n in REGIMES))
    add(
        f"  {MOMENTUM_TITLE:<{width}}  "
        + "".join(f"{_cell_n(result.momentum_regimes[n]):<28}" for n in REGIMES)
    )
    add("")

    add(
        f"With or against the prior 30-minute move: {PRIMARY_HORIZON}-minute excess over "
        "drift, bps (calls)"
    )
    add(
        "  A call that agrees with the move inherits whatever momentum is worth; 'against it' "
        "and 'no prior move' are the clean reads."
    )
    add(f"  {'':<{width}}  " + "".join(f"{name:<28}" for name in PRIOR_MOVE_SPLITS))
    for r in result.inputs:
        add(
            f"  {r.title:<{width}}  "
            + "".join(f"{_cell_n(r.prior_move[n]):<28}" for n in PRIOR_MOVE_SPLITS)
        )
    add("")

    add("Coverage: share of scored minutes with a reading / with a call")
    add(f"  {'':<{width}}  " + "".join(f"{sym:<18}" for sym in result.symbols))
    for r in result.inputs:
        add(
            f"  {r.title:<{width}}  "
            + "".join(f"{f'{r.available[s]:.1%} / {r.calling[s]:.1%}':<18}" for s in result.symbols)
        )
    return "\n".join(line.rstrip() for line in lines)
