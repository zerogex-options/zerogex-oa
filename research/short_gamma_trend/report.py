"""Plain-text report: the verdict first, then only what explains it."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from research.msi_regime_excursion.excursion import ET
from research.short_gamma_trend.outcomes import REST
from research.short_gamma_trend.study import (
    ALL_HORIZONS,
    ITERATIONS,
    LABELS,
    PRIMARY_HORIZON,
    BuildCounts,
    Estimate,
    Row,
    StudyResult,
)


def _h(h: object) -> str:
    return "rest" if h == REST else f"{h}m"


def _num(v: Optional[float], fmt: str = "+.2f") -> str:
    return "n/a" if v is None else format(v, fmt)


def _ci(e: Estimate, fmt: str = "+.1f") -> str:
    if e.value is None:
        return "n/a"
    if e.lo is None or e.hi is None:
        return format(e.value, fmt)
    return f"{e.value:{fmt}} [{e.lo:{fmt}},{e.hi:{fmt}}]"


def render(
    result: StudyResult,
    counts: BuildCounts,
    spans: dict[str, tuple[Optional[datetime], Optional[datetime], int]],
    *,
    iterations: int = ITERATIONS,
) -> str:
    lines: list[str] = []
    add = lines.append
    add("SHORT-GAMMA TREND STATE: REPLAY AGAINST HISTORY")
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

    v = result.verdict
    add(f"VERDICT: {v.decision}")
    for reason in v.reasons:
        add(f"  {reason}")
    add("")

    width = max(len(lab.title) for lab in LABELS)
    add("Directional excess over drift, bps, 95% interval (pooled)")
    add(
        f"  {'':<{width}}  {'calls':>7} {'days':>4}  "
        + "  ".join(f"{_h(h):<18}" for h in ALL_HORIZONS)
    )
    for lab in LABELS:
        c30 = result.cells[(lab.key, PRIMARY_HORIZON, "pooled")]
        row = "  ".join(
            f"{_ci(result.cells[(lab.key, h, 'pooled')].excess_bps):<18}" for h in ALL_HORIZONS
        )
        add(f"  {lab.title:<{width}}  {c30.n_calls:>7,} {c30.n_sessions:>4}  {row}")
    add("")

    add(f"At {PRIMARY_HORIZON} minutes (pooled)")
    add(f"  {'':<{width}}  {'hit rate':<22}{'trend quality, bps':<22}{'range vs all minutes':<22}")
    for lab in LABELS:
        c = result.cells[(lab.key, PRIMARY_HORIZON, "pooled")]
        add(
            f"  {lab.title:<{width}}  {_ci(c.hit_rate, '.1%'):<22}"
            f"{_ci(c.quality_bps):<22}{_ci(c.range_ratio, '.2f'):<22}"
        )
    add("")

    add(f"Per symbol, {PRIMARY_HORIZON}m excess, bps")
    for lab in LABELS:
        parts = []
        for sym in result.symbols:
            c = result.cells[(lab.key, PRIMARY_HORIZON, sym)]
            parts.append(f"{sym} {_ci(c.excess_bps)} ({c.n_calls:,})")
        add(f"  {lab.title:<{width}}  " + "   ".join(parts))
    add("")

    add("Flow votes minus price momentum alone (does the vote add anything?)")
    add("  " + "   ".join(f"{_h(h)} {_ci(result.flow_minus_momentum[h])}" for h in ALL_HORIZONS))
    add("")

    add("How often, and how jumpy")
    freq_width = max(len(lab.title) for lab in LABELS if not lab.entries_only)
    add(
        f"  {'':<{freq_width}}  "
        + "".join(f"{sym + ' min%':>10}" for sym in result.symbols)
        + f"{'days':>6}{'episodes':>10}{'median min':>12}"
    )
    titles = {lab.key: lab.title for lab in LABELS}
    for f in result.frequencies:
        add(
            f"  {titles[f.label]:<{freq_width}}  "
            + "".join(f"{f.share_of_minutes.get(sym, 0.0):>10.1%}" for sym in result.symbols)
            + f"{f.sessions:>6}{f.episodes:>10}"
            + f"{_num(f.median_episode_min, '.0f'):>12}"
        )
    add(
        "  panel label changes per symbol-session: "
        f"now {_num(result.changes_current, '.1f')}, "
        f"with the candidate {_num(result.changes_candidate, '.1f')}"
    )
    return "\n".join(lines)


def render_day(rows: Sequence[Row], day: str) -> str:
    """One session's states under the current rule and the candidate, as runs."""
    lines = [f"Session {day}: current rule vs candidate (flow), runs of identical states"]
    by_symbol: dict[str, list[Row]] = {}
    for row in rows:
        if row.session.isoformat() == day:
            by_symbol.setdefault(row.symbol, []).append(row)
    if not by_symbol:
        lines.append("  no scored minutes that day")
        return "\n".join(lines)
    for sym in sorted(by_symbol):
        lines.append(f"  {sym}")
        day_rows = sorted(by_symbol[sym], key=lambda r: r.ts)
        start = 0
        for i in range(1, len(day_rows) + 1):
            if i < len(day_rows) and (
                day_rows[i].states["current"] == day_rows[start].states["current"]
                and day_rows[i].states["flow"] == day_rows[start].states["flow"]
            ):
                continue
            first, last = day_rows[start], day_rows[i - 1]
            lines.append(
                f"    {first.ts.astimezone(ET):%H:%M}-{last.ts.astimezone(ET):%H:%M}  "
                f"{first.states['current']:<13} -> {first.states['flow']:<13} "
                f"px {first.out.entry:.2f} -> {last.out.entry:.2f}"
            )
            start = i
    return "\n".join(lines)
