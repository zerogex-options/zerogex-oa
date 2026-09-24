"""Print the Playbook's graded track record and the entry bar each pattern
has earned per symbol. Read-only.

    make playbook-record                 # last 90 days, every symbol
    make playbook-record DAYS=30 SYMBOL=SPY

Columns, per (pattern, symbol, direction), counting each idea once:

  ideas         graded ideas (a Card and its re-issues count as one)
  won/lost/flat target first / stop first / neither before the hold ran out
  avg R         average result in units of the Card's own risk (entry to
                stop); +1.0 means it made what it risked, -1.0 a full stop
  already moved median move price had already made the Card's way in the 30
                minutes before it; large values mean the Card was late
  repeats       extra Cards the old engine issued for an idea already live
  held back     ideas the entry bar kept off the site (still graded)
  status / bar  what the adaptive gate does with the pattern there now
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional

from src import config
from src.database.connection import db_connection
from src.signals.playbook import adaptive_gate
from src.signals.playbook.adaptive_gate import GRADED_OUTCOMES

_GRADED = ", ".join(f"'{o}'" for o in GRADED_OUTCOMES)


def _rows_sql(days: int, symbol: Optional[str]) -> tuple[str, tuple]:
    where = "issued_at > NOW() - (%s || ' days')::interval"
    params: tuple = (str(int(days)),)
    if symbol:
        where += " AND underlying = %s"
        params += (symbol.upper(),)
    first = f"NOT is_repeat AND outcome IN ({_GRADED})"
    return (
        f"""
        SELECT pattern, underlying, direction,
               COUNT(*) FILTER (WHERE {first}) AS ideas,
               COUNT(*) FILTER (WHERE NOT is_repeat AND outcome = 'target_hit') AS won,
               COUNT(*) FILTER (WHERE NOT is_repeat AND outcome = 'stop_hit') AS lost,
               COUNT(*) FILTER (WHERE NOT is_repeat AND outcome = 'time_exit') AS flat,
               AVG(r_multiple) FILTER (WHERE {first}) AS avg_r,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY prior_move_pct)
                   FILTER (WHERE NOT is_repeat AND prior_move_pct IS NOT NULL) AS moved,
               COUNT(*) FILTER (WHERE is_repeat) AS repeats,
               COUNT(*) FILTER (WHERE card_id IS NULL) AS held_back,
               COUNT(*) FILTER (WHERE outcome = 'pending') AS pending
        FROM playbook_card_outcomes
        WHERE {where}
        GROUP BY pattern, underlying, direction
        ORDER BY underlying, pattern, direction
        """,
        params,
    )


def _cohorts_sql(days: int, symbol: Optional[str]) -> tuple[str, tuple]:
    """First Cards vs re-issues vs held-back ideas, across every pattern."""
    where = f"issued_at > NOW() - (%s || ' days')::interval AND outcome IN ({_GRADED})"
    params: tuple = (str(int(days)),)
    if symbol:
        where += " AND underlying = %s"
        params += (symbol.upper(),)
    return (
        f"""
        SELECT CASE WHEN card_id IS NULL THEN 'held back (not published)'
                    WHEN is_repeat THEN 'repeat Cards (idea already live)'
                    ELSE 'first Card of each idea' END AS cohort,
               COUNT(*) AS n,
               COUNT(*) FILTER (WHERE outcome = 'target_hit') AS won,
               COUNT(*) FILTER (WHERE outcome = 'stop_hit') AS lost,
               AVG(r_multiple) AS avg_r,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY prior_move_pct)
                   FILTER (WHERE prior_move_pct IS NOT NULL) AS moved
        FROM playbook_card_outcomes
        WHERE {where}
        GROUP BY 1
        ORDER BY 1
        """,
        params,
    )


def _pct(value) -> str:
    return "—" if value is None else f"{float(value) * 100:+.2f}%"


def _r(value) -> str:
    return "—" if value is None else f"{float(value):+.2f}"


def report(conn, *, days: int, symbol: Optional[str]) -> str:
    cur = conn.cursor()
    store = adaptive_gate.load_store(conn)
    sql, params = _rows_sql(days, symbol)
    cur.execute(sql, params)
    rows = cur.fetchall()
    lines = [
        f"Playbook track record, last {days} days"
        + (f", {symbol.upper()}" if symbol else "")
        + f" (gate {'ON' if config.PLAYBOOK_ADAPTIVE_GATE_ENABLED else 'OFF'},"
        f" one Card per idea {'ON' if config.PLAYBOOK_ONE_CARD_PER_IDEA else 'OFF'})",
        "",
        f"  {'pattern':<28}{'sym':<6}{'dir':<9}{'ideas':>6}{'won':>5}{'lost':>5}{'flat':>5}"
        f"{'avg R':>7}{'moved':>9}{'repeats':>9}{'held':>6}{'pend':>6}  status      bar",
    ]
    if not rows:
        lines.append("  (nothing graded yet: run `make playbook-grade`)")
    for (
        pattern,
        underlying,
        direction,
        n,
        won,
        lost,
        flat,
        avg_r,
        moved,
        repeats,
        held,
        pending,
    ) in rows:
        verdict = adaptive_gate.assess(pattern, underlying, direction, store)
        bar = "paused" if verdict.paused else f"{verdict.bar:.2f}"
        lines.append(
            f"  {pattern:<28}{underlying:<6}{direction[:8]:<9}{n:>6}{won:>5}{lost:>5}{flat:>5}"
            f"{_r(avg_r):>7}{_pct(moved):>9}{repeats:>9}{held:>6}{pending:>6}"
            f"  {verdict.status:<11} {bar}"
        )

    sql, params = _cohorts_sql(days, symbol)
    cur.execute(sql, params)
    cohorts = cur.fetchall()
    if cohorts:
        lines += ["", "  Across all patterns:"]
        for cohort, n, won, lost, avg_r, moved in cohorts:
            win = f"{won / n:.0%}" if n else "—"
            lines.append(
                f"    {cohort:<34} {n:>5} graded, {won} won / {lost} lost ({win}),"
                f" avg {_r(avg_r)}R, median already-moved {_pct(moved)}"
            )
    where = "issued_at > NOW() - (%s || ' days')::interval"
    params = (str(int(days)),)
    if symbol:
        where += " AND underlying = %s"
        params += (symbol.upper(),)
    cur.execute(
        f"""
        SELECT outcome, COUNT(*) FROM playbook_card_outcomes
        WHERE {where} AND outcome NOT IN ({_GRADED}, 'pending')
        GROUP BY outcome ORDER BY COUNT(*) DESC
        """,
        params,
    )
    set_aside = cur.fetchall()
    if set_aside:
        lines.append(
            "    set aside (not counted): "
            + ", ".join(f"{count} {outcome.replace('_', ' ')}" for outcome, count in set_aside)
        )
    lines += [
        "",
        "  avg R: result in units of the Card's own risk (+1 = made what it risked,",
        "  -1 = full stop). moved: how far price had already gone the Card's way in the",
        "  30 minutes before it. status: proven/positive = lower bar, lagging = higher",
        "  bar, paused = no Cards (still graded), learning = too few ideas to judge.",
        "  mispriced: an at-market Card that quoted a price nobody traded (the API",
        "  quoted VWAP as the price until 2026-09-24).",
    ]
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Playbook graded track record (read-only)")
    parser.add_argument("--days", type=int, default=config.PLAYBOOK_ADAPTIVE_LOOKBACK_DAYS)
    parser.add_argument("--symbol", default=None)
    args = parser.parse_args(argv)
    with db_connection() as conn:
        print(report(conn, days=args.days, symbol=args.symbol))
    return 0


if __name__ == "__main__":
    sys.exit(main())
