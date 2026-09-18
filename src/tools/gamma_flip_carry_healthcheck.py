"""Report how much of ``gamma_regime_5min.gamma_flip`` was measured vs carried.

The flip level on a 5-minute bar is read from whichever ``gex_summary`` row
landed inside that bar's five minutes. When none does -- a slow analytics
cycle, a restart, an upstream stall -- there is nothing to measure, and the
writer carries the last level measured earlier in the same session rather than
storing a NULL that :mod:`src.analytics.flip_cushion` would read as
``STATE_NO_FLIP`` and the Gamma Weather panel would report as "no gamma flip in
the profile" (see :mod:`src.analytics.gamma_flip_carry`).

Carrying is the right answer, but a session mostly built from carried levels is
a degraded reading dressed as a normal one, and the carry is silent by design.
This is the check that makes it loud. It is also the reason no
``gamma_flip_carried`` column was added: whether a bar had its own
``gex_summary`` row is still recorded in ``gex_summary``, which is
retention-EXEMPT, so the provenance is derivable for as long as the bar exists
-- and derivable RETROACTIVELY, which a column written from today forward would
not be.

Three counts per session, from the same resolution the writer uses:

* **measured** -- the bar had its own ``gex_summary`` row. Healthy.
* **carried** -- no row in the bar's own window; the level stands in from an
  earlier bar. ``deepest`` is how many bars back the furthest stand-in reached,
  which over a session is also the longest unbroken carried stretch.
* **unresolved** -- no row at or before the bar in the session, so NULL is
  forced. Normal for the first bar or two of a session; sustained, it means
  ``gex_summary`` never started writing.

``pre-fix NULL`` counts bars stored with a NULL ``gamma_flip`` that a carry
WOULD have filled. The writer cannot produce those any more, so a non-zero
count is history -- bars written before the carry existed, or while the column
itself was still missing -- rather than a live fault.

READ-ONLY. Runs SELECTs and a rollback; writes nothing.

Usage:
    python -m src.tools.gamma_flip_carry_healthcheck
    python -m src.tools.gamma_flip_carry_healthcheck --symbols SPY QQQ --sessions 20
    python -m src.tools.gamma_flip_carry_healthcheck --max-carry-bars 6
    python -m src.tools.gamma_flip_carry_healthcheck --json

Symbols default to ``ANALYTICS_UNDERLYINGS`` -- the universe the analytics
engine writes both tables for -- so a new underlying is covered the moment the
engine starts writing it.

Exit codes:
    0 -- every session got a reading of its own at least every
         ``--max-carry-bars`` bars.
    1 -- at least one session carried a level that far, OR went that far with
         no reading to carry at all (``unresolved``, which is the shape a
         session with NO gex_summary rows takes and is the worse failure).
    2 -- database connection or query error.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Sequence

import pytz

from src.analytics.gamma_flip_carry import (
    CARRY_WARN_BARS,
    GAMMA_FLIP_OBSERVATIONS_SQL,
    resolve_session_flips,
    summarize,
)
from src.database.connection import db_connection
from src.symbols import get_canonical_symbol, parse_underlyings

logger = logging.getLogger("zerogex.gamma_flip_carry_healthcheck")

ET = pytz.timezone("America/New_York")

#: The grid the series is written on.
BAR_MINUTES = 5

#: Session window, matching ``_refresh_gamma_regime_snapshot`` and
#: ``_resolve_flow_series_session``: 09:30 ET plus 6h45m. Resolving a different
#: window here would measure a different day than the writer wrote.
SESSION_OPEN = time(9, 30)
SESSION_LENGTH = timedelta(hours=6, minutes=45)


def session_grid(session_date: date) -> List[datetime]:
    """Every 5-minute bar_start of one session, chronologically.

    The full grid rather than the stored bars, so a carry's depth is counted in
    MINUTES OF SILENCE. Counting stored bars instead would shorten every depth
    by however many bars the writer skipped for want of a chain.
    """
    start = ET.localize(datetime.combine(session_date, SESSION_OPEN))
    end = start + SESSION_LENGTH
    bars = []
    bar = start
    while bar <= end:
        bars.append(bar)
        bar += timedelta(minutes=BAR_MINUTES)
    return bars


def session_dates(cursor, symbol: str, limit: int, since: Optional[date]) -> List[date]:
    """The most recent ET dates with stored structure bars, oldest first."""
    cursor.execute(
        """
        SELECT DISTINCT (bar_start AT TIME ZONE 'America/New_York')::date AS session_date
        FROM gamma_regime_5min
        WHERE symbol = %(symbol)s
          AND (%(since)s::date IS NULL
               OR (bar_start AT TIME ZONE 'America/New_York')::date >= %(since)s::date)
        ORDER BY session_date DESC
        LIMIT %(limit)s
        """,
        {"symbol": symbol, "limit": limit, "since": since},
    )
    return sorted(row[0] for row in cursor.fetchall())


def check_session(cursor, symbol: str, session_date: date) -> Optional[dict]:
    """Classify one session's stored bars. ``None`` when it stored none."""
    grid = session_grid(session_date)
    start, end = grid[0], grid[-1]

    cursor.execute(
        """
        SELECT bar_start, gamma_flip
        FROM gamma_regime_5min
        WHERE symbol = %(symbol)s
          AND bar_start >= %(start)s
          AND bar_start <= %(end)s
        ORDER BY bar_start
        """,
        {"symbol": symbol, "start": start, "end": end},
    )
    stored: Dict[datetime, Optional[float]] = {row[0]: row[1] for row in cursor.fetchall()}
    if not stored:
        return None

    cursor.execute(
        GAMMA_FLIP_OBSERVATIONS_SQL,
        {"symbol": symbol, "session_start": start, "session_end": end},
    )
    resolved = resolve_session_flips(grid, cursor.fetchall())

    # Summarise the bars that EXIST, resolved against the full-time grid: a bar
    # the writer never wrote has no reading to classify, but it still separates
    # the bars either side of it in time.
    bars = [resolved[b] for b in grid if b in stored]
    summary = summarize(bars)

    # Bars stored NULL that a carry would have filled. The current writer
    # cannot emit one, so any count here is history: bars written before the
    # carry existed, or while gamma_flip itself was still missing.
    pre_fix_nulls = sum(
        1 for bar in bars if stored[bar.bar_start] is None and bar.carried and bar.flip is not None
    )

    return {
        "symbol": symbol,
        "session_date": session_date.isoformat(),
        "bars": summary.bars,
        "measured": summary.measured,
        "carried": summary.carried,
        "unresolved": summary.unresolved,
        "deepest_carry_bars": summary.max_stale_bars,
        "deepest_carry_minutes": summary.max_stale_bars * BAR_MINUTES,
        "pre_fix_nulls": pre_fix_nulls,
    }


def configured_symbols() -> List[str]:
    """The underlyings the analytics engine runs, from the environment.

    Mirrors ``main_engine.main``: ``ANALYTICS_UNDERLYINGS`` falling back to
    ``ANALYTICS_UNDERLYING``, default SPY. The engine writes BOTH tables this
    check compares, one worker per symbol, so its universe is the one that
    decides what there is to check -- a monitor pinned to SPY would pass a
    session in which every other symbol's flip went dark.
    """
    raw = os.getenv("ANALYTICS_UNDERLYINGS") or os.getenv("ANALYTICS_UNDERLYING") or "SPY"
    return [get_canonical_symbol(symbol) for symbol in parse_underlyings(raw)]


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Underlyings to check (default: ANALYTICS_UNDERLYINGS, else SPY).",
    )
    parser.add_argument(
        "--sessions", type=int, default=5, help="How many recent sessions to check (default: 5)."
    )
    parser.add_argument("--since", default=None, help="Earliest session date (YYYY-MM-DD).")
    parser.add_argument(
        "--max-carry-bars",
        type=int,
        default=CARRY_WARN_BARS,
        help=(
            "Fail when a session carried a level this many bars or further "
            f"(default: {CARRY_WARN_BARS} = {CARRY_WARN_BARS * BAR_MINUTES} minutes)."
        ),
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    symbols = [s.upper() for s in args.symbols] if args.symbols else configured_symbols()
    if not symbols:
        logger.error("No underlyings configured (ANALYTICS_UNDERLYINGS / ANALYTICS_UNDERLYING)")
        return 2
    since = date.fromisoformat(args.since) if args.since else None

    try:
        reports: List[dict] = []
        with db_connection() as conn:
            with conn.cursor() as cursor:
                for symbol in symbols:
                    for session_date in session_dates(cursor, symbol, args.sessions, since):
                        report = check_session(cursor, symbol, session_date)
                        if report is not None:
                            reports.append(report)
            conn.rollback()
    except Exception:
        logger.exception("gamma-flip carry healthcheck: database error")
        return 2

    # Unresolved counts against the same threshold as carry depth, and it has
    # to: a session where gex_summary wrote NOTHING produces bars that are all
    # unresolved and ZERO carried, because there was never a reading to carry.
    # Gating on carry depth alone would pass the worst case in the file --
    # every bar NULL, every consumer reading "no gamma flip in the profile",
    # and the check green. Unresolved bars are contiguous from the session's
    # first stored bar by construction, so the count is a run length and means
    # the same thing the depth does.
    degraded = [
        r
        for r in reports
        if r["deepest_carry_bars"] >= args.max_carry_bars or r["unresolved"] >= args.max_carry_bars
    ]

    if args.json:
        print(
            json.dumps(
                {
                    "symbols": symbols,
                    "max_carry_bars": args.max_carry_bars,
                    "sessions": reports,
                    "degraded": [f"{r['symbol']} {r['session_date']}" for r in degraded],
                },
                default=str,
            )
        )
    else:
        if not reports:
            logger.warning("%s: no stored gamma_regime_5min bars in range", ", ".join(symbols))
        for report in reports:
            logger.info(
                "%s %s: %d/%d measured, %d carried (deepest %dm), %d unresolved%s",
                report["symbol"],
                report["session_date"],
                report["measured"],
                report["bars"],
                report["carried"],
                report["deepest_carry_minutes"],
                report["unresolved"],
                (f", {report['pre_fix_nulls']} pre-fix NULL" if report["pre_fix_nulls"] else ""),
            )

    if degraded:
        logger.error(
            "%d+ bar(s) (%d+ min) without a gamma flip reading of its own on: %s "
            "-- gex_summary stopped landing in those 5-minute windows. The level was "
            "carried where there was one to carry and is NULL where there was not; "
            "check the analytics cycle (it writes gex_summary) and "
            "`make ingestion-freshness-healthcheck` for the feeds under it",
            args.max_carry_bars,
            args.max_carry_bars * BAR_MINUTES,
            ", ".join(f"{r['symbol']} {r['session_date']}" for r in degraded),
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
