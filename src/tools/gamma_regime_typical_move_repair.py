"""Recompute gamma_regime_5min.typical_move_30m causally, one bar at a time.

The problem
-----------
``AnalyticsEngine._typical_move_30m`` is computed ONCE per refresh call and
stamped on every bar that call writes, with ``until = session_end``. Live that
is almost always the newest bar, so the value is strictly backward-looking and
the docstring's "small and bounded inaccuracy" only applies to the handful of
bars a gap-fill catches up after downtime.

The backfill (``gamma_regime_5min_backfill``) writes a whole past session in
one call, where ``session_end`` is the 16:15 close. So a 10:00 bar on a
backfilled day carries a five-day median that includes that same day's
afternoon. The bar is stamped with a number that did not exist yet.

Why that is worth a pass of its own
-----------------------------------
``typical_move_30m`` is the yardstick the flip cushion is banded against, so a
shifted denominator moves bars across the SECURE / NORMAL / THIN boundaries --
precisely the bars a study of "did this state hold" is most sensitive to. It
leaves every other field alone: pressure, lean, stability, gamma trend, the
flip and spot are all per-bar and already causal.

Worse than the error itself is that it is INCONSISTENT. Sessions written live
are causal and sessions written by the backfill are not, and the split follows
the calendar, so pooling them puts a date-correlated artifact into any base
rate computed over the range.

Safe over the whole table
-------------------------
A causal recomputation is the correct value for every bar, however it was
written. For a bar the live engine stamped as the newest bar the recomputed
value equals the stored one and the UPDATE skips it; for a gap-filled or
backfilled bar it corrects it. So the default range is everything, and running
it twice changes nothing the second time.

Only ``typical_move_30m`` is written. Nothing else in the row is touched.

One statement per symbol-session, because the production database runs a 90s
``statement_timeout`` and a single UPDATE over the whole table would not
finish inside it. Each session is its own committed transaction, so an
interrupted run loses at most one day's correction.

Usage:
    python -m src.tools.gamma_regime_typical_move_repair --dry-run
    python -m src.tools.gamma_regime_typical_move_repair
    python -m src.tools.gamma_regime_typical_move_repair --start 2026-07-29 --end 2026-09-08
"""

import argparse
import logging
import sys
from datetime import date
from typing import List, Optional, Sequence

from src.analytics.main_engine import AnalyticsEngine
from src.database import db_connection

logger = logging.getLogger(__name__)

#: The engine's own lookback, read off the class so the two cannot drift. A
#: repair computing a different median than the live writer would be a second
#: definition of "typical move" wearing the same column name.
LOOKBACK_DAYS = AnalyticsEngine.GAMMA_MOVE_LOOKBACK_DAYS
MIN_MINUTES = AnalyticsEngine.GAMMA_MOVE_MIN_MINUTES


#: The engine's median, re-expressed as a correlated subquery so it can be
#: evaluated per bar. Same 30-minute buckets, same minimum-minutes guard, same
#: percentile_cont(0.5); only `until` changes, from the session's end to the
#: bar's own start.
#:
#: The lookback multiplies an interval rather than interpolating into
#: INTERVAL '%(lookback)s days'. A placeholder inside a quoted literal happens
#: to render correctly for an int today and breaks the moment the value is not
#: one, which is the kind of thing that works until it silently does not.
#:
#: '24 hours' and not '1 day': Postgres day arithmetic on a timestamptz is
#: CALENDAR arithmetic, while the engine's ``timedelta(days=...)`` on an aware
#: datetime is absolute. They agree except across a DST edge, where they would
#: put the window boundary an hour apart and this pass would start disagreeing
#: with the live writer on exactly the days nobody thinks to check.
def _causal_move(alias: str) -> str:
    return f"""
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY w.rng)
    FROM (
        SELECT MAX(uq.high) - MIN(uq.low) AS rng, COUNT(*) AS mins
        FROM underlying_quotes uq
        WHERE uq.symbol = {alias}.symbol
          AND uq.timestamp >= {alias}.bar_start - (%(lookback)s * INTERVAL '24 hours')
          AND uq.timestamp <= {alias}.bar_start
        GROUP BY date_trunc('hour', uq.timestamp)
               + FLOOR(EXTRACT(MINUTE FROM uq.timestamp)::int / 30)
                 * INTERVAL '30 minutes'
    ) w
    WHERE w.mins >= %(min_minutes)s
"""


#: One row per stored bar with the value it SHOULD carry. Computed once per
#: bar and joined back, rather than evaluated a second time in the WHERE: the
#: median is the expensive part of this and doing it twice doubles the pass.
_CANDIDATES = f"""
    SELECT g2.symbol, g2.bar_start, ({_causal_move('g2')}) AS val
    FROM gamma_regime_5min g2
    WHERE g2.symbol = %(symbol)s
      AND (g2.bar_start AT TIME ZONE 'America/New_York')::date = %(day)s
"""

_PREVIEW_SQL = f"""
    SELECT COUNT(*)
    FROM ({_CANDIDATES}) s
    JOIN gamma_regime_5min g
      ON g.symbol = s.symbol AND g.bar_start = s.bar_start
    WHERE g.typical_move_30m IS DISTINCT FROM s.val
"""

_UPDATE_SQL = f"""
    UPDATE gamma_regime_5min g
    SET typical_move_30m = s.val
    FROM ({_CANDIDATES}) s
    WHERE g.symbol = s.symbol
      AND g.bar_start = s.bar_start
      AND g.typical_move_30m IS DISTINCT FROM s.val
"""


def stored_days(cursor, symbol: str, start: Optional[date], end: Optional[date]) -> List[date]:
    """ET sessions that actually have bars, so empty days are never walked."""
    clauses = ["symbol = %(symbol)s"]
    params = {"symbol": symbol}
    if start:
        clauses.append("(bar_start AT TIME ZONE 'America/New_York')::date >= %(start)s")
        params["start"] = start
    if end:
        clauses.append("(bar_start AT TIME ZONE 'America/New_York')::date <= %(end)s")
        params["end"] = end
    cursor.execute(
        f"""
        SELECT DISTINCT (bar_start AT TIME ZONE 'America/New_York')::date AS d
        FROM gamma_regime_5min
        WHERE {' AND '.join(clauses)}
        ORDER BY d
        """,
        params,
    )
    return [r[0] for r in cursor.fetchall()]


def symbols_present(cursor) -> List[str]:
    cursor.execute("SELECT DISTINCT symbol FROM gamma_regime_5min ORDER BY symbol")
    return [r[0] for r in cursor.fetchall()]


def repair_symbol(
    symbol: str,
    start: Optional[date],
    end: Optional[date],
    dry_run: bool = False,
) -> dict:
    counts = {"days": 0, "changed": 0}
    params_base = {"lookback": LOOKBACK_DAYS, "min_minutes": MIN_MINUTES, "symbol": symbol}

    with db_connection() as conn:
        days = stored_days(conn.cursor(), symbol, start, end)
    logger.info("%s: %d stored session(s) to check", symbol, len(days))

    for day in days:
        params = dict(params_base, day=day)
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                if dry_run:
                    cursor.execute(_PREVIEW_SQL, params)
                    changed = int(cursor.fetchone()[0])
                else:
                    cursor.execute(_UPDATE_SQL, params)
                    changed = cursor.rowcount
        except Exception as exc:
            # One session's correction is not worth losing the rest of the run.
            logger.error("%s %s: failed, continuing: %s", symbol, day, exc)
            continue

        counts["days"] += 1
        counts["changed"] += changed
        if changed:
            logger.info(
                "%s %s: %d bar(s) %s",
                symbol,
                day,
                changed,
                "would change" if dry_run else "corrected",
            )

    return counts


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--symbols", help="Comma-separated (default: every symbol in the table)")
    p.add_argument("--start", help="ISO date, inclusive (default: earliest stored)")
    p.add_argument("--end", help="ISO date, inclusive (default: latest stored)")
    p.add_argument("--dry-run", action="store_true", help="Count what would change, write nothing")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None
    if start and end and start > end:
        logger.error("start %s is after end %s", start, end)
        return 2

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        with db_connection() as conn:
            symbols = symbols_present(conn.cursor())

    logger.info(
        "Recomputing typical_move_30m causally (%d-day median, >=%d min windows) for %s%s",
        LOOKBACK_DAYS,
        MIN_MINUTES,
        ",".join(symbols) or "(none)",
        " (dry-run)" if args.dry_run else "",
    )

    totals = {"days": 0, "changed": 0}
    for sym in symbols:
        # Isolate per-symbol failures, exactly as the backfill does: one
        # symbol's slow query must not cost the others their correction.
        try:
            counts = repair_symbol(sym, start, end, args.dry_run)
        except Exception as exc:
            logger.error("%s: repair failed, continuing: %s", sym, exc)
            continue
        totals["days"] += counts["days"]
        totals["changed"] += counts["changed"]

    logger.info(
        "Done: %d session(s) checked, %d bar(s) %s.",
        totals["days"],
        totals["changed"],
        "would change" if args.dry_run else "corrected",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
