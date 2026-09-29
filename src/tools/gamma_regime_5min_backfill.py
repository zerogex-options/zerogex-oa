"""Seed gamma_regime_5min history from the per-strike chains still on disk.

Why this has a clock on it
--------------------------
The Analytics Engine writes ONE bar per cycle, so on the day the table shipped
it held one day. Every session before that exists only as the inputs it would
be computed from: ``gex_by_strike`` (the per-strike chain) and
``underlying_quotes`` (spot).

``gex_by_strike`` is in ``DB_MAINTAIN_TABLES``, so ``make db-prune`` deletes it
at ``DATA_RETENTION_DAYS`` (90). Once a day falls out of that window the
structure series for it can never be built: the retention-exempt
``option_chains_archive`` carries prices and greeks but NOT open interest, and
GEX cannot be computed without it, while ``gex_summary`` is retention-exempt
but session-level rather than per-strike. So this is a one-way door: run it and
the structure history exists permanently, don't and it ages out a day at a time.

No second implementation
------------------------
This tool computes nothing itself. It calls the engine's own
``_refresh_gamma_regime_snapshot`` once per historical session, which is the
same method the live cycle calls, writing through the same SQL. A backfill that
re-derived the bars would be a second implementation of the classifier, and the
day someone retunes ``GAMMA_REGIME_ROLLING_BARS`` the backfilled history and
the live history would start disagreeing about what a bar means.

Calling one private method is the price of that. The alternative was extracting
the engine's hot path into a shared helper, which is a riskier change to live
code than a read-only tool reaching one level in.

Why passing a historical timestamp is enough
--------------------------------------------
The method resolves its own session from the timestamp's ET calendar day, and
takes ``session_end = min(current_bar, session_close)``. For any past day the
current bar is far beyond the close, so the whole session is written. It also
skips bars already present, so a rerun over a written day is nearly free, and
a past day's bars are deterministic given the stored chains, so rewriting the
last one changes nothing.

A market holiday needs no calendar here: the method returns early when the
session open has no chain, so a closed day writes nothing on its own.

Load
----
Each bar reads a ~1500-row chain, roughly 82 chain reads per symbol-session.
That is real work on a database serving live traffic, so days are processed one
at a time, each in its own committed transaction (see ``db_connection``), and
``--sleep`` puts a gap between them. Interrupting it loses at most the day in
flight. Prefer running it outside market hours.

Usage:
    python -m src.tools.gamma_regime_5min_backfill --symbols SPY --dry-run
    python -m src.tools.gamma_regime_5min_backfill --symbols SPY
    python -m src.tools.gamma_regime_5min_backfill --symbols SPY,QQQ --days 90
    python -m src.tools.gamma_regime_5min_backfill --symbols SPY --start 2026-08-01
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from typing import List, Optional, Sequence

from src.analytics.main_engine import AnalyticsEngine
from src.config import DATA_RETENTION_DAYS, SIGNALS_UNDERLYINGS
from src.database import db_connection

logger = logging.getLogger(__name__)

try:  # pytz in the engine, zoneinfo elsewhere; both resolve America/New_York.
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except ImportError:  # pragma: no cover - zoneinfo ships with 3.9+
    import pytz

    ET = pytz.timezone("America/New_York")

#: Bars in a full 09:30-16:15 ET session on the 5-minute grid.
SESSION_BARS = 82

#: Seconds between sessions. Not throttling for its own sake: the chain reads
#: are heavy and the same database is serving the live page.
DEFAULT_SLEEP = 1.0


def trading_days(start: date, end: date) -> List[date]:
    """Weekdays in [start, end]. Holidays are left in deliberately.

    A holiday has no chain at the session open, so the engine method returns
    without writing. Letting the data answer beats carrying a market calendar
    that has to be maintained and will be wrong the first year it isn't.
    """
    out, day = [], start
    while day <= end:
        if day.weekday() < 5:
            out.append(day)
        day += timedelta(days=1)
    return out


def _session_noon(day: date) -> datetime:
    """A timestamp inside `day`'s ET session, which is all the engine needs.

    Noon rather than the open so a DST transition cannot land the value on a
    nonexistent or ambiguous wall-clock hour.
    """
    naive = datetime(day.year, day.month, day.day, 12, 0)
    localize = getattr(ET, "localize", None)
    return localize(naive) if localize else naive.replace(tzinfo=ET)


def source_window(cursor, db_symbol: str) -> Optional[tuple]:
    """(first, last) ET dates with per-strike chain rows, or None if there are none.

    The real floor on how far back this can reach, and the number worth
    printing: asking for 90 days when the chain only holds 60 should say so
    rather than walking 30 empty sessions.
    """
    cursor.execute(
        """
        SELECT MIN(timestamp AT TIME ZONE 'America/New_York')::date,
               MAX(timestamp AT TIME ZONE 'America/New_York')::date
        FROM gex_by_strike
        WHERE underlying = %s
        """,
        (db_symbol,),
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        return None
    return (row[0], row[1])


def written_bars(cursor, db_symbol: str, day: date) -> int:
    """Structure bars already stored for one ET session."""
    cursor.execute(
        """
        SELECT COUNT(*) FROM gamma_regime_5min
        WHERE symbol = %s
          AND (bar_start AT TIME ZONE 'America/New_York')::date = %s
        """,
        (db_symbol, day),
    )
    return int(cursor.fetchone()[0])


def session_state(cursor, db_symbol: str, day: date) -> tuple:
    """(bars, bars carrying a flip, flips upstream, chain rows) for one session.

    The third number is what makes the second one interpretable. A session with
    no flip on any bar is either a day the profile never crossed zero, which is
    a real market condition and nothing to fix, or a day the flip was sitting
    in gex_summary and never reached the bars. Only the second is a hole.
    """
    cursor.execute(
        """
        SELECT COUNT(*), COUNT(gamma_flip) FROM gamma_regime_5min
        WHERE symbol = %s
          AND (bar_start AT TIME ZONE 'America/New_York')::date = %s
        """,
        (db_symbol, day),
    )
    bars, with_flip = (int(v) for v in cursor.fetchone())
    cursor.execute(
        """
        SELECT COUNT(gamma_flip_point) FROM gex_summary
        WHERE underlying = %s
          AND (timestamp AT TIME ZONE 'America/New_York')::date = %s
        """,
        (db_symbol, day),
    )
    upstream = int(cursor.fetchone()[0])
    # Whether the session can be rebuilt at all. A rebuild deletes first, so
    # without this a day whose chains have since been pruned would lose the
    # bars it had and get nothing back.
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM gex_by_strike
            WHERE underlying = %s
              AND (timestamp AT TIME ZONE 'America/New_York')::date = %s
        )
        """,
        (db_symbol, day),
    )
    return bars, with_flip, upstream, bool(cursor.fetchone()[0])


def needs_build(bars: int, with_flip: int, upstream_flips: int) -> bool:
    """Whether a session is worth (re)building.

    "Has 82 rows" was the first definition of done here and it was the wrong
    one. Four sessions written live before the gamma_flip column existed had a
    full set of bars carrying no flip at all, so the backfill called them
    complete and never looked at whether the bars were any good.

    A session is done when it has its bars AND either those bars carry a flip
    or there was no flip upstream to carry. That last clause is what stops a
    genuinely flip-less day from being rebuilt on every run forever, which is
    what a plain "any NULL means rebuild" rule would do.
    """
    if bars < SESSION_BARS:
        return True
    return with_flip == 0 and upstream_flips > 0


def backfill_symbol(
    symbol: str,
    start: date,
    end: date,
    dry_run: bool = False,
    sleep: float = DEFAULT_SLEEP,
    force: bool = False,
) -> dict:
    """Write every missing structure bar for one symbol over [start, end]."""
    engine = AnalyticsEngine(underlying=symbol)
    # The live gate is an operational switch for the analytics cycle, not a
    # statement about whether this history should exist. An operator running
    # this tool has asked for the write explicitly.
    engine._analytics_flow_cache_refresh_enabled = True
    db_symbol = engine.db_symbol

    counts = {"symbol": symbol, "days": 0, "written": 0, "already": 0, "empty": 0}

    with db_connection() as conn:
        cursor = conn.cursor()
        window = source_window(cursor, db_symbol)
        if window is None:
            logger.warning("%s: no gex_by_strike rows at all, nothing to build from", symbol)
            return counts
        src_first, src_last = window

    if start < src_first:
        logger.info(
            "%s: per-strike chain starts %s, so %s is the earliest session reachable "
            "(asked for %s)",
            symbol,
            src_first,
            src_first,
            start,
        )
        start = src_first
    if end > src_last:
        end = src_last

    days = trading_days(start, end)
    logger.info("%s: %d candidate session(s), %s to %s", symbol, len(days), start, end)

    for day in days:
        with db_connection() as conn:
            before, with_flip, upstream, has_chain = session_state(conn.cursor(), db_symbol, day)
        if not force and not needs_build(before, with_flip, upstream):
            counts["already"] += 1
            continue

        # A session with its bars already but no flip on them is a repair, not
        # a build, and saying so is the difference between a run that looks
        # like it did nothing and one that says what it fixed.
        stale_flip = before >= SESSION_BARS and with_flip == 0 and upstream > 0
        rebuild = stale_flip or (force and before > 0)
        if rebuild and not has_chain:
            # Deleting here would cost the day its bars and put nothing back,
            # because the chains it would be rebuilt from are gone.
            logger.warning(
                "%s %s: %d bar(s) stored but the per-strike chain has been pruned, "
                "leaving them alone",
                symbol,
                day,
                before,
            )
            counts["already"] += 1
            continue
        if dry_run:
            logger.info(
                "%s %s: would %s (%d/%d bars stored, %d with a flip, %d upstream)",
                symbol,
                day,
                "rebuild" if stale_flip else "build",
                before,
                SESSION_BARS,
                with_flip,
                upstream,
            )
            counts["days"] += 1
            continue

        try:
            if rebuild:
                # The engine writes only the bars it finds missing, so a full
                # set of bad rows would be skipped by its own todo list. Clear
                # the day first and let it rebuild from the chains.
                with db_connection() as conn:
                    conn.cursor().execute(
                        """
                        DELETE FROM gamma_regime_5min
                        WHERE symbol = %s
                          AND (bar_start AT TIME ZONE 'America/New_York')::date = %s
                        """,
                        (db_symbol, day),
                    )
                before = 0
            engine._refresh_gamma_regime_snapshot(_session_noon(day))
        except Exception as exc:
            # One bad session must not cost the rest of the window. The method
            # is best-effort by design, so reaching here at all is unusual.
            logger.error("%s %s: failed, continuing: %s", symbol, day, exc)
            continue

        with db_connection() as conn:
            after = written_bars(conn.cursor(), db_symbol, day)
        added = after - before
        if added <= 0 and after == 0:
            counts["empty"] += 1
            logger.info("%s %s: no chain at the open, skipped (holiday or gap)", symbol, day)
        else:
            counts["days"] += 1
            counts["written"] += added
            logger.info("%s %s: +%d bar(s), %d stored", symbol, day, added, after)

        if sleep:
            time.sleep(sleep)

    return counts


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--symbols", help="Comma-separated, e.g. SPY,QQQ (default: signals set)")
    p.add_argument(
        "--days",
        type=int,
        default=DATA_RETENTION_DAYS,
        help=f"Trailing calendar days (default {DATA_RETENTION_DAYS}); clamped to stored chains",
    )
    p.add_argument("--start", help="ISO date, overrides --days")
    p.add_argument(
        "--end",
        help="ISO date inclusive (default: yesterday, so the live engine owns today)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report which sessions are missing bars without writing. Coverage, not a preview: "
        "the bars themselves are only known once built.",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help=f"Seconds between sessions (default {DEFAULT_SLEEP})",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Rebuild every session in the window, even ones that look complete. "
        "Deletes the day's bars first, so the engine rewrites them from the chains.",
    )
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    symbols = (
        [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else [s.strip().upper() for s in (SIGNALS_UNDERLYINGS or "SPY").split(",") if s.strip()]
    )

    # Yesterday by default. Today's session is the live engine's, and a
    # backfill racing it would rewrite the open bar underneath the page.
    end = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    start = (
        date.fromisoformat(args.start) if args.start else end - timedelta(days=max(1, args.days))
    )
    if start > end:
        logger.error("start %s is after end %s", start, end)
        return 2

    logger.info(
        "Backfilling gamma_regime_5min for %s, %s to %s%s",
        ",".join(symbols),
        start,
        end,
        " (dry-run)" if args.dry_run else "",
    )

    totals = {"days": 0, "written": 0, "already": 0, "empty": 0}
    for sym in symbols:
        try:
            counts = backfill_symbol(sym, start, end, args.dry_run, args.sleep, args.force)
        except Exception as exc:
            logger.error("%s: backfill failed, continuing: %s", sym, exc)
            continue
        for key in totals:
            totals[key] += counts[key]

    logger.info(
        "Done: %d session(s) %s, %d bar(s) written, %d already complete, %d with no chain.",
        totals["days"],
        "identified" if args.dry_run else "built",
        totals["written"],
        totals["already"],
        totals["empty"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
