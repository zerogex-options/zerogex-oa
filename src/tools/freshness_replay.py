"""Replay the ingestion freshness check across a past session, from the DB.

The freshness check grades each stream against a delivery window, and a wrong
window is expensive in both directions: too wide and it pages through hours
when the market is shut (the option-chain window did exactly that, ~12 mails a
day until 2026-08-31), too narrow and it sleeps through a real outage. Neither
shows up in a unit test, because the question is not "is the logic right" but
"does this window match what the feed actually does".

This answers that from the only source that knows: the rows themselves. It
walks the real timer cadence over a chosen ET day, asks the SAME functions the
live check asks -- imported, never reimplemented, so the replay cannot drift
from the thing it certifies -- and reports every tick that would have paged.

Usage:
    python -m src.tools.freshness_replay                  # today
    python -m src.tools.freshness_replay --date 2026-08-31
    python -m src.tools.freshness_replay --date 2026-08-31 --legacy-chain-window
    python -m src.tools.freshness_replay --json

Exit codes:
    0 -- no tick would have paged.
    1 -- at least one tick would have paged (each is printed).
    2 -- database error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

import pytz

from src.market_calendar import (
    current_futures_session_start,
    is_futures_session_open,
    option_chain_feed_expected,
    underlying_feed_expected,
)
from src.tools.ingestion_freshness_healthcheck import (
    DEFAULT_MAX_STALE_MINUTES,
    FEED_CHAINS,
    FEED_FUTURES,
    FEED_UNDERLYING,
    FEED_VOLATILITY,
    _chain_anchor,
    _session_anchor,
    configured_symbols,
    evaluate_feed,
)

logger = logging.getLogger(__name__)

ET = pytz.timezone("US/Eastern")

# The live timer is OnCalendar=*:0/10 with up to 60s of jitter. Replaying on
# the nominal 10-minute grid reproduces its decisions; the jitter only shifts
# a page by under a minute, never creates or removes one.
TICK_MINUTES = 10


@dataclass(frozen=True)
class Page:
    """One tick that would have alerted."""

    at: datetime
    feed: str
    symbol: str
    stale_minutes: float
    last_write: Optional[datetime]


@dataclass(frozen=True)
class FeedSpec:
    """A stream, its write history, and how the live check grades it."""

    feed: str
    symbol: str
    writes: Sequence[datetime]  # ascending, ET
    kind: str  # "bars" | "chains" | "volatility" | "futures"


def _last_write_at(writes: Sequence[datetime], tick: datetime) -> Optional[datetime]:
    """Newest write at or before ``tick`` -- what the check would have read."""
    idx = bisect_right(writes, tick)
    return writes[idx - 1] if idx else None


def _grade(
    spec: FeedSpec, tick: datetime, session_template: str, legacy_chain_window: bool
) -> Tuple[bool, Optional[datetime]]:
    """``(expected, session_anchor)`` for one stream at one tick.

    Mirrors ``collect_feeds`` exactly. ``legacy_chain_window`` restores the
    pre-fix behaviour -- chains graded on the underlying's window -- so a run
    can quantify what the change actually bought.
    """
    if spec.kind == "chains" and not legacy_chain_window:
        return option_chain_feed_expected(tick, spec.symbol), _chain_anchor(spec.symbol, tick)
    if spec.kind == "futures":
        open_now = is_futures_session_open(tick)
        return open_now, (current_futures_session_start(tick) if open_now else None)
    # Bars, volatility (which borrows SPX's window), and legacy-mode chains.
    graded_as = "SPX" if spec.kind == "volatility" else spec.symbol
    return (
        underlying_feed_expected(tick, session_template, graded_as),
        _session_anchor(session_template, graded_as, tick),
    )


def replay_day(
    specs: Sequence[FeedSpec],
    day: date,
    session_template: str,
    max_stale: timedelta,
    *,
    legacy_chain_window: bool = False,
    tick_minutes: int = TICK_MINUTES,
) -> Tuple[List[Page], Dict[Tuple[str, str], int]]:
    """Walk the timer over ``day``. Returns (pages, ticks-graded per stream).

    The second value is the guard against a silent win: a window narrowed too
    far also produces zero pages, and only the graded-tick count tells the two
    apart.
    """
    pages: List[Page] = []
    graded_ticks: Dict[Tuple[str, str], int] = {(s.feed, s.symbol): 0 for s in specs}

    tick = ET.localize(datetime.combine(day, time(0, 0)))
    end = tick + timedelta(days=1)
    while tick < end:
        for spec in specs:
            expected, anchor = _grade(spec, tick, session_template, legacy_chain_window)
            if not expected:
                continue
            graded_ticks[(spec.feed, spec.symbol)] += 1
            last = _last_write_at(spec.writes, tick)
            result = evaluate_feed(
                spec.feed,
                spec.symbol,
                last,
                tick,
                expected=True,
                session_anchor=anchor,
                max_stale=max_stale,
            )
            if result.status == "stale":
                pages.append(Page(tick, spec.feed, spec.symbol, result.stale_minutes or 0.0, last))
        tick += timedelta(minutes=tick_minutes)
    return pages, graded_ticks


def _fetch_series(day: date, symbols: List[str]) -> List[FeedSpec]:
    """Every stream's write timestamps for ``day``, plus the prior session's
    tail so a pre-open tick anchors the way the live check does."""
    from src.database.connection import db_connection

    start = ET.localize(datetime.combine(day - timedelta(days=4), time(0, 0)))
    end = ET.localize(datetime.combine(day + timedelta(days=1), time(0, 0)))
    specs: List[FeedSpec] = []

    def rows_to_et(rows) -> List[datetime]:
        return sorted(r[0].astimezone(ET) for r in rows if r[0] is not None)

    with db_connection() as conn:
        with conn.cursor() as cursor:
            for symbol in symbols:
                cursor.execute(
                    "SELECT DISTINCT timestamp FROM underlying_quotes "
                    "WHERE symbol = %s AND timestamp >= %s AND timestamp < %s",
                    (symbol, start, end),
                )
                specs.append(
                    FeedSpec(FEED_UNDERLYING, symbol, rows_to_et(cursor.fetchall()), "bars")
                )
                # Bounded to the replay range and one underlying at a time --
                # the wide GROUP BY over option_chains is what blew the 90s
                # statement timeout in production once already.
                cursor.execute(
                    "SELECT DISTINCT timestamp FROM option_chains "
                    "WHERE underlying = %s AND timestamp >= %s AND timestamp < %s",
                    (symbol, start, end),
                )
                specs.append(FeedSpec(FEED_CHAINS, symbol, rows_to_et(cursor.fetchall()), "chains"))

            for ticker, table in (("VIX", "vix_bars"), ("VXN", "vxn_bars")):
                try:
                    cursor.execute(
                        f"SELECT DISTINCT timestamp FROM {table} "  # nosec B608
                        "WHERE timestamp >= %s AND timestamp < %s",
                        (start, end),
                    )
                except Exception:  # table absent on a deployment without VIX
                    conn.rollback()
                    continue
                specs.append(
                    FeedSpec(FEED_VOLATILITY, ticker, rows_to_et(cursor.fetchall()), "volatility")
                )

            for index_symbol in ("SPX", "NDX"):
                try:
                    cursor.execute(
                        "SELECT DISTINCT timestamp FROM futures_quotes "
                        "WHERE index_symbol = %s AND timestamp >= %s AND timestamp < %s",
                        (index_symbol, start, end),
                    )
                except Exception:
                    conn.rollback()
                    continue
                specs.append(
                    FeedSpec(FEED_FUTURES, index_symbol, rows_to_et(cursor.fetchall()), "futures")
                )
    return specs


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Replay the freshness check over a past session from the database."
    )
    parser.add_argument(
        "--date", default=None, help="ET date to replay (YYYY-MM-DD, default today)"
    )
    parser.add_argument("--max-stale-minutes", type=float, default=float(DEFAULT_MAX_STALE_MINUTES))
    parser.add_argument(
        "--legacy-chain-window",
        action="store_true",
        help="Grade chains on the underlying's window (pre-2026-08-31 behaviour), for comparison.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from dotenv import load_dotenv

    load_dotenv()

    import os

    day = date.fromisoformat(args.date) if args.date else datetime.now(ET).date()
    session_template = os.getenv("SESSION_TEMPLATE", "Default")
    max_stale = timedelta(minutes=args.max_stale_minutes)
    symbols = configured_symbols()

    try:
        specs = _fetch_series(day, symbols)
    except Exception as exc:
        logger.error("Replay could not read the feed tables: %s", exc, exc_info=True)
        return 2

    pages, graded = replay_day(
        specs,
        day,
        session_template,
        max_stale,
        legacy_chain_window=args.legacy_chain_window,
    )

    if args.json:
        print(
            json.dumps(
                {
                    "date": day.isoformat(),
                    "session_template": session_template,
                    "max_stale_minutes": args.max_stale_minutes,
                    "legacy_chain_window": args.legacy_chain_window,
                    "pages": [
                        {
                            "at": p.at.isoformat(),
                            "feed": p.feed,
                            "symbol": p.symbol,
                            "stale_minutes": p.stale_minutes,
                            "last_write": p.last_write.isoformat() if p.last_write else None,
                        }
                        for p in pages
                    ],
                    "graded_ticks": {f"{f}|{s}": n for (f, s), n in sorted(graded.items())},
                },
                indent=2,
            )
        )
        return 1 if pages else 0

    rule = "LEGACY (chains on the underlying window)" if args.legacy_chain_window else "current"
    logger.info(
        "Replay of %s under the %s rule, threshold %.0f min", day, rule, args.max_stale_minutes
    )
    logger.info("Ticks graded per stream (0 = never checked all day — a blind spot, not a pass):")
    for (feed, symbol), count in sorted(graded.items()):
        writes = next(s.writes for s in specs if s.feed == feed and s.symbol == symbol)
        same_day = [w for w in writes if w.date() == day]
        logger.info("  %-18s %-4s graded %3d ticks, %4d writes", feed, symbol, count, len(same_day))

    if not pages:
        logger.info("No tick would have paged. Clean.")
        return 0

    logger.error("%d tick(s) would have paged:", len(pages))
    for p in pages:
        logger.error(
            "  %s  %s %s stale %.1f min (last write %s)",
            p.at.strftime("%H:%M ET"),
            p.feed,
            p.symbol,
            p.stale_minutes,
            p.last_write.strftime("%H:%M") if p.last_write else "never",
        )
    return 1


if __name__ == "__main__":
    sys.exit(main())
