"""Seed hedging_flow_5min history from the flow facts still on disk.

Why this is urgent rather than optional
---------------------------------------
The Analytics Engine writes only the CURRENT session's snapshot rows each
cycle, so on the day the table ships it holds one day. Everything before that
exists solely in ``flow_contract_facts``, which ``make db-prune`` deletes at
``DATA_RETENTION_DAYS`` (90) — and once a day falls out of that window it is
unrecoverable, because the snapshot is the only thing that would have
outlived it.

So this is a one-way door with a clock on it: run it once, soon, and ~90 days
of Hedging Flow history exists permanently. Don't, and that history ages out a
day at a time while the feature that needs it is being built.

The sibling tool ``flow_series_5min_backfill`` deliberately covers only
current + prior, because that is all ``/api/flow/series`` can request. This
one has the opposite mandate: ``/api/flow/hedging?date=`` can request ANY
stored day, so the default is the whole retained window.

What it writes
--------------
The exact ``/api/flow/hedging`` CTE, rendered for psycopg2 from the same
template the live read and the engine writer use
(:mod:`src.hedging_flow_sql`), per (symbol, session, scope) window. Backfilled
rows are therefore identical to what the endpoint would have returned that
day rather than a second implementation's approximation of them.

Both scopes per session: ``all`` (unfiltered) and ``0dte`` (the session's own
date as the expirations filter). A day that was not an expiry writes no
``0dte`` rows at all — the CTE's timeline is gated on its ``filtered`` CTE
having rows, so a filter matching nothing yields nothing rather than a
session of synthetic zeros.

Safe to rerun. Rows UPSERT on (symbol, scope, bar_start) and closed bars are
window-invariant, so a second pass over an already-written day writes zero
rows — the IS DISTINCT FROM guard suppresses it.

Usage:
    python -m src.tools.hedging_flow_5min_backfill --symbols SPY
    python -m src.tools.hedging_flow_5min_backfill --symbols SPY,QQQ --days 90
    python -m src.tools.hedging_flow_5min_backfill --symbols SPY --date 2026-06-12
    python -m src.tools.hedging_flow_5min_backfill --symbols SPY --dry-run
"""

import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

from src.config import DATA_RETENTION_DAYS
from src.database import db_connection
from src.hedging_flow_sql import (
    HEDGING_FLOW_SCOPES,
    HEDGING_FLOW_SNAPSHOT_UPSERT_PSYCOPG2,
    SCOPE_0DTE,
)

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
_SESSION_LENGTH = timedelta(hours=6, minutes=45)  # 09:30 -> 16:15 ET


def _session_window(session_date: date) -> tuple[datetime, datetime]:
    """(session_start_utc, session_end_utc) for one ET trading date.

    Always the full 09:30–16:15 window. Unlike the flow-series tool there is
    no is_current branch: a session still open will simply have no facts past
    the current bar, and the CTE's generated timeline is gated on its
    ``filtered`` CTE having rows — so an over-wide end cannot manufacture
    bars, it can only fail to find any.
    """
    session_start = datetime(
        session_date.year, session_date.month, session_date.day, 9, 30, tzinfo=_ET
    ).astimezone(timezone.utc)
    return session_start, session_start + _SESSION_LENGTH


def _sessions_with_facts(cur, symbol: str, since: Optional[date]) -> List[date]:
    """ET dates that still have flow facts for this symbol, oldest first.

    Read from ``flow_contract_facts`` rather than a calendar because that is
    the table being raced: a calendar would confidently name days whose facts
    have already been pruned, and the backfill would write empty sessions
    over them.

    Oldest first on purpose. If the run is interrupted, the days closest to
    falling out of the retention window are the ones already saved.
    """
    if since is not None:
        cur.execute(
            """
            SELECT DISTINCT (timestamp AT TIME ZONE 'America/New_York')::date AS d
            FROM flow_contract_facts
            WHERE symbol = %s
              AND (timestamp AT TIME ZONE 'America/New_York')::date >= %s::date
            ORDER BY d
            """,
            (symbol, since),
        )
    else:
        cur.execute(
            """
            SELECT DISTINCT (timestamp AT TIME ZONE 'America/New_York')::date AS d
            FROM flow_contract_facts
            WHERE symbol = %s
            ORDER BY d
            """,
            (symbol,),
        )
    return [row[0] for row in cur.fetchall()]


def _upsert_session(cur, symbol: str, session_date: date) -> int:
    """UPSERT both scopes for one session. Returns rows written."""
    session_start, session_end = _session_window(session_date)
    written = 0
    for scope in HEDGING_FLOW_SCOPES:
        cur.execute(
            HEDGING_FLOW_SNAPSHOT_UPSERT_PSYCOPG2,
            {
                "symbol": symbol,
                "scope": scope,
                "session_start": session_start,
                "session_end": session_end,
                "strikes": None,
                # The 0DTE filter is the SESSION's own date, never today's.
                # On a backfill those differ, and using today's would write a
                # filter that matched nothing for every historical day.
                "expirations": [session_date] if scope == SCOPE_0DTE else None,
            },
        )
        written += cur.rowcount or 0
    return written


def backfill_symbol(
    conn,
    symbol: str,
    *,
    since: Optional[date],
    explicit_date: Optional[date],
    dry_run: bool,
) -> int:
    """Materialise every retained session for one symbol. Returns rows written."""
    cur = conn.cursor()
    if explicit_date is not None:
        sessions = [explicit_date]
    else:
        sessions = _sessions_with_facts(cur, symbol, since)

    if not sessions:
        logger.warning("  %s: no flow_contract_facts rows in range — skipping", symbol)
        return 0

    logger.info(
        "  %s: %d session(s) to write, %s .. %s",
        symbol,
        len(sessions),
        sessions[0],
        sessions[-1],
    )
    if dry_run:
        return 0

    total = 0
    for session_date in sessions:
        written = _upsert_session(cur, symbol, session_date)
        total += written
        # Commit per session so an interruption keeps every day it finished
        # rather than rolling the whole window back. The UPSERT is idempotent,
        # so a resumed run re-walks the earlier days for free.
        conn.commit()
        logger.info("    %s %s: %d row(s)", symbol, session_date, written)
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--symbols",
        default="SPY",
        help="Comma-separated underlying symbols (default: SPY).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DATA_RETENTION_DAYS,
        help=(
            "How far back to reach, in calendar days "
            f"(default: DATA_RETENTION_DAYS={DATA_RETENTION_DAYS}, i.e. everything "
            "the prune window has left). 0 means no lower bound."
        ),
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Write exactly one YYYY-MM-DD ET session instead of a range.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report which sessions would be written, write nothing.",
    )
    parser.add_argument("--verbose", action="store_true", help="Emit DEBUG-level logs.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    explicit_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None
    since = (
        None
        if explicit_date is not None or args.days <= 0
        else (datetime.now(_ET).date() - timedelta(days=args.days))
    )

    logger.info(
        "Backfilling hedging_flow_5min for %s%s%s",
        ",".join(symbols),
        f" since {since}" if since else "",
        " (dry run)" if args.dry_run else "",
    )

    total = 0
    with db_connection() as conn:
        for symbol in symbols:
            total += backfill_symbol(
                conn,
                symbol,
                since=since,
                explicit_date=explicit_date,
                dry_run=args.dry_run,
            )

    logger.info("Done — %d row(s) written", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
