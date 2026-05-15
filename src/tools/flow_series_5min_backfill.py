"""One-shot backfill for the flow_series_5min snapshot.

The Analytics Engine only writes the *current* session's snapshot rows on
each cycle (the same way it only refreshes the current/previous
flow_by_contract buckets). So when this table is first deployed, the
sessions the API can actually request — ``session=current`` and
``session=prior`` — have no snapshot rows until a full session has been
written live. This script materialises both of those windows for the
requested symbols straight from existing flow_by_contract rows.

It runs the *exact* /api/flow/series CTE (unfiltered) per (symbol,
session) window and UPSERTs the result, so backfilled rows are
byte-identical to what the live CTE would return for the same window —
the same query the Analytics Engine runs every cycle. Safe to rerun:
rows are UPSERTed on (symbol, bar_start) and closed bars are
window-invariant, so a rerun is a no-op for already-final bars.

Scope is deliberately current + prior session only (per design): that is
everything /api/flow/series can request. Deeper history would be cold
data the endpoint never reads.

Usage:
    python -m src.tools.flow_series_5min_backfill --symbols SPY
    python -m src.tools.flow_series_5min_backfill --symbols SPY,QQQ --date 2026-04-22
"""

import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from src.database import db_connection
from src.flow_series_sql import SNAPSHOT_UPSERT_PSYCOPG2

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
_SESSION_LENGTH = timedelta(hours=6, minutes=45)  # 09:30 -> 16:15 ET


def _session_window(session_date: date, *, is_current: bool) -> tuple[datetime, datetime]:
    """(session_start_utc, session_end_utc) for a session date, matching
    _resolve_flow_series_session exactly: open at 09:30 ET; a current
    session ends at min(now floored to 5 min, close); a prior session is
    always fully closed at 16:15 ET."""
    session_start = datetime(
        session_date.year, session_date.month, session_date.day, 9, 30, tzinfo=_ET
    ).astimezone(timezone.utc)
    session_close = session_start + _SESSION_LENGTH
    if not is_current:
        return session_start, session_close
    now_utc = datetime.now(timezone.utc)
    now_floor_epoch = int(now_utc.timestamp() // 300) * 300
    now_floored = datetime.fromtimestamp(now_floor_epoch, tz=timezone.utc)
    session_end = min(now_floored, session_close)
    if session_end < session_start:
        session_end = session_start
    return session_start, session_end


def _resolve_dates(
    cur, symbol: str, explicit: Optional[str]
) -> tuple[Optional[date], Optional[date]]:
    """Resolve (current_date, prior_date) ET the same way the API does:
    data-driven from MAX(timestamp). ``--date`` anchors the current
    session; the prior session is still the most recent ET day with data
    strictly before it (so weekends/holidays/gaps resolve correctly)."""
    if explicit:
        current_date: Optional[date] = datetime.strptime(explicit, "%Y-%m-%d").date()
    else:
        cur.execute(
            """
            SELECT (MAX(timestamp) AT TIME ZONE 'America/New_York')::date
            FROM flow_by_contract
            WHERE symbol = %s
            """,
            (symbol,),
        )
        row = cur.fetchone()
        current_date = row[0] if row else None
    if current_date is None:
        return None, None
    cur.execute(
        """
        SELECT (MAX(timestamp) AT TIME ZONE 'America/New_York')::date
        FROM flow_by_contract
        WHERE symbol = %s
          AND (timestamp AT TIME ZONE 'America/New_York')::date < %s::date
        """,
        (symbol, current_date),
    )
    row = cur.fetchone()
    prior_date = row[0] if row else None
    return current_date, prior_date


def _upsert_window(cur, symbol: str, session_start: datetime, session_end: datetime) -> int:
    cur.execute(
        SNAPSHOT_UPSERT_PSYCOPG2,
        {
            "symbol": symbol,
            "session_start": session_start,
            "session_end": session_end,
            "strikes": None,
            "expirations": None,
        },
    )
    return cur.rowcount


def backfill_symbol(conn, symbol: str, explicit_date: Optional[str]) -> int:
    """UPSERT the current and prior session snapshot windows for a symbol.

    Returns the total number of rows upserted across both windows.
    """
    cur = conn.cursor()
    current_date, prior_date = _resolve_dates(cur, symbol, explicit_date)
    if current_date is None:
        logger.warning("  %s: no flow_by_contract rows — skipping", symbol)
        return 0

    total = 0
    cur_start, cur_end = _session_window(current_date, is_current=True)
    n = _upsert_window(cur, symbol, cur_start, cur_end)
    total += n
    logger.info(
        "  %s current %s: upserted %d rows (window [%s, %s])",
        symbol,
        current_date,
        n,
        cur_start.isoformat(),
        cur_end.isoformat(),
    )

    if prior_date is None:
        logger.info("  %s: no prior session with data — current only", symbol)
        return total
    pri_start, pri_end = _session_window(prior_date, is_current=False)
    n = _upsert_window(cur, symbol, pri_start, pri_end)
    total += n
    logger.info(
        "  %s prior %s: upserted %d rows (window [%s, %s])",
        symbol,
        prior_date,
        n,
        pri_start.isoformat(),
        pri_end.isoformat(),
    )
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--symbols",
        default="SPY",
        help="Comma-separated underlying symbols (default: SPY).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "Anchor the current session to this YYYY-MM-DD ET date "
            "(default: data-driven from MAX(timestamp), like the API)."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Emit DEBUG-level logs.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    logger.info(
        "Backfilling flow_series_5min (current + prior) for %s",
        ",".join(symbols),
    )

    with db_connection() as conn:
        try:
            for symbol in symbols:
                backfill_symbol(conn, symbol, args.date)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
