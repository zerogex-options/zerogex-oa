"""Nightly archiver: copy minute option-chain rows into the durable archive.

The backtesting platform prices option legs from ``option_chains``, but that
hot table is pruned at ``DATA_RETENTION_DAYS`` (90) by ``make db-prune``. This
job copies the columns a backtest needs into ``option_chains_archive`` — a
retention-EXEMPT table (intentionally absent from ``DB_MAINTAIN_TABLES``) — so
backtests can reach past the 90-day horizon.

Idempotent: ``INSERT … SELECT … ON CONFLICT (option_symbol, timestamp) DO
NOTHING``, so re-running a day is a cheap no-op. Run it nightly for the prior
day, or with ``--days N`` / ``--start/--end`` to backfill a range before the
live rows age out.

Usage:
    python -m src.tools.backtest_archive                      # prior day, all
    python -m src.tools.backtest_archive --date 2026-06-15
    python -m src.tools.backtest_archive --days 30 --underlyings SPY SPX
    python -m src.tools.backtest_archive --start 2026-03-01 --end 2026-03-31
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from typing import Optional, Sequence

from src.config import SIGNALS_UNDERLYINGS
from src.database.connection import db_connection

logger = logging.getLogger(__name__)


# Only the columns a leg-level backtest needs (price + greeks). The volume /
# flow columns of option_chains are deliberately dropped to keep the archive
# lean enough to retain for years.
_ARCHIVE_INSERT = """
    INSERT INTO option_chains_archive
        (option_symbol, timestamp, underlying, strike, expiration, option_type,
         last, bid, ask, mid, implied_volatility, delta, gamma, theta, vega)
    SELECT
        option_symbol, timestamp, underlying, strike, expiration, option_type,
        last, bid, ask, mid, implied_volatility, delta, gamma, theta, vega
    FROM option_chains
    WHERE timestamp >= %s AND timestamp < %s
      {underlying_clause}
    ON CONFLICT (option_symbol, timestamp) DO NOTHING
"""


def _default_underlyings() -> list[str]:
    return [s.strip().upper() for s in (SIGNALS_UNDERLYINGS or "SPY").split(",") if s.strip()]


def archive_day(conn, day: date, underlyings: Optional[Sequence[str]] = None) -> int:
    """Archive one ET calendar day's option_chains rows. Returns rows inserted."""
    start = datetime(day.year, day.month, day.day)
    end = start + timedelta(days=1)
    cur = conn.cursor()
    params: list = [start, end]
    underlying_clause = ""
    if underlyings:
        underlying_clause = "AND underlying = ANY(%s)"
        params.append([u.upper() for u in underlyings])
    cur.execute(_ARCHIVE_INSERT.format(underlying_clause=underlying_clause), params)
    inserted = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    scope = ",".join(underlyings) if underlyings else "all"
    logger.info("archived %s: %d new rows (%s)", day, inserted, scope)
    return inserted


def archive_range(
    conn, start_day: date, end_day: date, underlyings: Optional[Sequence[str]] = None
) -> int:
    """Archive every day in [start_day, end_day] inclusive. Returns total rows."""
    total = 0
    day = start_day
    while day <= end_day:
        total += archive_day(conn, day, underlyings)
        day += timedelta(days=1)
    return total


def _resolve_window(args) -> tuple[date, date]:
    if args.start and args.end:
        return date.fromisoformat(args.start), date.fromisoformat(args.end)
    if args.date:
        d = date.fromisoformat(args.date)
        return d, d
    if args.days:
        end = date.today() - timedelta(days=1)
        return end - timedelta(days=args.days - 1), end
    # Default: just the prior calendar day.
    y = date.today() - timedelta(days=1)
    return y, y


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Archive option_chains into option_chains_archive")
    parser.add_argument("--date", help="single ISO date YYYY-MM-DD")
    parser.add_argument("--days", type=int, help="archive the last N days (ending yesterday)")
    parser.add_argument("--start", help="range start ISO date (with --end)")
    parser.add_argument("--end", help="range end ISO date (with --start)")
    parser.add_argument(
        "--underlyings", nargs="*", default=None,
        help="underlyings to archive (default: SIGNALS_UNDERLYINGS)",
    )
    args = parser.parse_args(argv)

    underlyings = args.underlyings if args.underlyings else _default_underlyings()
    start_day, end_day = _resolve_window(args)
    if end_day < start_day:
        parser.error("end date must be on or after start date")

    logger.info(
        "Archiving option_chains [%s … %s] for %s", start_day, end_day, ",".join(underlyings)
    )
    with db_connection() as conn:
        total = archive_range(conn, start_day, end_day, underlyings)
    logger.info("Archive complete: %d total new rows", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
