"""Backfill ``market_tide_snapshots`` for previous cash sessions.

Market Tide used to be computed live on every request, so nothing survived the
cash close: once the market shut, the live source tables had no fresh
cross-symbol data and the endpoint returned ``insufficient_data``.  This tool
seeds the persisted snapshot table from retained history so users can view
prior sessions (frozen at each day's 16:00 ET close) immediately after a
deploy, before the every-5-minute refresher has had time to accumulate a
series.

For every trading day in the requested range it reconstructs the reading the
same way the live path does — via ``DatabaseManager.write_market_tide_snapshots``
with an explicit historical anchor — using retained ``option_chains`` /
``gex_summary`` / ``flow_contract_facts`` history (pruned at
``DATA_RETENTION_DAYS`` ~= 90d, which bounds how far back a backfill can reach).
Only publishable readings (participation >= 60%) are stored.

By default it writes ONE snapshot per trading day: the 16:00 ET cash close,
which is what the endpoint serves once the session is over.  Pass
``--cadence-minutes N`` to also fill the intraday 09:30->16:00 series at N-minute
buckets (heavier — the live refresher builds today's intraday series going
forward, so this is only needed to reconstruct historical intraday charts).

Idempotent — re-running upserts the same (window, 5-min bucket) rows.  The
normalizer scales it reads are the current nightly caches (slowly-varying
30-day percentiles), so a backfilled reading uses the as-of-now scale; this is
the same approximation the live path and healthcheck already make.

Usage:
    python -m src.tools.market_tide_backfill                       # last 90 days, closes
    python -m src.tools.market_tide_backfill --days 30
    python -m src.tools.market_tide_backfill --start 2026-06-01 --end 2026-06-30
    python -m src.tools.market_tide_backfill --cadence-minutes 5   # + intraday series
    python -m src.tools.market_tide_backfill --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date, datetime, timedelta
from typing import Iterator, Sequence

from src.api.database import DatabaseManager
from src.api.market_tide import SUPPORTED_WINDOWS
from src.market_calendar import ET, NYSE_HOLIDAYS

logger = logging.getLogger(__name__)

# Default range depth. Matches DATA_RETENTION_DAYS (~90) — the source tables
# are pruned there, so this is the deepest a backfill can meaningfully reach.
DEFAULT_DAYS = 90


def _is_trading_day(day: date) -> bool:
    """Weekday that is not a configured NYSE holiday."""
    return day.weekday() < 5 and day not in NYSE_HOLIDAYS


def _trading_days(start: date, end: date) -> Iterator[date]:
    """Yield trading days in ``[start, end]`` inclusive, oldest first."""
    day = start
    while day <= end:
        if _is_trading_day(day):
            yield day
        day += timedelta(days=1)


def _session_anchors(day: date, cadence_minutes: int) -> list[datetime]:
    """ET-aware compute anchors for one trading day's cash session.

    ``cadence_minutes <= 0`` → just the 16:00 ET close (the reading the
    endpoint serves once the day is over).  Otherwise 09:30, 09:30+cadence,
    ..., with the 16:00 close always included last.  A single RTH day never
    crosses a DST boundary (transitions happen at 02:00 ET), so naive
    ``timedelta`` stepping within the session is safe.
    """
    close = ET.localize(datetime(day.year, day.month, day.day, 16, 0))
    if cadence_minutes <= 0:
        return [close]
    anchors: list[datetime] = []
    cursor = ET.localize(datetime(day.year, day.month, day.day, 9, 30))
    step = timedelta(minutes=cadence_minutes)
    while cursor < close:
        anchors.append(cursor)
        cursor += step
    anchors.append(close)
    return anchors


async def _backfill_day(
    db: DatabaseManager,
    day: date,
    windows: list[int],
    cadence_minutes: int,
    dry_run: bool,
) -> tuple[int, int]:
    """Backfill one trading day. Returns (rows_written, buckets_insufficient)."""
    written = 0
    insufficient = 0
    for anchor in _session_anchors(day, cadence_minutes):
        summary = await db.write_market_tide_snapshots(
            windows=windows, anchor=anchor, dry_run=dry_run
        )
        written += len(summary["written"])
        # A bucket with no publishable window contributes nothing; count it so
        # thin historical days are visible in the run summary.
        if not summary["written"]:
            insufficient += 1
    logger.info(
        "market-tide backfill %s: wrote=%d empty_buckets=%d%s",
        day.isoformat(),
        written,
        insufficient,
        " (dry-run)" if dry_run else "",
    )
    return written, insufficient


async def _run(
    windows: list[int],
    start: date,
    end: date,
    cadence_minutes: int,
    dry_run: bool,
) -> int:
    """Backfill every trading day in range. Returns process exit code."""
    days = list(_trading_days(start, end))
    logger.info(
        "market-tide backfill starting: %s .. %s (%d trading days) windows=%s "
        "cadence=%s dry_run=%s",
        start.isoformat(),
        end.isoformat(),
        len(days),
        windows,
        f"{cadence_minutes}m" if cadence_minutes > 0 else "close-only",
        dry_run,
    )
    if not days:
        logger.warning("No trading days in the requested range — nothing to do")
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception:
        logger.exception("market-tide backfill: database connect failed")
        return 1

    total_written = 0
    failed_days: list[str] = []
    try:
        for day in days:
            try:
                written, _ = await _backfill_day(db, day, windows, cadence_minutes, dry_run)
                total_written += written
            except Exception:
                # Per-day isolation: one bad day never aborts the whole run.
                logger.exception("market-tide backfill failed for day=%s", day.isoformat())
                failed_days.append(day.isoformat())
    finally:
        await db.disconnect()

    logger.info(
        "market-tide backfill done: rows_written=%d failed_days=%s",
        total_written,
        failed_days,
    )
    return 1 if failed_days else 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Trailing calendar days to backfill when --start is omitted "
        f"(default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--start",
        default=None,
        help="Inclusive start date YYYY-MM-DD (overrides --days).",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="Inclusive end date YYYY-MM-DD (default: today ET).",
    )
    parser.add_argument(
        "--windows",
        nargs="*",
        type=int,
        default=None,
        help=f"Lookback windows in minutes (default: {list(SUPPORTED_WINDOWS)}).",
    )
    parser.add_argument(
        "--cadence-minutes",
        type=int,
        default=0,
        help="Also fill the intraday 09:30-16:00 series at this bucket size. "
        "0 (default) = write only each day's 16:00 ET close.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and classify readings but do not write snapshots.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Log level (DEBUG, INFO, WARNING, ERROR).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    today_et = datetime.now(ET).date()
    if args.start:
        try:
            start = date.fromisoformat(args.start)
        except ValueError:
            parser.error("--start must be YYYY-MM-DD")
    else:
        if args.days <= 0:
            parser.error("--days must be positive")
        start = today_et - timedelta(days=args.days)

    if args.end:
        try:
            end = date.fromisoformat(args.end)
        except ValueError:
            parser.error("--end must be YYYY-MM-DD")
    else:
        end = today_et

    if end < start:
        parser.error(f"--end ({end}) is before --start ({start})")

    windows = list(args.windows) if args.windows else list(SUPPORTED_WINDOWS)
    if any(w <= 0 for w in windows):
        parser.error("--windows must be positive integers")

    return asyncio.run(_run(windows, start, end, args.cadence_minutes, args.dry_run))


if __name__ == "__main__":
    sys.exit(main())
