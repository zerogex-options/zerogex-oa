"""Persist the current Market Tide reading as a snapshot, off the request path.

``/api/flow/market-tide`` serves ``market_tide_snapshots`` as a pure cache
read.  This job computes the live reading for every supported window and
upserts one snapshot row per window, so the endpoint stays populated after
the cash close (frozen at the 16:00 ET reading) instead of collapsing to
``insufficient_data`` when the live source tables have no fresh cross-symbol
data — which is exactly what happens overnight, on weekends, and on holidays.

It reuses ``DatabaseManager.write_market_tide_snapshots`` (the same compute +
idempotent upsert the backfill uses), so there is no second copy of the SQL
to drift.  Only publishable readings (participation >= 60%) are stored, so the
endpoint's "latest row" is always a real reading.

Cash-session gated: the job no-ops outside 09:30-16:10 ET (weekends/holidays
included), so the wired every-5-minute timer only does real work during the
regular session, and the 09:30->16:00 series is captured with a short grace
past the close so the frozen 16:00 snapshot lands even if the final bar is
slightly late.  Use ``--force`` to bypass the gate for a manual run.

Wired to ``zerogex-oa-market-tide-refresh.{service,timer}`` (every 5 min,
Mon-Fri).  Safe to run by hand any time — it is idempotent (ON CONFLICT
upsert per 5-minute bucket) and only writes derived state.

Usage:
    python -m src.tools.market_tide_refresh
    python -m src.tools.market_tide_refresh --windows 5 15 30 60
    python -m src.tools.market_tide_refresh --force        # ignore the RTH gate
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, time
from typing import Sequence

from src.api.database import DatabaseManager
from src.api.market_tide import SUPPORTED_WINDOWS
from src.market_calendar import ET, NYSE_HOLIDAYS

logger = logging.getLogger(__name__)

# Compute through 16:10 ET so the frozen 16:00 close snapshot still lands if
# the final cash-session bar arrives a few minutes late.  After that the
# every-5-min timer keeps firing but the job no-ops (the close row is already
# written; re-running would only re-upsert the same 16:00 bucket).
_REFRESH_OPEN = time(9, 30)
_REFRESH_CLOSE_GRACE = time(16, 10)


def _within_refresh_window(now_et: datetime) -> bool:
    """True when ``now_et`` is inside the cash session (+ close grace)."""
    if now_et.weekday() > 4 or now_et.date() in NYSE_HOLIDAYS:
        return False
    return _REFRESH_OPEN <= now_et.time() <= _REFRESH_CLOSE_GRACE


async def _run(windows: list[int], force: bool, dry_run: bool = False) -> int:
    """Connect, write one snapshot per window, disconnect.  Returns exit code.

    A hard failure (pool can't connect, unexpected error) returns 1 so the
    systemd unit is marked failed and surfaces in ``systemctl --failed``.  A
    clean no-op outside the refresh window returns 0 — that is expected, not a
    failure.
    """
    now_et = datetime.now(ET)
    if not force and not _within_refresh_window(now_et):
        logger.info(
            "market-tide refresh: %s ET is outside the cash session (09:30-16:10, "
            "Mon-Fri, non-holiday) — nothing to do",
            now_et.strftime("%Y-%m-%d %H:%M:%S"),
        )
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception:
        logger.exception("market-tide refresh: database connect failed")
        return 1
    try:
        logger.info(
            "market-tide refresh starting: windows=%s force=%s dry_run=%s",
            windows,
            force,
            dry_run,
        )
        summary = await db.write_market_tide_snapshots(windows=windows, dry_run=dry_run)
        if summary.get("anchor") is None:
            logger.warning(
                "market-tide refresh: no common gex/option-chain anchor available "
                "(empty or not-yet-ingested database) — wrote nothing"
            )
            return 0
        logger.info(
            "market-tide refresh complete: anchor=%s bucket=%s wrote=%s skipped(insufficient)=%s",
            summary["anchor"],
            summary["bucket_ts"],
            summary["written"],
            summary["skipped"],
        )
        return 0
    except Exception:
        logger.exception("market-tide refresh: unexpected failure")
        return 1
    finally:
        await db.disconnect()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--windows",
        nargs="*",
        type=int,
        default=None,
        help=f"Lookback windows in minutes to write (default: {list(SUPPORTED_WINDOWS)}).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Compute even outside the cash session (for manual/testing runs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and classify the readings but do not write snapshots.",
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

    windows = list(args.windows) if args.windows else list(SUPPORTED_WINDOWS)
    if any(w <= 0 for w in windows):
        parser.error("--windows must be positive integers")

    return asyncio.run(_run(windows, args.force, args.dry_run))


if __name__ == "__main__":
    sys.exit(main())
