"""Intraday cone receipt — grades matured horizons.

Runs on a short cycle through the session and once after the close.  A claim
becomes gradeable the moment its ``target_ts`` passes; this job finds those,
reads what the tape actually did over the window, and writes the verdict once.

Two decisions here carry the integrity of the whole track record.

The window is the OPEN interval ``(forecast_ts, target_ts]``.  The anchor bar
is excluded because spot sits inside its own band by construction, so
including it could only ever flatter the verdict.

``held`` is containment over the entire window, not a check of where price
finished.  A path that pierced the band at +40 minutes and closed back inside
did not hold.  Grading that as a win would quietly redefine the published
probability into something other than what the page says it means, and would
do it in the direction that makes the numbers look better — which is exactly
the direction nobody would notice.

Like the rest of the family: never raises, exits 0 on every failure path.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.jobs.intraday_cone_model import grade_horizon

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

#: How long a matured claim may stay ungradeable before it is abandoned.
#:
#: A claim whose window has no bars at all — a feed outage, a halt, a
#: backfilled session that never landed — can never be graded, and because
#: the grader drains oldest-first a pile of those would eventually starve the
#: live ones.  Abandoning writes ``graded_at`` with ``held`` left NULL, which
#: is a THIRD state distinct from both "pending" and "graded": the reliability
#: query requires ``held IS NOT NULL``, so an abandoned claim is excluded from
#: the scoreboard without being deleted or, worse, silently counted as a win.
ABANDON_AFTER = timedelta(days=3)

#: Safety bound on the paging loop. At the default limit this covers ~1M
#: claims, far past any real backlog, and exists only so a bug cannot turn a
#: cron into an infinite loop.
MAX_BATCHES = 500


async def _grade_one(
    db: DatabaseManager, row: dict, now: datetime, dry_run: bool
) -> str:
    """Grade a single matured claim.  Returns a short outcome label."""
    symbol = row["symbol"]
    forecast_ts = row["forecast_ts"]
    horizon = int(row["horizon_min"])
    target_ts = row["target_ts"]

    extremes = await db.get_bar_extremes_between(symbol, forecast_ts, target_ts)
    window_low = extremes.get("window_low") if extremes else None
    window_high = extremes.get("window_high") if extremes else None

    if window_low is None or window_high is None:
        age = now - target_ts
        if age > ABANDON_AFTER:
            logger.warning(
                "intraday_cone_receipt: abandoning %s %s +%dm — no bars in the "
                "window after %s; excluded from the scoreboard, not scored",
                symbol, forecast_ts.isoformat(), horizon, age,
            )
            if not dry_run:
                await db.update_intraday_cone_receipt(
                    symbol=symbol, forecast_ts=forecast_ts, horizon_min=horizon,
                    graded_at=now, window_low=None, window_high=None,
                    held=None, brier=None,
                )
            return "abandoned"
        return "pending"

    verdict = grade_horizon(
        band_low=float(row["band_low"]),
        band_high=float(row["band_high"]),
        hold_prob=float(row["hold_prob"]) if row.get("hold_prob") is not None else None,
        window_low=float(window_low),
        window_high=float(window_high),
    )

    if dry_run:
        logger.info(
            "intraday_cone_receipt: DRY RUN %s %s +%dm — band [%.2f, %.2f] "
            "saw [%.2f, %.2f] -> held=%s brier=%s",
            symbol, forecast_ts.strftime("%H:%M"), horizon,
            float(row["band_low"]), float(row["band_high"]),
            float(window_low), float(window_high),
            verdict["held"], verdict["brier"],
        )
        return "dry-run"

    wrote = await db.update_intraday_cone_receipt(
        symbol=symbol,
        forecast_ts=forecast_ts,
        horizon_min=horizon,
        graded_at=now,
        window_low=float(window_low),
        window_high=float(window_high),
        held=bool(verdict["held"]),
        brier=verdict["brier"],
    )
    if not wrote:
        # Another run got there first; the trigger and the NULL guard make
        # that harmless rather than a double-count.
        return "already-graded"
    return "held" if verdict["held"] else "broke"


async def _run(args: argparse.Namespace) -> int:
    now = (
        datetime.fromisoformat(args.at).replace(tzinfo=ET)
        if args.at
        else datetime.now(tz=ET)
    )

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:  # noqa: BLE001
        logger.warning("intraday_cone_receipt: DB connect failed (%s) — exiting 0", exc)
        return 0

    try:
        tally: dict[str, int] = {}
        total = 0
        batches = 0

        # Drain the backlog in pages rather than grading one page and stopping.
        #
        # The single-query version silently truncated. The fetch orders
        # oldest-first and caps at --limit, so a backlog larger than the cap
        # left the NEWEST session ungraded — and because the calibration report
        # requires a verdict, that session simply vanished from every report.
        # It took three rounds to spot, and the giveaway was a run logging
        # "500 matured" against a limit of exactly 500.
        while batches < MAX_BATCHES:
            try:
                due = await db.get_matured_ungraded_cones(now, limit=args.limit)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "intraday_cone_receipt: fetch failed (%s) — stopping", exc
                )
                break
            if not due:
                break

            batches += 1
            departed = 0
            for row in due:
                try:
                    outcome = await _grade_one(db, row, now, args.dry_run)
                except Exception as exc:  # noqa: BLE001 — one bad row is not a run
                    logger.warning(
                        "intraday_cone_receipt: grading %s %s +%sm failed: %s",
                        row.get("symbol"), row.get("forecast_ts"),
                        row.get("horizon_min"), exc,
                    )
                    outcome = "error"
                tally[outcome] = tally.get(outcome, 0) + 1
                # Only outcomes that actually LEFT the queue count as progress.
                # "pending" is waiting on bars and "error" failed to write, so
                # both come back on the next page — counting either as a
                # departure would let a permanently bad row spin the loop.
                if outcome in ("held", "broke", "abandoned", "already-graded"):
                    departed += 1
            total += len(due)

            # A dry run never writes, so the same page would come back forever.
            if args.dry_run:
                break
            # Nothing in this page left the queue: every row is waiting on
            # bars or failing to write. Later pages are newer and no more
            # likely to be ready, so stop rather than spin — the next
            # scheduled run picks them up, and ABANDON_AFTER clears anything
            # permanently stuck.
            if departed == 0:
                break

        if total == 0:
            logger.info("intraday_cone_receipt: nothing matured to grade")
            return 0

        if batches >= MAX_BATCHES:
            logger.warning(
                "intraday_cone_receipt: stopped after %d pages of %d — a backlog "
                "this large means claims are going ungraded; raise --limit",
                batches, args.limit,
            )

        logger.info(
            "intraday_cone_receipt: %d matured across %d page(s) — %s",
            total, batches,
            ", ".join(f"{k}={v}" for k, v in sorted(tally.items())) or "nothing written",
        )
        graded = tally.get("held", 0) + tally.get("broke", 0)
        if graded:
            logger.info(
                "intraday_cone_receipt: %d graded this run, %d held (%.0f%%)",
                graded, tally.get("held", 0), tally.get("held", 0) / graded * 100.0,
            )
        return 0
    finally:
        try:
            await db.disconnect()
        except Exception:  # noqa: BLE001
            pass


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.environ.get("CONE_RECEIPT_LIMIT", "2000")),
        help="Claims fetched per page; the run pages until drained (default 2000).",
    )
    parser.add_argument(
        "--at",
        help="Grade as though it were this ET time (YYYY-MM-DDTHH:MM), for backfill.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute + log verdicts but do NOT write them.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
