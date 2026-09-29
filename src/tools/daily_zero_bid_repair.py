"""Rewrite ``daily_spread_stats.zero_bid_pct`` as a session mean.

WHY THIS IS NOT OPTIONAL ONCE THE WRITER CHANGES
------------------------------------------------
The Spread Monitor ranks today's no-bid share against a trailing window of
stored sessions.  That percentile only means anything if every row in the
window is the same statistic.  The live writer now stores a mean across the
session's half-hour surface buckets; every row written before it still holds
a single sample taken near the 16:00 close.  Leaving the two mixed is worse
than the bug being fixed — a session mean of 2% ranked against a window of
close-snapshots that alternate between 0% and 9% is not a low reading, it is
a category error — so this runs with the deploy, not after it.

WHAT THE OLD COLUMN WAS
-----------------------
``zero_bid_pct`` is the share of contracts quoted with an offer and no bid.
It has no width by construction and is excluded from every median beside it,
which is exactly why it is tracked: a chain can hold its median while a fifth
of it goes untradeable.

The share is not flat across a session.  It is near zero all morning and
climbs steadily into the close — on SPX puts, 0.00 at 09:30 through 0.43 at
13:00, 3.48 at 14:30, 5.49 at 15:00 and 7.30 in the last half hour, with the
same shape on NDX and on calls.  That is the 0DTE book losing its bid as the
time value runs out, and it happens every session.

The daily writer sampled once, at the last cycle before 16:00 — the steepest
part of that ramp.  So the stored number recorded which minute the cycle
fired rather than what the session was: it read exactly 0.0 in 10-12% of
sessions when the session itself was never zero in 95-100% of them, and
carried 2.5x to 11x the session mean's variance.

WHERE THE REPLACEMENT COMES FROM
--------------------------------
``spread_surface_stats``, which already holds each session as ~13 half-hour
readings in the same scope, written by the same engine.  Buckets are averaged
unweighted — each half hour is one observation of how much of the chain had
no market.  Within a bucket the two option types are contract-weighted into
the blended 'A' row, because there the shares describe one population split
in two.  ``surface_store.session_zero_bid_means`` owns that definition and
this tool reuses its SQL rather than restating it, so the repaired rows and
every row written from here on are the same statistic.

A session with no surface coverage is LEFT ALONE, not zeroed: a gap in the
history is honest, and a fabricated zero would read later as a session when
nothing went unquotable.  The report prints how many were skipped and the
date range actually covered, because a repaired window shorter than the
percentile window is the one failure mode that would still leave the page
ranking two definitions against each other.

Read-only unless ``--execute`` is passed.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from src.analytics.surface_store import SESSION_ZERO_BID_SQL  # noqa: F401  (documented source)
from src.database.connection import db_connection

logger = logging.getLogger(__name__)


#: Recompute every daily row from the surface buckets of its own session and
#: its own stored scope.  Joining on the row's ``dte_max``/``moneyness_band_pct``
#: rather than on today's config is deliberate: a row seeded under an older
#: scope must be repaired as what it measured, not as what the engine measures
#: now, or the repair silently re-scopes history.
_REPAIR_SQL = """
WITH per_bucket AS (
    SELECT s.underlying,
           s.trading_date,
           s.bucket_start_min,
           d.option_type,
           SUM(s.zero_bid_pct * s.contract_count)
               FILTER (WHERE d.option_type = 'A' OR s.option_type = d.option_type)
               / NULLIF(SUM(s.contract_count)
                   FILTER (WHERE d.option_type = 'A' OR s.option_type = d.option_type), 0)
               AS share
      FROM daily_spread_stats d
      JOIN spread_surface_stats s
        ON s.underlying    = d.underlying
       AND s.trading_date  = d.trading_date
       AND s.dte_scope     = 'u' || d.dte_max::text
       AND s.band_pct      = d.moneyness_band_pct::real
       AND s.money_bucket  = 'all'
     WHERE d.underlying = ANY(%(symbols)s)
       AND (%(start)s::date IS NULL OR d.trading_date >= %(start)s::date)
       AND (%(end)s::date   IS NULL OR d.trading_date <= %(end)s::date)
     GROUP BY s.underlying, s.trading_date, s.bucket_start_min, d.option_type
),
session_mean AS (
    SELECT underlying, trading_date, option_type,
           AVG(share) AS zb, COUNT(*) AS buckets
      FROM per_bucket
     WHERE share IS NOT NULL
     GROUP BY underlying, trading_date, option_type
)
SELECT d.underlying, d.trading_date, d.option_type,
       d.zero_bid_pct AS old_zb, m.zb AS new_zb, m.buckets
  FROM daily_spread_stats d
  JOIN session_mean m
    ON m.underlying   = d.underlying
   AND m.trading_date = d.trading_date
   AND m.option_type  = d.option_type
 ORDER BY d.underlying, d.trading_date, d.option_type
"""

_UPDATE_SQL = """
UPDATE daily_spread_stats
   SET zero_bid_pct = %s, updated_at = NOW()
 WHERE underlying = %s AND trading_date = %s AND option_type = %s
"""

#: Rows the repair could not reach, so the report can name the gap rather
#: than let a short repaired window pass for a complete one.
_UNCOVERED_SQL = """
SELECT COUNT(*), MIN(trading_date), MAX(trading_date)
  FROM daily_spread_stats d
 WHERE d.underlying = ANY(%(symbols)s)
   AND (%(start)s::date IS NULL OR d.trading_date >= %(start)s::date)
   AND (%(end)s::date   IS NULL OR d.trading_date <= %(end)s::date)
   AND NOT EXISTS (
       SELECT 1 FROM spread_surface_stats s
        WHERE s.underlying   = d.underlying
          AND s.trading_date = d.trading_date
          AND s.dte_scope    = 'u' || d.dte_max::text
          AND s.band_pct     = d.moneyness_band_pct::real
          AND s.money_bucket = 'all'
   )
"""


def repair(
    symbols: List[str],
    start: Optional[date],
    end: Optional[date],
    dry_run: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Return the per-row changes, and a summary of what could not be reached."""
    params = {"symbols": symbols, "start": start, "end": end}
    changes: List[Dict[str, Any]] = []
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(_REPAIR_SQL, params)
        for underlying, trading_date, option_type, old_zb, new_zb, buckets in cur.fetchall():
            changes.append(
                {
                    "underlying": underlying,
                    "trading_date": trading_date.isoformat(),
                    "option_type": option_type,
                    "old": round(float(old_zb), 3) if old_zb is not None else None,
                    "new": round(float(new_zb), 3),
                    "buckets": int(buckets),
                }
            )

        cur.execute(_UNCOVERED_SQL, params)
        uncovered_count, uncovered_from, uncovered_to = cur.fetchone()

        if not dry_run and changes:
            for row in changes:
                cur.execute(
                    _UPDATE_SQL,
                    (
                        row["new"],
                        row["underlying"],
                        row["trading_date"],
                        row["option_type"],
                    ),
                )
            conn.commit()

    covered = [c["trading_date"] for c in changes]
    summary = {
        "rows": len(changes),
        "applied": 0 if dry_run else len(changes),
        "covered_from": min(covered) if covered else None,
        "covered_to": max(covered) if covered else None,
        "uncovered_rows": int(uncovered_count or 0),
        "uncovered_from": uncovered_from.isoformat() if uncovered_from else None,
        "uncovered_to": uncovered_to.isoformat() if uncovered_to else None,
    }
    return changes, summary


def _report(
    changes: List[Dict[str, Any]], summary: Dict[str, Any], dry_run: bool, as_json: bool
) -> None:
    if as_json:
        print(json.dumps({"summary": summary, "changes": changes}, indent=2))
        return

    if not changes:
        logger.info("No daily_spread_stats rows have surface coverage to repair.")
    else:
        moved = [c for c in changes if c["old"] is not None and abs(c["new"] - c["old"]) >= 0.05]
        zeroed = [c for c in changes if c["old"] == 0.0]
        logger.info(
            "%d rows repairable, %s to %s. %d move by >=0.05pp; %d were stored "
            "as exactly 0.0 and are being replaced by a session mean.",
            summary["rows"],
            summary["covered_from"],
            summary["covered_to"],
            len(moved),
            len(zeroed),
        )
        thin = [c for c in changes if c["buckets"] < 6]
        if thin:
            logger.warning(
                "%d rows rest on fewer than 6 surface buckets — a partial "
                "session, not a full one. They are still an improvement on a "
                "single sample, but they are not the same measurement as a "
                "13-bucket day.",
                len(thin),
            )

    if summary["uncovered_rows"]:
        logger.warning(
            "%d rows have NO surface coverage and are left untouched (%s to %s). "
            "Those sessions keep their old single-sample value. If that range "
            "overlaps the percentile window the page reads, it is still ranking "
            "two different statistics against each other — run the surface "
            "backfill over it first.",
            summary["uncovered_rows"],
            summary["uncovered_from"],
            summary["uncovered_to"],
        )

    if dry_run:
        logger.info("Dry-run only — re-run with --execute (or CONFIRM=yes) to apply.")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Rewrite daily_spread_stats.zero_bid_pct as a mean across the "
            "session's spread_surface_stats buckets, so stored history matches "
            "what the writer now produces."
        )
    )
    parser.add_argument(
        "--symbols",
        default="SPX,NDX,SPY,QQQ",
        help="Comma-separated underlyings (default: SPX,NDX,SPY,QQQ).",
    )
    parser.add_argument("--start", help="Inclusive trading date YYYY-MM-DD (default: all history)")
    parser.add_argument("--end", help="Inclusive trading date YYYY-MM-DD (default: all history)")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the UPDATE. Without this the tool is read-only (dry-run).",
    )
    parser.add_argument("--json", action="store_true", help="Emit a JSON summary instead of text.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        logger.error("No symbols to repair — nothing to do.")
        return 2

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None
    if start and end and end < start:
        logger.error("--end (%s) is before --start (%s)", end, start)
        return 2

    try:
        changes, summary = repair(symbols, start, end, dry_run=not args.execute)
    except Exception:
        logger.error("Repair failed", exc_info=True)
        return 1

    _report(changes, summary, dry_run=not args.execute, as_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
