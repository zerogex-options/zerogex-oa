"""Backfill ``forced_flow_profile`` from historical ``option_chains`` snapshots.

The live analytics cycle writes one ``forced_flow_profile`` row per run going
forward, but the table is empty on first deploy -- so the Charm-into-Close
**track record** (``/api/forced-flow/backtest``) has nothing to score until days
of history accrue. This tool reconstructs the missing history: for each past
trading day it rebuilds the 09:35-10:30 ET "morning" option book from
``option_chains`` (which retains ``implied_volatility``), reprices the forced
flow, and writes the row the live cycle would have written that morning.

It reuses the engine's own ``_calculate_forced_flow`` / ``_store_forced_flow``,
so a backfilled row is identical to a live one -- same reprice, same model-A
sign convention, same ``close_charm_flow`` headline. Idempotent: the store's
``ON CONFLICT (underlying, timestamp) DO UPDATE`` makes re-running a range a
safe overwrite.

NOTE: this backfills only the *track record*. The Curve / Charm-into-Close /
Vanna Ladder / Surface / Rail charts are computed live from the latest snapshot
and read no history -- they populate on their own once ingestion has a recent
``option_chains`` snapshot (see --help epilog).

Scope: ``option_chains`` is pruned at ``DATA_RETENTION_DAYS`` (~90d), so the
reachable window is roughly the last 90 days.

Usage:
    python -m src.tools.forced_flow_backfill                       # 60d, all syms
    python -m src.tools.forced_flow_backfill --days 90 --underlyings SPY SPX
    python -m src.tools.forced_flow_backfill --start 2026-05-01 --end 2026-06-30
    python -m src.tools.forced_flow_backfill --days 30 --dry-run   # preview only
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

from src.analytics.main_engine import ET, AnalyticsEngine
from src.config import SIGNALS_UNDERLYINGS
from src.database.connection import db_connection

logger = logging.getLogger(__name__)

# How far back before the morning snapshot to reach for each contract's latest
# quote. Two hours comfortably covers the pre-open + early-session requote of
# every active contract without pulling a stale prior-session quote.
_CONTRACT_LOOKBACK = timedelta(hours=2)

# The morning window (ET hour, minute) the track record reads its charm forecast
# from. Kept in lockstep with DatabaseManager.get_charm_backtest_sessions so a
# backfilled row lands exactly where the backtest looks for it.
_MORNING_START = (9, 35)
_MORNING_END = (10, 30)

# Earliest snapshot inside ONE day's morning window. Plain timestamp-range MIN so
# it rides idx_option_chains_underlying_timestamp. The functional form --
# (timestamp AT TIME ZONE 'America/New_York')::time BETWEEN ... over a 90-day
# span -- cannot use the timestamp index and seq-scans the whole table (that is
# the statement-timeout you hit).
_MIN_MORNING_TS_SQL = """
    SELECT MIN(timestamp)
    FROM option_chains
    WHERE underlying = %s
      AND timestamp >= %s
      AND timestamp <= %s
"""

# Latest quote per contract as of the morning timestamp: the same
# latest-per-option_symbol shape the live snapshot builds, minus the spot-anchor
# / staleness machinery (irrelevant to a fixed historical instant). Only the
# columns build_legs needs are selected.
_ASOF_OPTIONS_SQL = """
    SELECT DISTINCT ON (option_symbol)
           option_symbol, strike, expiration, option_type,
           open_interest, implied_volatility
    FROM option_chains
    WHERE underlying = %s
      AND timestamp <= %s
      AND timestamp > %s
      AND implied_volatility IS NOT NULL
      AND open_interest > 0
      AND expiration >= %s
    ORDER BY option_symbol, timestamp DESC
"""

_ASOF_SPOT_SQL = """
    SELECT close
    FROM underlying_quotes
    WHERE symbol = %s AND timestamp <= %s
    ORDER BY timestamp DESC
    LIMIT 1
"""


def _default_underlyings() -> List[str]:
    raw = SIGNALS_UNDERLYINGS or "SPY"
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def _morning_timestamps(cursor, db_symbol: str, start: datetime, end: datetime) -> List[datetime]:
    """Earliest option-chain timestamp in each day's 09:35-10:30 ET window.

    Iterates day-by-day with tz-aware ET bounds (DST handled by pytz) and a
    plain index-friendly MIN per day, instead of one functional-expression scan
    across the whole span. Non-trading days return NULL and are skipped.
    """
    out: List[datetime] = []
    day = start.astimezone(ET).date()
    last = end.astimezone(ET).date()
    while day <= last:
        lo = ET.localize(datetime(day.year, day.month, day.day, *_MORNING_START))
        hi = ET.localize(datetime(day.year, day.month, day.day, *_MORNING_END))
        # Clip to the requested [start, end) so partial edge days stay in range.
        if hi >= start and lo < end:
            cursor.execute(_MIN_MORNING_TS_SQL, (db_symbol, lo, hi))
            row = cursor.fetchone()
            if row and row[0] is not None:
                out.append(row[0])
        day += timedelta(days=1)
    return out


def _asof_options(cursor, db_symbol: str, ts: datetime) -> List[Dict[str, Any]]:
    """Latest quote per contract as of ``ts`` -> option dicts for build_legs."""
    # Exclude already-expired contracts by the snapshot's ET calendar date, so
    # today's 0DTE (expiration == today) is kept and yesterday's is dropped.
    et_date = ts.astimezone(ET).date()
    cursor.execute(
        _ASOF_OPTIONS_SQL,
        (db_symbol, ts, ts - _CONTRACT_LOOKBACK, et_date),
    )
    cols = (
        "option_symbol",
        "strike",
        "expiration",
        "option_type",
        "open_interest",
        "implied_volatility",
    )
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _asof_spot(cursor, db_symbol: str, ts: datetime) -> Optional[float]:
    cursor.execute(_ASOF_SPOT_SQL, (db_symbol, ts))
    row = cursor.fetchone()
    return float(row[0]) if row and row[0] is not None else None


def backfill_symbol(
    engine: AnalyticsEngine, start: datetime, end: datetime, dry_run: bool
) -> Dict[str, int]:
    """Backfill one underlying over [start, end). Returns simple counts."""
    db_symbol = engine.db_symbol
    written = skipped = 0
    with db_connection() as conn:
        cursor = conn.cursor()
        mornings = _morning_timestamps(cursor, db_symbol, start, end)
        logger.info("%s: %d morning snapshot(s) in range", db_symbol, len(mornings))
        for ts in mornings:
            options = _asof_options(cursor, db_symbol, ts)
            spot = _asof_spot(cursor, db_symbol, ts)
            if not options or spot is None or spot <= 0:
                logger.info("  %s: no priceable book/spot -- skip", ts.isoformat())
                skipped += 1
                continue
            summary = {"underlying": db_symbol, "timestamp": ts, "underlying_price": spot}
            forced_flow = engine._calculate_forced_flow(options, summary)
            if not forced_flow:
                logger.info("  %s: degraded snapshot -- skip", ts.isoformat())
                skipped += 1
                continue
            ccf = forced_flow.get("close_charm_flow")
            smooth = forced_flow.get("close_charm_flow_smooth")
            logger.info(
                "  %s: spot %.2f, %d legs, close_charm_flow %s (smooth %s)%s",
                ts.isoformat(),
                spot,
                len(options),
                f"${ccf:,.0f}" if ccf is not None else "n/a",
                f"${smooth:,.0f}" if smooth is not None else "n/a",
                " (dry-run)" if dry_run else "",
            )
            if not dry_run:
                engine._store_forced_flow(summary, forced_flow, cursor)
                conn.commit()
            written += 1
    return {"written": written, "skipped": skipped}


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(
        description="Backfill forced_flow_profile from historical option_chains.",
        epilog=(
            "Only the /forced-flow TRACK RECORD reads this table. The live charts "
            "compute from the latest snapshot and need no backfill -- if they are "
            "blank, the engine has no option_chains snapshot inside its lookback "
            "window (raise ANALYTICS_SNAPSHOT_LOOKBACK_HOURS to reach the last "
            "session off-hours, or wait for live ingestion during RTH)."
        ),
    )
    p.add_argument("--underlyings", nargs="+", help="e.g. SPY SPX QQQ (default: signals set)")
    p.add_argument("--days", type=int, default=60, help="Trailing calendar days (default 60)")
    p.add_argument("--start", help="ISO date (overrides --days with --end)")
    p.add_argument("--end", help="ISO date exclusive (defaults to now)")
    p.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = p.parse_args(argv)

    now = datetime.now(timezone.utc)
    if args.start:
        start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc) if args.end else now
    else:
        end = now
        start = now - timedelta(days=args.days)
    if start >= end:
        logger.error("start (%s) must be before end (%s)", start, end)
        return 2

    underlyings = [s.upper() for s in (args.underlyings or _default_underlyings())]
    logger.info(
        "Backfilling forced flow for %s over %s .. %s%s",
        underlyings,
        start.date(),
        end.date(),
        " (dry-run)" if args.dry_run else "",
    )

    totals = {"written": 0, "skipped": 0}
    for sym in underlyings:
        # Isolate per-symbol failures (e.g. a slow query on the largest chain)
        # so one symbol can't abort the backfill of the others.
        try:
            engine = AnalyticsEngine(underlying=sym)
            counts = backfill_symbol(engine, start, end, args.dry_run)
        except Exception as e:
            logger.error("%s: backfill failed, continuing: %s", sym, e)
            continue
        totals["written"] += counts["written"]
        totals["skipped"] += counts["skipped"]

    logger.info(
        "Done: %d row(s) %s, %d skipped.",
        totals["written"],
        "previewed" if args.dry_run else "written",
        totals["skipped"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
