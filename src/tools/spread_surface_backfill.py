"""Seed ``spread_surface_stats`` from historical ``option_chains`` data.

The live analytics writer fills this table one time-bucket per cycle going
forward, which means a fresh deployment has no history and the Spread Surface
view can show today's curve but not whether it is unusual — the only question
it exists to answer.  This seeds the trailing window in one shot.

Backfilled and live rows land in the same population the percentile is drawn
from, so they have to be interchangeable.  Three things guarantee that, and
all three are deliberate:

* **The same reduction.**  Aggregation runs through
  ``src.analytics.spread_stats.surface_scopes`` — the module the live writer,
  the daily rollup and the API all use.  There is no second SQL
  implementation to drift from it.
* **The same persistence.**  Rows go through
  ``src.analytics.surface_store``, so the scope keys, the time-of-day grid and
  the minimum-contract floors cannot diverge between the two writers.
* **The same anchor rule.**  Each historical time bucket is read at the LAST
  chain snapshot inside it, which is what the live writer records when its
  cycle lands in that bucket.

Cost.  This is the expensive one in the family: it reads a full +/-10% /
30DTE chain once per time bucket per session, where the daily backfill reads
one chain per session.  Expect roughly 13 snapshots a day per symbol on the
default 30-minute grid.  ``--time-buckets`` thins the grid for a quick first
seed; ``--days`` and ``--symbols`` narrow it further.

Idempotent: re-running overwrites via ON CONFLICT.

Usage:
    python -m src.tools.spread_surface_backfill
    python -m src.tools.spread_surface_backfill --symbols SPX NDX --days 30
    python -m src.tools.spread_surface_backfill --time-buckets 60
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any, Dict, List, Sequence, Tuple

from src.analytics import spread_stats as spread_stats_mod
from src.analytics import surface_store
from src.config import SPREAD_SURFACE_BUCKET_MINUTES
from src.database.connection import db_connection
from src.tools.daily_spread_stats_backfill import _keep_contract

logger = logging.getLogger(__name__)

#: ES / NQ are absent for the same reason as everywhere else in this family:
#: they carry no option chain of their own, so there is no quote to measure.
DEFAULT_SYMBOLS = ["SPY", "QQQ", "SPX", "NDX"]

#: The cash session the surface covers, re-exported from the store so the
#: backfill seeds exactly the buckets the live writer will keep extending
#: and the API will clamp reads into.
SESSION_START_MIN = surface_store.SESSION_START_MIN
SESSION_END_MIN = surface_store.SESSION_END_MIN


def session_buckets(width_minutes: int) -> List[int]:
    """Every time-of-day bucket start in the cash session."""
    width = max(1, int(width_minutes))
    return list(range(SESSION_START_MIN, SESSION_END_MIN, width))


def _minutes_to_time(minutes: int) -> str:
    return f"{minutes // 60:02d}:{minutes % 60:02d}:00"


def _backfill_symbol(
    symbol: str, days: int, width_minutes: int, statement_timeout_ms: int
) -> Tuple[int, int, int]:
    """Backfill one symbol. Returns (cells_written, buckets_filled, buckets_skipped)."""
    cells = 0
    filled = 0
    skipped = 0
    widest_band = max(spread_stats_mod.MONEYNESS_BANDS)
    widest_dte = max(spread_stats_mod.DTE_UNIVERSES)
    buckets = session_buckets(width_minutes)

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = {statement_timeout_ms}")

            # Day list from underlying_quotes, the small table — and a day
            # with no underlying bar has no spot to centre the band on, so it
            # could not be measured anyway.
            cur.execute(
                """
                SELECT DISTINCT (timestamp AT TIME ZONE 'America/New_York')::date AS day
                FROM underlying_quotes
                WHERE symbol = %s
                  AND timestamp >= NOW() - (%s::int * INTERVAL '1 day')
                ORDER BY day DESC
                """,
                (symbol, days),
            )
            trading_days = [row[0] for row in cur.fetchall()]

            if not trading_days:
                logger.warning(
                    "spread_surface backfill [%s]: no underlying_quotes in the last "
                    "%d days; skipping",
                    symbol,
                    days,
                )
                return 0, 0, 0

            for day in trading_days:
                day_cells = 0
                for bucket_min in buckets:
                    end_min = min(bucket_min + width_minutes, SESSION_END_MIN)
                    # A bounded UTC range, never a ::time predicate on the
                    # indexed column: the function-wrapped form is not
                    # sargable and degrades to scanning the whole window.
                    cur.execute(
                        """
                        SELECT MAX(timestamp)
                        FROM option_chains
                        WHERE underlying = %s
                          AND timestamp >= ((%s::date + %s::time)::timestamp
                                            AT TIME ZONE 'America/New_York')
                          AND timestamp <  ((%s::date + %s::time)::timestamp
                                            AT TIME ZONE 'America/New_York')
                        """,
                        (
                            symbol,
                            day,
                            _minutes_to_time(bucket_min),
                            day,
                            _minutes_to_time(end_min),
                        ),
                    )
                    row = cur.fetchone()
                    anchor_ts = row[0] if row else None
                    if anchor_ts is None:
                        skipped += 1
                        continue

                    cur.execute(
                        """
                        SELECT close FROM underlying_quotes
                        WHERE symbol = %s AND timestamp <= %s
                        ORDER BY timestamp DESC LIMIT 1
                        """,
                        (symbol, anchor_ts),
                    )
                    spot_row = cur.fetchone()
                    spot = float(spot_row[0]) if spot_row and spot_row[0] else 0.0
                    if spot <= 0:
                        skipped += 1
                        continue

                    low = spot * (1.0 - widest_band / 100.0)
                    high = spot * (1.0 + widest_band / 100.0)
                    cur.execute(
                        """
                        SELECT option_symbol, strike, option_type, expiration,
                               bid, ask, open_interest, volume
                        FROM option_chains
                        WHERE underlying = %s
                          AND timestamp = %s
                          AND strike BETWEEN %s AND %s
                          AND expiration >= %s
                          AND expiration <= %s::date + %s::int
                        """,
                        (symbol, anchor_ts, low, high, day, day, widest_dte),
                    )
                    raw = [
                        {
                            "option_symbol": r[0],
                            "strike": r[1],
                            "option_type": r[2],
                            "expiration": r[3],
                            "bid": r[4],
                            "ask": r[5],
                            "open_interest": r[6],
                            "volume": r[7],
                        }
                        for r in cur.fetchall()
                    ]
                    raw = [
                        r
                        for r in raw
                        if _keep_contract(symbol, r["option_symbol"], r["expiration"], day)
                    ]
                    if not raw:
                        skipped += 1
                        continue

                    dte_of: Dict[Any, int] = {
                        r["expiration"]: (r["expiration"] - day).days for r in raw
                    }
                    spreads = spread_stats_mod.contract_spreads(raw, spot)
                    written = surface_store.store_surface_scopes(
                        cur,
                        symbol,
                        day,
                        bucket_min,
                        spot,
                        anchor_ts,
                        {
                            "C": [s for s in spreads if s.option_type == "C"],
                            "P": [s for s in spreads if s.option_type == "P"],
                        },
                        dte_of,
                    )
                    if written == 0:
                        skipped += 1
                        continue
                    cells += written
                    day_cells += written
                    filled += 1

                logger.info(
                    "spread_surface backfill [%s] %s: %d cells across %d buckets",
                    symbol,
                    day,
                    day_cells,
                    len(buckets),
                )
        conn.commit()
    return cells, filled, skipped


def _run(
    symbols: Sequence[str], days: int, width_minutes: int, statement_timeout_ms: int
) -> int:
    logger.info(
        "spread_surface backfill starting: symbols=%s days=%d grid=%dmin "
        "buckets/session=%d statement_timeout=%dms",
        list(symbols),
        days,
        width_minutes,
        len(session_buckets(width_minutes)),
        statement_timeout_ms,
    )
    total_cells = 0
    total_filled = 0
    total_skipped = 0
    failed: List[str] = []
    for symbol in symbols:
        try:
            cells, filled, skipped = _backfill_symbol(
                symbol, days, width_minutes, statement_timeout_ms
            )
            total_cells += cells
            total_filled += filled
            total_skipped += skipped
            logger.info(
                "spread_surface backfill [%s]: %d cells, %d buckets filled, "
                "%d skipped",
                symbol,
                cells,
                filled,
                skipped,
            )
        except Exception:
            logger.exception("spread_surface backfill failed for symbol=%s", symbol)
            failed.append(symbol)

    logger.info(
        "spread_surface backfill done: cells=%d buckets_filled=%d skipped=%d "
        "failed_symbols=%s",
        total_cells,
        total_filled,
        total_skipped,
        failed,
    )
    return 1 if failed else 0


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Seed spread_surface_stats from option_chains so Spread Surface vs "
            "History has a baseline to rank today against."
        )
    )
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument(
        "--days",
        type=int,
        default=45,
        help=(
            "Days of history to seed (default: %(default)s). Lower than the "
            "90-day option_chains retention on purpose: this reads one full "
            "chain per time bucket, so the window trades directly against "
            "runtime."
        ),
    )
    parser.add_argument(
        "--time-buckets",
        type=int,
        default=int(SPREAD_SURFACE_BUCKET_MINUTES),
        help=(
            "Grid width in minutes (default: %(default)s, matching "
            "SPREAD_SURFACE_BUCKET_MINUTES). A coarser grid seeds faster but "
            "will not line up with what the live writer records, so use it for "
            "a first look rather than the real baseline."
        ),
    )
    parser.add_argument("--statement-timeout-ms", type=int, default=300000)
    args = parser.parse_args(argv)

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", level=logging.INFO
    )
    if int(args.time_buckets) != int(SPREAD_SURFACE_BUCKET_MINUTES):
        logger.warning(
            "grid is %d min but SPREAD_SURFACE_BUCKET_MINUTES is %d — seeded rows "
            "will not share time buckets with the live writer, and the two will "
            "not rank against each other",
            int(args.time_buckets),
            int(SPREAD_SURFACE_BUCKET_MINUTES),
        )
    return _run(args.symbols, args.days, args.time_buckets, args.statement_timeout_ms)


if __name__ == "__main__":
    sys.exit(main())
