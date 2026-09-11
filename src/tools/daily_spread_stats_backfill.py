"""Backfill ``daily_spread_stats`` from historical ``option_chains`` data.

``daily_spread_stats`` is normally maintained by the analytics engine
(``src/analytics/main_engine.py:_store_daily_spread_stats``), which UPSERTs
today's three rows on every cash-session cycle.  After a fresh deploy the
table is empty, and the Spread Monitor's whole comparative read — "spreads
are wider than they have been all quarter" — reduces to "spreads are 6.2%
wide", which is a number nobody can act on.  This script seeds the history
in one shot from the ~90 days of ``option_chains`` already on disk.

**The backfilled rows and the live rows must be interchangeable**, because
the API's trailing percentile puts them in the same population.  Two design
choices exist entirely to guarantee that:

1. **Same code, not equivalent SQL.**  The aggregation runs through
   ``src.analytics.spread_stats`` — the same module the live writer calls —
   rather than a hand-written ``percentile_cont`` query.  A second
   implementation in SQL would be one refactor away from disagreeing with
   the first, and the disagreement would surface as a phantom regime change
   on the exact day the backfill stopped and the live writer took over.

2. **Same anchor.**  The live row settles at the last analytics cycle before
   the 16:15 ET gate closes, so each historical day is read at ONE
   timestamp: the last chain snapshot in the late-session window.  Sampling
   a whole day instead would blend the 09:32 open — reliably the widest
   quotes of the session — into days the live writer never saw it on.

Idempotent: re-running overwrites via ``ON CONFLICT``.  Uses a per-symbol
``SET LOCAL statement_timeout`` so a cold-cache scan isn't killed by the
pool default.

Usage:
    python -m src.tools.daily_spread_stats_backfill
    python -m src.tools.daily_spread_stats_backfill --symbols SPX NDX
    python -m src.tools.daily_spread_stats_backfill --days 90 --statement-timeout-ms 600000
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any, Dict, List, Sequence, Tuple

from src.analytics import spread_stats as spread_stats_mod
from src.config import SPREAD_STATS_DTE_MAX, SPREAD_STATS_MONEYNESS_BAND_PCT
from src.database.connection import db_connection
from src.market_calendar import is_spx_am_settled_expiration

logger = logging.getLogger(__name__)


# Matches the analytics workers that run in prod.  ES / NQ are absent on
# purpose and not an oversight: they carry no option chain of their own here
# (their surfaces are SPX / NDX projections), so there are no futures quotes
# to measure a width from.  See src/api/futures_middleware.py.
DEFAULT_SYMBOLS = ["SPY", "QQQ", "SPX", "NDX"]

# The late-session window the day's anchor snapshot is drawn from.  Ends at
# the live writer's 16:15 ET gate; starts early enough that a day with a
# thin tail of snapshots still finds one.
_ANCHOR_WINDOW_START = "15:00:00"
_ANCHOR_WINDOW_END = "16:15:00"

_UPSERT_SQL = """
    INSERT INTO daily_spread_stats (
        underlying, trading_date, option_type, spot_price,
        dte_max, moneyness_band_pct,
        contract_count, tradable_count,
        two_sided_pct, zero_bid_pct, crossed_or_locked_pct,
        median_spread, median_relative_spread_pct,
        p90_relative_spread_pct, median_spread_bps_underlying,
        p90_spread_bps_underlying,
        total_open_interest, total_volume, source_timestamp
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s)
    ON CONFLICT (underlying, trading_date, option_type) DO UPDATE
    SET spot_price                   = EXCLUDED.spot_price,
        dte_max                      = EXCLUDED.dte_max,
        moneyness_band_pct           = EXCLUDED.moneyness_band_pct,
        contract_count               = EXCLUDED.contract_count,
        tradable_count               = EXCLUDED.tradable_count,
        two_sided_pct                = EXCLUDED.two_sided_pct,
        zero_bid_pct                 = EXCLUDED.zero_bid_pct,
        crossed_or_locked_pct        = EXCLUDED.crossed_or_locked_pct,
        median_spread                = EXCLUDED.median_spread,
        median_relative_spread_pct   = EXCLUDED.median_relative_spread_pct,
        p90_relative_spread_pct      = EXCLUDED.p90_relative_spread_pct,
        median_spread_bps_underlying = EXCLUDED.median_spread_bps_underlying,
        p90_spread_bps_underlying    = EXCLUDED.p90_spread_bps_underlying,
        total_open_interest          = EXCLUDED.total_open_interest,
        total_volume                 = EXCLUDED.total_volume,
        source_timestamp             = EXCLUDED.source_timestamp,
        updated_at                   = NOW()
"""


def _keep_contract(symbol: str, option_symbol: Any, expiration: Any, day: Any) -> bool:
    """Drop same-day SPX AM-settled contracts, as the live snapshot does.

    Their SOQ happened at ~09:30 ET, so by the late-session anchor they are
    hours dead — quoted, if at all, at whatever wide marks the feed last
    carried.  The analytics engine filters them out of the snapshot the live
    writer measures, so the backfill has to as well, or the seeded history
    would carry a monthly-expiry blowout the live series never records.

    SPXW (weekly, PM-settled) shares the ``$SPX.X`` underlying and must NOT
    be filtered, so the option-symbol prefix decides when it is available.
    """
    if expiration != day:
        return True
    if (option_symbol or "").upper().startswith("SPXW"):
        return True
    return not is_spx_am_settled_expiration(symbol, expiration)


def _backfill_symbol(
    symbol: str, days: int, statement_timeout_ms: int
) -> Tuple[int, int]:
    """Backfill one symbol.  Returns (days_written, days_skipped)."""
    written = 0
    skipped = 0
    band = float(SPREAD_STATS_MONEYNESS_BAND_PCT)
    dte_max = int(SPREAD_STATS_DTE_MAX)

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = {statement_timeout_ms}")

            # One anchor snapshot per ET trading day: the last chain
            # timestamp inside the late-session window.  Converting the ET
            # date back to a UTC range (rather than wrapping the column in
            # AT TIME ZONE) is what lets the planner use the
            # (underlying, timestamp DESC) index for a range probe — the
            # same trick daily_atm_iv_backfill documents at length.
            cur.execute(
                f"""
                SELECT (timestamp AT TIME ZONE 'America/New_York')::date AS day,
                       MAX(timestamp) AS anchor_ts
                FROM option_chains
                WHERE underlying = %s
                  AND timestamp >= NOW() - (%s::int * INTERVAL '1 day')
                  AND (timestamp AT TIME ZONE 'America/New_York')::time
                        >= TIME '{_ANCHOR_WINDOW_START}'
                  AND (timestamp AT TIME ZONE 'America/New_York')::time
                        <  TIME '{_ANCHOR_WINDOW_END}'
                GROUP BY (timestamp AT TIME ZONE 'America/New_York')::date
                ORDER BY day DESC
                """,
                (symbol, days),
            )
            anchors = cur.fetchall()

            if not anchors:
                logger.warning(
                    "daily_spread_stats backfill [%s]: no late-session option_chains "
                    "rows in the last %d days; skipping (no live data yet?)",
                    symbol,
                    days,
                )
                return 0, 0

            for day, anchor_ts in anchors:
                # Spot at the anchor, not a day average: the moneyness band
                # has to be centred where the market actually was when the
                # quotes were taken, or the band drifts off the money on a
                # trending day and measures a different population.
                cur.execute(
                    """
                    SELECT close
                    FROM underlying_quotes
                    WHERE symbol = %s AND timestamp <= %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (symbol, anchor_ts),
                )
                spot_row = cur.fetchone()
                spot = float(spot_row[0]) if spot_row and spot_row[0] else 0.0
                if spot <= 0:
                    skipped += 1
                    continue

                low = spot * (1.0 - band / 100.0)
                high = spot * (1.0 + band / 100.0)

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
                    (symbol, anchor_ts, low, high, day, day, dte_max),
                )
                rows: List[Dict[str, Any]] = [
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
                rows = [
                    r
                    for r in rows
                    if _keep_contract(symbol, r["option_symbol"], r["expiration"], day)
                ]

                if not rows:
                    skipped += 1
                    continue

                by_type = spread_stats_mod.aggregate_by_option_type(
                    spread_stats_mod.contract_spreads(rows, spot)
                )
                day_written = 0
                for option_type, agg in (
                    ("C", by_type["calls"]),
                    ("P", by_type["puts"]),
                    ("A", by_type["all"]),
                ):
                    if agg.contract_count == 0:
                        continue
                    cur.execute(
                        _UPSERT_SQL,
                        (
                            symbol,
                            day,
                            option_type,
                            spot,
                            dte_max,
                            band,
                            agg.contract_count,
                            agg.tradable_count,
                            agg.two_sided_pct,
                            agg.zero_bid_pct,
                            agg.crossed_or_locked_pct,
                            agg.median_spread,
                            agg.median_relative_spread_pct,
                            agg.p90_relative_spread_pct,
                            agg.median_spread_bps_underlying,
                            agg.p90_spread_bps_underlying,
                            agg.total_open_interest,
                            agg.total_volume,
                            anchor_ts,
                        ),
                    )
                    day_written += 1

                if day_written == 0:
                    skipped += 1
                    continue

                written += 1
                blended = by_type["all"]
                logger.info(
                    "daily_spread_stats backfill [%s] %s: spot=%.2f contracts=%d "
                    "median_rel=%.3f%% p90_rel=%.3f%% zero_bid=%.1f%%",
                    symbol,
                    day,
                    spot,
                    blended.contract_count,
                    blended.median_relative_spread_pct or 0.0,
                    blended.p90_relative_spread_pct or 0.0,
                    blended.zero_bid_pct,
                )
        conn.commit()
    return written, skipped


def _run(symbols: Sequence[str], days: int, statement_timeout_ms: int) -> int:
    """Backfill every symbol.  Returns process exit code."""
    logger.info(
        "daily_spread_stats backfill starting: symbols=%s days=%d "
        "dte_max=%d band=%.2f%% statement_timeout=%dms",
        list(symbols),
        days,
        int(SPREAD_STATS_DTE_MAX),
        float(SPREAD_STATS_MONEYNESS_BAND_PCT),
        statement_timeout_ms,
    )
    total_written = 0
    total_skipped = 0
    failed_symbols: List[str] = []
    for symbol in symbols:
        try:
            written, skipped = _backfill_symbol(symbol, days, statement_timeout_ms)
            total_written += written
            total_skipped += skipped
            logger.info(
                "daily_spread_stats backfill [%s]: %d days written, %d skipped",
                symbol,
                written,
                skipped,
            )
        except Exception:
            logger.exception(
                "daily_spread_stats backfill failed for symbol=%s", symbol
            )
            failed_symbols.append(symbol)

    logger.info(
        "daily_spread_stats backfill done: wrote=%d skipped=%d failed_symbols=%s",
        total_written,
        total_skipped,
        failed_symbols,
    )
    return 1 if failed_symbols else 0


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill the daily_spread_stats rollup from option_chains so the "
            "Spread Monitor's trailing comparison has history on day one."
        )
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Underlying symbols to backfill (default: %(default)s)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help=(
            "Days of history to backfill (default: %(default)s, which is the "
            "live option_chains retention horizon)"
        ),
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=300000,
        help="Per-statement timeout in ms (default: %(default)s = 5 min)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO,
    )

    return _run(args.symbols, args.days, args.statement_timeout_ms)


if __name__ == "__main__":
    sys.exit(main())
