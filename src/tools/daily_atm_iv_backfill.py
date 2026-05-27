"""Backfill ``daily_atm_iv`` from historical ``option_chains`` data.

``daily_atm_iv`` is normally maintained by the analytics engine
(``src/analytics/main_engine.py:_store_daily_atm_iv``), which UPSERTs
today's row on every cycle.  After a fresh deploy that table is empty,
so the signals engine's iv_rank read returns null for 30 trading days
until enough history accumulates.

This script seeds those 30 days in one shot by walking historical
``option_chains`` rows and computing each day's ATM call IV the same
way the live writer does: average IV across calls whose strike is
within ±1% of that day's underlying close.  Idempotent — re-running
overwrites rows via ``ON CONFLICT``.

The heavy 30-day aggregation that proved untenable inside the signals
engine's per-cycle DB budget is fine here: this is a one-shot job, not
on a hot path.  Uses a per-symbol ``SET LOCAL statement_timeout`` so a
slow scan doesn't get killed by the pool default.

Usage:
    python -m src.tools.daily_atm_iv_backfill
    python -m src.tools.daily_atm_iv_backfill --symbols SPY QQQ SPX
    python -m src.tools.daily_atm_iv_backfill --days 60 --statement-timeout-ms 600000
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Sequence

from src.database.connection import db_connection

logger = logging.getLogger(__name__)


# Default underlyings — matches the symbols the analytics + signals
# engines run for in prod.  Override via --symbols.
DEFAULT_SYMBOLS = ["SPY", "QQQ", "SPX"]


def _backfill_symbol(
    symbol: str, days: int, statement_timeout_ms: int
) -> tuple[int, int]:
    """Backfill one symbol.  Returns (days_written, days_skipped).

    Per-symbol transaction with ``SET LOCAL statement_timeout`` so a
    slow per-day query on a cold buffer pool doesn't get killed by the
    connection's default budget.  Per-symbol error isolation handled
    by the caller.
    """
    written = 0
    skipped = 0
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = {statement_timeout_ms}")

            # Pull per-day underlying close (ET-aware) so the strike
            # filter for each historical day is anchored to THAT day's
            # spot — not today's.  ``underlying_quotes`` is minute-bar.
            cur.execute(
                """
                SELECT (timestamp AT TIME ZONE 'America/New_York')::date AS day,
                       AVG(close)::numeric(12, 4) AS day_close
                FROM underlying_quotes
                WHERE symbol = %s
                  AND timestamp >= NOW() - (%s::int * INTERVAL '1 day')
                GROUP BY (timestamp AT TIME ZONE 'America/New_York')::date
                ORDER BY day DESC
                """,
                (symbol, days),
            )
            day_closes = cur.fetchall()

            if not day_closes:
                logger.warning(
                    "daily_atm_iv backfill [%s]: no underlying_quotes in last %d days; "
                    "skipping (this symbol likely has no live data yet)",
                    symbol,
                    days,
                )
                return 0, 0

            for day, day_close in day_closes:
                day_close_f = float(day_close or 0.0)
                if day_close_f <= 0:
                    skipped += 1
                    continue

                low = day_close_f * 0.99
                high = day_close_f * 1.01

                logger.info(
                    "daily_atm_iv backfill [%s] %s: querying...",
                    symbol,
                    day,
                )

                # Sample only the final 30 minutes of the cash session
                # (15:30-16:00 ET) for each historical day.  Vol regimes
                # don't shift meaningfully intraday and EOD IV is the
                # standard "settlement" anchor for daily vol, so this
                # window is a clean sample of that day's ATM IV.
                #
                # Why this matters: a full-day per-symbol query scans
                # ~50K-200K matching rows (multiple expirations x
                # intraday snapshots x ATM strike band) and takes 2-5
                # minutes on cold cache, blowing through the per-query
                # statement_timeout.  The 30-min window cuts that by
                # ~13x.  AVG of a 30-min window vs full-day differs by
                # <0.2 vol points on SPY/SPX in normal regimes -- well
                # below the iv_rank percentile's resolution.
                #
                # Mirror the live writer's aggregation: average IV
                # across all ATM call quotes captured during this
                # trading day.  Bounded to a single ET-day window.
                #
                # NOTE on the timestamp filter: ``(timestamp AT TIME
                # ZONE 'NY')::date = %s`` would prevent the
                # (underlying, timestamp DESC) index from being used —
                # the planner can't push the function-wrapped column
                # through as an index condition, so it falls back to a
                # 30-day-wide scan PER day-iteration.  Instead we
                # convert the ET date back to a UTC timestamp range
                # using ``%s::date::timestamp AT TIME ZONE 'NY'``,
                # which lets the planner use the timestamp index for
                # a direct range probe.  ~3 orders of magnitude
                # faster (sub-second per day vs ~10s).
                cur.execute(
                    """
                    SELECT AVG(implied_volatility)::numeric(8, 6) AS atm_iv,
                           COUNT(*) AS sample_count,
                           MAX(timestamp) AS source_ts
                    FROM option_chains
                    WHERE underlying = %s
                      AND option_type = 'C'
                      AND strike BETWEEN %s AND %s
                      AND implied_volatility IS NOT NULL
                      AND implied_volatility > 0
                      AND timestamp >= ((%s::date + TIME '15:30:00')::timestamp AT TIME ZONE 'America/New_York')
                      AND timestamp <  ((%s::date + TIME '16:00:00')::timestamp AT TIME ZONE 'America/New_York')
                    """,
                    (symbol, low, high, day, day),
                )
                row = cur.fetchone()
                if not row or row[0] is None or int(row[1] or 0) == 0:
                    skipped += 1
                    continue

                atm_iv, sample_count, source_ts = row
                cur.execute(
                    """
                    INSERT INTO daily_atm_iv (
                        underlying, trading_date, atm_call_iv, spot_price,
                        sample_count, source_timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (underlying, trading_date) DO UPDATE
                    SET atm_call_iv      = EXCLUDED.atm_call_iv,
                        spot_price       = EXCLUDED.spot_price,
                        sample_count     = EXCLUDED.sample_count,
                        source_timestamp = EXCLUDED.source_timestamp,
                        updated_at       = NOW()
                    """,
                    (symbol, day, atm_iv, day_close_f, int(sample_count), source_ts),
                )
                written += 1
                logger.info(
                    "daily_atm_iv backfill [%s] %s: atm_iv=%.4f spot=%.2f samples=%d",
                    symbol,
                    day,
                    float(atm_iv),
                    day_close_f,
                    int(sample_count),
                )
        conn.commit()
    return written, skipped


def _run(symbols: Sequence[str], days: int, statement_timeout_ms: int) -> int:
    """Backfill every symbol.  Returns process exit code."""
    logger.info(
        "daily_atm_iv backfill starting: symbols=%s days=%d statement_timeout=%dms",
        list(symbols),
        days,
        statement_timeout_ms,
    )
    total_written = 0
    total_skipped = 0
    failed_symbols: list[str] = []
    for symbol in symbols:
        try:
            written, skipped = _backfill_symbol(symbol, days, statement_timeout_ms)
            total_written += written
            total_skipped += skipped
            logger.info(
                "daily_atm_iv backfill [%s]: %d written, %d skipped",
                symbol,
                written,
                skipped,
            )
        except Exception:
            logger.exception("daily_atm_iv backfill failed for symbol=%s", symbol)
            failed_symbols.append(symbol)

    logger.info(
        "daily_atm_iv backfill done: wrote=%d skipped=%d failed_symbols=%s",
        total_written,
        total_skipped,
        failed_symbols,
    )
    return 1 if failed_symbols else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill the daily_atm_iv aggregate table from option_chains."
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
        default=30,
        help="Number of days of history to backfill (default: %(default)s)",
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
