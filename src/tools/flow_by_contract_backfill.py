"""One-time backfill for flow_by_contract cumulative rollup.

The analytics engine only refreshes the current and previous 5-minute
buckets on each tick, so if you deploy the new per-contract cumulative
schema mid-session, the earlier buckets from that same session won't
be populated. This script walks through every 5-min bucket from 09:30
ET to min(now, 16:15 ET) for the requested session date and upserts
day-to-date cumulative rows per contract into flow_by_contract.

It computes cumulatives in a single query via window functions over
flow_contract_facts, so the cost scales O(M log M) in the number of
fact rows rather than O(buckets × facts). Safe to rerun — rows are
upserted via ON CONFLICT DO UPDATE.

Usage:
    python -m src.tools.flow_by_contract_backfill --symbols SPY
    python -m src.tools.flow_by_contract_backfill --symbols SPY,QQQ --date 2026-04-22
"""

import argparse
import logging
from datetime import date, datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from src.database import db_connection

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
_SESSION_OPEN = time(9, 30)
_SESSION_CLOSE = time(16, 15)
_BUCKET_MINUTES = 5


def _resolve_session_date(explicit: Optional[str]) -> date:
    if explicit:
        return datetime.strptime(explicit, "%Y-%m-%d").date()
    # Default to the current trading day in ET. Before 09:30, back up to the
    # prior weekday; on weekends, back up to Friday.
    now_et = datetime.now(_ET)
    d = now_et.date()
    if now_et.time() < _SESSION_OPEN:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _session_window(session_date: date) -> tuple[datetime, datetime]:
    """Return (session_open, effective_end) as timezone-aware ET datetimes.

    effective_end is min(now, session_close) so we never try to fill buckets
    that haven't happened yet.
    """
    session_open = datetime.combine(session_date, _SESSION_OPEN, tzinfo=_ET)
    session_close = datetime.combine(session_date, _SESSION_CLOSE, tzinfo=_ET)
    now_et = datetime.now(_ET)
    if session_date == now_et.date() and now_et < session_close:
        return session_open, now_et
    return session_open, session_close


def _fallback_underlying_price(cur, symbol: str, end_ts: datetime) -> Optional[float]:
    """Latest underlying close at or before end_ts, used when a bucket's
    facts have no underlying_price stamped on them."""
    cur.execute(
        """
        SELECT close::float
        FROM underlying_quotes
        WHERE symbol = %s AND timestamp <= %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (symbol, end_ts),
    )
    row = cur.fetchone()
    return row[0] if row else None


def backfill_symbol(conn, symbol: str, session_open: datetime, effective_end: datetime) -> int:
    """Upsert cumulative per-contract rows for every bucket in the window.

    Returns the number of rows upserted.
    """
    cur = conn.cursor()
    fallback_price = _fallback_underlying_price(cur, symbol, effective_end)

    # The CTE approach:
    #   1. Grab every fact in [session_open, effective_end) for this symbol.
    #   2. Tag each with its 5-min bucket_start.
    #   3. Aggregate each (bucket, contract) into that bucket's delta.
    #   4. Running-sum each contract's deltas across buckets to get cumulative.
    #   5. Upsert with ON CONFLICT so reruns are idempotent.
    cur.execute(
        """
        WITH facts AS (
            SELECT
                timestamp,
                symbol,
                option_type,
                strike,
                expiration,
                volume_delta,
                premium_delta,
                buy_volume,
                sell_volume,
                buy_premium,
                sell_premium,
                underlying_price
            FROM flow_contract_facts
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp <  %s
        ),
        bucketed AS (
            SELECT
                to_timestamp(
                    floor(extract(epoch FROM timestamp) / 300) * 300
                ) AS bucket_start,
                symbol,
                option_type,
                strike,
                expiration,
                volume_delta,
                premium_delta,
                buy_volume,
                sell_volume,
                buy_premium,
                sell_premium,
                underlying_price
            FROM facts
        ),
        bucket_deltas AS (
            SELECT
                bucket_start,
                symbol,
                option_type,
                strike,
                expiration,
                SUM(volume_delta)::bigint                  AS delta_raw_volume,
                SUM(premium_delta)::numeric                AS delta_raw_premium,
                SUM(buy_volume - sell_volume)::bigint      AS delta_net_volume,
                SUM(buy_premium - sell_premium)::numeric   AS delta_net_premium,
                MAX(underlying_price)::numeric             AS bucket_underlying_price
            FROM bucketed
            GROUP BY bucket_start, symbol, option_type, strike, expiration
            HAVING SUM(volume_delta) > 0
        ),
        cumulatives AS (
            SELECT
                bucket_start,
                symbol,
                option_type,
                strike,
                expiration,
                SUM(delta_raw_volume)  OVER w AS raw_volume,
                SUM(delta_raw_premium) OVER w AS raw_premium,
                SUM(delta_net_volume)  OVER w AS net_volume,
                SUM(delta_net_premium) OVER w AS net_premium,
                bucket_underlying_price
            FROM bucket_deltas
            WINDOW w AS (
                PARTITION BY symbol, option_type, strike, expiration
                ORDER BY bucket_start
            )
        )
        INSERT INTO flow_by_contract (
            timestamp,
            symbol,
            option_type,
            strike,
            expiration,
            raw_volume,
            raw_premium,
            net_volume,
            net_premium,
            underlying_price
        )
        SELECT
            bucket_start,
            symbol,
            option_type,
            strike,
            expiration,
            raw_volume,
            raw_premium,
            net_volume,
            net_premium,
            COALESCE(bucket_underlying_price, %s::numeric)
        FROM cumulatives
        ON CONFLICT (timestamp, symbol, option_type, strike, expiration)
        DO UPDATE SET
            raw_volume = EXCLUDED.raw_volume,
            raw_premium = EXCLUDED.raw_premium,
            net_volume = EXCLUDED.net_volume,
            net_premium = EXCLUDED.net_premium,
            underlying_price = EXCLUDED.underlying_price,
            updated_at = NOW()
        """,
        (symbol, session_open, effective_end, fallback_price),
    )
    return cur.rowcount


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
        help="Session date YYYY-MM-DD (default: today's trading date, ET).",
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
    session_date = _resolve_session_date(args.date)
    session_open, effective_end = _session_window(session_date)

    if effective_end <= session_open:
        logger.warning(
            "Session %s has no elapsed time (open=%s, end=%s); nothing to backfill.",
            session_date,
            session_open,
            effective_end,
        )
        return 0

    logger.info(
        "Backfilling flow_by_contract for %s: %s → %s (ET)",
        ",".join(symbols),
        session_open.strftime("%Y-%m-%d %H:%M"),
        effective_end.strftime("%H:%M"),
    )

    with db_connection() as conn:
        try:
            for symbol in symbols:
                rows = backfill_symbol(conn, symbol, session_open, effective_end)
                logger.info("  %s: upserted %d rows", symbol, rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
