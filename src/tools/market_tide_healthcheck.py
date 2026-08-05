"""Validate whether the live database can publish a Market Tide score.

Market Tide is computed on demand; there is no Market Tide table to backfill.
This check identifies the upstream input that prevents a score from publishing:
the active universe, option-chain heartbeat, GEX summary, flow facts, or the
historical normalization samples.

Usage:
    python -m src.tools.market_tide_healthcheck
    python -m src.tools.market_tide_healthcheck --json
    python -m src.tools.market_tide_healthcheck --freshness-minutes 15

Exit codes:
    0 -- participation meets the requested minimum.
    1 -- too few active symbols have fresh chain and GEX inputs.
    2 -- database connection or query error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Iterable, Sequence

from src.api.market_tide import _inputs_are_fresh
from src.database.connection import db_connection

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SymbolStatus:
    symbol: str
    status: str
    gex_timestamp: datetime | None
    chain_timestamp: datetime | None
    latest_flow_fact: datetime | None
    flow_samples: int
    gamma_samples: int


def _evaluate(rows: Iterable[tuple], anchor: datetime, freshness: timedelta) -> list[SymbolStatus]:
    statuses: list[SymbolStatus] = []
    for row in rows:
        symbol, gex_ts, chain_ts, flow_ts, flow_samples, gamma_samples = row
        flow_input_ts = max(
            (timestamp for timestamp in (chain_ts, flow_ts) if timestamp is not None),
            default=None,
        )
        if flow_input_ts is None:
            status = "missing_flow_input"
        elif gex_ts is None:
            status = "missing_gex"
        elif not _inputs_are_fresh(gex_ts, flow_input_ts, anchor, freshness):
            status = "stale_or_misaligned"
        else:
            status = "ready"
        statuses.append(
            SymbolStatus(
                symbol=symbol,
                status=status,
                gex_timestamp=gex_ts,
                chain_timestamp=chain_ts,
                latest_flow_fact=flow_ts,
                flow_samples=int(flow_samples or 0),
                gamma_samples=int(gamma_samples or 0),
            )
        )
    return statuses


def _fetch(cur, window_minutes: int) -> tuple[datetime | None, list[tuple]]:
    cur.execute("""
        WITH raw AS (
            SELECT LEAST(
                (SELECT MAX(timestamp) FROM gex_summary),
                (SELECT MAX(timestamp) FROM option_chains_latest)
            ) AS ts
        )
        SELECT CASE
            WHEN (ts AT TIME ZONE 'America/New_York')::time > TIME '16:00'
            THEN (
                (ts AT TIME ZONE 'America/New_York')::date + TIME '16:00'
            ) AT TIME ZONE 'America/New_York'
            ELSE ts
        END
        FROM raw
        """)
    anchor = cur.fetchone()[0]
    if anchor is None:
        return None, []

    cur.execute(
        """
        WITH active AS (
            SELECT s.symbol
            FROM symbols s
            WHERE COALESCE(s.is_active, TRUE) = TRUE
              AND EXISTS (
                  SELECT 1 FROM option_chains_latest ocl
                  WHERE ocl.underlying = s.symbol
              )
              AND EXISTS (
                  SELECT 1 FROM gex_summary gs
                  WHERE gs.underlying = s.symbol
              )
        ),
        flow_buckets AS (
            SELECT
                f.symbol,
                DATE_TRUNC('hour', f.timestamp)
                  + FLOOR(EXTRACT(MINUTE FROM f.timestamp) / %s)
                    * (%s::text || ' minutes')::interval AS bucket
            FROM flow_contract_facts f
            JOIN active a USING (symbol)
            WHERE f.timestamp >= %s - INTERVAL '30 days'
              AND f.timestamp < %s
            GROUP BY f.symbol, bucket
        )
        SELECT
            a.symbol,
            (SELECT MAX(gs.timestamp) FROM gex_summary gs
             WHERE gs.underlying = a.symbol AND gs.timestamp <= %s) AS gex_timestamp,
            (SELECT MAX(ocl.timestamp) FROM option_chains_latest ocl
             WHERE ocl.underlying = a.symbol AND ocl.timestamp <= %s) AS chain_timestamp,
            (SELECT MAX(f.timestamp) FROM flow_contract_facts f
             WHERE f.symbol = a.symbol AND f.timestamp <= %s) AS latest_flow_fact,
            (SELECT COUNT(*) FROM flow_buckets fb
             WHERE fb.symbol = a.symbol) AS flow_samples,
            (SELECT COUNT(*) FROM gex_summary gs
             WHERE gs.underlying = a.symbol
               AND gs.timestamp >= %s - INTERVAL '30 days'
               AND gs.timestamp < %s
               AND gs.net_gex_at_spot IS NOT NULL) AS gamma_samples
        FROM active a
        ORDER BY a.symbol
        """,
        (
            window_minutes,
            window_minutes,
            anchor,
            anchor,
            anchor,
            anchor,
            anchor,
            anchor,
            anchor,
        ),
    )
    return anchor, list(cur.fetchall())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--window", type=int, choices=(5, 15, 30, 60), default=15)
    parser.add_argument("--freshness-minutes", type=int, default=10)
    parser.add_argument("--minimum-participation", type=float, default=60.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                anchor, rows = _fetch(cur, args.window)
            conn.rollback()
    except Exception:
        logger.exception("market-tide healthcheck: database error")
        return 2

    if anchor is None:
        logger.error("No common GEX/option-chain anchor is available")
        return 1

    statuses = _evaluate(rows, anchor, timedelta(minutes=args.freshness_minutes))
    ready = sum(item.status == "ready" for item in statuses)
    participation = 100.0 * ready / len(statuses) if statuses else 0.0

    if args.json:
        print(
            json.dumps(
                {
                    "anchor": anchor.isoformat(),
                    "participation_pct": round(participation, 2),
                    "ready": ready,
                    "configured": len(statuses),
                    "symbols": [asdict(item) for item in statuses],
                },
                default=str,
            )
        )
    else:
        logger.warning(
            "Market Tide anchor=%s participation=%.2f%% (%d/%d)",
            anchor.isoformat(),
            participation,
            ready,
            len(statuses),
        )
        for item in statuses:
            logger.warning(
                "%s: %s chain=%s gex=%s latest_flow=%s norms(flow=%d gamma=%d)",
                item.symbol,
                item.status,
                item.chain_timestamp,
                item.gex_timestamp,
                item.latest_flow_fact,
                item.flow_samples,
                item.gamma_samples,
            )

    if participation < args.minimum_participation:
        logger.error(
            "Market Tide is not publishable: %.2f%% < %.2f%% minimum",
            participation,
            args.minimum_participation,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
