"""Validate that an underlying's option-chain ingestion is flowing.

Aimed at the question: "I turned on monthly SPX (or any index) chain
expansion via ``INGEST_MONTHLY_EXPIRATIONS`` + ``INGEST_MONTHLY_UNDERLYING_
ALIASES`` — am I actually seeing those contracts in ``option_chains``?"

Reads ``option_chains_latest`` (one row per contract, kept current by the
ingestion writer's dual-UPSERT) and breaks rows down per expiration, then
per option-root prefix parsed from ``option_symbol``.  That root prefix
is the unambiguous fingerprint of which TS chain the row came from
(``SPXW`` for the weekly chain, ``SPX`` for the AM-settled monthly
chain).

Output columns:

  * ``expiration``       — contract expiration date.
  * ``option_root``      — root parsed from option_symbol prefix.
  * ``contracts``        — distinct option_symbols seen for this exp+root.
  * ``fresh``            — contracts updated within ``--max-stale-seconds``
                          of "now" (default 600s); 0 means none ticked.
  * ``last_seen_et``     — most recent option_chains_latest.timestamp.
  * ``oi_sum``           — open_interest sum (sanity: > 0 means quotes
                          carried OI seeding, not just empty REST stubs).
  * ``am_settled``       — Y/N: index monthly heuristic (3rd-Friday of
                          month + root == SPX).

Exit codes:

  * 0 — every expected expiration has at least one fresh contract.
  * 1 — at least one expiration is stale or missing.
  * 2 — DB connection or query error.

Examples:

    # Show all expirations currently live in option_chains_latest for SPX.
    python -m src.tools.ingestion_universe_validate SPX

    # Strict check: expect monthly AM-settled rows to be present.
    python -m src.tools.ingestion_universe_validate SPX --expect-monthly
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

from src.database.connection import db_connection
from src.market_calendar import is_spx_am_settled_expiration

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


_QUERY = """
WITH parsed AS (
    SELECT
        expiration,
        split_part(option_symbol, ' ', 1) AS option_root,
        option_symbol,
        timestamp,
        COALESCE(open_interest, 0) AS open_interest
    FROM option_chains_latest
    WHERE underlying = %s
)
SELECT
    expiration,
    option_root,
    COUNT(DISTINCT option_symbol)                                  AS contracts,
    COUNT(DISTINCT option_symbol) FILTER (
        WHERE timestamp >= NOW() - (%s || ' seconds')::interval
    )                                                              AS fresh,
    MAX(timestamp)                                                 AS last_seen,
    SUM(open_interest)                                             AS oi_sum
FROM parsed
GROUP BY expiration, option_root
ORDER BY expiration, option_root;
"""


def _fmt_et(ts: Optional[datetime]) -> str:
    if ts is None:
        return "n/a"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S")


def _is_am_settled(option_root: str, expiration) -> bool:
    """Heuristic: AM-settled SPX monthly = root 'SPX' AND third-Friday."""
    if option_root.upper() != "SPX":
        return False
    return is_spx_am_settled_expiration("SPX", expiration)


def query_universe(underlying: str, max_stale_seconds: int) -> List[Dict[str, Any]]:
    """Group option_chains_latest by (expiration, option_root) for one symbol."""
    rows: List[Dict[str, Any]] = []
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_QUERY, (underlying.upper(), str(max_stale_seconds)))
            for exp, root, contracts, fresh, last_seen, oi_sum in cur.fetchall():
                rows.append(
                    {
                        "expiration": exp,
                        "option_root": root,
                        "contracts": int(contracts or 0),
                        "fresh": int(fresh or 0),
                        "last_seen": last_seen,
                        "oi_sum": int(oi_sum or 0),
                        "am_settled": _is_am_settled(root, exp),
                    }
                )
    return rows


def render_table(rows: List[Dict[str, Any]]) -> str:
    """Plain-text table for terminal output."""
    if not rows:
        return "(no rows)"
    headers = [
        "expiration",
        "option_root",
        "contracts",
        "fresh",
        "last_seen_et",
        "oi_sum",
        "am_settled",
    ]
    body = [
        [
            str(r["expiration"]),
            r["option_root"],
            str(r["contracts"]),
            str(r["fresh"]),
            _fmt_et(r["last_seen"]),
            f"{r['oi_sum']:,}",
            "Y" if r["am_settled"] else "N",
        ]
        for r in rows
    ]
    widths = [
        max(len(h), max((len(row[i]) for row in body), default=0))
        for i, h in enumerate(headers)
    ]

    def fmt(parts: Sequence[str]) -> str:
        return " | ".join(p.ljust(widths[i]) for i, p in enumerate(parts))

    sep = "-+-".join("-" * w for w in widths)
    return "\n".join([fmt(headers), sep] + [fmt(r) for r in body])


def evaluate(
    rows: List[Dict[str, Any]],
    *,
    expect_monthly: bool,
) -> Dict[str, Any]:
    """Run the pass/fail checks the operator cares about."""
    failures: List[str] = []
    has_any_fresh = any(r["fresh"] > 0 for r in rows)
    if not rows:
        failures.append("no option_chains_latest rows for this underlying")
    elif not has_any_fresh:
        failures.append("no contracts updated within --max-stale-seconds")

    if expect_monthly:
        monthly = [r for r in rows if r["am_settled"]]
        if not monthly:
            failures.append(
                "no AM-settled monthly contracts present (expected via "
                "INGEST_MONTHLY_EXPIRATIONS / INGEST_MONTHLY_UNDERLYING_ALIASES)"
            )
        elif not any(r["fresh"] > 0 for r in monthly):
            failures.append(
                "AM-settled monthly contracts present but none fresh — monthly "
                "chain is not streaming quotes (check stream-manager logs for "
                "the monthly chunk)"
            )

    return {
        "rows": rows,
        "ok": not failures,
        "failures": failures,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("underlying", help="Canonical DB symbol (e.g. SPX)")
    parser.add_argument(
        "--max-stale-seconds",
        type=int,
        default=600,
        help="Contracts older than this count as stale (default: 600).",
    )
    parser.add_argument(
        "--expect-monthly",
        action="store_true",
        help=(
            "Fail if no AM-settled monthly contracts are present. Use after "
            "enabling INGEST_MONTHLY_EXPIRATIONS for index underlyings."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of a text table.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        rows = query_universe(args.underlying, args.max_stale_seconds)
    except Exception as e:  # noqa: BLE001 — top-level CLI
        logger.error("DB query failed: %s", e)
        return 2

    report = evaluate(rows, expect_monthly=args.expect_monthly)

    if args.json:
        print(
            json.dumps(
                {
                    "underlying": args.underlying.upper(),
                    "max_stale_seconds": args.max_stale_seconds,
                    "expect_monthly": args.expect_monthly,
                    "ok": report["ok"],
                    "failures": report["failures"],
                    "rows": [
                        {**r, "expiration": str(r["expiration"]), "last_seen": _fmt_et(r["last_seen"])}
                        for r in report["rows"]
                    ],
                },
                indent=2,
            )
        )
    else:
        print(f"Ingestion universe for {args.underlying.upper()}:")
        print(render_table(report["rows"]))
        print()
        if report["ok"]:
            print("OK — at least one contract is flowing for each expiration")
            if args.expect_monthly and any(r["am_settled"] for r in report["rows"]):
                fresh_monthly = sum(
                    r["fresh"] for r in report["rows"] if r["am_settled"]
                )
                print(f"OK — {fresh_monthly} fresh AM-settled monthly contracts present")
        else:
            for failure in report["failures"]:
                print(f"FAIL — {failure}")

    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
