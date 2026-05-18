"""Recompute the daily max-pain OI snapshot, off the request path.

``/api/max-pain/current`` serves ``max_pain_oi_snapshot`` /
``max_pain_oi_snapshot_expiration`` as a pure cache read.  Max pain is a
*daily* figure — open interest only changes at settlement — so the heavy
multi-CTE recompute belongs in a single scheduled job that runs once a day
while the market is closed and the box is otherwise idle, NOT in a 5-minute
background loop or inline on the API request path (both of which scanned
``option_chains`` during the cash session and starved the Analytics
engine; see config.py history and PR thread).

This module is that job.  It reuses the exact, battle-tested
``DatabaseManager.refresh_max_pain_snapshots`` (per-symbol transaction +
``SET LOCAL statement_timeout`` + matching asyncpg ``timeout=``, with
per-symbol error isolation) so there is no second copy of the recompute
SQL to drift.

Wired to ``zerogex-oa-max-pain-refresh.{service,timer}`` (daily, pre-market,
after the maintenance + normalizer chain).  Safe to run by hand any time —
it is idempotent (ON CONFLICT upsert) and only writes derived state.

Usage:
    python -m src.tools.max_pain_refresh
    python -m src.tools.max_pain_refresh --symbols SPY QQQ
    python -m src.tools.max_pain_refresh --strike-limit 500 --statement-timeout-ms 300000
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Sequence

from src import config
from src.api.database import DatabaseManager

logger = logging.getLogger(__name__)


async def _run(symbols: list[str], strike_limit: int, statement_timeout_ms: int) -> int:
    """Connect, refresh every symbol, disconnect.  Returns process exit code.

    ``refresh_max_pain_snapshots`` already isolates per-symbol failures
    (logs + continues), so a single bad underlying never aborts the run.
    A hard failure here (e.g. the pool can't connect at all) returns 1 so
    the systemd unit is marked failed and surfaces in ``systemctl --failed``.
    """
    db = DatabaseManager()
    try:
        await db.connect()
    except Exception:
        logger.exception("max-pain refresh: database connect failed")
        return 1
    try:
        logger.info(
            "max-pain refresh starting: symbols=%s strike_limit=%d statement_timeout=%dms",
            symbols,
            strike_limit,
            statement_timeout_ms,
        )
        await db.refresh_max_pain_snapshots(symbols, strike_limit, statement_timeout_ms)
        logger.info("max-pain refresh complete")
        return 0
    except Exception:
        logger.exception("max-pain refresh: unexpected failure")
        return 1
    finally:
        await db.disconnect()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Symbols to refresh (default: MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS).",
    )
    parser.add_argument(
        "--strike-limit",
        type=int,
        default=config.MAX_PAIN_BACKGROUND_REFRESH_STRIKE_LIMIT,
        help="Settlement-candidate cap per expiration "
        f"(default: {config.MAX_PAIN_BACKGROUND_REFRESH_STRIKE_LIMIT}).",
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=config.MAX_PAIN_BACKGROUND_REFRESH_STATEMENT_TIMEOUT_MS,
        help="Per-statement timeout for the recompute "
        f"(default: {config.MAX_PAIN_BACKGROUND_REFRESH_STATEMENT_TIMEOUT_MS}).",
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

    symbols = (
        [s.upper() for s in args.symbols]
        if args.symbols
        else list(config.MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS)
    )
    if not symbols:
        logger.warning("No symbols to refresh (MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS is empty)")
        return 0
    if args.strike_limit <= 0:
        parser.error("--strike-limit must be positive")
    if args.statement_timeout_ms <= 0:
        parser.error("--statement-timeout-ms must be positive")

    return asyncio.run(_run(symbols, args.strike_limit, args.statement_timeout_ms))


if __name__ == "__main__":
    sys.exit(main())
