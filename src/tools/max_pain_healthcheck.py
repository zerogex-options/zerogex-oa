"""Verify the max-pain snapshot kept up with the available chain data.

Catches the silent-failure mode where ``zerogex-oa-max-pain-refresh.timer``
has stopped firing (or always errors) but ``/api/max-pain/current`` keeps
serving an ever-staler snapshot.  Used as the refresh service's
``ExecStartPost`` atomic post-validation and as a standalone monitor.

Freshness is defined against the *data*, not wall-clock: a refresh only
upserts when ``option_chains`` has a newer trading day than the snapshot
(its ``should_refresh`` gate), so on weekends/holidays ``updated_at``
legitimately doesn't advance.  We therefore compare, per symbol, the
snapshot's latest ``as_of_date`` to the latest trading date present in
``option_chains`` — the same notion of "caught up" the refresh itself
uses — which never false-alarms over a market closure.

Exit codes:
    0 — every expected symbol's snapshot as_of_date == its latest
        option_chains trading date (caught up).
    1 — at least one symbol is behind its available chain data.
        With ``--strict``, a symbol with no snapshot row also fails.
    2 — DB connection or query error.

A symbol with no snapshot row does NOT fail by default (a freshly added
underlying with no chain history yet is a legitimate state); ``--strict``
enforces full coverage.

Usage:
    python -m src.tools.max_pain_healthcheck
    python -m src.tools.max_pain_healthcheck --symbols SPY SPX QQQ
    python -m src.tools.max_pain_healthcheck --strict --json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import date
from typing import Sequence

from src import config
from src.database.connection import db_connection

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SymbolStatus:
    symbol: str
    status: str  # "fresh" | "behind" | "missing"
    snapshot_date: date | None
    chain_date: date | None


def _evaluate(cur, symbols: Sequence[str]) -> list[SymbolStatus]:
    out: list[SymbolStatus] = []
    for sym in symbols:
        cur.execute(
            "SELECT MAX(as_of_date) FROM max_pain_oi_snapshot WHERE symbol = %s",
            (sym,),
        )
        snap_date = cur.fetchone()[0]
        cur.execute(
            """
            SELECT (MAX(timestamp) AT TIME ZONE 'America/New_York')::date
            FROM option_chains
            WHERE underlying = %s
            """,
            (sym,),
        )
        chain_date = cur.fetchone()[0]

        if snap_date is None:
            status = "missing"
        elif chain_date is not None and snap_date < chain_date:
            status = "behind"
        else:
            status = "fresh"
        out.append(SymbolStatus(sym, status, snap_date, chain_date))
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Symbols to check (default: MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Also fail (exit 1) on a symbol that has no snapshot row at all.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--log-level", default="INFO")
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
        logger.warning("No symbols to check")
        return 0

    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                statuses = _evaluate(cur, symbols)
            conn.rollback()  # read-only; release the snapshot cleanly
    except Exception:
        logger.exception("max-pain healthcheck: DB error")
        return 2

    behind = [s for s in statuses if s.status == "behind"]
    missing = [s for s in statuses if s.status == "missing"]

    if args.json:
        print(
            json.dumps(
                [
                    asdict(s)
                    | {"snapshot_date": str(s.snapshot_date), "chain_date": str(s.chain_date)}
                    for s in statuses
                ]
            )
        )
    else:
        for s in statuses:
            logger.info(
                "%s: %s (snapshot=%s chain=%s)",
                s.symbol,
                s.status,
                s.snapshot_date,
                s.chain_date,
            )

    if behind:
        logger.error(
            "max-pain snapshot behind chain data for: %s",
            ", ".join(s.symbol for s in behind),
        )
        return 1
    if missing and args.strict:
        logger.error(
            "max-pain snapshot missing for: %s (strict)",
            ", ".join(s.symbol for s in missing),
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
