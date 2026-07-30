"""Verify ``market_tide_snapshots`` is populated for every supported window.

The ``/api/flow/market-tide`` endpoint is a pure cache read over
``market_tide_snapshots``; if the table is empty for a window the endpoint
falls back to a live compute that is ``insufficient_data`` outside the cash
session.  This check confirms each window has at least one publishable
snapshot — the signal that the backfill ran and/or the refresher is writing.

It deliberately does NOT flag an *old* latest snapshot as a failure: the
design freezes the reading at the last cash close, so over a weekend the most
recent snapshot is legitimately Friday's 16:00 ET row.  Age is reported for a
human to eyeball, not gated on.

Companion to ``market_tide_healthcheck`` (which inspects the upstream inputs
that decide whether a *new* score can publish); this one inspects the
persisted output.

Usage:
    python -m src.tools.market_tide_snapshot_healthcheck
    python -m src.tools.market_tide_snapshot_healthcheck --json
    python -m src.tools.market_tide_snapshot_healthcheck --windows 5 15 30 60

Exit codes:
    0 -- every requested window has a snapshot.
    1 -- one or more windows have no snapshot (run the backfill).
    2 -- database connection or query error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Sequence

from src.api.market_tide import SUPPORTED_WINDOWS
from src.database.connection import db_connection

logger = logging.getLogger(__name__)


def _latest_for_window(cur, window_minutes: int) -> dict | None:
    cur.execute(
        """
        SELECT snapshot_ts, session_date, score, label, participation_pct,
               EXTRACT(EPOCH FROM (NOW() - snapshot_ts))::bigint AS age_seconds
        FROM market_tide_snapshots
        WHERE window_minutes = %s
        ORDER BY snapshot_ts DESC
        LIMIT 1
        """,
        (window_minutes,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    snapshot_ts, session_date, score, label, participation_pct, age_seconds = row
    return {
        "window_minutes": window_minutes,
        "snapshot_ts": snapshot_ts.isoformat() if snapshot_ts else None,
        "session_date": session_date.isoformat() if session_date else None,
        "score": float(score) if score is not None else None,
        "label": label,
        "participation_pct": float(participation_pct) if participation_pct is not None else None,
        "age_seconds": int(age_seconds) if age_seconds is not None else None,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--windows",
        nargs="*",
        type=int,
        default=None,
        help=f"Windows to check (default: {list(SUPPORTED_WINDOWS)}).",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    windows = list(args.windows) if args.windows else list(SUPPORTED_WINDOWS)

    try:
        statuses: list[dict] = []
        missing: list[int] = []
        with db_connection() as conn:
            with conn.cursor() as cur:
                for window_minutes in windows:
                    status = _latest_for_window(cur, window_minutes)
                    if status is None:
                        missing.append(window_minutes)
                        statuses.append({"window_minutes": window_minutes, "snapshot_ts": None})
                    else:
                        statuses.append(status)
            conn.rollback()
    except Exception:
        logger.exception("market-tide snapshot healthcheck: database error")
        return 2

    if args.json:
        print(json.dumps({"windows": statuses, "missing": missing}, default=str))
    else:
        for status in statuses:
            if status.get("snapshot_ts") is None:
                logger.warning("window=%sm: NO SNAPSHOT", status["window_minutes"])
            else:
                logger.info(
                    "window=%sm: %s score=%s (%s) participation=%.1f%% age=%ss session=%s",
                    status["window_minutes"],
                    status["snapshot_ts"],
                    status["score"],
                    status["label"],
                    status["participation_pct"] or 0.0,
                    status["age_seconds"],
                    status["session_date"],
                )

    if missing:
        logger.error(
            "market-tide snapshots missing for windows=%s — run: make market-tide-backfill",
            missing,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
