"""Compute and store the Gamma Regime read for one or more sessions.

The Gamma Shift page's history strip renders one bar per session — "what was
that day?" — and the same stored rows are the z-score denominator that lets
the live card say "dramatically" and mean it.  This job writes them.

Two modes, one code path:

**Intraday refresh** (the common case, wired to a timer through the session)
    Recompute today's since-open read and upsert it.  Today's row therefore
    always answers "how has today gone so far", and once the analytics engine
    stops writing at the close it naturally freezes into "what today was".
    There is deliberately no separate end-of-day job to drift from this one.

**Backfill** (``--backfill N``)
    Walk the last N weekdays and write each one.  Needed once after deploy:
    until roughly ``MIN_SESSIONS_FOR_SIGMA`` sessions exist, the live card
    normalizes against a proxy and labels its magnitude provisional.  A
    60-session backfill takes it straight to the strong claim.

Everything is idempotent.  Both modes call the same
:func:`src.analytics.regime_session.compute_session_read`, every column is
derived from immutable ``gex_by_strike`` / ``gex_summary`` history, and the
write is an ``ON CONFLICT (underlying, session_date)`` upsert — so re-running
converges rather than accumulating, and a backfill over existing rows repairs
them instead of duplicating.

Usage:
    python -m src.tools.regime_session_refresh
    python -m src.tools.regime_session_refresh --symbols SPY QQQ SPX
    python -m src.tools.regime_session_refresh --backfill 60
    python -m src.tools.regime_session_refresh --backfill 60 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime, time
from typing import Any, List, Optional, Sequence

from zoneinfo import ZoneInfo

from src.analytics import regime_session as rsess
from src.api.database import DatabaseManager
from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

#: Earliest ET time the intraday refresh has anything to say.  The read is
#: "since the open", so before there is a second snapshot to compare against
#: there is no read — running earlier just logs a skip per symbol.
_REFRESH_OPEN = time(9, 45)

#: Latest ET time the intraday refresh runs.  The analytics engine keeps
#: writing for a few minutes past 16:00 and the last row it writes is the one
#: that should freeze into the session's read, so the final fire sits after
#: that rather than on the bell.
_REFRESH_CLOSE_GRACE = time(16, 20)

#: Default symbol set.  Reuses the bulletin's ticker list (the symbols the
#: site actually publishes reads for) so a new underlying does not need two
#: separate config entries to show up on the Gamma Shift page.
_DEFAULT_SYMBOLS_ENV = "BULLETIN_TWEET_SYMBOLS"
_FALLBACK_SYMBOLS = ("SPY",)


def default_symbols() -> List[str]:
    raw = os.getenv(_DEFAULT_SYMBOLS_ENV, "")
    parsed = [s.strip().upper() for s in raw.split(",") if s.strip()]
    return parsed or list(_FALLBACK_SYMBOLS)


async def refresh_symbol(
    db: DatabaseManager,
    symbol: str,
    sessions: Sequence[date],
    *,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Write each session's read for one symbol.  Returns ``(written, skipped)``.

    Sessions are processed OLDEST FIRST so that each one's z-score
    normalizes against the sessions before it, exactly as it would have on
    the day — a backfill that ran newest-first would give the oldest rows the
    benefit of the whole window and the newest rows nothing, which is
    backwards and produces a history strip that is most confident about its
    least-supported bars.

    A session with no usable snapshot pair (market holiday, an outage, the
    day before ingestion started) is skipped rather than written from a
    single frame.
    """
    written = 0
    skipped = 0
    for session in sessions:
        try:
            row = await rsess.compute_session_read(db, symbol, session)
        except Exception:
            logger.exception("regime refresh: %s %s failed to compute", symbol, session)
            skipped += 1
            continue
        if row is None:
            logger.debug("regime refresh: %s %s has no usable snapshots", symbol, session)
            skipped += 1
            continue
        if dry_run:
            logger.info(
                "regime refresh [dry-run]: %s %s -> %s (lean %.3g, stability %.3g, "
                "norm=%s, rolloff %.1f%%)",
                symbol,
                session,
                row["state"],
                row["lean_raw"],
                row["stability_raw"],
                row["normalization"],
                100.0 * (row["rolloff_share"] or 0.0),
            )
            written += 1
            continue
        try:
            await db.upsert_regime_session(row)
        except Exception:
            logger.exception("regime refresh: %s %s failed to write", symbol, session)
            skipped += 1
            continue
        written += 1
        logger.info(
            "regime refresh: %s %s -> %s (norm=%s)",
            symbol,
            session,
            row["state"],
            row["normalization"],
        )
    return written, skipped


def within_refresh_window(now_et: datetime) -> bool:
    """True when the intraday refresh has a session to read.

    Self-gating on the calendar, the way ``market_tide_refresh`` does, is what
    lets the timer fire on a fixed schedule while the tool stays silent on
    weekends and holidays — and, more importantly, is what makes "this run
    wrote nothing" a real failure worth marking the unit on, instead of the
    normal state of every non-trading day.
    """
    if now_et.weekday() > 4 or now_et.date() in NYSE_HOLIDAYS:
        return False
    return _REFRESH_OPEN <= now_et.time() <= _REFRESH_CLOSE_GRACE


async def _run(
    symbols: Sequence[str],
    backfill: int,
    dry_run: bool,
    as_of: Optional[date],
    *,
    force: bool = False,
    db: Optional[Any] = None,
) -> int:
    """Run the refresh and return the process exit code.

    ``db`` is the same duck-typed seam :mod:`src.analytics.regime_session`
    uses: pass one and this drives it as-is (the tests supply an in-memory
    stub), leave it out and a real :class:`DatabaseManager` is opened and
    closed around the run.
    """
    # Only the intraday mode (backfill=1, no explicit --as-of) is gated: a
    # backfill is a deliberate catch-up and must run whenever it is asked to,
    # including at 3am on a Sunday after a deploy.
    intraday = backfill == 1 and as_of is None
    if intraday and not force:
        now_et = datetime.now(UTC).astimezone(ET)
        if not within_refresh_window(now_et):
            logger.info(
                "regime refresh: %s ET is outside the cash session "
                "(%s-%s, Mon-Fri, non-holiday) — nothing to do",
                now_et.strftime("%Y-%m-%d %H:%M:%S"),
                _REFRESH_OPEN.strftime("%H:%M"),
                _REFRESH_CLOSE_GRACE.strftime("%H:%M"),
            )
            return 0

    owns_db = db is None
    if owns_db:
        db = DatabaseManager()
        try:
            await db.connect()
        except Exception:
            logger.exception("regime refresh: database connect failed")
            return 1

    today = as_of or datetime.now(UTC).astimezone(ET).date()
    # recent_sessions returns newest-first; refresh_symbol wants oldest-first.
    sessions = list(reversed(rsess.recent_sessions(today, max(1, backfill))))

    try:
        logger.info(
            "regime refresh starting: symbols=%s sessions=%d (%s..%s)%s",
            list(symbols),
            len(sessions),
            sessions[0],
            sessions[-1],
            " [dry-run]" if dry_run else "",
        )
        total_written = 0
        total_skipped = 0
        for symbol in symbols:
            # Per-symbol isolation: one underlying with a degraded chain must
            # not abort the rest of the run.
            try:
                written, skipped = await refresh_symbol(
                    db, symbol, sessions, dry_run=dry_run
                )
            except Exception:
                logger.exception("regime refresh: %s aborted", symbol)
                total_skipped += len(sessions)
                continue
            total_written += written
            total_skipped += skipped
        logger.info(
            "regime refresh complete: %d written, %d skipped", total_written, total_skipped
        )
        if total_written == 0:
            # The failure that produced an empty history strip for months is
            # not a crash — it is a run that exits 0 having written nothing,
            # every day, unnoticed.  Inside the trading window that is always
            # wrong: the calendar gate above has already excluded the days
            # where writing nothing is correct.
            logger.error(
                "regime refresh wrote no sessions for %s — the history strip "
                "and the z-score denominator both stay empty",
                list(symbols),
            )
            return 1
        return 0
    except Exception:
        logger.exception("regime refresh: unexpected failure")
        return 1
    finally:
        if owns_db:
            await db.disconnect()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help=f"Symbols to refresh (default: ${_DEFAULT_SYMBOLS_ENV} or SPY).",
    )
    parser.add_argument(
        "--backfill",
        type=int,
        default=1,
        help=(
            "How many weekdays back to (re)compute, ending today. 1 = today "
            "only (the intraday refresh); 60 seeds a full trailing window."
        ),
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="Treat this YYYY-MM-DD as 'today' (for replaying a past run).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and log the reads without writing them.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run the intraday refresh even outside the cash session.",
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

    if args.backfill < 1:
        parser.error("--backfill must be at least 1")
    as_of: Optional[date] = None
    if args.as_of:
        try:
            as_of = date.fromisoformat(args.as_of)
        except ValueError:
            parser.error("--as-of must be YYYY-MM-DD")

    symbols = (
        [s.upper() for s in args.symbols] if args.symbols else default_symbols()
    )
    if not symbols:
        logger.warning("No symbols to refresh")
        return 0

    return asyncio.run(
        _run(symbols, args.backfill, args.dry_run, as_of, force=args.force)
    )


if __name__ == "__main__":
    sys.exit(main())
