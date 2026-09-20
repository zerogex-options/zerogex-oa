"""Report how long the gamma flip was left UNPUBLISHED during a session.

A ``gex_summary`` row whose ``gamma_flip_point`` is NULL is the resolver
declining to publish: the span ladder found no structural interior crossing it
was willing to stand behind, so it persists NULL rather than inventing one (see
:meth:`src.analytics.main_engine.AnalyticsEngine._resolve_gamma_flip`).  That
is the correct behaviour and it is not an error.  What it is, is *invisible* --
every consumer downstream simply shows nothing.  On the NinjaTrader panel the
Flip line renders as an em dash, and a beta tester watched it sit blank across
several sessions without either of us being able to say how long, how often, or
which sessions.

This is the check that answers those three questions, retroactively.

**Why this is not the carry healthcheck.**
:mod:`src.tools.gamma_flip_carry_healthcheck` asks whether a five-minute
structure bar had a ``gex_summary`` row *at all*, and reports the stand-in when
one is missing.  This asks the opposite question about the rows that ARE there:
the row exists, the cycle ran, and the resolver declined.  A session can be
perfect by the carry check and blank all morning by this one.

**Why the database and not the journal.**  The engine already logs a full
diagnostic on the unresolved transition and every throttle interval after it,
and that log says *why* -- IV spike, 0DTE-dominant chain, stale defaulted IV,
one-sided chain.  But the journal is capped and rotates in weeks, so by the
time anybody asks about a particular Wednesday the answer is gone; that is
exactly how one of these went unanswered.  ``gex_summary`` is retention-exempt,
so the *when* and *how often* stay derivable for as long as the rows do.  Use
this to find the sessions, then the journal for the reason if it is recent
enough to still hold one.

The unit is MINUTES BLANK, not rows.  A run of NULL rows is measured from the
first NULL to the next resolved row, because that is the interval a trader
actually spent looking at an empty Flip line; counting rows instead would make
the number depend on the analytics cadence, which has changed.  A run still
open when the session ends is measured to its own last row and therefore
understates by up to one cycle.

READ-ONLY.  Runs SELECTs and a rollback; writes nothing.

Usage:
    python -m src.tools.gamma_flip_resolution_healthcheck
    python -m src.tools.gamma_flip_resolution_healthcheck --symbols NDX --sessions 30
    python -m src.tools.gamma_flip_resolution_healthcheck --max-blank-minutes 15
    python -m src.tools.gamma_flip_resolution_healthcheck --json

Symbols default to ``ANALYTICS_UNDERLYINGS`` -- the universe the analytics
engine writes ``gex_summary`` for -- so a new underlying is covered the moment
the engine starts writing it.

Exit codes:
    0 -- no session left the flip blank for longer than --max-blank-minutes.
    1 -- at least one did.
    2 -- database connection or query error.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import List, Optional, Sequence, Tuple

import pytz

from src.database.connection import db_connection
from src.symbols import get_canonical_symbol, parse_underlyings

logger = logging.getLogger("zerogex.gamma_flip_resolution_healthcheck")

ET = pytz.timezone("America/New_York")

#: The cash session.  Deliberately NOT the carry check's 09:30 + 6h45m: that
#: window matches the five-minute structure writer, whereas the question here
#: is how long a trader watching the chart saw nothing, and they stop watching
#: at the close.  Rows outside it still exist and are simply not this check's
#: business.
SESSION_OPEN = time(9, 30)
SESSION_CLOSE = time(16, 0)

#: Default ceiling on one unbroken blank stretch.  Half an hour is long enough
#: that the ordinary case -- a crossing that drifts outside the publish gate
#: for a few cycles around a fast move -- does not page anyone, and short
#: enough that a morning spent blank does.
DEFAULT_MAX_BLANK_MINUTES = 30.0


@dataclass(frozen=True)
class BlankRun:
    """One unbroken stretch of rows that carried no flip."""

    start: datetime
    end: datetime
    rows: int
    open_at_session_end: bool

    @property
    def minutes(self) -> float:
        return (self.end - self.start).total_seconds() / 60.0


@dataclass(frozen=True)
class SessionResolution:
    """One symbol's flip publication record for one session."""

    symbol: str
    session_date: date
    rows: int
    resolved: int
    unresolved: int
    longest: Optional[BlankRun]

    @property
    def unresolved_pct(self) -> float:
        return (self.unresolved / self.rows * 100.0) if self.rows else 0.0

    @property
    def longest_blank_minutes(self) -> float:
        return self.longest.minutes if self.longest else 0.0

    def as_dict(self) -> dict:
        out = {
            "symbol": self.symbol,
            "session_date": self.session_date.isoformat(),
            "rows": self.rows,
            "resolved": self.resolved,
            "unresolved": self.unresolved,
            "unresolved_pct": round(self.unresolved_pct, 1),
            "longest_blank_minutes": round(self.longest_blank_minutes, 1),
        }
        if self.longest is not None:
            out["longest_blank_start"] = self.longest.start.astimezone(ET).isoformat()
            out["longest_blank_end"] = self.longest.end.astimezone(ET).isoformat()
            out["longest_blank_open_at_close"] = self.longest.open_at_session_end
        return out


def session_window(session_date: date) -> Tuple[datetime, datetime]:
    """The cash session as an aware ET interval."""
    return (
        ET.localize(datetime.combine(session_date, SESSION_OPEN)),
        ET.localize(datetime.combine(session_date, SESSION_CLOSE)),
    )


def blank_runs(rows: Sequence[Tuple[datetime, Optional[float]]]) -> List[BlankRun]:
    """Maximal stretches of NULL-flip rows, measured to the next resolved row.

    ``rows`` must be chronological ``(timestamp, gamma_flip_point)``.  A run is
    closed by the first row that carries a flip, and its end is THAT row's
    timestamp: the blackout lasted until something was published.  A run still
    open at the end of the input is closed at its own last row, which
    understates it by up to one cycle and is marked so a caller can say so.
    """
    runs: List[BlankRun] = []
    start: Optional[datetime] = None
    count = 0

    for ts, flip in rows:
        if flip is None:
            if start is None:
                start = ts
                count = 0
            count += 1
            last = ts
        elif start is not None:
            runs.append(BlankRun(start=start, end=ts, rows=count, open_at_session_end=False))
            start = None

    if start is not None:
        runs.append(BlankRun(start=start, end=last, rows=count, open_at_session_end=True))
    return runs


def summarize_session(
    symbol: str,
    session_date: date,
    rows: Sequence[Tuple[datetime, Optional[float]]],
) -> Optional[SessionResolution]:
    """Classify one session's rows.  ``None`` when the session stored none."""
    if not rows:
        return None
    unresolved = sum(1 for _ts, flip in rows if flip is None)
    runs = blank_runs(rows)
    longest = max(runs, key=lambda r: r.minutes) if runs else None
    return SessionResolution(
        symbol=symbol,
        session_date=session_date,
        rows=len(rows),
        resolved=len(rows) - unresolved,
        unresolved=unresolved,
        longest=longest,
    )


def session_dates(cursor, symbol: str, limit: int, since: Optional[date]) -> List[date]:
    """The most recent ET dates with stored summary rows, oldest first."""
    cursor.execute(
        """
        SELECT DISTINCT (timestamp AT TIME ZONE 'America/New_York')::date AS session_date
        FROM gex_summary
        WHERE underlying = %(symbol)s
          AND (%(since)s::date IS NULL
               OR (timestamp AT TIME ZONE 'America/New_York')::date >= %(since)s::date)
        ORDER BY session_date DESC
        LIMIT %(limit)s
        """,
        {"symbol": symbol, "limit": limit, "since": since},
    )
    return sorted(row[0] for row in cursor.fetchall())


def check_session(cursor, symbol: str, session_date: date) -> Optional[SessionResolution]:
    """Read one session's rows and classify them."""
    start, end = session_window(session_date)
    cursor.execute(
        """
        SELECT timestamp, gamma_flip_point
        FROM gex_summary
        WHERE underlying = %(symbol)s
          AND timestamp >= %(start)s
          AND timestamp <= %(end)s
        ORDER BY timestamp
        """,
        {"symbol": symbol, "start": start, "end": end},
    )
    return summarize_session(symbol, session_date, list(cursor.fetchall()))


def configured_symbols() -> List[str]:
    """The underlyings the analytics engine runs, from the environment.

    Mirrors ``main_engine.main`` and the carry healthcheck: the engine writes
    ``gex_summary`` one worker per symbol, and it stores the CANONICAL symbol
    (``AnalyticsEngine.db_symbol``), which is what this table is keyed by.
    """
    raw = os.getenv("ANALYTICS_UNDERLYINGS") or os.getenv("ANALYTICS_UNDERLYING") or "SPY"
    return [get_canonical_symbol(symbol) for symbol in parse_underlyings(raw)]


def format_report(results: Sequence[SessionResolution], max_blank_minutes: float) -> List[str]:
    """Human-readable lines, worst blackout first."""
    if not results:
        return ["no gex_summary rows in the requested window"]

    lines = [
        f"{'symbol':<8} {'session':<12} {'rows':>6} {'blank':>7} {'blank%':>7} "
        f"{'longest':>9}  window",
    ]
    for r in sorted(results, key=lambda r: r.longest_blank_minutes, reverse=True):
        window = ""
        if r.longest is not None and r.longest.minutes > 0:
            window = (
                f"{r.longest.start.astimezone(ET):%H:%M}-"
                f"{r.longest.end.astimezone(ET):%H:%M} ET"
                + (" (open at close)" if r.longest.open_at_session_end else "")
            )
        flag = "  <-- over threshold" if r.longest_blank_minutes > max_blank_minutes else ""
        lines.append(
            f"{r.symbol:<8} {r.session_date.isoformat():<12} {r.rows:>6} "
            f"{r.unresolved:>7} {r.unresolved_pct:>6.1f}% "
            f"{r.longest_blank_minutes:>8.1f}m  {window}{flag}"
        )
    return lines


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Underlyings to check (default: ANALYTICS_UNDERLYINGS).",
    )
    parser.add_argument(
        "--sessions", type=int, default=10, help="How many recent sessions per symbol."
    )
    parser.add_argument("--since", default=None, help="Earliest session date (YYYY-MM-DD).")
    parser.add_argument(
        "--max-blank-minutes",
        type=float,
        default=DEFAULT_MAX_BLANK_MINUTES,
        help=(
            "Fail when one unbroken blank stretch exceeds this many minutes "
            f"(default: {DEFAULT_MAX_BLANK_MINUTES:g})."
        ),
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    symbols = (
        [get_canonical_symbol(s) for s in args.symbols]
        if args.symbols
        else configured_symbols()
    )
    since = date.fromisoformat(args.since) if args.since else None

    results: List[SessionResolution] = []
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            for symbol in symbols:
                for session_date in session_dates(cursor, symbol, args.sessions, since):
                    result = check_session(cursor, symbol, session_date)
                    if result is not None:
                        results.append(result)
            conn.rollback()
    except Exception as exc:  # noqa: BLE001 - a monitor reports, it does not raise
        logger.error("gamma flip resolution check failed: %s", exc, exc_info=True)
        return 2

    breaches = [r for r in results if r.longest_blank_minutes > args.max_blank_minutes]

    if args.json:
        print(
            json.dumps(
                {
                    "max_blank_minutes": args.max_blank_minutes,
                    "sessions": [r.as_dict() for r in results],
                    "breaches": [r.as_dict() for r in breaches],
                },
                indent=2,
            )
        )
    else:
        for line in format_report(results, args.max_blank_minutes):
            print(line)
        if breaches:
            print(
                f"\n{len(breaches)} session(s) left the flip blank for more than "
                f"{args.max_blank_minutes:g} minutes. The engine logs why on the "
                "unresolved transition: grep the analytics journal for "
                '"Gamma flip UNRESOLVED" over those dates, if it still reaches '
                "back that far."
            )

    return 1 if breaches else 0


if __name__ == "__main__":
    sys.exit(main())
