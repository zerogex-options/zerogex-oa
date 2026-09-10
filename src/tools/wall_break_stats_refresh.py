"""Populate wall_break_stats — measured odds a gamma wall gives way once tested.

The product draws a call wall for SPY and a call wall for QQQ with the same
line and the same word. Measured over mid-2026 they are not the same object:
an S&P wall held roughly two times in three within an hour of being tested, a
Nasdaq wall closer to one in two. This job measures that per symbol, nightly,
so the levels can carry their own hit rate instead of an implication.

What it computes
    For each symbol, every time price came within a touch band of the wall in
    force at that minute, and whether it then closed beyond the wall for
    ``confirm_minutes`` consecutive minutes. The labelling is
    :mod:`src.analytics.wall_breaks`, shared verbatim with
    ``research/wall_break_odds`` so the published figure and the study behind
    it cannot diverge.

Why a curve
    "Does the wall break" is not well posed without a clock. On SPX the same
    tests read 15.3% at a thirty-minute horizon and 34.4% at sixty, on
    non-overlapping intervals — most of a bare rate is the window somebody
    chose. One row per horizon, and consumers quote the horizon.

Why Kaplan-Meier
    A test at 15:45 cannot be watched for an hour. Discarding those biases the
    estimate away from the late-session tape where 0DTE gamma is heaviest, so
    they contribute for the time they WERE observed instead.

Usage:
    python -m src.tools.wall_break_stats_refresh
    python -m src.tools.wall_break_stats_refresh --symbols SPY QQQ
    python -m src.tools.wall_break_stats_refresh --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Iterable, Optional, Sequence
from zoneinfo import ZoneInfo

from src.analytics.wall_breaks import (
    ET,
    EventConfig,
    Observation,
    PriceBar,
    WallFrame,
    break_probability_at,
    extract_wall_tests,
    kaplan_meier,
)
from src.database.connection import db_connection

logger = logging.getLogger(__name__)

UTC = ZoneInfo("UTC")

#: Horizons published, in minutes. Bracket a 0DTE holding period rather than
#: flattering the curve: the shorter ones are the honest answer for anyone who
#: is not holding an hour.
HORIZONS: tuple[int, ...] = (15, 30, 45, 60)

#: Trailing sessions the estimate is computed over. Long enough to accumulate
#: events at the ~2-4 per session these symbols produce, short enough that the
#: figure still describes the current volatility regime rather than last year's.
DEFAULT_WINDOW_SESSIONS = 60

#: Below this many resolved-or-censored tests, the row is written but flagged
#: unreportable. A break probability from thirty observations is not a number
#: a trader should see next to a level.
MIN_TESTS_TO_PUBLISH = 60

#: The sides stored. 'both' is the pooled curve, which is what a levels page
#: shows when it is not distinguishing the two walls.
SIDES: tuple[str, ...] = ("both", "call", "put")


@dataclass
class SymbolResult:
    """One symbol's measurement, or the reason there isn't one."""

    symbol: str
    rows: list[dict[str, Any]]
    sessions: int
    window_start: Optional[date]
    window_end: Optional[date]
    skipped: Optional[str] = None


def _session_bounds(session: date) -> tuple[datetime, datetime]:
    start = datetime.combine(session, time(9, 30), tzinfo=ET)
    end = datetime.combine(session, time(16, 0), tzinfo=ET)
    return start.astimezone(UTC), end.astimezone(UTC)


def fetch_sessions(conn: Any, symbol: str, window: int) -> list[date]:
    """The most recent ``window`` ET session dates with published GEX frames.

    Driven off ``gex_summary`` rather than a market calendar so a holiday or an
    ingestion outage simply is not in the window, instead of entering it as a
    session with no walls.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT
                   (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date AS d
              FROM gex_summary
             WHERE underlying = %(symbol)s
             ORDER BY d DESC
             LIMIT %(window)s
            """,
            {"symbol": symbol, "window": window},
        )
        return sorted(r[0] for r in cur.fetchall() if r[0] is not None)


def fetch_session_inputs(
    conn: Any, symbol: str, session: date
) -> tuple[list[WallFrame], list[PriceBar]]:
    """The day's wall path and its minute bars."""
    start, end = _session_bounds(session)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT timestamp, call_wall, put_wall
              FROM gex_summary
             WHERE underlying = %(symbol)s
               AND timestamp BETWEEN %(start)s AND %(end)s
             ORDER BY timestamp
            """,
            {"symbol": symbol, "start": start, "end": end},
        )
        frames = [
            WallFrame(
                ts=r[0],
                call_wall=float(r[1]) if r[1] is not None else None,
                put_wall=float(r[2]) if r[2] is not None else None,
            )
            for r in cur.fetchall()
        ]
        cur.execute(
            """
            SELECT timestamp, high, low, close
              FROM underlying_quotes
             WHERE symbol = %(symbol)s
               AND timestamp BETWEEN %(start)s AND %(end)s
             ORDER BY timestamp
            """,
            {"symbol": symbol, "start": start, "end": end},
        )
        bars: list[PriceBar] = []
        for ts, high, low, close in cur.fetchall():
            if ts is None or close is None:
                continue
            c = float(close)
            # Pre-backfill rows carry a close only; fall back to it so the bar
            # still contributes to the path.
            bars.append(
                PriceBar(
                    ts=ts,
                    high=float(high) if high is not None else c,
                    low=float(low) if low is not None else c,
                    close=c,
                )
            )
    return frames, bars


def measure_symbol(
    conn: Any,
    symbol: str,
    *,
    window: int = DEFAULT_WINDOW_SESSIONS,
    cfg: Optional[EventConfig] = None,
) -> SymbolResult:
    """Label every wall test in the window and estimate the break curve."""
    cfg = cfg or EventConfig()
    sessions = fetch_sessions(conn, symbol, window)
    if not sessions:
        return SymbolResult(symbol, [], 0, None, None, skipped="no_sessions")

    tests = []
    for session in sessions:
        try:
            frames, bars = fetch_session_inputs(conn, symbol, session)
        except Exception:  # pragma: no cover - one bad day must not end the run
            logger.warning("%s %s: session read failed", symbol, session, exc_info=True)
            continue
        tests.extend(extract_wall_tests(symbol, session, frames, bars, cfg))

    if not tests:
        return SymbolResult(symbol, [], len(sessions), sessions[0], sessions[-1], "no_tests")

    rows: list[dict[str, Any]] = []
    for side in SIDES:
        subset = [t for t in tests if side == "both" or t.side == side]
        observations = [
            Observation(minutes=float(t.observed_minutes), broke=t.broke)
            for t in subset
            if t.observed_minutes is not None
        ]
        if not observations:
            continue
        curve = kaplan_meier(observations)
        n_tests = len(observations)
        n_breaks = sum(1 for o in observations if o.broke)
        n_censored = sum(1 for t in subset if t.outcome == "censored")
        reportable = n_tests >= MIN_TESTS_TO_PUBLISH
        for horizon in HORIZONS:
            point = break_probability_at(curve, horizon)
            rows.append(
                {
                    "underlying": symbol,
                    "side": side,
                    "horizon_minutes": horizon,
                    "break_prob": point.break_prob if point else None,
                    "ci_low": point.break_lo if point else None,
                    "ci_high": point.break_hi if point else None,
                    "at_risk": point.at_risk if point else None,
                    "n_tests": n_tests,
                    "n_breaks": n_breaks,
                    "n_censored": n_censored,
                    "window_sessions": len(sessions),
                    "window_start": sessions[0],
                    "window_end": sessions[-1],
                    "reportable": reportable,
                }
            )
    return SymbolResult(symbol, rows, len(sessions), sessions[0], sessions[-1])


def upsert(conn: Any, rows: Sequence[dict[str, Any]]) -> int:
    """Idempotent UPSERT keyed on (underlying, side, horizon_minutes)."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO wall_break_stats
                    (underlying, side, horizon_minutes, break_prob, ci_low, ci_high,
                     at_risk, n_tests, n_breaks, n_censored, window_sessions,
                     window_start, window_end, reportable, refreshed_at)
                VALUES
                    (%(underlying)s, %(side)s, %(horizon_minutes)s, %(break_prob)s,
                     %(ci_low)s, %(ci_high)s, %(at_risk)s, %(n_tests)s, %(n_breaks)s,
                     %(n_censored)s, %(window_sessions)s, %(window_start)s,
                     %(window_end)s, %(reportable)s, NOW())
                ON CONFLICT (underlying, side, horizon_minutes) DO UPDATE SET
                    break_prob = EXCLUDED.break_prob,
                    ci_low = EXCLUDED.ci_low,
                    ci_high = EXCLUDED.ci_high,
                    at_risk = EXCLUDED.at_risk,
                    n_tests = EXCLUDED.n_tests,
                    n_breaks = EXCLUDED.n_breaks,
                    n_censored = EXCLUDED.n_censored,
                    window_sessions = EXCLUDED.window_sessions,
                    window_start = EXCLUDED.window_start,
                    window_end = EXCLUDED.window_end,
                    reportable = EXCLUDED.reportable,
                    refreshed_at = NOW()
                """,
                row,
            )
    return len(rows)


def _default_symbols(conn: Any) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM symbols ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]


def run(symbols: Optional[Iterable[str]], window: int, dry_run: bool) -> int:
    written = 0
    with db_connection() as conn:
        targets = list(symbols) if symbols else _default_symbols(conn)
        for symbol in targets:
            result = measure_symbol(conn, symbol, window=window)
            if result.skipped:
                logger.info("%s: %s (%d sessions)", symbol, result.skipped, result.sessions)
                continue
            pooled = [r for r in result.rows if r["side"] == "both"]
            headline = next((r for r in pooled if r["horizon_minutes"] == 60), None)
            if headline:
                prob = headline["break_prob"]
                logger.info(
                    "%s: %d sessions, %d tests, P(break within 60m)=%s%s",
                    symbol,
                    result.sessions,
                    headline["n_tests"],
                    "n/a" if prob is None else f"{prob:.1%}",
                    "" if headline["reportable"] else "  [NOT REPORTABLE — thin sample]",
                )
            if dry_run:
                continue
            written += upsert(conn, result.rows)
        if not dry_run:
            conn.commit()
    logger.info("wrote %d rows%s", written, " (dry run: nothing written)" if dry_run else "")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--symbols", nargs="*", help="default: every row in symbols")
    parser.add_argument(
        "--window",
        type=int,
        default=DEFAULT_WINDOW_SESSIONS,
        help=f"trailing sessions to measure over (default {DEFAULT_WINDOW_SESSIONS})",
    )
    parser.add_argument("--dry-run", action="store_true", help="measure and log, write nothing")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        return run(args.symbols, args.window, args.dry_run)
    except Exception:
        logger.exception("wall_break_stats refresh failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
