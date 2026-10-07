"""Replay recent sessions' Call/Put Walls under the old argmax and the current rule.

The walls used to be a plain argmax re-run every minute, split on spot.  They
jumped and snapped back 639 times across SPX/SPY/NDX/QQQ in the eight
sessions from 2026-09-28, about half from spot chopping across the biggest
strike and half from two near-tied strikes trading places.  The current rule
(``src/analytics/walls.py``) splits strikes on the wall anchor -- a sticky copy
of spot that only follows a close beyond the break buffer -- and treats
strikes within the tie zone of the biggest as tied, nearest wins.

This tool answers "what does that do to real sessions" before or after a
deploy, from data that already exists:

* every minute's per-strike gamma from ``gex_by_strike``, summed across all
  expirations (the ``all`` view) or restricted to the session's own expiration
  (the ``0dte`` view);
* that minute's spot, paired the way the engine pairs it: the latest
  ``underlying_quotes`` close at or before the minute;
* the break buffer from the same typical 30-minute move the engine uses.

It walks each session twice -- old rule, new rule -- and counts, per side,
how often the wall changed and how often it jumped and came back within 15
minutes (the same "flip" the production report counted).

READ-ONLY.  Runs SELECTs and a rollback; writes nothing.  The per-minute
reads are heavy on ``gex_by_strike``: run it after the close.

Usage:
    python -m src.tools.wall_stability_report --sessions 5
    python -m src.tools.wall_stability_report --symbols SPY QQQ --since 2026-09-28
    python -m src.tools.wall_stability_report --tie-pct 0.15 --break-min-pct 0.0015

Exit codes:
    0 -- report produced.
    2 -- database connection or query error.
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from bisect import bisect_right
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from src.config import WALL_BREAK_MIN_PCT, WALL_BREAK_MOVE_FRACTION, WALL_TIE_PCT
from src.database.connection import db_connection
from src.symbols import get_canonical_symbol
from src.tools.gamma_flip_resolution_healthcheck import (
    ET,
    configured_symbols,
    session_dates,
)

logger = logging.getLogger("zerogex.wall_stability_report")

#: A wall that moves and comes back within this long is a flip.
FLIP_MINUTES = 15

#: Strikes further than this from the session's price range are not read.
#: Walls form where gamma concentrates, which is near price; the band only
#: keeps a full session of per-minute rows to a size worth pulling.
STRIKE_BAND_PCT = 0.10

SCOPES = ("all", "0dte")

Walls = Tuple[Optional[float], Optional[float]]


@dataclass
class SideStats:
    changes: int = 0
    flips: int = 0


@dataclass
class ScopeResult:
    symbol: str
    scope: str
    sessions: int = 0
    minutes: int = 0
    old_call: SideStats = field(default_factory=SideStats)
    old_put: SideStats = field(default_factory=SideStats)
    new_call: SideStats = field(default_factory=SideStats)
    new_put: SideStats = field(default_factory=SideStats)
    #: Minutes where the new rule shows a different Call/Put Wall than the old.
    differs: int = 0
    buffers: List[float] = field(default_factory=list)

    def add(self, other: "ScopeResult") -> None:
        self.sessions += other.sessions
        self.minutes += other.minutes
        for name in ("old_call", "old_put", "new_call", "new_put"):
            mine, theirs = getattr(self, name), getattr(other, name)
            mine.changes += theirs.changes
            mine.flips += theirs.flips
        self.differs += other.differs
        self.buffers.extend(other.buffers)


def count_changes_and_flips(
    times: Sequence[datetime], values: Sequence[Optional[float]], max_minutes: int = FLIP_MINUTES
) -> SideStats:
    """Changes, and jump-and-revert flips, in one side's minute series.

    A flip is a run of one value that the series entered from X and left back
    to X within ``max_minutes`` of entering -- the definition the production
    flip report used.
    """
    stats = SideStats()
    runs: List[Tuple[Optional[float], datetime]] = []
    for ts, value in zip(times, values):
        if not runs or runs[-1][0] != value:
            if runs:
                stats.changes += 1
            runs.append((value, ts))
    limit = timedelta(minutes=max_minutes)
    for i in range(1, len(runs) - 1):
        before, (value, start), (after, back_at) = runs[i - 1][0], runs[i], runs[i + 1]
        if before is not None and before == after and back_at - start <= limit:
            stats.flips += 1
    return stats


def replay_session(
    times: Sequence[datetime],
    rows_by_minute: Dict[datetime, List[Dict[str, float]]],
    spot_by_minute: Dict[datetime, float],
    buffer_pct_of_spot: Optional[float],
    buffer_points: Optional[float],
    tie_pct: float,
) -> Tuple[List[Walls], List[Walls]]:
    """Walk one session: ``(old_walls, new_walls)`` per minute.

    The buffer is ``buffer_points`` when given (the volatility term, computed
    once for the session) floored at ``buffer_pct_of_spot`` of each minute's
    spot -- :func:`src.analytics.walls.wall_break_buffer` with the typical move
    already scaled.
    """
    from src.analytics.walls import WallAnchor, compute_call_put_walls

    anchor = WallAnchor()
    old: List[Walls] = []
    new: List[Walls] = []
    for ts in times:
        rows = rows_by_minute.get(ts, [])
        spot = spot_by_minute[ts]
        old.append(compute_call_put_walls(rows, spot, tie_pct=0.0))
        buffer = max((buffer_pct_of_spot or 0.0) * spot, buffer_points or 0.0)
        a = anchor.update(spot, buffer, ts)
        new.append(compute_call_put_walls(rows, spot, anchor=a, tie_pct=tie_pct))
    return old, new


def summarize(
    symbol: str, scope: str, times: Sequence[datetime], old: List[Walls], new: List[Walls]
) -> ScopeResult:
    result = ScopeResult(symbol=symbol, scope=scope, sessions=1, minutes=len(times))
    result.old_call = count_changes_and_flips(times, [c for c, _ in old])
    result.old_put = count_changes_and_flips(times, [p for _, p in old])
    result.new_call = count_changes_and_flips(times, [c for c, _ in new])
    result.new_put = count_changes_and_flips(times, [p for _, p in new])
    result.differs = sum(1 for o, n in zip(old, new) if o != n)
    return result


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def _session_bounds(session_date: date) -> Tuple[datetime, datetime]:
    """[09:30, 16:00) ET: the 16:00 frame is built from tomorrow's expiries."""
    return (
        ET.localize(datetime.combine(session_date, time(9, 30))),
        ET.localize(datetime.combine(session_date, time(16, 0))),
    )


def read_spots(cursor, symbol: str, start: datetime, end: datetime):
    """Chronological ``(timestamps, closes)`` from a few minutes before ``start``."""
    cursor.execute(
        """
        SELECT timestamp, close
        FROM underlying_quotes
        WHERE symbol = %s AND timestamp >= %s AND timestamp < %s AND close IS NOT NULL
        ORDER BY timestamp
        """,
        (symbol, start - timedelta(minutes=10), end),
    )
    rows = cursor.fetchall()
    return [r[0] for r in rows], [float(r[1]) for r in rows]


def read_rows(
    cursor,
    symbol: str,
    start: datetime,
    end: datetime,
    lo: float,
    hi: float,
    expiration: Optional[date],
) -> Dict[datetime, List[Dict[str, float]]]:
    """Per-minute, per-strike gamma summed across the scope's expirations."""
    exp_filter = "AND expiration = %s" if expiration is not None else ""
    params: list = [symbol, start, end, lo, hi]
    if expiration is not None:
        params.append(expiration)
    cursor.execute(
        f"""
        SELECT timestamp, strike,
               SUM(COALESCE(call_gamma, 0)) AS call_gamma,
               SUM(COALESCE(put_gamma, 0)) AS put_gamma
        FROM gex_by_strike
        WHERE underlying = %s AND timestamp >= %s AND timestamp < %s
          AND strike BETWEEN %s AND %s
          {exp_filter}
        GROUP BY timestamp, strike
        ORDER BY timestamp, strike
        """,
        params,
    )
    out: Dict[datetime, List[Dict[str, float]]] = defaultdict(list)
    for ts, strike, call_gamma, put_gamma in cursor.fetchall():
        out[ts].append(
            {
                "strike": float(strike),
                "call_gamma": float(call_gamma),
                "put_gamma": float(put_gamma),
            }
        )
    return out


def examine_session(
    cursor,
    symbol: str,
    session_date: date,
    *,
    tie_pct: float,
    break_min_pct: float,
    break_move_fraction: float,
) -> List[ScopeResult]:
    from src.analytics.main_engine import AnalyticsEngine

    start, end = _session_bounds(session_date)
    quote_ts, closes = read_spots(cursor, symbol, start, end)
    session_closes = [c for t, c in zip(quote_ts, closes) if t >= start]
    if not session_closes:
        return []
    lo = min(session_closes) * (1.0 - STRIKE_BAND_PCT)
    hi = max(session_closes) * (1.0 + STRIKE_BAND_PCT)

    # The engine's volatility yardstick, read once at the open (a 5-day
    # median of half-hour ranges; it barely moves within a session).
    typical = AnalyticsEngine(underlying=symbol)._typical_move_30m(cursor, start)
    buffer_points = break_move_fraction * typical if typical else None

    results: List[ScopeResult] = []
    for scope in SCOPES:
        rows_by_minute = read_rows(
            cursor, symbol, start, end, lo, hi, session_date if scope == "0dte" else None
        )
        times = []
        spot_by_minute: Dict[datetime, float] = {}
        for ts in sorted(rows_by_minute):
            i = bisect_right(quote_ts, ts)
            if i == 0:
                continue
            times.append(ts)
            spot_by_minute[ts] = closes[i - 1]
        if not times:
            continue
        old, new = replay_session(
            times, rows_by_minute, spot_by_minute, break_min_pct, buffer_points, tie_pct
        )
        result = summarize(symbol, scope, times, old, new)
        result.buffers.append(
            max(break_min_pct * statistics.median(session_closes), buffer_points or 0.0)
        )
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def format_report(results: Sequence[ScopeResult]) -> List[str]:
    lines = [
        "Call/Put Wall stability: old argmax vs current rule "
        f"(flip = jump that reverts within {FLIP_MINUTES} min)",
        "",
        f"{'symbol':<7}{'view':<6}{'sess':>5}{'min':>7}  "
        f"{'old flips C/P':>14}{'new flips C/P':>15}  "
        f"{'old moves C/P':>14}{'new moves C/P':>15}  {'differs':>8}  {'buffer':>8}",
    ]
    total = ScopeResult(symbol="TOTAL", scope="")
    for r in results:
        total.add(r)
        lines.append(_line(r))
    if len(results) > 1:
        lines.append(_line(total))
    return lines


def _line(r: ScopeResult) -> str:
    differs = f"{100.0 * r.differs / r.minutes:.0f}%" if r.minutes else "-"
    buffer = f"{statistics.median(r.buffers):.2f}" if r.buffers else ""
    return (
        f"{r.symbol:<7}{r.scope:<6}{r.sessions:>5}{r.minutes:>7}  "
        f"{r.old_call.flips:>7}/{r.old_put.flips:<6}{r.new_call.flips:>8}/{r.new_put.flips:<6}  "
        f"{r.old_call.changes:>7}/{r.old_put.changes:<6}{r.new_call.changes:>8}/"
        f"{r.new_put.changes:<6}  {differs:>8}  {buffer:>8}"
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--symbols", nargs="*", default=None, help="Underlyings (default: ANALYTICS_UNDERLYINGS)."
    )
    parser.add_argument("--sessions", type=int, default=5, help="Recent sessions per symbol.")
    parser.add_argument("--since", default=None, help="Earliest session date (YYYY-MM-DD).")
    parser.add_argument("--tie-pct", type=float, default=WALL_TIE_PCT)
    parser.add_argument("--break-min-pct", type=float, default=WALL_BREAK_MIN_PCT)
    parser.add_argument("--break-move-fraction", type=float, default=WALL_BREAK_MOVE_FRACTION)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    symbols = (
        [get_canonical_symbol(s) for s in args.symbols] if args.symbols else configured_symbols()
    )
    since = date.fromisoformat(args.since) if args.since else None

    by_key: Dict[Tuple[str, str], ScopeResult] = {}
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            for symbol in symbols:
                for session_date in session_dates(cursor, symbol, args.sessions, since):
                    logger.info("Replaying %s %s", symbol, session_date)
                    for r in examine_session(
                        cursor,
                        symbol,
                        session_date,
                        tie_pct=args.tie_pct,
                        break_min_pct=args.break_min_pct,
                        break_move_fraction=args.break_move_fraction,
                    ):
                        key = (r.symbol, r.scope)
                        if key in by_key:
                            by_key[key].add(r)
                        else:
                            by_key[key] = r
            conn.rollback()
    except Exception as exc:  # noqa: BLE001 - a report reports, it does not raise
        logger.error("wall stability report failed: %s", exc, exc_info=True)
        return 2

    results = [by_key[k] for k in sorted(by_key)]
    if args.json:
        print(json.dumps([asdict(r) for r in results], indent=2, default=str))
    else:
        for line in format_report(results):
            print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
