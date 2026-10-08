"""Replay recent sessions' Call/Put Walls under the old argmax and the current rules.

The walls used to be a plain argmax re-run every minute, split on spot.  They
jumped and snapped back 639 times across SPX/SPY/NDX/QQQ in the eight
sessions from 2026-09-28, about half from spot chopping across the biggest
strike and half from two near-tied strikes trading places.  The rules in
``src/analytics/walls.py`` answer that in two steps, and this report shows
each one against the old argmax:

* ``live`` -- split strikes on the wall anchor (a sticky copy of spot that
  only follows a close beyond the break buffer) and treat strikes within the
  tie zone of the biggest as tied, nearest wins; re-picked every minute.
* ``new`` -- the same, but re-picked only on the minute price breaks out or
  when the ``--refresh-minutes`` re-check clock runs out (WallTracker), and
  held in between.

This tool answers "what does that do to real sessions" before or after a
deploy, from data that already exists:

* every minute's per-strike gamma from ``gex_by_strike``, summed across all
  expirations (the ``all`` view) or restricted to the session's own expiration
  (the ``0dte`` view);
* that minute's spot, paired the way the engine pairs it: the latest
  ``underlying_quotes`` close at or before the minute;
* the break buffer from the same typical 30-minute move the engine uses.

It walks each session once per rule and counts, per side, how often the wall
changed and how often it jumped and came back within 15 minutes (the same
"flip" the production report counted).

READ-ONLY.  Runs SELECTs and a rollback; writes nothing.  The per-minute
reads are heavy on ``gex_by_strike``: run it after the close.

Usage:
    python -m src.tools.wall_stability_report --sessions 5
    python -m src.tools.wall_stability_report --symbols SPY QQQ --since 2026-09-28
    python -m src.tools.wall_stability_report --tie-pct 0.15 --break-min-pct 0.0015
    python -m src.tools.wall_stability_report --refresh-minutes 30

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

from src.config import (
    WALL_BREAK_MIN_PCT,
    WALL_BREAK_MOVE_FRACTION,
    WALL_REFRESH_MINUTES,
    WALL_TIE_PCT,
)
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

#: The rules replayed: the old argmax, the rule re-picked every minute, and
#: the rule with re-pick timing.
VARIANTS = ("old", "live", "new")

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
    #: Per rule (see VARIANTS): (call side, put side).
    stats: Dict[str, Tuple[SideStats, SideStats]] = field(
        default_factory=lambda: {v: (SideStats(), SideStats()) for v in VARIANTS}
    )
    #: Minutes where the ``new`` rule shows a different Call/Put Wall than the old.
    differs: int = 0
    buffers: List[float] = field(default_factory=list)

    def add(self, other: "ScopeResult") -> None:
        self.sessions += other.sessions
        self.minutes += other.minutes
        for variant in VARIANTS:
            for mine, theirs in zip(self.stats[variant], other.stats[variant]):
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
    refresh_minutes: float = WALL_REFRESH_MINUTES,
) -> Dict[str, List[Walls]]:
    """Walk one session: each rule's ``(call_wall, put_wall)`` per minute.

    The buffer is ``buffer_points`` when given (the volatility term, computed
    once for the session) floored at ``buffer_pct_of_spot`` of each minute's
    spot -- :func:`src.analytics.walls.wall_break_buffer` with the typical move
    already scaled.  ``live`` and ``new`` run the production
    :class:`~src.analytics.walls.WallTracker`, so a re-pick here happens on the
    minute the engine's would.
    """
    from src.analytics.walls import WallTracker, compute_call_put_walls

    trackers = {"live": WallTracker(refresh_minutes=0), "new": WallTracker(refresh_minutes)}
    out: Dict[str, List[Walls]] = {v: [] for v in VARIANTS}
    for ts in times:
        rows = rows_by_minute.get(ts, [])
        spot = spot_by_minute[ts]
        out["old"].append(compute_call_put_walls(rows, spot, tie_pct=0.0))
        buffer = max((buffer_pct_of_spot or 0.0) * spot, buffer_points or 0.0)
        for name, tracker in trackers.items():
            step = tracker.update(spot, buffer, ts)
            if step.refreshed:
                tracker.hold(
                    compute_call_put_walls(rows, spot, anchor=step.anchor, tie_pct=tie_pct)
                )
            out[name].append(tracker.held())
    return out


def summarize(
    symbol: str, scope: str, times: Sequence[datetime], walls: Dict[str, List[Walls]]
) -> ScopeResult:
    result = ScopeResult(symbol=symbol, scope=scope, sessions=1, minutes=len(times))
    for variant in VARIANTS:
        series = walls[variant]
        result.stats[variant] = (
            count_changes_and_flips(times, [c for c, _ in series]),
            count_changes_and_flips(times, [p for _, p in series]),
        )
    result.differs = sum(1 for o, n in zip(walls["old"], walls["new"]) if o != n)
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
    refresh_minutes: float = WALL_REFRESH_MINUTES,
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
        walls = replay_session(
            times,
            rows_by_minute,
            spot_by_minute,
            break_min_pct,
            buffer_points,
            tie_pct,
            refresh_minutes,
        )
        result = summarize(symbol, scope, times, walls)
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
        "Call/Put Wall stability (flip = jump that reverts within "
        f"{FLIP_MINUTES} min; C/P = call side / put side)",
        "  old  = plain argmax, every minute (before 2026-10-08)",
        "  live = break test + tie zone, re-picked every minute",
        "  new  = live, re-picked only on a breakout or the re-check clock",
        "",
        f"{'':<18}{'':>7}  {'------ flips C/P ------':^29}  {'--- moves C/P ---':^19}",
        f"{'symbol':<7}{'view':<6}{'sess':>5}{'min':>7}  "
        f"{'old':>9}{'live':>10}{'new':>10}  {'old':>9}{'new':>10}  {'differs':>8}  "
        f"{'buffer':>8}",
    ]
    total = ScopeResult(symbol="TOTAL", scope="")
    for r in results:
        total.add(r)
        lines.append(_line(r))
    if len(results) > 1:
        lines.append(_line(total))
    return lines


def _pair(stats: Tuple[SideStats, SideStats], attr: str) -> str:
    return f"{getattr(stats[0], attr)}/{getattr(stats[1], attr)}"


def _line(r: ScopeResult) -> str:
    differs = f"{100.0 * r.differs / r.minutes:.0f}%" if r.minutes else "-"
    buffer = f"{statistics.median(r.buffers):.2f}" if r.buffers else ""
    return (
        f"{r.symbol:<7}{r.scope:<6}{r.sessions:>5}{r.minutes:>7}  "
        f"{_pair(r.stats['old'], 'flips'):>9}{_pair(r.stats['live'], 'flips'):>10}"
        f"{_pair(r.stats['new'], 'flips'):>10}  "
        f"{_pair(r.stats['old'], 'changes'):>9}{_pair(r.stats['new'], 'changes'):>10}  "
        f"{differs:>8}  {buffer:>8}"
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
    parser.add_argument(
        "--refresh-minutes",
        type=float,
        default=WALL_REFRESH_MINUTES,
        help="Re-check clock for the 'new' rule (0 = every minute).",
    )
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
                        refresh_minutes=args.refresh_minutes,
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
