"""Say WHY the gamma flip was left unpublished, from data that still exists.

:mod:`src.tools.gamma_flip_resolution_healthcheck` answers *when* and *how
often* the flip went blank.  It ends by telling the operator to grep the
analytics journal for the reason -- which is the right advice for last
Tuesday and useless for last month, because journald is capped and rotates
in weeks.  The blackout that prompted all of this ran from 2026-08-07, and
by the time anyone asked, every line that said why was gone.

This tool takes the same question to ``gex_summary``, which is
retention-EXEMPT.  It cannot reproduce the engine's four-way diagnostic, but
it does not need to: the row the resolver wrote on a blank cycle carries
enough neighbouring fields to separate the causes that matter.

**The discriminators, and what each one rules out.**

``gamma_flip_raw`` is the nearest zero crossing on the UN-DTE-weighted
profile, with no structural-significance gate (see the column comment in
``setup/database/schema.sql``).  It is computed from the same chain, in the
same cycle, as ``gamma_flip_point``.  So a blank row that still carries a
raw value is proof that the profile HAD a crossing and something downstream
of the profile refused to publish it -- the horizon-occupancy ramp, the
structural floor, or the interior test.  That is a gate verdict, not a data
outage, and no amount of fixing the feed would have changed it.

``call_wall`` / ``put_wall`` come from the same option chain by a completely
separate path (``src/analytics/walls.py``).  Walls present on a blank row
mean the chain was healthy enough to locate peak dollar gamma on both sides
of spot, which rules out "the snapshot was degraded".  Walls absent as well
points the other way, at ingestion.

``total_call_oi`` / ``total_put_oi`` are the chain's own size.  The profile
skips every contract with ``oi <= 0``, so an OI collapse empties the profile
without emptying the row.  Comparing the blank rows' median against the same
session's RESOLVED rows makes the collapse visible without needing a
healthy-session baseline from somewhere else.

``gamma_flip_span_used`` is the ladder rung the resolver stopped at.  On a
declined cycle it is the LAST rung, so seeing the maximum span on blank rows
confirms the ladder really was walked to exhaustion rather than the NULL
arriving by some other route.

**Every discriminator is calibrated in-session before it is believed.**  A
column that was added to the table after the session being examined is NULL
for reasons that have nothing to do with the flip, and reading that as
evidence would repeat the mistake this whole investigation already made once:
concluding something from an absence without first establishing that the
absence was capable of being a presence.  So each field is checked against
the same session's resolved rows, and when the field is blank there too the
verdict is ``inconclusive`` rather than a cause.

READ-ONLY.  Runs SELECTs and a rollback; writes nothing.

Usage:
    python -m src.tools.gamma_flip_blackout_forensics --symbols NDX --since 2026-08-01
    python -m src.tools.gamma_flip_blackout_forensics --sessions 40 --json
    python -m src.tools.gamma_flip_blackout_forensics --blank-only

Exit codes:
    0 -- report produced (whatever it says).  This is forensics, not a
         monitor; the alerting job belongs to the resolution healthcheck.
    2 -- database connection or query error.
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

from src.database.connection import db_connection
from src.symbols import get_canonical_symbol
from src.tools.gamma_flip_resolution_healthcheck import (
    configured_symbols,
    session_dates,
    session_window,
)

logger = logging.getLogger("zerogex.gamma_flip_blackout_forensics")

#: A discriminator has to hold across most of the blank rows before it is
#: called the cause.  A handful of cycles behaving differently mid-session is
#: ordinary; two thirds of them is a regime.
MAJORITY = 0.66

#: And the field has to be demonstrably populated in this session's resolved
#: rows before its absence on the blank ones means anything at all.
CALIBRATION = 0.50

#: An OI median this much below the same session's resolved rows is a
#: collapse rather than the ordinary drift between two parts of a session.
OI_COLLAPSE_RATIO = 0.25

VERDICT_CLEAN = "clean"
VERDICT_GATE = "gate_declined"
VERDICT_NO_CROSSING = "no_crossing"
VERDICT_CHAIN = "chain_degraded"
VERDICT_OI = "oi_collapse"
VERDICT_INCONCLUSIVE = "inconclusive"

#: One line each, printed under the table so the verdict column does not need
#: the reader to have read this module.
VERDICT_NOTES = {
    VERDICT_CLEAN: "flip published on every row in the session window",
    VERDICT_GATE: (
        "the profile HAD a crossing (gamma_flip_raw present on the blank rows) "
        "and the publish gate refused it -- DTE weighting or structural floor, "
        "not the feed"
    ),
    VERDICT_NO_CROSSING: (
        "chain healthy (walls resolved) but no crossing anywhere in the span "
        "ladder -- the flip was outside +/-max span of spot, or the profile was "
        "one-signed"
    ),
    VERDICT_CHAIN: (
        "walls did not resolve either -- the snapshot itself was degraded, so "
        "this is an ingestion question, not a resolver one"
    ),
    VERDICT_OI: (
        "open interest collapsed on the blank rows versus the resolved ones in "
        "the same session -- the profile skips oi<=0, so the chain emptied out "
        "underneath it"
    ),
    VERDICT_INCONCLUSIVE: (
        "the fields that would discriminate are not populated in this session's "
        "resolved rows either, so their absence on the blank rows proves nothing"
    ),
}


@dataclass(frozen=True)
class SessionForensics:
    """What one symbol-session's blank rows say about their own cause."""

    symbol: str
    session_date: date
    rows: int
    blank: int
    blank_with_raw: int
    blank_with_walls: int
    resolved_with_raw: int
    resolved_with_walls: int
    oi_blank_median: Optional[float]
    oi_resolved_median: Optional[float]
    spans_on_blank: Sequence[tuple]

    @property
    def resolved(self) -> int:
        return self.rows - self.blank

    @property
    def blank_pct(self) -> float:
        return (self.blank / self.rows * 100.0) if self.rows else 0.0

    @property
    def raw_calibrated(self) -> bool:
        """Was ``gamma_flip_raw`` being written at all in this session?"""
        return self.resolved > 0 and self.resolved_with_raw / self.resolved >= CALIBRATION

    @property
    def walls_calibrated(self) -> bool:
        return self.resolved > 0 and self.resolved_with_walls / self.resolved >= CALIBRATION

    @property
    def oi_collapsed(self) -> bool:
        if not self.oi_blank_median or not self.oi_resolved_median:
            return False
        return self.oi_blank_median < self.oi_resolved_median * OI_COLLAPSE_RATIO

    @property
    def verdict(self) -> str:
        if self.blank == 0:
            return VERDICT_CLEAN
        if self.oi_collapsed:
            return VERDICT_OI

        walls_share = self.blank_with_walls / self.blank
        raw_share = self.blank_with_raw / self.blank

        if self.walls_calibrated and walls_share <= 1.0 - MAJORITY:
            return VERDICT_CHAIN
        if raw_share >= MAJORITY:
            return VERDICT_GATE
        if self.raw_calibrated and self.walls_calibrated and walls_share >= MAJORITY:
            return VERDICT_NO_CROSSING
        return VERDICT_INCONCLUSIVE

    def as_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "session_date": self.session_date.isoformat(),
            "rows": self.rows,
            "blank": self.blank,
            "blank_pct": round(self.blank_pct, 1),
            "blank_with_raw": self.blank_with_raw,
            "blank_with_walls": self.blank_with_walls,
            "resolved_with_raw": self.resolved_with_raw,
            "resolved_with_walls": self.resolved_with_walls,
            "oi_blank_median": self.oi_blank_median,
            "oi_resolved_median": self.oi_resolved_median,
            "spans_on_blank": [list(pair) for pair in self.spans_on_blank],
            "verdict": self.verdict,
        }


def _median(values: Sequence[float]) -> Optional[float]:
    return float(statistics.median(values)) if values else None


def summarize_session(
    symbol: str,
    session_date: date,
    rows: Sequence[Sequence[Any]],
) -> Optional[SessionForensics]:
    """Classify one session's rows.

    ``rows`` is chronological ``(timestamp, gamma_flip_point, gamma_flip_raw,
    gamma_flip_span_used, call_wall, put_wall, total_call_oi, total_put_oi)``.
    """
    if not rows:
        return None

    blank = blank_raw = blank_walls = 0
    resolved_raw = resolved_walls = 0
    oi_blank: List[float] = []
    oi_resolved: List[float] = []
    spans: Counter = Counter()

    for (
        _ts,
        flip,
        raw,
        span,
        call_wall,
        put_wall,
        call_oi,
        put_oi,
    ) in rows:
        has_raw = raw is not None
        has_walls = call_wall is not None and put_wall is not None
        oi = float(call_oi or 0) + float(put_oi or 0)

        if flip is None:
            blank += 1
            blank_raw += int(has_raw)
            blank_walls += int(has_walls)
            oi_blank.append(oi)
            if span is not None:
                spans[round(float(span), 4)] += 1
        else:
            resolved_raw += int(has_raw)
            resolved_walls += int(has_walls)
            oi_resolved.append(oi)

    return SessionForensics(
        symbol=symbol,
        session_date=session_date,
        rows=len(rows),
        blank=blank,
        blank_with_raw=blank_raw,
        blank_with_walls=blank_walls,
        resolved_with_raw=resolved_raw,
        resolved_with_walls=resolved_walls,
        oi_blank_median=_median(oi_blank),
        oi_resolved_median=_median(oi_resolved),
        spans_on_blank=tuple(sorted(spans.items())),
    )


def examine_session(cursor, symbol: str, session_date: date) -> Optional[SessionForensics]:
    """Read one session's rows, with every field the verdict depends on."""
    start, end = session_window(session_date)
    cursor.execute(
        """
        SELECT timestamp,
               gamma_flip_point,
               gamma_flip_raw,
               gamma_flip_span_used,
               call_wall,
               put_wall,
               total_call_oi,
               total_put_oi
        FROM gex_summary
        WHERE underlying = %(symbol)s
          AND timestamp >= %(start)s
          AND timestamp <= %(end)s
        ORDER BY timestamp
        """,
        {"symbol": symbol, "start": start, "end": end},
    )
    return summarize_session(symbol, session_date, list(cursor.fetchall()))


def _format_oi(value: Optional[float]) -> str:
    if not value:
        return "-"
    return f"{value / 1_000_000:.1f}M" if value >= 1_000_000 else f"{value / 1_000:.0f}k"


def _format_spans(spans: Sequence[tuple]) -> str:
    if not spans:
        return "-"
    return ",".join(f"{span:g}" for span, _count in spans)


def format_report(results: Sequence[SessionForensics], blank_only: bool = False) -> List[str]:
    """Chronological per symbol: a cause that changes has to be readable as a date."""
    shown = [r for r in results if r.blank] if blank_only else list(results)
    if not shown:
        return ["no gex_summary rows in the requested window"]

    lines = [
        f"{'symbol':<8} {'session':<12} {'rows':>5} {'blank':>6} {'blank%':>7} "
        f"{'raw':>8} {'walls':>8} {'oi blank':>9} {'oi ok':>8} {'spans':>10}  verdict",
    ]
    for r in sorted(shown, key=lambda r: (r.symbol, r.session_date)):
        raw_cell = f"{r.blank_with_raw}/{r.blank}" if r.blank else "-"
        walls_cell = f"{r.blank_with_walls}/{r.blank}" if r.blank else "-"
        lines.append(
            f"{r.symbol:<8} {r.session_date.isoformat():<12} {r.rows:>5} "
            f"{r.blank:>6} {r.blank_pct:>6.1f}% {raw_cell:>8} {walls_cell:>8} "
            f"{_format_oi(r.oi_blank_median):>9} {_format_oi(r.oi_resolved_median):>8} "
            f"{_format_spans(r.spans_on_blank):>10}  {r.verdict}"
        )

    seen = sorted({r.verdict for r in shown})
    lines.append("")
    lines.append("raw / walls columns read 'present on blank rows / blank rows'.")
    for verdict in seen:
        lines.append(f"  {verdict}: {VERDICT_NOTES[verdict]}")
    return lines


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Underlyings to examine (default: ANALYTICS_UNDERLYINGS).",
    )
    parser.add_argument(
        "--sessions", type=int, default=10, help="How many recent sessions per symbol."
    )
    parser.add_argument("--since", default=None, help="Earliest session date (YYYY-MM-DD).")
    parser.add_argument(
        "--blank-only",
        action="store_true",
        help="Report only sessions that left the flip blank at least once.",
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

    results: List[SessionForensics] = []
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            for symbol in symbols:
                for session_date in session_dates(cursor, symbol, args.sessions, since):
                    result = examine_session(cursor, symbol, session_date)
                    if result is not None:
                        results.append(result)
            conn.rollback()
    except Exception as exc:  # noqa: BLE001 - forensics reports, it does not raise
        logger.error("gamma flip blackout forensics failed: %s", exc, exc_info=True)
        return 2

    if args.json:
        print(json.dumps({"sessions": [r.as_dict() for r in results]}, indent=2))
    else:
        for line in format_report(results, blank_only=args.blank_only):
            print(line)

    return 0


if __name__ == "__main__":
    sys.exit(main())
