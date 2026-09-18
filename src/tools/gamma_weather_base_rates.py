"""Base-rate report for Gamma Weather -- does a state mean anything?

Answers the question that has to be answered before anyone quotes the panel:
when it says STABLE_BID, does that condition hold longer than a bar picked at
random would have? A durability number on its own cannot say. A state that is
on screen most of the day, in a market where most conditions persist for a bar
or two, will post a healthy-looking hit rate purely from how often it is said.
So every rate here is printed next to the rate the same question gets with the
label ignored, and the ratio of the two -- the lift -- is the number to read.

This implements the Phase 1 validation grid from Barrie's spec, in the order it
asks the questions:

  * the 10:00 / 12:00 / 14:30 ET checkpoints -- did the state stay intact for
    the next 30 minutes?
  * transition warnings -- did the warning actually precede a change?
  * and, added here because the spec's age ladder is worth testing rather than
    assuming: does a state that has already held 30 minutes survive the next
    half hour more often than one five minutes old? If those bands come back
    flat, the clock on the panel is decoration.

WHY THIS IS POSSIBLE AT ALL
---------------------------
Nothing about the weather is stored. ``gamma_regime_5min`` holds components --
spot, the rolling structure scores, the raw gamma flip level -- and the state
is derived on read (see :mod:`src.analytics.gamma_weather`). So this pass
re-classifies the archive under whatever thresholds are live TODAY, rather than
grading a fossil of whatever rule happened to be deployed on each historical
day. Retune a threshold and re-run, and the numbers below describe the retuned
rule across all of history. That was the point of deriving on read, and this is
the tool that collects on it.

It also classifies through :func:`src.analytics.gamma_weather.pair_series`, the
same assembler the live endpoint uses, so these are measurements of the panel
users actually see and not of a lookalike rebuilt here.

READ-ONLY. Runs SELECTs and a rollback; writes nothing.

Usage:
    python -m src.tools.gamma_weather_base_rates
    python -m src.tools.gamma_weather_base_rates --symbol SPY --sessions 40
    python -m src.tools.gamma_weather_base_rates --horizon-minutes 15
    python -m src.tools.gamma_weather_base_rates --json /tmp/weather_base_rates.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Sequence

import pytz

from src.analytics import base_rates as br
from src.analytics import gamma_weather as gw
from src.database.connection import db_connection
from src.hedging_flow_sql import HEDGING_FLOW_COLUMNS, HEDGING_FLOW_CTE_PSYCOPG2

logger = logging.getLogger("zerogex.gamma_weather_base_rates")

ET = pytz.timezone("America/New_York")

#: The grid both stored series are written on.
BAR_MINUTES = 5

#: Session window, matching ``_resolve_flow_series_session`` exactly: 09:30 ET
#: plus 6h45m. Using anything else here would measure a different day than the
#: endpoint serves.
SESSION_OPEN = time(9, 30)
SESSION_LENGTH = timedelta(hours=6, minutes=45)

#: Leading bars barred from starting a measurement. The rolling structure
#: window is 6 bars; until it fills, ``rolling_stability`` is NULL, which
#: :func:`gamma_weather.classify_structure` reads as FLAT and which makes an
#: ACCELERATIVE state unreachable. Durability measured from those bars would
#: be measuring the warmup.
DEFAULT_WARMUP_BARS = 6

#: The spec's daily validation checkpoints.
CHECKPOINTS = (time(10, 0), time(12, 0), time(14, 30))

_REGIME_COLUMNS = (
    "bar_start",
    "spot",
    "rolling_lean",
    "rolling_stability",
    "anchored_stability",
    "gamma_flip",
)

#: Deployed later than the table, so a database one release behind still
#: reports -- it just classifies the cushion on the legacy spot fraction.
_REGIME_OPTIONAL_COLUMNS = ("typical_move_30m",)


def _minutes_to_bars(minutes: float) -> int:
    return max(1, int(round(minutes / BAR_MINUTES)))


# --------------------------------------------------------------------------- #
# Loading.
# --------------------------------------------------------------------------- #


def _present_columns(cursor, table: str, candidates: Sequence[str]) -> List[str]:
    """Which of ``candidates`` the live table actually has.

    The same defensive read the snapshot writer does. A tool that hard-codes a
    column list is one deploy away from failing on a database that has not had
    the migration applied, and a report that refuses to run is worse than one
    that runs with a slightly older cushion basis.
    """
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %(table)s
          AND column_name = ANY(%(names)s)
        """,
        {"table": table, "names": list(candidates)},
    )
    return [row[0] for row in cursor.fetchall()]


def session_dates(cursor, symbol: str, limit: int, since: Optional[date]) -> List[date]:
    """The most recent ET dates with stored structure bars, oldest first."""
    cursor.execute(
        """
        SELECT DISTINCT (bar_start AT TIME ZONE 'America/New_York')::date AS session_date
        FROM gamma_regime_5min
        WHERE symbol = %(symbol)s
          AND (%(since)s::date IS NULL
               OR (bar_start AT TIME ZONE 'America/New_York')::date >= %(since)s::date)
        ORDER BY session_date DESC
        LIMIT %(limit)s
        """,
        {"symbol": symbol, "limit": limit, "since": since},
    )
    return sorted(row[0] for row in cursor.fetchall())


def _window(session_date: date):
    start = ET.localize(datetime.combine(session_date, SESSION_OPEN))
    return start, start + SESSION_LENGTH


def load_session(
    cursor,
    symbol: str,
    session_date: date,
    optional_columns: Sequence[str],
    warmup_bars: int,
) -> Optional[br.Session]:
    """Rebuild one session's weather from stored components.

    Returns ``None`` when the day cannot be classified -- no structure bars, no
    aggressor-classified flow, or no bar the two series share. A skipped day is
    logged rather than silently dropped, because "we had 40 sessions" and "we
    had 40 sessions of which 11 produced nothing" are different claims.
    """
    start, end = _window(session_date)
    columns = list(_REGIME_COLUMNS) + list(optional_columns)

    cursor.execute(
        f"""
        SELECT {", ".join(columns)}
        FROM gamma_regime_5min
        WHERE symbol = %(symbol)s
          AND bar_start >= %(start)s
          AND bar_start <= %(end)s
        ORDER BY bar_start
        """,
        {"symbol": symbol, "start": start, "end": end},
    )
    regime_rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    if not regime_rows:
        return None

    cursor.execute(
        HEDGING_FLOW_CTE_PSYCOPG2,
        {
            "symbol": symbol,
            "session_start": start,
            "session_end": end,
            "strikes": None,
            "expirations": None,
        },
    )
    # The canonical query emits newest-first; pair_series wants chronological.
    flow_rows = [dict(zip(HEDGING_FLOW_COLUMNS, row)) for row in cursor.fetchall()]
    flow_rows.reverse()
    if not flow_rows:
        return None

    paired = gw.pair_series(regime_rows, flow_rows)
    if not paired:
        return None

    weather = gw.classify_series([bar.inputs for bar in paired], bar_minutes=float(BAR_MINUTES))

    return br.Session(
        label=session_date.isoformat(),
        bar_starts=[bar.bar_start for bar in paired],
        states=[w.state for w in weather],
        warnings=[w.cushion == gw.CUSHION_TRANSITION_RISK for w in weather],
        ages=[w.age_bars for w in weather],
        warmup=warmup_bars,
    )


def load_sessions(
    conn,
    symbol: str,
    limit: int,
    since: Optional[date],
    warmup_bars: int,
) -> tuple:
    """Load every classifiable session, returning ``(sessions, skipped)``."""
    with conn.cursor() as cursor:
        optional = _present_columns(cursor, "gamma_regime_5min", _REGIME_OPTIONAL_COLUMNS)
        missing = sorted(set(_REGIME_OPTIONAL_COLUMNS) - set(optional))
        if missing:
            logger.warning(
                "gamma_regime_5min is missing %s -- cushion states fall back to the "
                "legacy spot-fraction basis for this run",
                ", ".join(missing),
            )

        dates = session_dates(cursor, symbol, limit, since)
        sessions: List[br.Session] = []
        skipped: List[str] = []
        for session_date in dates:
            loaded = load_session(cursor, symbol, session_date, optional, warmup_bars)
            if loaded is None:
                skipped.append(session_date.isoformat())
                continue
            sessions.append(loaded)

    return sessions, skipped


# --------------------------------------------------------------------------- #
# Reporting.
# --------------------------------------------------------------------------- #


def _pct(rate: Optional[float]) -> str:
    return "    --" if rate is None else f"{rate * 100:5.1f}%"


def _lift_str(lift: Optional[float]) -> str:
    return "  --" if lift is None else f"{lift:4.2f}x"


def _p_str(p: Optional[float]) -> str:
    if p is None:
        return "    --"
    return "<0.001" if p < 0.001 else f"{p:6.3f}"


def _label(state: str) -> str:
    return gw.STATE_LABELS.get(state, state)


def _lift_rows(
    table: Sequence[br.Comparison],
    group_header: str = "state",
    outcome_header: str = "held",
) -> List[str]:
    """One lift table, with the outcome column named for what it counts.

    The last two tables do not measure the same thing as the first two -- one
    groups by age rather than by state, and one counts changes rather than
    survivals -- so the headers are arguments. A column labelled "held" over a
    count of transitions would invert the report's meaning.
    """
    lines = [
        f"   {group_header:<16}{'n':>5}{outcome_header:>8}{'others':>8}"
        f"{'lift':>8}{'p':>8}  verdict",
        f"   {'-' * 14:<16}{'-' * 4:>5}{'-' * 6:>8}{'-' * 6:>8}"
        f"{'-' * 6:>8}{'-' * 6:>8}  {'-' * 12}",
    ]
    for row in table:
        lines.append(
            f"   {_label(row.group):<16}{row.group_p.n:>5}"
            f"{_pct(row.group_p.rate):>8}{_pct(row.other_p.rate):>8}"
            f"{_lift_str(row.lift):>8}{_p_str(row.p_value):>8}  {row.verdict}"
        )
    return lines


def build_report(
    sessions: Sequence[br.Session],
    horizon_bars: int,
) -> Dict[str, Any]:
    """Everything the printer and the JSON both read from."""
    ladder_bars = [
        (gw.AGE_ESTABLISHED_MIN, _minutes_to_bars(gw.AGE_ESTABLISHED_MIN)),
        (gw.AGE_CONFIRMED_MIN, _minutes_to_bars(gw.AGE_CONFIRMED_MIN)),
        (gw.AGE_DURABLE_MIN, _minutes_to_bars(gw.AGE_DURABLE_MIN)),
    ]

    lengths = br.run_lengths(sessions)
    onset = br.lift_table(
        br.survival_trials(sessions, horizon_bars, br.onset_anchors),
        independent=True,
    )
    checkpoint = br.lift_table(
        br.survival_trials(
            sessions,
            horizon_bars,
            lambda s: br.checkpoint_anchors(
                s, CHECKPOINTS, ET, max_stale=timedelta(minutes=BAR_MINUTES)
            ),
        ),
        independent=True,
    )
    bands = [
        (f"under {gw.AGE_ESTABLISHED_MIN}m", 1, _minutes_to_bars(gw.AGE_ESTABLISHED_MIN)),
        (
            f"{gw.AGE_ESTABLISHED_MIN}-{gw.AGE_CONFIRMED_MIN}m",
            _minutes_to_bars(gw.AGE_ESTABLISHED_MIN),
            _minutes_to_bars(gw.AGE_CONFIRMED_MIN),
        ),
        (
            f"{gw.AGE_CONFIRMED_MIN}-{gw.AGE_DURABLE_MIN}m",
            _minutes_to_bars(gw.AGE_CONFIRMED_MIN),
            _minutes_to_bars(gw.AGE_DURABLE_MIN),
        ),
        (f"{gw.AGE_DURABLE_MIN}m+", _minutes_to_bars(gw.AGE_DURABLE_MIN), None),
    ]
    age = br.lift_table(
        br.age_band_trials(sessions, horizon_bars, bands),
        independent=False,
        order=[label for label, _, _ in bands],
    )
    warnings = br.lift_table(
        br.warning_trials(sessions, horizon_bars),
        independent=False,
        order=["WARNED", "QUIET"],
    )

    return {
        "sessions": len(sessions),
        "bars": sum(len(s) for s in sessions),
        "horizon_bars": horizon_bars,
        "horizon_minutes": horizon_bars * BAR_MINUTES,
        "share": {state: p.as_dict() for state, p in br.state_share(sessions).items()},
        "run_lengths": {
            state: {
                **lengths[state].as_dict(),
                "survival": {
                    f"{minutes}m": lengths[state].survival_at(bars).as_dict()
                    for minutes, bars in ladder_bars
                },
            }
            for state in sorted(lengths)
        },
        "onset_durability": [c.as_dict() for c in onset],
        "checkpoint_durability": [c.as_dict() for c in checkpoint],
        "age_bands": [c.as_dict() for c in age],
        "transition_warnings": [c.as_dict() for c in warnings],
        "_tables": {
            "onset": onset,
            "checkpoint": checkpoint,
            "age": age,
            "warnings": warnings,
            "lengths": lengths,
            "ladder": ladder_bars,
        },
    }


def format_report(symbol: str, report: Dict[str, Any], skipped: Sequence[str]) -> str:
    tables = report["_tables"]
    horizon = report["horizon_minutes"]
    lines = [
        "=" * 88,
        f"GAMMA WEATHER BASE RATES -- {symbol}  ({report['sessions']} sessions, "
        f"{report['bars']} bars, {horizon}-minute horizon)",
        "=" * 88,
        "",
        "Read the LIFT column, not the hit rate. A state that is on screen most of the",
        "day will post a good-looking hit rate for free; lift is that rate divided by",
        "the rate every OTHER bar got on the same question. 1.00x means the label added",
        "nothing. States are re-derived from stored components under today's thresholds,",
        "so this grades the rule that is live, not the one that shipped that week.",
        "",
        "A state still running at the close is censored, never counted as having ended.",
        "",
    ]
    if skipped:
        lines.append(
            f"Skipped {len(skipped)} session(s) with no classifiable bar: "
            f"{', '.join(skipped[:8])}{' ...' if len(skipped) > 8 else ''}"
        )
        lines.append("")

    if not report["sessions"]:
        lines.append("No classifiable sessions found. Nothing to report.")
        return "\n".join(lines)

    lines.append("1. HOW OFTEN EACH STATE IS ON SCREEN")
    lines.append("   The reason lift matters. Read this before anything below it.")
    for state, entry in sorted(report["share"].items(), key=lambda kv: -kv[1]["hits"]):
        rate = entry["rate"]
        lines.append(f"   {_label(state):<16}{entry['hits']:>6} bars   {_pct(rate):>7} of the tape")
    lines.append("")

    lines.append("2. HOW LONG A STATE LASTS ONCE IT APPEARS")
    lines.append("   Survival is the share of runs still intact after that long.")
    ladder = tables["ladder"]
    header = "".join(f"{str(m) + 'm':>9}" for m, _ in ladder)
    lines.append(f"   {'state':<16}{'runs':>6}{'median':>8}{'censored':>10}{header}")
    for state in sorted(tables["lengths"]):
        entry = tables["lengths"][state]
        summary = entry.as_dict()
        median = summary["median_complete_bars"]
        median_txt = "--" if median is None else f"{median * BAR_MINUTES}m"
        cells = "".join(f"{_pct(entry.survival_at(bars).rate):>9}" for _, bars in ladder)
        lines.append(
            f"   {_label(state):<16}{summary['runs']:>6}{median_txt:>8}"
            f"{summary['censored']:>10}{cells}"
        )
    lines.append("")

    lines.append(f"3. DURABILITY FROM ONSET -- held the next {horizon} minutes?")
    lines.append("   One observation per run, so these do not overlap and the p-value stands.")
    lines.extend(_lift_rows(tables["onset"]))
    lines.append("")

    lines.append(f"4. THE SPEC'S CHECKPOINTS -- 10:00 / 12:00 / 14:30 ET, next {horizon} minutes")
    lines.append("   Three anchors a day, independent by construction. Thin on purpose:")
    lines.append("   this is the grid Phase 1 said to record, measured the way it said.")
    lines.extend(_lift_rows(tables["checkpoint"]))
    lines.append("")

    lines.append(f"5. DOES AGE BUY ANYTHING? -- survival of the next {horizon} minutes by age")
    lines.append("   If these bands are flat, the state-age clock is describing elapsed time")
    lines.append("   and nothing else. Every bar anchors here, so the samples overlap and")
    lines.append("   no p-value is offered: read the direction across bands, not each row.")
    lines.extend(_lift_rows(tables["age"], group_header="state age", outcome_header="held"))
    lines.append("")

    lines.append(f"6. DID TRANSITION WARNINGS PRECEDE CHANGES? -- change within {horizon} minutes")
    lines.append("   A hit here is the state CHANGING, the opposite of the tables above: a")
    lines.append("   warning that is never followed by a transition is the failure to catch.")
    lines.append("   Warnings repeat across consecutive bars, so these overlap heavily.")
    lines.extend(_lift_rows(tables["warnings"], group_header="cushion", outcome_header="changed"))
    lines.append("")

    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Base-rate report for Gamma Weather states (read-only).",
    )
    parser.add_argument("--symbol", default="SPY", help="Symbol to report on (default: SPY).")
    parser.add_argument(
        "--sessions", type=int, default=20, help="Most recent sessions to load (default: 20)."
    )
    parser.add_argument("--since", default=None, help="Only sessions on or after this ET date.")
    parser.add_argument(
        "--horizon-minutes",
        type=float,
        default=30.0,
        help="How far ahead a state must hold (default: 30, the spec's checkpoint horizon).",
    )
    parser.add_argument(
        "--warmup-bars",
        type=int,
        default=DEFAULT_WARMUP_BARS,
        help="Leading bars barred from anchoring, while the rolling window fills (default: 6).",
    )
    parser.add_argument("--json", dest="json_path", default=None, help="Also write JSON here.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    symbol = args.symbol.strip().upper()
    since = date.fromisoformat(args.since) if args.since else None
    horizon_bars = _minutes_to_bars(args.horizon_minutes)

    logger.info(
        "Gamma Weather base rates: symbol=%s sessions=%s horizon=%s bars -- READ-ONLY",
        symbol,
        args.sessions,
        horizon_bars,
    )

    with db_connection() as conn:
        sessions, skipped = load_sessions(conn, symbol, args.sessions, since, args.warmup_bars)
        # This tool never writes. Make the contract explicit even if a future
        # edit adds a statement.
        conn.rollback()

    report = build_report(sessions, horizon_bars)
    print(format_report(symbol, report, skipped))

    if args.json_path:
        payload = {k: v for k, v in report.items() if not k.startswith("_")}
        payload["symbol"] = symbol
        payload["skipped_sessions"] = list(skipped)
        with open(args.json_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, default=str)
        logger.info("Wrote JSON summary to %s", args.json_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
