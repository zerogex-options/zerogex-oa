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
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Sequence

import pytz

from src.analytics import base_rates as br
from src.analytics import gamma_weather as gw
from src.analytics.flip_cushion import BASIS_MOVE, BASIS_NONE
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

#: Components shown in the churn table. The first two are the only inputs
#: :func:`gamma_weather._state_for` reads, so attribution over them must
#: account for every state change; the rest are shown because a modifier that
#: never fires is worth seeing too.
CHURN_COMPONENTS = ("pressure", "structure", "lean", "cushion", "cushion state")
STATE_INPUTS = ("pressure", "structure")

#: Components that need a stored gamma flip to mean anything. Sessions written
#: before the gamma_flip column existed carry NULL for every bar, which the
#: cushion correctly reads as "no flip in the profile" -- correct per bar, and
#: badly wrong in aggregate, where a schema rollout would otherwise read as a
#: market that never had a flip. Measured over covered sessions only.
CUSHION_COMPONENTS = ("cushion", "cushion state")

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


#: How a session's cushion readings were produced, which decides whether they
#: can be pooled with another session's.
CUSHION_CURRENT = "current"
CUSHION_LEGACY = "legacy"
CUSHION_MIXED = "mixed"
CUSHION_ABSENT = "absent"


def cushion_basis(session: br.Session) -> tuple:
    """``(classification, comparable bars, usable bars)`` for one session.

    Two schema rollouts landed inside the history this report reads, and each
    left bars that look measurable and are not comparable:

    * no ``gamma_flip`` means no boundary to measure against, so every bar
      reads NO_FLIP -- correct per bar, and in aggregate a schema rollout
      wearing the costume of a market that never had a flip;
    * no ``typical_move_30m`` means the cushion falls back to the legacy spot
      fraction. That one is worse, because the bars still carry ordinary
      SECURE and THIN labels produced by a different yardstick. It is not a
      matter of calibration either: the fallback path cannot return NORMAL at
      all, so pooling the two changes the SHAPE of the state distribution and
      not just its scale.

    :attr:`~src.analytics.flip_cushion.CushionBar.basis` already records which
    yardstick produced each reading, so it is the discriminator rather than
    anything this module has to infer. It also subsumes the question of
    whether the column was missing or merely NULL, which the writer's
    information_schema probe cannot answer and which turns out not to matter:
    what matters is the rule that produced the label.

    Classified per SESSION rather than per bar, and mixed sessions are held
    out whole. The transition-warning table measures a forward horizon and so
    needs contiguous bars, and one restriction rule that both cushion tables
    share is easier to trust than two that nearly agree.
    """
    anchors = br.every_bar_anchors(session)
    if not session.components:
        return CUSHION_ABSENT, 0, len(anchors)

    bases = [session.components[i].get("cushion basis") for i in anchors]
    measurable = [b for b in bases if b != BASIS_NONE]
    comparable = sum(1 for b in measurable if b == BASIS_MOVE)

    if not measurable:
        label = CUSHION_ABSENT
    elif comparable == len(measurable):
        label = CUSHION_CURRENT
    elif comparable == 0:
        label = CUSHION_LEGACY
    else:
        label = CUSHION_MIXED

    return label, comparable, len(anchors)


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


@dataclass(frozen=True)
class LoadedSession:
    """One session's classifier inputs, before any confirmation rule is applied.

    Held in this form so the report can classify the same day at more than one
    confirmation setting from a single database read, and so both settings go
    through the real :func:`gamma_weather.classify_series` rather than a
    lookalike. An earlier version debounced the output state strings instead,
    which measured a rule the panel does not run.
    """

    label: str
    bar_starts: List[Any]
    inputs: List[gw.WeatherInputs]
    #: Per-bar cushion state and basis. Neither depends on confirmation, so
    #: they are resolved once at load and reused at every setting.
    cushion_states: List[str]
    cushion_bases: List[str]

    def classify(self, warmup_bars: int, confirm_bars: int) -> br.Session:
        weather = gw.classify_series(
            self.inputs,
            bar_minutes=float(BAR_MINUTES),
            confirm_bars=confirm_bars,
        )
        return br.Session(
            label=self.label,
            bar_starts=self.bar_starts,
            states=[w.state for w in weather],
            warnings=[w.cushion == gw.CUSHION_TRANSITION_RISK for w in weather],
            ages=[w.age_bars for w in weather],
            warmup=warmup_bars,
            components=[
                {
                    "pressure": w.pressure,
                    "structure": w.structure,
                    "lean": str(w.lean_side),
                    "cushion": w.cushion,
                    "cushion state": state,
                    "cushion basis": basis,
                }
                for w, state, basis in zip(weather, self.cushion_states, self.cushion_bases)
            ],
        )


def load_session(
    cursor,
    symbol: str,
    session_date: date,
    optional_columns: Sequence[str],
) -> Optional[LoadedSession]:
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

    return LoadedSession(
        label=session_date.isoformat(),
        bar_starts=[bar.bar_start for bar in paired],
        inputs=[bar.inputs for bar in paired],
        cushion_states=[bar.cushion.state for bar in paired],
        cushion_bases=[bar.cushion.basis for bar in paired],
    )


def load_sessions(
    conn,
    symbol: str,
    limit: int,
    since: Optional[date],
) -> tuple:
    """Load every classifiable session, returning ``(loaded, skipped)``.

    Classification is deliberately NOT done here. The caller decides the
    confirmation setting, and may want the same days at more than one, which
    would otherwise mean reading the database twice.
    """
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
        sessions: List[LoadedSession] = []
        skipped: List[str] = []
        for session_date in dates:
            loaded = load_session(cursor, symbol, session_date, optional)
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


def _median_txt(lengths) -> str:
    if lengths is None:
        return "--"
    median = lengths.as_dict()["median_complete_bars"]
    return "--" if median is None else f"{median * BAR_MINUTES}m"


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


def _confirmation_whatif(
    sessions: Sequence[br.Session],
    raw_sessions: Optional[Sequence[br.Session]],
    horizon_bars: int,
    confirm_bars: int,
) -> Optional[Dict[str, Any]]:
    """What the confirmation rule buys, and what it costs.

    Both sides come from the real classifier, run twice over the same bars at
    different ``confirm_bars`` settings. An earlier version debounced the
    output state strings instead, which measured a rule the panel does not
    run; the point of this report is that it cannot do that.

    Reports the churn removed AND the bars of lateness added. Showing only the
    first would make any confirmation window look free.
    """
    if raw_sessions is None or confirm_bars <= 1:
        return None

    lags: List[int] = []
    for raw, confirmed in zip(raw_sessions, sessions):
        lags.extend(br.confirmation_lag(raw.states, confirmed.states))

    raw_lengths = br.run_lengths(raw_sessions)
    held_lengths = br.run_lengths(sessions)
    ordered_lags = sorted(lags)

    return {
        "raw": raw_lengths,
        "held": held_lengths,
        "summary": {
            "confirm_bars": confirm_bars,
            "raw_runs": sum(v.n_runs for v in raw_lengths.values()),
            "confirmed_runs": sum(v.n_runs for v in held_lengths.values()),
            "confirmed_changes": len(lags),
            "median_lag_bars": ordered_lags[len(ordered_lags) // 2] if ordered_lags else None,
            "survival_raw": br.tally(
                [
                    br.held(s.states, a, horizon_bars)
                    for s in raw_sessions
                    for a in br.onset_anchors(s)
                ]
            ).as_dict(),
            "survival_confirmed": br.tally(
                [br.held(s.states, a, horizon_bars) for s in sessions for a in br.onset_anchors(s)]
            ).as_dict(),
        },
    }


def build_report(
    sessions: Sequence[br.Session],
    horizon_bars: int,
    confirm_bars: int = 1,
    raw_sessions: Optional[Sequence[br.Session]] = None,
) -> Dict[str, Any]:
    """Everything the printer and the JSON both read from.

    ``sessions`` are already classified at whatever confirmation setting is
    being reported. ``raw_sessions`` are the same days classified with
    confirmation off, and when supplied the report adds the comparison that
    says what confirming is worth.
    """
    coverage = [cushion_basis(s) for s in sessions]
    covered = [s for s, (label, _, _) in zip(sessions, coverage) if label == CUSHION_CURRENT]
    basis_counts = {
        label: sum(1 for entry in coverage if entry[0] == label)
        for label in (CUSHION_CURRENT, CUSHION_LEGACY, CUSHION_MIXED, CUSHION_ABSENT)
    }
    # Only bars in the sessions the cushion tables actually use. A mixed
    # session has comparable bars and is still held out whole, so counting its
    # bars here would advertise evidence the tables never saw.
    comparable_bars = sum(bars for label, bars, _ in coverage if label == CUSHION_CURRENT)

    ladder_bars = [
        (gw.AGE_ESTABLISHED_MIN, _minutes_to_bars(gw.AGE_ESTABLISHED_MIN)),
        (gw.AGE_CONFIRMED_MIN, _minutes_to_bars(gw.AGE_CONFIRMED_MIN)),
        (gw.AGE_MATURE_MIN, _minutes_to_bars(gw.AGE_MATURE_MIN)),
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
            f"{gw.AGE_CONFIRMED_MIN}-{gw.AGE_MATURE_MIN}m",
            _minutes_to_bars(gw.AGE_CONFIRMED_MIN),
            _minutes_to_bars(gw.AGE_MATURE_MIN),
        ),
        (f"{gw.AGE_MATURE_MIN}m+", _minutes_to_bars(gw.AGE_MATURE_MIN), None),
    ]
    age = br.lift_table(
        br.age_band_trials(sessions, horizon_bars, bands),
        independent=False,
        order=[label for label, _, _ in bands],
    )
    warnings = br.lift_table(
        br.warning_trials(covered, horizon_bars),
        independent=False,
        order=["WARNED", "QUIET"],
    )

    churn = [
        br.component_churn(covered if name in CUSHION_COMPONENTS else sessions, name)
        for name in CHURN_COMPONENTS
    ]
    attribution = br.change_attribution(sessions, STATE_INPUTS)
    confirmation = _confirmation_whatif(sessions, raw_sessions, horizon_bars, confirm_bars)

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
        "component_churn": [c.as_dict() for c in churn],
        "cushion_coverage": {
            "sessions": len(sessions),
            "sessions_on_current_basis": basis_counts[CUSHION_CURRENT],
            "sessions_on_legacy_basis": basis_counts[CUSHION_LEGACY],
            "sessions_mixed_basis": basis_counts[CUSHION_MIXED],
            "sessions_without_cushion": basis_counts[CUSHION_ABSENT],
            "comparable_bars": comparable_bars,
            "usable_bars": sum(total for _, _, total in coverage),
        },
        "change_attribution": attribution,
        "confirmation": confirmation["summary"] if confirmation else None,
        "_tables": {
            "covered_sessions": len(covered),
            "churn": churn,
            "attribution": attribution,
            "confirmation": confirmation,
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

    cover = report["cushion_coverage"]
    if cover["sessions_on_current_basis"] < cover["sessions"]:
        lines.append(
            f"FLIP CUSHION COVERAGE: {cover['sessions_on_current_basis']} of "
            f"{cover['sessions']} sessions ({cover['comparable_bars']} of "
            f"{cover['usable_bars']} bars) were classified"
        )
        lines.append(
            "against the typical 30-minute move. Section 6 and the cushion rows of section 7"
        )
        lines.append("use only those. Of the rest:")
        lines.append(
            f"   {cover['sessions_on_legacy_basis']} on the legacy spot fraction,"
            f"   {cover['sessions_mixed_basis']} mixed,"
            f"   {cover['sessions_without_cushion']} with no flip stored."
        )
        lines.append("Two schema rollouts sit inside this history. A session with no stored flip")
        lines.append("reads NO_FLIP on every bar; one with no stored typical move still reports")
        lines.append("ordinary SECURE and THIN labels, from a different yardstick that cannot")
        lines.append("return NORMAL at all. Pooling either with the rest would put a rollout on")
        lines.append("the page as a finding about the market.")
        lines.append("")

    lines.append("1. HOW OFTEN EACH STATE IS ON SCREEN")
    lines.append("   The reason lift matters. Read this before anything below it.")
    for state, entry in sorted(report["share"].items(), key=lambda kv: -kv[1]["hits"]):
        rate = entry["rate"]
        lines.append(f"   {_label(state):<16}{entry['hits']:>6} bars   {_pct(rate):>7} of the tape")
    lines.append("")

    lines.append("2. HOW LONG A STATE LASTS ONCE IT APPEARS")
    lines.append("   Survival is the share of runs still intact after that long. If these")
    lines.append("   read one bar across the board, section 7 says which input is moving.")
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
    lines.append(
        f"   Measured over the {tables['covered_sessions']} session(s) on the current basis."
    )
    lines.extend(_lift_rows(tables["warnings"], group_header="cushion", outcome_header="changed"))
    lines.append("")

    lines.append("7. WHY THE STATE MOVES -- how restless each input is")
    lines.append("   A state cannot outlast the inputs it is built from. If the components")
    lines.append("   turn over several times an hour, no combination rule produces an hourly")
    lines.append("   state, and the fix belongs upstream of the vocabulary.")
    lines.append(f"   {'component':<16}{'changes/day':>13}{'mean run':>10}  distribution")
    lines.append(f"   {'-' * 14:<16}{'-' * 11:>13}{'-' * 8:>10}  {'-' * 40}")
    for entry in tables["churn"]:
        per_day = entry.changes_per_session
        per_day_txt = "--" if per_day is None else f"{per_day:.1f}"
        mean_run = entry.mean_run_bars
        run_txt = "--" if mean_run is None else f"{mean_run * BAR_MINUTES:.0f}m"
        spread = ", ".join(
            f"{value} {p.rate * 100:.0f}%"
            for value, p in sorted(entry.values.items(), key=lambda kv: -kv[1].hits)
            if p.rate
        )
        name = entry.name + (" *" if entry.name in CUSHION_COMPONENTS else "")
        lines.append(f"   {name:<16}{per_day_txt:>13}{run_txt:>10}  {spread}")
    if cover["sessions_on_current_basis"] < cover["sessions"]:
        lines.append("   * sessions classified against the typical move only")
    lines.append("")

    attribution = tables["attribution"]
    total_changes = sum(attribution.values())
    if total_changes:
        lines.append(f"   Of {total_changes} state changes, the input that moved with them:")
        for key, count in sorted(attribution.items(), key=lambda kv: -kv[1]):
            lines.append(f"     {key:<28}{count:>6}  {count / total_changes * 100:5.1f}%")
        if attribution.get("(none)"):
            lines.append("     (none) should be zero -- a state changed with no input change, so")
            lines.append("     the decomposition above is missing one of the classifier's inputs.")
        lines.append("")

    confirmation = tables["confirmation"]
    if confirmation:
        summary = confirmation["summary"]
        lines.append(
            f"8. WHAT CONFIRMATION BUYS -- {summary['confirm_bars']} bars before the header moves"
        )
        lines.append("   Everything above already reflects this rule, because the panel runs it.")
        lines.append("   This is the same days classified with confirmation off, for comparison.")
        lines.append("   Both sides are reported: the churn removed, and the lateness it costs.")
        lag = summary["median_lag_bars"]
        lag_txt = "--" if lag is None else f"{lag * BAR_MINUTES}m"
        lines.append(
            f"   runs {summary['raw_runs']} -> {summary['confirmed_runs']}   "
            f"changes surviving confirmation {summary['confirmed_changes']}   "
            f"median lag {lag_txt}"
        )
        lines.append(
            f"   {horizon}-minute survival from onset "
            f"{_pct(summary['survival_raw']['rate'])} -> "
            f"{_pct(summary['survival_confirmed']['rate'])}"
        )
        lines.append("")
        lines.append(f"   {'state':<16}{'runs':>7}{'median':>9}   {'runs':>7}{'median':>9}")
        lines.append(f"   {'':<16}{'-- unconfirmed -':>16}   {'--- as shipped -':>16}")
        for state in sorted(set(confirmation["raw"]) | set(confirmation["held"])):
            raw = confirmation["raw"].get(state)
            kept = confirmation["held"].get(state)
            lines.append(
                f"   {_label(state):<16}"
                f"{(raw.n_runs if raw else 0):>7}{_median_txt(raw):>9}   "
                f"{(kept.n_runs if kept else 0):>7}{_median_txt(kept):>9}"
            )
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
    parser.add_argument(
        "--confirm-bars",
        type=int,
        default=gw.CONFIRM_BARS,
        help=(
            "Bars a new state must repeat before it takes the header. Defaults to the "
            "value the panel runs; pass 1 to see the classifier without confirmation."
        ),
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
        loaded, skipped = load_sessions(conn, symbol, args.sessions, since)
        # This tool never writes. Make the contract explicit even if a future
        # edit adds a statement.
        conn.rollback()

    # One database read, classified twice: once at the setting being reported
    # and once with confirmation off, so the comparison in section 8 costs
    # nothing extra and both sides come from the same bars.
    sessions = [s.classify(args.warmup_bars, args.confirm_bars) for s in loaded]
    raw_sessions = (
        [s.classify(args.warmup_bars, 1) for s in loaded] if args.confirm_bars > 1 else None
    )

    report = build_report(
        sessions,
        horizon_bars,
        confirm_bars=args.confirm_bars,
        raw_sessions=raw_sessions,
    )
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
