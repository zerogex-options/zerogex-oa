"""The base-rate report tool -- loading, and the contracts it must not break.

The statistics live in :mod:`src.analytics.base_rates` and are tested there.
What is pinned here is everything the tool does around them: that it reads the
session window the endpoint reads, that it classifies through the same
assembler the panel uses rather than a lookalike, that it runs the canonical
hedging-flow query instead of a transcription of it, that it survives a
database missing the newest column, and that it writes nothing.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytz

from src.analytics import gamma_weather as gw
from src.hedging_flow_sql import HEDGING_FLOW_COLUMNS, HEDGING_FLOW_CTE_PSYCOPG2
from src.tools import gamma_weather_base_rates as tool

ET = pytz.timezone("America/New_York")

STRONG_FLOW = 3.0e7  # clears PRESSURE_FLOOR_USD
STRONG_PIN = 6.0e7  # clears STABILITY_FLAT_BAND_USD


class FakeCursor:
    """Dispatches on the SQL it is handed and records what it was asked."""

    def __init__(self, regime_rows, flow_rows, dates=(), columns=("typical_move_30m",)):
        self.regime_rows = regime_rows
        self.flow_rows = flow_rows
        self.dates = dates
        self.columns = columns
        self.executed = []
        self._result = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "information_schema" in sql:
            self._result = [(name,) for name in self.columns]
        elif "SELECT DISTINCT" in sql:
            self._result = [(d,) for d in self.dates]
        elif "FROM gamma_regime_5min" in sql:
            self._result = list(self.regime_rows)
        else:
            self._result = list(self.flow_rows)

    def fetchall(self):
        return self._result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _bars(n, start=None):
    start = start or ET.localize(datetime(2026, 9, 17, 9, 30))
    return [start + timedelta(minutes=5 * i) for i in range(n)]


def _regime_rows(bars, columns):
    """Rows in the tool's own column order -- buying pressure into a pinning
    book, which classifies as a stable bid once the average fills."""
    rows = []
    for i, bar in enumerate(bars):
        values = {
            "bar_start": bar,
            "spot": 600.0,
            "rolling_lean": 1.0e6,
            "rolling_stability": STRONG_PIN,
            "anchored_stability": STRONG_PIN,
            "gamma_flip": 580.0,
            "typical_move_30m": 3.0,
        }
        rows.append(tuple(values[c] for c in columns))
    return rows


def _flow_rows(bars):
    """Newest-first, matching the canonical query's ORDER BY."""
    rows = []
    for bar in reversed(bars):
        values = {
            "bar_start": bar,
            "call_flow_usd": STRONG_FLOW,
            "put_flow_usd": 0.0,
            "net_flow_usd": STRONG_FLOW,
            "cum_call_usd": STRONG_FLOW,
            "cum_put_usd": 0.0,
            "cum_net_usd": STRONG_FLOW,
            "classified_ratio": 1.0,
            "underlying_price": 600.0,
            "contract_count": 10,
            "is_synthetic": False,
        }
        rows.append(tuple(values[c] for c in HEDGING_FLOW_COLUMNS))
    return rows


def _cursor(n=12, columns=("typical_move_30m",)):
    bars = _bars(n)
    regime_cols = list(tool._REGIME_COLUMNS) + list(columns)
    return FakeCursor(_regime_rows(bars, regime_cols), _flow_rows(bars), columns=columns), bars


# --------------------------------------------------------------------------- #
# Horizons.
# --------------------------------------------------------------------------- #


def test_minutes_convert_to_whole_bars():
    assert tool._minutes_to_bars(30) == 6
    assert tool._minutes_to_bars(15) == 3
    assert tool._minutes_to_bars(5) == 1


def test_a_sub_bar_horizon_still_measures_one_bar():
    """Rounding to zero would make every state trivially "held"."""
    assert tool._minutes_to_bars(1) == 1
    assert tool._minutes_to_bars(0) == 1


# --------------------------------------------------------------------------- #
# Loading.
# --------------------------------------------------------------------------- #


def test_a_session_classifies_into_weather_states():
    cursor, bars = _cursor(12)

    session = tool.load_session(cursor, "SPY", date(2026, 9, 17), ["typical_move_30m"], 6)

    assert len(session) == len(bars)
    assert session.warmup == 6
    # Buying pressure into a pinning book. The first two bars have no three-bar
    # average yet, so they read MIXED before the state settles.
    assert session.states[:2] == [gw.STATE_MIXED, gw.STATE_MIXED]
    assert set(session.states[2:]) == {gw.STATE_STABLE_BID}


def test_ages_come_from_the_classifier_not_from_the_tool():
    """The panel's own clock, so the age bands in the report are the bands a
    user was looking at rather than a second count that could disagree."""
    cursor, _ = _cursor(12)

    session = tool.load_session(cursor, "SPY", date(2026, 9, 17), ["typical_move_30m"], 6)

    assert session.ages[0] == 1
    assert session.ages[1] == 2
    assert session.ages[2] == 1  # state changed, clock restarts
    assert session.ages[-1] == 10


def test_warnings_are_the_cushion_transition_risk_flag():
    cursor, _ = _cursor(12)

    session = tool.load_session(cursor, "SPY", date(2026, 9, 17), ["typical_move_30m"], 6)

    assert list(session.warnings) == [False] * 12  # a wide, steady cushion


def test_the_session_window_is_the_one_the_endpoint_serves():
    """09:30 ET plus 6h45m, matching _resolve_flow_series_session. Measuring a
    different day than the panel serves would make every number unfalsifiable
    against what a user saw."""
    cursor, _ = _cursor(12)

    tool.load_session(cursor, "SPY", date(2026, 9, 17), ["typical_move_30m"], 6)

    _, params = cursor.executed[0]
    start = params["start"].astimezone(ET)
    assert (start.hour, start.minute) == (9, 30)
    assert params["end"] - params["start"] == timedelta(hours=6, minutes=45)


def test_the_flow_query_is_the_canonical_one_not_a_transcription():
    """The single-source-of-truth argument in src/hedging_flow_sql. A second
    copy of this pipeline here would drift from the panel's, and the drift
    would show up as a base rate rather than as an error."""
    cursor, _ = _cursor(12)

    tool.load_session(cursor, "SPY", date(2026, 9, 17), ["typical_move_30m"], 6)

    flow_sql = [sql for sql, _ in cursor.executed if "flow_contract_facts" in sql]
    assert flow_sql == [HEDGING_FLOW_CTE_PSYCOPG2]


def test_a_session_with_no_structure_bars_is_skipped():
    cursor = FakeCursor([], _flow_rows(_bars(12)))

    assert tool.load_session(cursor, "SPY", date(2026, 9, 17), ["typical_move_30m"], 6) is None


def test_a_session_with_no_classified_flow_is_skipped():
    bars = _bars(12)
    regime_cols = list(tool._REGIME_COLUMNS) + ["typical_move_30m"]
    cursor = FakeCursor(_regime_rows(bars, regime_cols), [])

    assert tool.load_session(cursor, "SPY", date(2026, 9, 17), ["typical_move_30m"], 6) is None


def test_two_series_with_no_bar_in_common_produce_nothing():
    """Rather than pairing by position, which would put one series' pressure
    beside the other's structure and call it a five-minute reading."""
    regime_cols = list(tool._REGIME_COLUMNS) + ["typical_move_30m"]
    regime = _regime_rows(_bars(6), regime_cols)
    flow = _flow_rows(_bars(6, ET.localize(datetime(2026, 9, 17, 13, 0))))
    cursor = FakeCursor(regime, flow)

    assert tool.load_session(cursor, "SPY", date(2026, 9, 17), ["typical_move_30m"], 6) is None


def test_a_database_without_the_newest_column_still_reports():
    """The tool probes information_schema rather than hard-coding the column
    list. A report that refuses to run on a database one deploy behind is
    worse than one that runs on the legacy cushion basis."""
    cursor, _ = _cursor(12, columns=())

    session = tool.load_session(cursor, "SPY", date(2026, 9, 17), [], 6)

    assert len(session) == 12
    selected = cursor.executed[0][0]
    assert "typical_move_30m" not in selected


def test_column_probing_reports_only_what_exists():
    cursor = FakeCursor([], [], columns=("typical_move_30m",))

    present = tool._present_columns(cursor, "gamma_regime_5min", ("typical_move_30m", "nope"))

    assert present == ["typical_move_30m"]


def test_session_dates_come_back_oldest_first():
    """The query orders newest-first to honour the LIMIT; the classifier needs
    chronological order, and a reversed session list would age every state
    backwards."""
    cursor = FakeCursor([], [], dates=[date(2026, 9, 17), date(2026, 9, 15)])

    assert tool.session_dates(cursor, "SPY", 5, None) == [date(2026, 9, 15), date(2026, 9, 17)]


# --------------------------------------------------------------------------- #
# Reporting.
# --------------------------------------------------------------------------- #


def _session(states, warmup=0):
    from src.analytics import base_rates as br

    bars = _bars(len(states))
    return br.Session(
        label="2026-09-17",
        bar_starts=bars,
        states=states,
        warnings=[False] * len(states),
        ages=[],
        warmup=warmup,
    )


def test_the_report_carries_every_section_the_spec_asks_for():
    report = tool.build_report([_session(["A"] * 40)], horizon_bars=6)

    for key in (
        "share",
        "run_lengths",
        "onset_durability",
        "checkpoint_durability",
        "age_bands",
        "transition_warnings",
    ):
        assert key in report


def test_the_json_payload_carries_no_private_keys():
    """build_report keeps the rendered tables under a _-prefixed key for the
    printer; they are objects, not data, and must not reach the file."""
    report = tool.build_report([_session(["A"] * 40)], horizon_bars=6)
    payload = {k: v for k, v in report.items() if not k.startswith("_")}

    import json

    assert "_tables" not in payload
    json.loads(json.dumps(payload, default=str))


def test_an_empty_history_reports_nothing_rather_than_zero_rates():
    """A rate of 0% over no observations reads like a finding. It is an absence
    of data and has to say so."""
    report = tool.build_report([], horizon_bars=6)
    text = tool.format_report("SPY", report, skipped=[])

    assert "No classifiable sessions found" in text
    assert "0.0%" not in text


def test_the_report_leads_with_how_often_each_state_is_on_screen():
    """Lift is meaningless to a reader who has not seen the frequencies. The
    ordering of the report is part of the argument it makes."""
    report = tool.build_report([_session(["A"] * 20 + ["B"] * 20)], horizon_bars=3)
    text = tool.format_report("SPY", report, skipped=[])

    assert text.index("HOW OFTEN EACH STATE IS ON SCREEN") < text.index("DURABILITY FROM ONSET")


def test_skipped_sessions_are_named_rather_than_quietly_dropped():
    report = tool.build_report([_session(["A"] * 40)], horizon_bars=6)
    text = tool.format_report("SPY", report, skipped=["2026-09-01", "2026-09-02"])

    assert "Skipped 2 session(s)" in text
    assert "2026-09-01" in text


# --------------------------------------------------------------------------- #
# Diagnosing a restless state.
# --------------------------------------------------------------------------- #


def test_the_classifier_components_are_carried_for_diagnosis():
    """A durability table that fails every row without naming the input that
    is moving sends the reader back to the raw charts, which is the work the
    report exists to have already done."""
    cursor, _ = _cursor(12)

    session = tool.load_session(cursor, "SPY", date(2026, 9, 17), ["typical_move_30m"], 6)

    assert set(session.components[0]) == set(tool.CHURN_COMPONENTS)
    assert session.components[-1]["pressure"] == gw.PRESSURE_BUYING
    assert session.components[-1]["structure"] == gw.STRUCTURE_PINNING


def test_the_attributed_inputs_are_the_ones_the_state_is_built_from():
    """gamma_weather._state_for reads pressure and structure and nothing else,
    so attribution over those two must account for every state change. If this
    drifts, the report's "(none)" bucket fills up and says so."""
    assert set(tool.STATE_INPUTS) == {"pressure", "structure"}
    assert set(tool.STATE_INPUTS).issubset(set(tool.CHURN_COMPONENTS))


def test_the_churn_section_names_every_component():
    report = tool.build_report([_session(["A"] * 40)], horizon_bars=6)

    assert [c["component"] for c in report["component_churn"]] == list(tool.CHURN_COMPONENTS)


def test_the_what_if_section_is_absent_unless_asked_for():
    """Default output describes the live rule and nothing else. A speculative
    comparison printed by default would read as a claim about the product."""
    report = tool.build_report([_session(["A"] * 40)], horizon_bars=6)

    assert report["confirmation"] is None
    assert "WHAT-IF" not in tool.format_report("SPY", report, skipped=[])


def test_the_what_if_reports_both_sides():
    """Churn removed AND lateness added. Reporting only the first would make
    any confirmation window look free."""
    from src.analytics import base_rates as br

    flapping = _session(list("AB" * 20))
    report = tool.build_report([flapping], horizon_bars=3, confirm_bars=2)
    text = tool.format_report("SPY", report, skipped=[])

    assert "WHAT-IF" in text
    assert report["confirmation"]["raw_runs"] > report["confirmation"]["confirmed_runs"]
    assert "median lag" in text
    assert isinstance(br.debounce(flapping.states, 2), list)


def test_the_what_if_never_reaches_the_live_classification():
    """It rebuilds a separate session list; the loaded sessions keep the states
    the panel actually showed."""
    raw = _session(list("AABAA"))
    report = tool.build_report([raw], horizon_bars=1, confirm_bars=2)

    assert list(raw.states) == list("AABAA")
    assert report["onset_durability"][0]["group"] in {"A", "B"}
