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


def _loaded(cursor, optional_columns, warmup=6, confirm_bars=1):
    """Load a day and classify it.

    ``confirm_bars=1`` by default so these tests read the classifier's raw
    output: what is under test here is the loading, and the confirmation rule
    has its own tests in tests/test_gamma_weather.py.
    """
    loaded = tool.load_session(cursor, "SPY", date(2026, 9, 17), optional_columns)
    return None if loaded is None else loaded.classify(warmup, confirm_bars)


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

    session = _loaded(cursor, ["typical_move_30m"])

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

    session = _loaded(cursor, ["typical_move_30m"])

    assert session.ages[0] == 1
    assert session.ages[1] == 2
    assert session.ages[2] == 1  # state changed, clock restarts
    assert session.ages[-1] == 10


def test_warnings_are_the_cushion_transition_risk_flag():
    cursor, _ = _cursor(12)

    session = _loaded(cursor, ["typical_move_30m"])

    assert list(session.warnings) == [False] * 12  # a wide, steady cushion


def test_the_session_window_is_the_one_the_endpoint_serves():
    """09:30 ET plus 6h45m, matching _resolve_flow_series_session. Measuring a
    different day than the panel serves would make every number unfalsifiable
    against what a user saw."""
    cursor, _ = _cursor(12)

    _loaded(cursor, ["typical_move_30m"])

    _, params = cursor.executed[0]
    start = params["start"].astimezone(ET)
    assert (start.hour, start.minute) == (9, 30)
    assert params["end"] - params["start"] == timedelta(hours=6, minutes=45)


def test_the_flow_query_is_the_canonical_one_not_a_transcription():
    """The single-source-of-truth argument in src/hedging_flow_sql. A second
    copy of this pipeline here would drift from the panel's, and the drift
    would show up as a base rate rather than as an error."""
    cursor, _ = _cursor(12)

    _loaded(cursor, ["typical_move_30m"])

    flow_sql = [sql for sql, _ in cursor.executed if "flow_contract_facts" in sql]
    assert flow_sql == [HEDGING_FLOW_CTE_PSYCOPG2]


def test_a_session_with_no_structure_bars_is_skipped():
    cursor = FakeCursor([], _flow_rows(_bars(12)))

    assert _loaded(cursor, ["typical_move_30m"]) is None


def test_a_session_with_no_classified_flow_is_skipped():
    bars = _bars(12)
    regime_cols = list(tool._REGIME_COLUMNS) + ["typical_move_30m"]
    cursor = FakeCursor(_regime_rows(bars, regime_cols), [])

    assert _loaded(cursor, ["typical_move_30m"]) is None


def test_two_series_with_no_bar_in_common_produce_nothing():
    """Rather than pairing by position, which would put one series' pressure
    beside the other's structure and call it a five-minute reading."""
    regime_cols = list(tool._REGIME_COLUMNS) + ["typical_move_30m"]
    regime = _regime_rows(_bars(6), regime_cols)
    flow = _flow_rows(_bars(6, ET.localize(datetime(2026, 9, 17, 13, 0))))
    cursor = FakeCursor(regime, flow)

    assert _loaded(cursor, ["typical_move_30m"]) is None


def test_a_database_without_the_newest_column_still_reports():
    """The tool probes information_schema rather than hard-coding the column
    list. A report that refuses to run on a database one deploy behind is
    worse than one that runs on the legacy cushion basis."""
    cursor, _ = _cursor(12, columns=())

    session = _loaded(cursor, [])

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

    session = _loaded(cursor, ["typical_move_30m"])

    assert set(tool.CHURN_COMPONENTS).issubset(session.components[0])
    assert session.components[-1]["pressure"] == gw.PRESSURE_BUYING
    assert session.components[-1]["structure"] == gw.STRUCTURE_PINNING
    # Carried but deliberately not a churn row: the basis is the discriminator
    # that decides which sessions the cushion rows may pool, so it belongs in
    # the coverage banner rather than in a table it would restrict to itself.
    assert "cushion basis" in session.components[0]
    assert "cushion basis" not in tool.CHURN_COMPONENTS


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
    raw = _session(list("AB" * 20))  # a header that changes every bar
    confirmed = _session(["A"] * 40)  # the same days, with the rule applied
    report = tool.build_report([confirmed], horizon_bars=3, confirm_bars=2, raw_sessions=[raw])
    text = tool.format_report("SPY", report, skipped=[])

    assert "WHAT CONFIRMATION BUYS" in text
    assert report["confirmation"]["raw_runs"] > report["confirmation"]["confirmed_runs"]
    assert "median lag" in text


def test_the_comparison_needs_the_unconfirmed_side_to_exist():
    """Without it there is nothing to compare against, and a section claiming
    to show what confirmation bought would be showing one number twice."""
    report = tool.build_report([_session(["A"] * 40)], horizon_bars=3, confirm_bars=2)

    assert report["confirmation"] is None
    assert "WHAT CONFIRMATION BUYS" not in tool.format_report("SPY", report, skipped=[])


def test_the_loader_can_classify_one_day_at_two_settings():
    """One database read, two classifications, both through the real
    classifier. The comparison used to debounce the output strings instead,
    which measured a rule the panel does not run."""
    cursor, _ = _cursor(12)
    loaded = tool.load_session(cursor, "SPY", date(2026, 9, 17), ["typical_move_30m"])

    strict = loaded.classify(0, 3)
    raw = loaded.classify(0, 1)

    assert len(strict) == len(raw) == 12
    assert list(raw.states[:2]) == [gw.STATE_MIXED, gw.STATE_MIXED]
    # Three bars of confirmation cannot be met by a two-bar opening run, so
    # the header the session opened with holds longer.
    assert strict.states[2] == gw.STATE_MIXED
    assert raw.states[2] == gw.STATE_STABLE_BID


def test_the_what_if_never_reaches_the_live_classification():
    """It rebuilds a separate session list; the loaded sessions keep the states
    the panel actually showed."""
    raw = _session(list("AABAA"))
    report = tool.build_report([raw], horizon_bars=1, confirm_bars=2)

    assert list(raw.states) == list("AABAA")
    assert report["onset_durability"][0]["group"] in {"A", "B"}


# --------------------------------------------------------------------------- #
# Flip-cushion coverage.
# --------------------------------------------------------------------------- #


def _cushion_session(bases, warmup=0, states=None):
    """A session whose cushion readings were produced by the given bases."""
    from src.analytics import base_rates as br
    from src.analytics.flip_cushion import BASIS_NONE

    n = len(bases)
    return br.Session(
        label="2026-09-17",
        bar_starts=_bars(n),
        states=states or ["A"] * n,
        warnings=[False] * n,
        warmup=warmup,
        components=[
            {
                "pressure": "BUYING",
                "structure": "PINNING",
                "lean": "SUPPORTIVE",
                "cushion": "STEADY",
                "cushion state": "NO_FLIP" if b == BASIS_NONE else "SECURE",
                "cushion basis": b,
            }
            for b in bases
        ],
    )


def _basis(kind):
    from src.analytics.flip_cushion import BASIS_MOVE, BASIS_NONE, BASIS_SPOT

    return {"move": BASIS_MOVE, "spot": BASIS_SPOT, "none": BASIS_NONE}[kind]


def test_a_session_measured_entirely_against_the_typical_move_is_current():
    session = _cushion_session([_basis("move")] * 4)

    assert tool.cushion_basis(session) == (tool.CUSHION_CURRENT, 4, 4)


def test_bars_with_no_flip_do_not_make_a_session_mixed():
    """A profile with no zero crossing is an absence of measurement, not a
    second yardstick. It cannot make the readings around it incomparable."""
    session = _cushion_session([_basis("none"), _basis("move"), _basis("none")])

    assert tool.cushion_basis(session)[0] == tool.CUSHION_CURRENT


def test_a_session_predating_the_typical_move_column_is_legacy():
    session = _cushion_session([_basis("spot")] * 4)

    assert tool.cushion_basis(session) == (tool.CUSHION_LEGACY, 0, 4)


def test_a_column_applied_mid_session_reads_mixed():
    """Which is what actually happened: the ALTER landed during a session and
    the writer carried on. Both halves look measurable and are not comparable."""
    session = _cushion_session([_basis("spot"), _basis("spot"), _basis("move")])

    assert tool.cushion_basis(session)[0] == tool.CUSHION_MIXED


def test_a_session_with_no_cushion_at_all_is_absent():
    session = _cushion_session([_basis("none")] * 4)

    assert tool.cushion_basis(session) == (tool.CUSHION_ABSENT, 0, 4)


def test_basis_classification_respects_warmup():
    session = _cushion_session([_basis("spot"), _basis("spot"), _basis("move")], warmup=2)

    assert tool.cushion_basis(session) == (tool.CUSHION_CURRENT, 1, 1)


def test_the_legacy_yardstick_is_kept_out_of_the_cushion_tables():
    """Worse than a missing flip, because the bars carry ordinary SECURE and
    THIN labels from a rule that cannot return NORMAL at all: pooling changes
    the shape of the distribution, not just its scale."""
    legacy = _cushion_session([_basis("spot")] * 40)
    current = _cushion_session([_basis("move")] * 40)

    report = tool.build_report([legacy, current], horizon_bars=6)

    assert report["cushion_coverage"]["sessions_on_current_basis"] == 1
    assert report["cushion_coverage"]["sessions_on_legacy_basis"] == 1
    churn = {c["component"]: c for c in report["component_churn"]}
    assert churn["cushion state"]["values"]["SECURE"]["n"] == 40  # the legacy day is out
    assert churn["pressure"]["values"]["BUYING"]["n"] == 80  # unrestricted


def test_a_mixed_session_is_held_out_whole():
    """The transition-warning table measures a forward horizon, so it needs
    contiguous bars; taking half a session would leave gaps the horizon would
    silently step over."""
    mixed = _cushion_session([_basis("spot")] * 20 + [_basis("move")] * 20)
    current = _cushion_session([_basis("move")] * 40)

    report = tool.build_report([mixed, current], horizon_bars=6)

    assert report["cushion_coverage"]["sessions_mixed_basis"] == 1
    assert report["cushion_coverage"]["comparable_bars"] == 40  # not 60


def test_the_basis_split_is_stated_not_silently_applied():
    legacy = _cushion_session([_basis("spot")] * 40)
    current = _cushion_session([_basis("move")] * 40)

    text = tool.format_report(
        "SPY", tool.build_report([legacy, current], horizon_bars=6), skipped=[]
    )

    assert "FLIP CUSHION COVERAGE: 1 of 2 sessions" in text
    assert "1 on the legacy spot fraction" in text
    assert "sessions classified against the typical move only" in text


def test_a_fully_current_window_says_nothing_about_coverage():
    """The banner is a warning, not furniture. Once both rollouts have aged
    out of the window it has to disappear on its own."""
    text = tool.format_report(
        "SPY",
        tool.build_report([_cushion_session([_basis("move")] * 40)], horizon_bars=6),
        skipped=[],
    )

    assert "FLIP CUSHION COVERAGE" not in text
    assert "typical move only" not in text


def test_warnings_are_measured_only_where_the_yardstick_is_comparable():
    legacy = _cushion_session([_basis("spot")] * 40)
    current = _cushion_session([_basis("move")] * 40)

    report = tool.build_report([legacy, current], horizon_bars=6)
    total = sum(row["observed"]["n"] for row in report["transition_warnings"])

    assert total <= 40
