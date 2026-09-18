"""Sustained gamma-flip carry-forward has to be visible without a hand query.

Carrying the last measured flip across a five-minute window that received no
``gex_summary`` row is the right answer -- the alternative, a NULL, is read by
every consumer as "the profile has no crossing" (see
``tests/test_gamma_flip_carry_forward.py``). But a session mostly assembled
from carried levels is a degraded reading wearing the clothes of a normal one,
and the carry itself leaves no mark on the row. This check is the mark.

Without these tests the tool could report a stalled session as healthy (the
counts), fail a session for the ordinary lag of a bar that has only just opened
(the threshold), or measure carry depth in STORED bars rather than minutes, so
that a session the writer had skipped bars in would under-report how long
``gex_summary`` was actually silent.

It also pins the reason no ``gamma_flip_carried`` column was added: the
provenance is recoverable by joining the stored bars against ``gex_summary``,
which is retention-exempt -- so the check reads history written long before it
existed, which a column could not.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytz

from src.analytics.gamma_flip_carry import CARRY_WARN_BARS
from src.tools import gamma_flip_carry_healthcheck as tool

ET = pytz.timezone("America/New_York")

SESSION = date(2026, 9, 17)
BARS = tool.session_grid(SESSION)


class FakeCursor:
    """Dispatches on the SQL it is handed and records what it was asked.

    ``observations`` is ``{bar_start: gamma_flip_point}`` for the bars that had
    a ``gex_summary`` row; a bar absent from it had NO row, which is the
    distinction the whole check exists to report and is modelled the way the
    real query models it -- by emitting no row for that bar.
    """

    def __init__(self, stored, observations, dates=(SESSION,)):
        self.stored = stored
        self.observations = observations
        self.dates = dates
        self.executed = []
        self._result = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "SELECT DISTINCT" in sql:
            self._result = [(d,) for d in self.dates]
        elif "FROM gamma_regime_5min" in sql:
            self._result = sorted(self.stored.items())
        else:
            self._result = sorted(self.observations.items())

    def fetchall(self):
        return self._result


def _check(stored, observations):
    return tool.check_session(FakeCursor(stored, observations), "SPY", SESSION)


def test_a_fully_measured_session_reports_no_carry():
    report = _check({b: 690.0 for b in BARS}, {b: 690.0 for b in BARS})

    assert report["measured"] == report["bars"] == len(BARS)
    assert (report["carried"], report["unresolved"], report["deepest_carry_bars"]) == (0, 0, 0)


def test_a_session_resolved_from_one_reading_reports_the_whole_carry():
    """The shape a stalled upstream leaves: one row at the open and nothing
    after it. Every later bar is a stand-in, and the depth says how long."""
    report = _check({b: 690.0 for b in BARS}, {BARS[0]: 690.0})

    assert report["measured"] == 1
    assert report["carried"] == len(BARS) - 1
    assert report["deepest_carry_bars"] == len(BARS) - 1
    assert report["deepest_carry_minutes"] == (len(BARS) - 1) * tool.BAR_MINUTES


def test_a_measured_null_counts_as_measured_not_as_a_gap():
    """A row reporting no crossing is a reading. Counting it as a gap would
    make a genuinely one-signed session look like a broken feed."""
    report = _check({b: None for b in BARS}, {b: None for b in BARS})

    assert report["measured"] == len(BARS)
    assert report["carried"] == 0


def test_bars_before_the_first_reading_are_unresolved_not_carried():
    """Nothing legitimate sits behind them: reaching past 09:30 ET for a level
    would import yesterday's market, so NULL is forced and said so."""
    report = _check({b: 690.0 for b in BARS}, {BARS[3]: 690.0})

    assert report["unresolved"] == 3
    assert report["carried"] == len(BARS) - 4


def test_carry_depth_is_counted_in_minutes_not_in_stored_bars():
    """The writer skips a bar it has no chain for. Counting depth in stored
    bars would then shorten every gap by however many bars it skipped, and a
    twenty-minute silence would be filed as a five-minute one."""
    stored = {BARS[0]: 690.0, BARS[4]: 690.0}
    report = _check(stored, {BARS[0]: 690.0})

    assert report["bars"] == 2
    assert report["deepest_carry_bars"] == 4
    assert report["deepest_carry_minutes"] == 20


def test_bars_stored_null_that_a_carry_would_have_filled_are_flagged_as_pre_fix():
    """The footprint of the old per-bar lookup: a window with no row stored
    NULL. The writer cannot produce one now, so a count here is history."""
    stored = {BARS[0]: 690.0, BARS[1]: None, BARS[2]: None}
    report = _check(stored, {BARS[0]: 690.0})

    assert report["pre_fix_nulls"] == 2


def test_a_session_with_no_stored_bars_is_skipped_rather_than_scored():
    """A day the writer never ran is not a day with a carry problem."""
    assert _check({}, {BARS[0]: 690.0}) is None


def test_the_check_reads_the_same_session_window_the_writer_writes():
    """09:30 ET plus 6h45m, inclusive of the last bar's start. A different
    window would score bars the writer never resolved together."""
    assert BARS[0] == ET.localize(datetime(2026, 9, 17, 9, 30))
    assert BARS[-1] == ET.localize(datetime(2026, 9, 17, 16, 15))
    assert BARS[1] - BARS[0] == timedelta(minutes=tool.BAR_MINUTES)


def test_the_exit_code_fails_only_on_a_carry_past_the_threshold(monkeypatch):
    """A one-bar carry is the newest bar not yet having had a row of its own.
    Failing on it would make the check cry wolf ~78 times a session."""

    def run(observations):
        cursor = FakeCursor({b: 690.0 for b in BARS}, observations)
        conn = _FakeConn(cursor)
        monkeypatch.setattr(tool, "db_connection", lambda: conn)
        return tool.main(["--symbols", "SPY", "--sessions", "1"])

    shallow = {b: 690.0 for b in BARS}
    del shallow[BARS[5]]
    assert run(shallow) == 0

    deep = {b: 690.0 for b in BARS}
    for gap in range(5, 5 + CARRY_WARN_BARS):
        del deep[BARS[gap]]
    assert run(deep) == 1


def test_a_session_with_no_readings_at_all_fails_rather_than_passing(monkeypatch):
    """The worst case in the file, and the one a carry-depth-only gate would
    wave through: gex_summary wrote NOTHING, so every bar is unresolved and
    ZERO are carried -- there was never a reading to carry. Every consumer
    reads the stored NULLs as "no gamma flip in the profile"."""
    cursor = FakeCursor({b: None for b in BARS}, {})
    conn = _FakeConn(cursor)
    monkeypatch.setattr(tool, "db_connection", lambda: conn)

    report = _check({b: None for b in BARS}, {})
    assert (report["carried"], report["deepest_carry_bars"]) == (0, 0)
    assert report["unresolved"] == len(BARS)

    assert tool.main(["--symbols", "SPY", "--sessions", "1"]) == 1


def test_a_session_that_starts_slowly_is_not_failed_for_its_warmup(monkeypatch):
    """gex_summary's first row of the day can land a bar or two after 09:30.
    That is a warmup, and it is held to the same depth as a carry rather than
    alerting on the open of every session."""
    observations = {b: 690.0 for b in BARS[2:]}
    cursor = FakeCursor({b: 690.0 for b in BARS}, observations)
    conn = _FakeConn(cursor)
    monkeypatch.setattr(tool, "db_connection", lambda: conn)

    assert _check({b: 690.0 for b in BARS}, observations)["unresolved"] == 2
    assert tool.main(["--symbols", "SPY", "--sessions", "1"]) == 0


def test_the_check_writes_nothing(monkeypatch):
    """Read-only: it is meant to be safe to run mid-session against prod."""
    cursor = FakeCursor({b: 690.0 for b in BARS}, {BARS[0]: 690.0})
    conn = _FakeConn(cursor)
    monkeypatch.setattr(tool, "db_connection", lambda: conn)

    tool.main(["--symbols", "SPY", "--sessions", "1"])

    assert conn.rolled_back and not conn.committed
    for sql, _ in cursor.executed:
        assert sql.strip().upper().startswith(("SELECT", "WITH")), sql


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return _CursorCM(self._cursor)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _CursorCM:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self._cursor

    def __exit__(self, *exc):
        return False


def test_the_universe_defaults_to_the_engine_that_writes_both_tables(monkeypatch):
    """The analytics engine writes gamma_regime_5min AND gex_summary, one
    worker per symbol. A monitor pinned to SPY would pass a session in which
    every other underlying's flip went dark, so the default follows the engine
    rather than a constant."""
    monkeypatch.setenv("ANALYTICS_UNDERLYINGS", "SPY,QQQ")
    assert tool.configured_symbols() == ["SPY", "QQQ"]

    monkeypatch.delenv("ANALYTICS_UNDERLYINGS")
    monkeypatch.setenv("ANALYTICS_UNDERLYING", "SPX")
    assert tool.configured_symbols() == ["SPX"]

    monkeypatch.delenv("ANALYTICS_UNDERLYING")
    assert tool.configured_symbols() == ["SPY"]


def test_every_configured_symbol_is_checked_and_named_in_its_own_report(monkeypatch):
    """A failure has to say WHICH underlying went dark; one pooled count would
    send an operator to the wrong engine worker."""
    monkeypatch.setenv("ANALYTICS_UNDERLYINGS", "SPY,QQQ")
    cursor = FakeCursor({b: 690.0 for b in BARS}, {BARS[0]: 690.0})
    conn = _FakeConn(cursor)
    monkeypatch.setattr(tool, "db_connection", lambda: conn)

    assert tool.main(["--sessions", "1"]) == 1

    checked = [params["symbol"] for sql, params in cursor.executed if "SELECT DISTINCT" in sql]
    assert checked == ["SPY", "QQQ"]
