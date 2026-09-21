"""A blank gamma flip has to be measurable after the journal has rotated.

The resolver persists NULL when it will not stand behind a crossing, which is
correct and which every consumer renders as nothing at all -- an em dash on the
NinjaTrader panel. A tester watched that dash across several sessions and
neither he nor we could say how long or how often, because the engine's
diagnostic lives in a capped journal that had already rotated past the dates in
question. ``gex_summary`` is retention-exempt, so the answer was recoverable
all along; this tool recovers it.

Without these tests the tool could measure a blackout in ROWS and so report a
different number for the same outage either side of the analytics cadence
change, close a run at its own last row rather than at the row that ended the
blackout (understating every stretch by one cycle), or fail a session on the
share of NULL rows rather than on the longest unbroken stretch -- which is the
thing a trader actually experiences and the thing the sibling carry check is
also keyed on.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytz

from src.tools import gamma_flip_resolution_healthcheck as tool

ET = pytz.timezone("America/New_York")

SESSION = date(2026, 9, 16)
OPEN = ET.localize(datetime(2026, 9, 16, 9, 30))


def _rows(pattern, cadence_seconds=30):
    """``pattern`` is a string of R (resolved) and B (blank), one per row."""
    out = []
    for i, ch in enumerate(pattern):
        ts = OPEN + timedelta(seconds=cadence_seconds * i)
        out.append((ts, None if ch == "B" else 29000.0 + i))
    return out


# --- the measurement ------------------------------------------------------


def test_a_blank_run_ends_at_the_row_that_ended_the_blackout():
    """Not at its own last row: the dash was on screen until something was
    published, and closing early understates every stretch by one cycle."""
    # Four blank rows at 30s, then a resolved row: 09:30:30 -> 09:32:30.
    result = tool.summarize_session("NDX", SESSION, _rows("RBBBBR"))
    assert result.unresolved == 4
    assert result.longest.minutes == 2.0
    assert result.longest.rows == 4
    assert result.longest.open_at_session_end is False


def test_the_blackout_is_minutes_not_rows():
    """The analytics cadence moved from 60s to 30s. The same ten-minute
    outage has to report ten minutes at either cadence, which counting rows
    would not."""
    slow = tool.summarize_session("NDX", SESSION, _rows("R" + "B" * 10 + "R", cadence_seconds=60))
    fast = tool.summarize_session("NDX", SESSION, _rows("R" + "B" * 20 + "R", cadence_seconds=30))
    assert slow.longest.minutes == fast.longest.minutes == 10.0
    assert slow.longest.rows != fast.longest.rows


def test_a_run_open_at_the_session_end_is_flagged_and_measured_to_its_last_row():
    result = tool.summarize_session("NDX", SESSION, _rows("RRBBB"))
    assert result.longest.open_at_session_end is True
    assert result.longest.minutes == 1.0  # 09:31:00 -> 09:32:00, understated by one cycle


def test_the_longest_run_wins_not_the_last_or_the_most_rows():
    # Two stretches: four rows spanning 2.0m, then two rows spanning 1.0m.
    result = tool.summarize_session("NDX", SESSION, _rows("RBBBBRRBBR"))
    assert len(tool.blank_runs(_rows("RBBBBRRBBR"))) == 2
    assert result.longest.minutes == 2.0


def test_a_fully_resolved_session_reports_nothing_blank():
    result = tool.summarize_session("NDX", SESSION, _rows("RRRRRR"))
    assert result.unresolved == 0
    assert result.longest is None
    assert result.longest_blank_minutes == 0.0
    assert result.unresolved_pct == 0.0


def test_a_fully_blank_session_reports_the_whole_span():
    result = tool.summarize_session("NDX", SESSION, _rows("B" * 21))
    assert result.resolved == 0
    assert result.unresolved_pct == 100.0
    assert result.longest.minutes == 10.0
    assert result.longest.open_at_session_end is True


def test_a_session_with_no_rows_is_not_a_session():
    assert tool.summarize_session("NDX", SESSION, []) is None


def test_the_window_is_the_cash_session():
    start, end = tool.session_window(SESSION)
    assert (start.astimezone(ET).hour, start.astimezone(ET).minute) == (9, 30)
    assert (end.astimezone(ET).hour, end.astimezone(ET).minute) == (16, 0)


# --- the report and the threshold ----------------------------------------


def test_the_threshold_is_the_longest_stretch_not_the_share_of_null_rows():
    """A session that is a third NULL in short bursts is usable; one blank for
    forty minutes straight is not, even at a lower share."""
    scattered = tool.summarize_session("NDX", SESSION, _rows("RBRBRBRBRBRB" * 3))
    solid = tool.summarize_session("NDX", SESSION, _rows("R" + "B" * 80 + "R" * 200))

    assert scattered.unresolved_pct > 30.0
    assert scattered.longest_blank_minutes <= 1.0
    assert solid.unresolved_pct < 30.0
    assert solid.longest_blank_minutes == 40.0

    lines = "\n".join(tool.format_report([scattered, solid], max_blank_minutes=30.0))
    assert lines.count("over threshold") == 1
    # Worst blackout first, regardless of share.
    body = [ln for ln in lines.splitlines()[1:]]
    assert "40.0m" in body[0]


def test_the_report_names_the_window_so_the_journal_can_be_grepped():
    result = tool.summarize_session("NDX", SESSION, _rows("R" + "B" * 80 + "R"))
    lines = "\n".join(tool.format_report([result], max_blank_minutes=5.0))
    assert "09:30-10:10 ET" in lines


def test_as_dict_is_json_safe_and_carries_the_window():
    import json

    result = tool.summarize_session("NDX", SESSION, _rows("RBBBBR"))
    payload = json.loads(json.dumps(result.as_dict()))
    assert payload["symbol"] == "NDX"
    assert payload["session_date"] == "2026-09-16"
    assert payload["longest_blank_minutes"] == 2.0
    assert payload["longest_blank_open_at_close"] is False


# --- the queries ----------------------------------------------------------


class FakeCursor:
    """Dispatches on the SQL it is handed and records the bind parameters."""

    def __init__(self, rows, dates=(SESSION,)):
        self.rows = rows
        self.dates = dates
        self.executed = []
        self._result = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "SELECT DISTINCT" in sql:
            self._result = [(d,) for d in self.dates]
        else:
            self._result = list(self.rows)

    def fetchall(self):
        return self._result


def test_check_session_binds_the_cash_session_window():
    cursor = FakeCursor(_rows("RBBR"))
    result = tool.check_session(cursor, "NDX", SESSION)

    assert result.rows == 4
    _sql, params = cursor.executed[-1]
    assert params["symbol"] == "NDX"
    assert params["start"], params["end"] == tool.session_window(SESSION)
    assert "gex_summary" in _sql
    assert "gamma_flip_point" in _sql


def test_session_dates_come_back_oldest_first():
    days = (date(2026, 9, 16), date(2026, 9, 14), date(2026, 9, 15))
    cursor = FakeCursor([], dates=days)
    assert tool.session_dates(cursor, "NDX", 10, None) == sorted(days)


def test_by_date_orders_chronologically_so_a_regime_change_is_visible():
    """The first production run had NDX blank for essentially every session
    through one date and essentially none after it. Severity order interleaved
    the months and hid the break; this is the view that shows it."""
    early = tool.summarize_session("NDX", date(2026, 8, 7), _rows("B" * 21))
    late = tool.summarize_session("NDX", date(2026, 9, 18), _rows("R" * 21))
    middling = tool.summarize_session("NDX", date(2026, 9, 11), _rows("R" + "B" * 10 + "R"))
    results = [late, middling, early]

    by_severity = tool.format_report(results, 30.0)[1:]
    assert by_severity[0].split()[1] == "2026-08-07"  # worst first

    chronological = tool.format_report(results, 30.0, by_date=True)[1:]
    assert [ln.split()[1] for ln in chronological] == [
        "2026-08-07",
        "2026-09-11",
        "2026-09-18",
    ]


def test_by_date_groups_by_symbol_before_date():
    a = tool.summarize_session("NDX", date(2026, 9, 18), _rows("RR"))
    b = tool.summarize_session("SPX", date(2026, 9, 11), _rows("RR"))
    c = tool.summarize_session("NDX", date(2026, 9, 11), _rows("RR"))
    lines = tool.format_report([a, b, c], 30.0, by_date=True)[1:]
    assert [(ln.split()[0], ln.split()[1]) for ln in lines] == [
        ("NDX", "2026-09-11"),
        ("NDX", "2026-09-18"),
        ("SPX", "2026-09-11"),
    ]
