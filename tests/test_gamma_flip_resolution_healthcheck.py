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

import pytest
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

    def __init__(self, rows, dates=(SESSION,), spot=29000.0):
        self.rows = rows
        self.dates = dates
        self.spot = spot
        self.executed = []
        self._result = []
        self._one = None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._one = None
        if "SELECT DISTINCT" in sql:
            self._result = [(d,) for d in self.dates]
        elif "underlying_quotes" in sql:
            # The session spot, for the raw-distance test.
            self._one = (self.spot,)
            self._result = []
        else:
            self._result = list(self.rows)

    def fetchall(self):
        return self._result

    def fetchone(self):
        return self._one


def test_check_session_binds_the_cash_session_window():
    cursor = FakeCursor(_rows("RBBR"))
    result = tool.check_session(cursor, "NDX", SESSION)

    assert result.rows == 4
    # The gex_summary read, not the spot lookup that now follows it.
    summary_sql, params = next((sql, p) for sql, p in cursor.executed if "gex_summary" in sql)
    assert params["symbol"] == "NDX"
    assert params["start"], params["end"] == tool.session_window(SESSION)
    assert "gex_summary" in summary_sql
    assert "gamma_flip_raw" in summary_sql, "the raw-distance test needs it selected"

    # ...and the spot lookup binds the same window.
    spot_sql, spot_params = next(
        (sql, p) for sql, p in cursor.executed if "underlying_quotes" in sql
    )
    assert "AVG(close)" in spot_sql
    assert (spot_params["start"], spot_params["end"]) == tool.session_window(SESSION)
    assert "gamma_flip_point" in summary_sql


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


# --- the reason, and the excuse -------------------------------------------
#
# The timer installed on 2026-09-20 fires hourly through the session. On
# 2026-09-21 NDX was blank for all 390 rows and the resolver was RIGHT: the
# flip sat 12% to 37% below spot in a deep long-gamma book, and 375 of those
# rows say BEYOND_MAX_DISTANCE. Left alone, that unit would have emailed seven
# times that day and again the next, about a correct reading. A monitor people
# learn to ignore is the state this whole investigation started in, so the
# check has to be able to tell a fault from a market.


def _reason_rows(pattern, reasons, cadence_seconds=30):
    """``pattern`` of R/B, plus a reason per blank row in order."""
    out = []
    pending = list(reasons)
    for i, ch in enumerate(pattern):
        ts = OPEN + timedelta(seconds=cadence_seconds * i)
        if ch == "B":
            out.append((ts, None, pending.pop(0) if pending else None))
        else:
            out.append((ts, 29000.0 + i, None))
    return out


def test_reasons_are_tallied_over_the_blank_rows_only():
    rows = _reason_rows("BBBRB", ["BEYOND_MAX_DISTANCE"] * 3 + ["EDGE_ONLY"])
    result = tool.summarize_session("NDX", SESSION, rows)
    assert result.reasons == (("BEYOND_MAX_DISTANCE", 3), ("EDGE_ONLY", 1))
    assert result.dominant_reason == "BEYOND_MAX_DISTANCE"


def test_a_session_predating_the_column_reports_no_reason_rather_than_a_guess():
    rows = [(ts, flip) for ts, flip in _rows("BBRB")]
    result = tool.summarize_session("NDX", SESSION, rows)
    assert result.reasons == ()
    assert result.dominant_reason is None


def test_a_session_is_excused_only_when_every_blank_row_is_excusable():
    """99% correct plus 1% broken is not a quiet session."""
    mixed = tool.summarize_session(
        "NDX", SESSION, _reason_rows("BBB", ["BEYOND_MAX_DISTANCE"] * 2 + ["NO_PROFILE"])
    )
    assert not mixed.is_ignored(["BEYOND_MAX_DISTANCE"])
    assert mixed.is_ignored(["BEYOND_MAX_DISTANCE", "NO_PROFILE"])

    clean = tool.summarize_session("NDX", SESSION, _reason_rows("BBB", ["BEYOND_MAX_DISTANCE"] * 3))
    assert clean.is_ignored(["BEYOND_MAX_DISTANCE"])


def test_nothing_is_excused_when_the_operator_asked_for_nothing():
    result = tool.summarize_session("NDX", SESSION, _reason_rows("BBB", ["ONE_SIDED"] * 3))
    assert not result.is_ignored([])


def test_a_session_with_no_recorded_reason_is_never_excused():
    """Absence is not a cause, so it cannot be the grounds for staying quiet."""
    rows = [(ts, flip) for ts, flip in _rows("BBB")]
    result = tool.summarize_session("NDX", SESSION, rows)
    assert not result.is_ignored(["BEYOND_MAX_DISTANCE", "NO_PROFILE", "ONE_SIDED"])


def test_the_report_names_the_reason_and_flags_a_mixed_session():
    single = tool.summarize_session("NDX", SESSION, _reason_rows("BB", ["ONE_SIDED"] * 2))
    mixed = tool.summarize_session(
        "SPX", SESSION, _reason_rows("BB", ["BEYOND_MAX_DISTANCE", "NO_PROFILE"])
    )
    body = "\n".join(tool.format_report([single, mixed], max_blank_minutes=0.0))
    assert "ONE_SIDED" in body
    assert "+1" in body, "a second cause has to be visible, it is the part worth reading"


def test_the_json_payload_carries_the_reasons():
    result = tool.summarize_session("NDX", SESSION, _reason_rows("BB", ["EDGE_ONLY"] * 2))
    payload = result.as_dict()
    assert payload["reasons"] == {"EDGE_ONLY": 2}
    assert payload["dominant_reason"] == "EDGE_ONLY"


# --- the raw-distance discriminator ----------------------------------------
#
# The reason code cannot decide whether to page. BEYOND_MAX_DISTANCE was the
# verdict on 2026-09-17, when our own DTE ramp was hiding a crossing 2.8% from
# spot (a real bug, five weeks and a customer to find), and it was ALSO the
# verdict on 2026-09-21..23, when the crossing genuinely sat 27-37% away in a
# book that was long gamma across the whole band. Same code, opposite calls.
#
# gamma_flip_raw is what separates them: it is the same cycle's crossing with
# no ramp and no gates, so a raw sitting close to spot while the published flip
# is NULL means our pipeline refused something actionable. These use the real
# numbers from both days.

SPOT_SEP17 = 29_424.0
SPOT_SEP23 = 30_500.0


def _raw_rows(pattern, raw_price, reason="BEYOND_MAX_DISTANCE", cadence_seconds=30):
    out = []
    for i, ch in enumerate(pattern):
        ts = OPEN + timedelta(seconds=cadence_seconds * i)
        if ch == "B":
            out.append((ts, None, reason, raw_price))
        else:
            out.append((ts, 29_000.0 + i, None, raw_price))
    return out


def test_a_near_spot_raw_is_ours_and_still_breaches():
    """2026-09-17: raw 28,612 against spot 29,424. Our ramp hid it."""
    result = tool.summarize_session("NDX", SESSION, _raw_rows("BBBB", 28_612.0), SPOT_SEP17)
    assert result.raw_distance_pct == pytest.approx(2.76, abs=0.05)
    assert not result.is_distant(8.0), "a 2.8% crossing is one we refused, page for it"


def test_a_far_raw_is_the_market_and_is_excused():
    """2026-09-23: raw 19,442 against spot ~30,500. Nothing to refuse."""
    result = tool.summarize_session("NDX", SESSION, _raw_rows("BBBB", 19_442.0), SPOT_SEP23)
    assert result.raw_distance_pct == pytest.approx(36.3, abs=0.2)
    assert result.is_distant(8.0)


def test_the_threshold_is_the_one_being_asked_about():
    result = tool.summarize_session("NDX", SESSION, _raw_rows("BBBB", 19_442.0), SPOT_SEP23)
    assert result.is_distant(8.0)
    assert not result.is_distant(40.0)
    assert not result.is_distant(None), "unset means excuse nothing"


def test_a_session_with_no_raw_on_its_blank_rows_is_never_excused():
    """An absence is not evidence that the market did it."""
    rows = [(ts, flip, "ONE_SIDED", None) for ts, flip in _rows("BBBB")]
    result = tool.summarize_session("NDX", SESSION, rows, SPOT_SEP23)
    assert result.raw_distance_pct is None
    assert not result.is_distant(8.0)


def test_no_stored_spot_means_no_excuse():
    result = tool.summarize_session("NDX", SESSION, _raw_rows("BBBB", 19_442.0), None)
    assert result.raw_distance_pct is None
    assert not result.is_distant(8.0)


def test_only_the_blank_rows_count_toward_the_distance():
    """A resolved row's raw says nothing about why the blank ones were blank."""
    rows = _raw_rows("RRRB", 19_442.0)
    near = tool.summarize_session("NDX", SESSION, rows, SPOT_SEP23)
    assert near.raw_distance_pct == pytest.approx(36.3, abs=0.2)


def test_the_median_survives_a_couple_of_odd_cycles():
    """One wild raw should not flip a call the pager keys on."""
    rows = _raw_rows("BBBB", 19_442.0) + _raw_rows("B", 29_900.0)
    result = tool.summarize_session("NDX", SESSION, rows, SPOT_SEP23)
    assert result.is_distant(8.0)


def test_the_distance_is_reported_so_an_operator_can_see_the_call():
    far = tool.summarize_session("NDX", SESSION, _raw_rows("BB", 19_442.0), SPOT_SEP23)
    body = "\n".join(tool.format_report([far], max_blank_minutes=0.0))
    assert "36%" in body
    unknown = tool.summarize_session("NDX", SESSION, _raw_rows("BB", 19_442.0), None)
    assert "-" in "\n".join(tool.format_report([unknown], max_blank_minutes=0.0))
