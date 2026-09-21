"""A blackout is only explained if the explanation cannot come from an absence.

The investigation that produced this tool made exactly one wrong turn, twice:
reading a missing thing as evidence. An empty journal grep was taken to mean
the flip resolved, when it meant the journal had rotated. The same trap is
built into every column here -- ``gamma_flip_raw`` and the walls were added to
``gex_summary`` by later migrations, so for a session that predates them they
are NULL for reasons that have nothing to do with the resolver, and a verdict
read off them would be fiction stated with a straight face.

So the property these tests exist to hold is narrow and specific: a
discriminator is believed only after the SAME session's resolved rows have
shown that it was capable of being present. Everything else is detail.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytz

from src.tools import gamma_flip_blackout_forensics as tool

ET = pytz.timezone("America/New_York")

SESSION = date(2026, 8, 12)
OPEN = ET.localize(datetime(2026, 8, 12, 9, 30))

CALL_OI = 900_000
PUT_OI = 1_100_000


def _row(
    i,
    *,
    flip=None,
    raw=None,
    span=0.25,
    walls=True,
    call_oi=CALL_OI,
    put_oi=PUT_OI,
):
    return (
        OPEN + timedelta(seconds=30 * i),
        flip,
        raw,
        span,
        6400.0 if walls else None,
        6300.0 if walls else None,
        call_oi,
        put_oi,
    )


def _summarize(rows):
    return tool.summarize_session("NDX", SESSION, rows)


# --- calibration: an absence proves nothing until a presence is shown ------


def test_raw_missing_from_resolved_rows_too_is_not_a_cause():
    """The column was not being written that month. That is not a verdict."""
    rows = [_row(i, flip=23000.0 + i, raw=None) for i in range(10)]
    rows += [_row(i, flip=None, raw=None) for i in range(10, 20)]
    assert _summarize(rows).verdict == tool.VERDICT_INCONCLUSIVE


def test_raw_present_on_resolved_rows_makes_its_absence_mean_something():
    """Same blank rows, but now the column demonstrably works this session."""
    rows = [_row(i, flip=23000.0 + i, raw=22900.0 + i) for i in range(10)]
    rows += [_row(i, flip=None, raw=None) for i in range(10, 20)]
    assert _summarize(rows).verdict == tool.VERDICT_NO_CROSSING


def test_a_session_with_no_resolved_rows_cannot_calibrate_anything():
    """100% blank is the worst case and the one with no in-session baseline."""
    result = _summarize([_row(i, flip=None, raw=None) for i in range(20)])
    assert result.blank == 20
    assert not result.raw_calibrated
    assert not result.walls_calibrated
    assert result.verdict == tool.VERDICT_INCONCLUSIVE


def test_all_blank_with_raw_present_still_indicts_the_gate():
    """Raw needs no baseline when it is PRESENT: presence is self-evidencing."""
    rows = [_row(i, flip=None, raw=22900.0 + i) for i in range(20)]
    assert _summarize(rows).verdict == tool.VERDICT_GATE


# --- the discriminators ----------------------------------------------------


def test_raw_on_blank_rows_points_at_the_publish_gate_not_the_feed():
    rows = [_row(i, flip=23000.0 + i, raw=22900.0 + i) for i in range(5)]
    rows += [_row(i, flip=None, raw=22900.0 + i) for i in range(5, 20)]
    assert _summarize(rows).verdict == tool.VERDICT_GATE


def test_walls_also_blank_points_at_the_snapshot():
    rows = [_row(i, flip=23000.0 + i, raw=22900.0 + i, walls=True) for i in range(10)]
    rows += [_row(i, flip=None, raw=None, walls=False) for i in range(10, 20)]
    assert _summarize(rows).verdict == tool.VERDICT_CHAIN


def test_walls_holding_while_the_flip_is_blank_exonerates_ingestion():
    rows = [_row(i, flip=23000.0 + i, raw=22900.0 + i) for i in range(10)]
    rows += [_row(i, flip=None, raw=None, walls=True) for i in range(10, 20)]
    assert _summarize(rows).verdict == tool.VERDICT_NO_CROSSING


def test_open_interest_collapse_outranks_the_gate_verdict():
    """The profile skips oi<=0, so an empty chain explains a blank row first."""
    rows = [_row(i, flip=23000.0 + i, raw=22900.0 + i) for i in range(10)]
    rows += [_row(i, flip=None, raw=None, call_oi=1_000, put_oi=1_000) for i in range(10, 20)]
    result = _summarize(rows)
    assert result.oi_collapsed
    assert result.verdict == tool.VERDICT_OI


def test_ordinary_intraday_open_interest_drift_is_not_a_collapse():
    rows = [_row(i, flip=23000.0 + i, raw=22900.0 + i) for i in range(10)]
    rows += [
        _row(i, flip=None, raw=None, call_oi=int(CALL_OI * 0.9), put_oi=int(PUT_OI * 0.9))
        for i in range(10, 20)
    ]
    result = _summarize(rows)
    assert not result.oi_collapsed
    assert result.verdict == tool.VERDICT_NO_CROSSING


def test_a_clean_session_says_so_without_inspecting_anything_else():
    rows = [_row(i, flip=23000.0 + i, raw=None, walls=False) for i in range(20)]
    assert _summarize(rows).verdict == tool.VERDICT_CLEAN


def test_a_handful_of_odd_cycles_does_not_become_the_session_verdict():
    """Two blank rows carrying raw out of fifteen is noise, not a gate regime."""
    rows = [_row(i, flip=23000.0 + i, raw=22900.0 + i) for i in range(10)]
    rows += [_row(i, flip=None, raw=22900.0 + i) for i in range(10, 12)]
    rows += [_row(i, flip=None, raw=None) for i in range(12, 25)]
    assert _summarize(rows).verdict == tool.VERDICT_NO_CROSSING


# --- bookkeeping -----------------------------------------------------------


def test_counts_and_percentages_describe_the_session_that_was_read():
    rows = [_row(i, flip=23000.0 + i, raw=22900.0 + i) for i in range(6)]
    rows += [_row(i, flip=None, raw=None) for i in range(6, 10)]
    result = _summarize(rows)
    assert (result.rows, result.blank, result.resolved) == (10, 4, 6)
    assert result.blank_pct == 40.0
    assert result.resolved_with_raw == 6
    assert result.blank_with_raw == 0


def test_spans_on_blank_rows_are_reported_so_ladder_exhaustion_is_visible():
    rows = [_row(i, flip=None, raw=None, span=0.25) for i in range(8)]
    rows += [_row(i, flip=None, raw=None, span=0.4) for i in range(8, 10)]
    assert _summarize(rows).spans_on_blank == ((0.25, 8), (0.4, 2))


def test_a_session_with_no_rows_is_not_a_session():
    assert _summarize([]) is None


def test_every_verdict_the_tool_can_return_has_a_note_to_print():
    """The verdict column is useless to an operator who has to read the source."""
    verdicts = {
        value
        for name, value in vars(tool).items()
        if name.startswith("VERDICT_") and isinstance(value, str)
    }
    assert verdicts == set(tool.VERDICT_NOTES)


def test_report_renders_each_verdict_it_shows_and_hides_clean_on_request():
    clean = _summarize([_row(i, flip=23000.0 + i, raw=22900.0 + i) for i in range(4)])
    blank = _summarize([_row(i, flip=None, raw=22900.0 + i) for i in range(4)])
    body = "\n".join(tool.format_report([clean, blank]))
    assert tool.VERDICT_NOTES[tool.VERDICT_GATE] in body
    assert tool.VERDICT_CLEAN in body

    only_blank = "\n".join(tool.format_report([clean, blank], blank_only=True))
    assert tool.VERDICT_NOTES[tool.VERDICT_CLEAN] not in only_blank


def test_json_payload_carries_the_verdict_and_the_evidence_behind_it():
    rows = [_row(i, flip=23000.0 + i, raw=22900.0 + i) for i in range(5)]
    rows += [_row(i, flip=None, raw=22900.0 + i) for i in range(5, 10)]
    payload = _summarize(rows).as_dict()
    assert payload["verdict"] == tool.VERDICT_GATE
    assert payload["blank_with_raw"] == 5
    assert payload["resolved_with_raw"] == 5
    assert payload["spans_on_blank"] == [[0.25, 5]]
