"""Telling apart the two reasons an option snapshot maps nothing.

On 2026-10-19, the first production session on ThetaData, this warning fired
at 08:58 and again at 16:44 the next day:

    thetadata ohlc: 383 row(s) returned but no strike / right column matched.
    Columns present: [... 'right', 'strike' ...]

The columns it says did not match are listed as present, which is not a
contradiction -- `_pick` requires a key AND a non-null value, and the join
happens afterwards -- but the message could not say which of those had
happened, because it logged column NAMES and no values. Two investigations
reached two different wrong conclusions from it.

`matched == 0` has two causes needing opposite fixes:

* a field that cannot be READ (missing column, misspelled column, null
  value) -- a `_FIELD_CANDIDATES` or vendor problem;
* fields that read cleanly and JOIN TO NOTHING -- wrong strike units (the
  SPX-6500-as-6.50 shape `_normalise_strike` documents) or a response whose
  strike grid excludes every requested contract.

These tests pin that the log names which one, carries the values, and -- the
part that was actively harmful -- that one cause no longer silences the other.
"""

from __future__ import annotations

import logging

import pytest

import src.ingestion.providers.thetadata as td


@pytest.fixture(autouse=True)
def _clear_throttle():
    """The reporter dedupes for the life of the process; tests are not that."""
    td._REPORTED_SHAPES.clear()
    yield
    td._REPORTED_SHAPES.clear()


def _rows(strike, right, n=3):
    return [{"strike": strike, "right": right, "close": 1.0, "volume": 5} for _ in range(n)]


def test_an_unreadable_field_names_the_columns_and_the_remedy(caplog):
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        td.report_unmapped("ohlc", _rows(None, "C"), ("strike", "right"), unreadable=3)
    text = caplog.text
    assert "could not be READ" in text
    assert "_FIELD_CANDIDATES" in text
    # The values, which is the half that was never implemented.
    assert "'strike': None" in text


def test_a_clean_read_that_joins_to_nothing_does_not_blame_the_spelling(caplog):
    """The 2026-10-19 message sent two readers after a misspelled column when
    the columns were present and correctly spelled. That is worse than silence:
    it is a confident wrong instruction."""
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        td.report_unmapped("ohlc", _rows(6.50, "C"), ("strike", "right"), unmatched=3)
    text = caplog.text
    assert "JOINED TO NOTHING" in text
    assert "Not a column-spelling problem" in text
    assert "_FIELD_CANDIDATES" not in text
    assert "_normalise_strike" in text, "point at the units, which is the live hypothesis"
    assert "6.5" in text, "the value is what separates units from nulls"


def test_one_cause_no_longer_silences_the_other(caplog):
    """The throttle keyed on columns alone, so the FIRST report suppressed
    every later one with the same columns for the life of the process. A real
    mapping failure arriving after a benign-looking one was invisible."""
    same_columns = ("strike", "right")
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        td.report_unmapped("ohlc", _rows(6.50, "C"), same_columns, unmatched=3)
        td.report_unmapped("ohlc", _rows(None, "C"), same_columns, unreadable=3)
    assert "JOINED TO NOTHING" in caplog.text
    assert "could not be READ" in caplog.text


def test_the_same_cause_still_reports_only_once(caplog):
    """The throttle's purpose survives: once per poll would bury the journal."""
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        for _ in range(5):
            td.report_unmapped("ohlc", _rows(None, "C"), ("strike", "right"), unreadable=3)
    assert caplog.text.count("could not be READ") == 1


def test_a_different_endpoint_reports_separately(caplog):
    """quote and ohlc fail independently; one must not mask the other."""
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        td.report_unmapped("ohlc", _rows(None, "C"), ("strike", "right"), unreadable=3)
        td.report_unmapped("quote", _rows(None, "C"), ("strike", "right"), unreadable=3)
    assert caplog.text.count("could not be READ") == 2


def test_an_absent_column_is_distinguishable_from_a_null_one(caplog):
    """'<column absent>' versus None is the difference between a spelling bug
    and the vendor sending an empty field. Same symptom, different owner."""
    rows = [{"close": 1.0, "volume": 5}]
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        td.report_unmapped("ohlc", rows, ("strike", "right"), unreadable=1)
    assert "<column absent>" in caplog.text


def test_no_rows_reports_nothing(caplog):
    """An empty response is a closed market, not a mapping failure."""
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        td.report_unmapped("ohlc", [], ("strike", "right"), unreadable=0)
    assert caplog.text == ""


# ---------------------------------------------------------------------------
# Which chain, and what it was matched against: 2026-10-21
# ---------------------------------------------------------------------------
#
# 487 ohlc rows, first one {'strike': 31750.0, 'right': 'CALL'}, and no way to
# tell from the line which chain it was or what 31750 had been compared with.

EXP = td.date(2026, 10, 23)


def _priced(strikes, right="CALL"):
    return [{"strike": k, "right": right, "expiration": "2026-10-23"} for k in strikes]


def _report(rows, requested):
    td.report_unmapped(
        "ohlc",
        rows,
        ("strike", "right"),
        unmatched=len(rows),
        root="NDXP",
        expiration=EXP,
        requested_strikes=requested,
    )


def test_the_line_names_the_chain_and_both_strike_ranges(caplog):
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        _report(_priced([31750.0, 31800.0]), [28000.0, 28010.0, 28020.0])
    text = caplog.text
    assert "ohlc NDXP 2026-10-23" in text
    assert "2 strike(s) 31750..31800" in text
    assert "3 strike(s) 28000..28020" in text
    assert "ranges do not overlap" in text
    assert "_normalise_strike" in text


def test_shared_strikes_point_at_the_right_not_the_units(caplog):
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        _report(_priced([28000.0, 28010.0], right="X"), [28000.0, 28010.0])
    assert "2 requested strike(s) ARE in the response" in caplog.text
    assert "_CALL_CODES" in caplog.text


def test_interleaved_strikes_are_called_a_different_grid(caplog):
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        _report(_priced([28005.0, 28015.0]), [28000.0, 28010.0, 28020.0])
    assert "different strike grid" in caplog.text


def test_each_failing_expiration_reports_once(caplog):
    rows = _priced([31750.0])
    with caplog.at_level(logging.WARNING, logger=td.logger.name):
        for expiration in (EXP, EXP, td.date(2026, 10, 30)):
            td.report_unmapped(
                "ohlc",
                rows,
                ("strike", "right"),
                unmatched=1,
                root="NDXP",
                expiration=expiration,
                requested_strikes=[28000.0],
            )
    assert caplog.text.count("JOINED TO NOTHING") == 2
