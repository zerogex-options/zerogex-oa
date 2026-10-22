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
# Which chain, and what it was matched against: 2026-10-21/22
# ---------------------------------------------------------------------------
#
# 487 ohlc rows, first one {'strike': 31750.0, 'right': 'CALL'}, and no way to
# tell from the line which chain it was or what 31750 had been compared with.
# Once the line said, every ohlc occurrence was the same expected thing: the
# endpoint lists only contracts that have traded, and the restart seed asks
# for exactly the contracts that have not.

EXP = td.date(2026, 11, 3)


def _listed(contracts):
    return [{"strike": k, "right": r, "expiration": "2026-11-03"} for k, r in contracts]


def _report(kind, rows, requested):
    td.report_unmapped(
        kind,
        rows,
        ("strike", "right"),
        unmatched=len(rows),
        root="SPY",
        expiration=EXP,
        requested_contracts=requested,
    )


def _levels(caplog):
    return [r.levelname for r in caplog.records]


def test_untraded_contracts_missing_from_ohlc_are_info_not_a_warning(caplog):
    """SPY 2026-11-03, 07:24 on 10-22: 63 requested strikes, all of them on
    the response's grid, and not one requested contract listed. The 771 PUT
    traded; the 771 CALL the seed asked for had not."""
    rows = _listed([(771.0, "PUT"), (772.0, "CALL"), (550.0, "PUT"), (865.0, "CALL")])
    with caplog.at_level(logging.INFO, logger=td.logger.name):
        _report("ohlc", rows, [(771.0, "C"), (772.0, "P")])
    assert _levels(caplog) == ["INFO"]
    text = caplog.text
    assert "ohlc SPY 2026-11-03" in text
    assert "4 contract(s) on 4 strike(s) 550..865" in text
    assert "2 contract(s) on 2 strike(s) 771..772" in text
    assert "only contracts that have traded" in text
    assert "JOINED TO NOTHING" not in text


def test_the_same_shape_on_the_quote_endpoint_is_still_a_warning(caplog):
    """The quote snapshot lists both sides of every strike, so a requested
    contract missing from it is a real gap."""
    rows = _listed([(771.0, "PUT"), (772.0, "CALL")])
    with caplog.at_level(logging.INFO, logger=td.logger.name):
        _report("quote", rows, [(771.0, "C")])
    assert _levels(caplog) == ["WARNING"]
    assert "lists every contract" in caplog.text


def test_disjoint_ranges_are_a_warning_even_on_ohlc(caplog):
    """The units bug _normalise_strike documents must never read as routine."""
    rows = _listed([(6.50, "CALL"), (6.55, "PUT")])
    with caplog.at_level(logging.INFO, logger=td.logger.name):
        _report("ohlc", rows, [(6500.0, "C"), (6550.0, "P")])
    assert _levels(caplog) == ["WARNING"]
    assert "ranges do not overlap" in caplog.text
    assert "_normalise_strike" in caplog.text


def test_an_unknown_right_spelling_is_a_warning_even_on_ohlc(caplog):
    """_merge_frame reads any unknown right as a put, so a new spelling
    would join every call to nothing while looking like untraded contracts."""
    rows = _listed([(771.0, "Call"), (771.0, "X")])
    with caplog.at_level(logging.INFO, logger=td.logger.name):
        _report("ohlc", rows, [(771.0, "C")])
    assert _levels(caplog) == ["WARNING"]
    assert "['X']" in caplog.text


def test_each_expiration_reports_once(caplog):
    rows = _listed([(31750.0, "CALL")])
    with caplog.at_level(logging.INFO, logger=td.logger.name):
        for expiration in (EXP, EXP, td.date(2026, 11, 6)):
            td.report_unmapped(
                "ohlc",
                rows,
                ("strike", "right"),
                unmatched=1,
                root="NDXP",
                expiration=expiration,
                requested_contracts=[(29410.0, "C")],
            )
    assert len(caplog.records) == 2


def test_the_restart_seed_path_logs_info_end_to_end(caplog):
    """Through fetch_chain_state, the way seed_new_symbols calls it: the
    contract asked for is not listed by ohlc, its other side is."""
    from tests.test_thetadata_provider import _FakeClient

    symbol = td.build_occ_symbol("SPY", EXP, 771.0, "C")
    client = _FakeClient(
        quote_rows=[{"strike": 771.0, "right": "CALL", "bid": 1.2, "ask": 1.3}],
        ohlc_rows=[{"strike": 771.0, "right": "PUT", "close": 2.0, "volume": 5}],
    )
    with caplog.at_level(logging.INFO, logger=td.logger.name):
        state = td.ThetaDataProvider(client).fetch_chain_state(
            [symbol], include_open_interest=False
        )
    assert state[symbol]["bid"] == 1.2
    assert "last" not in state[symbol]
    assert [r.levelname for r in caplog.records if "ohlc" in r.getMessage()] == ["INFO"]
