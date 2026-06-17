"""Unit tests for the ingestion universe validator.

The CLI itself queries Postgres; these tests pin the pure-Python evaluation
logic (AM-settled detection, pass/fail decision) so the validator's
contract stays stable independent of the test DB.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from src.tools.ingestion_universe_validate import (
    _is_am_settled,
    evaluate,
    render_table,
)


def _row(**overrides):
    base = {
        "expiration": date(2026, 6, 19),
        "option_root": "SPXW",
        "contracts": 80,
        "fresh": 80,
        "last_seen": datetime(2026, 6, 17, 16, 0, tzinfo=timezone.utc),
        "oi_sum": 12345,
        "am_settled": False,
    }
    base.update(overrides)
    return base


def test_am_settled_detects_spx_third_friday():
    # 2026-06-19 is the third Friday of June 2026.
    assert _is_am_settled("SPX", date(2026, 6, 19)) is True


def test_am_settled_rejects_spxw_on_third_friday():
    # SPXW on a 3rd-Friday is PM-settled — root prefix disambiguates.
    assert _is_am_settled("SPXW", date(2026, 6, 19)) is False


def test_am_settled_rejects_non_third_friday():
    # 2026-06-12 = 2nd Friday — weekly only, not AM-settled.
    assert _is_am_settled("SPX", date(2026, 6, 12)) is False


def test_evaluate_passes_when_fresh_contracts_present():
    report = evaluate([_row()], expect_monthly=False)
    assert report["ok"] is True
    assert report["failures"] == []


def test_evaluate_fails_when_no_rows():
    report = evaluate([], expect_monthly=False)
    assert report["ok"] is False
    assert any("no option_chains_latest" in f for f in report["failures"])


def test_evaluate_fails_when_no_fresh_contracts():
    stale = _row(fresh=0)
    report = evaluate([stale], expect_monthly=False)
    assert report["ok"] is False
    assert any("no contracts updated" in f for f in report["failures"])


def test_evaluate_expect_monthly_fails_when_only_weeklies():
    # All rows are SPXW (weekly) -- expect_monthly should flag this.
    rows = [_row(option_root="SPXW", am_settled=False)]
    report = evaluate(rows, expect_monthly=True)
    assert report["ok"] is False
    assert any("AM-settled monthly" in f for f in report["failures"])


def test_evaluate_expect_monthly_passes_when_monthly_present_and_fresh():
    rows = [
        _row(option_root="SPXW", am_settled=False),
        _row(
            option_root="SPX",
            expiration=date(2026, 7, 17),  # 3rd Friday
            am_settled=True,
            contracts=80,
            fresh=80,
        ),
    ]
    report = evaluate(rows, expect_monthly=True)
    assert report["ok"] is True


def test_evaluate_expect_monthly_fails_when_monthly_present_but_stale():
    rows = [
        _row(option_root="SPXW", am_settled=False, fresh=80),
        _row(
            option_root="SPX",
            expiration=date(2026, 7, 17),
            am_settled=True,
            contracts=80,
            fresh=0,
        ),
    ]
    report = evaluate(rows, expect_monthly=True)
    assert report["ok"] is False
    assert any("monthly chain is not streaming" in f for f in report["failures"])


def test_render_table_renders_empty_state_without_crash():
    assert render_table([]).strip() == "(no rows)"


def test_render_table_includes_header_and_row_fields():
    output = render_table([_row(option_root="SPXW", contracts=80, fresh=80, oi_sum=1234)])
    assert "expiration" in output
    assert "option_root" in output
    assert "SPXW" in output
    assert "80" in output
    assert "1,234" in output
