"""Tests for the normalizer_cache_healthcheck tool."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.tools.normalizer_cache_healthcheck import _evaluate
from src.tools.normalizer_cache_refresh import FIELD_SPECS

NOW = datetime(2026, 5, 7, 12, 0, tzinfo=timezone.utc)
MAX_AGE = timedelta(hours=36)


def _row(symbol, field, age_hours, sample_size=500, p95=1e9):
    """Build one DB-shaped tuple matching the SELECT order in the SQL."""
    return (symbol, field, NOW - timedelta(hours=age_hours), sample_size, p95)


def test_fresh_row_within_window():
    rows = [_row("SPY", "dealer_vanna_exposure", age_hours=12)]
    expected = [("SPY", "dealer_vanna_exposure")]
    out = _evaluate(rows, expected, MAX_AGE, NOW)
    assert len(out) == 1
    assert out[0].status == "fresh"
    assert 11.5 < out[0].age_hours < 12.5
    assert out[0].sample_size == 500
    assert out[0].p95 == 1e9


def test_stale_row_past_window():
    rows = [_row("SPY", "dealer_vanna_exposure", age_hours=48)]
    expected = [("SPY", "dealer_vanna_exposure")]
    out = _evaluate(rows, expected, MAX_AGE, NOW)
    assert out[0].status == "stale"
    assert 47 < out[0].age_hours < 49


def test_missing_row_yields_missing_status():
    out = _evaluate(rows=[], expected=[("SPY", "dealer_vanna_exposure")], max_age=MAX_AGE, now=NOW)
    assert out[0].status == "missing"
    assert out[0].updated_at is None
    assert out[0].age_hours is None
    assert out[0].sample_size is None
    assert out[0].p95 is None


def test_mixed_states_are_classified_independently():
    rows = [
        _row("SPY", "dealer_vanna_exposure", age_hours=10),
        _row("SPY", "dealer_charm_exposure", age_hours=72),
        # local_gex deliberately absent
    ]
    expected = [
        ("SPY", "dealer_vanna_exposure"),
        ("SPY", "dealer_charm_exposure"),
        ("SPY", "local_gex"),
    ]
    out = _evaluate(rows, expected, MAX_AGE, NOW)
    by_field = {s.field_name: s.status for s in out}
    assert by_field == {
        "dealer_vanna_exposure": "fresh",
        "dealer_charm_exposure": "stale",
        "local_gex": "missing",
    }


def test_boundary_exactly_at_max_age_is_fresh():
    """`age == max_age` should not trip staleness — comparison is `>`, not `>=`."""
    rows = [_row("SPY", "dealer_vanna_exposure", age_hours=36)]
    out = _evaluate(rows, [("SPY", "dealer_vanna_exposure")], MAX_AGE, NOW)
    assert out[0].status == "fresh"


def test_one_microsecond_past_max_age_is_stale():
    rows = [(
        "SPY",
        "dealer_vanna_exposure",
        NOW - MAX_AGE - timedelta(microseconds=1),
        500,
        1e9,
    )]
    out = _evaluate(rows, [("SPY", "dealer_vanna_exposure")], MAX_AGE, NOW)
    assert out[0].status == "stale"


def test_status_preserves_expected_order():
    """Output order must follow ``expected`` so callers can format
    deterministically without resorting."""
    expected = [
        ("SPY", "dealer_vanna_exposure"),
        ("QQQ", "dealer_vanna_exposure"),
        ("SPY", "dealer_charm_exposure"),
    ]
    rows = [_row(s, f, age_hours=1) for s, f in expected]
    out = _evaluate(rows, expected, MAX_AGE, NOW)
    assert [(s.underlying, s.field_name) for s in out] == expected


def test_multi_symbol_cross_product():
    """Healthcheck should evaluate every (symbol × field) pair so a
    silent failure on one field for one symbol doesn't get masked by
    a fresh row on a different (symbol, field)."""
    expected = [(s, f.name) for s in ("SPY", "QQQ") for f in FIELD_SPECS]
    rows = [_row("SPY", f.name, age_hours=1) for f in FIELD_SPECS]
    # QQQ has no rows at all.
    out = _evaluate(rows, expected, MAX_AGE, NOW)
    statuses = {(s.underlying, s.field_name): s.status for s in out}
    for spec in FIELD_SPECS:
        assert statuses[("SPY", spec.name)] == "fresh"
        assert statuses[("QQQ", spec.name)] == "missing"
