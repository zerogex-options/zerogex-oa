"""Tests for the v2 envelope checker.

The first version of this check lived as an embedded Python one-liner in a
Makefile recipe. Make strips backslash-newlines and collapses the recipe to
a single line, so ``try:`` landed after a ``;`` and the whole thing was a
SyntaxError — which a ``2>/dev/null`` then swallowed into an empty string.
It reported "empty" for every endpoint on a perfectly healthy API. The
logic had been verified standalone but never through make.

Hence a real module with real tests: the grading is a pure function, so it
can be tested for what it accepts AND what it rejects.
"""

from __future__ import annotations

import pytest

from src.tools import v2_envelope_check as ec

FIELDS = ec.envelope_fields()


def _freshness(**overrides):
    base = {f: None for f in FIELDS}
    base["freshness_status"] = "fresh"
    base["cadence_profile"] = "analytics_cycle"
    base.update(overrides)
    return base


def test_a_complete_envelope_passes():
    ok, detail = ec.check_body({"data": {"a": 1}, "freshness": _freshness()}, fields=FIELDS)
    assert ok
    assert "fresh" in detail and "analytics_cycle" in detail


def test_the_field_list_matches_the_model_exactly():
    """The checker spells the field list out rather than importing the model,
    so that it needs nothing but the stdlib and does not build the whole app
    to check five URLs. This is the guard that keeps the copy honest: add a
    field to Freshness without adding it here and CI goes red."""
    from src.api.freshness import Freshness

    assert ec.envelope_fields() == frozenset(Freshness.model_fields)


# --- what it must REJECT ---------------------------------------------------


def test_a_bare_v1_body_is_rejected():
    """The regression that matters: if the mirror stopped enveloping, every
    endpoint would still answer 200."""
    ok, detail = ec.check_body({"symbol": "SPY", "spot": 676.0}, fields=FIELDS)
    assert not ok
    assert "not enveloped" in detail


def test_a_body_with_extra_top_level_keys_is_rejected():
    """The futures projection nearly attached `projection` beside `data`."""
    ok, detail = ec.check_body(
        {"data": {}, "freshness": _freshness(), "projection": {"ratio": 1.0}}, fields=FIELDS
    )
    assert not ok
    assert "unexpected projection" in detail


def test_a_dropped_freshness_field_is_rejected():
    """v2 promises every declared field is always present so a consumer can
    index the envelope unconditionally."""
    partial = _freshness()
    del partial["stale_after"]
    ok, detail = ec.check_body({"data": {}, "freshness": partial}, fields=FIELDS)
    assert not ok
    assert "stale_after" in detail


def test_a_null_valued_field_is_still_present_and_passes():
    """Null is a value; absent is a contract break. They must not be
    conflated — most fields are legitimately null on a closed market."""
    ok, _ = ec.check_body(
        {"data": {}, "freshness": _freshness(stale_after=None, source_timestamp=None)},
        fields=FIELDS,
    )
    assert ok


@pytest.mark.parametrize(
    "body,fragment",
    [
        ([1, 2, 3], "not an object"),
        ("a string", "not an object"),
        (None, "not an object"),
        ({"data": {}}, "missing freshness"),
        ({"freshness": {}}, "missing data"),
        ({"data": {}, "freshness": "fresh"}, "not an object"),
    ],
)
def test_malformed_bodies_are_rejected_with_a_useful_reason(body, fragment):
    ok, detail = ec.check_body(body, fields=FIELDS)
    assert not ok
    assert fragment in detail


# --- endpoint selection ----------------------------------------------------


def test_default_paths_span_the_shapes_that_can_break():
    """One per failure class: on-demand, the analytics snapshot, a path
    parameter, a JSONResponse route, and a route with no response_model —
    the class where the Decimal/datetime encoder divergence lived."""
    paths = ec.default_paths("2", "SPY")
    assert all(p.startswith("/api/v2/") for p in paths)
    assert any("levels/SPY" in p for p in paths), "path-parameter route not covered"
    assert any("flow/series" in p for p in paths), "JSONResponse route not covered"
    assert any("signals/score" in p for p in paths), "unmodelled route not covered"


def test_version_is_parameterised_not_hardcoded():
    assert all(p.startswith("/api/v3/") for p in ec.default_paths("3", "QQQ"))
    assert any("QQQ" in p for p in ec.default_paths("3", "QQQ"))
