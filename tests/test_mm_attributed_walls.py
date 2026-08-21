"""MM-attributed strike structure: both wall definitions, nodes, concentration.

Definition A must be a pure *input substitution* into ZeroGEX's production
wall helper — same ranking, same tie-breaks, same strength scale.  Definition B
must rank on total MM net gamma at the strike, and must keep the negative-gamma
accelerant strikes under their own name rather than dressing them up as walls.
"""

from __future__ import annotations

import pytest

from src.analytics.walls import compute_call_put_walls_with_strength

from research.mm_attributed_gex.walls import (
    build_strike_structure,
    compare_definitions,
    dominant_nodes,
    gamma_concentration,
    strike_profile,
    walls_definition_a,
    walls_definition_b,
)

SPOT = 6000.0


def row(strike, mm_call_gamma=0.0, mm_put_gamma=0.0, expiration="2026-06-12"):
    """A strike row shaped exactly as ``gex.strike_rows`` produces one."""
    scale = 100.0 * SPOT * SPOT * 0.01
    return {
        "strike": strike,
        "expiration": expiration,
        "mm_call_gamma": mm_call_gamma,
        "mm_put_gamma": mm_put_gamma,
        "call_gamma": mm_call_gamma,
        "put_gamma": -mm_put_gamma,
        "net_gex": (mm_call_gamma + mm_put_gamma) * scale,
    }


# ---------------------------------------------------------------------------
# Definition A
# ---------------------------------------------------------------------------


def test_definition_a_delegates_to_the_production_helper_unchanged():
    """Same inputs through both paths must give byte-identical answers."""
    rows = [
        row(5900.0, mm_call_gamma=1.0, mm_put_gamma=-4.0),
        row(6100.0, mm_call_gamma=5.0, mm_put_gamma=-1.0),
    ]
    walls = walls_definition_a(rows, SPOT)
    expected = compute_call_put_walls_with_strength(rows, SPOT)
    assert (
        walls.call_wall,
        walls.put_wall,
        walls.call_wall_strength,
        walls.put_wall_strength,
    ) == expected


def test_definition_a_picks_the_largest_mm_long_call_gamma_above_spot():
    rows = [
        row(6050.0, mm_call_gamma=2.0),
        row(6100.0, mm_call_gamma=7.0),  # biggest above spot
        row(5950.0, mm_call_gamma=99.0),  # below spot -> ineligible
    ]
    walls = walls_definition_a(rows, SPOT)
    assert walls.call_wall == 6100.0


def test_definition_a_picks_the_largest_mm_short_put_gamma_below_spot():
    """``put_gamma`` is ``−mm_put_gamma``, so most-negative MM put gamma wins."""
    rows = [
        row(5950.0, mm_put_gamma=-2.0),
        row(5900.0, mm_put_gamma=-9.0),  # MM most short puts here
        row(5850.0, mm_put_gamma=+5.0),  # MM long puts -> not a wall
    ]
    walls = walls_definition_a(rows, SPOT)
    assert walls.put_wall == 5900.0


def test_definition_a_ignores_strikes_where_mm_is_net_short_calls():
    """A negative call_gamma cannot be a call wall — the helper requires > 0."""
    rows = [row(6050.0, mm_call_gamma=-10.0), row(6100.0, mm_call_gamma=1.0)]
    assert walls_definition_a(rows, SPOT).call_wall == 6100.0


def test_definition_a_strength_is_on_the_production_dollar_scale():
    rows = [row(6100.0, mm_call_gamma=3.0)]
    walls = walls_definition_a(rows, SPOT)
    assert walls.call_wall_strength == pytest.approx(3.0 * 100.0 * SPOT * SPOT * 0.01)


def test_definition_a_ties_break_nearest_to_spot():
    """Above spot the lower strike wins; below spot the higher one does."""
    rows = [
        row(6050.0, mm_call_gamma=4.0),
        row(6150.0, mm_call_gamma=4.0),
        row(5950.0, mm_put_gamma=-4.0),
        row(5850.0, mm_put_gamma=-4.0),
    ]
    walls = walls_definition_a(rows, SPOT)
    assert walls.call_wall == 6050.0
    assert walls.put_wall == 5950.0


# ---------------------------------------------------------------------------
# Definition B
# ---------------------------------------------------------------------------


def test_definition_b_ranks_on_total_mm_net_gamma_at_the_strike():
    """Calls and puts combine, because the position now carries the sign.

    At 6100 the two legs cancel to +1; at 6150 they reinforce to +4. Definition
    A, splitting on option type, would prefer 6100's larger call leg — so this
    is exactly where the two definitions must diverge.
    """
    rows = [
        row(6100.0, mm_call_gamma=6.0, mm_put_gamma=-5.0),
        row(6150.0, mm_call_gamma=2.0, mm_put_gamma=2.0),
    ]
    b, _up, _down, _us, _ds = walls_definition_b(rows, SPOT)
    assert b.call_wall == 6150.0
    assert walls_definition_a(rows, SPOT).call_wall == 6100.0


def test_definition_b_reports_negative_gamma_strikes_as_accelerants_not_walls():
    rows = [
        row(6100.0, mm_call_gamma=3.0),  # positive -> a wall
        row(6050.0, mm_call_gamma=-8.0),  # negative -> an accelerant
        row(5900.0, mm_put_gamma=-6.0),  # negative below spot
        row(5950.0, mm_put_gamma=+4.0),  # positive below spot -> a wall
    ]
    b, accel_up, accel_down, accel_up_str, accel_down_str = walls_definition_b(rows, SPOT)
    assert b.call_wall == 6100.0
    assert b.put_wall == 5950.0
    assert accel_up == 6050.0
    assert accel_down == 5900.0
    assert accel_up_str > 0 and accel_down_str > 0


def test_definition_b_returns_no_wall_when_every_strike_is_negative():
    rows = [row(6100.0, mm_call_gamma=-3.0), row(5900.0, mm_put_gamma=-3.0)]
    b, accel_up, accel_down, _us, _ds = walls_definition_b(rows, SPOT)
    assert b.call_wall is None and b.put_wall is None
    assert accel_up == 6100.0 and accel_down == 5900.0


def test_definition_b_tie_breaks_nearest_to_spot():
    rows = [
        row(6050.0, mm_call_gamma=5.0),
        row(6150.0, mm_call_gamma=5.0),
        row(5950.0, mm_put_gamma=5.0),
        row(5850.0, mm_put_gamma=5.0),
    ]
    b, _u, _d, _us, _ds = walls_definition_b(rows, SPOT)
    assert b.call_wall == 6050.0
    assert b.put_wall == 5950.0


def test_definitions_agree_when_mm_is_long_calls_above_and_short_puts_below():
    """The positioning the production convention assumes -> the two coincide."""
    rows = [
        row(6100.0, mm_call_gamma=8.0),
        row(5900.0, mm_put_gamma=-7.0),
    ]
    a = walls_definition_a(rows, SPOT)
    b, _u, _d, _us, _ds = walls_definition_b(rows, SPOT)
    assert a.call_wall == b.call_wall == 6100.0
    # Below spot the two definitions ask different questions: A wants MM most
    # SHORT puts, B wants MM net gamma most POSITIVE. A short-put strike is
    # negative gamma, so B correctly declines to call it a wall.
    assert a.put_wall == 5900.0
    assert b.put_wall is None
    call_agree, put_agree = compare_definitions(a, b)
    assert call_agree is True
    assert put_agree is None  # one side absent -> undecidable, not "disagree"


# ---------------------------------------------------------------------------
# Aggregation, nodes, concentration
# ---------------------------------------------------------------------------


def test_rows_are_aggregated_across_expirations_before_ranking():
    """Two expirations at one strike sum, matching the production helper.

    6100 is the biggest single row, but 6050's two expirations sum higher —
    and dealers hedge the union, so 6050 is the wall.
    """
    rows = [
        row(6050.0, mm_call_gamma=3.0, expiration="2026-06-12"),
        row(6050.0, mm_call_gamma=3.0, expiration="2026-06-19"),
        row(6100.0, mm_call_gamma=5.0, expiration="2026-06-12"),
    ]
    b, _u, _d, _us, _ds = walls_definition_b(rows, SPOT)
    assert b.call_wall == 6050.0


def test_dominant_nodes_rank_by_absolute_dollar_gamma_with_side_labels():
    rows = [
        row(6100.0, mm_call_gamma=2.0),
        row(5900.0, mm_put_gamma=-9.0),
        row(6000.0, mm_call_gamma=1.0),
    ]
    nodes = dominant_nodes(rows, SPOT, top_k=3)
    assert [n.strike for n in nodes] == [5900.0, 6100.0, 6000.0]
    assert nodes[0].side == "below"
    assert nodes[2].side == "at"
    assert sum(n.share_of_abs for n in nodes) == pytest.approx(1.0)


def test_gamma_concentration_splits_positive_and_negative_and_measures_hhi():
    rows = [row(6100.0, mm_call_gamma=3.0), row(5900.0, mm_put_gamma=-1.0)]
    conc = gamma_concentration(rows)
    assert conc["positive_share"] == pytest.approx(0.75)
    assert conc["negative_share"] == pytest.approx(0.25)
    assert conc["hhi"] == pytest.approx(0.75**2 + 0.25**2)
    assert conc["n_strikes"] == 2


def test_single_strike_book_is_maximally_concentrated():
    conc = gamma_concentration([row(6100.0, mm_call_gamma=5.0)])
    assert conc["hhi"] == pytest.approx(1.0)


def test_strike_profile_is_ascending_and_aggregated():
    rows = [
        row(6100.0, mm_call_gamma=1.0),
        row(5900.0, mm_put_gamma=2.0),
        row(6100.0, mm_call_gamma=1.0, expiration="2026-06-19"),
    ]
    profile = strike_profile(rows)
    assert [s for s, _v in profile] == [5900.0, 6100.0]
    assert profile[1][1] == pytest.approx(2.0 * 100.0 * SPOT * SPOT * 0.01)


def test_build_strike_structure_assembles_every_reading():
    rows = [
        row(6100.0, mm_call_gamma=4.0),
        row(6050.0, mm_call_gamma=-2.0),
        row(5900.0, mm_put_gamma=-6.0),
        row(5950.0, mm_put_gamma=3.0),
    ]
    struct = build_strike_structure(rows, SPOT, universe="near_term")
    payload = struct.as_dict()
    assert payload["universe"] == "near_term"
    assert payload["mm_attributed_a_call_wall"] == 6100.0
    assert payload["mm_attributed_b_call_wall"] == 6100.0
    assert payload["mm_attributed_a_put_wall"] == 5900.0
    assert payload["mm_attributed_b_put_wall"] == 5950.0
    assert payload["mm_accelerant_down"] == 5900.0
    assert payload["mm_accelerant_up"] == 6050.0
    assert 0.0 < payload["mm_positive_gamma_share"] < 1.0
    assert len(struct.nodes) == 4


def test_empty_or_unusable_inputs_return_an_empty_structure():
    assert build_strike_structure([], SPOT).definition_a.call_wall is None
    assert build_strike_structure([row(6100.0, 1.0)], 0.0).definition_b.call_wall is None
