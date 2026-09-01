"""Tests for the ranked Call/Put Wall ladder (C1/C2/C3 · P1/P2/P3).

The ladder lives at :func:`src.analytics.walls.compute_wall_ladder` and is
the single ranking implementation the whole module shares, so the property
these tests care about most is that **rank 1 is always the canonical wall**:
``/api/gex/summary``, the strike-profile buckets and every chart that draws
``C1`` beside a "Call Wall" line must never disagree.
"""

from __future__ import annotations

from src.analytics.walls import (
    DEFAULT_WALL_LADDER_DEPTH,
    MAX_WALL_LADDER_DEPTH,
    align_wall_ladder,
    compute_call_put_walls,
    compute_call_put_walls_with_strength,
    compute_wall_ladder,
    wall_label,
)


def _row(strike: float, call_gamma: float = 0.0, put_gamma: float = 0.0) -> dict:
    return {
        "strike": strike,
        "call_gamma": call_gamma,
        "put_gamma": put_gamma,
        # Full gex_by_strike rows are commonly passed straight in.
        "net_gex": 0.0,
        "call_oi": 0,
        "put_oi": 0,
    }


def _strikes(levels: list[dict]) -> list[float]:
    return [level["strike"] for level in levels]


def _labels(levels: list[dict]) -> list[str]:
    return [level["label"] for level in levels]


# ── Ranking ────────────────────────────────────────────────────────────────


def test_ranks_call_side_by_gamma_descending_above_spot():
    spot = 100.0
    rows = [
        _row(105.0, call_gamma=30.0),
        _row(110.0, call_gamma=10.0),
        _row(115.0, call_gamma=20.0),
        # Below spot — ineligible for a call wall at any rank.
        _row(95.0, call_gamma=999.0),
    ]
    call_walls, _ = compute_wall_ladder(rows, spot)
    assert _strikes(call_walls) == [105.0, 115.0, 110.0]
    assert _labels(call_walls) == ["C1", "C2", "C3"]
    assert [w["rank"] for w in call_walls] == [1, 2, 3]


def test_ranks_put_side_by_gamma_descending_below_spot():
    spot = 100.0
    rows = [
        _row(95.0, put_gamma=30.0),
        _row(90.0, put_gamma=10.0),
        _row(85.0, put_gamma=20.0),
        # Above spot — ineligible for a put wall at any rank.
        _row(105.0, put_gamma=999.0),
    ]
    _, put_walls = compute_wall_ladder(rows, spot)
    assert _strikes(put_walls) == [95.0, 85.0, 90.0]
    assert _labels(put_walls) == ["P1", "P2", "P3"]


def test_tiebreaker_extends_the_primary_rule_across_every_rank():
    """Equal gamma ⇒ nearest-to-spot first, all the way down the ladder."""
    spot = 100.0
    call_rows = [
        _row(120.0, call_gamma=5.0),
        _row(105.0, call_gamma=5.0),
        _row(110.0, call_gamma=5.0),
    ]
    put_rows = [_row(80.0, put_gamma=5.0), _row(95.0, put_gamma=5.0), _row(90.0, put_gamma=5.0)]
    call_walls, _ = compute_wall_ladder(call_rows, spot)
    _, put_walls = compute_wall_ladder(put_rows, spot)
    # Calls: ascending from spot.  Puts: descending from spot.
    assert _strikes(call_walls) == [105.0, 110.0, 120.0]
    assert _strikes(put_walls) == [95.0, 90.0, 80.0]


def test_adjacent_strikes_may_be_consecutive_ranks():
    """No minimum-separation rule: rank is pure magnitude order.

    A spacing heuristic would make the ladder disagree with the per-strike
    bars the charts draw right beside it.
    """
    spot = 100.0
    rows = [_row(101.0, call_gamma=30.0), _row(102.0, call_gamma=29.0), _row(150.0, call_gamma=1.0)]
    call_walls, _ = compute_wall_ladder(rows, spot)
    assert _strikes(call_walls) == [101.0, 102.0, 150.0]


def test_at_spot_strike_is_eligible_on_both_sides():
    spot = 100.0
    rows = [
        _row(100.0, call_gamma=10.0, put_gamma=10.0),
        _row(105.0, call_gamma=1.0),
        _row(95.0, put_gamma=1.0),
    ]
    call_walls, put_walls = compute_wall_ladder(rows, spot)
    assert _strikes(call_walls) == [100.0, 105.0]
    assert _strikes(put_walls) == [100.0, 95.0]


def test_zero_and_negative_gamma_strikes_are_never_walls():
    spot = 100.0
    rows = [_row(105.0, call_gamma=0.0), _row(110.0, call_gamma=-5.0), _row(115.0, call_gamma=3.0)]
    call_walls, _ = compute_wall_ladder(rows, spot)
    assert _strikes(call_walls) == [115.0]


def test_aggregates_per_strike_across_expirations_before_ranking():
    """gex_by_strike is keyed (strike, expiration); ranking must sum first."""
    spot = 100.0
    rows = [
        # 105 wins on the SUM (6+6=12) though no single row beats 110's 10.
        _row(105.0, call_gamma=6.0),
        _row(105.0, call_gamma=6.0),
        _row(110.0, call_gamma=10.0),
        _row(115.0, call_gamma=1.0),
    ]
    call_walls, _ = compute_wall_ladder(rows, spot)
    assert _strikes(call_walls) == [105.0, 110.0, 115.0]


# ── Rank 1 is the canonical wall ────────────────────────────────────────────


def test_rank_one_matches_the_canonical_scalar_walls():
    spot = 100.0
    rows = [
        _row(90.0, call_gamma=1.0, put_gamma=9.0),
        _row(95.0, call_gamma=2.0, put_gamma=7.0),
        _row(105.0, call_gamma=30.0, put_gamma=1.0),
        _row(110.0, call_gamma=15.0, put_gamma=1.0),
        _row(115.0, call_gamma=20.0, put_gamma=0.0),
    ]
    call_wall, put_wall = compute_call_put_walls(rows, spot)
    call_walls, put_walls = compute_wall_ladder(rows, spot)
    assert call_walls[0]["strike"] == call_wall
    assert put_walls[0]["strike"] == put_wall


def test_rank_one_strength_matches_the_canonical_strength_helper():
    spot = 100.0
    rows = [_row(105.0, call_gamma=30.0), _row(95.0, put_gamma=9.0), _row(115.0, call_gamma=20.0)]
    cw, pw, cw_strength, pw_strength = compute_call_put_walls_with_strength(rows, spot)
    call_walls, put_walls = compute_wall_ladder(rows, spot)
    assert (call_walls[0]["strike"], call_walls[0]["strength"]) == (cw, cw_strength)
    assert (put_walls[0]["strike"], put_walls[0]["strength"]) == (pw, pw_strength)


def test_strength_uses_the_canonical_dollar_scale():
    spot = 100.0
    rows = [_row(105.0, call_gamma=30.0)]
    call_walls, _ = compute_wall_ladder(rows, spot)
    # γ × 100 × S² × 0.01
    assert call_walls[0]["strength"] == 30.0 * 100.0 * spot * spot * 0.01


# ── Depth ──────────────────────────────────────────────────────────────────


def test_depth_limits_the_ladder():
    spot = 100.0
    rows = [_row(100.0 + i, call_gamma=float(10 - i)) for i in range(1, 6)]
    assert len(compute_wall_ladder(rows, spot, depth=1)[0]) == 1
    assert len(compute_wall_ladder(rows, spot, depth=2)[0]) == 2
    assert len(compute_wall_ladder(rows, spot)[0]) == DEFAULT_WALL_LADDER_DEPTH


def test_depth_is_clamped_to_the_supported_range():
    spot = 100.0
    rows = [_row(100.0 + i, call_gamma=float(20 - i)) for i in range(1, 12)]
    assert len(compute_wall_ladder(rows, spot, depth=99)[0]) == MAX_WALL_LADDER_DEPTH
    assert compute_wall_ladder(rows, spot, depth=0) == ([], [])
    assert compute_wall_ladder(rows, spot, depth=-3) == ([], [])


def test_ladder_is_short_when_the_chain_has_fewer_eligible_strikes():
    """Never padded — a short list means the book has no further wall."""
    spot = 100.0
    rows = [_row(105.0, call_gamma=5.0), _row(95.0, put_gamma=5.0)]
    call_walls, put_walls = compute_wall_ladder(rows, spot, depth=3)
    assert len(call_walls) == 1
    assert len(put_walls) == 1


# ── Degenerate inputs ──────────────────────────────────────────────────────


def test_empty_sides_return_empty_lists():
    # Nothing above spot ⇒ no call walls; nothing below ⇒ no put walls.
    call_walls, _ = compute_wall_ladder([_row(95.0, put_gamma=5.0)], 100.0)
    _, put_walls = compute_wall_ladder([_row(105.0, call_gamma=5.0)], 100.0)
    assert call_walls == []
    assert put_walls == []


def test_invalid_spot_and_empty_input():
    rows = [_row(105.0, call_gamma=5.0)]
    assert compute_wall_ladder(rows, 0.0) == ([], [])
    assert compute_wall_ladder(rows, -1.0) == ([], [])
    assert compute_wall_ladder(rows, None) == ([], [])  # type: ignore[arg-type]
    assert compute_wall_ladder([], 100.0) == ([], [])


def test_skips_rows_with_missing_or_unparseable_strike():
    rows = [
        {"call_gamma": 99.0},  # no strike
        {"strike": None, "call_gamma": 99.0},
        {"strike": "not-a-number", "call_gamma": 99.0},
        _row(105.0, call_gamma=5.0),
    ]
    call_walls, _ = compute_wall_ladder(rows, 100.0)
    assert _strikes(call_walls) == [105.0]


def test_none_gamma_fields_are_treated_as_zero():
    rows = [{"strike": 105.0, "call_gamma": None, "put_gamma": None}, _row(110.0, call_gamma=2.0)]
    call_walls, _ = compute_wall_ladder(rows, 100.0)
    assert _strikes(call_walls) == [110.0]


# ── Labels ─────────────────────────────────────────────────────────────────


def test_wall_label_naming():
    assert wall_label("call", 1) == "C1"
    assert wall_label("call", 3) == "C3"
    assert wall_label("put", 2) == "P2"


# ── align_wall_ladder ──────────────────────────────────────────────────────


def _ladder(spot: float = 100.0) -> list[dict]:
    rows = [
        _row(105.0, call_gamma=30.0),
        _row(115.0, call_gamma=20.0),
        _row(110.0, call_gamma=15.0),
    ]
    return compute_wall_ladder(rows, spot)[0]


def test_align_is_a_noop_when_the_primary_already_leads():
    ladder = _ladder()
    aligned = align_wall_ladder(ladder, 105.0, "call")
    assert _strikes(aligned) == [105.0, 115.0, 110.0]
    assert _labels(aligned) == ["C1", "C2", "C3"]
    # Strength survives an unchanged promotion.
    assert aligned[0]["strength"] == ladder[0]["strength"]


def test_align_promotes_a_lower_ranked_primary_and_renumbers():
    aligned = align_wall_ladder(_ladder(), 110.0, "call")
    assert _strikes(aligned) == [110.0, 105.0, 115.0]
    assert _labels(aligned) == ["C1", "C2", "C3"]
    # The promoted strike keeps the strength it was ranked on.
    assert aligned[0]["strength"] == 15.0 * 100.0 * 100.0 * 100.0 * 0.01


def test_align_prepends_a_primary_that_is_not_in_the_recomputed_ladder():
    """Spot moved across the strike since the engine wrote the wall.

    The reported wall still leads, but with no ranked gamma to quote its
    strength is null rather than invented.
    """
    aligned = align_wall_ladder(_ladder(), 99.0, "call")
    assert _strikes(aligned) == [99.0, 105.0, 115.0]
    assert aligned[0]["strength"] is None


def test_align_respects_depth_and_leaves_the_input_untouched():
    ladder = _ladder()
    original = [dict(level) for level in ladder]
    aligned = align_wall_ladder(ladder, 110.0, "call", depth=2)
    assert _strikes(aligned) == [110.0, 105.0]
    assert ladder == original


def test_align_with_no_primary_returns_the_ladder_trimmed():
    ladder = _ladder()
    assert align_wall_ladder(ladder, None, "call", depth=2) == ladder[:2]
    assert align_wall_ladder([], None, "call") == []


def test_align_on_an_empty_ladder_still_surfaces_the_primary():
    aligned = align_wall_ladder([], 105.0, "call")
    assert aligned == [{"rank": 1, "label": "C1", "strike": 105.0, "strength": None}]


# ── /api/gex/summary assembly (DatabaseManager._attach_wall_ladders) ────────


def _summary_row(**overrides) -> dict:
    """A ``get_latest_gex_summary`` row as the SQL hands it back."""
    row = {
        "spot_price": 100.0,
        "call_wall": 105.0,
        "put_wall": 95.0,
        # Parallel rank-ordered arrays, straight from the ladder CTEs.
        "call_wall_ladder_strikes": [105.0, 115.0, 110.0],
        "call_wall_ladder_gammas": [30.0, 20.0, 15.0],
        "put_wall_ladder_strikes": [95.0, 85.0],
        "put_wall_ladder_gammas": [9.0, 4.0],
    }
    row.update(overrides)
    return row


def _attach(row: dict) -> dict:
    from src.api.database import DatabaseManager

    DatabaseManager._attach_wall_ladders(row)
    return row


def test_summary_arrays_become_labelled_wall_levels():
    row = _attach(_summary_row())
    assert _strikes(row["call_walls"]) == [105.0, 115.0, 110.0]
    assert _labels(row["call_walls"]) == ["C1", "C2", "C3"]
    assert _strikes(row["put_walls"]) == [95.0, 85.0]
    assert _labels(row["put_walls"]) == ["P1", "P2"]
    # Gamma → dollar GEX on the canonical × 100 × S² × 0.01 scale, matching
    # what compute_wall_ladder produces for a strike-profile bucket.
    assert row["call_walls"][0]["strength"] == 30.0 * 100.0 * 100.0 * 100.0 * 0.01


def test_summary_drops_the_raw_array_columns():
    """The arrays are an internal transport, not part of the response model."""
    row = _attach(_summary_row())
    for key in (
        "call_wall_ladder_strikes",
        "call_wall_ladder_gammas",
        "put_wall_ladder_strikes",
        "put_wall_ladder_gammas",
    ):
        assert key not in row


def test_summary_rank_one_is_pinned_to_the_reported_wall():
    """The engine-persisted wall wins over the freshly-recomputed ladder.

    ``call_wall`` comes from ``gex_summary`` (the engine's spot); the ladder
    is recomputed against the latest quote.  When spot has since crossed a
    strike the two can disagree — the reported wall must still be C1 or the
    chart would draw "Call Wall" and "C1" at different prices.
    """
    row = _attach(_summary_row(call_wall=110.0, put_wall=85.0))
    assert _strikes(row["call_walls"]) == [110.0, 105.0, 115.0]
    assert _labels(row["call_walls"]) == ["C1", "C2", "C3"]
    assert _strikes(row["put_walls"]) == [85.0, 95.0]


def test_summary_handles_null_walls_and_empty_ladders():
    row = _attach(
        _summary_row(
            call_wall=None,
            put_wall=None,
            call_wall_ladder_strikes=[],
            call_wall_ladder_gammas=[],
            put_wall_ladder_strikes=None,
            put_wall_ladder_gammas=None,
        )
    )
    assert row["call_walls"] == []
    assert row["put_walls"] == []


def test_summary_handles_unusable_spot():
    """No spot ⇒ no dollar scale; strikes still rank, strength reads zero."""
    row = _attach(_summary_row(spot_price=None))
    assert _strikes(row["call_walls"]) == [105.0, 115.0, 110.0]
    assert all(level["strength"] == 0.0 for level in row["call_walls"])
