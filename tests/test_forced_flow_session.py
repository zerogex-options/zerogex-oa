"""Full-session forced-flow FIELD (the actual-history + projection heatmap).

Exercises the pure engine function that turns a list of per-time-slice books
(:class:`SessionColumn`) into the z-grid the ``/session-surface`` endpoint serves:
grid shape and column-alignment of the metadata arrays, per-column magnets
bracketed by the price grid, each magnet sitting on its OWN column's sign change,
and distinct books/spots yielding distinct z-rows. Mirrors the pure-function
style of ``test_forced_flow_engine.py`` -- no DB, no clock.
"""

import math

from src.analytics.forced_flow import (
    ContractLeg,
    SessionColumn,
    _classified_crossings,
    _nearest_of_kind,
    session_column_flow,
    session_forced_flow_field,
)

R = 0.05
Q = 0.0


def leg(K, ot, oi, iv=0.20, dte_days=30):
    return ContractLeg(
        strike=K, option_type=ot, open_interest=oi, iv=iv, tte_years=dte_days / 365.0
    )


# A book whose dealer gamma flips near S=99 (same shape as the engine test's).
BOOK = [
    leg(95, "C", 800),
    leg(100, "C", 1500),
    leg(105, "C", 1200),
    leg(110, "C", 600),
    leg(90, "P", 700),
    leg(95, "P", 1300),
    leg(100, "P", 1400),
    leg(105, "P", 500),
]

# A visibly different book -- heavier, lower put wing and different strikes -- so
# its priced row cannot coincide with BOOK's.
BOOK_ALT = [
    leg(98, "C", 400),
    leg(103, "C", 900),
    leg(92, "P", 1800),
    leg(97, "P", 2200),
    leg(102, "P", 1600),
]

# Small ascending price grid spanning the flip (95.0 .. 105.0, 0.5 apart).
GRID = [95.0 + 0.5 * i for i in range(21)]


def _columns():
    """Three slices: session open (390 min, BOOK) and mid-session (200 min,
    BOOK_ALT) as ACTUAL recorded books, plus a projection to the close (30 min,
    current BOOK) -- distinct legs AND spots so the rows must differ."""
    return [
        SessionColumn(min_to_close=390.0, spot=99.0, is_past=True, legs=BOOK),
        SessionColumn(min_to_close=200.0, spot=100.5, is_past=True, legs=BOOK_ALT),
        SessionColumn(min_to_close=30.0, spot=100.0, is_past=False, legs=BOOK),
    ]


# --------------------------------------------------------------------------- #
# Shape + column alignment
# --------------------------------------------------------------------------- #
def test_field_shape_matches_columns_and_grid():
    cols = _columns()
    field = session_forced_flow_field(cols, GRID, R, Q)
    # The grid is echoed back verbatim; z is one row per column, each of grid len.
    assert field["prices"] == GRID
    assert len(field["z"]) == len(cols)
    for row in field["z"]:
        assert len(row) == len(GRID)
        assert all(math.isfinite(y) for y in row)
    # Every per-column metadata array is column-aligned (same length + order).
    assert field["spots"] == [c.spot for c in cols]
    assert field["min_to_close"] == [c.min_to_close for c in cols]
    assert field["is_past"] == [c.is_past for c in cols]
    assert len(field["magnets"]) == len(cols)


# --------------------------------------------------------------------------- #
# Magnets: bracketed by the grid, and sitting on the column's own sign change
# --------------------------------------------------------------------------- #
def test_every_magnet_is_none_or_inside_the_grid():
    field = session_forced_flow_field(_columns(), GRID, R, Q)
    lo, hi = min(GRID), max(GRID)
    for m in field["magnets"]:
        assert m is None or (lo <= m <= hi)


def test_magnet_sits_on_an_attractor_and_pivot_on_a_repeller():
    field = session_forced_flow_field(_columns(), GRID, R, Q)
    ref = sum(GRID) / len(GRID)
    # The construction is only meaningful if some column actually produced a
    # crossing (a stable magnet or an unstable pivot).
    assert any(m is not None for m in field["magnets"]) or any(
        p is not None for p in field["pivots"]
    )
    for row, magnet, pivot in zip(field["z"], field["magnets"], field["pivots"]):
        pts = list(zip(GRID, row))
        # magnet is None <=> the row has no ATTRACTOR (a repeller may still exist:
        # the short-gamma case this fix separates out).
        if magnet is None:
            assert _nearest_of_kind(pts, ref, "attractor") is None
        else:
            # The magnet brackets a + -> - crossing: buy below, sell above.
            lo = [p for p in GRID if p <= magnet][-1]
            hi = [p for p in GRID if p >= magnet][0]
            assert row[GRID.index(lo)] >= 0.0 >= row[GRID.index(hi)]
        # pivot is None <=> the row has no REPELLER.
        if pivot is None:
            assert _nearest_of_kind(pts, ref, "repeller") is None
        else:
            # The pivot brackets a - -> + crossing: sell below, buy above.
            lo = [p for p in GRID if p <= pivot][-1]
            hi = [p for p in GRID if p >= pivot][0]
            assert row[GRID.index(lo)] <= 0.0 <= row[GRID.index(hi)]


# --------------------------------------------------------------------------- #
# Distinct books/spots -> distinct rows (columns are not sharing one book)
# --------------------------------------------------------------------------- #
def test_distinct_columns_yield_distinct_z_rows():
    field = session_forced_flow_field(_columns(), GRID, R, Q)
    # Col 0 (BOOK @ 99, 390 min) vs col 1 (BOOK_ALT @ 100.5, 200 min): different
    # legs AND spot AND horizon -- the rows must not be identical.
    assert field["z"][0] != field["z"][1]
    # Col 0 vs col 2: same legs, but different spot AND horizon -- still distinct.
    assert field["z"][0] != field["z"][2]


def test_session_column_flow_matches_the_field():
    # session_forced_flow_field delegates to session_column_flow per column (the
    # cacheable primitive the API memoizes for immutable PAST columns), so one
    # column's (row, magnet, pivot) must equal that column's slice of the field.
    cols = _columns()
    field = session_forced_flow_field(cols, GRID, R, Q)
    for i, c in enumerate(cols):
        row, magnet, pivot = session_column_flow(c.spot, c.legs, c.min_to_close, GRID, R, Q)
        assert row == field["z"][i]
        assert magnet == field["magnets"][i]
        assert pivot == field["pivots"][i]


def test_crossings_are_classified_by_stability():
    # y over ascending x: +, -, -, +  -> a + -> - down-crossing (an ATTRACTOR:
    # positive/buy below, negative/sell above -> restoring) then a - -> +
    # up-crossing (a REPELLER: sell below, buy above -> amplifying).
    pts = [(0.0, 2.0), (1.0, -1.0), (2.0, -0.5), (3.0, 1.0)]
    assert [k for _, k in _classified_crossings(pts)] == ["attractor", "repeller"]
    ax = _nearest_of_kind(pts, 0.0, "attractor")
    rx = _nearest_of_kind(pts, 3.0, "repeller")
    assert ax is not None and 0.0 < ax < 1.0
    assert rx is not None and 2.0 < rx < 3.0
    # The magnet (attractor) and pivot (repeller) are distinct crossings.
    assert ax != rx
    # A row that only ever rises through zero has a repeller and NO attractor.
    up_only = [(0.0, -1.0), (1.0, 1.0)]
    assert _nearest_of_kind(up_only, 0.5, "attractor") is None
    assert _nearest_of_kind(up_only, 0.5, "repeller") is not None


def test_every_pivot_is_none_or_inside_the_grid():
    field = session_forced_flow_field(_columns(), GRID, R, Q)
    lo, hi = min(GRID), max(GRID)
    assert len(field["pivots"]) == len(field["magnets"])
    for p in field["pivots"]:
        assert p is None or (lo <= p <= hi)


def test_min_tte_floor_is_inert_for_longer_dated_book():
    # A 30-minute tte floor is EXACTLY inert on a 30-DTE book (tte >> floor), so
    # the regularization only ever touches the near-expiry columns of a real
    # 0DTE-heavy session -- the multi-day rows are byte-identical with/without it.
    floor = 30.0 / (365.0 * 24.0 * 60.0)
    base = session_forced_flow_field(_columns(), GRID, R, Q)
    floored = session_forced_flow_field(_columns(), GRID, R, Q, min_tte_years=floor)
    assert floored["z"] == base["z"]
    assert floored["magnets"] == base["magnets"]
