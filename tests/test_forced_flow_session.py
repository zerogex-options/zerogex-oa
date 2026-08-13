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
    _nearest_crossing,
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


def test_magnet_sits_on_its_columns_sign_change():
    field = session_forced_flow_field(_columns(), GRID, R, Q)
    # The construction is only meaningful if at least one column actually magnets.
    assert any(m is not None for m in field["magnets"])
    for row, magnet in zip(field["z"], field["magnets"]):
        if magnet is None:
            # No magnet <=> the row never crosses zero between adjacent samples.
            assert _nearest_crossing(list(zip(GRID, row)), sum(GRID) / len(GRID)) is None
            continue
        # The magnet must bracket a genuine sign change of THIS row: the grid
        # samples on either side of it differ in sign (or one is exactly zero).
        lo = [p for p in GRID if p <= magnet][-1]
        hi = [p for p in GRID if p >= magnet][0]
        y_lo = row[GRID.index(lo)]
        y_hi = row[GRID.index(hi)]
        assert y_lo == 0.0 or y_hi == 0.0 or y_lo * y_hi <= 0.0


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


def test_min_tte_floor_is_inert_for_longer_dated_book():
    # A 30-minute tte floor is EXACTLY inert on a 30-DTE book (tte >> floor), so
    # the regularization only ever touches the near-expiry columns of a real
    # 0DTE-heavy session -- the multi-day rows are byte-identical with/without it.
    floor = 30.0 / (365.0 * 24.0 * 60.0)
    base = session_forced_flow_field(_columns(), GRID, R, Q)
    floored = session_forced_flow_field(_columns(), GRID, R, Q, min_tte_years=floor)
    assert floored["z"] == base["z"]
    assert floored["magnets"] == base["magnets"]
