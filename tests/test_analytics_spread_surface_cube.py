"""The surface cube: one set of spread definitions, sliced three ways.

``surface_scopes`` is the only place the Spread Surface view gets its
numbers — the live endpoint narrows a chain through it, and the rollup
writer stores what it returns.  These tests pin the properties that would
make the stored history and the live reading disagree, which is the one
failure a "vs normal" view cannot survive:

* the moneyness band filters CONTRACTS before bucketing, not buckets;
* the DTE universes are cumulative and the DTE buckets are disjoint;
* a cell's population is the same whichever family you reach it through.
"""

from __future__ import annotations

import datetime as dt

from src.analytics import spread_stats as ss


SPOT = 6000.0


def _chain(spot: float = SPOT):
    """A smile: tight at the money, wide in the wings, across four expiries."""
    rows = []
    dte_of = {}
    for dte in (0, 1, 3, 10):
        expiration = dt.date(2026, 9, 16) + dt.timedelta(days=dte)
        for pct in (-9, -7, -4, -2.5, -1, 0, 1, 2.5, 4, 7, 9):
            strike = round(spot * (1 + pct / 100.0), 2)
            width = 0.10 + 0.05 * abs(pct)
            mid = 20.0
            rows.append(
                {
                    "option_symbol": f"T{dte}_{pct}",
                    "strike": strike,
                    "option_type": "P",
                    "expiration": expiration,
                    "bid": mid - width / 2,
                    "ask": mid + width / 2,
                }
            )
        # Keyed by EXPIRATION, as every production caller does — the cube
        # resolves a contract's bucket through ``dte_of[s.expiration]``.
        dte_of[expiration] = dte
    spreads = ss.contract_spreads(rows, spot)
    return spreads, dte_of


def test_band_filters_contracts_before_bucketing():
    """A ±2% cell holds only ±2% contracts, not a truncated ±10% aggregate.

    Filtering buckets instead of contracts is the subtle version of this
    bug: the -3%..-1.5% bucket would survive a ±2% filter carrying its
    -2.5% contracts, and the cell would then describe a wider population
    than the band it is labelled with.
    """
    spreads, dte_of = _chain()
    cells = {
        (c.dte_scope, c.band_pct, c.money_bucket): c
        for c in ss.surface_scopes(spreads, dte_of)
    }

    wide = cells[("u30", 10.0, ss.BAND_WIDE)]
    narrow = cells[("u30", 2.0, ss.BAND_WIDE)]
    assert narrow.aggregate.contract_count < wide.aggregate.contract_count

    # Every contract inside ±2% and nothing outside it: -1, 0, +1 at four
    # expiries.  The -2.5 / +2.5 strikes are outside and must be gone.
    assert narrow.aggregate.contract_count == 12

    key = ss.moneyness_bucket_key(-3.0, -1.5)
    assert ("u30", 10.0, key) in cells
    assert ("u30", 2.0, key) not in cells


def test_universes_are_cumulative_and_dte_buckets_are_disjoint():
    spreads, dte_of = _chain()
    cells = {
        (c.dte_scope, c.band_pct, c.money_bucket): c
        for c in ss.surface_scopes(spreads, dte_of)
    }

    counts = {
        u: cells[(ss.dte_universe_key(u), 10.0, ss.BAND_WIDE)].aggregate.contract_count
        for u in ss.DTE_UNIVERSES
    }
    assert counts[0] < counts[1] < counts[7] < counts[30]

    buckets = {
        key: cells[(key, 10.0, ss.BAND_WIDE)].aggregate.contract_count
        for key, _lo, _hi in ss.DTE_BUCKETS
        if (key, 10.0, ss.BAND_WIDE) in cells
    }
    # Disjoint: the five expiry buckets partition the same population the
    # widest universe covers.  If they overlapped, an expiry ranking would
    # double-count the front month and read tighter than it is.
    assert sum(buckets.values()) == counts[30]


def test_a_cell_is_the_same_population_however_it_is_reached():
    """``u0`` and ``b0`` describe the same contracts, so the same width."""
    spreads, dte_of = _chain()
    cells = {
        (c.dte_scope, c.band_pct, c.money_bucket): c
        for c in ss.surface_scopes(spreads, dte_of)
    }
    universe = cells[("u0", 5.0, ss.BAND_WIDE)].aggregate
    bucket = cells[("b0", 5.0, ss.BAND_WIDE)].aggregate
    assert universe.contract_count == bucket.contract_count
    assert (
        universe.median_relative_spread_pct == bucket.median_relative_spread_pct
    )


def test_the_curve_reproduces_the_smile():
    """Wings wider than the money — the shape the view exists to show."""
    spreads, dte_of = _chain()
    curve = {
        c.money_bucket: c.aggregate.median_relative_spread_pct
        for c in ss.surface_scopes(spreads, dte_of)
        if c.dte_scope == "u0" and c.band_pct == 10.0
        and c.money_bucket != ss.BAND_WIDE
    }
    atm = curve[ss.moneyness_bucket_key(-0.5, 0.5)]
    wing = curve[ss.moneyness_bucket_key(5.0, 10.0)]
    assert wing > atm * 3


def test_a_contract_with_no_bid_is_counted_but_never_widens_the_median():
    """The wings failing is coverage collapse, not a bigger number."""
    spreads, dte_of = _chain()
    rows = [
        {
            "option_symbol": "NOBID",
            "strike": SPOT * 0.93,
            "option_type": "P",
            "expiration": dt.date(2026, 9, 16),
            "bid": 0.0,
            "ask": 4.0,
        }
    ]
    spreads = list(spreads) + list(ss.contract_spreads(rows, SPOT))

    cells = {
        (c.dte_scope, c.band_pct, c.money_bucket): c
        for c in ss.surface_scopes(spreads, dte_of)
    }
    cell = cells[("u0", 10.0, ss.BAND_WIDE)].aggregate
    baseline, base_dte = _chain()
    clean = {
        (c.dte_scope, c.band_pct, c.money_bucket): c
        for c in ss.surface_scopes(baseline, base_dte)
    }[("u0", 10.0, ss.BAND_WIDE)].aggregate

    assert cell.contract_count == clean.contract_count + 1
    assert cell.tradable_count == clean.tradable_count
    assert cell.median_relative_spread_pct == clean.median_relative_spread_pct
    assert cell.zero_bid_pct > 0
