"""Coverage for ES / NQ as first-class symbols backed by SPX / NDX options.

ZeroGEX never ingests options on futures.  An ES surface is the SPX surface
with its price-space fields carried across by the index->future carry ratio,
so these tests pin the three things that make that safe:

* the symbol registry resolves both directions (ES<->SPX, NQ<->NDX);
* the ratio is measured off concurrent prints, rejects a bad one, and falls
  back to cost-of-carry rather than to a silent 1.0;
* projection moves LEVELS and leaves DOLLAR EXPOSURES alone — the bug that
  would otherwise invent dealer positioning that nobody holds.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from src.jobs.futures_projection import (
    NEVER_PROJECT,
    PRICE_FIELDS,
    FuturesBasis,
    next_quarterly_expiry,
    project_payload,
    projection_metadata,
    projection_tick,
    resolve_basis,
    theoretical_ratio,
)
from src.symbols import (
    is_futures_symbol,
    resolve_futures_alias,
    resolve_futures_index,
    resolve_futures_tick,
    round_to_tick,
)


class _FakeDB:
    """Stands in for DatabaseManager's basis reader."""

    def __init__(self, rows=None, raises=False):
        self._rows = rows or []
        self._raises = raises
        self.calls: list[dict] = []

    async def get_futures_basis_samples(self, index_symbol, **kwargs):
        self.calls.append({"index_symbol": index_symbol, **kwargs})
        if self._raises:
            raise RuntimeError("db down")
        return self._rows


def _pairs(count=5, *, ratio=1.0067, age_minutes=0, index=6600.0):
    """`count` concurrent print pairs at a known ratio, newest first."""
    now = datetime.now(timezone.utc)
    return [
        {
            "observed_at": now - timedelta(minutes=age_minutes + i),
            "future_symbol": "@ES",
            "index_close": index + i,
            "future_close": (index + i) * ratio,
        }
        for i in range(count)
    ]


# --- symbol registry -------------------------------------------------------


def test_futures_registry_resolves_both_directions():
    assert resolve_futures_index("ES") == "SPX"
    assert resolve_futures_index("nq") == "NDX"
    assert resolve_futures_alias("SPX") == "ES"
    assert resolve_futures_alias("NDX") == "NQ"


def test_futures_registry_rejects_non_futures():
    # ETFs and the cash indices themselves are not first-class futures.
    for symbol in ("SPY", "QQQ", "SPX", "NDX", "", "@ES"):
        assert not is_futures_symbol(symbol), symbol
    assert resolve_futures_index("SPY") is None
    assert resolve_futures_alias("SPY") is None


def test_projected_levels_round_to_the_contract_tick():
    assert resolve_futures_tick("ES") == 0.25
    assert round_to_tick(6644.31, 0.25) == 6644.25
    assert round_to_tick(6644.40, 0.25) == 6644.50
    # An unknown tick must pass the value through rather than crash.
    assert round_to_tick(6644.31, None) == 6644.31


# --- basis measurement -----------------------------------------------------


def test_basis_is_measured_from_concurrent_prints():
    basis = asyncio.run(resolve_basis(_FakeDB(_pairs(ratio=1.0067)), "ES"))
    assert basis.source == "measured"
    assert basis.ratio == pytest.approx(1.0067, rel=1e-6)
    assert basis.index_symbol == "SPX" and basis.futures_symbol == "ES"
    assert basis.sample_count == 5


def test_basis_accepts_either_side_of_the_pair():
    rows = _pairs()
    from_future = asyncio.run(resolve_basis(_FakeDB(rows), "ES"))
    from_index = asyncio.run(resolve_basis(_FakeDB(rows), "SPX"))
    assert from_future.ratio == from_index.ratio
    assert from_index.futures_symbol == "ES"


def test_absurd_ratio_is_rejected_and_median_survives_it():
    """A back-adjusted or wrong-symbol series must not set the axis."""
    rows = _pairs(ratio=1.0067)
    rows.insert(
        2,
        {
            "observed_at": rows[0]["observed_at"],
            "future_symbol": "@ES",
            "index_close": 6600.0,
            "future_close": 66000.0,
        },  # 10x — nonsense
    )
    basis = asyncio.run(resolve_basis(_FakeDB(rows), "ES"))
    assert basis.ratio == pytest.approx(1.0067, rel=1e-6)
    assert basis.sample_count == 5  # the poisoned pair was dropped


def test_stale_measurement_is_flagged_but_still_used():
    """Overnight there is no fresh pair; the last good one still projects."""
    basis = asyncio.run(resolve_basis(_FakeDB(_pairs(age_minutes=14 * 60)), "ES"))
    assert basis.source == "measured_stale"
    assert basis.ratio == pytest.approx(1.0067, rel=1e-6)


def test_empty_feed_falls_back_to_carry_never_to_one():
    """A ratio of 1.0 would publish cash levels on a futures chart."""
    basis = asyncio.run(resolve_basis(_FakeDB([]), "ES"))
    assert basis.source == "carry"
    assert basis.ratio != 1.0
    assert basis.ratio == pytest.approx(theoretical_ratio("SPX"), rel=1e-9)


def test_dead_db_degrades_to_carry_rather_than_raising():
    basis = asyncio.run(resolve_basis(_FakeDB(raises=True), "NQ"))
    assert basis.source == "carry"
    assert basis.futures_symbol == "NQ" and basis.index_symbol == "NDX"


def test_non_projectable_symbol_returns_none():
    assert asyncio.run(resolve_basis(_FakeDB([]), "SPY")) is None
    assert asyncio.run(resolve_basis(_FakeDB([]), "")) is None


# --- as-of (historical) basis ---------------------------------------------
#
# Basis walks down through each quarterly cycle toward expiry, so projecting a
# past frame with TODAY's ratio offsets every level by however much it has
# moved since — invisible on a chart, corrupting in a backtest. These pin that
# a historical caller's timestamp actually reaches the sample read.


def test_asof_is_passed_through_to_the_sample_read():
    """The whole point: a historical read must not sample today's tape."""
    at = datetime(2026, 5, 14, 18, 30, tzinfo=timezone.utc)
    db = _FakeDB(_pairs())
    asyncio.run(resolve_basis(db, "ES", at=at))
    assert db.calls[0]["at"] == at


def test_live_read_leaves_the_anchor_unset():
    """None keeps the live path on NOW(), and on its own cache entry."""
    db = _FakeDB(_pairs())
    asyncio.run(resolve_basis(db, "ES"))
    assert db.calls[0]["at"] is None


def test_asof_measures_staleness_against_the_anchor_not_wall_clock():
    """Prints beside a past anchor are fresh FOR it, however old they are now."""
    at = datetime.now(timezone.utc) - timedelta(days=90)
    rows = [
        {
            "observed_at": at - timedelta(minutes=i),
            "future_symbol": "@ES",
            "index_close": 6600.0 + i,
            "future_close": (6600.0 + i) * 1.0067,
        }
        for i in range(5)
    ]
    basis = asyncio.run(resolve_basis(_FakeDB(rows), "ES", at=at))
    assert basis.source == "measured"
    assert basis.ratio == pytest.approx(1.0067, rel=1e-6)


def test_asof_carry_fallback_prices_to_the_expiry_in_force_then():
    """With no prints, the fallback must not use today's quarterly either."""
    at = datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc)
    basis = asyncio.run(resolve_basis(_FakeDB([]), "ES", at=at))
    assert basis.source == "carry"
    assert basis.ratio == pytest.approx(theoretical_ratio("SPX", at), rel=1e-9)
    # The March expiry, not whichever quarter today happens to sit in.
    assert next_quarterly_expiry(at).month == 3


def test_next_quarterly_expiry_is_a_third_friday():
    expiry = next_quarterly_expiry(datetime(2026, 8, 22, tzinfo=timezone.utc))
    assert (expiry.year, expiry.month, expiry.day) == (2026, 9, 18)
    assert expiry.weekday() == 4
    # On expiry day itself the front contract is still the front contract.
    assert next_quarterly_expiry(datetime(2026, 9, 18, tzinfo=timezone.utc)) == expiry
    # The day after, it rolls to December.
    assert next_quarterly_expiry(datetime(2026, 9, 19, tzinfo=timezone.utc)).month == 12


# --- projection semantics --------------------------------------------------


def _basis(ratio=1.0067):
    return FuturesBasis(
        index_symbol="SPX",
        futures_symbol="ES",
        ratio=ratio,
        source="measured",
        observed_at=datetime.now(timezone.utc),
        sample_count=5,
        feed_symbol="@ES",
    )


def test_levels_move_to_the_futures_axis():
    basis = _basis()
    out = project_payload(
        {"call_wall": 6700.0, "put_wall": 6500.0, "gamma_flip": 6580.0, "max_pain": 6620.0},
        basis,
        tick=projection_tick("ES"),
    )
    assert out["call_wall"] == 6745.0
    assert out["put_wall"] == 6543.5
    # Every projected level lands on a tradable ES tick.
    for value in out.values():
        assert round(value / 0.25) * 0.25 == pytest.approx(value)


def test_dollar_exposures_are_never_rescaled():
    """The load-bearing rule: exposure belongs to the book, not the axis."""
    payload = {
        "net_gex": 1.23e9,
        "total_net_gex": -4.4e8,
        "call_wall_strength": 9.9e7,
        "put_call_ratio": 0.92,
        "pin_confidence": 0.7,
        "open_interest": 1200,
        "volume": 55_000,
        "implied_volatility": 0.184,
    }
    out = project_payload(dict(payload), _basis(), tick=projection_tick("ES"))
    assert out == payload


def test_nested_strike_ladders_are_projected_in_place():
    out = project_payload(
        {"strikes": [{"strike": 6650.0, "net_gex": 4.4e8}, {"strike": 6700.0, "net_gex": -1e8}]},
        _basis(),
        tick=projection_tick("ES"),
    )
    assert [s["strike"] for s in out["strikes"]] == [6694.5, 6745.0]
    assert [s["net_gex"] for s in out["strikes"]] == [4.4e8, -1e8]


def test_price_and_never_project_lists_do_not_overlap():
    """A field in both lists would silently resolve one way and confuse review."""
    assert PRICE_FIELDS.isdisjoint(NEVER_PROJECT)


def test_projection_round_trips():
    basis = _basis()
    assert basis.unproject(basis.project(6700.0)) == pytest.approx(6700.0)


def test_projection_metadata_discloses_the_derivation():
    meta = projection_metadata(_basis())
    assert meta["symbol"] == "ES"
    assert meta["derived_from"] == "SPX"
    assert meta["basis_source"] == "measured"
    assert meta["basis_ratio"] == pytest.approx(1.0067)


def test_booleans_are_not_treated_as_prices():
    """bool is an int subclass — projecting one would emit 1.0067 for True."""
    out = project_payload({"close": True}, _basis(), tick=None)
    assert out["close"] is True
