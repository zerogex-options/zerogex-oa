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
from datetime import date, datetime, timedelta, timezone
from math import exp

import pytest

from src.config import RISK_FREE_RATE, resolve_dividend_yield

from src.jobs.futures_projection import (
    NEVER_PROJECT,
    PRICE_FIELDS,
    FuturesBasis,
    active_contract_code,
    active_contract_expiry,
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


@pytest.fixture(autouse=True)
def _measured_path_enabled(monkeypatch):
    """These tests are about the MEASURED basis, so turn the gate off.

    Production now defaults FUTURES_BASIS_CARRY_ONLY on: the measured
    basis is a median of CME print pairs and therefore a derived work from
    CME's data, which we hold no licence for and which ThetaData does not
    sell. The measured code is not deleted -- it is correct, and it comes
    back the day there is a licence or a licensed redistributor behind it
    -- so it keeps its tests. It just no longer runs by default, and a
    test that silently exercised the carry path while claiming to measure
    print pairs would be worse than no test.
    """
    monkeypatch.setattr(
        "src.jobs.futures_projection.FUTURES_BASIS_CARRY_ONLY", False
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


def test_wall_ladder_strikes_move_with_the_primary_wall():
    """C2/C3 must land on the same axis as the Call Wall they sit beside.

    The ladder is a list of nested objects, so it only projects because the
    walker recurses into lists of dicts and ``strike`` is an allowlisted price
    field.  If that ever regressed, an ES chart would draw ``C1`` on the
    futures axis and ``C2`` on the cash-index axis — the two-incompatible-axes
    failure this module exists to prevent, and harder to spot because both
    numbers look plausible.
    """
    basis = _basis()
    out = project_payload(
        {
            "call_wall": 6700.0,
            "put_wall": 6500.0,
            "call_walls": [
                {"rank": 1, "label": "C1", "strike": 6700.0, "strength": 1.2e9},
                {"rank": 2, "label": "C2", "strike": 6750.0, "strength": 8.0e8},
            ],
            "put_walls": [{"rank": 1, "label": "P1", "strike": 6500.0, "strength": 9.0e8}],
        },
        basis,
    )

    # Rank 1 stays exactly the primary wall after projection, on both sides.
    assert out["call_walls"][0]["strike"] == out["call_wall"]
    assert out["put_walls"][0]["strike"] == out["put_wall"]
    # Deeper ranks move by the same basis.
    assert out["call_walls"][1]["strike"] == basis.project(6750.0)
    # Rank, label and the dollar magnitude are not price space — untouched.
    assert out["call_walls"][1]["rank"] == 2
    assert out["call_walls"][1]["label"] == "C2"
    assert out["call_walls"][1]["strength"] == 8.0e8


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


# ---------------------------------------------------------------------------
# Active contract naming (the badge / tooltip label)
# ---------------------------------------------------------------------------
#
# The roll offset is MEASURED, not assumed. On the Sep 2026 cycle (expiry Fri
# Sep 18) the production @NQ basis against NDX sat at 8.5bp through Thu Sep 10
# and stepped to 108.2bp on Fri Sep 11; @ES stepped 8.1 -> 93.7bp the same day.
# So the continuous feed was on U26 through the 10th and Z26 from the 11th.


@pytest.mark.parametrize(
    "day, expected",
    [
        ("2026-09-09", "NQU26"),  # well before the roll
        ("2026-09-10", "NQU26"),  # last day on the expiring contract
        ("2026-09-11", "NQZ26"),  # the observed roll
        ("2026-09-14", "NQZ26"),  # the day the mismatch was reported
        ("2026-09-18", "NQZ26"),  # Sep expiry itself — already long gone
        ("2026-12-10", "NQZ26"),  # last day before the next roll
        ("2026-12-11", "NQH27"),  # rolls into the new year
    ],
)
def test_active_contract_code_tracks_the_observed_roll(day, expected):
    at = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    assert active_contract_code("@NQ", at) == expected


def test_active_contract_code_strips_the_continuous_prefix():
    at = datetime(2026, 9, 14, tzinfo=timezone.utc)
    assert active_contract_code("@ES", at) == "ESZ26"
    assert active_contract_code("ES", at) == "ESZ26"


def test_active_contract_code_is_none_without_a_future():
    assert active_contract_code(None) is None
    assert active_contract_code("") is None
    assert active_contract_code("@") is None


def test_active_contract_differs_from_next_quarterly_inside_the_roll_week():
    """The two helpers disagree for exactly the week that caused the reports.

    ``next_quarterly_expiry`` answers "next expiry on or after today", which
    inside the roll window names the contract the feed has ALREADY LEFT.
    Labelling a December quote September is the bug this guards.
    """
    at = datetime(2026, 9, 14, tzinfo=timezone.utc)
    assert next_quarterly_expiry(at) == date(2026, 9, 18)
    assert active_contract_expiry(at) == date(2026, 12, 18)


def test_active_contract_expiry_is_always_a_third_friday():
    for month_day in ("2026-01-05", "2026-03-16", "2026-06-30", "2026-09-14", "2026-12-31"):
        at = datetime.fromisoformat(month_day).replace(tzinfo=timezone.utc)
        expiry = active_contract_expiry(at)
        assert expiry.weekday() == 4
        assert 15 <= expiry.day <= 21
        assert expiry.month in (3, 6, 9, 12)


def test_active_contract_expiry_roll_days_is_overridable():
    """CME publishes 8 days; TradeStation was observed at 7. Both reachable."""
    at = datetime(2026, 9, 10, tzinfo=timezone.utc)
    assert active_contract_expiry(at, roll_days=7) == date(2026, 9, 18)
    assert active_contract_expiry(at, roll_days=8) == date(2026, 12, 18)


def test_carry_fallback_prices_to_the_active_contract_through_the_roll():
    """The fallback must not collapse to ~1.0 during the roll week.

    Between the roll and the old contract's expiry the nearest quarterly is
    the contract the feed has ALREADY LEFT. Pricing a full quarter of carry
    over the few days to it makes the ratio nearly 1.0 — which publishes cash
    levels on a futures axis, the one outcome resolve_basis refuses to reach
    by falling back rather than defaulting to 1.0.

    This is the regime the fallback is actually for: overnight the cash index
    is frozen, so there is no concurrent pair and the measured path has
    nothing to read.
    """
    in_roll_week = datetime(2026, 9, 14, tzinfo=timezone.utc)
    ratio = theoretical_ratio("NDX", in_roll_week)

    # Sanity: the two expiries genuinely disagree on this date.
    assert next_quarterly_expiry(in_roll_week) == date(2026, 9, 18)
    assert active_contract_expiry(in_roll_week) == date(2026, 12, 18)

    # Priced to Dec, a quarter of carry is worth ~1% -- comfortably clear of
    # the ~0.05% that pricing to the four-day-away Sep expiry would give.
    assert ratio > 1.005, "carry fallback collapsed toward 1.0 inside the roll week"

    days_to_sep = (date(2026, 9, 18) - in_roll_week.date()).days
    wrong = exp((RISK_FREE_RATE - resolve_dividend_yield("NDX")) * (days_to_sep / 365.0))
    assert ratio > wrong * 1.005


def test_carry_fallback_is_continuous_across_the_roll():
    """No cliff in the published ratio on the day the contract switches.

    Before the fix the ratio decayed toward 1.0 into expiry and then jumped
    when next_quarterly_expiry finally moved on. Pricing to the active
    contract throughout means the step lands on the roll -- where the feed's
    own basis steps too -- and not a week later.
    """
    day_before = theoretical_ratio("NDX", datetime(2026, 9, 10, tzinfo=timezone.utc))
    day_of = theoretical_ratio("NDX", datetime(2026, 9, 11, tzinfo=timezone.utc))
    week_after = theoretical_ratio("NDX", datetime(2026, 9, 21, tzinfo=timezone.utc))

    # The roll is the only step: Sep 10 still prices to the expiring contract.
    assert day_before < 1.002
    assert day_of > 1.005
    # And it decays smoothly from there, with no second jump at Sep expiry.
    assert day_of > week_after > 1.005


# ---------------------------------------------------------------------------
# /api/gex/pin-stability — the pin under its other names
# ---------------------------------------------------------------------------


def _nq_basis():
    return FuturesBasis(
        futures_symbol="NQ",
        index_symbol="NDX",
        ratio=1.01047,  # measured on 2026-09-14
        source="measured",
        observed_at=datetime(2026, 9, 14, tzinfo=timezone.utc),
        sample_count=5,
        feed_symbol="@NQ",
    )


def test_pin_stability_levels_project_like_the_pin_they_are():
    """Renaming the pin must not smuggle a cash strike onto the futures axis.

    /api/gex/pin-stability sits under the projectable /api/gex/ prefix and
    reports the pin three times under its own names. The allowlist matches on
    KEY, so before those names were listed an NQ request returned raw NDX
    strikes next to a correctly projected `pin_strike` in the same payload —
    about 400 points below the axis they are drawn on.
    """
    payload = {
        "pin_strike": 28900.0,
        "current_pin": 28900.0,
        "held_pin": 28850.0,
        "session_open_pin": 28800.0,
    }
    out = project_payload(payload, _nq_basis())

    # The decisive property: one value, one answer, whatever it is called.
    assert out["current_pin"] == out["pin_strike"]
    for field in ("current_pin", "held_pin", "session_open_pin"):
        assert out[field] > payload[field], f"{field} was served on the cash axis"


def test_pin_migration_projects_as_a_price_delta():
    """net_migration is held_pin - session_open_pin, so it scales with them."""
    payload = {"session_open_pin": 28800.0, "held_pin": 28850.0, "net_migration": 50.0}
    out = project_payload(payload, _nq_basis())
    assert out["net_migration"] == pytest.approx(
        out["held_pin"] - out["session_open_pin"], rel=1e-9
    )


def test_pin_stability_counters_are_never_projected():
    """Minutes observed and strikes occupied are counts, not prices."""
    payload = {
        "current_samples": 42,
        "held_samples": 37,
        "quiet_samples": 5,
        "total_samples": 390,
        "distinct_values": 3,
    }
    out = project_payload(payload, _nq_basis())
    assert out == payload

# The default path: carry only, and no CME data read at all
# ---------------------------------------------------------------------------
class _ExplodingDB:
    """Any read of CME print pairs is a licensing failure, so make it loud."""

    async def get_futures_basis_samples(self, *a, **kw):
        raise AssertionError(
            "carry-only must not read futures basis samples: those are CME "
            "print pairs, and deriving from them is the exposure this flag exists "
            "to remove"
        )


def test_carry_only_never_reads_a_cme_print(monkeypatch):
    monkeypatch.setattr("src.jobs.futures_projection.FUTURES_BASIS_CARRY_ONLY", True)
    basis = asyncio.run(resolve_basis(_ExplodingDB(), "ES"))
    assert basis is not None
    assert basis.source == "carry"
    assert basis.sample_count == 0
    assert basis.observed_at is None


def test_carry_only_still_prices_from_the_index(monkeypatch):
    """A basis of 1.0 would publish cash levels on a futures chart, which
    is the one failure this function has always refused to produce."""
    monkeypatch.setattr("src.jobs.futures_projection.FUTURES_BASIS_CARRY_ONLY", True)
    basis = asyncio.run(resolve_basis(_ExplodingDB(), "ES"))
    assert basis.ratio == pytest.approx(theoretical_ratio("SPX", None))
    assert basis.ratio != 1.0


def test_carry_only_still_declines_a_pair_it_cannot_project(monkeypatch):
    """The gate must not turn "not a projectable symbol" into a projection."""
    monkeypatch.setattr("src.jobs.futures_projection.FUTURES_BASIS_CARRY_ONLY", True)
    assert asyncio.run(resolve_basis(_ExplodingDB(), "NOT_A_PAIR")) is None


def test_production_default_is_carry_only():
    """What actually ships. The autouse fixture turns the gate off for the
    measured tests above; this asserts the value a deployment gets when it
    sets nothing, because that is the one that decides whether we read CME
    data in production."""
    from src.config import FUTURES_BASIS_CARRY_ONLY as configured

    assert configured is True


def test_the_gate_is_consulted_not_hardcoded(monkeypatch):
    """Both directions, because only both directions prove it is read.

    Asserting carry behaviour with the flag forced True passes just as
    happily against a branch someone hard-coded, which is exactly the
    mutant that survived the first time this was tested.
    """
    reads = []

    class _CountingDB:
        async def get_futures_basis_samples(self, *a, **kw):
            reads.append(1)
            return []

    monkeypatch.setattr("src.jobs.futures_projection.FUTURES_BASIS_CARRY_ONLY", True)
    on = asyncio.run(resolve_basis(_CountingDB(), "ES"))
    assert reads == [], "carry-only read CME print pairs"
    assert on.source == "carry"

    monkeypatch.setattr("src.jobs.futures_projection.FUTURES_BASIS_CARRY_ONLY", False)
    asyncio.run(resolve_basis(_CountingDB(), "ES"))
    assert reads == [1], "the measured path did not read its samples"
