"""MM-attributed gamma: sign encoding, spot-shift, and zero crossings.

The load-bearing claim of :mod:`research.mm_attributed_gex.gex` is that a
signed MM quantity can be pushed through ZeroGEX's *unmodified* production
gamma kernel by encoding its sign as the option type.  These tests verify that
claim numerically rather than trusting the argument, then exercise the
downstream flip behavior it enables: positive gamma, negative gamma, spot-shift
re-pricing, one crossing, several crossings, and no crossing at all.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.market_calendar import calculate_time_to_expiration, settlement_close_time_for_contract

from research.mm_attributed_gex.gex import (
    CONTRACT_MULTIPLIER,
    ChainQuote,
    ExpirationUniverse,
    MMContract,
    build_engine,
    compute_mm_gex,
    dollar_gex,
    join_positions_to_chain,
    production_profile_rows,
    signed_profile_rows,
    strike_rows,
)
from research.mm_attributed_gex.inventory import MMPosition

SYMBOL = "SPX"
SPOT = 6000.0
TS = datetime(2026, 6, 10, 18, 0, tzinfo=timezone.utc)  # 14:00 ET
NEAR = date(2026, 6, 12)
FAR = date(2026, 7, 17)


@pytest.fixture(scope="module")
def engine():
    return build_engine(SYMBOL)


def _position(strike: float, option_type: str, expiration: date = NEAR, censored: bool = False):
    return MMPosition(
        key=(SYMBOL, expiration, strike, option_type),
        symbol=SYMBOL,
        expiration=expiration,
        strike=strike,
        option_type=option_type,
        option_root="SPXW",
        left_censored=censored,
        left_censor_reason="first_trade_at_window_edge" if censored else "clean",
    )


def _quote(strike: float, option_type: str, expiration: date = NEAR, iv: float = 0.15):
    stamp = expiration.strftime("%y%m%d")
    return ChainQuote(
        option_symbol=f"SPXW {stamp}{option_type}{int(strike):08d}",
        strike=strike,
        expiration=expiration,
        option_type=option_type,
        implied_volatility=iv,
        gamma=None,
        open_interest=1_000,
        volume=100,
    )


def _contract(strike, option_type, net, expiration=NEAR, iv=0.15, censored=False):
    return MMContract(
        position=_position(strike, option_type, expiration, censored),
        quote=_quote(strike, option_type, expiration, iv),
        net_contracts=float(net),
    )


def _manual_dollar_gamma(engine, contracts, spot, ts):
    """Independent recomputation of MM dollar gamma at spot, contract by contract."""
    total = 0.0
    for c in contracts:
        close_t = settlement_close_time_for_contract(SYMBOL, c.quote.option_symbol, c.expiration)
        T = calculate_time_to_expiration(ts, c.expiration, market_close_time=close_t)
        gamma = engine._calculate_bs_gamma(
            spot, c.strike, T, engine.risk_free_rate, c.implied_volatility, engine.dividend_yield
        )
        total += dollar_gex(gamma * c.net_contracts, spot)
    return total


# ---------------------------------------------------------------------------
# Sign encoding
# ---------------------------------------------------------------------------


def test_sign_encoding_maps_long_to_call_and_short_to_put():
    """A negative MM net becomes a synthetic put row carrying |net| contracts."""
    rows = signed_profile_rows(
        [_contract(6000.0, "C", +800), _contract(6050.0, "C", -500), _contract(5950.0, "P", +300)]
    )
    encoded = {(r["strike"], r["option_type"], r["open_interest"]) for r in rows}
    assert encoded == {
        (6000.0, "C", 800.0),  # long call  -> call row
        (6050.0, "P", 500.0),  # short call -> put row (sign flip)
        (5950.0, "C", 300.0),  # long put   -> call row (sign flip)
    }
    # The real option type stays available for the strike-structure layer.
    by_strike = {r["strike"]: r for r in rows}
    assert by_strike[6050.0]["mm_option_type"] == "C"
    assert by_strike[5950.0]["mm_option_type"] == "P"


def test_zero_net_contracts_are_dropped_from_the_profile_rows():
    """A flat series contributes nothing and must not reach the kernel's oi>0 guard."""
    assert signed_profile_rows([_contract(6000.0, "C", 0)]) == []


def test_encoded_rows_reproduce_the_manual_dollar_gamma_exactly(engine):
    """The encoding identity, checked against a contract-by-contract sum.

    If this ever drifts, the experiment is no longer isolating attribution —
    it is also changing the gamma math, and the comparison becomes meaningless.
    """
    contracts = [
        _contract(5950.0, "P", -1200),
        _contract(6000.0, "C", +800),
        _contract(6050.0, "C", -500),
        _contract(5900.0, "P", +300),
    ]
    profile = engine._gamma_exposure_profile(
        signed_profile_rows(contracts), SPOT, TS, apply_dte_weight=False
    )
    kernel = engine._net_gex_at_spot(profile, SPOT)
    manual = _manual_dollar_gamma(engine, contracts, SPOT, TS)
    assert kernel == pytest.approx(manual, rel=1e-12)


def test_a_long_position_carries_positive_gamma_for_calls_and_puts(engine):
    """Gamma sign follows the POSITION, not the option type."""
    for option_type in ("C", "P"):
        contracts = [_contract(6000.0, option_type, +500)]
        profile = engine._gamma_exposure_profile(
            signed_profile_rows(contracts), SPOT, TS, apply_dte_weight=False
        )
        assert engine._net_gex_at_spot(profile, SPOT) > 0


def test_a_short_position_carries_negative_gamma_for_calls_and_puts(engine):
    for option_type in ("C", "P"):
        contracts = [_contract(6000.0, option_type, -500)]
        profile = engine._gamma_exposure_profile(
            signed_profile_rows(contracts), SPOT, TS, apply_dte_weight=False
        )
        assert engine._net_gex_at_spot(profile, SPOT) < 0


def test_call_and_put_of_equal_size_and_strike_contribute_identically(engine):
    """BSM gamma is option-type independent — the premise the encoding rests on."""
    call = engine._gamma_exposure_profile(
        signed_profile_rows([_contract(6000.0, "C", +500)]), SPOT, TS, apply_dte_weight=False
    )
    put = engine._gamma_exposure_profile(
        signed_profile_rows([_contract(6000.0, "P", +500)]), SPOT, TS, apply_dte_weight=False
    )
    assert engine._net_gex_at_spot(call, SPOT) == pytest.approx(
        engine._net_gex_at_spot(put, SPOT), rel=1e-12
    )


def test_existing_methodology_rows_keep_open_interest_and_true_option_type():
    """The control arm must be the production convention, unaltered."""
    rows = production_profile_rows([_quote(6000.0, "C"), _quote(5900.0, "P")])
    assert {(r["strike"], r["option_type"], r["open_interest"]) for r in rows} == {
        (6000.0, "C", 1000),
        (5900.0, "P", 1000),
    }


# ---------------------------------------------------------------------------
# Spot-shift behavior
# ---------------------------------------------------------------------------


def test_gamma_is_repriced_across_the_spot_grid_not_held_fixed(engine):
    """A single ATM long peaks at its strike and decays away from it.

    This is the property that makes the flip a spot-shift construct rather than
    a cumulative-sum approximation: the curve's shape comes from re-pricing
    gamma at every hypothetical spot.
    """
    profile = engine._gamma_exposure_profile(
        signed_profile_rows([_contract(6000.0, "C", +1000)]), SPOT, TS, apply_dte_weight=False
    )
    values = dict(profile)
    peak_price = max(values, key=lambda s: values[s])
    assert abs(peak_price - 6000.0) < 30.0
    far = min(values)
    assert values[far] < values[peak_price] * 0.10


def test_horizon_weighting_downweights_near_dated_contributions(engine):
    """ZeroGEX's DTE ramp applied to MM positioning shrinks a 2-DTE position."""
    contracts = [_contract(6000.0, "C", +1000, expiration=NEAR)]
    weighted = compute_mm_gex(
        contracts, SPOT, TS, engine=engine, apply_horizon_weighting=True
    ).mm_attributed_gamma_at_spot
    raw = compute_mm_gex(
        contracts, SPOT, TS, engine=engine, apply_horizon_weighting=False
    ).mm_attributed_gamma_at_spot
    assert 0 < weighted < raw


# ---------------------------------------------------------------------------
# Zero crossings
# ---------------------------------------------------------------------------


def _crossings(profile):
    out = []
    for (s1, v1), (s2, v2) in zip(profile, profile[1:]):
        if v1 == 0.0:
            out.append(s1)
        elif v1 * v2 < 0:
            out.append(s1 + (s2 - s1) * (-v1) / (v2 - v1))
    return out


def test_single_zero_crossing_is_located_between_the_two_positions(engine):
    """Short below spot, long above -> exactly one crossing, in between."""
    contracts = [_contract(5900.0, "P", -2000), _contract(6100.0, "C", +2000)]
    profile = engine._gamma_exposure_profile(
        signed_profile_rows(contracts), SPOT, TS, apply_dte_weight=False
    )
    crossings = _crossings(profile)
    assert len(crossings) == 1
    assert 5900.0 < crossings[0] < 6100.0
    flip = engine._calculate_gamma_flip_point(profile, SPOT)
    assert flip == pytest.approx(crossings[0])


def test_multiple_zero_crossings_resolve_to_the_one_nearest_spot(engine):
    """Alternating sign across strikes produces several crossings.

    Production's tie-break keeps the crossing nearest spot; the MM-attributed
    path inherits it unchanged.
    """
    contracts = [
        _contract(5700.0, "C", +3000),
        _contract(5850.0, "P", -3000),
        _contract(6150.0, "C", +3000),
        _contract(6300.0, "P", -3000),
    ]
    profile = engine._gamma_exposure_profile(
        signed_profile_rows(contracts), SPOT, TS, apply_dte_weight=False
    )
    crossings = _crossings(profile)
    assert len(crossings) >= 2
    flip = engine._calculate_gamma_flip_point(profile, SPOT)
    nearest = min(crossings, key=lambda c: abs(c - SPOT))
    assert flip == pytest.approx(nearest)


def test_one_signed_book_has_no_crossing_and_reports_none(engine):
    """All-long MM inventory never flips sign — the honest answer is None."""
    contracts = [_contract(5900.0, "P", +1000), _contract(6100.0, "C", +1000)]
    profile = engine._gamma_exposure_profile(
        signed_profile_rows(contracts), SPOT, TS, apply_dte_weight=False
    )
    assert all(v > 0 for _s, v in profile)
    assert engine._calculate_gamma_flip_point(profile, SPOT) is None

    result = compute_mm_gex(contracts, SPOT, TS, engine=engine)
    assert result.mm_attributed_gamma_flip is None
    assert result.flip_unresolved is True
    # ...and gamma at spot is still reported, because it is well defined.
    assert result.mm_attributed_gamma_at_spot > 0


def test_gamma_at_spot_and_flip_come_from_the_same_curve(engine):
    """Sign consistency: spot below the flip must read short gamma."""
    contracts = [_contract(5900.0, "P", -3000), _contract(6300.0, "C", +3000)]
    result = compute_mm_gex(contracts, SPOT, TS, engine=engine, apply_horizon_weighting=False)
    assert result.mm_attributed_gamma_flip is not None
    if SPOT < result.mm_attributed_gamma_flip:
        assert result.mm_attributed_gamma_at_spot < 0
        assert result.regime == "short_gamma"
    else:
        assert result.mm_attributed_gamma_at_spot > 0
        assert result.regime == "long_gamma"


# ---------------------------------------------------------------------------
# Strike rows and universes
# ---------------------------------------------------------------------------


def test_strike_rows_sign_the_production_slots_so_net_gex_matches(engine):
    """``call_gamma − put_gamma`` must equal MM net gamma at the strike."""
    contracts = [_contract(6000.0, "C", +800), _contract(6000.0, "P", -400)]
    rows = strike_rows(contracts, engine, SPOT, TS)
    assert len(rows) == 1
    row = rows[0]
    scale = CONTRACT_MULTIPLIER * SPOT * SPOT * 0.01
    assert row["call_gamma"] == pytest.approx(row["mm_call_gamma"])
    assert row["put_gamma"] == pytest.approx(-row["mm_put_gamma"])
    assert row["net_gex"] == pytest.approx((row["call_gamma"] - row["put_gamma"]) * scale)
    # Long 800 calls beats short 400 puts at the same strike and vol.
    assert row["net_gex"] > 0


def test_strike_rows_track_clean_versus_censored_series(engine):
    contracts = [
        _contract(6000.0, "C", +500, censored=False),
        _contract(6000.0, "P", +500, censored=True),
    ]
    row = strike_rows(contracts, engine, SPOT, TS)[0]
    assert row["n_series"] == 2
    assert row["n_clean_series"] == 1


def test_expiration_universes_select_the_right_contracts(engine):
    """0DTE, nearest, weekly and all must partition as their names claim."""
    as_of = TS.date()
    same_day = date(2026, 6, 10)
    contracts = [
        _contract(6000.0, "C", +100, expiration=same_day),
        _contract(6000.0, "C", +100, expiration=NEAR),  # 2 DTE
        _contract(6000.0, "C", +100, expiration=FAR),  # 37 DTE
    ]
    assert len(ExpirationUniverse("0dte", max_dte=0).select(contracts, as_of)) == 1
    assert len(ExpirationUniverse("nearest", nearest_only=True).select(contracts, as_of)) == 1
    assert len(ExpirationUniverse("weekly", max_dte=7).select(contracts, as_of)) == 2
    assert len(ExpirationUniverse("all").select(contracts, as_of)) == 3


def test_contribution_by_bucket_sums_to_one(engine):
    contracts = [
        _contract(6000.0, "C", +500, expiration=NEAR),
        _contract(6000.0, "C", +500, expiration=FAR),
    ]
    result = compute_mm_gex(contracts, SPOT, TS, engine=engine)
    assert set(result.contribution_by_bucket) <= {"0dte", "weekly", "monthly", "leaps"}
    assert sum(result.contribution_by_bucket.values()) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------


def test_positions_without_a_usable_quote_are_reported_not_dropped():
    """Unpriceable inventory is a completeness fact, so it must surface."""
    positions = [_position(6000.0, "C"), _position(9999.0, "C")]
    for p in positions:
        p.long_contracts = 100.0
    joined, unpriceable = join_positions_to_chain(positions, [_quote(6000.0, "C")])
    assert len(joined) == 1
    assert len(unpriceable) == 1
    assert unpriceable[0].strike == 9999.0


def test_join_can_use_the_open_close_agnostic_estimator():
    """``use_net_flow`` swaps the quantity without touching anything else."""
    pos = _position(6000.0, "C")
    # What the recursion would have produced from 100 opening buys, plus 40
    # contracts sold with no usable position effect.
    pos.opening_buys = 100.0
    pos.long_contracts = 100.0
    pos.unknown_effect_sells = 40.0
    recursion, _ = join_positions_to_chain([pos], [_quote(6000.0, "C")])
    net_flow, _ = join_positions_to_chain([pos], [_quote(6000.0, "C")], use_net_flow=True)
    assert recursion[0].net_contracts == 100.0
    assert net_flow[0].net_contracts == 60.0


def test_empty_inputs_produce_an_empty_but_well_formed_result(engine):
    result = compute_mm_gex([], SPOT, TS, engine=engine)
    assert result.n_contracts == 0
    assert result.mm_attributed_gamma_at_spot is None
    assert result.mm_attributed_gamma_flip is None
    assert result.regime is None
