"""Inventory confidence scoring and the independent reconciliation checks.

Confidence exists so an MM-attributed result can be read in the context of how
complete the inferred inventory is — the difference between "the methodology
failed" and "the reconstruction was not complete enough".  These tests pin the
ordering properties that make the score usable: censoring dominates, factors
are conjunctive, and gamma-weighting beats contract counting.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from research.mm_attributed_gex.confidence import (
    ConfidenceWeights,
    confidence_band,
    score_position,
    score_universe,
)
from research.mm_attributed_gex.inventory import MMPosition
from research.mm_attributed_gex.reconcile import (
    reconcile_open_interest,
    summarize,
    zero_sum_residual,
)
from research.mm_attributed_gex.schema import (
    Exchange,
    Interval,
    ParticipantActivity,
    ParticipantType,
    PositionEffect,
    Side,
)

SYMBOL = "SPX"
EXPIRATION = date(2026, 6, 19)


def _pos(
    strike: float = 6000.0,
    *,
    option_type: str = "C",
    net: float = 100.0,
    censored: bool = False,
    reason: str = "clean",
    sessions: int = 5,
    missing: tuple = (),
    breaches: int = 0,
    unknown_buys: float = 0.0,
) -> MMPosition:
    p = MMPosition(
        key=(SYMBOL, EXPIRATION, strike, option_type),
        symbol=SYMBOL,
        expiration=EXPIRATION,
        strike=strike,
        option_type=option_type,
        left_censored=censored,
        left_censor_reason=reason,
    )
    p.long_contracts = max(0.0, net)
    p.short_contracts = max(0.0, -net)
    p.opening_buys = max(0.0, net)
    p.opening_sells = max(0.0, -net)
    p.active_sessions = {date(2026, 6, 1 + i) for i in range(sessions)}
    p.missing_sessions = missing
    p.long_floor_breaches = breaches
    p.unknown_effect_buys = unknown_buys
    return p


# ---------------------------------------------------------------------------
# Per-series confidence
# ---------------------------------------------------------------------------


def test_a_clean_series_observed_from_listing_scores_highest():
    conf = score_position(_pos(reason="observed_from_listing_date"))
    assert conf.score == pytest.approx(1.0)
    assert conf.origin == 1.0
    assert confidence_band(conf.score) == "high"


def test_a_series_clean_only_by_the_window_heuristic_scores_slightly_lower():
    """Inference is not evidence — an unconfirmed origin costs a little."""
    confirmed = score_position(_pos(reason="observed_from_listing_date"))
    inferred = score_position(_pos(reason="first_trade_inside_window"))
    assert inferred.score < confirmed.score
    assert inferred.score > 0.8


def test_left_censoring_dominates_every_other_factor():
    """An unknown starting position is close to fatal for a LEVEL."""
    censored = score_position(_pos(censored=True, reason="first_trade_at_window_edge", sessions=30))
    clean_but_gappy = score_position(_pos(missing=(date(2026, 6, 3),), sessions=1))
    assert censored.score < clean_but_gappy.score
    assert confidence_band(censored.score) == "low"


def test_factors_are_conjunctive_so_one_strong_factor_cannot_rescue_a_fatal_one():
    """A product, not an average: perfect continuity does not offset censoring."""
    conf = score_position(_pos(censored=True, sessions=30, missing=()))
    assert conf.continuity == 1.0
    assert conf.maturity == 1.0
    assert conf.score == pytest.approx(conf.origin)


def test_each_missing_session_decays_the_continuity_factor():
    one = score_position(_pos(missing=(date(2026, 6, 3),)))
    two = score_position(_pos(missing=(date(2026, 6, 3), date(2026, 6, 4))))
    assert two.continuity < one.continuity < 1.0
    assert two.score < one.score


def test_unknown_position_effect_volume_lowers_effect_coverage():
    clean = score_position(_pos())
    murky = score_position(_pos(net=100.0, unknown_buys=100.0))
    assert murky.effect_coverage < clean.effect_coverage
    assert murky.score < clean.score


def test_floor_breaches_decay_the_score_and_mark_it_censored():
    conf = score_position(
        _pos(censored=True, reason="closing_volume_exceeded_observed_opens", breaches=2)
    )
    assert conf.breach < 1.0
    assert conf.reason == "closing_volume_exceeded_observed_opens"


def test_a_one_session_series_is_penalized_relative_to_a_mature_one():
    young = score_position(_pos(sessions=1))
    mature = score_position(_pos(sessions=10))
    assert young.maturity < mature.maturity
    assert mature.maturity == 1.0


def test_weights_are_tunable_without_touching_the_scoring_code():
    lenient = ConfidenceWeights(censored_origin=0.9)
    assert (
        score_position(_pos(censored=True), lenient).score
        > score_position(_pos(censored=True)).score
    )


def test_confidence_band_thresholds():
    assert confidence_band(0.9) == "high"
    assert confidence_band(0.5) == "medium"
    assert confidence_band(0.1) == "low"
    assert confidence_band(0.0) == "none"
    assert confidence_band(None) == "none"


# ---------------------------------------------------------------------------
# Universe confidence
# ---------------------------------------------------------------------------


def test_universe_confidence_weights_by_gamma_not_by_contract_count():
    """Many clean but gamma-less series must not make a snapshot look trustworthy.

    The censored series here carries almost all the gamma, so a gamma-weighted
    score has to land low even though 9 of 10 series are clean.
    """
    positions = [_pos(strike=5000.0 + i, net=10.0) for i in range(9)]
    positions.append(_pos(strike=6000.0, net=10.0, censored=True))
    gamma = {p.key: 1.0 for p in positions[:9]}
    gamma[positions[-1].key] = 1_000.0

    conf = score_universe(positions, gamma_by_key=gamma)
    assert conf.n_clean_series == 9
    assert conf.gamma_weighted_confidence < 0.3
    assert conf.band == "low"
    # Contract-count weighting would have told the opposite story.
    naive = score_universe(positions)
    assert naive.gamma_weighted_confidence > conf.gamma_weighted_confidence


def test_percent_of_gamma_universe_reconstructed_uses_the_production_denominator():
    positions = [_pos(strike=6000.0)]
    conf = score_universe(
        positions, gamma_by_key={positions[0].key: 250.0}, universe_gamma_abs=1_000.0
    )
    assert conf.pct_gamma_universe_reconstructed == pytest.approx(0.25)


def test_reconstructed_gamma_cannot_exceed_the_universe():
    positions = [_pos(strike=6000.0)]
    conf = score_universe(
        positions, gamma_by_key={positions[0].key: 5_000.0}, universe_gamma_abs=1_000.0
    )
    assert conf.pct_gamma_universe_reconstructed == 1.0


def test_empty_universe_scores_zero_without_raising():
    conf = score_universe([])
    assert conf.n_series == 0
    assert conf.gamma_weighted_confidence == 0.0
    assert conf.band == "none"


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


def _rec(participant, side, effect, contracts, *, trading_date=date(2026, 6, 15), strike=6000.0):
    return ParticipantActivity(
        exchange=Exchange.CBOE_C1,
        symbol=SYMBOL,
        expiration=EXPIRATION,
        strike=strike,
        option_type="C",
        participant=participant,
        side=side,
        position_effect=effect,
        contracts=contracts,
        timestamp=datetime(
            trading_date.year, trading_date.month, trading_date.day, 20, tzinfo=timezone.utc
        ),
        trading_date=trading_date,
        interval=Interval.SESSION,
    )


def test_open_interest_identity_matches_a_consistent_session():
    """200 contracts opened between two participants moves OI by 200.

    Each opened contract shows up twice in the feed — once as somebody's buy,
    once as somebody's sell — so 400 units of reported opening volume represent
    200 real contracts, and the implied ΔOI is half the reported total.
    """
    d0, d1 = date(2026, 6, 12), date(2026, 6, 15)
    records = [
        _rec(ParticipantType.MARKET_MAKER, Side.SELL, PositionEffect.OPEN, 200, trading_date=d1),
        _rec(ParticipantType.CUSTOMER, Side.BUY, PositionEffect.OPEN, 200, trading_date=d1),
    ]
    key = (SYMBOL, EXPIRATION, 6000.0, "C")
    observed = {(key, d0): 500, (key, d1): 700}

    report = reconcile_open_interest(records, observed)
    assert report.n_checked == 1
    assert report.n_matched == 1
    assert report.verdict == "consistent"
    assert report.checks[0].implied_delta_oi == pytest.approx(200.0)
    assert report.checks[0].observed_delta_oi == pytest.approx(200.0)


def test_open_interest_identity_flags_an_inconsistent_session():
    d0, d1 = date(2026, 6, 12), date(2026, 6, 15)
    records = [
        _rec(ParticipantType.MARKET_MAKER, Side.SELL, PositionEffect.OPEN, 200, trading_date=d1),
        _rec(ParticipantType.CUSTOMER, Side.BUY, PositionEffect.OPEN, 200, trading_date=d1),
    ]
    key = (SYMBOL, EXPIRATION, 6000.0, "C")
    observed = {(key, d0): 500, (key, d1): 505}  # only +5, not +200

    report = reconcile_open_interest(records, observed)
    assert report.n_matched == 0
    assert report.verdict == "inconsistent"
    assert any("column mapping" in n for n in report.notes)


def test_closing_volume_reduces_the_implied_open_interest_change():
    d0, d1 = date(2026, 6, 12), date(2026, 6, 15)
    records = [
        _rec(ParticipantType.MARKET_MAKER, Side.SELL, PositionEffect.OPEN, 200, trading_date=d1),
        _rec(ParticipantType.CUSTOMER, Side.BUY, PositionEffect.OPEN, 200, trading_date=d1),
        _rec(ParticipantType.MARKET_MAKER, Side.BUY, PositionEffect.CLOSE, 60, trading_date=d1),
        _rec(ParticipantType.CUSTOMER, Side.SELL, PositionEffect.CLOSE, 60, trading_date=d1),
    ]
    key = (SYMBOL, EXPIRATION, 6000.0, "C")
    observed = {(key, d0): 500, (key, d1): 640}  # +200 opened, −60 closed

    report = reconcile_open_interest(records, observed)
    assert report.checks[0].implied_delta_oi == pytest.approx(140.0)
    assert report.n_matched == 1


def test_reconciliation_reports_when_open_interest_is_unavailable():
    records = [
        _rec(ParticipantType.MARKET_MAKER, Side.SELL, PositionEffect.OPEN, 200),
        _rec(ParticipantType.CUSTOMER, Side.BUY, PositionEffect.OPEN, 200),
    ]
    report = reconcile_open_interest(records, {})
    assert report.verdict == "no_overlapping_oi"
    assert report.n_missing_oi == 1


def test_reconciliation_needs_open_close_designations():
    records = [
        _rec(ParticipantType.MARKET_MAKER, Side.SELL, PositionEffect.UNKNOWN, 200),
    ]
    report = reconcile_open_interest(records, {})
    assert report.verdict == "no_open_close_records"


def test_zero_sum_holds_when_every_trade_has_a_reported_counterparty():
    records = [
        _rec(ParticipantType.MARKET_MAKER, Side.SELL, PositionEffect.OPEN, 200),
        _rec(ParticipantType.CUSTOMER, Side.BUY, PositionEffect.OPEN, 200),
    ]
    residual, total_abs, share = zero_sum_residual(records)
    assert total_abs == 0.0
    assert share == 0.0


def test_zero_sum_residual_surfaces_a_missing_participant_category():
    """Options are in zero net supply; a residual means coverage is incomplete."""
    records = [_rec(ParticipantType.MARKET_MAKER, Side.SELL, PositionEffect.OPEN, 200)]
    _residual, total_abs, share = zero_sum_residual(records)
    assert total_abs == 200.0
    assert share == 1.0

    report = summarize(records)
    assert report.zero_sum_residual_share == 1.0
    assert any("do not sum to zero" in n for n in report.notes)


def test_summarize_warns_when_no_market_maker_records_are_present():
    records = [_rec(ParticipantType.CUSTOMER, Side.BUY, PositionEffect.OPEN, 10)]
    report = summarize(records)
    assert any("MARKET_MAKER" in n for n in report.notes)
