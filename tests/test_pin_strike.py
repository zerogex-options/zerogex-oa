"""Tests for the Pin Strike metric.

Pin Strike (:mod:`src.analytics.pin_strike`) is the reachable 0DTE strike with
the strongest modeled POSITIVE (restoring) dealer gamma into expiration. The
suite is organized as:

* building-block tests (reachability, bandwidth, ATM IV, candidate range,
  local restoring GEX) — pure and deterministic;
* the ten scenario tests called out in the spec, driven through the pure
  orchestrator with the REAL canonical BSM gamma
  (:meth:`AnalyticsEngine._calculate_bs_gamma`, injected exactly as production
  wires it) so they exercise real Black-Scholes, not a stand-in;
* an engine-adapter test that exercises the wiring in
  :meth:`AnalyticsEngine._calculate_pin_strike` end-to-end (0DTE selection,
  ATM-IV derivation, intraday tau, summary fields).

The headline property — Pin Strike is NOT King Node / max-GEX — is scenario 1.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.analytics import pin_strike as ps
from src.analytics.main_engine import AnalyticsEngine


# --------------------------------------------------------------------------- #
# Shared helpers.
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def gamma_fn():
    """The real canonical BSM gamma, injected exactly as the engine does."""
    engine = AnalyticsEngine(underlying="SPY")
    return engine._calculate_bs_gamma


def _c(strike, oi, option_type="C", iv=0.15, T=0.0011):
    """A normalized contract as the pure orchestrator consumes it."""
    return {"strike": strike, "option_type": option_type, "open_interest": oi, "iv": iv, "T": T}


def _by_strike(result):
    return {round(c.strike, 4): c for c in result.candidates}


# 0DTE-representative inputs: ~9.7h to the close (sigma·√tau ≈ 0.005) gives the
# sharp reachability discrimination that is the point of the metric.
SPOT = 772.0
SIGMA = 0.15
TAU_0DTE = 0.0011
EXP = date(2026, 8, 7)


# --------------------------------------------------------------------------- #
# Building blocks.
# --------------------------------------------------------------------------- #
def test_reachability_peaks_at_spot_and_decays_off_spot():
    at = ps.calculate_pin_reachability(SPOT, spot=SPOT, sigma=SIGMA, tau=TAU_0DTE)
    near = ps.calculate_pin_reachability(SPOT + 1, spot=SPOT, sigma=SIGMA, tau=TAU_0DTE)
    far = ps.calculate_pin_reachability(SPOT + 8, spot=SPOT, sigma=SIGMA, tau=TAU_0DTE)
    assert at == pytest.approx(1.0)
    assert 0.0 < far < near < at


def test_reachability_zero_on_degenerate_inputs():
    # Missing/zero sigma or tau, non-positive spot/strike -> 0 (no fabricated pin).
    assert ps.calculate_pin_reachability(775, spot=SPOT, sigma=0.0, tau=TAU_0DTE) == 0.0
    assert ps.calculate_pin_reachability(775, spot=SPOT, sigma=SIGMA, tau=0.0) == 0.0
    assert ps.calculate_pin_reachability(775, spot=0.0, sigma=SIGMA, tau=TAU_0DTE) == 0.0
    assert ps.calculate_pin_reachability(0.0, spot=SPOT, sigma=SIGMA, tau=TAU_0DTE) == 0.0


def test_bandwidth_from_strike_spacing_scales_per_product():
    # Bandwidth tracks the product's grid: $1 (SPY/QQQ), 5 (SPX), 25 (NDX-like).
    assert ps.estimate_strike_bandwidth([100, 101, 102, 103], mult=1.0) == pytest.approx(1.0)
    assert ps.estimate_strike_bandwidth([760, 765, 770, 775], mult=1.0) == pytest.approx(5.0)
    assert ps.estimate_strike_bandwidth([20000, 20025, 20050], mult=1.0) == pytest.approx(25.0)
    # Multiplier applies; irregular grid uses the MEDIAN interval (robust to a
    # single wide gap).  Intervals here: 5,5,5,20 -> median 5.
    assert ps.estimate_strike_bandwidth([100, 105, 110, 115, 135], mult=2.0) == pytest.approx(10.0)


def test_bandwidth_fallback_when_spacing_unmeasurable():
    # A single distinct strike can't reveal spacing -> use the fallback grid.
    assert ps.estimate_strike_bandwidth([500.0], mult=1.0, fallback_increment=5.0) == pytest.approx(
        5.0
    )
    assert ps.estimate_strike_bandwidth([500.0], mult=1.0) is None


def test_representative_atm_iv_prefers_calls_then_puts():
    contracts = [
        {"strike": 771, "option_type": "C", "iv": 0.20},
        {"strike": 773, "option_type": "C", "iv": 0.22},
        {"strike": 900, "option_type": "C", "iv": 0.90},  # outside ±1% band
        {"strike": 772, "option_type": "P", "iv": 0.40},  # ignored (calls present)
    ]
    assert ps.representative_atm_iv(contracts, SPOT, band_pct=0.01) == pytest.approx(0.21)
    # No ATM calls -> fall back to puts.
    puts_only = [{"strike": 772, "option_type": "P", "iv": 0.40}]
    assert ps.representative_atm_iv(puts_only, SPOT, band_pct=0.01) == pytest.approx(0.40)
    # No ATM IV at all -> None (honest INSUFFICIENT_IV signal, no fabrication).
    assert ps.representative_atm_iv([], SPOT) is None
    assert ps.representative_atm_iv([{"strike": 772, "option_type": "C", "iv": 0.0}], SPOT) is None


def test_candidate_range_restricts_to_reachable_listed_strikes():
    strikes = [700, 760, 770, 772, 775, 785, 850]
    cands = ps.select_candidate_strikes(strikes, spot=SPOT, sigma=SIGMA, tau=TAU_0DTE, max_z=2.5)
    # Only actual listed strikes, and only those within ~2.5 expected moves.
    assert set(cands).issubset(set(strikes))
    assert 772 in cands and 770 in cands and 775 in cands
    assert 700 not in cands and 850 not in cands  # far outside the reachable band
    # Degenerate band (no vol/time) still yields the single nearest strike so a
    # valid book always has a candidate.
    assert ps.select_candidate_strikes(strikes, spot=SPOT, sigma=0.0, tau=0.0) == [772]


def test_local_restoring_gex_zero_for_put_dominated_neighborhood(gamma_fn):
    # A neighborhood of dealer-short puts has negative local gex -> restoring 0.
    puts = [_c(770, 5000, "P"), _c(772, 8000, "P"), _c(774, 5000, "P")]
    local, restoring = ps.calculate_local_restoring_gex(
        772, puts, gamma_fn=gamma_fn, bandwidth=2.0, risk_free_rate=0.05
    )
    assert local < 0
    assert restoring == 0.0


# --------------------------------------------------------------------------- #
# Scenario 1 — Strong reachable pin (Pin Strike is NOT King Node / max GEX).
# --------------------------------------------------------------------------- #
def test_scenario1_reachable_pin_beats_larger_but_unreachable_gamma(gamma_fn):
    # 773: modest gamma, highly reachable.  780: ~10x the OI (much larger raw
    # restoring gamma) but far enough that reachability collapses.
    contracts = [_c(770, 2000), _c(773, 8000), _c(780, 80000)]
    res = ps.calculate_pin_strike(
        contracts,
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
        candidate_max_z=3.0,  # keep 780 in-band so it competes on score, not filtering
    )
    assert res.pin_strike == 773.0
    assert res.status == ps.STATUS_ACTIVE
    assert res.reason is None

    cands = _by_strike(res)
    # The whole point: 780 has the LARGER raw restoring gamma, yet 773 wins on
    # pin_score because reachability down-weights the unreachable node.
    assert cands[780.0].restoring_gex > cands[773.0].restoring_gex
    assert cands[780.0].reachability < cands[773.0].reachability
    assert cands[773.0].pin_score > cands[780.0].pin_score


# --------------------------------------------------------------------------- #
# Scenario 2 — Coincides with the dominant-gamma (King Node) strike.
# --------------------------------------------------------------------------- #
def test_scenario2_coincides_with_dominant_reachable_gamma(gamma_fn):
    # Dominant gamma concentration sits AT spot and is highly reachable, so Pin
    # Strike and the max-gamma strike naturally coincide.
    contracts = [_c(769, 3000), _c(772, 60000), _c(775, 3000)]
    res = ps.calculate_pin_strike(
        contracts,
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
        candidate_max_z=3.0,
    )
    assert res.pin_strike == 772.0
    # It is also the strike with the largest restoring gamma (the "king node").
    cands = _by_strike(res)
    assert max(cands.values(), key=lambda c: c.restoring_gex).strike == 772.0


# --------------------------------------------------------------------------- #
# Scenario 3 — Negative gamma regime -> no pin.
# --------------------------------------------------------------------------- #
def test_scenario3_all_negative_local_gamma_returns_null(gamma_fn):
    # Every reachable candidate is put-dominated (dealer short gamma) -> no
    # positive restoring gamma anywhere -> no active pin.
    contracts = [_c(768, 6000, "P"), _c(772, 9000, "P"), _c(776, 6000, "P")]
    res = ps.calculate_pin_strike(
        contracts,
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
        candidate_max_z=3.0,
    )
    assert res.pin_strike is None
    assert res.status == ps.STATUS_UNAVAILABLE
    assert res.reason == ps.REASON_NO_POSITIVE_RESTORING_GAMMA
    # Every scored candidate had zero restoring gamma.
    assert all(c.restoring_gex == 0.0 for c in res.candidates)


# --------------------------------------------------------------------------- #
# Scenario 4 — Mixed gamma: negative candidates get zero restoring score.
# --------------------------------------------------------------------------- #
def test_scenario4_mixed_gamma_zeroes_negative_candidates(gamma_fn):
    # Puts concentrated below spot (negative), calls concentrated above spot
    # (positive).  The pin must land on the positive side; negative-side
    # candidates must score zero, not a negative pin_score.
    contracts = [_c(766, 9000, "P"), _c(778, 12000, "C")]
    res = ps.calculate_pin_strike(
        contracts,
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
        candidate_max_z=3.0,
    )
    assert res.pin_strike == 778.0
    cands = _by_strike(res)
    assert cands[766.0].local_gex < 0
    assert cands[766.0].restoring_gex == 0.0
    assert cands[766.0].pin_score == 0.0
    assert cands[778.0].pin_score > 0.0


# --------------------------------------------------------------------------- #
# Scenario 5 — Far-away giant gamma must not win.
# --------------------------------------------------------------------------- #
def test_scenario5_far_giant_gamma_does_not_beat_nearby_positive(gamma_fn):
    # A colossal gamma node 16% away is unreachable into the 0DTE close; a small
    # nearby positive node wins.  (Here the far node is also filtered out of the
    # candidate set by the reachability range — either way it cannot pin.)
    contracts = [_c(773, 4000), _c(900, 5_000_000)]
    res = ps.calculate_pin_strike(
        contracts,
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
    )
    assert res.pin_strike == 773.0
    assert all(c.strike != 900.0 for c in res.candidates)


# --------------------------------------------------------------------------- #
# Scenario 6 — Expiration proximity changes reachability.
# --------------------------------------------------------------------------- #
def test_scenario6_reachability_shrinks_as_time_decays():
    # For a fixed off-spot strike, less remaining time -> lower reachability
    # (harder to travel there), while an at-spot strike stays pinned at 1.0.
    k = SPOT + 5
    r_morning = ps.calculate_pin_reachability(k, spot=SPOT, sigma=SIGMA, tau=0.0015)
    r_midday = ps.calculate_pin_reachability(k, spot=SPOT, sigma=SIGMA, tau=0.0008)
    r_close = ps.calculate_pin_reachability(k, spot=SPOT, sigma=SIGMA, tau=0.0002)
    assert r_morning > r_midday > r_close > 0.0
    for tau in (0.0015, 0.0008, 0.0002):
        at_spot = ps.calculate_pin_reachability(SPOT, spot=SPOT, sigma=SIGMA, tau=tau)
        assert at_spot == pytest.approx(1.0)


def test_scenario6_pin_score_reflects_time_decay(gamma_fn):
    # The same off-spot candidate scores lower as expiration approaches.
    contracts = [_c(777, 20000)]
    scores = []
    for tau in (0.0015, 0.0006):
        res = ps.calculate_pin_strike(
            contracts,
            spot=SPOT,
            tau=tau,
            sigma=SIGMA,
            gamma_fn=gamma_fn,
            risk_free_rate=0.05,
            expiration=EXP,
            candidate_max_z=4.0,
            fallback_increment=1.0,  # single-strike book: supply the grid as production does
        )
        scores.append(_by_strike(res)[777.0].pin_score)
    assert scores[0] > scores[1]


# --------------------------------------------------------------------------- #
# Scenario 7 — Missing same-day expiration.
# --------------------------------------------------------------------------- #
def test_scenario7_no_0dte_returns_reasoned_null(gamma_fn):
    res = ps.calculate_pin_strike(
        [_c(772, 5000)],
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=None,
        has_0dte=False,
    )
    assert res.pin_strike is None
    assert res.reason == ps.REASON_NO_0DTE_EXPIRATION
    assert res.summary_fields() == {
        "pin_strike": None,
        "pin_score": None,
        "pin_confidence": None,
        "pin_strike_reason": ps.REASON_NO_0DTE_EXPIRATION,
    }


# --------------------------------------------------------------------------- #
# Scenario 8 — Missing / invalid IV.
# --------------------------------------------------------------------------- #
def test_scenario8_missing_iv_is_handled_gracefully(gamma_fn):
    # No representative sigma supplied -> INSUFFICIENT_IV_DATA, no crash.
    res = ps.calculate_pin_strike(
        [_c(772, 5000)],
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=None,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
    )
    assert res.pin_strike is None
    assert res.reason == ps.REASON_INSUFFICIENT_IV_DATA


def test_scenario8_contracts_with_bad_iv_are_dropped(gamma_fn):
    # Individual contracts with missing/invalid IV are skipped; a single valid
    # ATM contract still yields a pin (sigma passed explicitly).
    contracts = [
        {"strike": 772, "option_type": "C", "open_interest": 9000, "iv": None, "T": TAU_0DTE},
        {"strike": 771, "option_type": "C", "open_interest": 9000, "iv": -1.0, "T": TAU_0DTE},
        _c(772, 9000, iv=0.15),
    ]
    res = ps.calculate_pin_strike(
        contracts,
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
        candidate_max_z=3.0,
        fallback_increment=1.0,  # only one valid strike survives IV filtering
    )
    assert res.pin_strike == 772.0


def test_all_invalid_contracts_returns_insufficient_option_data(gamma_fn):
    contracts = [
        {"strike": 772, "option_type": "C", "open_interest": 0, "iv": 0.15, "T": TAU_0DTE},
        {"strike": 773, "option_type": "C", "open_interest": 100, "iv": 0.15, "T": 0.0},
    ]
    res = ps.calculate_pin_strike(
        contracts,
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
    )
    assert res.pin_strike is None
    assert res.reason == ps.REASON_INSUFFICIENT_OPTION_DATA


def test_expired_tau_returns_expired_reason(gamma_fn):
    res = ps.calculate_pin_strike(
        [_c(772, 5000)],
        spot=SPOT,
        tau=0.0,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
    )
    assert res.pin_strike is None
    assert res.reason == ps.REASON_EXPIRED


# --------------------------------------------------------------------------- #
# Scenario 9 — Confidence never divides by zero.
# --------------------------------------------------------------------------- #
def test_scenario9_confidence_guarded_single_positive(gamma_fn):
    # Exactly one positive candidate -> confidence is 1.0 (best/best), not a
    # ZeroDivisionError.
    contracts = [_c(772, 40000)]
    res = ps.calculate_pin_strike(
        contracts,
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
        candidate_max_z=0.6,
        fallback_increment=1.0,
    )
    assert res.pin_strike == 772.0
    assert res.pin_confidence == pytest.approx(1.0)


def test_scenario9_confidence_is_dominance_fraction(gamma_fn):
    # Two comparable positive pins -> confidence is the winner's share of the
    # total positive score, strictly between 0 and 1.
    contracts = [_c(771, 20000), _c(773, 20000)]
    res = ps.calculate_pin_strike(
        contracts,
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
        candidate_max_z=3.0,
    )
    positives = [c.pin_score for c in res.candidates if c.pin_score > 0]
    assert res.pin_confidence == pytest.approx(max(positives) / sum(positives))
    assert 0.0 < res.pin_confidence < 1.0


def test_pin_score_too_weak_gate_when_enabled(gamma_fn):
    # With a very high magnitude floor, an otherwise-valid pin is suppressed.
    contracts = [_c(772, 5000)]
    res = ps.calculate_pin_strike(
        contracts,
        spot=SPOT,
        tau=TAU_0DTE,
        sigma=SIGMA,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
        candidate_max_z=1.0,
        min_pin_score=1e30,
        fallback_increment=1.0,
    )
    assert res.pin_strike is None
    assert res.reason == ps.REASON_PIN_SCORE_TOO_WEAK


# --------------------------------------------------------------------------- #
# Scenario 10 — Different strike spacing (coarse-grid product).
# --------------------------------------------------------------------------- #
def test_scenario10_works_on_coarse_grid_product(gamma_fn):
    # An NDX-like 25-point grid around a 20,000 spot.  The kernel/bandwidth is
    # derived from the grid, so the pin resolves to a listed strike with no
    # per-product dollar constants.
    spot = 20_000.0
    grid = [19_900, 19_925, 19_950, 19_975, 20_000, 20_025, 20_050]
    contracts = [_c(k, 5000, iv=0.14, T=0.0011) for k in grid]
    # Concentrate gamma at 20,025 (one step above spot, highly reachable).
    contracts.append(_c(20_025, 60000, iv=0.14, T=0.0011))
    res = ps.calculate_pin_strike(
        contracts,
        spot=spot,
        tau=0.0011,
        sigma=0.14,
        gamma_fn=gamma_fn,
        risk_free_rate=0.05,
        expiration=EXP,
        candidate_max_z=3.0,
    )
    assert res.pin_strike == 20_025.0
    # Bandwidth tracked the 25-point grid, not a $1 default.
    assert res.bandwidth == pytest.approx(25.0)
    assert res.pin_strike in grid  # only listed strikes are returned


# --------------------------------------------------------------------------- #
# Engine adapter wiring (0DTE selection, ATM IV, intraday tau, summary fields).
# --------------------------------------------------------------------------- #
def _opt(strike, oi, option_type="C", iv=0.15, exp=None):
    return {
        "strike": strike,
        "expiration": exp,
        "option_type": option_type,
        "open_interest": oi,
        "implied_volatility": iv,
        "gamma": 0.01,
        "volume": 10,
        "option_symbol": "SPY",
    }


def test_engine_adapter_active_pin_from_0dte_options():
    engine = AnalyticsEngine(underlying="SPY")
    # 2026-08-07 14:30 UTC == 10:30 ET (Fri), so today_et is 2026-08-07 and tau
    # to the 16:00 ET close is ~5.5h (> 0).
    ts = datetime(2026, 8, 7, 14, 30, tzinfo=timezone.utc)
    exp_today = date(2026, 8, 7)
    exp_next_week = date(2026, 8, 14)
    options = [
        _opt(770, 2000, exp=exp_today),
        _opt(772, 9000, exp=exp_today),
        _opt(773, 8000, exp=exp_today),
        _opt(775, 4000, exp=exp_today),
        # A next-week contract that must NOT influence a 0DTE pin.
        _opt(780, 500000, exp=exp_next_week),
    ]
    res = engine._calculate_pin_strike(options, 772.0, ts)
    assert res.status == ps.STATUS_ACTIVE
    assert res.expiration == exp_today
    assert res.pin_strike in {770.0, 772.0, 773.0, 775.0}  # a listed 0DTE strike, not 780
    fields = res.summary_fields()
    assert fields["pin_strike"] == res.pin_strike
    assert fields["pin_strike_reason"] is None
    assert 0.0 < fields["pin_confidence"] <= 1.0


def test_engine_adapter_no_same_day_expiration():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 8, 7, 14, 30, tzinfo=timezone.utc)
    # Only a next-week expiration exists -> no 0DTE.
    options = [_opt(772, 9000, exp=date(2026, 8, 14))]
    res = engine._calculate_pin_strike(options, 772.0, ts)
    assert res.pin_strike is None
    assert res.reason == ps.REASON_NO_0DTE_EXPIRATION


def test_engine_summary_includes_pin_strike_fields():
    """`_calculate_gex_summary` must surface the pin fields for persistence."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 8, 7, 14, 30, tzinfo=timezone.utc)
    spot = 772.0
    gex_by_strike = [
        {
            "strike": 771.0,
            "call_gamma": 40.0,
            "put_gamma": 0.0,
            "net_gex": 1.0,
            "call_oi": 100,
            "put_oi": 0,
        },
        {
            "strike": 773.0,
            "call_gamma": 60.0,
            "put_gamma": 0.0,
            "net_gex": 2.0,
            "call_oi": 100,
            "put_oi": 0,
        },
    ]
    options = [
        _opt(771, 9000, exp=date(2026, 8, 7)),
        _opt(772, 12000, exp=date(2026, 8, 7)),
        _opt(773, 9000, exp=date(2026, 8, 7)),
    ]
    summary = engine._calculate_gex_summary(
        gex_by_strike=gex_by_strike, options=options, underlying_price=spot, timestamp=ts
    )
    assert summary is not None
    # The four additive keys are always present (nullable), never crashing the
    # existing summary assembly.
    for key in ("pin_strike", "pin_score", "pin_confidence", "pin_strike_reason"):
        assert key in summary
