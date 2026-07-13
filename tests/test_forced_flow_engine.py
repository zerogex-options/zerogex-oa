"""Phase 2 -- the Forced Flow engine.

Exercises the single primitive (dealer_hedge_flow, full reprice), the first-order
attribution and residual, the model-A squeeze sign, the grid-derived charts
(curve / charm-into-close / vanna ladder), the derived levels, and the near-
expiry (0DTE) intrinsic handling.
"""

import math

from src.analytics.forced_flow import (
    ContractLeg,
    ForcedFlow,
    charm_flip,
    charm_into_close,
    combine_flow_sources,
    dealer_hedge_flow,
    forced_flow_curve,
    vanna_flip,
    vanna_ladder,
    zero_flow_level,
)

R = 0.05


def leg(K, ot, oi, iv=0.20, dte_days=30):
    return ContractLeg(
        strike=K, option_type=ot, open_interest=oi, iv=iv, tte_years=dte_days / 365.0
    )


# A book whose dealer gamma flips near S=99 (verified numerically).
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


# --------------------------------------------------------------------------- #
# The primitive: sign, exactness, degenerate cases
# --------------------------------------------------------------------------- #
def test_zero_scenario_is_zero_flow():
    ff = dealer_hedge_flow(BOOK, 100.0, 0.0, 0.0, 0.0, R)
    assert ff.total_usd == 0.0
    assert ff.gamma_component == 0.0
    assert ff.charm_component == 0.0
    assert ff.vanna_component == 0.0
    assert ff.residual == 0.0


def test_empty_book_is_zero_flow():
    ff = dealer_hedge_flow([], 100.0, 0.05, 0.5, 1.0, R)
    assert ff.total_usd == 0.0


def test_short_gamma_squeeze_sign_below_and_above_flip():
    # Below the flip (dealers short gamma): a spot rise forces BUYING into
    # strength -- the short-gamma squeeze (section 2). Above it: SELLING.
    below = dealer_hedge_flow(BOOK, 93.0, 0.01, 0.0, 0.0, R).total_usd
    above = dealer_hedge_flow(BOOK, 103.0, 0.01, 0.0, 0.0, R).total_usd
    assert below > 0.0, below
    assert above < 0.0, above


def test_spot_move_sign_is_antisymmetric_ish():
    up = dealer_hedge_flow(BOOK, 93.0, 0.01, 0.0, 0.0, R).total_usd
    dn = dealer_hedge_flow(BOOK, 93.0, -0.01, 0.0, 0.0, R).total_usd
    # A down move below the flip forces selling (opposite sign to the up move).
    assert up > 0.0 and dn < 0.0


# --------------------------------------------------------------------------- #
# Attribution: axis isolation, and full-reprice vs first-order residual
# --------------------------------------------------------------------------- #
def test_components_isolate_by_axis():
    # Pure spot move -> only gamma component is non-zero.
    ff = dealer_hedge_flow(BOOK, 100.0, 0.01, 0.0, 0.0, R)
    assert ff.charm_component == 0.0 and ff.vanna_component == 0.0
    assert ff.gamma_component != 0.0

    # Pure time -> only charm component.
    ff = dealer_hedge_flow(BOOK, 100.0, 0.0, 1.0, 0.0, R)
    assert ff.gamma_component == 0.0 and ff.vanna_component == 0.0
    assert ff.charm_component != 0.0

    # Pure vol -> only vanna component.
    ff = dealer_hedge_flow(BOOK, 100.0, 0.0, 0.0, 1.0, R)
    assert ff.gamma_component == 0.0 and ff.charm_component == 0.0
    assert ff.vanna_component != 0.0


def test_residual_small_for_small_move_and_grows_for_large():
    small = dealer_hedge_flow(BOOK, 100.0, 0.0005, 0.0, 0.0, R)
    large = dealer_hedge_flow(BOOK, 100.0, 0.05, 0.0, 0.0, R)
    # First-order attribution nearly reproduces the exact total for a small move.
    assert abs(small.residual) <= 0.02 * abs(small.total_usd)
    # ...and linear intuition degrades on a big move: residual is a larger share.
    small_share = abs(small.residual) / abs(small.total_usd)
    large_share = abs(large.residual) / abs(large.total_usd)
    assert large_share > small_share


def test_total_is_exact_reprice_not_component_sum():
    # total_usd must equal the direct reprice, independent of the attribution.
    from src.analytics.forced_flow import _aggregate_dealer_delta

    spot, move, days, vol = 100.0, 0.03, 0.5, 1.0
    spot_scn = spot * (1 + move)
    dd_now = _aggregate_dealer_delta(BOOK, spot, 0.0, 0.0, R, 0.0)
    dd_scn = _aggregate_dealer_delta(BOOK, spot_scn, days, vol, R, 0.0)
    expected = -(dd_scn - dd_now) * spot_scn
    ff = dealer_hedge_flow(BOOK, spot, move, days, vol, R)
    assert math.isclose(ff.total_usd, expected, rel_tol=1e-12, abs_tol=1e-6)
    # And the components + residual reconstruct the total by definition.
    assert math.isclose(
        ff.total_usd,
        ff.gamma_component + ff.charm_component + ff.vanna_component + ff.residual,
        rel_tol=1e-9,
        abs_tol=1e-6,
    )


# --------------------------------------------------------------------------- #
# The charts (grid helpers)
# --------------------------------------------------------------------------- #
def test_forced_flow_curve_shape_and_zero_matches_level():
    curve = forced_flow_curve(BOOK, 100.0, 0.0, 0.0, R, span_pct=0.05, step_pct=0.0025)
    assert len(curve) > 10
    levels = [lvl for lvl, _ in curve]
    assert levels == sorted(levels)  # ascending
    # The zero of the curve is the zero-flow level.
    zfl = zero_flow_level(BOOK, 100.0, 0.0, R, span_pct=0.05, step_pct=0.0025)
    if zfl is not None:
        # find the curve's own nearest zero for comparison
        from src.analytics.forced_flow import _nearest_crossing

        curve_zero = _nearest_crossing([(lvl, ff.total_usd) for lvl, ff in curve], 100.0)
        assert math.isclose(zfl, curve_zero, rel_tol=1e-9)


def test_charm_into_close_steepens():
    # Near-the-money 0DTE calls: their delta is still substantial with hours to
    # go and must resolve to 0/1 at the bell, so the per-step forced flow
    # accelerates -- charm steepens into the close.
    book = [
        leg(99.5, "C", 1500, iv=0.18, dte_days=0.25),
        leg(100.0, "C", 2000, iv=0.18, dte_days=0.25),
        leg(100.5, "C", 1500, iv=0.18, dte_days=0.25),
    ]
    pts = charm_into_close(book, 100.0, 0.25, R, steps=20)
    assert pts[0][1] == 0.0  # zero time elapsed -> zero flow
    incs = [abs(pts[i + 1][1] - pts[i][1]) for i in range(len(pts) - 1)]
    assert all(math.isfinite(x) for x in incs)
    # last increment (into the close) is larger than the first (early session).
    assert incs[-1] > incs[0]
    # the flow concentrates into the back half of the session.
    assert sum(incs[len(incs) // 2 :]) > sum(incs[: len(incs) // 2])


def test_vanna_ladder_zero_at_no_change_and_monotone():
    ladder = vanna_ladder(BOOK, 100.0, R, lo_pts=-3.0, hi_pts=3.0, step_pts=0.5)
    xs = [x for x, _ in ladder]
    assert min(xs) == -3.0 and max(xs) == 3.0
    zero = [y for x, y in ladder if x == 0.0][0]
    assert zero == 0.0
    ys = [y for _, y in ladder]
    # flow is monotic in vol change (vanna sign is stable over a small range).
    diffs = [ys[i + 1] - ys[i] for i in range(len(ys) - 1)]
    assert all(d >= -1e-6 for d in diffs) or all(d <= 1e-6 for d in diffs)


# --------------------------------------------------------------------------- #
# Derived levels
# --------------------------------------------------------------------------- #
def _within_span(level, spot, span_pct):
    return level is None or (spot * (1 - span_pct) <= level <= spot * (1 + span_pct))


def test_levels_are_within_grid_or_none():
    spot, span = 100.0, 0.05
    assert _within_span(charm_flip(BOOK, spot, 0.5, R, span_pct=span), spot, span)
    assert _within_span(vanna_flip(BOOK, spot, R, span_pct=span), spot, span)
    assert _within_span(zero_flow_level(BOOK, spot, 0.25, R, span_pct=span), spot, span)


def test_zero_flow_level_actually_zeroes_the_flow():
    spot, session = 100.0, 0.0
    zfl = zero_flow_level(BOOK, spot, session, R, span_pct=0.06, step_pct=0.0025)
    if zfl is not None:
        move = zfl / spot - 1.0
        flow_at_level = dealer_hedge_flow(BOOK, spot, move, session, 0.0, R).total_usd
        assert abs(flow_at_level) < abs(
            dealer_hedge_flow(BOOK, spot, 0.05, session, 0.0, R).total_usd
        )


# --------------------------------------------------------------------------- #
# 0DTE intrinsic handling at expiry
# --------------------------------------------------------------------------- #
def test_expiry_resolves_to_intrinsic_no_nan():
    # A 0DTE call, time elapsed exactly to the bell (tte -> 0). Delta must resolve
    # to intrinsic (finite), not the degenerate 0 or NaN.
    book = [leg(100.0, "C", 1000, iv=0.20, dte_days=1)]
    ff = dealer_hedge_flow(book, 105.0, 0.0, 1.0, 0.0, R)  # ITM call at expiry
    assert math.isfinite(ff.total_usd)
    # ITM call (long by the dealer) resolves to delta 1; it was < 1 before, so
    # dealer delta rises -> dealer must SELL (negative flow).
    assert ff.total_usd < 0.0


# --------------------------------------------------------------------------- #
# Phase 6 seam
# --------------------------------------------------------------------------- #
def test_combine_flow_sources_is_additive():
    ff = ForcedFlow(
        total_usd=1_000_000.0, gamma_component=0, charm_component=0, vanna_component=0, residual=0
    )
    assert combine_flow_sources(ff) == 1_000_000.0
    assert combine_flow_sources(ff, 250_000.0) == 1_250_000.0


# --------------------------------------------------------------------------- #
# Near-expiry time-to-expiry floor (the reprice regularization)
# --------------------------------------------------------------------------- #
from src.analytics.forced_flow import _scenario_delta, flow_total  # noqa: E402

_FLOOR_30MIN = 30.0 / (365.0 * 24.0 * 60.0)


def test_min_tte_floor_is_inert_by_default_and_for_longer_dated():
    # No floor is the default; passing 0.0 must be identical.
    a = flow_total(BOOK, 100.0, 0.02, 5.0, 0.0, R)
    b = flow_total(BOOK, 100.0, 0.02, 5.0, 0.0, R, 0.0, 0.0)
    assert a == b
    # A 30-minute floor is EXACTLY inert on a book whose tte >> floor (30 DTE),
    # so the regularization only ever touches near-expiry contracts.
    c = flow_total(BOOK, 100.0, 0.02, 5.0, 0.0, R, 0.0, _FLOOR_30MIN)
    assert a == c


def test_min_tte_floor_smooths_the_expiry_step():
    # At the bell (T -> 0) with spot exactly at the strike, the unfloored
    # scenario snaps to the intrinsic 0/1 step (a knife-edge). Floored, delta
    # stays a smooth Black-Scholes value strictly inside (0, 1).
    resolved = _scenario_delta(100.0, 100.0, 0.0, R, 0.15, "C", 0.0)
    floored = _scenario_delta(100.0, 100.0, 0.0, R, 0.15, "C", 0.0, _FLOOR_30MIN)
    assert resolved == 0.0  # S is not > K -> hard 0
    assert 0.0 < floored < 1.0  # regularized -> no step
