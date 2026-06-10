from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock

import numpy as np
import pytest
from scipy import stats

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine


def _opt(strike, otype, *, oi=1000, iv=0.20, exp=None, gamma=0.0, volume=0):
    """Minimal option-chain row for the spot-shift gamma profile."""
    return {
        "strike": strike,
        "option_type": otype,
        "open_interest": oi,
        "implied_volatility": iv,
        "expiration": exp,
        "gamma": gamma,
        "volume": volume,
    }


def test_gex_by_strike_weights_gamma_by_open_interest():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 3, 27, 15, 55, tzinfo=timezone.utc)

    # Two call rows at same strike/expiry simulate split snapshots/contracts.
    options = [
        {
            "strike": 500.0,
            "expiration": ts.date(),
            "option_type": "C",
            "gamma": 0.01,
            "open_interest": 10,
            "volume": 1,
            "implied_volatility": 0.2,
        },
        {
            "strike": 500.0,
            "expiration": ts.date(),
            "option_type": "C",
            "gamma": 0.02,
            "open_interest": 20,
            "volume": 1,
            "implied_volatility": 0.2,
        },
    ]

    result = engine._calculate_gex_by_strike(options, underlying_price=500.0, timestamp=ts)
    row = result[0]

    expected_weighted_gamma = (0.01 * 10) + (0.02 * 20)
    # Industry-standard dollar GEX per 1% move: γ × OI × 100 × S² × 0.01.
    expected_call_gex = expected_weighted_gamma * 100 * 500.0 * 500.0 * 0.01

    assert row["call_gamma"] == expected_weighted_gamma
    assert row["net_gex"] == expected_call_gex


def test_max_pain_minimizes_total_intrinsic_payout():
    engine = AnalyticsEngine(underlying="SPY")
    options = [
        {
            "strike": 100.0,
            "option_type": "C",
            "open_interest": 100,
            "volume": 0,
            "gamma": 0,
            "expiration": datetime(2026, 3, 27).date(),
            "implied_volatility": 0.2,
        },
        {
            "strike": 110.0,
            "option_type": "P",
            "open_interest": 100,
            "volume": 0,
            "gamma": 0,
            "expiration": datetime(2026, 3, 27).date(),
            "implied_volatility": 0.2,
        },
    ]
    # Settlement @100 => put payout 1000; @110 => call payout 1000;
    # tie picks the lower strike via the sort.
    assert engine._calculate_max_pain(options) == 100.0


def test_bs_gamma_matches_closed_form_and_degenerates():
    engine = AnalyticsEngine(underlying="SPY")
    S, K, T, r, sigma = 100.0, 100.0, 0.5, 0.05, 0.2
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    expected = stats.norm.pdf(d1) / (S * sigma * np.sqrt(T))
    assert abs(engine._calculate_bs_gamma(S, K, T, r, sigma) - expected) < 1e-12

    # Vectorised over a price grid == element-wise scalar calls.
    grid = np.array([80.0, 100.0, 130.0])
    arr = engine._calculate_bs_gamma(grid, K, T, r, sigma)
    assert isinstance(arr, np.ndarray)
    for s, g in zip(grid, arr):
        assert abs(float(g) - engine._calculate_bs_gamma(float(s), K, T, r, sigma)) < 1e-12
    assert arr[1] > arr[0] and arr[1] > arr[2]  # gamma peaks near ATM

    # Degenerate inputs => 0 / zeros, never NaN.
    assert engine._calculate_bs_gamma(100.0, 100.0, 0.0, r, sigma) == 0.0
    assert engine._calculate_bs_gamma(100.0, 100.0, T, r, 0.0) == 0.0
    assert list(engine._calculate_bs_gamma(grid, K, -1.0, r, sigma)) == [0.0, 0.0, 0.0]


def test_gamma_profile_resolves_interior_flip_and_is_sign_consistent():
    """Put mass below, call mass above => the spot-shift profile is short
    gamma near the puts and long near the calls, so it has a genuine
    interior zero crossing (the old cumulative-by-strike curve, given only
    these strikes, was one-signed => None => carry-forward freeze)."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 100.0
    options = [
        _opt(92.0, "P", oi=8000, iv=0.30, exp=exp),
        _opt(112.0, "C", oi=8000, iv=0.30, exp=exp),
    ]
    profile = engine._gamma_exposure_profile(options, spot, ts)
    assert profile
    xs = [s for s, _ in profile]
    assert xs == sorted(xs)
    assert xs[0] <= spot * 0.80 + 1e-6 and xs[-1] >= spot * 1.20 - 1e-6
    # Negative near the put strike, positive near the call strike =>
    # exactly one interior crossing.
    assert profile[0][1] < 0 < profile[-1][1]
    flip = engine._calculate_gamma_flip_point(profile, spot)
    assert flip is not None
    assert 92.0 < flip < 112.0  # between the put and the call

    # The core invariant (independent of which side spot lands on): the
    # profile is short gamma strictly below the flip and long strictly
    # above it, and net_gex_at_spot's sign tracks the spot-vs-flip side.
    d = spot * 0.01
    assert engine._net_gex_at_spot(profile, flip - d) < 0
    assert engine._net_gex_at_spot(profile, flip + d) > 0
    assert (engine._net_gex_at_spot(profile, spot) < 0) == (spot < flip)


def test_gamma_flip_none_when_profile_one_signed():
    """A pure long-call book is dealer-long-gamma at every grid price (no
    crossing). The flip is NOT clamped to a grid edge — it returns None so
    the caller can mark it unresolved (degraded/one-sided chain) instead
    of fabricating a level or letting the carry-forward re-freeze."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 100.0
    profile = engine._gamma_exposure_profile(
        [_opt(100.0, "C", oi=5000, iv=0.25, exp=exp)], spot, ts
    )
    assert all(v > 0 for _, v in profile)  # long gamma everywhere
    assert engine._calculate_gamma_flip_point(profile, spot) is None


def test_gamma_profile_none_when_no_usable_contracts():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    assert engine._gamma_exposure_profile([], 100.0, ts) == []
    assert engine._gamma_exposure_profile([_opt(100.0, "C", exp=exp)], 0.0, ts) == []
    # σ<=0 and OI<=0 contracts are skipped => no usable contracts.
    bad = [
        _opt(100.0, "C", oi=0, exp=exp),
        _opt(100.0, "P", iv=0.0, exp=exp),
    ]
    assert engine._gamma_exposure_profile(bad, 100.0, ts) == []
    assert engine._calculate_gamma_flip_point([], 100.0) is None
    assert engine._net_gex_at_spot([], 100.0) is None


def _brute_force_max_pain(options, strike_range=None):
    """Reference max-pain: the original strikes×options double loop. The
    vectorized _calculate_max_pain must agree with this exactly, including the
    lowest-strike tie-break."""
    strikes = sorted(set(o["strike"] for o in options))
    if strike_range:
        strikes = [s for s in strikes if strike_range[0] <= s <= strike_range[1]]
    if not strikes:
        return None
    payouts = {}
    for test in strikes:
        total = 0.0
        for o in options:
            if o["open_interest"] == 0:
                continue
            k = o["strike"]
            oi = o["open_interest"]
            if o["option_type"] == "C":
                if test > k:
                    total += (test - k) * oi * 100
            else:
                if test < k:
                    total += (k - test) * oi * 100
        payouts[test] = total
    return min(payouts.items(), key=lambda x: x[1])[0]


def test_max_pain_vectorized_matches_brute_force_and_tiebreak():
    """The vectorized _calculate_max_pain reproduces the original double-loop
    exactly across a randomized multi-strike chain, an empty set, an all-zero-OI
    set (every payout 0 => lowest strike wins the tie), and a strike_range."""
    engine = AnalyticsEngine(underlying="SPY")
    rng = np.random.default_rng(20260529)
    exp = datetime(2026, 6, 19).date()
    opts = [
        _opt(float(k), otype, oi=int(rng.integers(0, 9000)), exp=exp)
        for k in range(80, 121)
        for otype in ("C", "P")
    ]
    assert engine._calculate_max_pain(opts) == _brute_force_max_pain(opts)
    assert engine._calculate_max_pain(opts, strike_range=(95.0, 105.0)) == _brute_force_max_pain(
        opts, strike_range=(95.0, 105.0)
    )
    assert engine._calculate_max_pain([]) is None
    # All payouts identically zero -> argmin picks the first (lowest) strike.
    zero_oi = [_opt(float(k), "C", oi=0, exp=exp) for k in range(80, 121)]
    assert engine._calculate_max_pain(zero_oi) == 80.0


def test_bs_gamma_inline_pdf_matches_scipy():
    """The inline standard-normal pdf in _calculate_bs_gamma is bit-identical to
    scipy.stats.norm.pdf (the swap was a hot-path speedup, not a model change)."""
    engine = AnalyticsEngine(underlying="SPY")
    grid = np.linspace(60.0, 140.0, 161)
    for K, T, sigma in [(100.0, 0.5, 0.20), (95.0, 0.05, 0.45), (120.0, 1.0, 0.15)]:
        got = engine._calculate_bs_gamma(grid, K, T, 0.05, sigma)
        sqrt_T = np.sqrt(T)
        d1 = (np.log(grid / K) + (0.05 + 0.5 * sigma**2) * T) / (sigma * sqrt_T)
        expected = stats.norm.pdf(d1) / (grid * sigma * sqrt_T)
        assert np.allclose(got, expected, rtol=0, atol=0)  # exact


def test_net_gex_at_spot_interpolates_and_clamps_generic_curve():
    """_net_gex_at_spot piecewise-linearly samples the profile and clamps
    to its endpoints outside the grid."""
    engine = AnalyticsEngine(underlying="SPY")
    profile = [(100.0, -14.0), (105.0, -14.0), (110.0, 6.0)]
    # Between 105 (-14) and 110 (+6): -14 + 20*(107-105)/5 = -6.
    assert abs(engine._net_gex_at_spot(profile, 107.0) - (-6.0)) < 1e-9
    assert abs(engine._net_gex_at_spot(profile, 109.0) - 2.0) < 1e-9
    assert engine._net_gex_at_spot(profile, 90.0) == -14.0  # clamp low edge
    assert engine._net_gex_at_spot(profile, 120.0) == 6.0  # clamp high edge


def test_dte_profile_weight_is_horizon_occupancy_ramp(monkeypatch):
    """min(1, DTE / ref_days): a linear horizon-occupancy ramp — the
    fraction of the reference horizon over which the contract still
    exists. 1.0 at/beyond the reference horizon, linearly less below it,
    0 for non-positive DTE, and a hard 1.0 when weighting is disabled."""
    engine = AnalyticsEngine(underlying="SPY")
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_WEIGHTING", True)
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_REF_DAYS", 5.0)
    day = 1.0 / 365.0  # one calendar day, in years (T's unit)

    assert engine._dte_profile_weight(5.0 * day) == 1.0  # exactly at ref
    assert engine._dte_profile_weight(30.0 * day) == 1.0  # saturates beyond ref
    assert abs(engine._dte_profile_weight(1.0 * day) - 0.2) < 1e-12  # 1/5
    assert abs(engine._dte_profile_weight(2.5 * day) - 0.5) < 1e-12  # 2.5/5
    assert engine._dte_profile_weight(0.0) == 0.0
    assert engine._dte_profile_weight(-1.0) == 0.0

    # Linear in DTE below ref => the weight halves when DTE halves.
    assert (
        abs(engine._dte_profile_weight(1.0 * day) - 2.0 * engine._dte_profile_weight(0.5 * day))
        < 1e-12
    )

    # Disabled => identically 1.0 regardless of (even degenerate) DTE.
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_WEIGHTING", False)
    assert engine._dte_profile_weight(1e-9) == 1.0
    assert engine._dte_profile_weight(0.0) == 1.0
    assert engine._dte_profile_weight(10.0) == 1.0


def test_dte_profile_weight_shapes_at_key_points(monkeypatch):
    """The three curve shapes (linear | sqrt | exp) at a few canonical
    DTE points.  Sanity: each shape returns the documented closed-form
    value within a small tolerance, all three send w → 0 as DTE → 0 (so
    the 0DTE-pinning bug stays solved), and the module constant for the
    shape is correctly overridden by the per-call ``shape`` kwarg
    without leaking back into the module."""
    engine = AnalyticsEngine(underlying="SPY")
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_WEIGHTING", True)
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_REF_DAYS", 5.0)
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_WEIGHT_SHAPE", "linear")
    day = 1.0 / 365.0

    # Linear (module default) — already covered above; spot-check ref point.
    assert engine._dte_profile_weight(5.0 * day) == 1.0
    assert abs(engine._dte_profile_weight(2.0 * day) - 0.4) < 1e-12

    # Sqrt — per-call override.  At ref days, saturates to 1.0; below,
    # equals √(DTE/ref).
    assert abs(engine._dte_profile_weight(5.0 * day, shape="sqrt") - 1.0) < 1e-12
    assert abs(engine._dte_profile_weight(2.0 * day, shape="sqrt") - (0.4**0.5)) < 1e-9
    assert abs(engine._dte_profile_weight(1.0 * day, shape="sqrt") - (0.2**0.5)) < 1e-9
    assert engine._dte_profile_weight(0.0, shape="sqrt") == 0.0
    # Sqrt is always >= linear below the reference (more weight on
    # near-dated).  At DTE = 1, sqrt ≈ 0.447 > linear = 0.20.
    assert engine._dte_profile_weight(1.0 * day, shape="sqrt") > engine._dte_profile_weight(
        1.0 * day, shape="linear"
    )

    # Exp — asymptotic saturation.  At DTE=ref, w = 1 - 1/e ≈ 0.632.
    assert abs(engine._dte_profile_weight(5.0 * day, shape="exp") - (1.0 - np.exp(-1.0))) < 1e-9
    # Near zero, exp ≈ linear (Taylor: 1 - exp(-x) ≈ x for small x).
    near_zero = 0.01 * day
    exp_v = engine._dte_profile_weight(near_zero, shape="exp")
    lin_v = engine._dte_profile_weight(near_zero, shape="linear")
    assert abs(exp_v - lin_v) / max(lin_v, 1e-12) < 0.01
    assert engine._dte_profile_weight(0.0, shape="exp") == 0.0
    # Exp saturates asymptotically — at DTE=5*ref it's ~0.993 (well
    # under linear's hard 1.0 cap at the same point).  Past DTE ~= 35*ref
    # the float64 representation of 1 - exp(-x) collapses to 1.0 by
    # underflow; the asymptotic property is about the early band where
    # the difference vs linear is actually observable.
    assert 0.99 < engine._dte_profile_weight(25.0 * day, shape="exp") < 1.0
    assert engine._dte_profile_weight(25.0 * day, shape="linear") == 1.0

    # Invalid shape falls back to linear silently (the warn fires once at
    # module load; per-call invalid values just use linear).
    assert engine._dte_profile_weight(2.0 * day, shape="bogus") == engine._dte_profile_weight(
        2.0 * day, shape="linear"
    )

    # Disabled => identically 1.0 regardless of shape.
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_WEIGHTING", False)
    for s in ("linear", "sqrt", "exp"):
        assert engine._dte_profile_weight(1e-9, shape=s) == 1.0
        assert engine._dte_profile_weight(0.0, shape=s) == 1.0
        assert engine._dte_profile_weight(10.0, shape=s) == 1.0


def test_dte_weighting_unpins_flip_from_0dte_wall(monkeypatch):
    """0DTE-heavy book: a same-day put wall just below spot, plus the
    real multi-day regime structure (far-dated put/call mass) whose
    zero-gamma level sits well below spot (~82 here).

    The spot-shift rewrite alone does NOT tame this — re-greeking *adds*
    a razor 1/√T gamma spike at the same-day strike, so with DTE
    weighting off the nearest-to-spot crossing is pinned to the 0DTE
    wall (~99), the original 751.82-vs-spot pathology. With weighting on
    (the production default) the same-day expiry is horizon-occupancy
    down-weighted out of contention and the flip resolves to the
    multi-day regime level (~82) instead — the two differ by ~17pts."""
    engine = AnalyticsEngine(underlying="SPY")
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_WEIGHTING", True)
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_REF_DAYS", 5.0)

    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)  # ~5h to the 0DTE close
    zero_dte = ts.date()
    far = datetime(2026, 10, 16).date()  # ~5 months out -> weight 1.0
    spot = 100.0

    options = [
        # Multi-day regime structure (heavy far-dated put mass low, even
        # heavier call mass above) -> zero-gamma level sits down at ~82.
        _opt(80.0, "P", oi=180000, iv=0.30, exp=far),
        _opt(110.0, "C", oi=420000, iv=0.30, exp=far),
        # Same-day 0DTE put wall just below spot — irrelevant to any
        # multi-day horizon, but with a colossal re-greeked 1/√T spike.
        _opt(98.0, "P", oi=50000, iv=0.20, exp=zero_dte),
    ]

    weighted_flip = engine._calculate_gamma_flip_point(
        engine._gamma_exposure_profile(options, spot, ts), spot
    )

    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_WEIGHTING", False)
    unweighted_flip = engine._calculate_gamma_flip_point(
        engine._gamma_exposure_profile(options, spot, ts), spot
    )

    assert weighted_flip is not None and unweighted_flip is not None
    # Weighting off: the 0DTE wall's 1/√T spike pins the flip to itself.
    assert abs(unweighted_flip - 98.0) <= 2.5
    # Weighting on: the flip is the multi-day regime level, well clear of
    # the same-day wall (down at ~82 here).
    assert weighted_flip <= 90.0
    assert abs(weighted_flip - 98.0) >= 6.0
    # ...and the two readings differ materially (>10% of spot here).
    assert abs(weighted_flip - unweighted_flip) >= 0.10 * spot


def test_dte_ref_days_override_reshapes_profile_per_horizon(monkeypatch):
    """Override semantics — per-call ``dte_ref_days`` substitutes for the
    module-level constant for that one profile build only, without
    mutating module state.

    Verified at the profile level (rather than the resolved flip) so the
    test isn't entangled with the resolver's interior / structural /
    actionable-distance gates: a near-dated wall whose weight is 1.0 at
    ref=2 but only 0.1 at ref=20 contributes 10× more dollar gamma to
    the grid point at its strike under the short ref.  That contribution
    is on top of an asymmetric base profile, so the *sign at spot*
    flips with horizon — a directly observable, gate-free witness that
    the override took effect."""
    engine = AnalyticsEngine(underlying="SPY")
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_WEIGHTING", True)
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_REF_DAYS", 5.0)

    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    near = datetime(2026, 5, 20).date()  # 2 DTE
    far = datetime(2026, 10, 16).date()  # ~5 months, weight 1.0 at any reasonable ref
    spot = 100.0

    # Single 2DTE-only contract dominates the profile at spot.  At ref=2
    # it counts at weight 1.0; at ref=20 it counts at weight 0.1.  The
    # contribution at every grid point — including spot — is therefore
    # exactly 10× larger in magnitude under the short-ref profile.
    # Asserting the magnitude ratio is approximately the weight ratio is
    # the cleanest, gate-free witness that the override took effect.
    options = [
        _opt(100.0, "P", oi=200000, iv=0.20, exp=near),
        _opt(120.0, "C", oi=12000, iv=0.30, exp=far),
    ]

    profile_short = engine._gamma_exposure_profile(options, spot, ts, dte_ref_days=2.0)
    profile_long = engine._gamma_exposure_profile(options, spot, ts, dte_ref_days=20.0)

    # Both profiles built over the same grid (same span_pct default).
    assert profile_short and profile_long
    assert [p[0] for p in profile_short] == [p[0] for p in profile_long]

    # Value at the grid point closest to spot.
    def _at_spot(prof):
        idx = min(range(len(prof)), key=lambda i: abs(prof[i][0] - spot))
        return prof[idx][1]

    v_short = _at_spot(profile_short)
    v_long = _at_spot(profile_long)

    # Short-ref weights the near-dated wall ~10× more heavily; the
    # at-spot magnitude reflects that.  The far-dated contract gets
    # weight 1.0 under both refs (longer than either reference) so it
    # cancels out of the ratio.
    assert abs(v_short) > 4.0 * abs(v_long), (v_short, v_long)

    # Module state unchanged by the override calls.
    assert main_engine.GAMMA_PROFILE_DTE_REF_DAYS == 5.0


def test_compute_flip_term_structure_shape_and_alignment(monkeypatch):
    """The public multi-horizon method returns one entry per requested
    horizon in the requested order, with the documented keys.  Invalid /
    non-positive horizons are skipped silently (so a client passing
    ``"0,5"`` gets one row, not a 500)."""
    engine = AnalyticsEngine(underlying="SPY")
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_WEIGHTING", True)
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_REF_DAYS", 5.0)

    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    far = datetime(2026, 10, 16).date()
    spot = 100.0
    options = [
        _opt(80.0, "P", oi=180000, iv=0.30, exp=far),
        _opt(110.0, "C", oi=420000, iv=0.30, exp=far),
        _opt(98.0, "P", oi=50000, iv=0.20, exp=datetime(2026, 5, 20).date()),
    ]

    results = engine.compute_flip_term_structure(options, spot, ts, [0.0, 2.0, -1.0, 5.0, 20.0])
    # 0 and -1 are dropped; 2, 5, 20 survive.
    assert [r["horizon_days"] for r in results] == [2.0, 5.0, 20.0]
    for r in results:
        assert set(r.keys()) == {
            "horizon_days",
            "flip",
            "resolved",
            "span_used",
            "net_gex_at_spot",
        }
        assert isinstance(r["resolved"], bool)
        if r["resolved"]:
            assert r["flip"] is not None
            assert r["net_gex_at_spot"] is not None


def test_compute_flip_surface_shared_grid_and_shape(monkeypatch):
    """The surface method's hard contract: every horizon's profile is
    aligned to a single shared grid (len(profiles[i]) == len(grid)
    for every i), the grid is strictly ascending, walls are returned
    when requested, and the resolved-flip rows match the term-structure
    method's per-horizon output."""
    engine = AnalyticsEngine(underlying="SPY")
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_WEIGHTING", True)
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_DTE_REF_DAYS", 5.0)

    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    far = datetime(2026, 10, 16).date()
    spot = 100.0
    # gamma is the snapshot per-contract gamma the walls helper reads
    # (production rows are populated by the BS calculator upstream);
    # set a small positive value so _calculate_gex_by_strike produces
    # the call_gamma / put_gamma columns the wall picker scans.
    options = [
        _opt(92.0, "P", oi=180000, iv=0.30, exp=far, gamma=0.01),
        _opt(108.0, "C", oi=420000, iv=0.30, exp=far, gamma=0.01),
        _opt(98.0, "P", oi=50000, iv=0.20, exp=datetime(2026, 5, 20).date(), gamma=0.01),
    ]

    surface = engine.compute_flip_surface(
        options,
        spot,
        ts,
        [2.0, 5.0, 20.0],
        span_pct=0.10,  # narrower to keep grid small for the test
        step_pct=0.005,
        include_walls=True,
    )

    # Top-level shape: documented keys present.
    assert set(surface.keys()) == {"grid", "horizons_days", "profiles", "flips", "walls"}

    # Grid: strictly ascending, brackets spot, all positive.
    grid = surface["grid"]
    assert grid and all(grid[i] < grid[i + 1] for i in range(len(grid) - 1))
    assert grid[0] < spot < grid[-1]
    assert all(g > 0 for g in grid)

    # Profiles: rectangular array — every row matches len(grid).
    assert len(surface["profiles"]) == len(surface["horizons_days"]) == 3
    for row in surface["profiles"]:
        assert len(row) == len(grid)

    # Flips: one entry per horizon with the documented keys.
    assert len(surface["flips"]) == 3
    for r in surface["flips"]:
        assert set(r.keys()) == {
            "horizon_days",
            "flip",
            "resolved",
            "span_used",
            "net_gex_at_spot",
        }

    # Cross-check vs the term-structure method: the resolver's flip per
    # horizon must match between the two endpoints (both call
    # _resolve_gamma_flip with the same dte_ref_days).
    ts_result = engine.compute_flip_term_structure(options, spot, ts, [2.0, 5.0, 20.0])
    for s_row, t_row in zip(surface["flips"], ts_result):
        assert s_row["horizon_days"] == t_row["horizon_days"]
        assert s_row["flip"] == t_row["flip"]
        assert s_row["resolved"] == t_row["resolved"]

    # Walls: 92P and 108C are the dominant near-spot strikes; we should
    # get exactly one call wall (108) and one put wall (92).
    assert len(surface["walls"]) == 2
    by_type = {w["type"]: w for w in surface["walls"]}
    assert {"call", "put"} == set(by_type.keys())
    assert by_type["call"]["strike"] == 108.0
    assert by_type["put"]["strike"] == 92.0
    assert by_type["call"]["abs_dollar_gex"] > 0
    assert by_type["put"]["abs_dollar_gex"] > 0

    # include_walls=False returns an empty walls list.
    surface_no_walls = engine.compute_flip_surface(
        options, spot, ts, [5.0], span_pct=0.10, step_pct=0.005, include_walls=False
    )
    assert surface_no_walls["walls"] == []


def test_find_structural_interior_crossing_geometry():
    """Unit test for the interior + structural gates on a hand-built profile.

    With the default 10% interior margin, a sign change in the last grid
    cell is rejected (edge → expand the grid).  With a structural floor
    that demands non-trivial magnitude, a sign change in the noise floor
    is rejected (noise → expand the grid).  An interior sign change
    surrounded by structurally-significant magnitude is accepted, and
    among multiple qualifiers the one nearest spot wins.
    """
    engine = AnalyticsEngine(underlying="SPY")
    # Grid 80..120, width 40.  At 10% margin, interior is (84, 116).
    # Sign change in the LAST cell (118->120) is at the edge => reject.
    profile_edge = [(s, -1.0) for s in range(80, 119)] + [(119.0, -1.0), (120.0, 5.0)]
    assert engine._find_structural_interior_crossing(profile_edge, 100.0) is None

    # Sign change in the FIRST cell (80->82): below interior_lo => reject.
    profile_edge_lo = [(80.0, 5.0), (82.0, -1.0)] + [(s, -1.0) for s in range(84, 121)]
    assert engine._find_structural_interior_crossing(profile_edge_lo, 100.0) is None

    # Interior crossing at ~100, surrounded by large magnitudes both sides
    # (|profile| peak ~ 1000): structural gate satisfied.
    profile_clean = [
        (90.0, -1000.0),
        (95.0, -800.0),
        (99.0, -100.0),
        (101.0, 100.0),
        (105.0, 800.0),
        (110.0, 1000.0),
    ]
    flip = engine._find_structural_interior_crossing(profile_clean, 100.0)
    assert flip is not None and abs(flip - 100.0) < 0.5

    # Same crossing geometry, but the surrounding magnitudes are in the
    # noise floor (peak |profile| dominated by a far-edge value).  The
    # structural gate (default 2% of peak) rejects.
    profile_noisy = [
        (90.0, -10_000.0),  # dominates peak; sets noise floor at 0.02 * 10_000 = 200
        (95.0, -50.0),
        (99.0, -1.0),
        (101.0, 1.0),
        (105.0, 50.0),
        (110.0, 100.0),  # window peak around 101 is ~50, well below 200
    ]
    assert engine._find_structural_interior_crossing(profile_noisy, 100.0) is None

    # Two qualifying interior crossings.  Bracketing pairs are placed
    # tightly enough that BOTH grid points fall inside the structural
    # window (±1% of the candidate crossing, the default), so the
    # structural gate sees the bracket's magnitude rather than zero:
    #   (92.5, -100) → (93.5, +100) crosses at 93.0
    #   (104, +1000) → (106, -1000) crosses at 105.0 (and (105, 0)
    #   also sits in the structural window of 105)
    profile_multi = [
        (88.0, -1000.0),
        (92.5, -100.0),
        (93.5, 100.0),  # crossing at 93
        (96.0, 1000.0),
        (104.0, 1000.0),
        (105.0, 0.0),
        (106.0, -1000.0),  # crossing at 105
        (110.0, -1000.0),
        (112.0, -1000.0),
    ]
    # Spot 100 — both crossings interior, 105 is nearer (dist 5 vs 7).
    flip = engine._find_structural_interior_crossing(profile_multi, 100.0)
    assert flip is not None and abs(flip - 105.0) < 0.5
    # Bias spot toward each side to prove the "nearest to spot" tie-break.
    flip_low = engine._find_structural_interior_crossing(profile_multi, 92.0)
    assert flip_low is not None and abs(flip_low - 93.0) < 0.5
    flip_high = engine._find_structural_interior_crossing(profile_multi, 108.0)
    assert flip_high is not None and abs(flip_high - 105.0) < 0.5


def test_resolve_gamma_flip_uses_first_rung_when_interior_crossing_exists(monkeypatch):
    """Default case: the flip is interior on the initial ±20% rung.
    The resolver returns that rung's profile + crossing without
    expanding — no wasted work on a regime where the smaller grid
    already resolves cleanly.
    """
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 100.0
    options = [
        # Heavy put mass clearly below, call mass clearly above => interior
        # crossing right in the middle on the first rung.
        _opt(92.0, "P", oi=8000, iv=0.30, exp=exp),
        _opt(112.0, "C", oi=8000, iv=0.30, exp=exp),
    ]
    profile, flip, span_used = engine._resolve_gamma_flip(options, spot, ts)
    assert flip is not None and 92.0 < flip < 112.0
    # First rung used (no expansion).
    assert abs(span_used - main_engine.GAMMA_PROFILE_SPAN_LADDER[0]) < 1e-9
    # Profile extends across only the first rung's span.
    assert profile[0][0] >= spot * (1.0 - span_used) - 1e-6
    assert profile[-1][0] <= spot * (1.0 + span_used) + 1e-6


def test_resolve_gamma_flip_expands_grid_for_wide_real_flip(monkeypatch):
    """Genuinely wide flip case: at the initial ±20% rung the profile is
    one-signed (no crossing at all), so the first rung's resolver yields
    None.  The ladder steps up and resolves the real interior crossing
    on a wider rung.  This is exactly the deep-short-gamma regime where
    the previous code would have NULL+WARN'd a resolvable flip.

    Chain: ATM put + deep-OTM call (K = +60% above spot).  Below ~+25%
    spot move, the ATM put dominates dealer dollar gamma (calls there are
    far OTM, gamma small); above ~+25% the OTM call's gamma climbs faster
    than the put's drops.  Crossing lands around +25% — outside ±20%,
    inside ±35%.

    The actionable-distance gate (default 8% of spot) is disabled here
    so the resolver is exercised on its pre-gate semantics; a separate
    test asserts the gate behavior.
    """
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT", 1.0)
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 100.0
    options = [
        _opt(100.0, "P", oi=200_000, iv=0.30, exp=exp),
        _opt(160.0, "C", oi=200_000, iv=0.30, exp=exp),
    ]
    # Premise: first rung yields no structural interior crossing.
    first_rung = main_engine.GAMMA_PROFILE_SPAN_LADDER[0]
    first_rung_profile = engine._gamma_exposure_profile(options, spot, ts, span_pct=first_rung)
    assert engine._find_structural_interior_crossing(first_rung_profile, spot) is None

    profile, flip, span_used = engine._resolve_gamma_flip(options, spot, ts)
    assert flip is not None
    # The crossing sits outside the first rung's window — that's the point.
    assert flip > spot * (1.0 + first_rung)
    assert span_used > first_rung
    assert profile[0][0] < flip < profile[-1][0]
    # And the resolver still produced a sign-consistent net_gex_at_spot.
    n_at_spot = engine._net_gex_at_spot(profile, spot)
    assert n_at_spot is not None and (n_at_spot < 0) == (spot < flip)


def test_resolve_gamma_flip_rejects_crossing_beyond_distance_gate(monkeypatch):
    """Actionable-distance gate: even when a structurally valid interior
    crossing exists on a wider rung, the resolver rejects it when it
    sits further from spot than ``GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT``.

    Uses the same put/deep-OTM-call chain as the "wide real flip" test
    above (crossing at ~+25% of spot) and sets the distance gate to 10%.
    The far-from-spot crossing is rejected at every rung, so the
    resolver returns ``(last_profile, None, last_span)`` — the honest
    "not actionable on any reasonable trading horizon" signal that
    makes both the dashboard and the heatmap go NULL together instead
    of one going N/A and the other walking the line off the chart.
    """
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT", 0.10)
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 100.0
    options = [
        _opt(100.0, "P", oi=200_000, iv=0.30, exp=exp),
        _opt(160.0, "C", oi=200_000, iv=0.30, exp=exp),
    ]
    profile, flip, span_used = engine._resolve_gamma_flip(options, spot, ts)
    assert flip is None
    # The widest rung was reached because every rung's only candidate
    # crossing was outside the actionable-distance window.
    assert abs(span_used - main_engine.GAMMA_PROFILE_SPAN_LADDER[-1]) < 1e-9
    # Profile is still built on the last rung (net_gex_at_spot reads off it).
    assert profile


def test_gamma_flip_unresolved_diagnostics_surfaces_each_failure_mode():
    """The diagnostic helper must surface the four documented failure
    modes from the inputs alone, so the WARN log line is enough to
    diagnose without cross-referencing the option_chains snapshot.

    Exercised here on hand-built chains, one per mode, asserting only
    the field that mode is meant to flag.
    """
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    multi_day_exp = datetime(2026, 9, 18).date()
    zero_dte_exp = ts.date()
    spot = 100.0

    # (1) IV-spike artifact: every contract priced at high σ.  Median /
    # p90 / max should all sit near the elevated level, well above a
    # normal regime (~0.20-0.25).
    spike_options = [
        _opt(95.0, "P", oi=5000, iv=0.85, exp=multi_day_exp),
        _opt(100.0, "P", oi=5000, iv=0.90, exp=multi_day_exp),
        _opt(105.0, "C", oi=5000, iv=0.88, exp=multi_day_exp),
    ]
    spike_profile = engine._gamma_exposure_profile(spike_options, spot, ts)
    spike_diag = engine._gamma_flip_unresolved_diagnostics(spike_options, spike_profile, spot, ts)
    assert spike_diag["iv_p50"] >= 0.80
    assert spike_diag["iv_max"] >= 0.88
    # IVs aren't the default sentinel — should NOT trip the stale-IV bucket.
    assert spike_diag["iv_at_default_share"] == 0.0

    # (2) 0DTE-dominant chain × DTE weighting: raw OI share heavy in
    # 0DTE, but the DTE-weighted share collapses (0DTE weighted ~0,
    # short-dated weighted small), exposing the weighting failure mode.
    zero_dte_options = [
        _opt(100.0, "C", oi=50_000, iv=0.30, exp=zero_dte_exp),
        _opt(100.0, "P", oi=50_000, iv=0.30, exp=zero_dte_exp),
        _opt(100.0, "C", oi=1_000, iv=0.30, exp=multi_day_exp),
    ]
    zdte_profile = engine._gamma_exposure_profile(zero_dte_options, spot, ts)
    zdte_diag = engine._gamma_flip_unresolved_diagnostics(zero_dte_options, zdte_profile, spot, ts)
    # Raw OI is overwhelmingly 0DTE.
    assert zdte_diag["oi_share_0dte"] >= 0.95
    # After DTE weighting, the 0DTE share is meaningfully smaller than
    # its raw share — the weighting is doing its job (and the
    # remaining weighted OI from far-dated contracts is what the
    # resolver actually sees).
    assert zdte_diag["weighted_oi_share_0dte"] < zdte_diag["oi_share_0dte"]

    # (3) Stale-IV pipeline: NULL/0 IVs in the source row are filled
    # with 0.20 at fetch time (see snapshot loader around line 503),
    # so the sigma>0 filter doesn't catch them.  Diagnostic flags the
    # exact-default cluster.
    stale_options = [
        _opt(95.0, "P", oi=5000, iv=0.20, exp=multi_day_exp),
        _opt(100.0, "P", oi=5000, iv=0.20, exp=multi_day_exp),
        _opt(105.0, "C", oi=5000, iv=0.20, exp=multi_day_exp),
        _opt(110.0, "C", oi=5000, iv=0.35, exp=multi_day_exp),  # only 1 real IV
    ]
    stale_profile = engine._gamma_exposure_profile(stale_options, spot, ts)
    stale_diag = engine._gamma_flip_unresolved_diagnostics(stale_options, stale_profile, spot, ts)
    assert stale_diag["iv_at_default_count"] == 3
    assert abs(stale_diag["iv_at_default_share"] - 0.75) < 1e-9

    # (4) One-sided chain: all calls (or all puts) on the usable side.
    # Diagnostic call/put counts are skewed, and the last-rung profile
    # is monotonic (one of pos_pts / neg_pts is zero).
    one_sided_options = [
        _opt(100.0, "C", oi=5000, iv=0.25, exp=multi_day_exp),
        _opt(105.0, "C", oi=5000, iv=0.25, exp=multi_day_exp),
    ]
    one_sided_profile = engine._gamma_exposure_profile(one_sided_options, spot, ts)
    one_sided_diag = engine._gamma_flip_unresolved_diagnostics(
        one_sided_options, one_sided_profile, spot, ts
    )
    assert one_sided_diag["usable_calls"] == 2
    assert one_sided_diag["usable_puts"] == 0
    # Pure long-call book is dealer-long-gamma everywhere => negative-side
    # is empty, positive-side has every point.
    assert one_sided_diag["profile_neg_pts"] == 0
    assert one_sided_diag["profile_pos_pts"] > 0


def test_find_structural_interior_crossing_accepts_real_crossing_in_spike_shaped_profile():
    """SPX 2026-05-20 pathology: low-IV, OI-concentrated chain produces
    a dealer-gamma profile dominated by ONE colossal spike (e.g., a
    near-ATM wall whose narrow BS gammas pile up at one strike) with
    the rest of the profile at noise-floor magnitudes.

    Under the prior max-relative gate the threshold was driven by the
    spike (floor = 2% × 7.5B = 150M) and every legitimate interior
    crossing in the rest of the chain was rejected as "noise relative
    to the spike", yielding NULL on a healthy chain.  Under the
    robust-percentile reference the spike is an outlier above p90, so
    the floor is set by the chain's typical high magnitude and the
    real crossing resolves.

    The profile here is a hand-shaped synthetic that captures both
    features: a tall narrow spike well above spot, a clear interior
    sign change near spot in a region of moderate magnitude, and a
    long stretch of noise-floor magnitudes filling out the grid.
    """
    engine = AnalyticsEngine(underlying="SPX")
    spot = 100.0

    profile = []
    # 50 noise-floor points below the interior region (1% from edge to
    # 10% margin).  Profile is in the negative noise floor here.
    profile.extend((50.0 + 0.5 * i, -1e-3) for i in range(50))
    # Active negative region heading into the crossing.
    profile.extend(
        [
            (76.0, -8.0e6),
            (82.0, -1.2e7),
            (88.0, -3.0e7),
            (94.0, -2.0e7),
            (97.0, -1.0e7),
            (99.0, -5.0e6),
            # crossing somewhere in here
            (101.0, 6.0e6),
            (103.0, 1.5e7),
            (106.0, 3.0e7),
        ]
    )
    # Then a colossal spike high above spot (an ATM-of-tomorrow wall).
    profile.extend(
        [
            (115.0, 2.0e8),
            (117.0, 5.0e9),  # the spike — dominates global max
            (119.0, 2.0e8),
        ]
    )
    # And more noise floor up to the grid edge.
    profile.extend((120.0 + 0.5 * i, 1e-3) for i in range(50))

    # The spike at 117 is two orders of magnitude above the active
    # region near spot, so the prior gate's max-relative floor (2% of
    # 5e9 = 1e8) is well above the inner-window peak at the crossing
    # near 100 (~3e7) — that's exactly the SPX failure mode.
    flip = engine._find_structural_interior_crossing(profile, spot)
    assert flip is not None
    assert 99.0 < flip < 102.0


def test_resolve_gamma_flip_returns_none_at_max_rung_when_profile_truly_one_signed():
    """A book that is one-signed at EVERY ladder rung (pathological
    chain, e.g. all calls, no puts — or after-hours when every put has
    been NULL-greeked out): the resolver walks the whole ladder, finds
    no structural interior crossing anywhere, and returns (last_profile,
    None, max_rung).  The caller persists NULL+WARN — not a fabricated
    edge value.
    """
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 100.0
    options = [_opt(100.0, "C", oi=5000, iv=0.25, exp=exp)]  # pure long calls
    profile, flip, span_used = engine._resolve_gamma_flip(options, spot, ts)
    assert flip is None
    assert profile  # last profile is still built (net_gex_at_spot reads off it)
    assert all(v >= 0 for _, v in profile)  # one-signed across the widest rung
    assert abs(span_used - main_engine.GAMMA_PROFILE_SPAN_LADDER[-1]) < 1e-9


def test_resolve_gamma_flip_rejects_noise_floor_edge_crossing(monkeypatch):
    """The 2026-05-19 QQQ pathology: at the initial ±20% rung the only
    sign change sits right at the grid edge in the noise floor (far
    from spot, where every contract's gamma has decayed near zero).
    The resolver's interior + structural gates reject it.  In this
    synthesised case the wider rungs also have no qualifying crossing,
    so the resolver correctly returns NULL — *not* the spurious
    edge-resolved value the old code would have persisted.
    """
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    # Tighten the ladder for the test so we can construct a focused
    # one-signed-everywhere case without enormous synthetic OI.
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_SPAN_LADDER", [0.20, 0.35, 0.50])

    exp = datetime(2026, 9, 18).date()
    spot = 100.0
    # Pure put book — short dealer gamma everywhere; no real interior
    # crossing, but the BS-gamma vectorised re-pricing across the grid
    # leaves rounding-scale residuals at the extreme edges that can
    # produce a hairline sign change in the noise floor.  Structural
    # gate must reject it.
    options = [
        _opt(80.0, "P", oi=10_000, iv=0.30, exp=exp),
        _opt(85.0, "P", oi=10_000, iv=0.30, exp=exp),
        _opt(90.0, "P", oi=10_000, iv=0.30, exp=exp),
        _opt(95.0, "P", oi=10_000, iv=0.30, exp=exp),
    ]
    profile, flip, span_used = engine._resolve_gamma_flip(options, spot, ts)
    assert flip is None
    assert span_used == main_engine.GAMMA_PROFILE_SPAN_LADDER[-1]


def test_resolve_gamma_flip_keeps_sign_consistency_invariant_at_every_rung(monkeypatch):
    """The flip/net_gex_at_spot sign-consistency invariant must hold
    regardless of which ladder rung resolved.  Read both off the SAME
    returned profile: net_gex_at_spot's sign matches sign(spot - flip).

    The actionable-distance gate is disabled here so the wide-flip rung
    is exercisable; the invariant under test is independent of the gate.
    """
    monkeypatch.setattr(main_engine, "GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT", 1.0)
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 100.0

    # Interior on the first rung.
    options_a = [
        _opt(92.0, "P", oi=8000, iv=0.30, exp=exp),
        _opt(112.0, "C", oi=8000, iv=0.30, exp=exp),
    ]
    prof_a, flip_a, _ = engine._resolve_gamma_flip(options_a, spot, ts)
    assert flip_a is not None
    n_a = engine._net_gex_at_spot(prof_a, spot)
    assert n_a is not None
    # Same convention as the existing interior-flip test: short below,
    # long above.  sign(net_gex_at_spot) == sign(spot - flip).
    assert (n_a < 0) == (spot < flip_a)

    # Wider regime: forces expansion (same construction as the
    # wide-flip test above).
    options_b = [
        _opt(100.0, "P", oi=200_000, iv=0.30, exp=exp),
        _opt(160.0, "C", oi=200_000, iv=0.30, exp=exp),
    ]
    prof_b, flip_b, _ = engine._resolve_gamma_flip(options_b, spot, ts)
    assert flip_b is not None
    n_b = engine._net_gex_at_spot(prof_b, spot)
    assert n_b is not None
    assert (n_b < 0) == (spot < flip_b)


def test_store_gex_summary_carries_forward_previous_gamma_flip_when_missing():
    engine = AnalyticsEngine(underlying="SPY")
    cursor = MagicMock()
    cursor.fetchone.return_value = (501.25,)

    summary = {
        "underlying": "SPY",
        "timestamp": datetime(2026, 4, 17, 14, 30, tzinfo=timezone.utc),
        "max_gamma_strike": 500.0,
        "max_gamma_value": 1234.0,
        "gamma_flip_point": None,
        "put_call_ratio": 0.9,
        "max_pain": 505.0,
        "total_call_volume": 1000,
        "total_put_volume": 900,
        "total_call_oi": 2000,
        "total_put_oi": 1800,
        "total_net_gex": 555.0,
    }

    engine._store_gex_summary(summary, cursor)

    # First execute fetches prior non-null gamma flip.
    assert cursor.execute.call_count >= 2
    insert_args = cursor.execute.call_args_list[-1][0][1]
    assert insert_args[4] == 501.25


def test_store_gex_summary_keeps_current_gamma_flip_when_present():
    engine = AnalyticsEngine(underlying="SPY")
    cursor = MagicMock()

    summary = {
        "underlying": "SPY",
        "timestamp": datetime(2026, 4, 17, 14, 31, tzinfo=timezone.utc),
        "max_gamma_strike": 500.0,
        "max_gamma_value": 1234.0,
        "gamma_flip_point": 499.75,
        "put_call_ratio": 0.9,
        "max_pain": 505.0,
        "total_call_volume": 1000,
        "total_put_volume": 900,
        "total_call_oi": 2000,
        "total_put_oi": 1800,
        "total_net_gex": 555.0,
    }

    engine._store_gex_summary(summary, cursor)

    # No carry-forward SELECT when current gamma flip exists.
    insert_args = cursor.execute.call_args_list[-1][0][1]
    assert insert_args[4] == 499.75


def test_store_gex_summary_persists_null_when_flip_unresolved():
    """Degraded/one-sided chain => gamma_flip_unresolved is set, so the
    carry-forward is SKIPPED and NULL is persisted (a visible gap),
    instead of silently re-freezing the last level (the original bug)."""
    engine = AnalyticsEngine(underlying="SPY")
    cursor = MagicMock()
    cursor.fetchone.return_value = (501.25,)  # a prior exists; must NOT be used

    summary = {
        "underlying": "SPY",
        "timestamp": datetime(2026, 4, 17, 14, 32, tzinfo=timezone.utc),
        "max_gamma_strike": 500.0,
        "max_gamma_value": 1234.0,
        "gamma_flip_point": None,
        "gamma_flip_unresolved": True,
        "put_call_ratio": 0.9,
        "max_pain": 505.0,
        "total_call_volume": 1000,
        "total_put_volume": 900,
        "total_call_oi": 2000,
        "total_put_oi": 1800,
        "total_net_gex": 555.0,
    }

    engine._store_gex_summary(summary, cursor)

    # No carry-forward SELECT — only the INSERT — and flip persists NULL.
    assert cursor.execute.call_count == 1
    insert_args = cursor.execute.call_args_list[-1][0][1]
    assert insert_args[4] is None


def test_store_gex_summary_persists_net_gex_at_spot():
    """End-to-end: net_gex_at_spot from the summary dict reaches the INSERT
    params (regression: it was dropped between compute and persist, so the
    column was always written NULL)."""
    engine = AnalyticsEngine(underlying="SPY")
    cursor = MagicMock()

    summary = {
        "underlying": "SPY",
        "timestamp": datetime(2026, 4, 17, 14, 31, tzinfo=timezone.utc),
        "max_gamma_strike": 500.0,
        "max_gamma_value": 1234.0,
        "gamma_flip_point": 499.75,
        "put_call_ratio": 0.9,
        "max_pain": 505.0,
        "total_call_volume": 1000,
        "total_put_volume": 900,
        "total_call_oi": 2000,
        "total_put_oi": 1800,
        "total_net_gex": 555.0,
        "net_gex_at_spot": -1_234_567.0,
    }

    engine._store_gex_summary(summary, cursor)

    insert_args = cursor.execute.call_args_list[-1][0][1]
    # Param order: ... total_net_gex (11), net_gex_at_spot (12), flip_distance (13) ...
    assert insert_args[4] == 499.75  # gamma_flip_point index unchanged
    assert insert_args[11] == 555.0
    assert insert_args[12] == -1_234_567.0


def test_gex_summary_includes_flip_distance_local_gex_and_convexity():
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 4, 21, 14, 30, tzinfo=timezone.utc)
    spot = 500.0
    exp = datetime(2026, 7, 17).date()
    # Put mass at/below spot, call block above => spot-shift profile is
    # short-gamma at spot and crosses to long above => a real flip in
    # (spot, call strike).
    options = [
        _opt(485.0, "P", oi=6000, iv=0.22, exp=exp, volume=10),
        _opt(520.0, "C", oi=6000, iv=0.22, exp=exp, volume=12),
    ]
    gex_by_strike = [
        {"strike": 495.0, "net_gex": -2_000_000.0},
        {"strike": 500.0, "net_gex": 3_000_000.0},
        {"strike": 505.0, "net_gex": 1_000_000.0},
    ]

    summary = engine._calculate_gex_summary(
        gex_by_strike=gex_by_strike,
        options=options,
        underlying_price=spot,
        timestamp=ts,
    )

    flip = summary["gamma_flip_point"]
    assert flip is not None
    assert 485.0 < flip < 520.0  # between the put and the call strike
    # flip_distance / convexity use the produced flip with the same formulas.
    assert summary["flip_distance"] == pytest.approx((spot - flip) / spot)
    # local_gex still comes from gex_by_strike (±1% of spot => [495,505]).
    assert summary["local_gex"] == 2_000_000.0 + 3_000_000.0 + 1_000_000.0
    expected_convexity = abs(summary["total_net_gex"]) / max(abs(summary["flip_distance"]), 1e-6)
    assert summary["convexity_risk"] == pytest.approx(expected_convexity)
    # net_gex_at_spot is read off the SAME profile, so its sign tracks the
    # spot-vs-flip regime (short gamma iff spot is below the flip).
    assert "net_gex_at_spot" in summary
    assert (summary["net_gex_at_spot"] < 0) == (spot < flip)


def test_gex_summary_marks_flip_unresolved_on_one_sided_chain(caplog):
    """One-signed (degraded) usable chain => no crossing => the summary
    reports gamma_flip_point=None, gamma_flip_unresolved=True, and WARNs
    (so it's visible in analytics-health, not a silent clamp/freeze)."""
    import logging

    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 500.0
    # Pure long-call book => dealer-long-gamma everywhere => no crossing.
    options = [_opt(500.0, "C", oi=5000, iv=0.25, exp=exp, volume=3)]
    gex_by_strike = [
        {"strike": 495.0, "net_gex": 1_000_000.0},
        {"strike": 505.0, "net_gex": 2_000_000.0},
    ]

    with caplog.at_level(logging.WARNING):
        summary = engine._calculate_gex_summary(
            gex_by_strike=gex_by_strike,
            options=options,
            underlying_price=spot,
            timestamp=ts,
        )

    assert summary["gamma_flip_point"] is None
    assert summary["gamma_flip_unresolved"] is True
    assert summary["flip_distance"] is None  # no flip => no distance
    assert "net_gex_at_spot" in summary  # still sampled from the profile
    unresolved_warns = [
        r
        for r in caplog.records
        if "Gamma flip UNRESOLVED" in r.message and r.levelno == logging.WARNING
    ]
    assert unresolved_warns
    # Healthy IV pipeline (iv=0.25 ≠ 0.20 sentinel for every contract): the
    # "high share indicates stale IV pipeline" hint must NOT render, since
    # the actual share is 0%. The 2026-06-09 prod regression unconditionally
    # rendered the hint alongside "share 0.0%", inverting the meaning.
    assert "stale IV pipeline" not in unresolved_warns[0].message


def test_unresolved_warn_renders_stale_iv_hint_when_share_actually_high(caplog):
    """Mirror of the prior test for the affirmative case: when the IV
    pipeline really is stale (many contracts pinned at the 0.20 sentinel),
    the hint must render so operators see the root cause."""
    import logging

    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 500.0
    # 4/5 contracts pinned at the default IV sentinel (80% share, well
    # above the 20% threshold). All calls so the chain is one-signed and
    # the UNRESOLVED branch fires.
    options = [
        _opt(500.0, "C", oi=5000, iv=0.20, exp=exp, volume=3),
        _opt(505.0, "C", oi=5000, iv=0.20, exp=exp, volume=3),
        _opt(510.0, "C", oi=5000, iv=0.20, exp=exp, volume=3),
        _opt(515.0, "C", oi=5000, iv=0.20, exp=exp, volume=3),
        _opt(520.0, "C", oi=5000, iv=0.35, exp=exp, volume=3),
    ]
    gex_by_strike = [{"strike": 500.0, "net_gex": 1_000_000.0}]

    with caplog.at_level(logging.WARNING):
        engine._calculate_gex_summary(gex_by_strike, options, spot, ts)
    unresolved_warns = [
        r
        for r in caplog.records
        if "Gamma flip UNRESOLVED" in r.message and r.levelno == logging.WARNING
    ]
    assert unresolved_warns
    assert "stale IV pipeline" in unresolved_warns[0].message


def test_gex_summary_throttles_repeated_unresolved_warnings(caplog):
    """When the unresolved condition persists across cycles (e.g. SPX
    morning regime placing the flip beyond ±MAX_FLIP_DISTANCE_PCT for
    an entire morning), the verbose WARN must NOT fire on every cycle.
    The first call gets the full diagnostic (state transition), but a
    second call inside the throttle window stays silent."""
    import logging

    engine = AnalyticsEngine(underlying="SPY")
    # Long throttle so the second call in this test definitely lands
    # inside the window regardless of test-runner wall-clock jitter.
    engine._gamma_flip_unresolved_warn_throttle_seconds = 3600.0
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 500.0
    options = [_opt(500.0, "C", oi=5000, iv=0.25, exp=exp, volume=3)]
    gex_by_strike = [{"strike": 500.0, "net_gex": 1_000_000.0}]

    with caplog.at_level(logging.WARNING):
        engine._calculate_gex_summary(gex_by_strike, options, spot, ts)
    first_warns = [
        r
        for r in caplog.records
        if "Gamma flip UNRESOLVED" in r.message and r.levelno == logging.WARNING
    ]
    assert len(first_warns) == 1, "first call must emit the verbose WARN"

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        engine._calculate_gex_summary(gex_by_strike, options, spot, ts)
    repeat_warns = [
        r
        for r in caplog.records
        if "Gamma flip UNRESOLVED" in r.message and r.levelno == logging.WARNING
    ]
    assert repeat_warns == [], "second call within throttle window must stay silent"


def test_gex_summary_logs_resolved_transition_after_unresolved_period(caplog):
    """When the chain recovers and a flip resolves after a persistent
    unresolved period, log an INFO line so the recovery is visible in
    the analytics-health timeline (mirrors the WARN that opened the
    unresolved period)."""
    import logging

    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc)
    exp = datetime(2026, 9, 18).date()
    spot = 500.0

    # Unresolved cycle: pure-call book, no crossing.
    unresolved_options = [_opt(500.0, "C", oi=5000, iv=0.25, exp=exp, volume=3)]
    engine._calculate_gex_summary([{"strike": 500.0, "net_gex": 1.0}], unresolved_options, spot, ts)
    assert engine._gamma_flip_unresolved_state is True

    # Resolved cycle: balanced book with a real flip.
    resolved_options = [
        _opt(485.0, "P", oi=6000, iv=0.22, exp=exp, volume=10),
        _opt(520.0, "C", oi=6000, iv=0.22, exp=exp, volume=12),
    ]
    caplog.clear()
    with caplog.at_level(logging.INFO):
        summary = engine._calculate_gex_summary(
            [{"strike": 500.0, "net_gex": 1.0}], resolved_options, spot, ts
        )
    assert summary["gamma_flip_point"] is not None
    assert engine._gamma_flip_unresolved_state is False
    assert any(
        "Gamma flip RESOLVED" in r.message and r.levelno == logging.INFO for r in caplog.records
    )


def _full_gex_row(ts):
    return {
        "underlying": "SPY",
        "timestamp": ts,
        "strike": 500.0,
        "expiration": ts.date(),
        "total_gamma": 0.3,
        "call_gamma": 0.2,
        "put_gamma": 0.1,
        "net_gex": 1_000_000.0,
        "call_volume": 10,
        "put_volume": 5,
        "call_oi": 100,
        "put_oi": 50,
        "vanna_exposure": 1.0,
        "charm_exposure": 2.0,
        "call_vanna_exposure": 0.5,
        "put_vanna_exposure": 0.5,
        "call_charm_exposure": 1.0,
        "put_charm_exposure": 1.0,
        "dealer_vanna_exposure": -1.0,
        "dealer_charm_exposure": -2.0,
        "expiration_bucket": "0dte",
    }


def _full_summary(ts):
    return {
        "underlying": "SPY",
        "timestamp": ts,
        "max_gamma_strike": 500.0,
        "max_gamma_value": 1_000_000.0,
        # Provide a non-None gamma_flip so _store_gex_summary skips the
        # carry-forward SELECT and the very first cursor.execute() is the
        # summary INSERT — i.e. the failure lands mid-transaction, AFTER
        # the by-strike write has already been issued.
        "gamma_flip_point": 499.0,
        "put_call_ratio": 0.9,
        "max_pain": 505.0,
        "total_call_volume": 1000,
        "total_put_volume": 900,
        "total_call_oi": 2000,
        "total_put_oi": 1800,
        "total_net_gex": 1_000_000.0,
    }


def test_store_calculation_results_is_atomic_on_mid_transaction_failure(monkeypatch):
    """C1: by-strike + summary must commit together (all rows land or none).

    Simulate the summary write blowing up AFTER the by-strike rows were
    already issued on the shared cursor.  The whole transaction must roll
    back (never commit), so the by-strike rows do not persist — proving
    the single-transaction grouping survived the conn/cursor refactor.
    """
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 4, 17, 14, 30, tzinfo=timezone.utc)

    cursor = MagicMock()
    # Every cursor.execute() raises; with gamma_flip_point set the first
    # (and only) execute in _store_gex_summary is the summary INSERT.
    cursor.execute.side_effect = RuntimeError("summary insert blew up")
    conn = MagicMock()
    conn.cursor.return_value = cursor

    by_strike_writes = []

    def fake_execute_values(cur, sql, rows):
        # _store_gex_by_strike succeeds: record that the by-strike INSERT
        # was issued into this (soon-to-be-rolled-back) transaction.
        assert cur is cursor
        by_strike_writes.append(rows)

    monkeypatch.setattr(main_engine, "execute_values", fake_execute_values)

    @contextmanager
    def fake_db_connection():
        # Mirror src/database/connection.py: commit on clean exit,
        # rollback on any exception.
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    monkeypatch.setattr(main_engine, "db_connection", fake_db_connection)

    with pytest.raises(RuntimeError, match="summary insert blew up"):
        engine._store_calculation_results([_full_gex_row(ts)], _full_summary(ts))

    # The by-strike INSERT WAS issued (it ran before the summary failure)…
    assert len(by_strike_writes) == 1
    # …but exactly one connection/transaction was used…
    conn.cursor.assert_called_once()
    # …and it was rolled back, never committed: the by-strike rows that
    # were written in this transaction do NOT persist. All-or-nothing.
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    assert engine.errors_count == 1


def test_store_calculation_results_commits_once_on_success(monkeypatch):
    """Happy path: both writes land in a single committed transaction."""
    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 4, 17, 14, 31, tzinfo=timezone.utc)

    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor

    monkeypatch.setattr(main_engine, "execute_values", lambda cur, sql, rows: None)

    committed = []

    @contextmanager
    def fake_db_connection():
        try:
            yield conn
            conn.commit()
            committed.append(True)
        except Exception:
            conn.rollback()
            raise

    monkeypatch.setattr(main_engine, "db_connection", fake_db_connection)

    engine._store_calculation_results([_full_gex_row(ts)], _full_summary(ts))

    conn.cursor.assert_called_once()  # one connection => one transaction
    conn.rollback.assert_not_called()
    assert conn.commit.called  # committed (explicit + CM are harmless dups)
    assert committed == [True]
    assert engine.errors_count == 0
