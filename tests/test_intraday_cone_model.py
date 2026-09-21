"""Unit tests for the intraday re-anchored cone.

The load-bearing claim in this module is ``hold_probability`` — every number
the cone publishes is downstream of it, and it is the one piece a reader
cannot check by eye.  It is therefore validated twice, against two
independent references:

* a **spectral (Fourier sine) solution** of the same absorbed-diffusion
  problem, which is a genuinely different derivation from the method of
  images used in the model — the two series even converge in opposite
  regimes, so agreement across a grid is strong evidence rather than a
  restatement; and
* a **Monte Carlo** simulation, which is slow and imprecise but answers the
  question the closed forms cannot: whether either of them describes an
  actual random walk.

The rest of the file anchors the diurnal profile, the cone assembly and the
grading semantics so a later "simplification" cannot quietly re-scale a band
or turn containment back into a terminal-value test.
"""

from __future__ import annotations

import math

import pytest

from src.jobs.intraday_cone_model import (
    CONE_HORIZONS_MIN,
    CONE_VOL_RATIO_MAX,
    CONE_VOL_RATIO_MIN,
    VOL_BASIS_MULT_MAX,
    VOL_BASIS_MULT_MIN,
    GAMMA_MULT_MAX,
    GAMMA_MULT_MIN,
    HOLD_PROB_MAX,
    HOLD_PROB_MIN,
    SESSION_MINUTES,
    ConeInputs,
    base_rate_brier,
    blended_daily_sigma,
    calibration_error,
    compute_cone,
    diurnal_density,
    grade_horizon,
    hold_probability,
    horizon_sigma,
    realized_daily_sigma,
    reliability_table,
    variance_fraction,
)


# ---------------------------------------------------------------------------
# Independent reference: spectral solution
# ---------------------------------------------------------------------------


def _survival_spectral(x0: float, width: float, sigma: float, terms: int = 400) -> float:
    """Survival probability between absorbing barriers at 0 and ``width``.

    Eigenfunction expansion of the heat equation with Dirichlet boundaries:

        P = Σ_{k odd} (4/(kπ))·sin(kπ·x₀/W)·exp(−k²π²σ²/(2W²))

    ``sigma`` is the TOTAL stdev over the horizon (σ√T), matching the model's
    convention.  Converges quickly for wide sigma and slowly for narrow — the
    exact opposite of the image series it is checking, which is the point.
    """
    total = 0.0
    for k in range(1, terms + 1, 2):
        total += (4.0 / (k * math.pi)) * math.sin(k * math.pi * x0 / width) * math.exp(
            -(k**2) * (math.pi**2) * (sigma**2) / (2.0 * width**2)
        )
    return total


@pytest.mark.parametrize(
    "lower,upper,sigma",
    [
        (-1.0, 1.0, 0.5),      # symmetric, tight-ish
        (-1.0, 1.0, 1.0),      # symmetric, sigma == half-width
        (-1.0, 1.0, 2.0),      # symmetric, wide sigma (low survival)
        (-2.0, 1.0, 0.8),      # asymmetric, more room below
        (-1.0, 3.0, 1.1),      # asymmetric, more room above
        (-0.5, 0.5, 0.35),     # narrow band
        (-4.0, 4.0, 0.9),      # wide band (high survival)
        (-1.5, 2.5, 1.7),      # asymmetric + wide sigma
    ],
)
def test_hold_probability_matches_spectral_solution(lower, upper, sigma):
    """The image series and the spectral series must agree to near machine
    precision.  They are different expansions of the same operator, so a
    disagreement means one of them is wrong — and only one of them ships."""
    spot = 100.0
    got = hold_probability(
        spot=spot, band_low=spot + lower, band_high=spot + upper, sigma=sigma
    )
    want = _survival_spectral(x0=-lower, width=upper - lower, sigma=sigma)
    # Only compare where the model is not clamping for publication safety.
    if HOLD_PROB_MIN < want < HOLD_PROB_MAX:
        assert got == pytest.approx(want, abs=1e-6)


def test_hold_probability_matches_monte_carlo():
    """Both closed forms are checked against an actual simulated walk.

    Discrete sampling can only MISS a barrier touch that happened between two
    steps, never invent one, so the simulation is biased slightly HIGH on
    survival.  The tolerance is one-sided-aware rather than symmetric-tight.
    """
    np = pytest.importorskip("numpy")
    rng = np.random.default_rng(20260921)

    spot, lower, upper, sigma = 100.0, -1.2, 1.6, 1.0
    steps, paths, chunks = 4000, 8000, 5

    survivors = 0
    total = 0
    step_sd = sigma / math.sqrt(steps)
    for _ in range(chunks):
        increments = rng.normal(0.0, step_sd, size=(paths, steps))
        walk = np.cumsum(increments, axis=1)
        lo = walk.min(axis=1)
        hi = walk.max(axis=1)
        survivors += int(np.count_nonzero((lo > lower) & (hi < upper)))
        total += paths

    mc = survivors / total
    analytic = hold_probability(
        spot=spot, band_low=spot + lower, band_high=spot + upper, sigma=sigma
    )
    # ~40k paths gives a standard error near 0.0025; the discretization bias
    # at 4000 steps is a few tenths of a percent on top of that.
    assert analytic == pytest.approx(mc, abs=0.012)
    assert analytic <= mc + 0.02, "analytic survival should not exceed the (high-biased) simulation"


def test_hold_probability_limits_and_guards():
    spot = 100.0
    # A very wide band with tiny vol is a near-certainty, but never published
    # as one.
    wide = hold_probability(spot=spot, band_low=50.0, band_high=150.0, sigma=0.01)
    assert wide == HOLD_PROB_MAX
    # A hairline band with large vol is a near-impossibility, likewise floored.
    narrow = hold_probability(spot=spot, band_low=99.99, band_high=100.01, sigma=5.0)
    assert narrow == HOLD_PROB_MIN
    # Unusable sigma yields no claim at all rather than a fabricated one.
    assert hold_probability(spot=spot, band_low=99.0, band_high=101.0, sigma=0.0) is None
    assert hold_probability(spot=spot, band_low=99.0, band_high=101.0, sigma=-1.0) is None
    # Spot outside its own band is dead on arrival, not a coin flip.
    assert hold_probability(spot=spot, band_low=101.0, band_high=102.0, sigma=1.0) == HOLD_PROB_MIN


def test_hold_probability_is_monotone_in_width_and_sigma():
    spot = 100.0
    widening = [
        hold_probability(spot=spot, band_low=spot - w, band_high=spot + w, sigma=1.0)
        for w in (0.5, 1.0, 1.5, 2.0, 3.0)
    ]
    assert widening == sorted(widening), "a wider band cannot be less likely to hold"
    noisier = [
        hold_probability(spot=spot, band_low=spot - 1.5, band_high=spot + 1.5, sigma=s)
        for s in (0.4, 0.8, 1.2, 2.0)
    ]
    assert noisier == sorted(noisier, reverse=True), "more vol cannot make holding easier"


def test_two_sided_hold_is_below_the_naive_independent_product():
    """The reason the model does not just multiply two one-sided reflections.

    Treating the edges as independent events ignores that a path which would
    have touched both is counted once by the true survival and twice by the
    product, so the naive number sits ABOVE the truth — biased toward claiming
    containment exactly where the band is tight.
    """
    spot, half, sigma = 100.0, 1.0, 1.0

    def one_sided_no_touch(distance: float) -> float:
        # 1 − reflection-principle touch probability, the daily model's shape.
        z = distance / sigma
        touch = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(z / math.sqrt(2.0))))
        return 1.0 - touch

    naive = one_sided_no_touch(half) * one_sided_no_touch(half)
    true = hold_probability(
        spot=spot, band_low=spot - half, band_high=spot + half, sigma=sigma
    )
    assert true < naive


# ---------------------------------------------------------------------------
# Diurnal variance profile
# ---------------------------------------------------------------------------


def test_variance_fraction_covers_the_whole_session():
    assert variance_fraction(0, SESSION_MINUTES) == pytest.approx(1.0, abs=1e-9)


def test_variance_fraction_is_additive_and_ordered():
    a = variance_fraction(0, 120)
    b = variance_fraction(120, 300)
    c = variance_fraction(300, SESSION_MINUTES)
    assert a + b + c == pytest.approx(1.0, abs=1e-9)
    assert variance_fraction(60, 60) == 0.0
    assert variance_fraction(200, 100) == 0.0


def test_diurnal_profile_is_u_shaped():
    """Open and close carry more variance per minute than the midday trough —
    the property that makes flat sqrt-of-time scaling wrong."""
    at_open = diurnal_density(0.0)
    midday = diurnal_density(0.55)
    at_close = diurnal_density(1.0)
    assert at_open > 2.0 * midday
    assert at_close > 2.0 * midday
    assert at_open > at_close  # the opening auction outweighs the bell


def test_equal_length_windows_are_not_equal_variance():
    """The concrete consequence: the first 30 minutes are worth far more than
    30 minutes at lunch, so the cone cannot use elapsed time alone."""
    opening = variance_fraction(0, 30)
    lunch = variance_fraction(180, 210)
    assert opening > 2.5 * lunch


def test_horizon_sigma_grows_sublinearly_with_horizon():
    sigmas = [
        horizon_sigma(daily_sigma=6.0, elapsed_min=60, horizon_min=h)
        for h in CONE_HORIZONS_MIN
    ]
    assert sigmas == sorted(sigmas)
    # Doubling the horizon must not double the sigma (variance adds, not stdev).
    assert sigmas[1] < 2.0 * sigmas[0]
    assert all(s < 6.0 for s in sigmas), "an intraday window cannot exceed the day"


def test_realized_daily_sigma_round_trips_through_the_profile():
    """A range that is exactly Parkinson-consistent with a known daily sigma
    must extrapolate back to that sigma."""
    daily_sigma = 5.0
    elapsed = 150
    frac = variance_fraction(0, elapsed)
    window_sigma = daily_sigma * math.sqrt(frac)
    observed_range = math.sqrt(8.0 / math.pi) * window_sigma
    got = realized_daily_sigma(
        session_high=100.0 + observed_range, session_low=100.0, elapsed_min=elapsed
    )
    assert got == pytest.approx(daily_sigma, rel=1e-9)


def test_realized_daily_sigma_refuses_a_too_thin_window():
    """Very early in the session the divisor is small enough that one wide bar
    would dominate, so the estimate is withheld rather than published noisy."""
    assert realized_daily_sigma(session_high=101.0, session_low=100.0, elapsed_min=3) is None
    assert realized_daily_sigma(session_high=None, session_low=100.0, elapsed_min=120) is None
    assert realized_daily_sigma(session_high=100.0, session_low=100.0, elapsed_min=120) is None


def test_realized_blend_weight_grows_through_the_session():
    early, _ = blended_daily_sigma(implied_sigma=4.0, realized_sigma=8.0, elapsed_min=20)
    late, _ = blended_daily_sigma(implied_sigma=4.0, realized_sigma=8.0, elapsed_min=330)
    assert 4.0 < early < late < 8.0


def test_blend_degrades_to_whichever_input_exists():
    only_implied, notes = blended_daily_sigma(
        implied_sigma=4.0, realized_sigma=None, elapsed_min=100
    )
    assert only_implied == 4.0 and "implied only" in notes[0]
    only_realized, notes = blended_daily_sigma(
        implied_sigma=None, realized_sigma=7.0, elapsed_min=100
    )
    assert only_realized == 7.0 and "realized only" in notes[0]
    none_at_all, notes = blended_daily_sigma(
        implied_sigma=None, realized_sigma=None, elapsed_min=100
    )
    assert none_at_all == 0.0 and "unavailable" in notes[0]


# ---------------------------------------------------------------------------
# Cone assembly
# ---------------------------------------------------------------------------


def _inputs(**overrides) -> ConeInputs:
    base = ConeInputs(
        symbol="SPY",
        spot=600.0,
        elapsed_min=90,          # 11:00 ET
        implied_move=4.5,
        session_high=602.0,
        session_low=598.5,
        call_wall=606.0,
        put_wall=594.0,
        gamma_flip=601.0,
        net_gex_at_spot=4.0e8,
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def test_cone_publishes_every_horizon_that_fits_before_the_bell():
    result = compute_cone(_inputs())
    assert [h.horizon_min for h in result.horizons] == list(CONE_HORIZONS_MIN)
    for h in result.horizons:
        assert h.band_low < 600.0 < h.band_high
        assert HOLD_PROB_MIN <= h.hold_prob <= HOLD_PROB_MAX


def test_cone_withholds_horizons_that_would_run_past_the_close():
    """A '+2h' label on a claim graded over 40 minutes would be a lie, so the
    horizon is dropped rather than truncated to the bell."""
    at_1500 = compute_cone(_inputs(elapsed_min=330))   # 60 minutes left
    assert [h.horizon_min for h in at_1500.horizons] == [30, 60]

    at_1530 = compute_cone(_inputs(elapsed_min=360))   # 30 minutes left
    assert [h.horizon_min for h in at_1530.horizons] == [30]

    at_the_bell = compute_cone(_inputs(elapsed_min=SESSION_MINUTES))
    assert at_the_bell.horizons == []
    assert "no horizon completes" in " ".join(at_the_bell.rationale)


def test_later_horizons_are_wider_and_less_likely_to_hold():
    result = compute_cone(_inputs(call_wall=None, put_wall=None))
    widths = [h.band_high - h.band_low for h in result.horizons]
    holds = [h.hold_prob for h in result.horizons]
    assert widths == sorted(widths)
    assert holds == sorted(holds, reverse=True)


def test_gamma_moves_the_hold_probability_the_way_a_reader_expects():
    """Pinning must raise the odds of containment, not lower them.

    Regression on a sign error that is easy to reintroduce: if the gamma tilt
    is applied to the drawn band but not to the process sigma, a long-gamma
    day publishes a TIGHTER band against an unchanged walk and therefore a
    LOWER hold — the model would report its least containable odds on the day
    price is most contained.  Applying the identical multiplier to both is the
    other failure: band and walk scale together, and gamma cancels out of the
    probability entirely.
    """
    kw = dict(call_wall=None, put_wall=None, gamma_flip=None)
    pinned = compute_cone(_inputs(net_gex_at_spot=1.2e9, **kw))
    neutral = compute_cone(_inputs(net_gex_at_spot=0.0, **kw))
    accelerant = compute_cone(_inputs(net_gex_at_spot=-1.2e9, **kw))

    for i in range(len(CONE_HORIZONS_MIN)):
        p_hold = pinned.horizons[i].hold_prob
        n_hold = neutral.horizons[i].hold_prob
        a_hold = accelerant.horizons[i].hold_prob
        assert p_hold > n_hold > a_hold, f"horizon index {i} ordered wrong"

    # ...while the BAND still tightens under long gamma and widens under short.
    p_width = pinned.horizons[0].band_high - pinned.horizons[0].band_low
    n_width = neutral.horizons[0].band_high - neutral.horizons[0].band_low
    a_width = accelerant.horizons[0].band_high - accelerant.horizons[0].band_low
    assert p_width < n_width < a_width

    # The two effects must not cancel: gamma has to actually move the odds.
    assert pinned.horizons[0].hold_prob - accelerant.horizons[0].hold_prob > 0.05


def test_hold_probability_varies_materially_across_horizons():
    """Regression on scale-invariance.

    Barrier survival depends only on the ratio band/sigma, so a band drawn at
    a fixed multiple of the horizon sigma yields the SAME hold at every
    horizon — the first version of this model published one number four times
    and called it a term structure.  The spread must be large enough to carry
    information, not merely non-zero.
    """
    result = compute_cone(_inputs(call_wall=None, put_wall=None, gamma_flip=None))
    holds = [h.hold_prob for h in result.horizons]
    assert holds == sorted(holds, reverse=True)
    assert holds[0] - holds[-1] > 0.05, "hold must decay meaningfully with horizon"


def test_reference_horizon_lands_in_the_informative_band():
    """The shortest published horizon should sit where miscalibration is
    visible — neither a band that always holds nor one nobody would draw."""
    result = compute_cone(_inputs(call_wall=None, put_wall=None, gamma_flip=None))
    assert 0.65 < result.horizons[0].hold_prob < 0.85


def test_a_fixed_wall_makes_hold_decay_faster_with_horizon():
    """The structural mechanism the cone exists to capture: a wall sits at a
    fixed price, so a longer window gives price more chances to reach it."""
    kw = dict(put_wall=None, gamma_flip=None)
    walled = compute_cone(_inputs(call_wall=601.0, **kw))
    open_field = compute_cone(_inputs(call_wall=None, **kw))
    walled_decay = walled.horizons[0].hold_prob - walled.horizons[-1].hold_prob
    open_decay = open_field.horizons[0].hold_prob - open_field.horizons[-1].hold_prob
    assert walled_decay > open_decay


def test_long_gamma_tightens_and_short_gamma_widens():
    bare = dict(gamma_flip=None, call_wall=None, put_wall=None)
    long_gamma = compute_cone(_inputs(net_gex_at_spot=1.5e9, **bare))
    short_gamma = compute_cone(_inputs(net_gex_at_spot=-1.5e9, **bare))
    neutral = compute_cone(_inputs(net_gex_at_spot=0.0, **bare))
    lw = long_gamma.horizons[0].band_high - long_gamma.horizons[0].band_low
    sw = short_gamma.horizons[0].band_high - short_gamma.horizons[0].band_low
    nw = neutral.horizons[0].band_high - neutral.horizons[0].band_low
    assert lw < nw < sw
    assert GAMMA_MULT_MIN <= long_gamma.gamma_mult <= GAMMA_MULT_MAX
    assert GAMMA_MULT_MIN <= short_gamma.gamma_mult <= GAMMA_MULT_MAX


def test_gamma_multiplier_is_bounded_against_an_extreme_print():
    absurd = compute_cone(_inputs(net_gex_at_spot=-9.9e12, gamma_flip=None))
    assert absurd.gamma_mult <= GAMMA_MULT_MAX


def test_sitting_on_the_flip_widens_the_cone():
    on_flip = compute_cone(_inputs(gamma_flip=600.05, call_wall=None, put_wall=None))
    far_flip = compute_cone(_inputs(gamma_flip=560.0, call_wall=None, put_wall=None))
    on = on_flip.horizons[0].band_high - on_flip.horizons[0].band_low
    far = far_flip.horizons[0].band_high - far_flip.horizons[0].band_low
    assert on > far
    assert "gamma flip" in " ".join(on_flip.rationale)


def test_walls_lean_the_edges_without_sealing_them():
    """A wall inside the raw cone pulls the edge back, but never all the way —
    walls are where hedging resists, not where price is forbidden."""
    tight_wall = 601.0
    no_wall = compute_cone(_inputs(call_wall=None, put_wall=None, gamma_flip=None))
    with_wall = compute_cone(_inputs(call_wall=tight_wall, put_wall=None, gamma_flip=None))
    raw_high = no_wall.horizons[-1].band_high
    leaned_high = with_wall.horizons[-1].band_high
    assert leaned_high < raw_high, "the wall should pull the edge in"
    assert leaned_high > tight_wall, "but the band must not stop dead at the wall"
    assert "call wall" in " ".join(with_wall.rationale)


def test_hold_probability_describes_the_band_actually_drawn():
    """After a wall lean the published probability must be recomputed from the
    leaned band — otherwise the number describes a band the page never drew."""
    leaned = compute_cone(_inputs(call_wall=601.0, put_wall=None, gamma_flip=None))
    for h in leaned.horizons:
        expected = hold_probability(
            spot=600.0, band_low=h.band_low, band_high=h.band_high, sigma=h.sigma
        )
        assert h.hold_prob == pytest.approx(expected, abs=1e-9)


def test_the_committed_vol_call_scales_the_implied_leg():
    """The correction for this model's largest early error.

    The first version used the raw implied move, which is arithmetically
    identical to asserting that every session delivers a normal day's range.
    Three backfilled sessions came back at roughly half that, so the bands
    were sized for movement that never arrived and nearly everything held.
    """
    normal = compute_cone(_inputs(expected_vol_ratio=None))
    quiet = compute_cone(_inputs(expected_vol_ratio=0.60))
    assert quiet.daily_sigma < normal.daily_sigma
    assert quiet.daily_sigma == pytest.approx(normal.daily_sigma * 0.60, rel=0.35), (
        "the ratio scales only the implied leg, not the realized one"
    )
    q_width = quiet.horizons[0].band_high - quiet.horizons[0].band_low
    n_width = normal.horizons[0].band_high - normal.horizons[0].band_low
    assert q_width < n_width
    assert "morning vol call" in " ".join(quiet.rationale)


def test_the_vol_call_raises_hold_only_through_the_structural_clamp():
    """WHY the fix works, pinned so a later change cannot break the channel.

    Barrier survival is scale-free, so shrinking the band and the walk
    together cannot move the probability — and with no wall in range, it
    doesn't. The gain comes entirely from a correctly-sized band overshooting
    the walls less and therefore being clamped less. Losing that distinction
    is how someone later "simplifies" the ratio onto the band alone and
    silently reintroduces the inverted-gamma bug from the first draft.
    """
    bare = dict(call_wall=None, put_wall=None, gamma_flip=None)
    open_normal = compute_cone(_inputs(expected_vol_ratio=None, **bare))
    open_quiet = compute_cone(_inputs(expected_vol_ratio=0.60, **bare))
    # Not bit-exact: band edges and sigma are rounded to the precision they are
    # published at before the probability is derived from them, so the two
    # differ in the fifth decimal. A tenth of a point is far below anything a
    # reader could act on, and far below the gain the clamped case must show.
    for a, b in zip(open_normal.horizons, open_quiet.horizons):
        assert a.hold_prob == pytest.approx(b.hold_prob, abs=0.001), (
            "with no wall in range the ratio must not move the odds"
        )

    # Put a wall where a normal-sized band overshoots it and a quiet one does not.
    walled = dict(call_wall=601.2, put_wall=598.8, gamma_flip=None)
    clamped_normal = compute_cone(_inputs(expected_vol_ratio=None, **walled))
    clamped_quiet = compute_cone(_inputs(expected_vol_ratio=0.60, **walled))
    gain = clamped_quiet.horizons[-1].hold_prob - clamped_normal.horizons[-1].hold_prob
    assert gain > 0.02, (
        f"the structural channel must carry a real gain, got {gain:.4f}"
    )


def test_a_nonsense_vol_call_cannot_wreck_the_session():
    """A ratio is a published claim, not a measurement."""
    floored = compute_cone(_inputs(expected_vol_ratio=0.001))
    at_floor = compute_cone(_inputs(expected_vol_ratio=CONE_VOL_RATIO_MIN))
    assert floored.daily_sigma == pytest.approx(at_floor.daily_sigma, rel=1e-9)

    capped = compute_cone(_inputs(expected_vol_ratio=999.0))
    at_cap = compute_cone(_inputs(expected_vol_ratio=CONE_VOL_RATIO_MAX))
    assert capped.daily_sigma == pytest.approx(at_cap.daily_sigma, rel=1e-9)

    # Garbage that is not a number degrades to "no call", not to a crash.
    assert compute_cone(_inputs(expected_vol_ratio="nonsense")).horizons


def test_no_committed_call_behaves_as_it_did_before():
    """A session with no morning forecast must still produce an honest cone."""
    none_given = compute_cone(_inputs(expected_vol_ratio=None))
    assert len(none_given.horizons) == len(CONE_HORIZONS_MIN)
    assert "morning vol call" not in " ".join(none_given.rationale)


def test_realized_now_dominates_by_the_end_of_the_session():
    """The old 0.55 cap left a stale morning number carrying 49% of the weight
    at 15:30, when the realized read had already seen ~88% of the day."""
    early, _ = blended_daily_sigma(implied_sigma=10.0, realized_sigma=2.0, elapsed_min=15)
    late, _ = blended_daily_sigma(implied_sigma=10.0, realized_sigma=2.0, elapsed_min=360)
    assert early > late, "realized should pull harder as the session accumulates"
    # At the last fire the realized read must be the majority of the blend.
    assert late < 0.5 * (10.0 + 2.0), f"realized should dominate by 15:30, got {late}"
    assert late == pytest.approx(10.0 - 0.80 * 8.0, abs=0.15)


def test_the_committed_vol_basis_recentres_the_implied_leg():
    """The variance-risk premium, absorbed rather than rediscovered.

    The daily model learns a per-symbol vol_range_basis_mult nightly, for the
    stated purpose of absorbing the structural fact that realized vol runs
    below implied. The cone ignored it for three rounds and kept measuring
    the same shortfall by hand. Without it the cone's 'normal day' is the
    daily model's normal day divided by this scalar, so two surfaces grading
    the same tape disagree about what average means.
    """
    plain = compute_cone(_inputs(vol_basis_mult=None))
    recentred = compute_cone(_inputs(vol_basis_mult=0.65))
    assert recentred.daily_sigma == pytest.approx(plain.daily_sigma * 0.65, rel=0.35)
    assert recentred.daily_sigma < plain.daily_sigma
    assert "committed vol basis" in " ".join(recentred.rationale)


def test_the_basis_and_the_anchor_compose():
    """They answer different questions and both apply: the basis re-centres
    what 'normal' means for this symbol, the anchor says how much of normal
    today should deliver."""
    trailing = [0.8, 0.82, 0.78, 0.81, 0.8, 0.79]
    neither = compute_cone(_inputs())
    basis_only = compute_cone(_inputs(vol_basis_mult=0.65))
    both = compute_cone(_inputs(vol_basis_mult=0.65, trailing_vol_ratios=trailing))
    assert both.daily_sigma < basis_only.daily_sigma < neither.daily_sigma
    assert both.vol_ratio_source == "measured"


def test_the_basis_is_clamped_to_the_calibrators_own_range():
    floored = compute_cone(_inputs(vol_basis_mult=0.001))
    at_floor = compute_cone(_inputs(vol_basis_mult=VOL_BASIS_MULT_MIN))
    assert floored.daily_sigma == pytest.approx(at_floor.daily_sigma, rel=1e-9)

    capped = compute_cone(_inputs(vol_basis_mult=99.0))
    at_cap = compute_cone(_inputs(vol_basis_mult=VOL_BASIS_MULT_MAX))
    assert capped.daily_sigma == pytest.approx(at_cap.daily_sigma, rel=1e-9)

    assert compute_cone(_inputs(vol_basis_mult="nonsense")).horizons


def test_the_model_version_tracks_the_math_it_stamps():
    """A version string that never changes is worse than none at all.

    This one sat at v1_0 through four substantive changes to the math, so
    nine backfilled sessions spanning two very different models all claimed
    to be the same model. The calibration report averaged them and the result
    read as a regression that was really an averaging artifact.

    The assertion is deliberately blunt: if someone changes how a band or a
    probability is computed and leaves the version alone, this fails and says
    why.
    """
    from src.jobs import intraday_cone_model as m

    assert m.MODEL_VERSION != "cone_v1_0", (
        "v1_0 predates the vol anchor, the measured anchor and the committed "
        "vol basis — stamping it on current claims makes stale and current "
        "rows indistinguishable in the calibration report"
    )
    # The constants whose values define the published band. Changing any of
    # them changes every claim, so the version must move with them.
    fingerprint = (
        m.CONE_SIGMA_MULT, m.CONE_TERM_DECAY, m.GAMMA_TILT, m.GAMMA_BAND_DAMPING,
        m.REALIZED_BLEND_MAX, m.WALL_LEAN, m.CONE_VOL_RATIO_MIN,
        m.CONE_VOL_RATIO_MAX, m.VOL_BASIS_MULT_MIN, m.VOL_BASIS_MULT_MAX,
    )
    assert fingerprint == (1.50, 0.07, 0.22, 0.50, 0.85, 0.35, 0.45, 1.90, 0.45, 1.40), (
        "a band-defining constant moved — bump MODEL_VERSION and update this "
        "fingerprint, so committed claims stay attributable to the math that "
        "produced them"
    )


def test_every_claim_carries_the_version(_=None):
    result = compute_cone(_inputs())
    from src.jobs import intraday_cone_model as m

    assert result.model_version == m.MODEL_VERSION
    assert result.horizons, "a version is only useful on rows that exist"


def test_cone_degrades_rather_than_raises_on_a_thin_surface():
    bare = compute_cone(
        ConeInputs(symbol="SPY", spot=600.0, elapsed_min=90, implied_move=4.5)
    )
    assert len(bare.horizons) == len(CONE_HORIZONS_MIN)

    no_vol = compute_cone(
        ConeInputs(symbol="SPY", spot=600.0, elapsed_min=90)
    )
    assert no_vol.horizons == []
    assert "no vol basis" in " ".join(no_vol.rationale)

    no_spot = compute_cone(
        ConeInputs(symbol="SPY", spot=0.0, elapsed_min=90, implied_move=4.5)
    )
    assert no_spot.horizons == []
    assert "no usable spot" in " ".join(no_spot.rationale)


# ---------------------------------------------------------------------------
# Grading
# ---------------------------------------------------------------------------


def test_held_requires_containment_for_the_whole_window():
    inside = grade_horizon(
        band_low=598.0, band_high=602.0, hold_prob=0.7,
        window_low=598.5, window_high=601.5,
    )
    assert inside["held"] is True
    assert inside["brier"] == pytest.approx((0.7 - 1.0) ** 2, abs=1e-9)


def test_a_pierce_that_closes_back_inside_is_not_a_hold():
    """The case a terminal-value probability would score as a win, and the
    trader who got stopped out would not."""
    pierced = grade_horizon(
        band_low=598.0, band_high=602.0, hold_prob=0.7,
        window_low=597.2, window_high=601.0,   # dipped out, recovered
    )
    assert pierced["held"] is False
    assert pierced["brier"] == pytest.approx(0.49, abs=1e-9)


def test_touching_the_edge_exactly_still_counts_as_held():
    edge = grade_horizon(
        band_low=598.0, band_high=602.0, hold_prob=0.6,
        window_low=598.0, window_high=602.0,
    )
    assert edge["held"] is True


def test_grading_withholds_a_verdict_without_a_window():
    missing = grade_horizon(
        band_low=598.0, band_high=602.0, hold_prob=0.7,
        window_low=None, window_high=None,
    )
    assert missing == {"held": None, "brier": None}


def test_brier_is_withheld_when_no_probability_was_committed():
    ungraded = grade_horizon(
        band_low=598.0, band_high=602.0, hold_prob=None,
        window_low=599.0, window_high=601.0,
    )
    assert ungraded["held"] is True
    assert ungraded["brier"] is None


# ---------------------------------------------------------------------------
# Reliability / the receipt
# ---------------------------------------------------------------------------


def test_reliability_table_reports_predicted_against_realized():
    # Twenty forecasts at 0.7 of which fourteen held — perfectly calibrated.
    preds = [(0.7, True)] * 14 + [(0.7, False)] * 6
    rows = reliability_table(preds, buckets=5)
    assert len(rows) == 1
    row = rows[0]
    assert row["n"] == 20
    assert row["predicted"] == pytest.approx(0.7)
    assert row["realized"] == pytest.approx(0.7)
    assert row["gap"] == pytest.approx(0.0)
    assert calibration_error(preds, buckets=5) == pytest.approx(0.0)


def test_reliability_table_exposes_overconfidence():
    """A cone that says 90% and delivers 50% has nowhere to hide."""
    preds = [(0.9, True)] * 5 + [(0.9, False)] * 5
    rows = reliability_table(preds, buckets=5)
    assert rows[0]["predicted"] == pytest.approx(0.9)
    assert rows[0]["realized"] == pytest.approx(0.5)
    assert rows[0]["gap"] == pytest.approx(-0.4)
    assert calibration_error(preds, buckets=5) == pytest.approx(0.4)


def test_reliability_table_omits_empty_buckets():
    preds = [(0.75, True), (0.78, False)]
    rows = reliability_table(preds, buckets=5)
    assert len(rows) == 1
    assert rows[0]["bucket_low"] == pytest.approx(0.6)


def test_reliability_table_includes_a_perfect_one():
    rows = reliability_table([(1.0, True)], buckets=5)
    assert len(rows) == 1 and rows[0]["n"] == 1


def test_calibration_error_weights_buckets_by_sample_size():
    """A tiny badly-missed bucket must not outweigh a large well-hit one."""
    preds = [(0.5, True)] * 50 + [(0.5, False)] * 50 + [(0.9, False)] * 2
    err = calibration_error(preds, buckets=5)
    assert err is not None and err < 0.05


def test_base_rate_brier_is_the_bar_to_beat():
    preds = [(0.8, True)] * 7 + [(0.8, False)] * 3
    baseline = base_rate_brier(preds)
    # Base rate is 0.7; its Brier is 0.7·(0.3²) + 0.3·(0.7²) = 0.21.
    assert baseline == pytest.approx(0.21, abs=1e-6)
    model = sum(grade_horizon(
        band_low=0.0, band_high=1.0, hold_prob=p,
        window_low=0.1, window_high=0.9 if held else 1.9,
    )["brier"] for p, held in preds) / len(preds)
    # This particular set of predictions is worse than just saying "70%".
    assert model > baseline


def test_empty_inputs_yield_no_receipt_rather_than_a_zero():
    assert reliability_table([], buckets=5) == []
    assert calibration_error([], buckets=5) is None
    assert base_rate_brier([]) is None
