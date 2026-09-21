"""Intraday re-anchored forecast cone — the model layer.

The daily forecast (``forecast_range_model``) commits one band before the
open and then leaves it alone to be graded at 16:05.  That is a good claim
precisely *because* it is frozen: nobody can nudge it at 2 PM once the tape
has shown its hand.

It is also the wrong shape for the question a trader asks at 11:40 — "from
*here*, where does this thing stay for the next two hours?"  Answering that
needs a cone that re-anchors on the current bar and re-reads the current
dealer surface, which is a different claim with a different failure mode:
instead of one commitment a day it makes ~20, so it earns a track record in
days rather than months, and a miscalibrated probability shows up fast.

This module is the pure math.  It holds no database handle and performs no
IO so the numbers live in exactly one place and can be tested directly —
same discipline as ``forecast_range_model``.  Three pieces:

1. **A diurnal variance profile.**  Intraday volatility is not flat, so
   scaling a daily sigma by sqrt(minutes/390) overstates a midday window and
   understates one straddling the open or the close.  ``variance_fraction``
   integrates a U-shaped density instead, and the same curve is run backwards
   to turn "realized range so far" into a full-day sigma estimate.

2. **A double-barrier no-touch probability.**  The published number is
   ``hold`` — P(price never leaves the band for the whole window) — not
   P(inside the band at the end.  Those differ a lot: a path can exit at
   +40 minutes, come back, and close inside.  A terminal-value probability
   would score that as a win while the trader who got stopped out knows
   better.  ``hold_probability`` is the survival probability of a driftless
   walk between two absorbing barriers, via the method of images.

3. **Grading + reliability.**  ``grade_horizon`` turns a matured claim into
   held/Brier; ``reliability_table`` buckets many of them into predicted-vs-
   realized frequencies.  That table is the point of the whole exercise: a
   cone that says 77% should hold 77% of the time, and the only way to know
   is to publish the bucket counts and let anyone check.

Nothing here forecasts direction.  The cone is a magnitude claim about
containment, exactly like the daily band it complements.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from src.jobs.forecast_range_model import robust_persistence_anchor

# ---------------------------------------------------------------------------
# Session geometry
# ---------------------------------------------------------------------------

#: Cash-session length in minutes (09:30–16:00 ET).  The diurnal profile is
#: parameterized on the fraction of this window elapsed, so a half-day
#: (13:00 close) is handled by passing the shortened length, not by special
#: casing the curve.
SESSION_MINUTES = 390

#: Horizons published per fire, in minutes.  A horizon whose target lands
#: after the close is not issued at all rather than silently truncated to the
#: bell — a "+2h" claim that was really graded over 40 minutes is not the
#: claim the label makes.
CONE_HORIZONS_MIN = (30, 60, 90, 120)

#: First and last fire, in minutes after the open.  15 minutes of tape before
#: the first cone so the realized-vol blend has something to read; the last
#: fire is the one where the shortest horizon still completes before 16:00.
FIRST_FIRE_MIN = 15          # 09:45 ET
LAST_FIRE_MIN = 360          # 15:30 ET — +30m lands exactly on the bell
FIRE_INTERVAL_MIN = 15

# ---------------------------------------------------------------------------
# Diurnal variance profile
# ---------------------------------------------------------------------------
# Relative variance DENSITY across the session, as a function of elapsed
# fraction u in [0, 1]:
#
#     w(u) = FLOOR + OPEN_AMP·exp(-u/OPEN_DECAY) + CLOSE_AMP·exp(-(1-u)/CLOSE_DECAY)
#
# A floor plus two exponential humps — the auction at the open and the
# imbalance/0DTE pin into the close — which reproduces the familiar U without
# needing a lookup table.  Normalized by its own integral so it always
# integrates to 1 over the session; the constants therefore set the SHAPE and
# never the level, and a future calibration job can refit them from realized
# 5-min variance without touching any caller.
#
# With the defaults the curve runs ≈2.9× average density at the open, ≈0.65×
# in the midday trough, ≈2.1× at the bell.
DIURNAL_FLOOR = 0.55
DIURNAL_OPEN_AMP = 2.00
DIURNAL_OPEN_DECAY = 0.11    # ≈43 min e-folding
DIURNAL_CLOSE_AMP = 1.30
DIURNAL_CLOSE_DECAY = 0.09   # ≈35 min e-folding

# ---------------------------------------------------------------------------
# Cone construction
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# On the residual conservatism, and why it is not tuned away
# ---------------------------------------------------------------------------
# Across 9 graded sessions the cone runs about 10 points UNDERCONFIDENT at
# every horizon: bands hold more often than the published probability says.
# A single multiplier on the sigma used in hold_probability would close that
# gap on this sample in one line. It is deliberately not applied.
#
# The sample is 7 quiet sessions against 2 active ones, and the two regimes
# lean opposite ways. On the quiet days the cone is conservative by 30-40
# points; on 2026-09-16, the one genuinely volatile session (2.0-2.75x the
# expected range), it was already slightly OVERconfident. A level correction
# fitted to the majority would improve the average and make the days that
# actually matter worse — the classic fit to the common case.
#
# There is also a product reason. This model exists to publish a reliability
# table, and a known bias that the table displays honestly is worth more than
# a number tuned until it looks calibrated on a sample that is mostly calm.
# Tuning to the sample is what a scoreboard is supposed to catch.
#
# Revisit when the graded history holds a materially larger share of active
# sessions, and fit the level then — from the reliability table, per regime,
# not globally.

#: Half-width of the raw cone in horizon sigmas before gamma conditioning.
#:
#: Calibrated against the model's own output rather than assumed.  Note that
#: PATH containment is much harder than terminal containment: a ±1σ band is
#: breached at some point during the window about 63% of the time, so a 1σ
#: cone would publish a ~37% hold and be a band no one would draw.  1.5σ puts
#: the reference horizon near 73%, which is informative in both directions —
#: wide enough to be worth drawing, tight enough that miscalibration shows up
#: in the reliability table instead of hiding behind a band that always holds.
CONE_SIGMA_MULT = 1.50

#: How much TIGHTER than proportional the band gets at longer horizons.
#:
#: This constant exists because of a property that is easy to miss and fatal
#: if missed: barrier survival is scale-free.  A band drawn at exactly k·σ(h)
#: has the SAME hold probability at every horizon, because both the band and
#: the walk scale with σ — so a naive k·σ cone publishes one number four
#: times and calls it a term structure.
#:
#: The fix is not cosmetic.  Containment is a LEVEL phenomenon: dealer walls
#: sit at fixed prices, and a longer horizon gives price more chances to
#: reach one.  Where walls are known, ``_lean_to_wall`` models that directly.
#: This term is the residual for everything the walls do not capture, and it
#: keeps the term structure honest when no wall is in range: band width grows
#: as ``vf^(0.5 − δ)`` rather than ``vf^0.5``, so hold decays with horizon.
#:
#: δ is the one frankly empirical constant in this model.  It is set to a
#: modest default and is exactly what the reliability table is for — if the
#: +2h bucket comes in persistently under its predicted hold, δ is too small
#: and the receipt will say so in public before anyone has to argue about it.
#: Fitted against 3,024 graded claims over 9 sessions (v1_3), where realized
#: hold decayed 6.8 points across the horizon ladder while the published
#: probability decayed 15.9 — 2.3x too steep. 0.07 reproduces the observed
#: decay. The reference horizon is untouched by construction (its decay
#: factor is 1.0), so this flattens the term structure without shifting the
#: level, which matters because the level should NOT be fitted here — see
#: the note on residual conservatism below.
CONE_TERM_DECAY = 0.07

#: Gamma conditioning bounds.  Long dealer gamma damps realized movement
#: (hedging leans against the tape); short gamma amplifies it.  The multiplier
#: scales the PROCESS sigma and is clamped so a single extreme GEX print
#: cannot collapse or explode the cone.
GAMMA_MULT_MIN = 0.78
GAMMA_MULT_MAX = 1.35

#: How much of the gamma tilt the drawn band absorbs, against the full tilt
#: applied to the process sigma.
#:
#: This split is what makes gamma affect the hold probability in the direction
#: a reader expects.  Applying the same multiplier to both would cancel — the
#: band and the walk would scale together and gamma would move the band
#: without moving the odds — and applying it to the band ALONE inverts the
#: result: tightening the cone on a pinned day while leaving the process
#: untouched publishes a LOWER hold on the day price is most contained, which
#: is exactly backwards.
#:
#: Damping below 1.0 encodes that dealer gamma changes realized volatility
#: more than it changes where a level-anchored band belongs — the band is
#: partly structural (the walls do not move because gamma got bigger), so it
#: should not breathe as hard as the process does.
GAMMA_BAND_DAMPING = 0.50
#: |net_gex_at_spot| that saturates the damping/amplifying pull.
GAMMA_SATURATION = 1.5e9
#: Maximum fractional tightening/widening from the gamma term alone.
GAMMA_TILT = 0.22

#: Flip-proximity widening.  Sitting on the gamma flip is the unstable place
#: — dealer hedging changes sign there, so containment is worth less.
FLIP_PROX_SPAN = 0.006       # |spot-flip|/spot within this counts as "near"
FLIP_PROX_TILT = 0.18        # max fractional widening when sitting on it

#: Wall lean.  A wall inside the raw cone is where hedging resists, so the
#: band leans toward it rather than through it — but only partially, because
#: walls break and a band that stopped dead at every wall would be a liar on
#: exactly the days that matter.
WALL_LEAN = 0.35             # fraction of the overshoot pulled back to the wall
WALL_LEAN_MIN_FRACTION = 0.45  # never pull an edge inside this × raw half-width

#: Blend weight on the realized-so-far sigma against the implied sigma.  Early
#: in the session realized is a thin sample, so the weight ramps in with the
#: square root of elapsed variance rather than being applied flat from 09:45.
#:
#: Raised from 0.55 after three backfilled sessions came back with realized
#: range running about half the vol basis at every horizon.  The old cap was
#: indefensible at the tail of the session: by 15:30 the realized read has
#: seen ~88% of the day's variance and the implied number is a stale morning
#: guess, yet implied still carried 49% of the weight.  At 0.85 the weight
#: runs ~0.26 at 09:45 and ~0.80 at 15:30 — realized dominates once it has
#: earned the right to, and implied never vanishes entirely because it is the
#: only input that knows about a scheduled event later in the day.
REALIZED_BLEND_MAX = 0.85

#: Bounds on the committed per-symbol vol-basis multiplier, matching the
#: range ``forecast_calibrate`` already constrains it to.
VOL_BASIS_MULT_MIN = 0.45
VOL_BASIS_MULT_MAX = 1.40

#: Bounds on the morning's committed expected-vol ratio when the cone adopts
#: it.  A ratio is a published claim, not a measurement, so a bad one is
#: clamped rather than allowed to collapse or explode the whole session's
#: bands.  The range matches the daily model's own VOL_RATIO_MIN/MAX.
CONE_VOL_RATIO_MIN = 0.45
CONE_VOL_RATIO_MAX = 1.90

#: Published hold probabilities are clamped so a degenerate band never reads
#: as a certainty in either direction.
HOLD_PROB_MIN = 0.02
HOLD_PROB_MAX = 0.98

#: Absolute floor/ceiling on the cone half-width as a fraction of spot, the
#: same guardrail shape the daily model uses.
MIN_HALF_FRACTION = 0.0004
MAX_HALF_FRACTION = 0.0250

#: How the volatility used for the PROBABILITY scales with the horizon.
#:
#: Sigma over a window is ``daily_sigma * variance_fraction ** exponent``.
#: 0.5 is Brownian motion — variance linear in time — and was the model's
#: implicit assumption from the start.
#:
#: Mind the direction, because it is the opposite of what the phrase
#: "more contained" suggests. The variance fraction is BELOW 1, so a LARGER
#: exponent yields a SMALLER sub-daily sigma against the same daily figure.
#: Raising it above 0.5 therefore says: for the range this day actually
#: delivered, the path spent less of it wandering inside short windows — which
#: is what nine graded sessions show, and what dealer hedging around a pin
#: should do to a tape. Dropping it below 0.5 says the reverse and makes the
#: cone far more pessimistic.
#:
#: This is deliberately a separate knob from CONE_TERM_DECAY, which shapes the
#: BAND. Conflating them is what made the v1_4 fit miss: widening the band
#: raises the realized hold rate too, because the band is the thing being
#: graded, so band geometry can never close a gap that lives in the process
#: assumption. Held at 0.5 until a holdout says otherwise — see cone_tune.
CONE_PATH_EXPONENT = 0.5

#: Terms kept on each side of the image series in ``hold_probability``.  The
#: reflections decay super-exponentially; 6 is far past the point where any
#: term moves the fourth decimal.
_IMAGE_TERMS = 6

#: Stamped on every committed claim.
#:
#: BUMP THIS whenever the math that produces a band or a probability changes.
#: It is not decoration: the calibration report groups by it, and a sample
#: that silently mixes versions reads as one model performing badly rather
#: than two models measured together.
#:
#: That is not hypothetical — it already happened. This string sat at v1_0
#: through the vol anchor, the measured anchor, the committed vol basis and
#: the realized-blend change, so nine backfilled sessions spanning two very
#: different models all claimed to be the same one, and the aggregate looked
#: like a regression that was really an averaging artifact.
#:
#: Lineage:
#:   v1_0  original — implied move only, no vol call
#:   v1_1  committed expected_vol_ratio adopted (which defaults to 1.0, so
#:         this changed almost nothing in practice)
#:   v1_2  measured anchor: median of prior GRADED realized ratios
#:   v1_3  per-symbol vol_range_basis_mult applied; realized blend to 0.85
#:   v1_4  term decay fitted to 9 graded sessions (0.12 -> 0.07)
MODEL_VERSION = "cone_v1_4"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _normal_cdf(x: float) -> float:
    """Standard-normal CDF via erf — no scipy dependency, mirroring
    ``forecast_range_model._normal_cdf`` so both layers agree to the bit."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _diurnal_cdf(u: float) -> float:
    """Unnormalized cumulative variance from the open to elapsed fraction ``u``.

    The profile is a floor plus two exponentials, so this integrates in closed
    form — no quadrature, no lookup table, and exact by construction:

        ∫₀ˣ w = F·x + A·λa·(1 − e^{−x/λa}) + B·λb·(e^{−(1−x)/λb} − e^{−1/λb})
    """
    u = _clamp(u, 0.0, 1.0)
    floor_term = DIURNAL_FLOOR * u
    open_term = DIURNAL_OPEN_AMP * DIURNAL_OPEN_DECAY * (
        1.0 - math.exp(-u / DIURNAL_OPEN_DECAY)
    )
    close_term = DIURNAL_CLOSE_AMP * DIURNAL_CLOSE_DECAY * (
        math.exp(-(1.0 - u) / DIURNAL_CLOSE_DECAY)
        - math.exp(-1.0 / DIURNAL_CLOSE_DECAY)
    )
    return floor_term + open_term + close_term


#: Full-session integral — the normalizer that makes ``variance_fraction``
#: return a true fraction of the day's variance.
_DIURNAL_TOTAL = _diurnal_cdf(1.0)


def diurnal_density(u: float) -> float:
    """Normalized relative variance density at elapsed fraction ``u``.

    1.0 means "an average minute of the session".  Exposed for tests and for
    the calibration job that will eventually refit the constants.
    """
    u = _clamp(u, 0.0, 1.0)
    raw = (
        DIURNAL_FLOOR
        + DIURNAL_OPEN_AMP * math.exp(-u / DIURNAL_OPEN_DECAY)
        + DIURNAL_CLOSE_AMP * math.exp(-(1.0 - u) / DIURNAL_CLOSE_DECAY)
    )
    return raw / _DIURNAL_TOTAL


def variance_fraction(
    start_min: float, end_min: float, session_minutes: int = SESSION_MINUTES
) -> float:
    """Fraction of the session's total variance expected in ``[start, end]``.

    Both bounds are minutes after the open.  Returns 0.0 for an empty or
    inverted window.  This is the whole reason the cone does not use naive
    sqrt-of-time: the 30 minutes after the open carry several times the
    variance of 30 minutes at 12:30, and a cone that ignored that would be
    too tight in the morning and too loose at lunch — in both cases wrong in
    a way the hold probability would faithfully report as miscalibration.
    """
    if session_minutes <= 0:
        return 0.0
    lo = _clamp(start_min / session_minutes, 0.0, 1.0)
    hi = _clamp(end_min / session_minutes, 0.0, 1.0)
    if hi <= lo:
        return 0.0
    return (_diurnal_cdf(hi) - _diurnal_cdf(lo)) / _DIURNAL_TOTAL


def horizon_sigma(
    *,
    daily_sigma: float,
    elapsed_min: float,
    horizon_min: float,
    session_minutes: int = SESSION_MINUTES,
) -> float:
    """Dollar 1-σ for the window ``[elapsed, elapsed + horizon]``.

    ``daily_sigma`` is the full-session 1-σ move in dollars — the same
    ``implied_move`` the daily forecast commits each morning, so the intraday
    cone and the daily band are denominated in one vol basis and a
    disagreement between them is a real disagreement, not a units artifact.
    """
    if daily_sigma <= 0:
        return 0.0
    frac = variance_fraction(elapsed_min, elapsed_min + horizon_min, session_minutes)
    return daily_sigma * math.sqrt(frac)


def realized_daily_sigma(
    *,
    session_high: Optional[float],
    session_low: Optional[float],
    elapsed_min: float,
    session_minutes: int = SESSION_MINUTES,
    range_over_sigma: float = math.sqrt(8.0 / math.pi),
) -> Optional[float]:
    """Back out a full-day sigma from the range realized SO FAR.

    Runs the diurnal profile backwards.  The high-low range observed over the
    first ``elapsed_min`` is a Parkinson estimate of the sigma accumulated in
    that window (E[range] ≈ √(8/π)·σ); dividing by the variance fraction that
    window should have carried extrapolates to the session.  Returns None when
    the inputs are unusable or the elapsed window is too thin to extrapolate
    from without amplifying noise.
    """
    if session_high is None or session_low is None:
        return None
    observed_range = float(session_high) - float(session_low)
    if observed_range <= 0:
        return None
    frac = variance_fraction(0.0, elapsed_min, session_minutes)
    # Below ~4% of the day's variance the divisor is small enough that a
    # single wide bar would dominate the extrapolation.
    if frac < 0.04:
        return None
    sigma_so_far = observed_range / range_over_sigma
    return sigma_so_far / math.sqrt(frac)


def resolve_vol_anchor(
    *,
    trailing_ratios: Sequence[float],
    committed_ratio: Optional[float],
) -> tuple[Optional[float], str]:
    """How much of a normal day's range today should deliver, and where that
    number came from.

    Order matters, and the first version got it wrong by skipping straight to
    the last resort:

    1. **measured** — the median of prior sessions' GRADED realized ratios.
       Vol clusters, so what the tape has actually been delivering is the
       best available statement about what it will deliver next.
    2. **committed** — the morning forecast's prediction. A real fallback,
       but a weak one: ``predict_expected_vol`` itself degrades to a neutral
       1.0 without enough history, so adopting it on a cold start silently
       reasserts "today is average" — the exact error this is meant to fix.
       That is what happened on the first attempt, and why the sigma moved
       12% when it needed to move about 40%.
    3. **none** — no claim. The caller uses the raw implied move and the
       cone is honestly wide rather than confidently wrong.

    Returning the source alongside the value is not bookkeeping. Twice now the
    only way to tell what this model actually did was to invert the published
    bands by hand; a claim should say what it was built from.
    """
    measured = robust_persistence_anchor(list(trailing_ratios or ()))
    if measured is not None and measured > 0:
        return _clamp(measured, CONE_VOL_RATIO_MIN, CONE_VOL_RATIO_MAX), "measured"
    if committed_ratio is not None:
        try:
            value = float(committed_ratio)
        except (TypeError, ValueError):
            return None, "none"
        if value > 0:
            return _clamp(value, CONE_VOL_RATIO_MIN, CONE_VOL_RATIO_MAX), "committed"
    return None, "none"


def blended_daily_sigma(
    *,
    implied_sigma: Optional[float],
    realized_sigma: Optional[float],
    elapsed_min: float,
    session_minutes: int = SESSION_MINUTES,
    expected_vol_ratio: Optional[float] = None,
    vol_basis_mult: Optional[float] = None,
) -> tuple[float, list[str]]:
    """Blend the implied and realized full-day sigmas, weighting realized by
    how much of the day it has actually seen.

    At 09:45 the realized estimate rests on 15 minutes and gets little weight;
    by 15:30 it rests on ~88% of the session's variance and gets most of
    ``REALIZED_BLEND_MAX``.  Returns ``(sigma, rationale)`` and falls back to
    whichever input exists when the other is missing.

    ``expected_vol_ratio`` is the morning forecast's committed call on how
    much of a NORMAL day's range today should deliver, and it scales the
    implied leg before the blend.

    That scaling is not a refinement, it is the correction for this model's
    largest early error.  The first version used the raw implied move, which
    is arithmetically identical to asserting a ratio of 1.0 — every day is an
    average day — and three backfilled sessions came back at roughly 0.5.
    The bands were sized for movement that did not arrive, so nearly
    everything held and the published probabilities meant nothing.

    The daily model had already learned this and said so in its own
    docstrings: vol clusters, so how the tape has actually been trading beats
    structure alone, and anchoring on that is what stops the prediction
    defaulting to "normal".  The cone now inherits that call instead of
    re-deciding it worse.

    Worth stating plainly: the daily model publishes expected_vol_ratio
    UNGRADED, because it does not beat a majority-bucket baseline.  Adopting
    it inherits a claim its own authors will not score.  It is still the
    better of the two available options — a mediocre estimator beats a
    constant that is wrong by a factor of two — and it ties the two surfaces
    together, so improving the vol call improves both at once.
    """
    notes: list[str] = []
    if implied_sigma is not None and implied_sigma > 0 and vol_basis_mult is not None:
        try:
            mult = _clamp(float(vol_basis_mult), VOL_BASIS_MULT_MIN, VOL_BASIS_MULT_MAX)
        except (TypeError, ValueError):
            mult = None
        if mult is not None:
            implied_sigma = implied_sigma * mult
            notes.append(f"implied re-centred on the committed vol basis ({mult:.2f})")
    if implied_sigma is not None and implied_sigma > 0 and expected_vol_ratio is not None:
        try:
            ratio = _clamp(
                float(expected_vol_ratio), CONE_VOL_RATIO_MIN, CONE_VOL_RATIO_MAX
            )
        except (TypeError, ValueError):
            ratio = None
        if ratio is not None:
            implied_sigma = implied_sigma * ratio
            notes.append(f"implied scaled to the morning vol call ({ratio:.2f}x a normal day)")
    have_implied = implied_sigma is not None and implied_sigma > 0
    have_realized = realized_sigma is not None and realized_sigma > 0
    if have_implied and have_realized:
        seen = variance_fraction(0.0, elapsed_min, session_minutes)
        weight = REALIZED_BLEND_MAX * math.sqrt(_clamp(seen, 0.0, 1.0))
        sigma = (1.0 - weight) * implied_sigma + weight * realized_sigma
        notes.append(
            f"sigma blend: {(1 - weight) * 100:.0f}% implied / {weight * 100:.0f}% realized"
        )
        return sigma, notes
    if realized_sigma is not None and realized_sigma > 0:
        notes.append("sigma from realized only (no implied move committed)")
        return realized_sigma, notes
    if implied_sigma is not None and implied_sigma > 0:
        notes.append("sigma from implied only (realized not yet usable)")
        return implied_sigma, notes
    return 0.0, ["sigma unavailable"]


# ---------------------------------------------------------------------------
# Double-barrier survival
# ---------------------------------------------------------------------------


def hold_probability(
    *,
    spot: float,
    band_low: float,
    band_high: float,
    sigma: float,
) -> Optional[float]:
    """P(a driftless path starting at ``spot`` never leaves ``[low, high]``).

    This is the survival probability of Brownian motion between two absorbing
    barriers, by the method of images.  With the walk started at 0, barriers
    at L<0<U, width W = U−L and total stdev s:

        P = Σₙ [ Φ((U−2nW)/s) − Φ((L−2nW)/s)
                 − Φ((U−2L−2nW)/s) + Φ((−L−2nW)/s) ]

    summed over n ∈ [−N, N].  The single-barrier reflection used for the daily
    level-touch odds (``_barrier_touch_prob``) is the W→∞ limit of this and is
    NOT a substitute: treating the two edges independently and multiplying
    would double-count the paths that would have touched both, biasing the
    published hold probability upward exactly where the band is tight and the
    number matters most.

    Returns None when the inputs are unusable, and clamps to
    ``[HOLD_PROB_MIN, HOLD_PROB_MAX]`` so a degenerate band never publishes a
    certainty.  Drift is deliberately omitted: the cone makes no directional
    claim, so assuming zero drift is the honest prior rather than a
    simplification that quietly smuggles one in.
    """
    if sigma is None or sigma <= 0:
        return None
    lower = float(band_low) - float(spot)
    upper = float(band_high) - float(spot)
    if not (lower < 0.0 < upper):
        # Spot outside its own band — the claim is already dead on arrival.
        return HOLD_PROB_MIN
    width = upper - lower
    s = float(sigma)

    total = 0.0
    for n in range(-_IMAGE_TERMS, _IMAGE_TERMS + 1):
        shift = 2.0 * n * width
        total += (
            _normal_cdf((upper - shift) / s)
            - _normal_cdf((lower - shift) / s)
            - _normal_cdf((upper - 2.0 * lower - shift) / s)
            + _normal_cdf((-lower - shift) / s)
        )
    return _clamp(total, HOLD_PROB_MIN, HOLD_PROB_MAX)


# ---------------------------------------------------------------------------
# Cone construction
# ---------------------------------------------------------------------------


@dataclass
class ConeInputs:
    """Everything the cone consumes at one fire.

    Only ``symbol``, ``spot`` and ``elapsed_min`` are hard requirements; every
    other field degrades gracefully so a thin surface still produces a claim
    rather than a gap in the track record.
    """

    symbol: str
    spot: float
    elapsed_min: float                     # minutes after the 09:30 open

    # Vol basis.  ``implied_move`` is the committed full-day 1-σ dollar move
    # from the morning forecast; session high/low drive the realized estimate.
    implied_move: Optional[float] = None
    #: The morning forecast's committed expected_vol_ratio — the PREDICTED
    #: realized range as a multiple of a normal day's.  Used only as a
    #: fallback; see ``resolve_vol_anchor``.
    expected_vol_ratio: Optional[float] = None
    #: GRADED realized vol ratios from prior sessions, oldest-first.  Their
    #: median is the preferred anchor: a measurement rather than a forecast.
    trailing_vol_ratios: Sequence[float] = ()
    #: The per-symbol ``vol_range_basis_mult`` the morning forecast committed.
    #:
    #: This is the daily model's nightly-learned correction for the
    #: variance-risk premium — the structural fact that realized vol runs
    #: below implied — and the cone spent three rounds rediscovering its
    #: absence.  Without it the cone's "normal day" is the daily model's
    #: normal day divided by this scalar, so the two surfaces disagree about
    #: what an average session looks like while grading against the same tape.
    vol_basis_mult: Optional[float] = None
    session_high: Optional[float] = None
    session_low: Optional[float] = None

    # Current dealer surface (re-read every fire — this is the whole point of
    # re-anchoring, since walls migrate strike to strike through a session).
    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    gamma_flip: Optional[float] = None
    net_gex_at_spot: Optional[float] = None

    session_minutes: int = SESSION_MINUTES
    horizons: Sequence[int] = CONE_HORIZONS_MIN


@dataclass
class ConeHorizon:
    """One falsifiable claim: a band and a hold probability at one horizon."""

    horizon_min: int
    band_low: float
    band_high: float
    hold_prob: Optional[float]
    sigma: float


@dataclass
class ConeResult:
    horizons: list[ConeHorizon] = field(default_factory=list)
    daily_sigma: float = 0.0
    gamma_mult: float = 1.0
    #: The vol anchor actually applied, and where it came from
    #: ("measured" | "committed" | "none"). Published so a reader never has to
    #: invert the bands to find out what the cone assumed.
    vol_ratio_applied: Optional[float] = None
    vol_ratio_source: str = "none"
    model_version: str = MODEL_VERSION
    rationale: list[str] = field(default_factory=list)


def _gamma_multiplier(inp: ConeInputs) -> tuple[float, list[str]]:
    """Multiplier on the PROCESS sigma from the current dealer surface.

    Two effects, both bounded.  Long gamma at spot damps realized movement,
    short gamma amplifies it; and proximity to the flip widens regardless of
    sign, because that is where the hedging response changes character and
    containment is worth least.

    The returned value scales the volatility the cone assumes, not directly
    the band that is drawn — see ``GAMMA_BAND_DAMPING`` for why those are
    deliberately different numbers.
    """
    notes: list[str] = []
    mult = 1.0

    net_gex = inp.net_gex_at_spot
    if net_gex is not None:
        saturated = _clamp(float(net_gex) / GAMMA_SATURATION, -1.0, 1.0)
        # Positive net gamma (dealers long) tightens; negative widens.
        mult *= 1.0 - GAMMA_TILT * saturated
        if saturated > 0.05:
            notes.append(f"long gamma at spot tightens {GAMMA_TILT * saturated * 100:.0f}%")
        elif saturated < -0.05:
            notes.append(f"short gamma at spot widens {GAMMA_TILT * -saturated * 100:.0f}%")

    if inp.gamma_flip is not None and inp.spot > 0:
        distance = abs(float(inp.spot) - float(inp.gamma_flip)) / float(inp.spot)
        if distance < FLIP_PROX_SPAN:
            nearness = 1.0 - (distance / FLIP_PROX_SPAN)
            mult *= 1.0 + FLIP_PROX_TILT * nearness
            widening = FLIP_PROX_TILT * nearness * 100
            notes.append(f"sitting near the gamma flip widens {widening:.0f}%")

    return _clamp(mult, GAMMA_MULT_MIN, GAMMA_MULT_MAX), notes


def _lean_to_wall(
    edge: float, spot: float, wall: Optional[float], raw_half: float, upward: bool
) -> tuple[float, bool]:
    """Pull a cone edge partway back to a wall it has overshot.

    Returns ``(edge, leaned)``.  The pull is partial (``WALL_LEAN``) and
    floored at ``WALL_LEAN_MIN_FRACTION`` of the raw half-width, because walls
    are where hedging resists, not where price is forbidden — a cone that
    stopped dead at every wall would be most wrong on the days the wall breaks,
    which are the days a reader most needs it to have been honest.
    """
    if wall is None:
        return edge, False
    wall_f = float(wall)
    if upward:
        if not (spot < wall_f < edge):
            return edge, False
        pulled = edge - WALL_LEAN * (edge - wall_f)
        floor = spot + WALL_LEAN_MIN_FRACTION * raw_half
        return max(pulled, floor), True
    if not (edge < wall_f < spot):
        return edge, False
    pulled = edge + WALL_LEAN * (wall_f - edge)
    ceiling = spot - WALL_LEAN_MIN_FRACTION * raw_half
    return min(pulled, ceiling), True


def compute_cone(
    inp: ConeInputs, *, path_exponent: float = CONE_PATH_EXPONENT
) -> ConeResult:
    """Build the full set of horizon claims for one fire.

    Order matters and mirrors the daily model's pipeline: establish the vol
    basis, condition it on the dealer surface, then let structure (the walls)
    lean the edges — never the reverse, so a missing wall degrades the shape
    of the band without changing its scale.
    """
    result = ConeResult()
    spot = float(inp.spot)
    if spot <= 0:
        result.rationale.append("no usable spot — no cone")
        return result

    realized = realized_daily_sigma(
        session_high=inp.session_high,
        session_low=inp.session_low,
        elapsed_min=inp.elapsed_min,
        session_minutes=inp.session_minutes,
    )
    vol_anchor, vol_source = resolve_vol_anchor(
        trailing_ratios=inp.trailing_vol_ratios,
        committed_ratio=inp.expected_vol_ratio,
    )
    result.vol_ratio_applied = vol_anchor
    result.vol_ratio_source = vol_source
    daily_sigma, sigma_notes = blended_daily_sigma(
        implied_sigma=inp.implied_move,
        realized_sigma=realized,
        elapsed_min=inp.elapsed_min,
        session_minutes=inp.session_minutes,
        expected_vol_ratio=vol_anchor,
        vol_basis_mult=inp.vol_basis_mult,
    )
    if vol_anchor is not None:
        result.rationale.append(f"vol anchor {vol_anchor:.2f}x ({vol_source})")
    result.daily_sigma = daily_sigma
    result.rationale.extend(sigma_notes)
    if daily_sigma <= 0:
        result.rationale.append("no vol basis — no cone")
        return result

    vol_mult, gamma_notes = _gamma_multiplier(inp)
    # The band absorbs only part of the tilt the process takes (see
    # GAMMA_BAND_DAMPING), which is what lets gamma move the hold probability
    # instead of cancelling out of it.
    band_mult = 1.0 + GAMMA_BAND_DAMPING * (vol_mult - 1.0)
    result.gamma_mult = vol_mult
    result.rationale.extend(gamma_notes)

    remaining = max(0.0, inp.session_minutes - inp.elapsed_min)
    publishable = sorted(h for h in inp.horizons if h <= remaining)
    if not publishable:
        result.rationale.append("no horizon completes before the close")
        return result

    # Reference window for the term-decay term: the shortest horizon actually
    # published at this fire, so that one is drawn at exactly CONE_SIGMA_MULT
    # and every longer horizon tightens relative to it.  Anchoring on the
    # published set rather than a fixed 30 minutes keeps the late-session
    # fires — where only the short horizons survive — on the same footing as
    # a 10 AM fire.
    ref_vf = variance_fraction(
        inp.elapsed_min, inp.elapsed_min + publishable[0], inp.session_minutes
    )
    leaned_up = leaned_down = False

    for horizon in publishable:
        vf = variance_fraction(
            inp.elapsed_min, inp.elapsed_min + horizon, inp.session_minutes
        )
        base_sigma = daily_sigma * math.sqrt(vf) if vf > 0 else 0.0
        if base_sigma <= 0:
            continue
        # The volatility the cone assumes for this window, after gamma. The
        # path exponent scales the PROBABILITY's sigma only; the band keeps
        # ordinary sqrt-variance growth (see CONE_PATH_EXPONENT).
        sigma_h = daily_sigma * (vf ** path_exponent) * vol_mult

        # Sub-proportional widening (see CONE_TERM_DECAY).  Without this the
        # band is a fixed multiple of sigma and the hold probability is the
        # same number at every horizon.
        decay = (vf / ref_vf) ** -CONE_TERM_DECAY if ref_vf > 0 else 1.0
        raw_half = CONE_SIGMA_MULT * base_sigma * band_mult * decay
        raw_half = _clamp(raw_half, MIN_HALF_FRACTION * spot, MAX_HALF_FRACTION * spot)

        high, up = _lean_to_wall(spot + raw_half, spot, inp.call_wall, raw_half, upward=True)
        low, down = _lean_to_wall(spot - raw_half, spot, inp.put_wall, raw_half, upward=False)
        leaned_up = leaned_up or up
        leaned_down = leaned_down or down

        # Round to what gets PUBLISHED, then derive the probability from those
        # exact numbers.  Computing it from the unrounded edges would leave a
        # published band and a published probability that describe subtly
        # different claims, and would make the page impossible to audit — a
        # reader recomputing the number from the band would not reproduce it.
        pub_low = round(low, 4)
        pub_high = round(high, 4)
        pub_sigma = round(sigma_h, 4)

        result.horizons.append(
            ConeHorizon(
                horizon_min=int(horizon),
                band_low=pub_low,
                band_high=pub_high,
                # Computed from the FINAL band, after the wall lean and the
                # rounding — otherwise the number describes a band that was
                # never drawn.
                hold_prob=hold_probability(
                    spot=spot, band_low=pub_low, band_high=pub_high, sigma=pub_sigma
                ),
                sigma=pub_sigma,
            )
        )

    if leaned_up:
        result.rationale.append("upper edge leans to the call wall")
    if leaned_down:
        result.rationale.append("lower edge leans to the put wall")
    return result


# ---------------------------------------------------------------------------
# Grading
# ---------------------------------------------------------------------------


def grade_horizon(
    *,
    band_low: float,
    band_high: float,
    hold_prob: Optional[float],
    window_low: Optional[float],
    window_high: Optional[float],
) -> dict[str, Any]:
    """Grade one matured horizon against what the tape actually did.

    ``window_low``/``window_high`` are the extremes over the OPEN interval
    (t, t+h] — the anchor bar itself is excluded, since spot starts inside the
    band by construction and including it could only ever flatter the verdict.

    ``held`` is a containment test over the whole window, not a check of where
    price finished: a path that pierced the band at +40 minutes and closed back
    inside did not hold, and grading it as a win would make the published
    probability mean something other than what the page says it means.
    """
    if window_low is None or window_high is None:
        return {"held": None, "brier": None}
    held = float(window_low) >= float(band_low) and float(window_high) <= float(band_high)
    brier: Optional[float] = None
    if hold_prob is not None:
        brier = round((float(hold_prob) - (1.0 if held else 0.0)) ** 2, 6)
    return {"held": held, "brier": brier}


def reliability_table(
    predictions: Sequence[tuple[float, bool]], buckets: int = 5
) -> list[dict[str, Any]]:
    """Bucket ``(hold_prob, held)`` pairs into predicted-vs-realized rows.

    This is the receipt that the cone exists to produce.  A Brier score alone
    can be gamed by never leaving the middle of the distribution; a reliability
    table cannot hide, because it shows what actually happened inside each
    confidence band along with the sample size behind it.

    Returns one row per non-empty bucket with ``{bucket_low, bucket_high, n,
    predicted, realized, gap}``.  Empty buckets are omitted rather than
    rendered as zeroes — a band nobody forecast into is not a band that was
    wrong.
    """
    if buckets <= 0:
        return []
    edges = [i / buckets for i in range(buckets + 1)]
    rows: list[dict[str, Any]] = []
    for i in range(buckets):
        lo, hi = edges[i], edges[i + 1]
        # Last bucket is closed on the right so p == 1.0 lands somewhere.
        members = [
            (p, h)
            for p, h in predictions
            if p is not None and (lo <= p < hi or (i == buckets - 1 and p == hi))
        ]
        if not members:
            continue
        n = len(members)
        predicted = sum(p for p, _ in members) / n
        realized = sum(1 for _, h in members if h) / n
        rows.append(
            {
                "bucket_low": round(lo, 4),
                "bucket_high": round(hi, 4),
                "n": n,
                "predicted": round(predicted, 4),
                "realized": round(realized, 4),
                "gap": round(realized - predicted, 4),
            }
        )
    return rows


def calibration_error(
    predictions: Sequence[tuple[float, bool]], buckets: int = 5
) -> Optional[float]:
    """Sample-weighted mean |realized − predicted| across reliability buckets.

    The single number to put next to the Brier score.  Lower is better; 0 means
    every confidence band came in exactly as advertised.
    """
    rows = reliability_table(predictions, buckets=buckets)
    if not rows:
        return None
    total = sum(r["n"] for r in rows)
    if total == 0:
        return None
    return round(sum(abs(r["gap"]) * r["n"] for r in rows) / total, 4)


def base_rate_brier(predictions: Sequence[tuple[float, bool]]) -> Optional[float]:
    """Brier score of the honest strawman: always predict the base rate.

    The cone has to beat this to have earned anything.  Same role the
    majority-bucket baseline plays for the daily vol call — and the same
    consequence if it loses, which is that the claim gets published marked
    informational rather than quietly counted as a win.
    """
    outcomes = [1.0 if h else 0.0 for _, h in predictions]
    if not outcomes:
        return None
    base = sum(outcomes) / len(outcomes)
    return round(sum((base - o) ** 2 for o in outcomes) / len(outcomes), 6)
