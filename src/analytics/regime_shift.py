"""Gamma Regime Shift — the *derivative* of the dealer-gamma surface.

Every GEX view on the market (ours included, until now) shows a **snapshot**:
where dealer gamma sits right now.  This module computes what CHANGED between
two snapshots and reduces it to a read a trader can take in at a glance:

    "Dealers dramatically flipped long gamma into 763-770."

It answers three questions, and each has its own entry point here:

1. **How much did gamma shift, by strike and expiration?**
   :func:`shift_rows` — an expiry-aware per-strike diff.

2. **How much gamma disappears at the next expiration, and is that a lot?**
   :func:`rolloff_tranches` — dealer gamma grouped by expiration, with the
   nearest tranche broken out by strike.

3. **What does the shift mean — stabilizing, fragile, bullish, bearish?**
   :func:`weighted_scores` + :func:`classify` — two orthogonal scores and the
   quadrant they land in.

--------------------------------------------------------------------------
Why two scores and not one
--------------------------------------------------------------------------

A single signed number per strike ("gamma went up here") cannot express what
a trader needs, because the same sign means opposite things on opposite sides
of spot.  Gamma BUILDING at a strike **below** spot is a floor firming up;
the identical build **above** spot is a ceiling coming down on the tape.  A
view that colours both green — which is exactly what a raw dNet ladder does —
throws that away and forces the reader to re-derive it strike by strike.

So the per-strike delta vector is projected onto two axes:

``stability``
    Proximity-weighted signed change.  Positive = more long gamma near spot =
    dealers hedge AGAINST moves = pinning, vol suppression.  Negative = more
    short gamma near spot = dealers hedge WITH moves = accelerant.

``lean``
    The same weighted change, signed by which side of spot it landed on.
    Positive = the change is supportive (building below / eroding above).
    Negative = the change is capping (building above / eroding below).

The two are close to independent: a book can firm up symmetrically (stability
up, lean flat), or roll its gamma from below spot to above it without changing
total near-spot gamma at all (lean down, stability flat).  Collapsing them
into one number loses precisely the distinction the reader is after.

--------------------------------------------------------------------------
Why the proximity weight
--------------------------------------------------------------------------

Dealer gamma only produces hedging flow where spot can actually reach.  A
$4B build 8% out of the money changes nothing about how the tape trades
today; the same build one expected-move away changes everything.  Weighting
by a Gaussian kernel in *expected-move* units encodes that, and it is what
stops a far-OTM roll (very common on monthly-expiration weeks) from
dominating the score.

The kernel width defaults to one expected daily move.  The score is
deliberately insensitive to the exact width — it sets relative weighting, not
scale — but it must not be so wide that far-OTM strikes carry real weight.

--------------------------------------------------------------------------
Why the diff is expiry-aware
--------------------------------------------------------------------------

This is the trap that makes a naive day-over-day GEX diff worse than useless.
Yesterday's 0DTE contracts do not exist today.  Differencing "all expirations
today" against "all expirations yesterday" therefore books the ENTIRE expired
tranche as a shed — on SPY, routinely a larger number than any real
repositioning — and the read prints DETERIORATING every single morning.

:func:`shift_rows` restricts both sides to the expirations they have in
common and reports what it dropped, so the shift measures repositioning and
the roll-off measures expiry.  They are different questions and this module
never mixes them.

--------------------------------------------------------------------------
Units
--------------------------------------------------------------------------

Every gamma quantity in and out of this module is dollar GEX in the site-wide
per-1% convention (``gamma x OI x 100 x S^2 x 0.01``, calls positive / puts
negative), the same basis ``gex_by_strike.net_gex`` is stored in and every
other surface renders.  Nothing here rescales; callers that display in
per-1-point apply the site's usual factor at the edge.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable, Mapping, Optional, Sequence

# ---------------------------------------------------------------------------
# Tunables.  These are the only magic numbers in the module; each is stated
# with the reasoning that picked it so a future change is an argument, not a
# guess.
# ---------------------------------------------------------------------------

#: Gaussian kernel width as a fraction of spot, when the caller supplies no
#: vol-derived width.  1% of spot approximates one expected daily move on a
#: major index ETF at a typical ~15 vol (S x IV / sqrt(252) ~= 0.0094 x S).
#: Strikes ~2.5 kernel-widths out contribute <2% of their dollar change, which
#: is the intent: unreachable gamma should not move the read.
DEFAULT_SIGMA_PCT = 0.01

#: Below this combined z-magnitude the shift is indistinguishable from a
#: normal session's churn and the read is QUIET.  Set at 0.75 sigma rather
#: than 1.0 so a genuine-but-small repositioning still gets named; the adverb
#: ladder below then keeps it honest about size.
QUIET_Z = 0.75

#: |z| ladder -> adverb.  These are the words that reach the headline, so
#: they are pinned to distribution facts rather than taste: ~1 sigma is a
#: routine day, ~2 sigma is roughly a fortnightly event, >2.5 sigma is rare
#: enough to deserve "dramatically".
_ADVERB_LADDER: tuple[tuple[float, str], ...] = (
    (QUIET_Z, "barely"),
    (1.5, "modestly"),
    (2.5, "clearly"),
    (math.inf, "dramatically"),
)

#: Fraction of total |change| the concentration band must cover.  60% is the
#: point where the band stays tight enough to name a level ("763-770") on a
#: typical session instead of degenerating to the whole window.
BAND_COVERAGE = 0.60

#: A concentration band wider than this fraction of spot is not a band — the
#: change is diffuse and claiming a level would be a false precision.
BAND_MAX_WIDTH_PCT = 0.035

#: Minimum stored sessions before a trailing standard deviation is trusted as
#: the z-score denominator.  Under this the caller falls back to a proxy and
#: must label the read accordingly (see ``normalization`` on the API payload).
MIN_SESSIONS_FOR_SIGMA = 10

#: Length of a regular cash session, in hours.  The stored per-session reads
#: are all SINCE-OPEN measurements, so this is the horizon every trailing
#: sigma is implicitly measured over — see :func:`horizon_factor`.
SESSION_HOURS = 6.5

#: Bootstrap denominator, as a fraction of the near-spot dealer-gamma STOCK.
#:
#: The proxy has one job: stand in for "how big is a typical session's shift
#: for this symbol" before enough sessions are stored to answer it properly.
#: The scale it borrows therefore has to be a scale of the same KIND — a
#: proximity-weighted dollar change.  The obvious cheap sources are not:
#: the dispersion of ``net_gex_at_spot`` (what this used to borrow) is the
#: spread of a LEVEL read at a single point on the spot-shift curve, while
#: lean/stability are signed sums across ~30 weighted strikes.  Nothing ties
#: those two magnitudes together, so the ratio was arbitrary — and in
#: practice large enough to push every z toward zero, which is why a
#: freshly-deployed symbol read QUIET on every session regardless of what
#: the book did.
#:
#: A fraction of the near-spot stock is the same kind of quantity as the
#: score (same weights, same units, same strikes), so the ratio is a real
#: "how much of the book near spot got re-worked".  12% is the bootstrap
#: guess for one session's worth of that; it is deliberately a single named
#: number because it stops mattering the moment ``MIN_SESSIONS_FOR_SIGMA``
#: sessions exist and the trailing sigma takes over.
PROXY_SHIFT_FRACTION = 0.12

#: Clamp on :func:`horizon_factor`.  Below 0.35 (~48 minutes) and above 2.5
#: (~40 hours, eight sessions) the square-root rule stops being a useful
#: approximation, and an unclamped factor would let a 30-minute window
#: manufacture sigma out of a tiny denominator.
_HORIZON_MIN = 0.35
_HORIZON_MAX = 2.5


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StrikeAgg:
    """One strike's dealer gamma and open interest, summed over expirations."""

    net_gex: float = 0.0
    call_gex: float = 0.0
    put_gex: float = 0.0
    call_oi: float = 0.0
    put_oi: float = 0.0


@dataclass(frozen=True)
class ShiftRow:
    """The change at one strike between snapshot A and snapshot B.

    ``positioning`` isolates the part of ``d_net`` attributable to contracts
    actually being opened or closed, holding per-contract gamma fixed;
    ``repricing`` is the remainder — the surviving book re-valuing as spot and
    implied vol moved.  A trader reading "dealers added gamma here" almost
    always means the former, and level-differencing alone conflates the two.
    """

    strike: float
    net_a: float
    net_b: float
    d_net: float
    positioning: float
    repricing: float
    call_oi_a: float
    call_oi_b: float
    put_oi_a: float
    put_oi_b: float


@dataclass(frozen=True)
class ShiftDiff:
    """Output of :func:`shift_rows` — the diff plus what it had to exclude."""

    rows: tuple[ShiftRow, ...]
    #: Expirations present on BOTH sides; the only ones the diff measures.
    common_expirations: tuple[date, ...]
    #: Expirations in A that are gone from B (expired, or pruned post-close).
    #: Their gamma is excluded from every row above and reported here so the
    #: caller can say what it dropped instead of silently booking it as a shed.
    expired_expirations: tuple[date, ...]
    #: Signed dollar gamma that left with ``expired_expirations``.
    expired_net_gex: float
    #: Absolute dollar gamma that left with ``expired_expirations``.
    expired_abs_gex: float
    #: Expirations newly listed in B (a new weekly opening for trading).
    #: Excluded for symmetry: their gamma has no A-side counterpart, so
    #: counting it would book brand-new listings as dealer repositioning.
    added_expirations: tuple[date, ...]
    added_net_gex: float
    #: How many strikes saw their call or put open interest actually change
    #: between the two snapshots.
    #:
    #: This is what makes the "repositioning" lens honest.  Open interest is
    #: a once-a-day settlement figure: the feed republishes the same number
    #: all session, so an INTRADAY A->B pair has zero OI change at every
    #: strike and ``positioning`` is identically 0 by construction — not
    #: because dealers sat on their hands.  Reported so the surface can say
    #: "this window cannot answer that question" instead of drawing a flat
    #: line that reads as "nothing happened".
    oi_moved_strikes: int = 0


@dataclass(frozen=True)
class ShiftScores:
    """Raw (un-normalized) two-axis scores, in dollar-GEX units."""

    lean: float
    stability: float
    #: Total signed change across every strike in the diff, unweighted.  Not a
    #: score — the headline "net shift" figure, kept here so callers do not
    #: re-sum the rows.
    net_shift: float
    #: Total |change|, unweighted.  The denominator for the concentration band.
    gross_shift: float
    #: Kernel width actually used, in price units (for display + audit).
    sigma_price: float
    #: Proximity-weighted |dealer gamma| present across the two snapshots —
    #: the STOCK the change happened against, in the same weights and units
    #: as ``lean``/``stability``.  Not a score: it is the only scale available
    #: before any session history exists, and :func:`proxy_sigma` turns it
    #: into the bootstrap z-score denominator.
    near_spot_stock: float = 0.0


@dataclass(frozen=True)
class RegimeRead:
    """The classified read — what reaches the headline."""

    state: str  # FIRMING | CAPPING | FRAGILE_BID | DETERIORATING | QUIET
    lean_z: float
    stability_z: float
    magnitude: float  # hypot(lean_z, stability_z)
    adverb: str
    #: Human-readable one-liner about what the state means mechanically.
    meaning: str


@dataclass(frozen=True)
class ConcentrationBand:
    """The contiguous strike range carrying most of the change."""

    low: float
    high: float
    share: float  # fraction of gross |change| inside the band, 0..1
    #: False when the change is too diffuse to name a level; ``low``/``high``
    #: are then the full window and callers must not quote them as a band.
    resolved: bool


@dataclass(frozen=True)
class RolloffTranche:
    """Dealer gamma sitting in one expiration."""

    expiration: date
    dte: int
    net_gex: float
    abs_gex: float
    #: This tranche's |gamma| as a fraction of the whole chain's |gamma|.
    share: float


@dataclass(frozen=True)
class Rolloff:
    """What expires next, and how much of the book goes with it."""

    tranches: tuple[RolloffTranche, ...]
    #: The nearest expiration — the one whose gamma disappears first.  None
    #: when the chain carries no expirations (degraded snapshot).
    next_tranche: Optional[RolloffTranche]
    #: Per-strike breakdown of ``next_tranche``, descending by |net_gex|.
    next_by_strike: tuple[tuple[float, float], ...] = field(default=())
    total_abs_gex: float = 0.0
    total_net_gex: float = 0.0


# ---------------------------------------------------------------------------
# Coercion helpers.  ``gex_by_strike`` numerics arrive as Decimal from
# asyncpg and as str through some JSON paths, so every read goes through here.
# ---------------------------------------------------------------------------


def _f(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _row_expiration(row: Mapping[str, Any]) -> Optional[date]:
    return _as_date(row.get("expiration"))


def _row_net(row: Mapping[str, Any]) -> float:
    """Net dealer gamma on a row, tolerating both column vocabularies.

    ``gex_by_strike`` and ``/api/replay/frame`` say ``net_gex``;
    ``/api/gex/strike-profile-timeseries`` says ``net_gamma`` for the same
    dollar quantity (a naming wart documented on ``StrikeProfileStrike``).
    Accepting both lets one implementation serve every caller.
    """
    if row.get("net_gex") is not None:
        return _f(row.get("net_gex"))
    return _f(row.get("net_gamma"))


def _row_call(row: Mapping[str, Any]) -> float:
    if row.get("call_gex") is not None:
        return _f(row.get("call_gex"))
    return _f(row.get("call_gamma"))


def _row_put(row: Mapping[str, Any]) -> float:
    if row.get("put_gex") is not None:
        return _f(row.get("put_gex"))
    return _f(row.get("put_gamma"))


# ---------------------------------------------------------------------------
# 1. Expiry-aware per-strike diff
# ---------------------------------------------------------------------------


def expirations_in(rows: Iterable[Mapping[str, Any]]) -> set[date]:
    """Distinct expirations carried by a snapshot's rows."""
    return {e for e in (_row_expiration(r) for r in rows) if e is not None}


def aggregate_by_strike(
    rows: Iterable[Mapping[str, Any]],
    expirations: Optional[Iterable[date]] = None,
) -> dict[float, StrikeAgg]:
    """Sum per-(strike, expiration) rows down to per-strike totals.

    ``expirations`` restricts the sum to that set; ``None`` includes every
    row.  Rows carrying no expiration are included only when unrestricted —
    a filtered call cannot tell whether they belong.
    """
    wanted = set(expirations) if expirations is not None else None
    acc: dict[float, dict[str, float]] = defaultdict(
        lambda: {"net": 0.0, "call": 0.0, "put": 0.0, "coi": 0.0, "poi": 0.0}
    )
    for row in rows:
        strike = _f(row.get("strike"), default=math.nan)
        if not math.isfinite(strike) or strike <= 0:
            continue
        if wanted is not None:
            exp = _row_expiration(row)
            if exp is None or exp not in wanted:
                continue
        bucket = acc[strike]
        bucket["net"] += _row_net(row)
        bucket["call"] += _row_call(row)
        bucket["put"] += _row_put(row)
        bucket["coi"] += _f(row.get("call_oi"))
        bucket["poi"] += _f(row.get("put_oi"))
    return {
        strike: StrikeAgg(
            net_gex=v["net"],
            call_gex=v["call"],
            put_gex=v["put"],
            call_oi=v["coi"],
            put_oi=v["poi"],
        )
        for strike, v in acc.items()
    }


def _positioning_leg(gamma_a: float, oi_a: float, gamma_b: float, oi_b: float) -> float:
    """OI-driven part of one leg's dollar-gamma change.

    Takes a per-contract dollar-gamma unit from whichever snapshot carries
    open interest (A preferred — it is the reference state) and multiplies it
    by the change in contracts.  Holding the unit fixed is what separates
    "dealers traded here" from "the same book re-valued"; it is a first-order
    estimate and is labelled as such everywhere it surfaces.
    """
    if oi_a > 0:
        unit = gamma_a / oi_a
    elif oi_b > 0:
        unit = gamma_b / oi_b
    else:
        return 0.0
    return unit * (oi_b - oi_a)


def shift_rows(
    rows_a: Sequence[Mapping[str, Any]],
    rows_b: Sequence[Mapping[str, Any]],
    *,
    restrict_expirations: Optional[Iterable[date]] = None,
) -> ShiftDiff:
    """Per-strike dealer-gamma change from snapshot A to snapshot B.

    Only expirations present on BOTH sides are differenced — see the module
    docstring for why this is not optional.  ``restrict_expirations`` narrows
    that intersection further (the page's expiry filter); passing a set with
    no overlap yields an empty diff rather than an error, so a stale filter
    degrades to "nothing to compare" instead of a wrong number.
    """
    exps_a = expirations_in(rows_a)
    exps_b = expirations_in(rows_b)

    # A snapshot with no expiration column at all (older frame payloads) can
    # still be differenced — there is simply nothing to intersect, and the
    # roll-off caveat does not apply.
    if not exps_a and not exps_b:
        common: Optional[set[date]] = None
        expired: set[date] = set()
        added: set[date] = set()
    else:
        common = exps_a & exps_b
        expired = exps_a - exps_b
        added = exps_b - exps_a
        if restrict_expirations is not None:
            common &= set(restrict_expirations)

    agg_a = aggregate_by_strike(rows_a, common)
    agg_b = aggregate_by_strike(rows_b, common)

    zero = StrikeAgg()
    rows: list[ShiftRow] = []
    oi_moved = 0
    for strike in sorted(set(agg_a) | set(agg_b)):
        a = agg_a.get(strike, zero)
        b = agg_b.get(strike, zero)
        d_net = b.net_gex - a.net_gex
        positioning = _positioning_leg(
            a.call_gex, a.call_oi, b.call_gex, b.call_oi
        ) + _positioning_leg(a.put_gex, a.put_oi, b.put_gex, b.put_oi)
        if a.call_oi != b.call_oi or a.put_oi != b.put_oi:
            oi_moved += 1
        rows.append(
            ShiftRow(
                strike=strike,
                net_a=a.net_gex,
                net_b=b.net_gex,
                d_net=d_net,
                positioning=positioning,
                repricing=d_net - positioning,
                call_oi_a=a.call_oi,
                call_oi_b=b.call_oi,
                put_oi_a=a.put_oi,
                put_oi_b=b.put_oi,
            )
        )

    expired_rows = [r for r in rows_a if _row_expiration(r) in expired] if expired else []
    added_rows = [r for r in rows_b if _row_expiration(r) in added] if added else []

    return ShiftDiff(
        rows=tuple(rows),
        common_expirations=tuple(sorted(common)) if common else (),
        expired_expirations=tuple(sorted(expired)),
        expired_net_gex=sum(_row_net(r) for r in expired_rows),
        expired_abs_gex=sum(abs(_row_net(r)) for r in expired_rows),
        added_expirations=tuple(sorted(added)),
        added_net_gex=sum(_row_net(r) for r in added_rows),
        oi_moved_strikes=oi_moved,
    )


# ---------------------------------------------------------------------------
# 2. Expiry roll-off
# ---------------------------------------------------------------------------


def rolloff_tranches(
    rows: Iterable[Mapping[str, Any]],
    *,
    as_of: date,
    max_tranches: int = 8,
    next_strike_limit: int = 24,
) -> Rolloff:
    """Group a snapshot's dealer gamma by expiration, nearest first.

    The nearest tranche is the gamma that stops existing at its close — the
    single largest scheduled change to the surface, and the one thing a
    same-day snapshot cannot show you.  ``share`` is measured on |gamma| so a
    tranche that is large but internally offsetting (a big call wall against a
    big put wall) still reads as large: all of it is leaving.

    ``max_tranches`` bounds the term ladder; everything beyond it is folded
    into a single trailing "later" row so the shares still sum to 1.
    """
    per_exp: dict[date, dict[str, float]] = defaultdict(lambda: {"net": 0.0, "abs": 0.0})
    per_exp_strikes: dict[date, dict[float, float]] = defaultdict(lambda: defaultdict(float))

    for row in rows:
        exp = _row_expiration(row)
        if exp is None:
            continue
        strike = _f(row.get("strike"), default=math.nan)
        if not math.isfinite(strike) or strike <= 0:
            continue
        net = _row_net(row)
        per_exp[exp]["net"] += net
        per_exp[exp]["abs"] += abs(net)
        per_exp_strikes[exp][strike] += net

    if not per_exp:
        return Rolloff(tranches=(), next_tranche=None)

    total_abs = sum(v["abs"] for v in per_exp.values())
    total_net = sum(v["net"] for v in per_exp.values())
    denom = total_abs if total_abs > 0 else 1.0

    ordered = sorted(per_exp.items())
    head = ordered[:max_tranches]
    tail = ordered[max_tranches:]

    tranches = [
        RolloffTranche(
            expiration=exp,
            dte=max(0, (exp - as_of).days),
            net_gex=v["net"],
            abs_gex=v["abs"],
            share=v["abs"] / denom,
        )
        for exp, v in head
    ]
    if tail:
        tail_abs = sum(v["abs"] for _, v in tail)
        tail_net = sum(v["net"] for _, v in tail)
        # Folded row: dated at the FARTHEST expiration it covers so the ladder
        # stays monotone in DTE and the label can read "and later".
        tranches.append(
            RolloffTranche(
                expiration=tail[-1][0],
                dte=max(0, (tail[-1][0] - as_of).days),
                net_gex=tail_net,
                abs_gex=tail_abs,
                share=tail_abs / denom,
            )
        )

    nearest_exp = ordered[0][0]
    by_strike = sorted(
        per_exp_strikes[nearest_exp].items(), key=lambda kv: abs(kv[1]), reverse=True
    )[:next_strike_limit]

    return Rolloff(
        tranches=tuple(tranches),
        next_tranche=tranches[0] if tranches else None,
        next_by_strike=tuple(sorted(by_strike, key=lambda kv: kv[0])),
        total_abs_gex=total_abs,
        total_net_gex=total_net,
    )


# ---------------------------------------------------------------------------
# 3. Two-axis scores and the classified read
# ---------------------------------------------------------------------------


def proximity_weight(strike: float, spot: float, sigma_price: float) -> float:
    """Gaussian weight in expected-move units.  1.0 at spot, ->0 far out."""
    if sigma_price <= 0:
        return 1.0 if strike == spot else 0.0
    z = (strike - spot) / sigma_price
    # Guard the exponent: a far-OTM strike on a tiny sigma would otherwise
    # underflow through math.exp into an OverflowError on some platforms.
    if abs(z) > 8:
        return 0.0
    return math.exp(-(z * z))


def weighted_scores(
    rows: Sequence[ShiftRow],
    spot: float,
    *,
    sigma_price: Optional[float] = None,
    use_positioning: bool = False,
) -> ShiftScores:
    """Project the per-strike change onto the lean and stability axes.

    ``use_positioning`` scores the OI-driven component instead of the total
    change — the honest "did dealers actually trade here" lens.  It is the
    stricter reading and the one to prefer when the two disagree, but it is
    also noisier on thin chains, so the default stays on total change and the
    surface exposes both.
    """
    if sigma_price is None or sigma_price <= 0:
        sigma_price = max(DEFAULT_SIGMA_PCT * spot, 1e-9) if spot > 0 else 1.0

    lean = 0.0
    stability = 0.0
    net_shift = 0.0
    gross_shift = 0.0
    near_spot_stock = 0.0
    for row in rows:
        value = row.positioning if use_positioning else row.d_net
        net_shift += value
        gross_shift += abs(value)
        w = proximity_weight(row.strike, spot, sigma_price)
        if w == 0.0:
            continue
        stability += w * value
        # Below spot a build is support (+), above spot it is resistance (-).
        # A strike exactly at spot is neither and contributes nothing to lean.
        side = 0.0 if row.strike == spot else (1.0 if row.strike < spot else -1.0)
        lean += side * w * value
        # The book the change is measured against, under the SAME kernel, so
        # it can act as the bootstrap denominator.  Averaged over the two
        # snapshots rather than taken from either end: anchoring on A alone
        # would scale a day-over-day read against a book that has since
        # expired, and on B alone against one that did not exist at A.
        near_spot_stock += w * (abs(row.net_a) + abs(row.net_b)) / 2.0

    return ShiftScores(
        lean=lean,
        stability=stability,
        net_shift=net_shift,
        gross_shift=gross_shift,
        sigma_price=sigma_price,
        near_spot_stock=near_spot_stock,
    )


_STATE_MEANING: dict[str, str] = {
    "FIRMING": (
        "Floor building and vol compressing — dealers are hedging against "
        "moves here, so dips get bought back into the range."
    ),
    "CAPPING": (
        "Ceiling building — the range is tightening from above, and rallies "
        "run into dealer selling."
    ),
    "FRAGILE_BID": (
        "Support improved but the cushion thinned — moves travel further in "
        "both directions than the bid suggests."
    ),
    "DETERIORATING": (
        "Support eroding into a shortening-gamma book — dealers hedge WITH "
        "the move, which is the accelerant setup."
    ),
    "QUIET": (
        "No meaningful repositioning. The prior session's map still applies."
    ),
}


def adverb_for(magnitude: float) -> str:
    """|z| -> the word that reaches the headline."""
    for ceiling, word in _ADVERB_LADDER:
        if magnitude < ceiling:
            return word
    return _ADVERB_LADDER[-1][1]


def classify(lean_z: float, stability_z: float) -> RegimeRead:
    """Quadrant + magnitude -> the named state.

    QUIET wins inside ``QUIET_Z`` regardless of quadrant: naming a direction
    for a sub-noise move is the fastest way to lose a reader's trust, and
    "nothing happened" is a genuinely useful answer.
    """
    magnitude = math.hypot(lean_z, stability_z)
    if magnitude < QUIET_Z:
        state = "QUIET"
    elif stability_z >= 0:
        state = "FIRMING" if lean_z >= 0 else "CAPPING"
    else:
        state = "FRAGILE_BID" if lean_z >= 0 else "DETERIORATING"
    return RegimeRead(
        state=state,
        lean_z=lean_z,
        stability_z=stability_z,
        magnitude=magnitude,
        adverb=adverb_for(magnitude),
        meaning=_STATE_MEANING[state],
    )


def concentration_band(
    rows: Sequence[ShiftRow],
    *,
    spot: float,
    use_positioning: bool = False,
    coverage: float = BAND_COVERAGE,
) -> ConcentrationBand:
    """Narrowest contiguous strike run holding ``coverage`` of the |change|.

    Sweeps a growing window over the strike-ordered rows and keeps the
    shortest one that clears the threshold, so the answer is the tightest
    honest description of where the change happened.  Marked unresolved when
    the winning window is wider than :data:`BAND_MAX_WIDTH_PCT` of spot — at
    that point the change is spread across the chain and quoting a range
    would invent a level that is not there.
    """
    ordered = sorted(rows, key=lambda r: r.strike)
    values = [abs(r.positioning if use_positioning else r.d_net) for r in ordered]
    total = sum(values)
    if not ordered or total <= 0:
        return ConcentrationBand(low=0.0, high=0.0, share=0.0, resolved=False)

    target = total * coverage
    best: Optional[tuple[int, int, float]] = None  # (width_strikes, lo, hi_sum)
    left = 0
    running = 0.0
    for right, v in enumerate(values):
        running += v
        while running - values[left] >= target and left < right:
            running -= values[left]
            left += 1
        if running >= target:
            width = right - left
            if best is None or width < best[0]:
                best = (width, left, right)

    if best is None:
        lo_strike, hi_strike = ordered[0].strike, ordered[-1].strike
        return ConcentrationBand(
            low=lo_strike, high=hi_strike, share=1.0, resolved=False
        )

    _, lo_i, hi_i = best
    lo_strike = ordered[lo_i].strike
    hi_strike = ordered[hi_i].strike
    share = sum(values[lo_i : hi_i + 1]) / total
    width_ok = spot <= 0 or (hi_strike - lo_strike) <= BAND_MAX_WIDTH_PCT * spot
    return ConcentrationBand(
        low=lo_strike, high=hi_strike, share=share, resolved=bool(width_ok)
    )


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def zscore(raw: float, sigma: Optional[float]) -> float:
    """Raw score -> sigma units, clamped to a renderable range.

    The clamp matters for display, not for truth: the shift plane draws a
    3-sigma box, and an un-clamped 40-sigma outlier (a degraded snapshot, a
    symbol's first day of data) would otherwise fly off the panel and take
    the axis scale with it.  The stored raw value is never clamped.
    """
    if sigma is None or not math.isfinite(sigma) or sigma <= 0:
        return 0.0
    z = raw / sigma
    if not math.isfinite(z):
        return 0.0
    return max(-6.0, min(6.0, z))


def proxy_sigma(scores: ShiftScores) -> Optional[float]:
    """Bootstrap z-score denominator, derived from the chain itself.

    Returns ``None`` when the snapshot carries no near-spot gamma at all, so
    the caller degrades to "direction measured, magnitude not" rather than
    dividing by a number it invented.

    Used only until :data:`MIN_SESSIONS_FOR_SIGMA` sessions are stored; every
    surface that renders a read built on it must label the magnitude
    provisional (``normalization == "proxy"``).  See
    :data:`PROXY_SHIFT_FRACTION` for why the scale is a fraction of the
    near-spot stock rather than the dispersion of some stored level.
    """
    stock = scores.near_spot_stock
    if not math.isfinite(stock) or stock <= 0:
        return None
    sigma = PROXY_SHIFT_FRACTION * stock
    return sigma if sigma > 0 else None


def horizon_factor(hours: float) -> float:
    """Rescale a session-length sigma to a window of ``hours`` trading hours.

    Every stored session read is a SINCE-OPEN measurement, so the trailing
    sigma answers "how big is a full session's shift".  The live card applies
    it to windows that are nothing like a full session: a 1h lookback, or a
    "since open" read taken at 09:45.  Comparing a 15-minute change against a
    6.5-hour yardstick makes the morning read QUIET every day and the weekly
    one read dramatic every week — the denominator is measuring the clock, not
    the book.

    Repositioning accumulates roughly like a random walk, so the yardstick
    scales with the square root of elapsed trading time.  That is a
    first-order rule and it is wrong in the details (dealer gamma churns
    hardest at the open and into the close), but it is right about the shape,
    and being right about the shape is the difference between a magnitude
    that means something across lookbacks and one that does not.  Clamped at
    both ends — see :data:`_HORIZON_MIN`.
    """
    if not math.isfinite(hours) or hours <= 0:
        return 1.0
    factor = math.sqrt(hours / SESSION_HOURS)
    if not math.isfinite(factor):
        return 1.0
    return max(_HORIZON_MIN, min(_HORIZON_MAX, factor))


#: Rescales a mean absolute value onto the standard deviation's scale.
#: ``sqrt(pi/2)`` is the exact ratio for a zero-mean normal, so
#: :func:`robust_scale` and :func:`stdev` agree on Gaussian data and diverge
#: only by however heavy the real tail is — which makes their ratio a direct
#: readout of a chain's tail weight (see :func:`robust_scale`).
_MAD_TO_SIGMA = math.sqrt(math.pi / 2)


def robust_scale(values: Sequence[float]) -> Optional[float]:
    """Typical magnitude of a session's shift, on the standard deviation's
    scale — the z-score denominator.

    Two departures from :func:`stdev`, and the second is the one that matters.

    **Measured about zero, not about the mean.** The numerator these divide
    is the RAW score, never ``raw - mean``, so the denominator has to describe
    spread about the same origin or the ratio is not a z at all. Zero is also
    the right null on its own terms: a chain that sheds gamma every single
    session IS deteriorating every session, and centring on the mean would
    define that away as "normal for this symbol" and report QUIET straight
    through it.

    **Built from the first absolute moment, not the second.** Squaring gives
    one 5-sigma session as much pull on the denominator as twenty-five
    ordinary ones. Where the shift distribution is heavy-tailed that inflates
    the denominator until an ordinary day cannot clear :data:`QUIET_Z`, and
    the card reports "no meaningful repositioning" on a book that moved.

    That failure is measurable without reference to any threshold, because
    SPY and SPX track the SAME index and should therefore mostly agree. Over
    one 42-session window they did not: 17% of SPY's sessions read QUIET
    against 48% of SPX's. The giveaway is ``stdev / mean|shift|``, which is
    ``sqrt(pi/2)`` = 1.2533 for anything Gaussian and came out at 1.30 for
    SPY, 1.27 for QQQ, 1.39 for NDX — and 1.90 for SPX. Only SPX's
    denominator was set by its tail, and only SPX called quiet a market the
    other three called active.

    This moderates the tail's influence rather than removing it: one enormous
    session still lifts a mean. The fully robust choice — a median absolute
    value — would resist it outright, but at 37% statistical efficiency
    against this estimator's 88% it is too noisy on the 10-60 session windows
    this runs against, and would trade a bias that is fixed here for a
    variance nobody asked for. The measured effect of the estimator actually
    chosen is a ~34% smaller denominator for SPX against 3% for SPY and 1%
    for QQQ: it corrects the chain that was wrong and leaves alone the ones
    that were not.

    Returns ``None`` only when every value is zero — there is no scale to be
    had then, and zero would make the next real move's z infinite.
    """
    finite = [v for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
    if len(finite) < 2:
        return None
    scale = (sum(abs(v) for v in finite) / len(finite)) * _MAD_TO_SIGMA
    return scale if scale > 0 else None


def stdev(values: Sequence[float]) -> Optional[float]:
    """Population standard deviation, or None when there is nothing to learn.

    Population rather than sample: the stored session reads are the whole
    history we have, not a draw from it, and on the small N this operates at
    (10-60 sessions) the Bessel correction is noise dressed as rigor.

    NOT the z-score denominator — :func:`robust_scale` is, for the reasons
    documented there.  Kept because "how disperse is this series" is still a
    question worth being able to ask directly, and because the two together
    are what measure a chain's tail weight.
    """
    finite = [v for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
    if len(finite) < 2:
        return None
    mean = sum(finite) / len(finite)
    var = sum((v - mean) ** 2 for v in finite) / len(finite)
    return math.sqrt(var) if var > 0 else None


def percentile_of(value: float, population: Sequence[float]) -> Optional[float]:
    """Fraction of ``population`` at or below ``value`` (0..1), or None.

    Used for "is this roll-off a lot?", where a percentile answers the
    question far better than a dollar figure: 38% of the book expiring means
    nothing in isolation and everything against "the largest in 60 sessions".
    """
    finite = [v for v in population if isinstance(v, (int, float)) and math.isfinite(v)]
    if not finite:
        return None
    at_or_below = sum(1 for v in finite if v <= value)
    return at_or_below / len(finite)
