"""Canonical Call/Put Wall computation.

Single source of truth for Call/Put Wall strikes consumed by:
  - ``gex_summary`` row written by :class:`src.analytics.main_engine.AnalyticsEngine`
  - ``/api/gex/summary`` and ``/api/gex/history`` endpoints
  - ``/api/gex/strike-profile-timeseries`` (per-bucket walls follow the
    request's ``expirations`` filter — the helper is the same, the input
    rows differ)
  - :class:`src.signals.unified_signal_engine.UnifiedSignalEngine` (current and
    ~30min-prior walls used by ``trap_detection`` and ``gamma_vwap_confluence``)
  - all playbook patterns that read ``ctx.level("call_wall" | "put_wall")``

The canonical definition starts from the industry-standard one (SpotGamma /
SqueezeMetrics / Cheddar Flow) and adds two stability rules:

* **Call Wall** — strike above the wall anchor with the largest dollar call
  gamma exposure ``γ_call × OI × 100 × S² × 0.01``, aggregated across the
  expirations the caller chose to include.
* **Put Wall**  — strike below the wall anchor with the largest dollar put
  gamma exposure ``γ_put  × OI × 100 × S² × 0.01``, same aggregation.
* **Tie zone** — strikes within ``WALL_TIE_PCT`` (10%) of the largest on
  their side count as tied, and the tied strike nearest the anchor wins
  (lowest for calls, highest for puts).  An exact tie is the zero-width case.
* **Wall anchor** — the price strikes are split on.  Not spot itself but a
  sticky copy of it (:func:`step_wall_anchor`) that only moves once price
  closes more than a break buffer away, so a strike changes sides only when
  price breaks decisively through it.  Callers without an anchor split on
  spot.

**When** the walls are re-picked is the third piece (:class:`WallTracker`):
on the minute price breaks out, and otherwise every ``WALL_REFRESH_MINUTES``
(15), never while price merely lingers.  Between re-picks the walls stay the
ones picked last time.

**Who keeps the wall** at a re-pick is the fourth (``incumbent``): the
current wall stays while it is still on its side of the anchor and within
``WALL_KEEP_PCT`` (25%) of the biggest strike there, so a rival has to be
clearly bigger to take over.  Strike sizes drift with price even when nothing
else changes -- on NDX 2026-10-09 the 30800 put swung smoothly between 75% and
100% of the 30700 put while price moved 70 points -- and without this edge
every re-pick near the tie zone's 90% line could land on the other strike, so
the walls traded places at each re-check.  Price breaking through the current
wall still moves it at once: a wall on the wrong side of the anchor has no
claim to keep.

Why the two rules: a plain argmax re-run every minute made the published
walls jump and snap back (639 times across SPX/SPY/NDX/QQQ in the eight
sessions from 2026-09-28).  About half were spot chopping across the
biggest strike, which flipped it between Call Wall and Put Wall every time
it crossed; the anchor makes that strike keep its role while price lingers
and hand it over the minute price plows through.  The other half were two
near-tied strikes trading places on small moves; the tie zone stops that
without delaying a strike that is clearly bigger.  Both depend only on the
strikes and on price, so every expiration selection behaves the same way.

The **wall ladder** (:func:`compute_wall_ladder`) generalises that to the
top-N strikes per side — ``C1``/``C2``/``C3`` above the anchor and
``P1``/``P2``/``P3`` below — so ``C1`` and ``P1`` are by construction the
Call Wall and Put Wall above.  Below rank 1, ranks are pure magnitude: the
2nd-largest eligible strike is ``C2`` even when it sits one tick from
``C1``.  No minimum-separation filter is applied, because any spacing rule
would make the ladder disagree with the per-strike bars the charts draw
right beside it.

Notes on the formula choice:

* Gamma **exposure** (γ × OI × 100 × S² × 0.01) captures both contract count
  *and* per-contract sensitivity, which is what determines the size of dealer
  hedging flow at that strike.  Raw OI alone is misleading for far-OTM strikes
  with tiny gamma.
* The ordering is monotone in ``call_gamma`` (resp. ``put_gamma``) at a fixed
  timestamp because ``100 × S² × 0.01`` is a positive constant common to all
  strikes.  Callers that already have the OI-weighted ``call_gamma`` /
  ``put_gamma`` aggregate (as produced by ``_calculate_gex_by_strike`` or
  stored in ``gex_by_strike``) can rank on those directly without re-deriving
  the dollar exposure.
* The side filter (``strike >= anchor`` for call, ``strike <= anchor`` for
  put) preserves the structural meaning of a "wall": calls above act as
  resistance, puts below act as support.  A historical bug where the
  ``/api/gex/summary`` endpoint disagreed with the signals layer was caused by
  the endpoint omitting this filter.
* Cross-expiration aggregation is performed **inside** the helper.
  ``gex_by_strike`` is keyed ``(strike, expiration)`` so a single strike
  surfaces multiple rows when several expirations have OI there; ranking
  per-row instead of per-strike picks the single largest-expiration outlier
  and disagrees with every cross-expiration view of the chain
  (``/api/gex/by-strike`` summed, ``/api/gex/strike-profile-timeseries``
  bars, ``max_gamma_strike``).  Aggregating by strike before ranking
  matches what dealers actually hedge and what the chart actually shows.
  Restrict expirations *before* calling the helper to get walls scoped to
  a specific expiration grouping (e.g. 0DTE only).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

# The gamma-flip proxy below is gated with the SAME constants the canonical
# spot-shift resolver uses, so the two paths reject the same noise-floor
# crossings.  src.config is stdlib + dotenv only, so this keeps walls.py free
# of heavy imports on the API request path.
from src.config import (
    GAMMA_PROFILE_INTERIOR_MARGIN,
    GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT,
    GAMMA_PROFILE_STRUCTURAL_MIN_FRAC,
    GAMMA_PROFILE_STRUCTURAL_REFERENCE_PERCENTILE,
    GAMMA_PROFILE_STRUCTURAL_WINDOW_PCT,
    WALL_BREAK_MIN_PCT,
    WALL_BREAK_MOVE_FRACTION,
    WALL_KEEP_PCT,
    WALL_REFRESH_MINUTES,
    WALL_TIE_PCT,
)

_ET = ZoneInfo("America/New_York")

#: ``(call_wall, put_wall)`` strikes; either side ``None`` when there is none.
WallPair = Tuple[Optional[float], Optional[float]]

# ── Wall-ladder depth ───────────────────────────────────────────────────────
# How many ranked walls per side the API computes by default (C1..C3 /
# P1..P3) and the hard ceiling a caller may ask for.  The ceiling exists
# because the ladder is serialised onto every ``/api/gex/summary`` response
# and every strike-profile bucket: past a handful of strikes the deeper
# ranks are noise on a chart yet still cost payload on every poll.
DEFAULT_WALL_LADDER_DEPTH = 3
MAX_WALL_LADDER_DEPTH = 5


def compute_call_put_walls(
    gex_by_strike: Iterable[Mapping[str, Any]],
    spot_price: float,
    *,
    anchor: Optional[float] = None,
    tie_pct: float = WALL_TIE_PCT,
    incumbent: Optional[WallPair] = None,
    keep_pct: float = WALL_KEEP_PCT,
) -> Tuple[Optional[float], Optional[float]]:
    """Return ``(call_wall, put_wall)`` from per-strike gamma rows.

    :param gex_by_strike: iterable of rows with at least the keys ``strike``,
        ``call_gamma``, ``put_gamma``.  Extra keys are ignored.  Rows may
        be per-(strike, expiration) — the helper aggregates ``call_gamma``
        and ``put_gamma`` by strike before ranking, so passing the raw
        ``gex_by_strike`` table rows produces the same answer as passing
        already-summed rows.  To scope the walls to a specific expiration
        grouping (e.g. 0DTE only, or a single date), filter rows on the
        caller side before passing them in.
    :param spot_price: current underlying price; scales the dollar strength
        and, when no ``anchor`` is given, splits strikes into the call and put
        regions.
    :param anchor: the wall anchor (:func:`step_wall_anchor`) to split on
        instead of spot.  Publishing paths pass it so a strike keeps its side
        while price lingers around it.
    :param tie_pct: width of the tie zone (see the module docstring).
    :param incumbent: the ``(call_wall, put_wall)`` in place before this pick.
        A side keeps its wall while that strike is still on its side of the
        anchor and within ``keep_pct`` of the side's largest.  ``None`` (or a
        ``None`` side) picks fresh.
    :param keep_pct: the incumbent's edge; never narrower than ``tie_pct``.
    :returns: ``(call_wall_strike, put_wall_strike)``.  Either side is
        ``None`` when no eligible strike exists (e.g. all-zero gamma on that
        side, or no strikes on that side of the anchor).

    Within the tie zone the strike nearest the anchor wins:

    * Call wall → lowest tied strike above the anchor.
    * Put wall  → highest tied strike below the anchor.

    This is the strike-only view.  Callers that also need the wall's
    dollar-gamma magnitude (e.g. TradeWorkz position sizing) should call
    :func:`compute_call_put_walls_with_strength`, which shares this exact
    ranking and additionally returns the dollar exposure at each wall.
    """
    call_wall, put_wall, _cw_strength, _pw_strength = compute_call_put_walls_with_strength(
        gex_by_strike,
        spot_price,
        anchor=anchor,
        tie_pct=tie_pct,
        incumbent=incumbent,
        keep_pct=keep_pct,
    )
    return call_wall, put_wall


def compute_call_put_walls_with_strength(
    gex_by_strike: Iterable[Mapping[str, Any]],
    spot_price: float,
    *,
    anchor: Optional[float] = None,
    tie_pct: float = WALL_TIE_PCT,
    incumbent: Optional[WallPair] = None,
    keep_pct: float = WALL_KEEP_PCT,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Return ``(call_wall, put_wall, call_wall_strength, put_wall_strength)``.

    Same wall selection as :func:`compute_call_put_walls`, plus the
    **dollar-gamma magnitude at each wall strike**.  Both are the rank-1
    entries of :func:`compute_wall_ladder`, which is the single ranking
    implementation the three functions share — so ``C1`` on the ladder can
    never disagree with the scalar ``call_wall`` a caller reads beside it.

    The magnitude is the OI-weighted gamma aggregate at the chosen strike,
    converted to dollar GEX per 1% move via the canonical
    ``γ_aggregate × 100 × S² × 0.01`` formula — the same convention
    ``AnalyticsEngine`` uses for the strike-profile ``abs_dollar_gex`` and
    ``_calculate_gex_by_strike`` uses inline, so a persisted
    ``call_wall_strength`` equals the timeseries wall magnitude for the
    same tick.

    Strength is ``None`` on whichever side has no wall (mirroring the
    strike being ``None``) and ``0.0`` never appears for a real wall,
    because a strike only becomes a wall when its gamma aggregate is
    strictly positive.

    :returns: strikes as in :func:`compute_call_put_walls`; strengths are
        non-negative dollar magnitudes (``abs`` applied defensively) or
        ``None`` when that side has no wall / spot is unusable.
    """
    call_walls, put_walls = compute_wall_ladder(
        gex_by_strike,
        spot_price,
        depth=1,
        anchor=anchor,
        tie_pct=tie_pct,
        incumbent=incumbent,
        keep_pct=keep_pct,
    )
    call_top = call_walls[0] if call_walls else None
    put_top = put_walls[0] if put_walls else None
    return (
        call_top["strike"] if call_top else None,
        put_top["strike"] if put_top else None,
        call_top["strength"] if call_top else None,
        put_top["strength"] if put_top else None,
    )


def wall_label(side: str, rank: int) -> str:
    """``('call', 2) -> 'C2'`` — the naming every surface shows for a wall.

    Kept here rather than in each consumer so the API payload, the charts
    and any future export all spell a wall the same way.
    """
    return f"{'C' if side == 'call' else 'P'}{rank}"


def compute_wall_ladder(
    gex_by_strike: Iterable[Mapping[str, Any]],
    spot_price: float,
    depth: int = DEFAULT_WALL_LADDER_DEPTH,
    *,
    anchor: Optional[float] = None,
    tie_pct: float = WALL_TIE_PCT,
    incumbent: Optional[WallPair] = None,
    keep_pct: float = WALL_KEEP_PCT,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return ``(call_walls, put_walls)`` — the top-``depth`` walls per side.

    This is the generalisation of :func:`compute_call_put_walls` to the
    secondary/tertiary walls (``C2``/``C3``, ``P2``/``P3``) charts draw as
    optional levels, and the single ranking implementation the whole module
    shares.  Rank 1 is by construction the canonical Call/Put Wall.

    Each entry is a plain dict, JSON-serialisable as-is::

        {"rank": 1, "label": "C1", "strike": 105.0, "strength": 1.2e9}

    ``strength`` is the dollar-gamma magnitude at that strike, on the same
    ``γ_aggregate × 100 × S² × 0.01`` scale as
    :func:`compute_call_put_walls_with_strength`, so a client can size the
    marker (or dim a weak ``C3``) without a second pass over the chain.

    :param gex_by_strike: rows with at least ``strike``, ``call_gamma``,
        ``put_gamma``.  Rows may be per-(strike, expiration) — they are
        aggregated by strike first, exactly as
        :func:`compute_call_put_walls` does.  Restrict expirations on the
        caller side to scope the ladder.
    :param spot_price: current underlying price; scales the dollar strength
        and is the split price when no ``anchor`` is given.
    :param depth: how many ranks to return per side.  Clamped to
        ``[0, MAX_WALL_LADDER_DEPTH]``.  Fewer are returned when the chain
        has fewer eligible strikes on that side — a short list means the
        book genuinely has no further wall, so callers should render what
        they get rather than padding.
    :param anchor: the wall anchor to split strikes on (call side
        ``strike >= anchor``, put side ``strike <= anchor``).  ``None`` or a
        non-positive value splits on ``spot_price``.
    :param tie_pct: the tie zone.  Rank 1 is the strike nearest the anchor
        among those within ``tie_pct`` of the side's largest; ``0`` makes rank
        1 the largest (nearest-to-anchor on an exact tie).
    :param incumbent: the ``(call_wall, put_wall)`` in place before this pick.
        A side's incumbent stays rank 1 while it is still eligible (on its
        side of the anchor, positive gamma) and its gamma is within
        ``keep_pct`` of the side's largest; otherwise that side picks fresh.
    :param keep_pct: the incumbent's edge, clamped to ``[tie_pct, 1]``: a
        strike already tied with the largest is never handed over to another
        tied strike.  ``1`` keeps an eligible incumbent unconditionally,
        which is how a reader reproduces a stored pick.
    :returns: two lists ordered by rank ascending.  Both are empty when
        ``spot_price`` is unusable.

    Ordering per side:

    * Rank 1 → the incumbent when it keeps its place, else the tie-zone pick
      described above.
    * Ranks 2.. → the remaining strikes by gamma DESC, then nearest to the
      anchor (strike ASC for calls, DESC for puts).
    """
    depth = max(0, min(int(depth), MAX_WALL_LADDER_DEPTH))
    if depth == 0 or spot_price is None or spot_price <= 0:
        return [], []
    split = float(anchor) if anchor is not None and anchor > 0 else float(spot_price)
    tie = min(max(float(tie_pct or 0.0), 0.0), 1.0)
    keep = min(max(float(keep_pct or 0.0), tie), 1.0)
    held_call, held_put = _incumbent_strikes(incumbent)

    # Aggregate per-(strike, expiration) rows into per-strike sums so the
    # ranking matches the cross-expiration view consumers actually see.
    agg_call: "defaultdict[float, float]" = defaultdict(float)
    agg_put: "defaultdict[float, float]" = defaultdict(float)
    for row in gex_by_strike:
        try:
            strike = float(row["strike"])
        except (KeyError, TypeError, ValueError):
            continue
        agg_call[strike] += float(row.get("call_gamma") or 0.0)
        agg_put[strike] += float(row.get("put_gamma") or 0.0)

    # OI-weighted gamma → dollar GEX per 1% move (canonical scale).
    dollar_scale = 100.0 * spot_price * spot_price * 0.01

    def _rank(
        agg: "defaultdict[float, float]",
        eligible: Any,
        nearest_first: Any,
        side: str,
        held: Optional[float],
    ) -> List[Dict[str, Any]]:
        candidates = [
            (strike, gamma) for strike, gamma in agg.items() if gamma > 0 and eligible(strike)
        ]
        # Magnitude order, nearest-to-anchor on an exact tie.
        candidates.sort(key=lambda sg: (-sg[1], nearest_first(sg[0])))
        if candidates:
            largest = candidates[0][1]
            keeper = (
                next((sg for sg in candidates if abs(sg[0] - held) <= 1e-6), None)
                if held is not None
                else None
            )
            if keeper is not None and keeper[1] >= largest * (1.0 - keep):
                # The wall in place is still on its side and not clearly
                # beaten: it stays, however the strikes around it wobble.
                pick = keeper
            elif tie > 0:
                # Rank 1 is the strike price reaches first among those within
                # the tie zone of the largest; the rest keep magnitude order.
                floor = largest * (1.0 - tie)
                pick = min(
                    (sg for sg in candidates if sg[1] >= floor),
                    key=lambda sg: nearest_first(sg[0]),
                )
            else:
                pick = candidates[0]
            candidates = [pick] + [sg for sg in candidates if sg is not pick]
        return [
            {
                "rank": rank,
                "label": wall_label(side, rank),
                "strike": strike,
                "strength": abs(gamma * dollar_scale),
            }
            for rank, (strike, gamma) in enumerate(candidates[:depth], start=1)
        ]

    call_walls = _rank(agg_call, lambda s: s >= split, lambda s: s, "call", held_call)
    put_walls = _rank(agg_put, lambda s: s <= split, lambda s: -s, "put", held_put)
    return call_walls, put_walls


def _incumbent_strikes(incumbent: Optional[WallPair]) -> Tuple[Optional[float], Optional[float]]:
    """``incumbent`` as two floats, a side ``None`` when absent or unreadable."""
    if not incumbent:
        return None, None

    def _f(value: Any) -> Optional[float]:
        try:
            return None if value is None else float(value)
        except (TypeError, ValueError):
            return None

    return _f(incumbent[0]), _f(incumbent[1])


def align_wall_ladder(
    ladder: List[Dict[str, Any]],
    primary_strike: Optional[float],
    side: str,
    depth: int = DEFAULT_WALL_LADDER_DEPTH,
) -> List[Dict[str, Any]]:
    """Force ``primary_strike`` to rank 1 of an already-ranked ``ladder``.

    Callers that recompute the ladder from ``gex_by_strike`` but report the
    primary wall from somewhere else — ``/api/gex/summary`` reads the
    Analytics-Engine-persisted ``gex_summary.call_wall`` and ranks the ladder
    in SQL by plain magnitude — can end up with a ladder whose ``C1`` is not
    the ``call_wall`` drawn beside it: the published wall is the tie-zone
    pick, which need not be the largest strike, and the anchor it was split
    on may sit away from the price the ladder was split on.

    Rather than let a chart draw ``C1`` at one price and "Call Wall" at
    another, promote the reported wall and renumber everything below it.  A
    promoted strike that was not in the recomputed ladder has no ranked gamma
    to quote, so its ``strength`` is ``None`` — honest about the mismatch
    instead of inventing a magnitude.

    :param ladder: entries as produced by :func:`compute_wall_ladder`.
    :param primary_strike: the wall the caller reports as canonical.  ``None``
        (no wall) leaves the ladder untouched — there is nothing to align to.
    :param side: ``"call"`` or ``"put"``, for relabelling.
    :param depth: ranks to keep after promotion.
    :returns: a new list; the input is not mutated.
    """
    if primary_strike is None:
        return ladder[:depth]

    primary = float(primary_strike)
    rest = [w for w in ladder if w["strike"] != primary]
    existing = next((w for w in ladder if w["strike"] == primary), None)
    head: Dict[str, Any] = {
        "rank": 1,
        "label": wall_label(side, 1),
        "strike": primary,
        "strength": existing["strength"] if existing else None,
    }
    out = [head]
    for rank, entry in enumerate(rest[: max(0, depth - 1)], start=2):
        out.append({**entry, "rank": rank, "label": wall_label(side, rank)})
    return out


def _et_day(ts: datetime) -> date:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(_ET).date()


def wall_break_buffer(
    spot_price: float,
    typical_move_30m: Optional[float],
    *,
    move_fraction: float = WALL_BREAK_MOVE_FRACTION,
    min_pct: float = WALL_BREAK_MIN_PCT,
) -> float:
    """How far past a strike price must close before the strike changes sides.

    The larger of ``move_fraction`` of the typical 30-minute range (the
    volatility yardstick ``AnalyticsEngine._typical_move_30m`` computes: a
    5-day median of half-hour high-low ranges) and ``min_pct`` of spot.  A
    fixed percentage alone reads the same on a quiet morning and a fast
    afternoon; scaling by how far price usually travels makes "price plowed
    through the wall" mean the same thing in both.  The floor keeps the
    buffer sane when the typical move is unknown or unusually small — and a
    pinned market is exactly when price chops tightest around a big strike.
    """
    if spot_price is None or spot_price <= 0:
        return 0.0
    floor = max(0.0, float(min_pct)) * float(spot_price)
    if typical_move_30m is None or typical_move_30m <= 0:
        return floor
    return max(floor, max(0.0, float(move_fraction)) * float(typical_move_30m))


def step_wall_anchor(prev_anchor: Optional[float], spot_price: float, buffer: float) -> float:
    """Advance the wall anchor by one bucket.

    The anchor is a sticky copy of spot: it stays put while price moves
    within ``buffer`` of it and otherwise follows price at exactly
    ``buffer`` behind (a "play" or backlash operator)::

        anchor = clamp(prev_anchor, spot - buffer, spot + buffer)

    Splitting strikes on the anchor instead of on spot is the same as giving
    every strike its own break test with no extra state: a strike only moves
    from the call side to the put side once price has been more than
    ``buffer`` above it, and only moves back once price has been more than
    ``buffer`` below it.  Price chopping around a strike therefore leaves its
    role alone, and price plowing through it hands the strike over on the
    first bucket beyond the buffer.  ``prev_anchor`` of ``None`` (the first
    bucket of a day) starts at spot.
    """
    if prev_anchor is None:
        return float(spot_price)
    b = max(0.0, float(buffer))
    return min(max(float(prev_anchor), spot_price - b), spot_price + b)


@dataclass(frozen=True)
class WallStep:
    """One bucket's answer from :class:`WallTracker`.

    ``anchor`` is the price the walls are split on.  ``refresh_ts`` is the
    bucket whose rows the walls are picked from: this bucket when
    ``refreshed``, otherwise the last bucket that re-picked.  Every
    expiration selection reads the same two values, which is what makes them
    break and hold together.  ``incumbent`` is, on a re-pick, the payload the
    previous bucket ended on -- the walls in place, which keep their place
    unless clearly beaten (see :func:`compute_wall_ladder`).  ``None`` on the
    first bucket of a day and on a hold.
    """

    anchor: float
    refresh_ts: datetime
    refreshed: bool
    incumbent: Any = None


class WallTracker:
    """When one symbol's walls are re-picked, and the price they are split on.

    Re-picking the walls every minute, even split on the wall anchor, still
    flips them: strike sizes wobble as price jiggles (0DTE gamma most of all)
    and any re-pick that lands near a tie can go either way.  So the walls are
    re-picked only when there is a reason to:

    * the wall anchor moved -- price closed beyond the break buffer, so a
      strike may have changed sides and the walls must follow at once;
    * ``refresh_minutes`` have passed since the last re-pick -- slow drift
      such as time decay or a shift in implied volatility, while price sat
      still;
    * the first bucket of a New York day.

    Between re-picks the walls are the ones picked at ``refresh_ts``.  The
    tracker never looks at gamma, only at price and the clock, so every
    expiration selection re-picks on the same minutes and splits on the same
    anchor.  ``refresh_minutes <= 0`` re-picks every bucket.  A re-pick is
    not a fresh start either: :attr:`WallStep.incumbent` carries the walls in
    place, which the pick keeps unless a rival is clearly bigger.

    The engine recomputes a bucket several times while its minute is open, so
    each update steps from the state the PREVIOUS bucket ended on, never from
    an earlier pass over the same bucket: a bucket's answer depends only on
    the prior bucket's final state and this bucket's latest spot.

    The tracker also carries the walls themselves, as an opaque payload the
    caller stores with :meth:`hold` on a re-pick and reads back with
    :meth:`held`.  It follows the same per-bucket bookkeeping, so a bucket
    that re-picks on one pass and holds on a later one ends up holding the
    walls of the last real re-pick, not those of its own abandoned pass.
    """

    def __init__(self, refresh_minutes: float = WALL_REFRESH_MINUTES) -> None:
        self.refresh_every = timedelta(minutes=max(0.0, float(refresh_minutes)))
        self._day: Optional[date] = None
        self._bucket: Optional[datetime] = None
        self._anchor: Optional[float] = None
        self._refresh_ts: Optional[datetime] = None
        self._payload: Any = None
        self._prior_anchor: Optional[float] = None
        self._prior_refresh_ts: Optional[datetime] = None
        self._prior_payload: Any = None

    def seed(
        self,
        bucket_ts: datetime,
        anchor: Any,
        refresh_ts: Optional[datetime],
        payload: Any = None,
    ) -> None:
        """Resume from the state stored for an EARLIER bucket (after a restart).

        ``payload`` is the walls that bucket published.  A ``refresh_ts`` or
        ``payload`` of ``None`` (a row written before re-pick timing existed)
        makes the next bucket re-pick.
        """
        self._day = _et_day(bucket_ts)
        self._bucket = bucket_ts
        self._anchor = None if anchor is None else float(anchor)
        self._refresh_ts = refresh_ts
        self._payload = payload if refresh_ts is not None else None
        self._prior_anchor = None
        self._prior_refresh_ts = None
        self._prior_payload = None

    def update(self, spot_price: float, buffer: float, bucket_ts: datetime) -> WallStep:
        """This bucket's anchor and re-pick decision, given its latest spot."""
        day = _et_day(bucket_ts)
        if day != self._day:
            self._day = day
            self._bucket = None
            self._anchor = None
            self._refresh_ts = None
            self._payload = None
        if bucket_ts != self._bucket:
            self._prior_anchor = self._anchor
            self._prior_refresh_ts = self._refresh_ts
            self._prior_payload = self._payload
            self._bucket = bucket_ts
        anchor = step_wall_anchor(self._prior_anchor, spot_price, buffer)
        prior_refresh_ts = self._prior_refresh_ts
        self._anchor = anchor
        if (
            prior_refresh_ts is None
            or self._prior_anchor is None
            or self._prior_payload is None
            or anchor != self._prior_anchor
            or bucket_ts - prior_refresh_ts >= self.refresh_every
        ):
            self._refresh_ts = bucket_ts
            self._payload = None  # set by hold() once this pass has its walls
            return WallStep(
                anchor=anchor,
                refresh_ts=bucket_ts,
                refreshed=True,
                incumbent=self._prior_payload,
            )
        self._refresh_ts = prior_refresh_ts
        self._payload = self._prior_payload
        return WallStep(anchor=anchor, refresh_ts=prior_refresh_ts, refreshed=False)

    def hold(self, payload: Any) -> None:
        """Store the walls a re-picking bucket published."""
        self._payload = payload

    def held(self) -> Any:
        """The walls picked at the current ``refresh_ts`` (``None`` until held)."""
        return self._payload


def _percentile_linear(values: List[float], percentile: float) -> float:
    """``numpy.percentile(values, percentile)`` with the default linear
    interpolation, in pure Python.

    :func:`compute_gamma_flip_from_strikes` needs the same p90 reference the
    canonical resolver builds with numpy, but ``walls.py`` is deliberately
    stdlib-only (it is imported by the API layer on every
    strike-profile-timeseries request).  Reimplementing the one statistic
    keeps that property and keeps the two floors numerically identical.
    """
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * (percentile / 100.0)
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (idx - lo) * (ordered[hi] - ordered[lo])


def compute_gamma_flip_from_strikes(
    gex_by_strike: Iterable[Mapping[str, Any]],
    spot_price: float,
) -> Optional[float]:
    """Best-available gamma flip for an arbitrary expiration subset.

    The canonical gamma flip
    (:meth:`src.analytics.main_engine.AnalyticsEngine._calculate_gamma_flip_point`)
    is the zero crossing of the **spot-shift** dealer-gamma profile — every
    option's gamma re-priced across a hypothetical-price grid.  That needs
    the live chain with per-strike IV, which isn't persisted in the
    ``gex_by_strike`` snapshot, so it can't be rebuilt for an arbitrary
    subset of expirations (or for any historical bucket).

    This helper computes the pragmatic proxy the app already describes to
    users ("the low→high cumulative curve whose zero crossing is the gamma
    flip", see the GEX-Profile / Net-GEX-at-spot copy): accumulate net
    dealer gamma (``call_gamma - put_gamma``) across strikes ascending and
    return the price where the running total changes sign.  The scaling
    constant (``100 × S² × 0.01``) that turns raw gamma into dollar GEX is
    positive and common to every strike, so the crossing strike is
    scale-invariant — passing raw summed gamma yields the same answer as
    passing dollar GEX.

    **Gated like the canonical resolver.**  A raw nearest-crossing scan is
    not safe on this curve.  The cumulative starts at ~0 on the lowest
    strike and ends at the book's TOTAL net gamma, so when that total is
    negative — the ordinary state of an afternoon 0DTE book — the curve
    leaves zero through the put mass and never returns.  The only sign
    changes left are in the deep-OTM tail, where ``γ × OI`` has decayed to
    denormal-small values and the running total wobbles across zero at the
    1e-45 level.  Scanning for the crossing nearest spot then reports the
    TOP EDGE OF THAT NOISE BAND as the flip: a line drawn tens of dollars
    below the entire gamma cluster, which is the same "flip walked off the
    bottom of the chart" pathology
    :meth:`~src.analytics.main_engine.AnalyticsEngine._find_structural_interior_crossing`
    exists to prevent on the canonical path.

    So this applies that method's three gates, against the same config
    constants, translated onto the cumulative curve:

      * **Interior** — the candidate sits inside the strike range by
        ``GAMMA_PROFILE_INTERIOR_MARGIN`` of its width; edge crossings are
        the tail by construction.
      * **Structural** — ``|cumulative|`` peaks at no less than
        ``GAMMA_PROFILE_STRUCTURAL_MIN_FRAC × p90(|cumulative|)`` somewhere in
        a window around the candidate.  A genuine crossing is a curve that
        travels — it dives through real put gamma and climbs back through
        real call gamma, so it is large on at least one side.  A noise
        crossing is flat near zero on both.  The window is
        ``GAMMA_PROFILE_STRUCTURAL_WINDOW_PCT`` of the candidate but never
        narrower than the two strikes bracketing it: a strike ladder is far
        coarser than the canonical price grid, and a fixed 1% window can
        otherwise contain no strike at all.
      * **Actionable distance** — within
        ``GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT`` of ``spot_price``.

    With several qualifying crossings on a lumpy book, keep the one nearest
    spot (the actionable level / established tie-break, matching the
    canonical resolver).

    :param gex_by_strike: rows with at least ``strike``, ``call_gamma``,
        ``put_gamma``.  Rows may be per-(strike, expiration) — they're
        aggregated by strike first, same as :func:`compute_call_put_walls`.
        Filter to the desired expiration grouping on the caller side.
    :param spot_price: current underlying price; used for the
        actionable-distance gate and as the nearest-crossing tie-break when
        the cumulative curve crosses zero more than once (a lumpy book).
    :returns: the flip price, or ``None`` when the curve is one-signed
        across the whole chain, when no crossing clears the gates, or when
        the inputs are unusable.  ``None`` means *unresolved* — callers draw
        no flip line rather than substituting a differently-scoped level
        (a whole-chain flip over subset bars is the contradiction
        ``core/gammaRegime`` documents on the web side).
    """
    if spot_price is None or spot_price <= 0:
        return None

    agg: "defaultdict[float, float]" = defaultdict(float)
    for row in gex_by_strike:
        try:
            strike = float(row["strike"])
        except (KeyError, TypeError, ValueError):
            continue
        agg[strike] += float(row.get("call_gamma") or 0.0) - float(
            row.get("put_gamma") or 0.0
        )

    if len(agg) < 2:
        # A single strike (or none) has no interval over which the
        # cumulative curve can cross zero.
        return None

    # Build the ascending cumulative curve [(strike, running_net_gamma), …].
    cumulative = 0.0
    curve: List[Tuple[float, float]] = []
    for strike in sorted(agg.keys()):
        cumulative += agg[strike]
        curve.append((strike, cumulative))

    # ── Gate inputs ────────────────────────────────────────────────────────
    lo_strike, hi_strike = curve[0][0], curve[-1][0]
    width = hi_strike - lo_strike
    if width <= 0:
        return None
    interior_lo = lo_strike + GAMMA_PROFILE_INTERIOR_MARGIN * width
    interior_hi = hi_strike - GAMMA_PROFILE_INTERIOR_MARGIN * width

    abs_curve = [abs(value) for _, value in curve]
    if max(abs_curve) <= 0.0:
        # Identically-zero curve: no basis for the structural test.
        return None
    reference = _percentile_linear(
        abs_curve, GAMMA_PROFILE_STRUCTURAL_REFERENCE_PERCENTILE
    )
    if reference <= 0.0:
        reference = max(abs_curve)
    floor_abs = GAMMA_PROFILE_STRUCTURAL_MIN_FRAC * reference

    best_flip: Optional[float] = None
    best_dist = float("inf")

    def _consider(candidate: float, bracket_gap: float) -> None:
        nonlocal best_flip, best_dist
        if candidate < interior_lo or candidate > interior_hi:
            return
        if abs(candidate - spot_price) / spot_price > GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT:
            return
        half = max(GAMMA_PROFILE_STRUCTURAL_WINDOW_PCT * candidate, bracket_gap)
        window_peak = 0.0
        for strike, value in curve:
            if candidate - half <= strike <= candidate + half:
                magnitude = abs(value)
                if magnitude > window_peak:
                    window_peak = magnitude
        if window_peak < floor_abs:
            return
        dist = abs(candidate - spot_price)
        if dist < best_dist:
            best_dist = dist
            best_flip = candidate

    # Same crossing scan the canonical resolver uses: exact zeros count, and
    # a sign change between adjacent points is linearly interpolated.  Every
    # candidate goes through the gates above; with several survivors keep the
    # one nearest spot.
    for i in range(len(curve) - 1):
        s1, c1 = curve[i]
        s2, c2 = curve[i + 1]
        if c1 == 0.0:
            _consider(s1, s2 - s1)
        elif c1 * c2 < 0.0:
            _consider(s1 + (s2 - s1) * (-c1) / (c2 - c1), s2 - s1)
    last_s, last_c = curve[-1]
    if last_c == 0.0:
        _consider(last_s, last_s - curve[-2][0])

    return best_flip


# SQL fragment exposed for callers that need to compute walls directly in
# Postgres against ``gex_by_strike``.  Parameters: ``$strike`` column,
# ``$call_gamma`` column, ``$put_gamma`` column, ``$spot`` numeric.  Wrap in a
# CTE that selects from the relevant partition (e.g. a single timestamp).
#
# This is the SQL counterpart of :func:`compute_call_put_walls` with
# ``tie_pct=0`` split on spot -- the plain argmax, without the tie zone or the
# wall anchor, which need Python.  It is used by ``get_historical_gex`` for
# buckets that pre-date the column backfill.  New writes go through the
# Analytics Engine, which calls the Python helper and persists the result to
# ``gex_summary.call_wall`` / ``gex_summary.put_wall``.
#
# Note the GROUP BY strike — ``gex_by_strike`` is keyed
# ``(strike, expiration)`` and the Python helper aggregates by strike before
# ranking; the SQL fallback must match.
CANONICAL_WALL_SQL_DOC = """
call_wall (per timestamp):
    WITH per_strike AS (
        SELECT strike, SUM(COALESCE(call_gamma, 0)) AS call_gamma
        FROM gex_by_strike
        WHERE underlying = :symbol AND timestamp = :ts AND strike >= :spot
        GROUP BY strike
    )
    SELECT strike
    FROM per_strike
    WHERE call_gamma > 0
    ORDER BY call_gamma DESC, strike ASC
    LIMIT 1;

put_wall (per timestamp):
    WITH per_strike AS (
        SELECT strike, SUM(COALESCE(put_gamma, 0)) AS put_gamma
        FROM gex_by_strike
        WHERE underlying = :symbol AND timestamp = :ts AND strike <= :spot
        GROUP BY strike
    )
    SELECT strike
    FROM per_strike
    WHERE put_gamma > 0
    ORDER BY put_gamma DESC, strike DESC
    LIMIT 1;
""".strip()
