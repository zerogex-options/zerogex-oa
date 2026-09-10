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

The canonical definition (industry-standard, matching SpotGamma /
SqueezeMetrics / Cheddar Flow):

* **Call Wall** — strike at or above spot with the largest dollar call gamma
  exposure ``γ_call × OI × 100 × S² × 0.01``, aggregated across the
  expirations the caller chose to include.  Ties broken by nearest-to-spot
  (lowest strike above spot wins).
* **Put Wall**  — strike at or below spot with the largest dollar put gamma
  exposure ``γ_put  × OI × 100 × S² × 0.01``, aggregated across the
  expirations the caller chose to include.  Ties broken by nearest-to-spot
  (highest strike below spot wins).

The **wall ladder** (:func:`compute_wall_ladder`) generalises that to the
top-N strikes per side — ``C1``/``C2``/``C3`` above spot and ``P1``/``P2``/
``P3`` below — using the *same* ordering, so ``C1`` and ``P1`` are by
construction the Call Wall and Put Wall above.  Secondary walls are pure
rank: the 2nd-largest eligible strike is ``C2`` even when it sits one tick
from ``C1``.  No minimum-separation filter is applied, because any spacing
rule would make the ladder disagree with the per-strike bars the charts
draw right beside it.

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
* The spot-direction filter (``strike >= spot`` for call, ``strike <= spot``
  for put) preserves the structural meaning of a "wall": calls above act as
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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

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
    :param spot_price: current underlying price; used to split strikes into
        the above-spot (call) and below-spot (put) regions.
    :returns: ``(call_wall_strike, put_wall_strike)``.  Either side is
        ``None`` when no eligible strike exists (e.g. all-zero gamma on that
        side, or no strikes on that side of spot).

    Tie-breaking matches the SQL counterpart in
    :mod:`src.api.database` and the wall-migration query in
    :mod:`src.signals.unified_signal_engine`:

    * Call wall ties → lowest strike (nearest to spot from above).
    * Put wall ties  → highest strike (nearest to spot from below).

    This is the strike-only view.  Callers that also need the wall's
    dollar-gamma magnitude (e.g. TradeWorkz position sizing) should call
    :func:`compute_call_put_walls_with_strength`, which shares this exact
    ranking and additionally returns the dollar exposure at each wall.
    """
    call_wall, put_wall, _cw_strength, _pw_strength = compute_call_put_walls_with_strength(
        gex_by_strike, spot_price
    )
    return call_wall, put_wall


def compute_call_put_walls_with_strength(
    gex_by_strike: Iterable[Mapping[str, Any]],
    spot_price: float,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Return ``(call_wall, put_wall, call_wall_strength, put_wall_strength)``.

    Same wall-selection ranking as :func:`compute_call_put_walls`, plus the
    **dollar-gamma magnitude at each wall strike**.  Both are the rank-1
    entries of :func:`compute_wall_ladder`, which is the single ranking
    implementation the three functions share — so ``C1`` on the ladder can
    never disagree with the scalar ``call_wall`` a caller reads beside it.

    The magnitude is the OI-weighted gamma aggregate the ranking selected
    on, converted to dollar GEX per 1% move via the canonical
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
    call_walls, put_walls = compute_wall_ladder(gex_by_strike, spot_price, depth=1)
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
    :param spot_price: current underlying price; splits strikes into the
        above-spot (call) and below-spot (put) regions.
    :param depth: how many ranks to return per side.  Clamped to
        ``[0, MAX_WALL_LADDER_DEPTH]``.  Fewer are returned when the chain
        has fewer eligible strikes on that side — a short list means the
        book genuinely has no further wall, so callers should render what
        they get rather than padding.
    :returns: two lists ordered by rank ascending (strongest first).  Both
        are empty when ``spot_price`` is unusable.

    Ordering per side extends the primary tie-break exactly:

    * Call walls → ``call_gamma`` DESC, then strike ASC (nearest above spot).
    * Put walls  → ``put_gamma``  DESC, then strike DESC (nearest below spot).

    Ranks are pure magnitude order.  Two adjacent strikes can be ``C1`` and
    ``C2``; the chain, not a spacing heuristic, decides.
    """
    depth = max(0, min(int(depth), MAX_WALL_LADDER_DEPTH))
    if depth == 0 or spot_price is None or spot_price <= 0:
        return [], []

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
    ) -> List[Dict[str, Any]]:
        candidates = [
            (strike, gamma) for strike, gamma in agg.items() if gamma > 0 and eligible(strike)
        ]
        # Sort key mirrors the SQL ``ORDER BY gamma DESC, strike ASC|DESC``.
        candidates.sort(key=lambda sg: (-sg[1], nearest_first(sg[0])))
        return [
            {
                "rank": rank,
                "label": wall_label(side, rank),
                "strike": strike,
                "strength": abs(gamma * dollar_scale),
            }
            for rank, (strike, gamma) in enumerate(candidates[:depth], start=1)
        ]

    call_walls = _rank(agg_call, lambda s: s >= spot_price, lambda s: s, "call")
    put_walls = _rank(agg_put, lambda s: s <= spot_price, lambda s: -s, "put")
    return call_walls, put_walls


def align_wall_ladder(
    ladder: List[Dict[str, Any]],
    primary_strike: Optional[float],
    side: str,
    depth: int = DEFAULT_WALL_LADDER_DEPTH,
) -> List[Dict[str, Any]]:
    """Force ``primary_strike`` to rank 1 of an already-ranked ``ladder``.

    Callers that recompute the ladder from ``gex_by_strike`` but report the
    primary wall from somewhere else — ``/api/gex/summary`` reads the
    Analytics-Engine-persisted ``gex_summary.call_wall``, and recomputes the
    ladder against the latest ``underlying_quotes`` close — can end up with a
    ladder whose ``C1`` is not the ``call_wall`` drawn beside it.  Normally
    the two agree exactly (same rows, same helper); they diverge only when
    spot has moved across a strike since the engine wrote the row, which
    changes which side of spot that strike falls on and so whether it is
    eligible at all.

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

    :param gex_by_strike: rows with at least ``strike``, ``call_gamma``,
        ``put_gamma``.  Rows may be per-(strike, expiration) — they're
        aggregated by strike first, same as :func:`compute_call_put_walls`.
        Filter to the desired expiration grouping on the caller side.
    :param spot_price: current underlying price; used only as the
        nearest-crossing tie-break when the cumulative curve crosses zero
        more than once (a lumpy book), matching the canonical resolver.
    :returns: the flip price, or ``None`` when the curve is one-signed
        across the whole chain (no crossing) or the inputs are unusable.
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
    curve: list[Tuple[float, float]] = []
    for strike in sorted(agg.keys()):
        cumulative += agg[strike]
        curve.append((strike, cumulative))

    best_flip: Optional[float] = None
    best_dist = float("inf")

    def _consider(candidate: float) -> None:
        nonlocal best_flip, best_dist
        dist = abs(candidate - spot_price)
        if dist < best_dist:
            best_dist = dist
            best_flip = candidate

    # Same crossing scan the canonical resolver uses: exact zeros count, and
    # a sign change between adjacent points is linearly interpolated.  With
    # multiple crossings keep the one nearest spot.
    for i in range(len(curve) - 1):
        s1, c1 = curve[i]
        s2, c2 = curve[i + 1]
        if c1 == 0.0:
            _consider(s1)
        elif c1 * c2 < 0.0:
            _consider(s1 + (s2 - s1) * (-c1) / (c2 - c1))
    last_s, last_c = curve[-1]
    if last_c == 0.0:
        _consider(last_s)

    return best_flip


# SQL fragment exposed for callers that need to compute walls directly in
# Postgres against ``gex_by_strike``.  Parameters: ``$strike`` column,
# ``$call_gamma`` column, ``$put_gamma`` column, ``$spot`` numeric.  Wrap in a
# CTE that selects from the relevant partition (e.g. a single timestamp).
#
# This is the canonical SQL counterpart of :func:`compute_call_put_walls` and
# is used by ``get_historical_gex`` for buckets that pre-date the column
# backfill.  New writes go through the Analytics Engine, which calls the
# Python helper and persists the result to ``gex_summary.call_wall`` /
# ``gex_summary.put_wall``.
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
