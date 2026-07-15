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
from typing import Any, Iterable, Mapping, Optional, Tuple


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
    """
    if spot_price is None or spot_price <= 0:
        return None, None

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

    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    best_call: float = 0.0
    best_put: float = 0.0

    # Iterate strikes ascending so the tiebreaker (lowest strike above
    # spot for call, highest strike below spot for put) falls out of a
    # strictly-greater comparison without an explicit min/max-distance
    # rule.
    for strike in sorted(agg_call.keys() | agg_put.keys()):
        call_gamma = agg_call.get(strike, 0.0)
        put_gamma = agg_put.get(strike, 0.0)

        if strike >= spot_price and call_gamma > 0:
            if call_gamma > best_call or (
                call_gamma == best_call and (call_wall is None or strike < call_wall)
            ):
                best_call = call_gamma
                call_wall = strike

        if strike <= spot_price and put_gamma > 0:
            if put_gamma > best_put or (
                put_gamma == best_put and (put_wall is None or strike > put_wall)
            ):
                best_put = put_gamma
                put_wall = strike

    return call_wall, put_wall


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
