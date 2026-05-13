"""Canonical Call/Put Wall computation.

Single source of truth for Call/Put Wall strikes consumed by:
  - ``gex_summary`` row written by :class:`src.analytics.main_engine.AnalyticsEngine`
  - ``/api/gex/summary`` and ``/api/gex/history`` endpoints
  - :class:`src.signals.unified_signal_engine.UnifiedSignalEngine` (current and
    ~30min-prior walls used by ``trap_detection`` and ``gamma_vwap_confluence``)
  - all playbook patterns that read ``ctx.level("call_wall" | "put_wall")``

The canonical definition (industry-standard, matching SpotGamma /
SqueezeMetrics / Cheddar Flow):

* **Call Wall** — strike at or above spot with the largest dollar call gamma
  exposure ``γ_call × OI × 100 × S² × 0.01``.  Ties broken by nearest-to-spot
  (lowest strike above spot wins).
* **Put Wall**  — strike at or below spot with the largest dollar put gamma
  exposure ``γ_put  × OI × 100 × S² × 0.01``.  Ties broken by nearest-to-spot
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
"""

from __future__ import annotations

from typing import Iterable, Mapping, Optional, Tuple


def compute_call_put_walls(
    gex_by_strike: Iterable[Mapping[str, object]],
    spot_price: float,
) -> Tuple[Optional[float], Optional[float]]:
    """Return ``(call_wall, put_wall)`` from per-strike gamma rows.

    :param gex_by_strike: iterable of rows with at least the keys ``strike``,
        ``call_gamma``, ``put_gamma``.  Extra keys are ignored.  Each row's
        ``call_gamma`` / ``put_gamma`` is the OI-weighted aggregate
        ``Σ(γ × OI)`` at that strike (same convention as
        :func:`src.analytics.main_engine.AnalyticsEngine._calculate_gex_by_strike`
        and the ``gex_by_strike`` table).
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

    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    best_call: float = 0.0
    best_put: float = 0.0

    for row in gex_by_strike:
        try:
            strike = float(row["strike"])
        except (KeyError, TypeError, ValueError):
            continue
        call_gamma = float(row.get("call_gamma") or 0.0)
        put_gamma = float(row.get("put_gamma") or 0.0)

        if strike >= spot_price and call_gamma > 0:
            # Strictly-greater keeps the lowest-strike tiebreaker because we
            # iterate strikes in input order; callers must pre-sort by strike
            # ascending if they want deterministic ties.  When ties arise we
            # explicitly prefer the lowest strike (nearest to spot).
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
CANONICAL_WALL_SQL_DOC = """
call_wall (per timestamp):
    SELECT strike
    FROM gex_by_strike
    WHERE underlying = :symbol AND timestamp = :ts AND strike >= :spot
      AND COALESCE(call_gamma, 0) > 0
    ORDER BY call_gamma DESC, strike ASC
    LIMIT 1;

put_wall (per timestamp):
    SELECT strike
    FROM gex_by_strike
    WHERE underlying = :symbol AND timestamp = :ts AND strike <= :spot
      AND COALESCE(put_gamma, 0) > 0
    ORDER BY put_gamma DESC, strike DESC
    LIMIT 1;
""".strip()
