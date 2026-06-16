"""Scope taxonomy and tier bundles for the ZeroGEX API.

This is the single source of truth for *what capabilities exist* and
*which bundles (tiers) grant them*. It exists so three things stay in
lockstep:

1. the per-endpoint ``Depends(require_scopes(...))`` declarations in
   ``main.py`` (authorization);
2. the ``api_keys.scopes`` values provisioned by ``admin_keys`` (grants);
3. the commercial packaging — which tier a customer is sold.

Capability scopes
-----------------
Each scope names one analytics domain:

* :data:`GEX` — gamma-exposure analytics (summary, by-strike, profile,
  walls, vol-surface, flip horizon, volatility gauge).
* :data:`FLOW` — options-flow aggregates (by-contract, series, smart
  money, buying pressure).
* :data:`MAXPAIN` — max-pain analytics.
* :data:`TECHNICALS` — intraday technicals (VWAP, ORB, volume, momentum).
* :data:`SIGNALS` — the composite signal engine (Market State Index and
  components).
* :data:`MARKET_RAW` — **raw, license-restricted market data**: per-
  contract option quotes (bid/ask/last/volume/OI), underlying OHLC bars,
  and anything that re-exposes the upstream feed rather than a computed
  derivative. Held in its own scope precisely so it can be granted to the
  internal website BFF and **withheld from every external customer** — the
  derived scopes above are broadly redistributable, this one is not.
* :data:`DEV_PORTAL` — **self-serve developer-portal administration**:
  list/issue/rotate/revoke a logged-in SaaS user's own ``api_keys`` rows
  and read their ``api_usage_daily`` rollups. Held by the website BFF only
  — never by an external customer — because these endpoints mint
  bearer credentials, so a key carrying ``DEV_PORTAL`` can spawn keys for
  any end-user it can sign a token for.

Tier bundles
------------
Tiers are named bundles of scopes — the unit of commercial packaging:

* :data:`TIER_ANALYTICS` — the clean, derived B2B/B2B2C product:
  GEX + FLOW + MAXPAIN + TECHNICALS. **No raw data, no signals.**
* :data:`TIER_SIGNALS` — analytics plus the signal engine.
* :data:`TIER_FULL` — everything *including* ``MARKET_RAW`` and
  ``DEV_PORTAL``. Intended for the internal website backend only, never
  for external resale.

Enforcement is opt-in (``API_SCOPE_ENFORCEMENT`` in ``security.py``) and a
key carrying the wildcard ``"*"`` always passes, so adding these scope
declarations to endpoints is a no-op until keys are backfilled with the
tier bundle that matches what each caller is entitled to.
"""

from __future__ import annotations

from typing import Dict, FrozenSet, List

# --- Capability scopes ----------------------------------------------------

GEX: str = "gex"
FLOW: str = "flow"
MAXPAIN: str = "maxpain"
TECHNICALS: str = "technicals"
SIGNALS: str = "signals"
MARKET_RAW: str = "market_raw"
DEV_PORTAL: str = "dev_portal"

#: Every capability scope the API knows about.
ALL_SCOPES: FrozenSet[str] = frozenset(
    {GEX, FLOW, MAXPAIN, TECHNICALS, SIGNALS, MARKET_RAW, DEV_PORTAL}
)

#: The derived-analytics scopes — the subset that is broadly licensable
#: for redistribution because each is a computed output, not raw upstream
#: market data. ``MARKET_RAW`` and ``DEV_PORTAL`` are deliberately excluded.
DERIVED_SCOPES: FrozenSet[str] = frozenset({GEX, FLOW, MAXPAIN, TECHNICALS, SIGNALS})

# --- Tier bundles ---------------------------------------------------------

TIER_ANALYTICS: str = "analytics"
TIER_SIGNALS: str = "signals"
TIER_FULL: str = "full"

#: Tier name -> the scopes it grants. ``TIER_FULL`` is the only bundle
#: that includes ``MARKET_RAW`` and is for the internal BFF only.
TIERS: Dict[str, FrozenSet[str]] = {
    TIER_ANALYTICS: frozenset({GEX, FLOW, MAXPAIN, TECHNICALS}),
    TIER_SIGNALS: frozenset({GEX, FLOW, MAXPAIN, TECHNICALS, SIGNALS}),
    TIER_FULL: frozenset(ALL_SCOPES),
}


def expand_tier(name: str) -> List[str]:
    """Return the sorted scope list granted by tier ``name``.

    Raises ``KeyError`` (with the set of known tiers) for an unknown tier
    so a typo at provisioning time fails loudly rather than silently
    issuing a key with no scopes.
    """
    try:
        scopes = TIERS[name]
    except KeyError:
        raise KeyError(f"unknown tier {name!r}; known tiers: {sorted(TIERS)}") from None
    return sorted(scopes)
