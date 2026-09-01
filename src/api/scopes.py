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
* :data:`MARKET_REFERENCE` — the UNDERLYING's own tape: its current quote,
  its OHLC bars, prior-session closes and session high/low. Upstream data,
  but about the one instrument the caller is already charting, and every
  levels integration needs it to place a level against a price. Bundled
  with the derived scopes.
* :data:`MARKET_RAW` — **license-restricted market data**: the OPTION
  CHAIN enumerated contract by contract (per-contract quotes, per-contract
  open interest, the contract detail and calculator surfaces). Held in its
  own scope precisely so it can be granted to the internal website BFF and
  **withheld from every external customer** — the other scopes are broadly
  redistributable, this one is not.

  The line between the two is *what gets enumerated*, not whether the data
  is upstream. Fetching the price of the symbol you are analysing is not
  the redistribution concern; walking a vendor's option chain contract by
  contract is. Drawing it anywhere else has been tried and was wrong:
  MARKET_REFERENCE used to live inside MARKET_RAW, so switching
  enforcement on 2026-08-31 took out eleven paying integrations, of which
  nine only ever wanted the underlying's price. Under this split that
  change would have reached two, both genuinely reading the chain.

Tier bundles
------------
Tiers are named bundles of scopes — the unit of commercial packaging:

* :data:`TIER_ANALYTICS` — the clean, derived B2B/B2B2C product:
  GEX + FLOW + MAXPAIN + TECHNICALS + MARKET_REFERENCE. **No option chain,
  no signals.**
* :data:`TIER_SIGNALS` — analytics plus the signal engine.
* :data:`TIER_FULL` — everything *including* ``MARKET_RAW``. Intended for
  the internal website backend only, never for external resale.

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
MARKET_REFERENCE: str = "market_reference"
MARKET_RAW: str = "market_raw"

#: Every capability scope the API knows about.
ALL_SCOPES: FrozenSet[str] = frozenset(
    {GEX, FLOW, MAXPAIN, TECHNICALS, SIGNALS, MARKET_REFERENCE, MARKET_RAW}
)

#: The broadly licensable subset. Each of these is either a computed output
#: or reference data about the underlying the caller is already analysing.
#: ``MARKET_RAW`` — the option chain, contract by contract — is deliberately
#: excluded; it is the only thing here that cannot be redistributed.
DERIVED_SCOPES: FrozenSet[str] = frozenset(
    {GEX, FLOW, MAXPAIN, TECHNICALS, SIGNALS, MARKET_REFERENCE}
)

# --- Tier bundles ---------------------------------------------------------

TIER_ANALYTICS: str = "analytics"
TIER_SIGNALS: str = "signals"
TIER_FULL: str = "full"

#: Tier name -> the scopes it grants. ``TIER_FULL`` is the only bundle
#: that includes ``MARKET_RAW`` and is for the internal BFF only.
TIERS: Dict[str, FrozenSet[str]] = {
    TIER_ANALYTICS: frozenset({GEX, FLOW, MAXPAIN, TECHNICALS, MARKET_REFERENCE}),
    TIER_SIGNALS: frozenset({GEX, FLOW, MAXPAIN, TECHNICALS, SIGNALS, MARKET_REFERENCE}),
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
