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
* :data:`MARKET_RAW` — **per-contract QUOTED PRICES**: bid, ask, last and
  mid for an individual option contract. Three surfaces return them —
  ``/api/option/quote``, ``/api/option/contract`` and
  ``/api/tools/option-calculator`` — and this scope exists so they can be
  granted to the internal website BFF and **withheld from every external
  customer**.

  WHERE THE LINE IS DRAWN, AND WHY IT MOVED
  -----------------------------------------
  This scope used to be described as "the option chain enumerated contract
  by contract", on the theory that *what gets enumerated* — not whether the
  data is upstream — is the licensing trigger. That theory is disputed:
  ``docs/compliance/market-data-licensing-audit-2026-09-02.md`` (F5) holds
  that the trigger is whose data it is and whether an end user sees it, and
  that serving one symbol's live price is redistribution just as much as
  walking a chain. Counsel has not confirmed either reading.

  So the boundary here is deliberately NOT a claim about what is
  licensable. It is drawn on the one distinction the code can actually
  hold and test: **does the payload carry a quoted price?** That is
  enforceable (``tests/test_market_data_scope_boundary.py`` walks the
  mounted route table and asserts it) and it is honest about its own
  scope — an engineering boundary, not a legal conclusion.

  Two consequences worth stating plainly, because both have already been
  learned the expensive way:

  * MARKET_REFERENCE used to live inside MARKET_RAW, so switching
    enforcement on 2026-08-31 took out eleven paying integrations, of
    which nine only ever wanted the underlying's price.
  * ``/api/market/open-interest`` used to live here too, and it returns no
    quote at all — just ``open_interest`` and a derived ``exposure``. It
    was the only quoteless route in the bundle, and gating it withheld
    nothing: the same per-strike OI ships on GEX via
    ``/api/gex/by-strike`` and ``/api/gex/strike-profile-timeseries``
    (every strike, uncapped). All it did was 403 the integrations honest
    enough to ask for it by name. It now rides GEX.

  THE OPEN QUESTION
  -----------------
  Whether **per-strike open interest** is redistributable at all is not
  settled, and this taxonomy does not pretend to settle it. Today OI is
  served on :data:`GEX`, deliberately and in one place. If counsel
  determines it is not redistributable, the change is a known, finite set
  — ``/api/market/open-interest``, ``/api/gex/by-strike`` and
  ``/api/gex/strike-profile-timeseries`` — and it is a re-gating, not a
  rebuild. Recording it here means that decision gets made on purpose
  rather than discovered by a customer's 403.

  What is NOT open, and was settled on 2026-09-03:
  ``/api/gex/premium_surface`` rode :data:`GEX` while returning ``premium``
  — its own model calls it "quoted premium used (mid, or last as
  fallback)". It now requires :data:`MARKET_RAW`, because the surface
  cannot be served without the quote. ``intrinsic`` is
  ``max(0, spot - strike)``, so for every OTM strike ``extrinsic ==
  premium`` exactly, and elsewhere ``premium == extrinsic + intrinsic``.
  Redacting the ``premium`` field alone would leave the quote recoverable
  by addition — the one place where classifying by field, rather than by
  route, does not work. The vol surface stays on :data:`GEX` by contrast:
  it publishes implied volatilities, and an IV does not invert to a price
  without the rate, dividend and time conventions behind it.

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

#: The subset granted to external keys. Each of these is either a computed
#: output or reference data about the underlying the caller is already
#: analysing. ``MARKET_RAW`` — per-contract quoted prices — is deliberately
#: excluded; see the module docstring for where that boundary now sits and
#: which question about it is still open.
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
