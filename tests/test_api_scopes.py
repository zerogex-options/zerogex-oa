"""Scope taxonomy, tier bundles, require_scopes enforcement, and the
admin_keys ``--tier`` provisioning flow.

The enforcement tests follow the module-reload pattern used by the other
``test_api_*`` suites: ``API_SCOPE_ENFORCEMENT`` is read at import time in
``src.api.security``, so a test that needs it on flushes ``src.api.*`` from
``sys.modules`` and re-imports.
"""

from __future__ import annotations

import asyncio
import importlib
import sys

import pytest
from fastapi import HTTPException

from src.api import scopes

# --------------------------------------------------------------------------
# Taxonomy invariants
# --------------------------------------------------------------------------


def test_market_raw_is_isolated_from_derived():
    """The whole point of the taxonomy: raw market data is its own scope,
    never folded into the derived bundle."""
    assert scopes.MARKET_RAW not in scopes.DERIVED_SCOPES
    assert scopes.MARKET_RAW in scopes.ALL_SCOPES
    assert scopes.DERIVED_SCOPES < scopes.ALL_SCOPES


def test_all_scopes_is_the_union():
    assert scopes.ALL_SCOPES == scopes.DERIVED_SCOPES | {scopes.MARKET_RAW}


def test_analytics_tier_excludes_raw_and_signals():
    """The clean B2B/B2B2C product: derived analytics plus the underlying's
    own tape, no option chain, no signals."""
    bundle = scopes.TIERS[scopes.TIER_ANALYTICS]
    assert scopes.MARKET_RAW not in bundle
    assert scopes.SIGNALS not in bundle
    assert bundle == {
        scopes.GEX,
        scopes.FLOW,
        scopes.MAXPAIN,
        scopes.TECHNICALS,
        scopes.MARKET_REFERENCE,
    }


def test_analytics_tier_can_price_a_level():
    """A charting integration must be able to fetch the underlying's price.
    Without MARKET_REFERENCE it can compute a call wall and has nothing to
    draw it against — which is how enforcement took out nine integrations
    that never wanted the option chain at all."""
    assert scopes.MARKET_REFERENCE in scopes.TIERS[scopes.TIER_ANALYTICS]
    assert scopes.MARKET_REFERENCE in scopes.DERIVED_SCOPES


def test_reference_and_raw_are_distinct_scopes():
    """Collapsing them is the bug this split exists to prevent."""
    assert scopes.MARKET_REFERENCE != scopes.MARKET_RAW
    assert scopes.MARKET_RAW not in scopes.DERIVED_SCOPES
    assert {scopes.MARKET_REFERENCE, scopes.MARKET_RAW} <= scopes.ALL_SCOPES


def test_signals_tier_is_analytics_plus_signals():
    assert scopes.TIERS[scopes.TIER_SIGNALS] == (
        scopes.TIERS[scopes.TIER_ANALYTICS] | {scopes.SIGNALS}
    )


def test_full_tier_is_the_only_bundle_with_raw():
    assert scopes.MARKET_RAW in scopes.TIERS[scopes.TIER_FULL]
    assert scopes.TIERS[scopes.TIER_FULL] == scopes.ALL_SCOPES
    raw_bearing = [t for t, s in scopes.TIERS.items() if scopes.MARKET_RAW in s]
    assert raw_bearing == [scopes.TIER_FULL]


def test_expand_tier_returns_sorted_scopes():
    assert scopes.expand_tier(scopes.TIER_ANALYTICS) == sorted(scopes.TIERS[scopes.TIER_ANALYTICS])


def test_expand_unknown_tier_raises():
    with pytest.raises(KeyError):
        scopes.expand_tier("enterprise-platinum")


# --------------------------------------------------------------------------
# require_scopes enforcement semantics
# --------------------------------------------------------------------------


def _reload_security(monkeypatch: pytest.MonkeyPatch, *, enforce: bool):
    monkeypatch.delenv("API_SCOPE_ENFORCEMENT", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    if enforce:
        monkeypatch.setenv("API_SCOPE_ENFORCEMENT", "1")
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    return importlib.import_module("src.api.security")


def _run(dep, info):
    return asyncio.run(dep(info=info))


def test_enforcement_off_allows_missing_scope(monkeypatch: pytest.MonkeyPatch):
    security = _reload_security(monkeypatch, enforce=False)
    dep = security.require_scopes(scopes.GEX)
    # Key has no scopes; with enforcement off this is a (logged) dry-run pass.
    assert _run(dep, {"user_id": "u", "scopes": []}) == {"user_id": "u", "scopes": []}


def test_enforcement_on_blocks_missing_scope(monkeypatch: pytest.MonkeyPatch):
    security = _reload_security(monkeypatch, enforce=True)
    dep = security.require_scopes(scopes.MARKET_RAW)
    with pytest.raises(HTTPException) as exc:
        _run(dep, {"user_id": "u", "scopes": [scopes.GEX, scopes.FLOW]})
    assert exc.value.status_code == 403


def test_enforcement_on_allows_present_scope(monkeypatch: pytest.MonkeyPatch):
    security = _reload_security(monkeypatch, enforce=True)
    dep = security.require_scopes(scopes.GEX)
    info = {"user_id": "u", "scopes": [scopes.GEX, scopes.FLOW]}
    assert _run(dep, info) == info


def test_wildcard_scope_passes_everything(monkeypatch: pytest.MonkeyPatch):
    security = _reload_security(monkeypatch, enforce=True)
    dep = security.require_scopes(scopes.MARKET_RAW)
    info = {"user_id": "bff", "scopes": ["*"]}
    assert _run(dep, info) == info


def test_none_info_passes(monkeypatch: pytest.MonkeyPatch):
    """Static break-glass / disabled-auth contexts have no scope list."""
    security = _reload_security(monkeypatch, enforce=True)
    dep = security.require_scopes(scopes.SIGNALS)
    assert _run(dep, None) is None


def test_full_tier_grant_passes_raw_endpoint(monkeypatch: pytest.MonkeyPatch):
    """A key provisioned with TIER_FULL clears the MARKET_RAW gate; a key
    provisioned with TIER_ANALYTICS does not — the licensing line, enforced."""
    security = _reload_security(monkeypatch, enforce=True)
    raw_gate = security.require_scopes(scopes.MARKET_RAW)

    full = {"user_id": "bff", "scopes": scopes.expand_tier(scopes.TIER_FULL)}
    assert _run(raw_gate, full) == full

    analytics = {"user_id": "cust", "scopes": scopes.expand_tier(scopes.TIER_ANALYTICS)}
    with pytest.raises(HTTPException) as exc:
        _run(raw_gate, analytics)
    assert exc.value.status_code == 403


# --------------------------------------------------------------------------
# admin_keys --tier provisioning (merge tier bundle + explicit scopes)
# --------------------------------------------------------------------------


def test_admin_keys_tier_expands_and_merges(monkeypatch: pytest.MonkeyPatch):
    from src.api import admin_keys

    captured = {}

    async def _fake_create(user_id, name, scopes_arg):
        captured["user_id"] = user_id
        captured["name"] = name
        captured["scopes"] = scopes_arg
        return 0

    monkeypatch.setattr(admin_keys, "_create", _fake_create)
    rc = admin_keys.main(
        [
            "create",
            "alice@example.com",
            "--name",
            "alice-prod",
            "--tier",
            scopes.TIER_ANALYTICS,
            "--scope",
            "beta_feature",
        ]
    )
    assert rc == 0
    expected = sorted(set(scopes.expand_tier(scopes.TIER_ANALYTICS)) | {"beta_feature"})
    assert captured["scopes"] == expected
    assert scopes.MARKET_RAW not in captured["scopes"]


def test_admin_keys_no_tier_no_scope_is_none(monkeypatch: pytest.MonkeyPatch):
    from src.api import admin_keys

    captured = {}

    async def _fake_create(user_id, name, scopes_arg):
        captured["scopes"] = scopes_arg
        return 0

    monkeypatch.setattr(admin_keys, "_create", _fake_create)
    rc = admin_keys.main(["create", "svc", "--name", "svc-key"])
    assert rc == 0
    # No grants requested → None (stored as the empty-array default).
    assert captured["scopes"] is None


# --------------------------------------------------------------------------
# Which endpoints sit on which side of the licence boundary
# --------------------------------------------------------------------------
#
# The taxonomy tests above pin the SETS; these pin the WIRING, which is where
# the 2026-08-31 incident actually lived. Every scope declaration was correct
# in scopes.py and the bundles were right — the endpoints were simply hung off
# the wrong one, so switching enforcement on 403'd eleven paying integrations,
# nine of which only ever wanted the underlying's price. A set-level test
# cannot catch that; only asserting the routes can.


def _dependency_scopes(app, path: str) -> set:
    """The scopes required by the route serving ``path``.

    Reaches through the ``require_scopes`` closure rather than re-deriving
    them, so the assertion is about what FastAPI will actually enforce.
    """
    for route in app.routes:
        if getattr(route, "path", None) != path:
            continue
        found = set()
        for dep in getattr(route, "dependencies", []):
            call = getattr(dep, "dependency", None)
            closure = getattr(call, "__closure__", None) or ()
            for cell in closure:
                if isinstance(cell.cell_contents, set):
                    found |= cell.cell_contents
        return found
    raise AssertionError(f"no route serving {path}")


@pytest.mark.parametrize(
    "path",
    [
        "/api/market/quote",
        "/api/market/historical",
        "/api/market/session-closes",
        "/api/market/session-levels",
    ],
)
def test_underlying_tape_is_reference_not_raw(path):
    """A level is meaningless without the price it sits against, so the
    underlying's own tape must reach an analytics-tier key."""
    from src.api.main import app

    required = _dependency_scopes(app, path)
    assert scopes.MARKET_REFERENCE in required, path
    assert scopes.MARKET_RAW not in required, path
    assert required <= scopes.TIERS[scopes.TIER_ANALYTICS], path


@pytest.mark.parametrize(
    "path",
    [
        "/api/option/quote",
    ],
)
def test_per_contract_surfaces_stay_raw(path):
    """A per-contract QUOTED PRICE must not be reachable from a customer
    bundle.

    ``/api/market/open-interest`` used to be listed here, on the older
    theory that enumerating the chain was itself the concern. It returns no
    quote — ``open_interest`` and a derived ``exposure`` — and the same
    per-strike OI already ships on GEX via ``/api/gex/by-strike``, so the
    gate withheld nothing while 403ing paying integrations. It moved to GEX
    on 2026-09-03; ``tests/test_market_data_scope_boundary.py`` now pins
    both halves of that boundary against the mounted route table. See the
    ``scopes.py`` docstring for the question that is still open.
    """
    from src.api.main import app

    required = _dependency_scopes(app, path)
    assert scopes.MARKET_RAW in required, path
    assert not required <= scopes.TIERS[scopes.TIER_SIGNALS], path
