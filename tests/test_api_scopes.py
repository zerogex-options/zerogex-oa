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
    """The clean B2B/B2B2C product: derived analytics, no raw, no signals."""
    bundle = scopes.TIERS[scopes.TIER_ANALYTICS]
    assert scopes.MARKET_RAW not in bundle
    assert scopes.SIGNALS not in bundle
    assert bundle == {scopes.GEX, scopes.FLOW, scopes.MAXPAIN, scopes.TECHNICALS}


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
