"""Tests for the admin key-provisioning endpoints (/api/admin/api-keys).

The DB pool is faked with a small in-memory ``api_keys`` table so we can
assert the real behaviour — name incrementing, revoke-then-create
("regenerate"), single active key per user, and the raw secret being
returned exactly once — without a live PostgreSQL. The admin gate
(``X-Admin-Token`` vs ``KEY_ADMIN_TOKEN``) is exercised directly.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock

ADMIN_TOKEN = "test-admin-secret"
_FIXED_TS = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


def _reload_app(monkeypatch: pytest.MonkeyPatch, *, key_admin_token: Optional[str] = ADMIN_TOKEN):
    """Reload src.api.main with a clean, DB-mocked security module.

    ``key_admin_token`` seeds ``KEY_ADMIN_TOKEN`` before import so
    ``security.require_admin`` picks it up (pass ``None`` to test the
    fail-closed path). API_KEY is left unset and no key_store pool is
    registered, so the global ``api_key_auth`` runs in dev "allow" mode and
    the admin token is the only gate under test.
    """
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS", "KEY_ADMIN_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    if key_admin_token is not None:
        monkeypatch.setenv("KEY_ADMIN_TOKEN", key_admin_token)

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod  # noqa: E402

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)

    from src.api.main import app  # noqa: E402
    from src.api import security  # noqa: E402
    from src.api.routers import admin_api_keys  # noqa: E402

    # Don't let the lifespan wire a real pool getter into the key store.
    monkeypatch.setattr(security.key_store, "configure", lambda get_pool: None)

    return app, security, admin_api_keys


class _FakeConn:
    """In-memory api_keys table sufficient for the provisioning SQL."""

    def __init__(self, store: Dict[str, Any]):
        self.store = store

    @staticmethod
    def _norm(query: str) -> str:
        return " ".join(query.split())

    def transaction(self):
        class _T:
            async def __aenter__(self_inner):
                return None

            async def __aexit__(self_inner, *exc):
                return False

        return _T()

    async def execute(self, query: str, *args: Any) -> str:
        q = self._norm(query)
        if q.startswith("UPDATE api_keys SET revoked_at = NOW() WHERE user_id ="):
            (user_id,) = args
            n = 0
            for r in self.store["rows"]:
                if r["user_id"] == user_id and r["revoked_at"] is None:
                    r["revoked_at"] = _FIXED_TS
                    n += 1
            return f"UPDATE {n}"
        raise AssertionError(f"unexpected execute: {q}")

    async def fetch(self, query: str, *args: Any) -> List[Dict[str, Any]]:
        q = self._norm(query)
        if q.startswith("SELECT name FROM api_keys WHERE user_id ="):
            (user_id,) = args
            return [{"name": r["name"]} for r in self.store["rows"] if r["user_id"] == user_id]
        if q.startswith("SELECT id, user_id, name, prefix, scopes"):
            (user_id,) = args
            active_only = "revoked_at IS NULL" in q
            out = []
            for r in self.store["rows"]:
                if r["user_id"] != user_id:
                    continue
                if active_only and r["revoked_at"] is not None:
                    continue
                out.append(r)
            return out
        raise AssertionError(f"unexpected fetch: {q}")

    async def fetchrow(self, query: str, *args: Any) -> Dict[str, Any]:
        q = self._norm(query)
        if q.startswith("INSERT INTO api_keys"):
            user_id, name, key_hash, prefix, scopes = args
            self.store["seq"] += 1
            row = {
                "id": self.store["seq"],
                "user_id": user_id,
                "name": name,
                "key_hash": key_hash,
                "prefix": prefix,
                "scopes": list(scopes),
                "created_at": _FIXED_TS,
                "last_used_at": None,
                "revoked_at": None,
            }
            self.store["rows"].append(row)
            return {"id": row["id"], "created_at": row["created_at"]}
        raise AssertionError(f"unexpected fetchrow: {q}")


class _FakePool:
    def __init__(self, store: Dict[str, Any]):
        self.store = store

    def acquire(self):
        store = self.store

        class _CM:
            async def __aenter__(self_inner):
                return _FakeConn(store)

            async def __aexit__(self_inner, *exc):
                return False

        return _CM()


class _FakeDB:
    def __init__(self, store: Dict[str, Any]):
        self.pool = _FakePool(store)


def _client(app, admin_api_keys, store: Dict[str, Any]) -> TestClient:
    app.dependency_overrides[admin_api_keys.get_db] = lambda: _FakeDB(store)
    return TestClient(app)


def _fresh_store() -> Dict[str, Any]:
    return {"rows": [], "seq": 0}


def _hdr(token: Optional[str] = ADMIN_TOKEN) -> Dict[str, str]:
    return {"X-Admin-Token": token} if token is not None else {}


def test_provision_first_key_uses_email_local_part(monkeypatch):
    app, _security, admin_api_keys = _reload_app(monkeypatch)
    store = _fresh_store()
    with _client(app, admin_api_keys, store) as client:
        resp = client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr(),
            json={"user_id": "alice@example.com", "base_name": "alice"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["name"] == "alice"
    assert body["revoked_previous"] == 0
    assert body["prefix"] == body["api_key"][:8]
    assert len(body["api_key"]) >= 40  # token_urlsafe(32) ~ 43 chars
    # Exactly one active key persisted, hashed (never the raw secret).
    assert len(store["rows"]) == 1
    assert store["rows"][0]["key_hash"] != body["api_key"]


def test_regenerate_revokes_and_increments_name(monkeypatch):
    app, _security, admin_api_keys = _reload_app(monkeypatch)
    store = _fresh_store()
    with _client(app, admin_api_keys, store) as client:
        first = client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr(),
            json={"user_id": "bob@example.com", "base_name": "bob"},
        ).json()
        second = client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr(),
            json={"user_id": "bob@example.com", "base_name": "bob"},
        ).json()
        third = client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr(),
            json={"user_id": "bob@example.com", "base_name": "bob"},
        ).json()

    assert first["name"] == "bob"
    assert second["name"] == "bob1"
    assert third["name"] == "bob2"
    # Each regenerate deprecated exactly one prior key.
    assert second["revoked_previous"] == 1
    assert third["revoked_previous"] == 1
    # New raw secret each time.
    assert len({first["api_key"], second["api_key"], third["api_key"]}) == 3
    # Only the newest key is active; the earlier two are revoked.
    active = [r for r in store["rows"] if r["revoked_at"] is None]
    assert len(active) == 1 and active[0]["name"] == "bob2"


def test_list_active_only_returns_metadata_without_secret(monkeypatch):
    app, _security, admin_api_keys = _reload_app(monkeypatch)
    store = _fresh_store()
    with _client(app, admin_api_keys, store) as client:
        client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr(),
            json={"user_id": "carol@example.com", "base_name": "carol"},
        )
        client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr(),
            json={"user_id": "carol@example.com", "base_name": "carol"},
        )
        resp = client.get(
            "/api/admin/api-keys",
            headers=_hdr(),
            params={"user_id": "carol@example.com", "active_only": "true"},
        )
    assert resp.status_code == 200, resp.text
    keys = resp.json()["keys"]
    assert len(keys) == 1  # only the active one
    k = keys[0]
    assert set(k) == {
        "id",
        "user_id",
        "name",
        "prefix",
        "scopes",
        "created_at",
        "last_used_at",
        "revoked_at",
    }
    assert "api_key" not in k and "key_hash" not in k


def test_revoke_all_deprovisions_and_is_idempotent(monkeypatch):
    app, _security, admin_api_keys = _reload_app(monkeypatch)
    store = _fresh_store()
    with _client(app, admin_api_keys, store) as client:
        client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr(),
            json={"user_id": "dave@example.com", "base_name": "dave"},
        )
        first = client.post(
            "/api/admin/api-keys/revoke-all",
            headers=_hdr(),
            json={"user_id": "dave@example.com"},
        ).json()
        second = client.post(
            "/api/admin/api-keys/revoke-all",
            headers=_hdr(),
            json={"user_id": "dave@example.com"},
        ).json()
    assert first["revoked"] == 1
    assert second["revoked"] == 0  # nothing left to revoke
    assert all(r["revoked_at"] is not None for r in store["rows"])


def test_provision_with_tier_expands_scopes(monkeypatch):
    app, _security, admin_api_keys = _reload_app(monkeypatch)
    store = _fresh_store()
    with _client(app, admin_api_keys, store) as client:
        resp = client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr(),
            json={"user_id": "eve@example.com", "base_name": "eve", "tier": "signals"},
        )
    assert resp.status_code == 200, resp.text
    scopes = resp.json()["scopes"]
    # The signals tier bundle (derived analytics + signals, no market_raw).
    assert set(scopes) == {"gex", "flow", "maxpain", "technicals", "signals"}
    assert "market_raw" not in scopes


def test_provision_unknown_tier_is_400(monkeypatch):
    app, _security, admin_api_keys = _reload_app(monkeypatch)
    store = _fresh_store()
    with _client(app, admin_api_keys, store) as client:
        resp = client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr(),
            json={"user_id": "eve@example.com", "base_name": "eve", "tier": "nope"},
        )
    assert resp.status_code == 400, resp.text


def test_missing_admin_token_is_403(monkeypatch):
    app, _security, admin_api_keys = _reload_app(monkeypatch)
    store = _fresh_store()
    with _client(app, admin_api_keys, store) as client:
        resp = client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr(None),  # no X-Admin-Token
            json={"user_id": "alice@example.com", "base_name": "alice"},
        )
    assert resp.status_code == 403, resp.text
    assert store["rows"] == []  # nothing minted


def test_wrong_admin_token_is_403(monkeypatch):
    app, _security, admin_api_keys = _reload_app(monkeypatch)
    store = _fresh_store()
    with _client(app, admin_api_keys, store) as client:
        resp = client.post(
            "/api/admin/api-keys/provision",
            headers=_hdr("not-the-token"),
            json={"user_id": "alice@example.com", "base_name": "alice"},
        )
    assert resp.status_code == 403, resp.text


def test_unconfigured_admin_token_fails_closed_503(monkeypatch):
    app, _security, admin_api_keys = _reload_app(monkeypatch, key_admin_token=None)
    store = _fresh_store()
    with _client(app, admin_api_keys, store) as client:
        resp = client.post(
            "/api/admin/api-keys/provision",
            headers={"X-Admin-Token": "anything"},
            json={"user_id": "alice@example.com", "base_name": "alice"},
        )
    assert resp.status_code == 503, resp.text
