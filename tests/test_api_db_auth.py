"""Tests for the DB-backed per-user API key path.

These cover the behavior added in ``src/api/security.py`` on top of the
existing static-key tests in ``test_api_auth_and_cors.py``.  The DB pool
is mocked: each request that reaches ``key_store.lookup()`` is routed to
a stubbed ``fetchrow`` so we don't need a live PostgreSQL.
"""

from __future__ import annotations

import sys
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
from fastapi.testclient import TestClient


def _reload_app(monkeypatch: pytest.MonkeyPatch, *, api_key: Optional[str] = None):
    """Reload src.api.main with a clean security module.

    The real lifespan calls ``key_store.configure(lambda: db_manager.pool)``
    at startup; in tests we don't want it clobbering the fake pool we install
    per-test, so we replace ``key_store.configure`` with a no-op for the
    duration of the test.  Tests then use ``_install_pool`` below to plug in
    their stub, which assigns a getter to ``security.key_store._get_pool``.
    """
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    if api_key is not None:
        monkeypatch.setenv("API_KEY", api_key)
    monkeypatch.setenv("ENVIRONMENT", "development")

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod  # noqa: E402

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)

    from src.api.main import app  # noqa: E402
    from src.api import security  # noqa: E402

    # Neutralize the lifespan's configure() so it can't overwrite our pool.
    monkeypatch.setattr(security.key_store, "configure", lambda get_pool: None)

    return app, security


def _install_pool(security_module, pool: Optional[Any]) -> None:
    """Plug a fake pool directly into the key store, bypassing configure().

    Passing ``pool=None`` leaves the key store disabled (``is_enabled() ==
    False``), matching the production "no DB pool registered" state.
    """
    if pool is None:
        security_module.key_store._get_pool = None
    else:
        security_module.key_store._get_pool = lambda: pool
    security_module.key_store._cache.clear()
    security_module.key_store._last_touch.clear()
    security_module.key_store._touch_tasks.clear()


class _FakePool:
    """Minimal asyncpg.Pool stand-in: ``async with pool.acquire() as conn``."""

    def __init__(self, fetchrow_result: Optional[Dict[str, Any]]):
        self._row = fetchrow_result
        self.fetchrow_calls: list = []
        self.execute_calls: list = []

    def acquire(self):
        outer = self

        class _CM:
            async def __aenter__(self):
                conn = MagicMock()

                async def _fetchrow(query, *args):
                    outer.fetchrow_calls.append((query, args))
                    return outer._row

                async def _execute(query, *args):
                    outer.execute_calls.append((query, args))
                    return "UPDATE 1"

                conn.fetchrow = _fetchrow
                conn.execute = _execute
                return conn

            async def __aexit__(self, *exc):
                return False

        return _CM()


def test_db_key_authorizes_request(monkeypatch: pytest.MonkeyPatch):
    app, security = _reload_app(monkeypatch, api_key=None)
    raw_key = "live-user-key-abc"
    key_hash = security._hash_key(raw_key)
    pool = _FakePool({"id": 1, "user_id": "alice", "name": "alice-laptop", "scopes": []})
    _install_pool(security, pool)

    with TestClient(app) as client:
        response = client.get("/api/health", headers={"X-API-Key": raw_key})

    assert response.status_code == 200, response.text
    # The lookup hashes the key before querying — verify we never sent the raw secret.
    assert pool.fetchrow_calls, "expected a DB lookup"
    _query, args = pool.fetchrow_calls[0]
    assert args[0] == key_hash
    assert raw_key not in args


def test_db_key_via_bearer_header(monkeypatch: pytest.MonkeyPatch):
    app, security = _reload_app(monkeypatch, api_key=None)
    pool = _FakePool({"id": 2, "user_id": "bob", "name": "bob-ci", "scopes": []})
    _install_pool(security, pool)

    with TestClient(app) as client:
        response = client.get(
            "/api/health",
            headers={"Authorization": "Bearer some-bob-key"},
        )
    assert response.status_code == 200, response.text


def test_db_unknown_key_rejected(monkeypatch: pytest.MonkeyPatch):
    app, security = _reload_app(monkeypatch, api_key=None)
    pool = _FakePool(None)
    _install_pool(security, pool)

    with TestClient(app) as client:
        response = client.get("/api/health", headers={"X-API-Key": "no-such-key"})
    assert response.status_code == 401
    assert response.headers.get("WWW-Authenticate") == "Bearer"


def test_db_keyless_request_rejected(monkeypatch: pytest.MonkeyPatch):
    """When the DB pool is configured, missing credentials means 401 — even
    if API_KEY env var is unset.  This is the production happy path."""
    app, security = _reload_app(monkeypatch, api_key=None)
    pool = _FakePool(None)
    _install_pool(security, pool)

    with TestClient(app) as client:
        response = client.get("/api/health")
    assert response.status_code == 401


def test_static_key_still_works_alongside_db(monkeypatch: pytest.MonkeyPatch):
    """Static API_KEY env var continues to authorize requests even when DB
    auth is enabled — the two mechanisms coexist for ops/legacy callers."""
    app, security = _reload_app(monkeypatch, api_key="legacy-shared")
    pool = _FakePool(None)  # DB has no matching keys
    _install_pool(security, pool)

    with TestClient(app) as client:
        response = client.get("/api/health", headers={"X-API-Key": "legacy-shared"})
    assert response.status_code == 200, response.text


def test_bearer_wins_when_xapikey_is_nginx_injected_static(monkeypatch: pytest.MonkeyPatch):
    """Migration scenario: nginx injects X-API-Key=<static> regardless of what
    the caller sent. Caller's per-user key arrives in Authorization: Bearer.
    Bearer must win so the caller's key authenticates and last_used_at ticks
    — otherwise the static path short-circuits and every migrating caller
    appears anonymous."""
    app, security = _reload_app(monkeypatch, api_key="nginx-injected-static")
    alice_row = {"id": 42, "user_id": "alice", "name": "alice-prod", "scopes": []}
    pool = _FakePool(alice_row)
    _install_pool(security, pool)

    with TestClient(app) as client:
        response = client.get(
            "/api/health",
            headers={
                "X-API-Key": "nginx-injected-static",  # nginx clobbering layer
                "Authorization": "Bearer alice-raw-key",  # caller's actual key
            },
        )
    assert response.status_code == 200, response.text
    # Critical: the DB lookup must have used the Bearer value, not the static.
    assert pool.fetchrow_calls, "Bearer credential must trigger DB lookup"
    _query, args = pool.fetchrow_calls[0]
    assert args[0] == security._hash_key("alice-raw-key")


def test_db_lookup_failure_returns_401_not_500(monkeypatch: pytest.MonkeyPatch):
    """A DB outage during lookup must surface as 401 (key invalid), not 500."""
    app, security = _reload_app(monkeypatch, api_key=None)

    class _BrokenPool:
        def acquire(self):
            class _CM:
                async def __aenter__(self_inner):
                    raise RuntimeError("db down")

                async def __aexit__(self_inner, *exc):
                    return False

            return _CM()

    _install_pool(security, _BrokenPool())

    with TestClient(app) as client:
        response = client.get("/api/health", headers={"X-API-Key": "anything"})
    assert response.status_code == 401


def test_lookup_cache_hits_avoid_repeat_db_calls(monkeypatch: pytest.MonkeyPatch):
    """Repeated requests with the same key must hit the in-memory cache."""
    app, security = _reload_app(monkeypatch, api_key=None)
    pool = _FakePool({"id": 5, "user_id": "carol", "name": "carol", "scopes": []})
    _install_pool(security, pool)

    with TestClient(app) as client:
        for _ in range(3):
            r = client.get("/api/health", headers={"X-API-Key": "carol-key"})
            assert r.status_code == 200, r.text

    # Exactly one DB lookup despite three requests.
    assert len(pool.fetchrow_calls) == 1


def test_disabled_when_neither_static_nor_db_configured(monkeypatch: pytest.MonkeyPatch):
    """Dev mode: API_KEY unset AND no DB pool registered ⇒ open access."""
    app, security = _reload_app(monkeypatch, api_key=None)
    _install_pool(security, None)

    with TestClient(app) as client:
        response = client.get("/api/health")
    assert response.status_code == 200, response.text


def test_lookup_picks_up_reconnected_pool(monkeypatch: pytest.MonkeyPatch):
    """After DatabaseManager._reconnect_pool swaps in a fresh pool (and
    closes the old one), the next key_store lookup must use the new pool
    instead of holding a stale, closed reference.

    Regression for the 2026-05-11 prod incident: a transient DB acquire
    error triggered _reconnect_pool, but pre-fix _KeyStore cached the
    pool reference at configure-time and silently 401'd every request
    until the service was restarted.
    """
    app, security = _reload_app(monkeypatch, api_key=None)

    class _ClosedPool:
        """Simulates the old pool after _reconnect_pool tears it down."""

        def acquire(self):
            class _CM:
                async def __aenter__(self_inner):
                    raise asyncpg.exceptions.InterfaceError("pool is closed")

                async def __aexit__(self_inner, *exc):
                    return False

            return _CM()

    pool_new = _FakePool({"id": 9, "user_id": "eve", "name": "eve-laptop", "scopes": []})

    # Install a getter that returns the current pool — mirrors the
    # lifespan's `lambda: db_manager.pool`.
    current = {"pool": _ClosedPool()}
    security.key_store._get_pool = lambda: current["pool"]
    security.key_store._cache.clear()
    security.key_store._last_touch.clear()

    with TestClient(app) as client:
        # First request lands on the closed pool — must 401, not 500.
        r1 = client.get("/api/health", headers={"X-API-Key": "eve-key"})
        assert r1.status_code == 401

        # Simulate _reconnect_pool replacing db_manager.pool.
        current["pool"] = pool_new

        # Same raw key. The exception path doesn't populate the cache,
        # so the second lookup re-consults the getter and reaches pool_new.
        r2 = client.get("/api/health", headers={"X-API-Key": "eve-key"})
        assert r2.status_code == 200, r2.text

    # The new pool — not the closed one — served the second lookup.
    assert len(pool_new.fetchrow_calls) == 1
