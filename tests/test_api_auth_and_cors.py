"""End-to-end API authentication and CORS tests using FastAPI's TestClient.

These tests exercise the full request → middleware → router path, which
is where the production 401 from the same-host frontend was hiding:
nothing in the unit tests hit the auth dependency chain until now.

The DatabaseManager is mocked at the module level so the tests don't
need a running PostgreSQL — we're testing the HTTP surface, not the
queries.
"""

from __future__ import annotations

import importlib
import sys
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient


def _build_app(
    monkeypatch: pytest.MonkeyPatch,
    *,
    api_key: str | None,
    environment: str = "development",
    cors_origins: str | None = None,
):
    """Reload src.api.main with the requested env vars in place.

    API_KEY and ENVIRONMENT are read at module-import time in
    ``src.api.security``, so we have to flush and re-import every
    subtree that depends on them.
    """
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    if api_key is not None:
        monkeypatch.setenv("API_KEY", api_key)
    monkeypatch.setenv("ENVIRONMENT", environment)
    if cors_origins is not None:
        monkeypatch.setenv("CORS_ALLOW_ORIGINS", cors_origins)

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    # Stub out the async DB connection so lifespan startup doesn't need
    # a live Postgres.
    from src.api import database as dbmod  # noqa: E402
    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)

    from src.api.main import app  # noqa: E402
    return app


# --------------------------------------------------------------------------
# Auth dependency
# --------------------------------------------------------------------------

def test_api_without_key_allows_unauth_when_API_KEY_unset(monkeypatch: pytest.MonkeyPatch):
    """Dev mode: API_KEY unset ⇒ every request succeeds without auth."""
    app = _build_app(monkeypatch, api_key=None)
    with TestClient(app) as client:
        response = client.get("/api/health")
    assert response.status_code == 200, response.text


def test_api_with_key_rejects_keyless_request(monkeypatch: pytest.MonkeyPatch):
    """Prod pattern: API_KEY set ⇒ keyless request returns 401."""
    app = _build_app(monkeypatch, api_key="s3cret-test-key")
    with TestClient(app) as client:
        response = client.get("/api/health")
    assert response.status_code == 401
    assert response.headers.get("WWW-Authenticate") == "Bearer"
    assert "invalid" in response.json()["detail"].lower()


def test_api_accepts_x_api_key_header(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch, api_key="s3cret-test-key")
    with TestClient(app) as client:
        response = client.get("/api/health", headers={"X-API-Key": "s3cret-test-key"})
    assert response.status_code == 200, response.text


def test_api_accepts_authorization_bearer_header(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch, api_key="s3cret-test-key")
    with TestClient(app) as client:
        response = client.get(
            "/api/health",
            headers={"Authorization": "Bearer s3cret-test-key"},
        )
    assert response.status_code == 200, response.text


def test_api_rejects_wrong_key(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch, api_key="s3cret-test-key")
    with TestClient(app) as client:
        response = client.get("/api/health", headers={"X-API-Key": "wrong"})
    assert response.status_code == 401


def test_api_rejects_bearer_with_wrong_key(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch, api_key="s3cret-test-key")
    with TestClient(app) as client:
        response = client.get(
            "/api/health",
            headers={"Authorization": "Bearer wrong-key"},
        )
    assert response.status_code == 401


def test_api_auth_is_timing_safe(monkeypatch: pytest.MonkeyPatch):
    """Sanity-check that short & long invalid keys both return 401 with
    the same shape — proxy for ``hmac.compare_digest`` being used."""
    app = _build_app(monkeypatch, api_key="s3cret-test-key")
    with TestClient(app) as client:
        short = client.get("/api/health", headers={"X-API-Key": "a"})
        long_ = client.get("/api/health", headers={"X-API-Key": "a" * 1024})
    assert short.status_code == 401
    assert long_.status_code == 401
    assert short.json() == long_.json()


# --------------------------------------------------------------------------
# CORS production guard
# --------------------------------------------------------------------------

def test_cors_production_refuses_wildcard(monkeypatch: pytest.MonkeyPatch):
    """ENVIRONMENT=production + empty CORS ⇒ RuntimeError at app creation."""
    with pytest.raises(RuntimeError, match="CORS_ALLOW_ORIGINS"):
        _build_app(monkeypatch, api_key=None, environment="production", cors_origins="")


def test_cors_production_refuses_explicit_wildcard(monkeypatch: pytest.MonkeyPatch):
    """ENVIRONMENT=production with CORS_ALLOW_ORIGINS='*' also refuses."""
    with pytest.raises(RuntimeError, match="wildcard"):
        _build_app(monkeypatch, api_key=None, environment="production", cors_origins="*")


def test_cors_production_accepts_explicit_origins(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(
        monkeypatch,
        api_key="k",
        environment="production",
        cors_origins="https://app.example.com,https://admin.example.com",
    )
    with TestClient(app) as client:
        response = client.get(
            "/api/health",
            headers={"X-API-Key": "k", "Origin": "https://app.example.com"},
        )
    assert response.status_code == 200
    # Echoes back the matching origin — not '*', and allow_credentials is on.
    assert response.headers.get("access-control-allow-origin") == "https://app.example.com"


def test_cors_development_allows_wildcard_by_default(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch, api_key=None, environment="development", cors_origins="")
    with TestClient(app) as client:
        response = client.get("/api/health")
    assert response.status_code == 200


# --------------------------------------------------------------------------
# Error-handling decorator
# --------------------------------------------------------------------------

def test_500_response_does_not_leak_exception_details(monkeypatch: pytest.MonkeyPatch):
    """Any exception raised inside a @handle_api_errors route must surface
    to the client as a generic 500 — never the original exception message."""
    app = _build_app(monkeypatch, api_key=None)

    from src.api import database as dbmod
    dbmod.DatabaseManager.get_latest_gex_summary = AsyncMock(
        side_effect=RuntimeError("database on fire: secret_user_id=42"),
    )

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/api/gex/summary?symbol=SPY")
    assert response.status_code == 500
    detail = response.json().get("detail", "")
    assert detail == "Internal server error"
    assert "secret_user_id" not in detail
    assert "database on fire" not in detail


def test_404_from_empty_result_passes_through(monkeypatch: pytest.MonkeyPatch):
    """HTTPException from the route (404 "no data") must NOT be wrapped
    into 500 by the decorator."""
    app = _build_app(monkeypatch, api_key=None)
    from src.api import database as dbmod
    dbmod.DatabaseManager.get_latest_gex_summary = AsyncMock(return_value=None)

    with TestClient(app) as client:
        response = client.get("/api/gex/summary?symbol=SPY")
    assert response.status_code == 404
    assert "no gex" in response.json()["detail"].lower()
