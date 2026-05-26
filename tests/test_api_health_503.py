"""``/api/health`` must return HTTP 503 when the database is unreachable.

The prior implementation returned HTTP 200 with ``status="degraded"`` —
which every standard uptime monitor, load balancer, and Kubernetes probe
treated as healthy. A degraded backend then sat unactioned indefinitely.

Probes consume the status code; only the status code reflects "this
worker is fit to take traffic."
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient


def _build_app(monkeypatch: pytest.MonkeyPatch, *, check_health_returns: bool):
    """Construct the FastAPI app with ``check_health`` stubbed to a fixed bool."""
    import sys

    # Drop any cached api module so the lifespan re-binds against our mocks.
    for mod_name in list(sys.modules):
        if mod_name.startswith("src.api"):
            sys.modules.pop(mod_name, None)

    # Stub auth so /api/health is publicly accessible regardless of API_KEY.
    monkeypatch.delenv("API_KEY", raising=False)

    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=check_health_returns)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)

    from src.api.main import app

    return app


def test_health_returns_200_when_db_healthy(monkeypatch):
    app = _build_app(monkeypatch, check_health_returns=True)
    with TestClient(app) as client:
        response = client.get("/api/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["database_connected"] is True


def test_health_returns_503_when_db_unreachable(monkeypatch):
    """The whole point of this test file: degraded backend ⇒ 503."""
    app = _build_app(monkeypatch, check_health_returns=False)
    with TestClient(app) as client:
        response = client.get("/api/health")
    assert response.status_code == 503
    body = response.json()
    # Body still carries the structured status so callers/dashboards
    # that DO read the body see the same diagnostics they always did.
    assert body["status"] == "degraded"
    assert body["database_connected"] is False
