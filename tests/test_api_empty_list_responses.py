"""W4.4: list-returning endpoints must return 200 + [] when there's no data.

Previously these endpoints raised HTTPException(404, "No <foo> data available")
when the underlying query returned an empty list. That made frontends crash
with a 404 in legitimate "market just opened, no data yet" scenarios. The
new contract — already used by /api/flow/series — is:

    - 404 means "unknown identifier" (e.g. symbol not in symbols table)
    - 200 + []  means "request was valid, there just isn't any data"

Single-object endpoints (e.g. /api/gex/summary, /api/option/quote) keep
returning 404 on empty since there's no natural "empty" representation
for a single record. Those are intentionally out of scope here.
"""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient


def _build_app_with_mocked_method(monkeypatch: pytest.MonkeyPatch, method_name: str, returns):
    """Reload src.api.main with the named DB method patched at class level.

    Mirrors the pattern from test_api_auth_and_cors.py: patch the class
    *before* importing main so the module-level db_manager picks up the
    mock. Patching the instance after import doesn't propagate reliably
    because handler attribute lookup hits the class first when the
    instance attribute hasn't been bound by __init__.
    """
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)
    setattr(dbmod.DatabaseManager, method_name, AsyncMock(return_value=returns))

    from src.api.main import app

    return app


# ---------------------------------------------------------------------------
# Each parametrize tuple = (URL path, db_manager method name, expected_shape)
#
# expected_shape: "list" — empty list returns 200 + []
# ---------------------------------------------------------------------------
LIST_ENDPOINTS = [
    ("/api/gex/by-strike?symbol=SPY", "get_gex_by_strike"),
    ("/api/gex/historical?symbol=SPY", "get_historical_gex"),
    ("/api/market/historical?symbol=SPY", "get_historical_quotes"),
    ("/api/max-pain/timeseries?symbol=SPY", "get_max_pain_timeseries"),
    ("/api/technicals/vwap-deviation?symbol=SPY", "get_vwap_deviation"),
    ("/api/technicals/opening-range?symbol=SPY", "get_opening_range_breakout"),
    ("/api/technicals/dealer-hedging?symbol=SPY", "get_dealer_hedging_pressure"),
    ("/api/technicals/volume-spikes?symbol=SPY", "get_unusual_volume_spikes"),
    ("/api/technicals/momentum-divergence?symbol=SPY", "get_momentum_divergence"),
]


@pytest.mark.parametrize("path,method_name", LIST_ENDPOINTS)
def test_list_endpoint_empty_returns_200_with_empty_list(
    monkeypatch: pytest.MonkeyPatch, path: str, method_name: str
):
    app = _build_app_with_mocked_method(monkeypatch, method_name, returns=[])

    with TestClient(app) as client:
        response = client.get(path)

    assert response.status_code == 200, (path, response.text)
    assert response.json() == [], path


def test_single_object_endpoint_empty_still_returns_404(monkeypatch: pytest.MonkeyPatch):
    """Regression guard: /api/gex/summary is single-object and stays 404 on empty.

    The W4.4 standardization was deliberately scoped to list endpoints; flipping
    single-object endpoints to 200+null would silently change client semantics.
    """
    app = _build_app_with_mocked_method(monkeypatch, "get_latest_gex_summary", returns=None)

    with TestClient(app) as client:
        response = client.get("/api/gex/summary?symbol=SPY")

    assert response.status_code == 404
