"""API tests for /api/instruments/* and /api/futures/* (no live Postgres)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient


def _build_app(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("API_KEY", "")
    monkeypatch.setenv("ENVIRONMENT", "development")
    for mod in list(sys.modules):
        if mod.startswith("src.api") or mod.startswith("src.signals.playbook"):
            sys.modules.pop(mod, None)
    from src.api import database as dbmod  # noqa: E402

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    from src.api.main import app  # noqa: E402

    return app, dbmod


def _inject_db(**methods):
    """Inject a mock db_manager directly into src.api.main.

    Avoids running the app lifespan (which would try to open real Postgres
    connections for the QuoteBroadcaster and overwrite db_manager), keeping
    the DB-backed endpoint tests deterministic and offline.  Returns the mock.
    """
    import src.api.main as main_mod

    mock_db = MagicMock()
    for name, value in methods.items():
        setattr(mock_db, name, AsyncMock(return_value=value))
    main_mod.db_manager = mock_db
    return mock_db


# -- /api/instruments -------------------------------------------------------


def test_list_instruments_defaults_futures_off(monkeypatch):
    app, _ = _build_app(monkeypatch)
    client = TestClient(app)
    r = client.get("/api/instruments")
    assert r.status_code == 200
    body = r.json()
    syms = {i["symbol"] for i in body["instruments"]}
    assert {"SPY", "SPX", "QQQ", "NDX", "ES", "NQ"} <= syms
    # Equities enabled, futures not (flags off).
    assert body["enabled_symbols"] == ["SPY", "SPX", "QQQ", "NDX"]
    es = next(i for i in body["instruments"] if i["symbol"] == "ES")
    assert es["asset_class"] == "future"
    assert es["contract_multiplier"] == 50
    assert es["analytics_source"] == "cme_futures_options"
    assert es["availability"]["available"] is False
    assert es["capabilities"]["gex"] is False


def test_instrument_detail_es_shows_contracts_and_limitations(monkeypatch):
    app, _ = _build_app(monkeypatch)
    client = TestClient(app)
    r = client.get("/api/instruments/ES")
    assert r.status_code == 200
    body = r.json()
    assert body["symbol"] == "ES"
    assert body["contracts"]["front_contract"]["contract_id"].startswith("ES")
    assert body["contracts"]["contract_source"] == "cme_quarterly_listing_schedule"
    assert any("not enabled" in x for x in body["known_limitations"])


def test_instrument_detail_nq_reference_limitation(monkeypatch):
    app, _ = _build_app(monkeypatch)
    client = TestClient(app)
    body = client.get("/api/instruments/NQ").json()
    assert any("naive QQQ" in x for x in body["known_limitations"])


def test_unknown_instrument_404(monkeypatch):
    app, _ = _build_app(monkeypatch)
    client = TestClient(app)
    assert client.get("/api/instruments/TSLA").status_code == 404


def test_es_becomes_enabled_with_flags(monkeypatch):
    monkeypatch.setenv("ENABLE_CME_INGESTION", "true")
    monkeypatch.setenv("ENABLE_ES_ANALYTICS", "true")
    app, _ = _build_app(monkeypatch)
    client = TestClient(app)
    body = client.get("/api/instruments").json()
    assert "ES" in body["enabled_symbols"]
    es = next(i for i in body["instruments"] if i["symbol"] == "ES")
    assert es["availability"]["true_analytics"] is True
    assert es["capabilities"]["gex"] is True


# -- /api/futures/{product}/contracts + roll-state --------------------------


def test_contracts_endpoint_returns_schedule(monkeypatch):
    app, _ = _build_app(monkeypatch)
    client = TestClient(app)
    r = client.get("/api/futures/ES/contracts")
    assert r.status_code == 200
    body = r.json()
    assert body["product"] == "ES"
    assert body["contract_source"] == "cme_quarterly_listing_schedule"
    assert len(body["contracts"]) == 4
    # No fabricated market data.
    assert all(c["volume"] is None and c["open_interest"] is None for c in body["contracts"])
    assert all(c["multiplier"] == 50 for c in body["contracts"])


def test_roll_state_endpoint(monkeypatch):
    app, _ = _build_app(monkeypatch)
    client = TestClient(app)
    r = client.get("/api/futures/ES/roll-state")
    assert r.status_code == 200
    body = r.json()
    assert body["product"] == "ES"
    assert body["policy"] == "hybrid"
    assert body["front_contract"]["contract_id"].startswith("ES")
    assert "recommended_display_contract" in body


def test_contracts_unknown_product_404(monkeypatch):
    app, _ = _build_app(monkeypatch)
    client = TestClient(app)
    assert client.get("/api/futures/SPY/contracts").status_code == 404


# -- /api/futures/reference-levels ------------------------------------------


def test_reference_levels_disabled_returns_409(monkeypatch):
    app, _ = _build_app(monkeypatch)
    client = TestClient(app)
    r = client.get("/api/futures/reference-levels?source=SPX&target=ES")
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "reference_mode_unavailable"


def test_reference_levels_nq_disabled(monkeypatch):
    monkeypatch.setenv("ENABLE_NQ_REFERENCE_MODE", "true")
    app, _ = _build_app(monkeypatch)
    client = TestClient(app)
    # Even with the flag, NQ has no wired source -> still unavailable.
    r = client.get("/api/futures/reference-levels?source=NDX&target=NQ")
    assert r.status_code == 409


def test_reference_levels_projects_from_mocked_db(monkeypatch):
    monkeypatch.setenv("ENABLE_ES_REFERENCE_MODE", "true")
    app, _ = _build_app(monkeypatch)
    ts = datetime(2026, 3, 2, 15, 0, tzinfo=timezone.utc)
    _inject_db(
        get_latest_gex_summary={
            "gamma_flip": 6000.0,
            "call_wall": 6100.0,
            "put_wall": 5900.0,
            "max_pain": 5950.0,
            "underlying_price": 5990.0,
            "timestamp": ts,
        },
        get_latest_future_quote={"close": 6025.0, "timestamp": ts, "future_symbol": "@ES"},
    )
    client = TestClient(app)
    r = client.get("/api/futures/reference-levels?source=SPX&target=ES&contract=front")
    assert r.status_code == 200
    body = r.json()
    assert body["analytics_source"] == "cash_index_projection"
    assert body["quality_status"] == "good"
    assert body["basis"] == pytest.approx(35.0)
    assert body["projected_levels"]["gamma_flip"] == pytest.approx(6035.0)
    assert "not calculated from es futures options" in body["disclaimer"].lower()
    assert body["target_contract"].startswith("ES")


def test_health_endpoint_includes_futures_section(monkeypatch):
    app, _ = _build_app(monkeypatch)
    _inject_db(check_health=True, get_latest_quote=None)
    client = TestClient(app)
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert "futures" in body
    assert body["futures"]["real_feed"] == "pending_licensed_cme_adapter"
    assert body["futures"]["products"]["ES"]["available"] is False


def test_reference_levels_missing_data_is_unavailable(monkeypatch):
    monkeypatch.setenv("ENABLE_ES_REFERENCE_MODE", "true")
    app, _ = _build_app(monkeypatch)
    _inject_db(get_latest_gex_summary=None, get_latest_future_quote=None)
    client = TestClient(app)
    r = client.get("/api/futures/reference-levels?source=SPX&target=ES")
    assert r.status_code == 200
    assert r.json()["quality_status"] == "unavailable"
