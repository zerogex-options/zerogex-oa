"""API tests for /api/gex/gamma-surface.

Patches the router's ``_load_book`` (snapshot -> legs by expiration) so no
Postgres is needed, the same way the forced-flow API tests patch ``_load``.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from src.analytics.forced_flow import ContractLeg

TS = datetime(2026, 10, 14, 15, 0, tzinfo=timezone.utc)  # 11:00 ET
HOURS = 1.0 / 24 / 365


def _build_app(monkeypatch):
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    from src.api.main import app

    return app


def _book(symbol):
    return {
        "spot": 500.0,
        "timestamp": TS,
        "r": 0.04,
        "q": 0.0,
        "legs_by_expiration": {
            # Settles today at 16:00 ET, 5 hours after TS.
            "2026-10-14": [
                ContractLeg(500.0, "C", 3000, 0.15, 5 * HOURS),
                ContractLeg(495.0, "P", 2500, 0.17, 5 * HOURS),
            ],
            "2026-10-16": [
                ContractLeg(510.0, "C", 4000, 0.14, 53 * HOURS),
                ContractLeg(490.0, "P", 5000, 0.18, 53 * HOURS),
                ContractLeg(500.0, "P", 1000, 0.16, 53 * HOURS),
            ],
        },
    }


def _client(monkeypatch, loader=_book):
    app = _build_app(monkeypatch)
    from src.api.routers import gamma_surface as mod

    calls = []

    def counting_loader(symbol):
        calls.append(symbol)
        return loader(symbol)

    monkeypatch.setattr(mod, "_load_book", counting_loader)
    return TestClient(app), calls


def test_all_expirations_by_default(monkeypatch):
    client, _ = _client(monkeypatch)
    with client as c:
        r = c.get("/api/gex/gamma-surface?symbol=spy")
    assert r.status_code == 200
    b = r.json()
    assert b["symbol"] == "SPY" and b["spot"] == 500.0
    assert b["expirations"] == ["2026-10-14", "2026-10-16"]
    assert b["contract_count"] == 5
    # The axis ends at the first settlement: today's 16:00 ET close.
    assert b["settling_expiration"] == "2026-10-14"
    settles = datetime.fromisoformat(b["settles_at"])
    assert abs(settles - (TS + timedelta(hours=5))) < timedelta(seconds=1)
    assert len(b["times_days"]) == len(b["profiles"]) == 41
    assert b["times_days"][0] == 0.0
    assert all(len(row) == len(b["grid"]) == 121 for row in b["profiles"])
    assert b["grid"][0] < 500.0 < b["grid"][-1]


def test_a_single_later_expiration_runs_to_its_own_settlement(monkeypatch):
    client, _ = _client(monkeypatch)
    with client as c:
        r = c.get("/api/gex/gamma-surface?symbol=SPY&expirations=2026-10-16")
    assert r.status_code == 200
    b = r.json()
    assert b["expirations"] == ["2026-10-16"]
    assert b["settling_expiration"] == "2026-10-16"
    assert b["contract_count"] == 3
    settles = datetime.fromisoformat(b["settles_at"])
    assert abs(settles - (TS + timedelta(hours=53))) < timedelta(seconds=1)


def test_selection_shares_one_snapshot_load(monkeypatch):
    client, calls = _client(monkeypatch)
    with client as c:
        assert c.get("/api/gex/gamma-surface?symbol=SPY").status_code == 200
        assert c.get("/api/gex/gamma-surface?symbol=SPY&expirations=2026-10-14").status_code == 200
        assert c.get("/api/gex/gamma-surface?symbol=SPY&expirations=all").status_code == 200
    assert calls == ["SPY"]


def test_selection_matches_regardless_of_order_and_duplicates(monkeypatch):
    client, _ = _client(monkeypatch)
    with client as c:
        a = c.get("/api/gex/gamma-surface?symbol=SPY&expirations=2026-10-16,2026-10-14").json()
        b = c.get(
            "/api/gex/gamma-surface?symbol=SPY&expirations=2026-10-14,2026-10-16,2026-10-14"
        ).json()
        whole = c.get("/api/gex/gamma-surface?symbol=SPY").json()
    assert a == b
    assert a["profiles"] == whole["profiles"]


def test_unknown_expiration_is_a_404_with_a_reason(monkeypatch):
    client, _ = _client(monkeypatch)
    with client as c:
        r = c.get("/api/gex/gamma-surface?symbol=SPY&expirations=2026-10-13")
    assert r.status_code == 404
    assert "selected expirations" in r.json()["detail"]


def test_no_snapshot_is_a_404(monkeypatch):
    client, _ = _client(monkeypatch, loader=lambda symbol: None)
    with client as c:
        r = c.get("/api/gex/gamma-surface?symbol=SPY")
    assert r.status_code == 404
    assert "snapshot" in r.json()["detail"]


def test_bad_parameters_are_400s(monkeypatch):
    client, calls = _client(monkeypatch)
    with client as c:
        assert c.get("/api/gex/gamma-surface?expirations=10/16/2026").status_code == 400
        assert c.get("/api/gex/gamma-surface?price_points=241&time_steps=97").status_code == 400
        assert c.get("/api/gex/gamma-surface?span_pct=0.5").status_code == 422
    assert calls == []


def test_blank_selection_means_all(monkeypatch):
    client, _ = _client(monkeypatch)
    with client as c:
        r = c.get("/api/gex/gamma-surface?symbol=SPY&expirations=,")
    assert r.status_code == 200
    assert r.json()["expirations"] == ["2026-10-14", "2026-10-16"]
