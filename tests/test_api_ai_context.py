"""Tests for /api/ai/context — the AI grounding snapshot.

The endpoint composes derived analytics (GEX summary + MSI score + Action
Card) into one LLM-ready payload. These tests drive the HTTP surface with
canned DB rows (no Postgres) to verify composition, graceful partial
assembly, the regime-hint mapping, and the all-missing 404.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient


def _build_app_with_mock_db(monkeypatch: pytest.MonkeyPatch):
    """Reload src.api.main with an AsyncMock DatabaseManager (no real DB)."""
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

    from src.api.main import app
    from src.api import main as mainmod

    return app, mainmod


def _attach(mainmod, *, summary, score):
    # Patch at the class level so the mocks survive the lifespan-time
    # ``db_manager = DatabaseManager()`` reassignment inside main.py — an
    # instance-only patch is a no-op once the TestClient lifespan runs.
    from src.api import database as dbmod

    dbmod.DatabaseManager.get_latest_gex_summary = AsyncMock(  # type: ignore[method-assign]
        return_value=summary
    )
    # fmt: off
    dbmod.DatabaseManager.get_latest_signal_score_enriched = (  # type: ignore[method-assign]
        AsyncMock(return_value=score)
    )
    # fmt: on
    mainmod.db_manager = mainmod.db_manager or mainmod.DatabaseManager()
    return mainmod.db_manager


def _stub_action_card(monkeypatch, value):
    """Stub the lazily-imported playbook context builder.

    The endpoint imports ``build_playbook_context`` inside the handler from
    ``src.signals.playbook.context_builder``; returning ``None`` keeps the
    Action Card out of the payload without booting the playbook engine.
    """
    import src.signals.playbook.context_builder as cb

    monkeypatch.setattr(cb, "build_playbook_context", AsyncMock(return_value=value))


_SUMMARY = {
    "spot_price": Decimal("678.40"),
    "net_gex": Decimal("7100000000"),
    "net_gex_at_spot": Decimal("3200000000"),
    "gamma_flip": Decimal("675.00"),
    "flip_distance": Decimal("0.50"),
    "max_pain": Decimal("676.00"),
    "call_wall": Decimal("680.00"),
    "put_wall": Decimal("670.00"),
    "put_call_ratio": Decimal("0.92"),
    "timestamp": None,
}

# Enriched-score row shape consumed by _normalize_signal_score_row.
_SCORE = {"composite_score": 72.5, "components": {}, "timestamp": None}


def test_context_composes_market_state_and_msi(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    _attach(mainmod, summary=_SUMMARY, score=_SCORE)
    _stub_action_card(monkeypatch, None)

    with TestClient(app) as client:
        resp = client.get("/api/ai/context", params={"underlying": "spy"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "SPY"
    assert "generated_at" in body
    assert "disclaimer" in body and "not financial advice" in body["disclaimer"].lower()

    # market_state floatifies Decimals and carries the structural levels.
    ms = body["market_state"]
    assert ms["spot_price"] == pytest.approx(678.40)
    assert ms["gamma_flip"] == pytest.approx(675.00)
    assert ms["call_wall"] == pytest.approx(680.00)

    # MSI composite >= 70 maps to the trend_expansion band.
    assert body["msi"]["composite_score"] == pytest.approx(72.5)
    assert body["msi"]["regime_hint"] == "trend_expansion"

    # Action card omitted (builder stubbed to None) but key still present.
    assert body["action_card"] is None


def test_context_partial_when_summary_missing(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    _attach(mainmod, summary=None, score=_SCORE)
    _stub_action_card(monkeypatch, None)

    with TestClient(app) as client:
        resp = client.get("/api/ai/context", params={"underlying": "SPY"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["market_state"] is None
    assert body["msi"] is not None


def test_context_regime_hint_bands(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    _stub_action_card(monkeypatch, None)

    cases = {15.0: "high_risk_reversal", 30.0: "chop_range", 55.0: "controlled_trend"}
    with TestClient(app) as client:
        for score_value, expected in cases.items():
            _attach(
                mainmod,
                summary=None,
                score={"composite_score": score_value, "components": {}, "timestamp": None},
            )
            resp = client.get("/api/ai/context", params={"underlying": "SPY"})
            assert resp.status_code == 200
            assert resp.json()["msi"]["regime_hint"] == expected


def test_context_404_when_nothing_available(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app_with_mock_db(monkeypatch)
    _attach(mainmod, summary=None, score=None)
    _stub_action_card(monkeypatch, None)

    with TestClient(app) as client:
        resp = client.get("/api/ai/context", params={"underlying": "SPY"})

    assert resp.status_code == 404
