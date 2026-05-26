"""API tests for GET /api/signals/advanced/market-pressure.

Mocks ``DatabaseManager.get_advanced_signal`` so no live Postgres is
needed.  Verifies:

  * 404 when no row exists.
  * 200 with the canonical advanced-signal envelope when a row exists
    (mirrors the shape of the other ``/advanced/*`` endpoints).
  * ``market_pressure`` is registered for the events endpoint and the
    advanced confluence matrix.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient


def _build_app(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api import database as dbmod  # noqa: E402

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    from src.api.main import app  # noqa: E402

    return app, dbmod


def _market_pressure_row(*, score: float = 0.56, loading: float = 78.2) -> dict:
    """Shape returned by ``get_advanced_signal('market_pressure')``."""
    return {
        "underlying": "SPY",
        "timestamp": datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc),
        "clamped_score": score,
        "weighted_score": 0.0,
        "weight": 0.0,
        "direction": "bullish" if score > 0 else "bearish" if score < 0 else "neutral",
        "score": round(score * 100.0, 2),
        "score_history": [
            {"score": round(score * 100.0, 2), "timestamp": "2026-05-01T18:30:00Z"},
            {"score": 41.0, "timestamp": "2026-05-01T18:29:00Z"},
        ],
        "context_values": {
            "loading": loading,
            "direction": 0.91,
            "direction_sign": "bullish",
            "label": "Critical" if loading >= 75 else "Loaded",
            "playbook": "Coil at the limit. Expect violent resolution to the upside. "
            "Take the directional trade with reduced size on stops; cut "
            "all counter-pressure exposure.",
            "triggered": True,
            "signal": "bullish_pressure",
            "confidence_mult": 1.3,
            "compression": {
                "magnitude": 0.85,
                "wall_pinch": 0.91,
                "flip_proximity": 1.0,
                "regime_mult": 1.0,
            },
            "hedging": {"signed": 1.0, "magnitude": 1.0, "vanna": 1.2e8, "charm": 9.0e9},
            "flow": {"signed": 0.89, "magnitude": 1.0},
            "tension": {"magnitude": 0.92, "iv_rank": 0.15, "vol_squeeze": 0.81},
            "dealer": {"signed": 0.76, "dealer_net_delta": -3.0e8},
            "weights": {"hedging": 0.45, "flow": 0.40, "dealer": 0.15},
        },
    }


def test_market_pressure_endpoint_returns_404_when_no_row(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_advanced_signal = AsyncMock(return_value=None)
    with TestClient(app) as client:
        resp = client.get("/api/signals/advanced/market-pressure?symbol=SPY")
    assert resp.status_code == 404
    body = resp.json()
    assert "market-pressure" in body["detail"].lower() or "SPY" in body["detail"]


def test_market_pressure_endpoint_surfaces_loaded_context(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_advanced_signal = AsyncMock(return_value=_market_pressure_row())
    with TestClient(app) as client:
        resp = client.get("/api/signals/advanced/market-pressure?symbol=SPY")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Canonical advanced-signal envelope.
    assert body["clamped_score"] == 0.56
    assert body["score"] == 56.0
    assert body["direction"] == "bullish"
    # Promoted context fields.
    assert body["triggered"] is True
    assert body["signal"] == "bullish_pressure"
    assert body["loading"] == 78.2
    assert body["label"] in ("Loaded", "Critical")
    assert body["playbook"].startswith("Coil at the limit")
    assert body["confidence_mult"] == 1.3
    # ``direction_value`` is the signed numeric vector — distinct from the
    # ``direction`` string surfaced by the generic query layer.
    assert body["direction_value"] == 0.91
    # Full sub-component context still available under context_values.
    ctx = body["context_values"]
    assert set(ctx).issuperset({"compression", "hedging", "flow", "tension", "dealer", "weights"})
    # Score history passes through unmodified.
    assert isinstance(body["score_history"], list)
    assert len(body["score_history"]) == 2


def test_market_pressure_endpoint_handles_discharged_signal(monkeypatch):
    """When the row exists but score≈0 (signal dormant), the envelope
    should still be returned 200 with a sensible default label."""
    app, dbmod = _build_app(monkeypatch)
    discharged = _market_pressure_row(score=0.0, loading=12.0)
    discharged["context_values"]["triggered"] = False
    discharged["context_values"]["signal"] = "discharged"
    discharged["context_values"]["label"] = "Discharged"
    discharged["context_values"]["direction"] = 0.0
    discharged["context_values"]["direction_sign"] = "neutral"
    discharged["context_values"]["playbook"] = "No actionable loading."
    dbmod.DatabaseManager.get_advanced_signal = AsyncMock(return_value=discharged)
    with TestClient(app) as client:
        resp = client.get("/api/signals/advanced/market-pressure?symbol=SPY")
    assert resp.status_code == 200
    body = resp.json()
    assert body["triggered"] is False
    assert body["signal"] == "discharged"
    assert body["label"] == "Discharged"
    assert body["loading"] == 12.0
    assert body["direction_value"] == 0.0


def test_market_pressure_is_registered_in_signal_event_names(monkeypatch):
    """The /{signal_name}/events endpoint must whitelist market_pressure."""
    app, _ = _build_app(monkeypatch)
    from src.api.routers.trade_signals import _VALID_SIGNAL_EVENT_NAMES

    assert "market_pressure" in _VALID_SIGNAL_EVENT_NAMES


def test_market_pressure_is_in_advanced_signal_names(monkeypatch):
    """The /advanced/confluence-matrix endpoint enumerates this tuple."""
    app, _ = _build_app(monkeypatch)
    from src.api.routers.trade_signals import _ADVANCED_SIGNAL_NAMES

    assert "market_pressure" in _ADVANCED_SIGNAL_NAMES
    # And it sits last (additive, doesn't reorder the existing matrix).
    assert _ADVANCED_SIGNAL_NAMES[-1] == "market_pressure"
