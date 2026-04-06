"""Tests for /api/signals/score and /api/signals/vol-expansion endpoints."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.main import app
from src.api.routers.trade_signals import get_db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_db():
    db = AsyncMock()
    app.dependency_overrides[get_db] = lambda: db
    yield db
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture()
def client():
    return TestClient(app)


# ---------------------------------------------------------------------------
# Sample payloads
# ---------------------------------------------------------------------------

_SCORE_ROW = {
    "underlying": "SPY",
    "timestamp": datetime(2026, 4, 6, 14, 30, tzinfo=timezone.utc),
    "composite_score": 0.72,
    "normalized_score": 0.74,
    "direction": 1,
    "components": {"gex_regime": {"value": -8.5}},
    "regime": "short_gamma",
    "analytics": {
        "sample_size": 120,
        "hit_rate": 0.63,
        "expected_move_bp": 12.5,
        "confidence": 0.42,
        "action": "enter",
        "calibration_scope": "regime+strength",
    },
}

_VOL_EXPANSION_ROW = {
    "underlying": "SPY",
    "timestamp": datetime(2026, 4, 6, 14, 30, tzinfo=timezone.utc),
    "composite_score": 7.5,
    "max_possible_score": 10.0,
    "normalized_score": 0.75,
    "move_probability": 0.82,
    "expected_direction": "up",
    "expected_magnitude_pct": 1.2,
    "confidence": 0.68,
    "catalyst_type": "gamma_squeeze",
    "time_horizon": "intraday",
    "strategy_type": "long_call",
    "entry_window": "next_15m",
    "current_price": 520.50,
    "net_gex": -1500000.0,
    "gamma_flip": 525.0,
    "max_pain": 518.0,
    "put_call_ratio": 0.85,
    "dealer_net_delta": -200000.0,
    "smart_money_direction": "bullish",
    "vwap_deviation_pct": -0.3,
    "hours_to_next_expiry": 4.5,
    "components": {"gex_regime": {"value": -8.5}},
}


# ---------------------------------------------------------------------------
# /api/signals/score
# ---------------------------------------------------------------------------

class TestScoreEndpoint:
    def test_returns_latest_score(self, client, mock_db):
        mock_db.get_latest_signal_score_enriched = AsyncMock(return_value=_SCORE_ROW)

        resp = client.get("/api/signals/score")

        assert resp.status_code == 200
        body = resp.json()
        assert body["underlying"] == "SPY"
        assert body["composite_score"] == 0.72
        assert body["regime"] == "short_gamma"
        assert body["analytics"]["action"] == "enter"
        mock_db.get_latest_signal_score_enriched.assert_awaited_once_with("SPY")

    def test_passes_underlying_param(self, client, mock_db):
        mock_db.get_latest_signal_score_enriched = AsyncMock(return_value={**_SCORE_ROW, "underlying": "QQQ"})

        resp = client.get("/api/signals/score?underlying=qqq")

        assert resp.status_code == 200
        mock_db.get_latest_signal_score_enriched.assert_awaited_once_with("QQQ")

    def test_404_when_no_rows(self, client, mock_db):
        mock_db.get_latest_signal_score_enriched = AsyncMock(return_value=None)

        resp = client.get("/api/signals/score?underlying=XYZ")

        assert resp.status_code == 404
        assert "XYZ" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# /api/signals/vol-expansion
# ---------------------------------------------------------------------------

class TestVolExpansionEndpoint:
    def test_returns_vol_expansion_signal(self, client, mock_db):
        mock_db.get_vol_expansion_signal = AsyncMock(return_value=_VOL_EXPANSION_ROW)

        resp = client.get("/api/signals/vol-expansion")

        assert resp.status_code == 200
        body = resp.json()
        assert body["underlying"] == "SPY"
        assert body["composite_score"] == 7.5
        assert body["move_probability"] == 0.82
        assert body["catalyst_type"] == "gamma_squeeze"
        mock_db.get_vol_expansion_signal.assert_awaited_once_with("SPY")

    def test_passes_symbol_param(self, client, mock_db):
        mock_db.get_vol_expansion_signal = AsyncMock(return_value={**_VOL_EXPANSION_ROW, "underlying": "QQQ"})

        resp = client.get("/api/signals/vol-expansion?symbol=qqq")

        assert resp.status_code == 200
        mock_db.get_vol_expansion_signal.assert_awaited_once_with("QQQ")

    def test_404_when_no_rows(self, client, mock_db):
        mock_db.get_vol_expansion_signal = AsyncMock(return_value=None)

        resp = client.get("/api/signals/vol-expansion?symbol=XYZ")

        assert resp.status_code == 404
        assert "XYZ" in resp.json()["detail"]
