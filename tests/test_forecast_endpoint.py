"""Integration tests for GET /api/forecast/* — the public read surface
for the daily Gamma Forecast Card. Stubs the DB layer; no live Postgres."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient


def _build_app(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("API_KEY", raising=False)
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


def _morning_row(**overrides):
    base = {
        "symbol": "SPY",
        "date": date(2026, 6, 29),
        "open_ts": datetime(2026, 6, 29, 11, 0, tzinfo=timezone.utc),
        "open_spot": Decimal("600.00"),
        "call_wall": Decimal("606.00"),
        "put_wall": Decimal("594.00"),
        "gamma_flip": Decimal("600.50"),
        "open_msi": Decimal("-32.00"),
        "regime": "short_gamma",
        "projected_low": Decimal("593.40"),
        "projected_high": Decimal("606.60"),
        "projected_close": Decimal("599.00"),
        "pin_strike": Decimal("599.00"),
        "flagship_setup": {"action": "SELL_CALL_SPREAD", "pattern": "call_wall_fade", "confidence": 0.68},
        "range_model": "heuristic_v1",
        "content_hash": "a" * 64,
        "receipt_ts": None,
        "actual_low": None,
        "actual_high": None,
        "actual_close": None,
        "range_respected": None,
        "pin_hit": None,
        "regime_correct": None,
        "setup_outcome": None,
        "created_at": datetime(2026, 6, 29, 11, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 6, 29, 11, 0, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


def _with_receipt(**overrides):
    row = _morning_row(
        receipt_ts=datetime(2026, 6, 29, 20, 5, tzinfo=timezone.utc),
        actual_low=Decimal("595.12"),
        actual_high=Decimal("604.88"),
        actual_close=Decimal("599.40"),
        range_respected=True,
        pin_hit=True,
        regime_correct=True,
    )
    row.update(overrides)
    return row


def test_get_forecast_morning_only(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_daily_forecast = AsyncMock(return_value=_morning_row())
    with TestClient(app) as client:
        r = client.get("/api/forecast/2026-06-29?symbol=SPY")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["symbol"] == "SPY"
    assert body["date"] == "2026-06-29"
    assert body["morning"]["regime"] == "short_gamma"
    assert body["morning"]["projected_low"] == pytest.approx(593.4)
    assert body["morning"]["projected_high"] == pytest.approx(606.6)
    assert body["morning"]["pin_strike"] == pytest.approx(599.0)
    assert body["morning"]["range_model"] == "heuristic_v1"
    # Pre-receipt rows surface receipt as null — the frontend branches on this.
    assert body["receipt"] is None


def test_get_forecast_with_receipt_passes_verdicts(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_daily_forecast = AsyncMock(return_value=_with_receipt())
    with TestClient(app) as client:
        r = client.get("/api/forecast/2026-06-29")
    body = r.json()
    assert body["receipt"]["range_respected"] is True
    assert body["receipt"]["pin_hit"] is True
    assert body["receipt"]["regime_correct"] is True
    assert body["receipt"]["actual_close"] == pytest.approx(599.4)


def test_get_forecast_404_when_no_row(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_daily_forecast = AsyncMock(return_value=None)
    with TestClient(app) as client:
        r = client.get("/api/forecast/2026-06-29")
    assert r.status_code == 404


def test_get_forecast_rejects_invalid_date(monkeypatch):
    app, _ = _build_app(monkeypatch)
    with TestClient(app) as client:
        r = client.get("/api/forecast/2026-13-99")
    assert r.status_code == 422


def test_get_latest_forecast_returns_most_recent(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_daily_forecast_history = AsyncMock(
        return_value=[_morning_row(date=date(2026, 6, 30))]
    )
    dbmod.DatabaseManager.get_daily_forecast = AsyncMock(return_value=_morning_row(date=date(2026, 6, 30)))
    with TestClient(app) as client:
        r = client.get("/api/forecast?symbol=SPY")
    body = r.json()
    assert body["date"] == "2026-06-30"


def test_get_latest_forecast_404_when_history_empty(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_daily_forecast_history = AsyncMock(return_value=[])
    with TestClient(app) as client:
        r = client.get("/api/forecast")
    assert r.status_code == 404


def test_recent_history_returns_compact_rows(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_daily_forecast_history = AsyncMock(
        return_value=[
            _with_receipt(date=date(2026, 6, 27)),
            _with_receipt(date=date(2026, 6, 26), pin_hit=False),
            _morning_row(date=date(2026, 6, 29)),  # no receipt yet
        ]
    )
    with TestClient(app) as client:
        r = client.get("/api/forecast/history/recent?limit=10")
    body = r.json()
    assert body["count"] == 3
    assert body["rows"][0]["has_receipt"] is True
    assert body["rows"][2]["has_receipt"] is False


def test_rolling_stats_computes_only_over_scored(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_daily_forecast_history = AsyncMock(
        return_value=[
            _with_receipt(date=date(2026, 6, 27), range_respected=True,  pin_hit=True,  regime_correct=True),
            _with_receipt(date=date(2026, 6, 26), range_respected=False, pin_hit=False, regime_correct=True),
            _with_receipt(date=date(2026, 6, 25), range_respected=True,  pin_hit=True,  regime_correct=None),
            _morning_row(date=date(2026, 6, 29)),  # excluded: no receipt
        ]
    )
    with TestClient(app) as client:
        r = client.get("/api/forecast/stats/rolling?window=30")
    body = r.json()
    assert body["n_scored"] == 3
    assert body["range_respected_rate"] == pytest.approx(2 / 3, abs=1e-4)
    assert body["pin_hit_rate"] == pytest.approx(2 / 3, abs=1e-4)
    # regime_correct: 2 out of 3 had a value, both True → 1.0.
    assert body["regime_correct_rate"] == pytest.approx(1.0, abs=1e-4)
