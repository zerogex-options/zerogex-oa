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


def test_available_dates_lists_symbol_history(monkeypatch):
    """The /forecast landing page picker fetches this; must route to the
    literal '/available-dates' handler (not the /{forecast_date} catchall)
    and return newest-first date rows with verdict badges."""
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_forecast_available_dates = AsyncMock(return_value=[
        {"date": date(2026, 6, 29), "regime": "short_gamma", "has_receipt": True,
         "range_respected": True, "pin_hit": False, "regime_correct": True},
        {"date": date(2026, 6, 26), "regime": "long_gamma", "has_receipt": False,
         "range_respected": None, "pin_hit": None, "regime_correct": None},
    ])
    with TestClient(app) as client:
        r = client.get("/api/forecast/available-dates?symbol=SPY&limit=30")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["symbol"] == "SPY"
    assert body["count"] == 2
    assert body["dates"][0]["date"] == "2026-06-29"
    assert body["dates"][0]["has_receipt"] is True
    assert body["dates"][0]["range_respected"] is True
    assert body["dates"][1]["has_receipt"] is False


def test_available_dates_tolerates_pre_migration_rows(monkeypatch):
    """A row shaped like the pre-v1.4 schema (no expected_vol_state /
    vol_state_correct keys) must still list, with null vol badges. The router
    reads every badge via .get(), and the query uses SELECT *, so a column a
    pending migration hasn't added yet degrades to a blank pill instead of
    500ing — or silently blanking — the whole landing page."""
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_forecast_available_dates = AsyncMock(return_value=[
        {"date": date(2026, 6, 29), "regime": "long_gamma", "has_receipt": True,
         "range_respected": True, "pin_hit": False, "regime_correct": True},
    ])
    with TestClient(app) as client:
        r = client.get("/api/forecast/available-dates?symbol=SPY")
    assert r.status_code == 200, r.text
    row = r.json()["dates"][0]
    assert row["range_respected"] is True
    assert row["expected_vol_state"] is None
    assert row["vol_state_correct"] is None


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
    # This test predates the fix-date cutoff and only cares about the
    # scored-vs-unscored split, so pin the regime cutoff before its rows.
    monkeypatch.setenv("FORECAST_REGIME_FIX_DATE", "2026-01-01")
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


def _scored_row(d: str, regime_correct, range_respected=True, pin_hit=True):
    """Minimal receipted-row shape the rolling-stats endpoint reads."""
    return {
        "date": date.fromisoformat(d),
        "receipt_ts": datetime(2026, 7, 9, 20, 5, tzinfo=timezone.utc),
        "range_respected": range_respected,
        "pin_hit": pin_hit,
        "regime_correct": regime_correct,
    }


def test_rolling_stats_regime_rate_excludes_pre_fix_rows(monkeypatch):
    """The regime rate must only count corrected-era (>= fix date) forecasts;
    the pre-fix all-long_gamma rows must not inflate it. Range/pin still count
    every scored row."""
    app, dbmod = _build_app(monkeypatch)
    monkeypatch.setenv("FORECAST_REGIME_FIX_DATE", "2026-07-09")
    dbmod.DatabaseManager.get_daily_forecast_history = AsyncMock(
        return_value=[
            # Pre-fix buggy rows — all "correct", must be excluded from regime rate.
            _scored_row("2026-07-06", True),
            _scored_row("2026-07-07", True),
            _scored_row("2026-07-08", True),
            # Corrected era — 1 of 2 correct.
            _scored_row("2026-07-09", True),
            _scored_row("2026-07-10", False, range_respected=False),
        ]
    )
    with TestClient(app) as client:
        r = client.get("/api/forecast/stats/rolling?symbol=SPY&window=30")
    body = r.json()
    assert body["regime_stats_from"] == "2026-07-09"
    assert body["regime_n_scored"] == 2
    # Only the 2 corrected-era rows count → 1/2, NOT 4/5.
    assert body["regime_correct_rate"] == pytest.approx(0.5, abs=1e-4)
    # Range/pin still span all 5 scored rows.
    assert body["n_scored"] == 5
    assert body["range_respected_rate"] == pytest.approx(0.8, abs=1e-4)
    assert body["pin_hit_rate"] == pytest.approx(1.0, abs=1e-4)


def test_rolling_stats_regime_rate_null_when_no_corrected_rows(monkeypatch):
    """Before any corrected receipts accrue, the regime rate is null (renders
    '—') rather than showing the misleading pre-fix number."""
    app, dbmod = _build_app(monkeypatch)
    monkeypatch.setenv("FORECAST_REGIME_FIX_DATE", "2026-07-09")
    dbmod.DatabaseManager.get_daily_forecast_history = AsyncMock(
        return_value=[
            _scored_row("2026-07-06", True),
            _scored_row("2026-07-07", True),
        ]
    )
    with TestClient(app) as client:
        r = client.get("/api/forecast/stats/rolling?symbol=SPY&window=30")
    body = r.json()
    assert body["regime_correct_rate"] is None
    assert body["regime_n_scored"] == 0
    # Range/pin are unaffected by the regime cutoff.
    assert body["range_respected_rate"] == pytest.approx(1.0, abs=1e-4)


def _vol_row(d: str, correct, ratio):
    """A receipted row carrying the v1.4 vol verdict + realized ratio."""
    return {
        "date": date.fromisoformat(d),
        "receipt_ts": datetime(2026, 8, 3, 20, 5, tzinfo=timezone.utc),
        "range_respected": True,
        "pin_hit": True,
        "regime_correct": True,
        "vol_state_correct": correct,
        "realized_vol_ratio": ratio,
    }


def test_rolling_stats_vol_gated_baselined_and_ci(monkeypatch):
    """The vol track record excludes pre-scale-fix rows, reports a Wilson CI,
    and carries the majority-realized-bucket strawman as its baseline."""
    app, dbmod = _build_app(monkeypatch)
    monkeypatch.setenv("FORECAST_REGIME_FIX_DATE", "2026-01-01")
    monkeypatch.setenv("FORECAST_VOL_SCALE_FIX_DATE", "2026-08-01")
    dbmod.DatabaseManager.get_daily_forecast_history = AsyncMock(
        return_value=[
            # Pre-fix, old-scale row — must be excluded from the vol stats.
            _vol_row("2026-07-30", True, 1.60),
            # Corrected era: 2 of 3 correct; realized buckets normal/normal/expansion.
            _vol_row("2026-08-01", True, 1.00),
            _vol_row("2026-08-02", True, 0.95),
            _vol_row("2026-08-03", False, 1.40),
        ]
    )
    with TestClient(app) as client:
        r = client.get("/api/forecast/stats/rolling?symbol=SPY&window=30")
    body = r.json()
    assert body["vol_stats_from"] == "2026-08-01"
    assert body["vol_n_scored"] == 3  # pre-fix row excluded
    assert body["vol_state_correct_rate"] == pytest.approx(2 / 3, abs=1e-4)
    ci = body["vol_state_correct_ci"]
    assert isinstance(ci, list) and len(ci) == 2
    lo, hi = ci
    assert 0.0 <= lo <= body["vol_state_correct_rate"] <= hi <= 1.0
    # Majority realized bucket over the corrected rows is "normal" (2 of 3).
    assert body["vol_baseline_label"] == "normal"
    assert body["vol_baseline"] == pytest.approx(2 / 3, abs=1e-4)
    # Range gets a CI + published-coverage baseline too.
    assert isinstance(body["range_respected_ci"], list)
    assert body["range_baseline"] == pytest.approx(0.80)
