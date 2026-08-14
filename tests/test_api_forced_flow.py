"""API tests for the Phase 3 /forced-flow/* router.

Patches the router's ``_load`` (chain -> legs) so no live Postgres/snapshot is
needed, and mocks ``get_latest_gex_summary`` for the /levels gamma flip. Verifies
each endpoint returns 200 with the snapshot timestamp + spot and the expected
shape, and 404 on a degraded snapshot.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from src.analytics.forced_flow import ContractLeg


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
    dbmod.DatabaseManager.get_latest_gex_summary = AsyncMock(
        return_value={"gamma_flip_point": 498.0}
    )
    from src.api.main import app

    return app, dbmod


def _ctx(symbol="SPY", expiry=None):
    legs = [
        ContractLeg(495, "C", 800, 0.18, 30 / 365),
        ContractLeg(500, "C", 1500, 0.18, 30 / 365),
        ContractLeg(505, "C", 1200, 0.18, 30 / 365),
        ContractLeg(490, "P", 700, 0.18, 30 / 365),
        ContractLeg(500, "P", 1400, 0.18, 30 / 365),
        ContractLeg(505, "P", 500, 0.18, 30 / 365),
    ]
    return {
        "legs": legs,
        "spot": 500.0,
        "timestamp": datetime(2026, 7, 13, 18, 0, tzinfo=timezone.utc),
        "r": 0.05,
        "q": 0.0,
        "session_days": 0.1,
    }


def _app_with_loader(monkeypatch, loader=_ctx):
    app, _ = _build_app(monkeypatch)
    from src.api.routers import forced_flow as ff_mod

    monkeypatch.setattr(ff_mod, "_load", loader)
    return app


def test_scenario_returns_flow_and_attribution(monkeypatch):
    app = _app_with_loader(monkeypatch)
    with TestClient(app) as c:
        r = c.get("/api/forced-flow/scenario?symbol=SPY&spot_move_pct=0.01&days=0&vol_change_pts=0")
    assert r.status_code == 200
    b = r.json()
    assert b["symbol"] == "SPY" and b["spot"] == 500.0 and b["timestamp"]
    assert set(b) >= {
        "total_usd",
        "gamma_component",
        "charm_component",
        "vanna_component",
        "residual",
    }
    # Pure spot move -> only the gamma component is non-zero.
    assert b["charm_component"] == 0.0 and b["vanna_component"] == 0.0
    assert b["gamma_component"] != 0.0


def test_curve_has_attribution_bands(monkeypatch):
    app = _app_with_loader(monkeypatch)
    with TestClient(app) as c:
        r = c.get("/api/forced-flow/curve?symbol=SPY&spot_range_pct=0.03")
    assert r.status_code == 200
    b = r.json()
    assert b["spot"] == 500.0 and b["timestamp"]
    assert len(b["curve"]) > 5
    assert set(b["curve"][0]) == {"price", "total", "gamma", "charm", "vanna"}
    assert "zero_flow_level" in b


def test_charm_decay_starts_at_zero(monkeypatch):
    app = _app_with_loader(monkeypatch)
    with TestClient(app) as c:
        r = c.get("/api/forced-flow/charm-decay?symbol=SPY&steps=10")
    assert r.status_code == 200
    b = r.json()
    assert len(b["curve"]) == 11
    assert b["curve"][0]["days_elapsed"] == 0.0 and b["curve"][0]["flow"] == 0.0
    assert "close_flow_usd" in b


def test_vanna_ladder_zero_at_no_change(monkeypatch):
    app = _app_with_loader(monkeypatch)
    with TestClient(app) as c:
        r = c.get("/api/forced-flow/vanna-ladder?symbol=SPY&lo_pts=-2&hi_pts=2&step_pts=1")
    assert r.status_code == 200
    b = r.json()
    xs = [p["vol_change_pts"] for p in b["curve"]]
    assert min(xs) == -2.0 and max(xs) == 2.0
    zero = [p["flow"] for p in b["curve"] if p["vol_change_pts"] == 0.0][0]
    assert zero == 0.0


def test_surface_grid_dimensions(monkeypatch):
    app = _app_with_loader(monkeypatch)
    with TestClient(app) as c:
        r = c.get("/api/forced-flow/surface?symbol=SPY&spot_range_pct=0.02&time_steps=4")
    assert r.status_code == 200
    b = r.json()
    assert len(b["times_days"]) == 5
    assert len(b["z"]) == len(b["spots"])
    assert all(len(row) == len(b["times_days"]) for row in b["z"])


def test_levels_uses_existing_gamma_flip(monkeypatch):
    app = _app_with_loader(monkeypatch)
    with TestClient(app) as c:
        r = c.get("/api/forced-flow/levels?symbol=SPY")
    assert r.status_code == 200
    b = r.json()
    # Gamma flip comes from the persisted gex_summary (existing; not recomputed).
    assert b["gamma_flip"] == 498.0
    assert {"charm_flip", "vanna_flip", "zero_flow_level"} <= set(b)


def test_degraded_snapshot_returns_404(monkeypatch):
    app = _app_with_loader(monkeypatch, loader=lambda symbol, expiry=None: None)
    with TestClient(app) as c:
        r = c.get("/api/forced-flow/scenario?symbol=SPY")
    assert r.status_code == 404


def test_backtest_reports_hit_rate(monkeypatch):
    # /backtest is a pure DB read -- no snapshot/_load needed, so mock the
    # session-assembling query directly. The endpoint scores each session's flow
    # against its own trailing-median baseline (default 10-session warm-up), so
    # we feed 10 flat warm-up sessions and then decisive days whose flow runs
    # above / below that baseline. full and smooth deviate in opposite directions
    # to prove the A/B scores them independently.
    app, dbmod = _build_app(monkeypatch)
    sessions = [
        {
            "session_date": f"2026-06-{d:02d}",
            "charm_flow": 5.0,
            "charm_flow_smooth": 5.0,
            "noon_px": 100.0,
            "close_px": 100.0,
        }
        for d in range(1, 11)  # 10 warm-up sessions establishing a baseline of 5.0
    ]
    # Decisive days: full above/below 5.0 => Buy/Sell; smooth is the mirror.
    sessions += [
        {"session_date": "2026-06-11", "charm_flow": 6.0, "charm_flow_smooth": 4.0,
         "noon_px": 100.0, "close_px": 101.0},  # full Buy/up hit ; smooth Sell/up miss
        {"session_date": "2026-06-12", "charm_flow": 4.0, "charm_flow_smooth": 6.0,
         "noon_px": 100.0, "close_px": 99.0},   # full Sell/down hit; smooth Buy/down miss
        {"session_date": "2026-06-13", "charm_flow": 6.0, "charm_flow_smooth": 4.0,
         "noon_px": 100.0, "close_px": 99.0},   # full Buy/down miss; smooth Sell/down hit
    ]
    dbmod.DatabaseManager.get_charm_backtest_sessions = AsyncMock(return_value=sessions)
    with TestClient(app) as c:
        r = c.get("/api/forced-flow/backtest?symbol=SPY&lookback_days=90")
    assert r.status_code == 200
    b = r.json()
    assert b["symbol"] == "SPY" and b["lookback_days"] == 90
    full, smooth = b["full"], b["smooth"]
    assert full["total_sessions"] == 13 and full["warmup_sessions"] == 10
    assert full["evaluated_sessions"] == 3 and full["hits"] == 2
    assert abs(full["hit_rate"] - (2 / 3)) < 1e-9
    # Smooth deviates opposite to full, so it hits exactly where full misses: 1/3.
    assert smooth["hits"] == 1 and abs(smooth["hit_rate"] - (1 / 3)) < 1e-9
    # Statistics present on both; the tiny sample is not certified significant.
    assert full["hit_rate_ci_low"] is not None and full["hit_rate_ci_high"] is not None
    assert full["significant"] is False and smooth["significant"] is False
    assert "edge_p_value" in full and "signal_t_stat" in smooth
    assert len(full["records"]) == 3
    assert full["records"][0]["date"] == "2026-06-13"  # most-recent first
    assert set(full["records"][0]) == {
        "date",
        "charm_flow",
        "charm_baseline",
        "charm_deviation",
        "return_pct",
        "predicted_dir",
        "realized_dir",
        "hit",
    }


def test_backtest_empty_history_is_200_not_404(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_charm_backtest_sessions = AsyncMock(return_value=[])
    with TestClient(app) as c:
        r = c.get("/api/forced-flow/backtest?symbol=SPY")
    assert r.status_code == 200
    b = r.json()
    for variant in (b["full"], b["smooth"]):
        assert variant["total_sessions"] == 0 and variant["evaluated_sessions"] == 0
        assert variant["warmup_sessions"] == 0
        assert variant["hit_rate"] is None and variant["records"] == []
