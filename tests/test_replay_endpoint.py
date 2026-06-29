"""Integration tests for /api/replay/* — the public read surface for the
GEX Replay scrubber. Stubs the DB layer so no live Postgres is needed."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from urllib.parse import quote
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


def _summary(ts: datetime, **overrides) -> dict:
    base = {
        "timestamp": ts,
        "spot_price": Decimal("600.00"),
        "call_wall": Decimal("606.00"),
        "put_wall": Decimal("594.00"),
        "gamma_flip": Decimal("600.50"),
        "max_pain": Decimal("599.00"),
        "net_gex": Decimal("12345.6789"),
        "net_gex_at_spot": Decimal("2345.0"),
        "put_call_ratio": Decimal("1.23"),
    }
    base.update(overrides)
    return base


def _strikes(spot: float = 600.0):
    return [
        {"timestamp": None, "strike": Decimal(str(spot + d)), "call_gex": Decimal(str(1000 - 5 * d)),
         "put_gex": Decimal(str(-800 + 4 * d)),
         "net_gex": Decimal(str(200 - d)),
         "distance_from_spot": Decimal(str(d))}
        for d in (-5, -3, -1, 0, 1, 3, 5)
    ]


def test_sessions_returns_recent_dates(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_replay_session_dates = AsyncMock(return_value=[
        {"session_date": date(2026, 6, 29), "bar_count": 390,
         "first_ts": datetime(2026, 6, 29, 13, 30, tzinfo=timezone.utc),
         "last_ts": datetime(2026, 6, 29, 20, 0, tzinfo=timezone.utc)},
        {"session_date": date(2026, 6, 26), "bar_count": 95,
         "first_ts": datetime(2026, 6, 26, 13, 30, tzinfo=timezone.utc),
         "last_ts": datetime(2026, 6, 26, 15, 5, tzinfo=timezone.utc)},
    ])
    with TestClient(app) as client:
        r = client.get("/api/replay/sessions?symbol=SPY&limit=10")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 2
    assert body["sessions"][0]["date"] == "2026-06-29"
    assert body["sessions"][0]["bar_count"] == 390
    assert body["sessions"][1]["bar_count"] == 95


def test_frame_returns_summary_plus_strikes(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    frame_ts = datetime(2026, 6, 29, 14, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_summary_at_ts = AsyncMock(return_value=_summary(frame_ts))
    dbmod.DatabaseManager.get_gex_by_strike_at_ts = AsyncMock(return_value=_strikes())
    with TestClient(app) as client:
        r = client.get("/api/replay/frame?symbol=SPY&ts=2026-06-29T14:35:00Z")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["frame_ts"] == frame_ts.isoformat()
    assert body["requested_ts"] == "2026-06-29T14:35:00+00:00"
    assert body["summary"]["spot"] == pytest.approx(600.0)
    assert body["summary"]["call_wall"] == pytest.approx(606.0)
    assert len(body["strikes"]) == 7
    # Strikes carry the four expected fields.
    assert all({"strike", "call_gex", "put_gex", "net_gex"} <= row.keys() for row in body["strikes"])


def test_frame_returns_404_when_no_data(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_gex_summary_at_ts = AsyncMock(return_value=None)
    with TestClient(app) as client:
        r = client.get("/api/replay/frame?symbol=SPY&ts=2024-01-01T14:35:00Z")
    assert r.status_code == 404


def test_frame_rejects_invalid_ts(monkeypatch):
    app, _ = _build_app(monkeypatch)
    with TestClient(app) as client:
        r = client.get("/api/replay/frame?symbol=SPY&ts=not-a-timestamp")
    assert r.status_code == 422


def test_range_filters_to_session_window(monkeypatch):
    """The heatmap call returns newest-first; the route must filter to the
    requested session window (13:30 UTC = 09:30 ET in EDT) and reverse
    to chronological order."""
    app, dbmod = _build_app(monkeypatch)
    # Build heatmap with bars across two days; only 2026-06-29 should survive.
    in_session = datetime(2026, 6, 29, 14, 30, tzinfo=timezone.utc)
    out_of_session_early = datetime(2026, 6, 29, 12, 0, tzinfo=timezone.utc)  # pre-open
    out_of_session_late = datetime(2026, 6, 29, 21, 0, tzinfo=timezone.utc)   # post-close
    other_day = datetime(2026, 6, 28, 14, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_heatmap = AsyncMock(return_value=[
        {"timestamp": other_day, "gamma_flip": Decimal("601"), "heatmap": []},
        {"timestamp": out_of_session_late, "gamma_flip": Decimal("601"), "heatmap": []},
        {"timestamp": in_session, "gamma_flip": Decimal("600.5"),
         "heatmap": [{"strike": Decimal("600"), "net_gex": Decimal("1234.5")}]},
        {"timestamp": out_of_session_early, "gamma_flip": Decimal("600"), "heatmap": []},
    ])
    with TestClient(app) as client:
        r = client.get("/api/replay/range?symbol=SPY&date=2026-06-29")
    body = r.json()
    assert r.status_code == 200, r.text
    assert body["date"] == "2026-06-29"
    assert body["count"] == 1
    assert body["frames"][0]["timestamp"] == in_session.isoformat()
    assert body["frames"][0]["strikes"][0]["strike"] == pytest.approx(600.0)


def test_diff_computes_strike_deltas(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    ts_a = datetime(2026, 6, 29, 14, 0, tzinfo=timezone.utc)
    ts_b = datetime(2026, 6, 29, 15, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_summary_at_ts = AsyncMock(side_effect=[
        _summary(ts_a, spot_price=Decimal("600.0")),
        _summary(ts_b, spot_price=Decimal("602.0")),
    ])
    dbmod.DatabaseManager.get_gex_by_strike_at_ts = AsyncMock(side_effect=[
        [{"strike": Decimal("600"), "net_gex": Decimal("100"), "call_gex": Decimal("60"),
          "put_gex": Decimal("-40"), "distance_from_spot": Decimal("0")}],
        [{"strike": Decimal("600"), "net_gex": Decimal("150"), "call_gex": Decimal("90"),
          "put_gex": Decimal("-60"), "distance_from_spot": Decimal("-2")},
         {"strike": Decimal("605"), "net_gex": Decimal("50"), "call_gex": Decimal("30"),
          "put_gex": Decimal("-20"), "distance_from_spot": Decimal("3")}],
    ])
    with TestClient(app) as client:
        # URL-encode the timestamps because ``+`` is a reserved query-string
        # delimiter that decodes to a literal space in the route handler.
        r = client.get(
            f"/api/replay/diff?symbol=SPY&ts_a={quote(ts_a.isoformat())}&ts_b={quote(ts_b.isoformat())}"
        )
    body = r.json()
    assert r.status_code == 200, r.text
    deltas = {row["strike"]: row for row in body["deltas"]}
    assert deltas[600.0]["delta"] == pytest.approx(50.0)   # 150 − 100
    assert deltas[605.0]["delta"] == pytest.approx(50.0)   # 50 − 0 (new strike)


def test_diff_rejects_identical_timestamps(monkeypatch):
    app, _ = _build_app(monkeypatch)
    ts = "2026-06-29T14:00:00Z"
    with TestClient(app) as client:
        r = client.get(f"/api/replay/diff?symbol=SPY&ts_a={ts}&ts_b={ts}")
    assert r.status_code == 422


def test_clip_endpoint_returns_503_v1_status(monkeypatch):
    """v1 doesn't ship the MP4 worker — endpoint exists so clients can
    feature-detect, but must return 503 with a stable status string."""
    app, _ = _build_app(monkeypatch)
    with TestClient(app) as client:
        r = client.post("/api/replay/clip")
    assert r.status_code == 503
    body = r.json()
    detail = body.get("detail")
    assert isinstance(detail, dict)
    assert detail["status"] == "not_implemented_v1"
    assert "MP4" in detail["message"]
