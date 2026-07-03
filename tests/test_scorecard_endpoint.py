"""Integration tests for GET /api/scorecard/daily.

Stubs DatabaseManager.get_daily_scorecard so no live Postgres is needed;
verifies the route does its own ET-day → UTC bound conversion correctly,
labels the regime from the composite score, and synthesizes the canonical
``tweet_text`` field the OG image and cron job both rely on.
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
        if mod.startswith("src.api") or mod.startswith("src.signals.playbook"):
            sys.modules.pop(mod, None)
    from src.api import database as dbmod  # noqa: E402

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    from src.api.main import app  # noqa: E402

    return app, dbmod


def _scorecard_payload(**overrides):
    """Default payload the query layer would return for a real trading day."""
    base = {
        "symbol": "SPY",
        "window_start_utc": datetime(2026, 6, 29, 4, 0, tzinfo=timezone.utc),
        "window_end_utc": datetime(2026, 6, 30, 4, 0, tzinfo=timezone.utc),
        "horizon_minutes": 60,
        "cards": {
            "total": 12,
            "by_action": [
                {"action": "SELL_CALL_SPREAD", "count": 4},
                {"action": "BUY_PUT_DEBIT", "count": 3},
            ],
            "first_card_id": 4221,
        },
        "signals": {
            "events": [
                {"name": "squeeze_setup", "flips": 3, "scored": 3, "wins": 2, "losses": 1, "avg_directional_return": 0.0074},
                {"name": "vanna_charm_flow", "flips": 3, "scored": 3, "wins": 1, "losses": 2, "avg_directional_return": -0.0031},
            ],
            "best": {"name": "squeeze_setup", "flips": 3, "scored": 3, "wins": 2, "losses": 1, "avg_directional_return": 0.0074},
            "worst": {"name": "vanna_charm_flow", "flips": 3, "scored": 3, "wins": 1, "losses": 2, "avg_directional_return": -0.0031},
        },
        "regime": {
            "timestamp": datetime(2026, 6, 29, 20, 0, tzinfo=timezone.utc),
            "composite_score": -0.28,
            "normalized_score": -28.0,
            "direction": "bearish",
        },
    }
    base.update(overrides)
    return base


def test_daily_scorecard_returns_full_payload(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_daily_scorecard = AsyncMock(return_value=_scorecard_payload())

    with TestClient(app) as client:
        r = client.get("/api/scorecard/daily?date=2026-06-29&symbol=SPY")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["date"] == "2026-06-29"
    assert body["symbol"] == "SPY"
    assert body["tz"] == "America/New_York"
    assert body["is_empty"] is False
    assert body["cards"]["total"] == 12
    assert body["cards"]["first_card_permalink"] == "/cards/4221"
    assert body["regime"]["label"] == "short gamma"  # composite -0.28 → short
    # tweet_text wording must be deterministic enough for the cron job to
    # post verbatim without further templating.
    tweet = body["tweet_text"]
    assert tweet.startswith("SPY · 2026-06-29")
    assert "12 Playbook calls" in tweet
    assert "Best: Squeeze Setup +0.74%" in tweet
    # En-dash for negative percent — matches the typographic convention used
    # by the FinTwit accounts we're competing with.
    assert "Worst: Vanna Charm Flow −0.31%" in tweet
    assert "Regime: short gamma" in tweet


def test_daily_scorecard_passes_correct_utc_window(monkeypatch: pytest.MonkeyPatch):
    """ET-day 2026-06-29 → UTC 04:00 06-29 .. 04:00 06-30 (EDT, UTC-4)."""
    app, dbmod = _build_app(monkeypatch)
    captured: dict = {}

    async def _capture(**kwargs):
        captured.update(kwargs)
        return _scorecard_payload()

    dbmod.DatabaseManager.get_daily_scorecard = AsyncMock(side_effect=_capture)

    with TestClient(app) as client:
        r = client.get("/api/scorecard/daily?date=2026-06-29")
    assert r.status_code == 200
    assert captured["symbol"] == "SPY"
    # 2026-06-29 is in EDT (UTC-4) so local midnight is 04:00 UTC. The
    # route forwards the bounds to the query layer as ``start_utc`` and
    # ``end_utc`` — the query helper renames them internally.
    assert captured["start_utc"] == datetime(2026, 6, 29, 4, 0, tzinfo=timezone.utc)
    assert captured["end_utc"] == datetime(2026, 6, 30, 4, 0, tzinfo=timezone.utc)


def test_daily_scorecard_handles_dst_transition(monkeypatch: pytest.MonkeyPatch):
    """ET-day 2026-01-15 is in EST (UTC-5) so local midnight is 05:00 UTC."""
    app, dbmod = _build_app(monkeypatch)
    captured: dict = {}

    async def _capture(**kwargs):
        captured.update(kwargs)
        return _scorecard_payload()

    dbmod.DatabaseManager.get_daily_scorecard = AsyncMock(side_effect=_capture)

    with TestClient(app) as client:
        r = client.get("/api/scorecard/daily?date=2026-01-15")
    assert r.status_code == 200
    assert captured["start_utc"] == datetime(2026, 1, 15, 5, 0, tzinfo=timezone.utc)
    assert captured["end_utc"] == datetime(2026, 1, 16, 5, 0, tzinfo=timezone.utc)


def test_daily_scorecard_empty_day_returns_quiet_tweet(monkeypatch: pytest.MonkeyPatch):
    """No cards, no flips → is_empty=true and a "quiet tape" tweet."""
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_daily_scorecard = AsyncMock(return_value=_scorecard_payload(
        cards={"total": 0, "by_action": [], "first_card_id": None},
        signals={"events": [], "best": None, "worst": None},
        regime=None,
    ))

    with TestClient(app) as client:
        r = client.get("/api/scorecard/daily?date=2026-06-29")
    assert r.status_code == 200
    body = r.json()
    assert body["is_empty"] is True
    assert body["cards"]["first_card_permalink"] is None
    assert "quiet tape" in body["tweet_text"]
    # Quiet-tape tweets must NEVER end with empty trailing punctuation that
    # would look like a templating bug to a reader on X.
    assert not body["tweet_text"].endswith(" — ")


def test_daily_scorecard_regime_label_from_composite_score(monkeypatch: pytest.MonkeyPatch):
    """Strong positive composite → long gamma, near-zero → transition."""
    app, dbmod = _build_app(monkeypatch)

    # Long gamma.
    dbmod.DatabaseManager.get_daily_scorecard = AsyncMock(return_value=_scorecard_payload(
        regime={"timestamp": datetime(2026, 6, 29, 20, tzinfo=timezone.utc),
                "composite_score": 0.42, "normalized_score": 42.0, "direction": "bullish"},
    ))
    with TestClient(app) as client:
        r = client.get("/api/scorecard/daily?date=2026-06-29")
    assert r.json()["regime"]["label"] == "long gamma"

    # Transition (near zero).
    dbmod.DatabaseManager.get_daily_scorecard = AsyncMock(return_value=_scorecard_payload(
        regime={"timestamp": datetime(2026, 6, 29, 20, tzinfo=timezone.utc),
                "composite_score": 0.08, "normalized_score": 8.0, "direction": "bullish"},
    ))
    with TestClient(app) as client:
        r = client.get("/api/scorecard/daily?date=2026-06-29")
    assert r.json()["regime"]["label"] == "transition"


def test_daily_scorecard_rejects_invalid_date(monkeypatch: pytest.MonkeyPatch):
    app, _ = _build_app(monkeypatch)
    with TestClient(app) as client:
        r = client.get("/api/scorecard/daily?date=2026/06/29")
    assert r.status_code == 422


def test_daily_scorecard_rejects_out_of_range_date(monkeypatch: pytest.MonkeyPatch):
    app, _ = _build_app(monkeypatch)
    with TestClient(app) as client:
        r = client.get("/api/scorecard/daily?date=1999-01-01")
    assert r.status_code == 422


def test_daily_scorecard_best_only_no_worst(monkeypatch: pytest.MonkeyPatch):
    """When best == worst (single qualifying signal) the tweet must not
    repeat the same signal as both — keeps copy clean for low-volume days."""
    app, dbmod = _build_app(monkeypatch)
    only = {"name": "skew_delta", "flips": 2, "scored": 2, "wins": 2, "losses": 0,
            "avg_directional_return": 0.0050}
    dbmod.DatabaseManager.get_daily_scorecard = AsyncMock(return_value=_scorecard_payload(
        signals={"events": [only], "best": only, "worst": only},
    ))
    with TestClient(app) as client:
        r = client.get("/api/scorecard/daily?date=2026-06-29")
    tweet = r.json()["tweet_text"]
    assert "Best: Skew Delta +0.50%" in tweet
    assert "Worst:" not in tweet
