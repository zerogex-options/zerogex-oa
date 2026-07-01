"""Integration test for GET /api/signals/action.

Mocks the DatabaseManager so no live Postgres is needed.  Verifies the
endpoint:

  * Returns 404 when no signal_score row exists.
  * Returns a STAND_DOWN Card when patterns can't match.
  * Returns a populated trade Card when triggers align.
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
    # Pop both the API surface and the entire playbook subtree.  Pattern
    # modules cache their PATTERN instances against the *currently loaded*
    # PatternBase class; if we reload PatternBase but leave pattern modules
    # cached, the post-reload isinstance() check fails and the engine
    # silently drops every pattern.  Popping the patterns submodules forces
    # a clean re-import on the next PlaybookEngine() call.
    for mod in list(sys.modules):
        if (
            mod.startswith("src.api")
            or mod.startswith("src.signals.playbook")
            or mod == "src.signals.playbook"
        ):
            sys.modules.pop(mod, None)
    from src.api import database as dbmod  # noqa: E402

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)
    # PR-3 persistence: stub by default; tests override per-case.  The returned
    # value is the persisted row id (None for STAND_DOWN and DB failures); the
    # /action handler attaches it to the response payload as ``id`` so the
    # live UI can deep-link to /cards/{id}.
    dbmod.DatabaseManager.insert_action_card = AsyncMock(return_value=4221)
    dbmod.DatabaseManager.get_recent_action_cards = AsyncMock(return_value=[])
    from src.api.main import app  # noqa: E402

    return app, dbmod


def _no_score_row():
    return None


def _score_row(net_gex: float = 7.1e9):
    """Return a minimal signal_scores row plausible enough for context build."""
    return {
        "underlying": "SPY",
        "timestamp": datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc),
        "composite_score": 0.0,
        "normalized_score": 0.0,
        "direction": "high_risk_reversal",
        "components": {
            "net_gex_sign": {
                "max_points": 16,
                "contribution": -16,
                "score": -1.0,
                "context": {"net_gex": net_gex, "score": -1.0, "max_points": 20},
            },
            "order_flow_imbalance": {
                "max_points": 19,
                "contribution": -19,
                "score": -1.0,
                "context": {
                    "smart_call_premium": -765000.0,
                    "smart_put_premium": -134000.0,
                },
            },
            "put_call_ratio": {
                "max_points": 12,
                "contribution": -12,
                "score": -1.0,
                "context": {"put_call_ratio": 0.36},
            },
            "volatility_regime": {
                "max_points": 6,
                "contribution": -1.96,
                "score": -0.326,
                "context": {"vix_level": 16.7},
            },
            "dealer_delta_pressure": {
                "max_points": 17,
                "contribution": 0.68,
                "score": 0.04,
                "context": {"dealer_net_delta_estimated": -12_000_000.0},
            },
        },
    }


def _gvc_signal_row(call_wall=678.0, max_pain=675.0, gamma_flip=676.5, vwap=677.8):
    """Mock the gamma_vwap_confluence advanced signal row, with bearish trigger."""
    return {
        "clamped_score": -0.30,
        "score": -30.0,
        "direction": "bearish",
        "context_values": {
            "triggered": True,
            "signal": "bearish_confluence",
            "call_wall": call_wall,
            "max_pain": max_pain,
            "gamma_flip": gamma_flip,
            "vwap": vwap,
            "max_gamma": call_wall,  # near the wall
            "close": 678.4,
        },
    }


def _trap_signal_row():
    return {
        "clamped_score": -0.35,
        "score": -35.0,
        "direction": "bearish",
        "context_values": {"triggered": True, "signal": "bearish_fade"},
    }


def _tape_signal_row(score=-50.0):
    return {
        "clamped_score": score / 100.0,
        "score": score,
        "direction": "bearish",
        "context_values": {},
    }


def _empty_signal_row():
    return None


def test_action_endpoint_returns_404_when_no_score_row(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=None)
    with TestClient(app) as client:
        r = client.get("/api/signals/action?underlying=SPY")
    assert r.status_code == 404


def test_action_endpoint_returns_stand_down_when_triggers_unmet(
    monkeypatch: pytest.MonkeyPatch,
):
    """call_wall_fade can't match because no advanced signal corroborates."""
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_advanced_signal = AsyncMock(return_value=_empty_signal_row())
    dbmod.DatabaseManager.get_basic_signal = AsyncMock(return_value=_empty_signal_row())

    with TestClient(app) as client:
        r = client.get("/api/signals/action?underlying=SPY")
    assert r.status_code == 200
    body = r.json()
    assert body["action"] == "STAND_DOWN"
    assert body["pattern"] == "stand_down"
    assert body["confidence"] == 0.0
    # Trade fields stripped on STAND_DOWN.
    assert "legs" not in body
    assert "near_misses" in body


def test_action_endpoint_returns_trade_card_when_call_wall_fade_triggers(
    monkeypatch: pytest.MonkeyPatch,
):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())

    async def _adv(symbol, name):
        if name == "trap_detection":
            return _trap_signal_row()
        if name == "gamma_vwap_confluence":
            return _gvc_signal_row()
        if name == "range_break_imminence":
            return {
                "clamped_score": 0.10,
                "score": 10.0,
                "context_values": {"label": "Range Fade"},
            }
        return None

    async def _basic(symbol, name):
        if name == "tape_flow_bias":
            return _tape_signal_row(-50.0)
        if name == "positioning_trap":
            return {"clamped_score": -0.30, "score": -30.0, "context_values": {}}
        if name == "vanna_charm_flow":
            return {"clamped_score": -0.20, "score": -20.0, "context_values": {}}
        if name == "dealer_delta_pressure":
            return {"clamped_score": -0.10, "score": -10.0, "context_values": {}}
        return None

    dbmod.DatabaseManager.get_advanced_signal = AsyncMock(side_effect=_adv)
    dbmod.DatabaseManager.get_basic_signal = AsyncMock(side_effect=_basic)

    with TestClient(app) as client:
        r = client.get("/api/signals/action?underlying=SPY")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["pattern"] == "call_wall_fade"
    assert body["action"] in ("SELL_CALL_SPREAD", "BUY_PUT_DEBIT")
    assert body["tier"] == "0DTE"
    assert body["direction"] == "bearish"
    assert 0.20 <= body["confidence"] <= 0.95
    assert "legs" in body and len(body["legs"]) >= 1
    assert body["target"]["level_name"] in ("max_pain", "gamma_flip")
    assert body["context"]["call_wall"] == 678.0
    assert "trap_detection" in body["context"]["advanced_signals_aligned"]
    # Trade Card persistence (PR-3): the endpoint must call insert_action_card.
    dbmod.DatabaseManager.insert_action_card.assert_called_once()
    persisted_payload = dbmod.DatabaseManager.insert_action_card.call_args.args[0]
    assert persisted_payload["pattern"] == "call_wall_fade"
    assert persisted_payload["action"] != "STAND_DOWN"
    # Phase 1 permalink wiring: the row id returned by insert_action_card
    # must be attached to the response payload as ``id`` so the live UI
    # can render a /cards/{id} deep-link.
    assert body["id"] == 4221


# --------------------------------------------------------------------------
# PR-3 persistence + hysteresis
# --------------------------------------------------------------------------


def test_stand_down_card_is_not_persisted(monkeypatch: pytest.MonkeyPatch):
    """STAND_DOWN must not pollute signal_action_cards."""
    app, dbmod = _build_app(monkeypatch)
    # Override the default to mirror the real impl: STAND_DOWN short-circuits
    # internally and returns None, so the response must not carry an ``id``.
    dbmod.DatabaseManager.insert_action_card = AsyncMock(return_value=None)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_advanced_signal = AsyncMock(return_value=None)
    dbmod.DatabaseManager.get_basic_signal = AsyncMock(return_value=None)

    with TestClient(app) as client:
        r = client.get("/api/signals/action?underlying=SPY")
    assert r.status_code == 200
    body = r.json()
    assert body["action"] == "STAND_DOWN"
    # STAND_DOWN must not carry a persisted id — it isn't shareable.
    assert "id" not in body
    # insert_action_card is called, but it short-circuits internally for
    # STAND_DOWN — assert the impl-level guard via the payload it received.
    assert dbmod.DatabaseManager.insert_action_card.call_count == 1
    payload = dbmod.DatabaseManager.insert_action_card.call_args.args[0]
    assert payload["action"] == "STAND_DOWN"


# --------------------------------------------------------------------------
# /action/{card_id} permalink + /action/recent feed (Phase 1: Action Card
# permalinks + OG images).  Both endpoints back the public /cards/{id} page.
# --------------------------------------------------------------------------


def test_action_by_id_returns_404_for_missing_card(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_action_card_by_id = AsyncMock(return_value=None)
    with TestClient(app) as client:
        r = client.get("/api/signals/action/99999")
    assert r.status_code == 404
    assert "99999" in r.json()["detail"]


def test_action_by_id_returns_404_for_nonpositive_id(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    # Guard runs before DB lookup; should not be queried.
    dbmod.DatabaseManager.get_action_card_by_id = AsyncMock(return_value={"id": 0})
    with TestClient(app) as client:
        r = client.get("/api/signals/action/0")
    assert r.status_code == 404
    dbmod.DatabaseManager.get_action_card_by_id.assert_not_called()


def test_action_by_id_returns_full_payload(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    sample = {
        "id": 4221,
        "underlying": "SPY",
        "timestamp": "2026-05-01T18:42:13+00:00",
        "pattern": "call_wall_fade",
        "action": "SELL_CALL_SPREAD",
        "tier": "0DTE",
        "direction": "bearish",
        "confidence": 0.68,
        "rationale": "Price pinned at call wall ...",
        "legs": [
            {"expiry": "2026-05-01", "strike": 678.0, "right": "C", "side": "SELL", "qty": 1},
        ],
        "entry": {"ref_price": 678.40, "trigger": "at_touch"},
        "target": {"ref_price": 675.00, "kind": "level", "level_name": "max_pain"},
        "stop": {"ref_price": 680.03, "kind": "premium_pct", "level_name": "call_wall_break"},
        "created_at": "2026-05-01T18:42:14+00:00",
    }
    dbmod.DatabaseManager.get_action_card_by_id = AsyncMock(return_value=sample)
    with TestClient(app) as client:
        r = client.get("/api/signals/action/4221")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["id"] == 4221
    assert body["pattern"] == "call_wall_fade"
    assert body["entry"]["ref_price"] == 678.40
    assert len(body["legs"]) == 1


def test_action_recent_returns_chronological_with_permalinks(
    monkeypatch: pytest.MonkeyPatch,
):
    app, dbmod = _build_app(monkeypatch)
    rows = [
        {
            "id": 4222,
            "underlying": "SPY",
            "timestamp": "2026-05-01T18:42:13+00:00",
            "pattern": "call_wall_fade",
            "action": "SELL_CALL_SPREAD",
            "tier": "0DTE",
            "direction": "bearish",
            "confidence": 0.68,
            "rationale": "Pinned at call wall",
            "created_at": "2026-05-01T18:42:14+00:00",
        },
        {
            "id": 4221,
            "underlying": "SPY",
            "timestamp": "2026-05-01T17:35:00+00:00",
            "pattern": "put_wall_bounce",
            "action": "BUY_CALL_DEBIT",
            "tier": "intraday",
            "direction": "bullish",
            "confidence": 0.54,
            "rationale": "Bouncing off put wall",
            "created_at": "2026-05-01T17:35:01+00:00",
        },
    ]
    dbmod.DatabaseManager.get_action_cards_chronological = AsyncMock(return_value=rows)
    with TestClient(app) as client:
        r = client.get("/api/signals/action/recent?underlying=SPY&limit=10")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 2
    assert [c["id"] for c in body["cards"]] == [4222, 4221]
    assert body["cards"][0]["permalink"] == "/cards/4222"
    # underlying was uppercased before being passed to the query layer
    args, kwargs = dbmod.DatabaseManager.get_action_cards_chronological.call_args
    assert kwargs.get("underlying") == "SPY"
    assert kwargs.get("limit") == 10


def test_action_recent_rejects_out_of_range_limit(monkeypatch: pytest.MonkeyPatch):
    app, _ = _build_app(monkeypatch)
    with TestClient(app) as client:
        r = client.get("/api/signals/action/recent?limit=9999")
    assert r.status_code == 422


def test_recently_emitted_blocks_re_emission_via_hysteresis(monkeypatch: pytest.MonkeyPatch):
    """If get_recent_action_cards returns a recent emission, hysteresis suppresses re-fire."""
    from datetime import timedelta

    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())

    async def _adv(symbol, name):
        if name == "trap_detection":
            return _trap_signal_row()
        if name == "gamma_vwap_confluence":
            return _gvc_signal_row()
        if name == "range_break_imminence":
            return {
                "clamped_score": 0.10,
                "score": 10.0,
                "context_values": {"label": "Range Fade"},
            }
        return None

    async def _basic(symbol, name):
        if name == "tape_flow_bias":
            return _tape_signal_row(-50.0)
        if name == "positioning_trap":
            return {"clamped_score": -0.30, "score": -30.0, "context_values": {}}
        return None

    dbmod.DatabaseManager.get_advanced_signal = AsyncMock(side_effect=_adv)
    dbmod.DatabaseManager.get_basic_signal = AsyncMock(side_effect=_basic)

    # Simulate call_wall_fade having fired 2 minutes ago — well inside the
    # 5-minute 0DTE dwell window.  The score row's timestamp is 2026-05-01
    # 18:30 UTC; recent emission is at 18:28 UTC.
    score_ts = _score_row()["timestamp"]
    recent_emit = score_ts - timedelta(minutes=2)
    dbmod.DatabaseManager.get_recent_action_cards = AsyncMock(
        return_value=[
            {
                "pattern": "call_wall_fade",
                "timestamp": recent_emit,
                "action": "SELL_CALL_SPREAD",
            }
        ]
    )

    with TestClient(app) as client:
        r = client.get("/api/signals/action?underlying=SPY")
    body = r.json()
    assert body["action"] == "STAND_DOWN", body
    assert any(
        nm["pattern"] == "call_wall_fade" and any("hysteresis" in m for m in nm["missing"])
        for nm in body["near_misses"]
    )
