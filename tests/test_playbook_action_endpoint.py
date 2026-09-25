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


# (closes, lows, highs), oldest -> newest, as get_recent_underlying_bars returns.
_BARS = ([678.1, 678.3, 678.4], [678.0, 678.2, 678.3], [678.2, 678.4, 678.5])

# When the request arrives: 30 seconds into the score row's 14:30 ET bar.
_NOW = datetime(2026, 5, 1, 18, 30, 30, tzinfo=timezone.utc)


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
    # Learning loop inputs: no live idea per pattern, nothing graded yet.
    dbmod.DatabaseManager.get_open_playbook_ideas = AsyncMock(return_value=[])
    dbmod.DatabaseManager.get_playbook_track_records = AsyncMock(return_value=[])
    # The newest bar's close is the Card's price (678.40, a touch above the
    # 677.80 VWAP the confluence signal reports). Three bars keep call_wall_fade's
    # realized-vol read at zero, as before.
    dbmod.DatabaseManager.get_recent_underlying_bars = AsyncMock(return_value=_BARS)
    from src.api.main import app  # noqa: E402
    from src.api.routers import trade_signals  # noqa: E402

    # The route checks the wall clock against the session and the newest bar's
    # age. Pin it 30 seconds past the fixtures' 14:30 ET bar; tests override it.
    monkeypatch.setattr(trade_signals, "_now", lambda: _NOW)

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


def _bulk_stub(adv=None, basic=None):
    """Adapt per-name signal stubs to ``get_component_signals_bulk``.

    ``build_playbook_context`` asks for every component in ONE query now, so
    these tests keep describing the board the way they always did — which name
    yields which row — and this assembles the ``{name: row}`` mapping the bulk
    reader returns. A name with no row is absent from the mapping, which is
    what the real query does and what ``signal_rows.get(name)`` expects.
    """

    async def _bulk(symbol, names):
        out = {}
        for name in names:
            row = await adv(symbol, name) if adv is not None else None
            if row is None and basic is not None:
                row = await basic(symbol, name)
            if row is not None:
                out[name] = row
        return out

    return _bulk


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
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(return_value={})

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


async def _cwf_adv(symbol, name):
    """Advanced signals for a board where call_wall_fade triggers."""
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


async def _cwf_basic(symbol, name):
    """Basic signals for a board where call_wall_fade triggers."""
    if name == "tape_flow_bias":
        return _tape_signal_row(-50.0)
    if name == "positioning_trap":
        return {"clamped_score": -0.30, "score": -30.0, "context_values": {}}
    if name == "vanna_charm_flow":
        return {"clamped_score": -0.20, "score": -20.0, "context_values": {}}
    if name == "dealer_delta_pressure":
        return {"clamped_score": -0.10, "score": -10.0, "context_values": {}}
    return None


def test_action_endpoint_returns_trade_card_when_call_wall_fade_triggers(
    monkeypatch: pytest.MonkeyPatch,
):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(
        side_effect=_bulk_stub(adv=_cwf_adv, basic=_cwf_basic)
    )

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


def test_action_card_prices_off_the_latest_bar_not_vwap(monkeypatch: pytest.MonkeyPatch):
    """Card #11418: this path quoted the $768.99 VWAP as SPY's price.

    The confluence signal reports VWAP 677.80 and publishes no close; the
    newest bar closed at 678.40. The Card must carry 678.40.
    """
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(
        side_effect=_bulk_stub(adv=_cwf_adv, basic=_cwf_basic)
    )

    with TestClient(app) as client:
        body = client.get("/api/signals/action?underlying=SPY").json()
    assert body["pattern"] == "call_wall_fade"
    assert body["entry"]["ref_price"] == 678.4
    # The bars end at the Card's timestamp, not whenever the request arrived.
    bars_call = dbmod.DatabaseManager.get_recent_underlying_bars.call_args
    assert bars_call.kwargs["as_of"] == datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc)


def test_no_bars_means_no_trade_card(monkeypatch: pytest.MonkeyPatch):
    """Without a real price there is no Card. VWAP is never a stand-in."""
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(
        side_effect=_bulk_stub(adv=_cwf_adv, basic=_cwf_basic)
    )
    dbmod.DatabaseManager.get_recent_underlying_bars = AsyncMock(return_value=([], [], []))

    with TestClient(app) as client:
        body = client.get("/api/signals/action?underlying=SPY").json()
    assert body["action"] == "STAND_DOWN"


def test_no_trade_card_off_the_last_bar_after_the_close(monkeypatch: pytest.MonkeyPatch):
    """A cash index's newest bar stays at 15:59 after the close, so the bar
    alone said the session was open: an NDX Card stamped 15:59 went out after
    4 PM. A request at 16:15 ET gets a Stand Down."""
    app, dbmod = _build_app(monkeypatch)
    # Imported after _build_app, which reloads the API modules the app runs on.
    from src.api.routers import trade_signals

    last_bar = {**_score_row(), "timestamp": datetime(2026, 5, 1, 19, 59, tzinfo=timezone.utc)}
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=last_bar)
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(
        side_effect=_bulk_stub(adv=_cwf_adv, basic=_cwf_basic)
    )
    after_close = datetime(2026, 5, 1, 20, 15, tzinfo=timezone.utc)  # 16:15 EDT
    monkeypatch.setattr(trade_signals, "_now", lambda: after_close)

    with TestClient(app) as client:
        body = client.get("/api/signals/action?underlying=SPY").json()
    assert body["action"] == "STAND_DOWN"
    assert body["rationale"].startswith("Market closed")
    # Only the Stand Down reaches the writer, which never stores one.
    assert dbmod.DatabaseManager.insert_action_card.call_args.args[0]["action"] == "STAND_DOWN"


def test_no_trade_card_off_a_stale_bar(monkeypatch: pytest.MonkeyPatch):
    """Mid-session, a bar ten minutes old means the feed has stalled; its
    price is no longer the market's."""
    app, dbmod = _build_app(monkeypatch)
    # Imported after _build_app, which reloads the API modules the app runs on.
    from src.api.routers import trade_signals

    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(
        side_effect=_bulk_stub(adv=_cwf_adv, basic=_cwf_basic)
    )
    ten_minutes_on = datetime(2026, 5, 1, 18, 40, tzinfo=timezone.utc)  # 14:40 EDT
    monkeypatch.setattr(trade_signals, "_now", lambda: ten_minutes_on)

    with TestClient(app) as client:
        body = client.get("/api/signals/action?underlying=SPY").json()
    assert body["action"] == "STAND_DOWN"
    assert body["rationale"].startswith("Stale data")
    # Only the Stand Down reaches the writer, which never stores one.
    assert dbmod.DatabaseManager.insert_action_card.call_args.args[0]["action"] == "STAND_DOWN"


def test_a_live_idea_blocks_a_repeat_card(monkeypatch: pytest.MonkeyPatch):
    """The board still says "fade the call wall", but the pattern's Card from
    20 minutes ago is still inside its hold window: no second Card."""
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(
        side_effect=_bulk_stub(adv=_cwf_adv, basic=_cwf_basic)
    )
    dbmod.DatabaseManager.get_open_playbook_ideas = AsyncMock(
        return_value=[
            {
                "pattern": "call_wall_fade",
                "direction": "bearish",
                "action": "SELL_CALL_SPREAD",
                "issued_at": datetime(2026, 5, 1, 18, 10, tzinfo=timezone.utc),
                "max_hold": "90",
                "outcome": None,
            }
        ]
    )

    with TestClient(app) as client:
        body = client.get("/api/signals/action?underlying=SPY").json()
    assert body["action"] == "STAND_DOWN"
    misses = {nm["pattern"]: nm["missing"] for nm in body["near_misses"]}
    assert "one Card per idea" in misses["call_wall_fade"][0]
    dbmod.DatabaseManager.get_open_playbook_ideas.assert_awaited_with("SPY")


def test_a_pattern_paused_on_the_symbol_issues_no_card(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(
        side_effect=_bulk_stub(adv=_cwf_adv, basic=_cwf_basic)
    )
    dbmod.DatabaseManager.get_playbook_track_records = AsyncMock(
        return_value=[
            {
                "pattern": "call_wall_fade",
                "underlying": "SPY",
                "direction": "bearish",
                "n": 40,
                "weight": 40.0,
                "wins": 10,
                "losses": 30,
                "sum_wr": -20.0,
            }
        ]
    )

    with TestClient(app) as client:
        body = client.get("/api/signals/action?underlying=SPY").json()
    assert body["action"] == "STAND_DOWN"
    misses = {nm["pattern"]: nm["missing"] for nm in body["near_misses"]}
    assert misses["call_wall_fade"][0].startswith("paused")
    dbmod.DatabaseManager.insert_action_card.assert_called_once()
    assert dbmod.DatabaseManager.insert_action_card.call_args.args[0]["action"] == "STAND_DOWN"


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
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(return_value={})

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

    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(
        side_effect=_bulk_stub(adv=_adv, basic=_basic)
    )

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


# --- request cost --------------------------------------------------------
#
# The Action Card was the slowest endpoint on the API: it rebuilt an identical
# Card on every poll, and built it from one query per signal. These pin both
# halves of that fix, because both are invisible in the response body — a
# regression would look perfectly correct and simply be slow again.


def test_action_card_is_cached_between_requests(monkeypatch: pytest.MonkeyPatch):
    """A Card only advances on the 60s analytics cycle, so a second poll
    inside the TTL must be served without rebuilding it."""
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(return_value={})

    with TestClient(app) as client:
        first = client.get("/api/signals/action?underlying=SPY")
        second = client.get("/api/signals/action?underlying=SPY")

    assert first.status_code == 200 and second.status_code == 200
    assert first.json() == second.json()
    assert dbmod.DatabaseManager.get_latest_signal_score.await_count == 1
    assert dbmod.DatabaseManager.get_component_signals_bulk.await_count == 1


def test_action_card_cache_does_not_cross_symbols(monkeypatch: pytest.MonkeyPatch):
    """A cache keyed too loosely would serve SPY's Card on a QQQ chart."""
    app, dbmod = _build_app(monkeypatch)

    async def _score(symbol, *args, **kwargs):
        row = _score_row()
        row["underlying"] = symbol
        return row

    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(side_effect=_score)
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(return_value={})

    with TestClient(app) as client:
        spy = client.get("/api/signals/action?underlying=SPY").json()
        qqq = client.get("/api/signals/action?underlying=QQQ").json()

    assert spy["underlying"] == "SPY"
    assert qqq["underlying"] == "QQQ"
    assert dbmod.DatabaseManager.get_latest_signal_score.await_count == 2


def test_action_card_cache_can_be_disabled(monkeypatch: pytest.MonkeyPatch):
    """TTL 0 turns it off, so an operator can rule the cache out during an
    incident without shipping code."""
    monkeypatch.setenv("ACTION_CARD_CACHE_TTL_SECONDS", "0")
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(return_value={})

    with TestClient(app) as client:
        client.get("/api/signals/action?underlying=SPY")
        client.get("/api/signals/action?underlying=SPY")

    assert dbmod.DatabaseManager.get_latest_signal_score.await_count == 2


def test_context_build_reads_every_signal_in_one_query(monkeypatch: pytest.MonkeyPatch):
    """One bulk read for the whole board, not one per component. The per-name
    readers must not be reached at all — each also fetches a score history the
    playbook never looks at."""
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_latest_signal_score = AsyncMock(return_value=_score_row())
    dbmod.DatabaseManager.get_component_signals_bulk = AsyncMock(return_value={})
    dbmod.DatabaseManager.get_advanced_signal = AsyncMock(return_value=None)
    dbmod.DatabaseManager.get_basic_signal = AsyncMock(return_value=None)

    with TestClient(app) as client:
        assert client.get("/api/signals/action?underlying=SPY").status_code == 200

    assert dbmod.DatabaseManager.get_component_signals_bulk.await_count == 1
    dbmod.DatabaseManager.get_advanced_signal.assert_not_awaited()
    dbmod.DatabaseManager.get_basic_signal.assert_not_awaited()

    # And it asked for the whole board in that one call.
    _, names = dbmod.DatabaseManager.get_component_signals_bulk.await_args.args
    assert "trap_detection" in names and "tape_flow_bias" in names
    assert len(names) >= 13
