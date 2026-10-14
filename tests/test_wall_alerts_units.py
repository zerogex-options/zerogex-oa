"""Wall alerts: the pure pieces and the API's wiring, without a database.

The SQL itself is proven against a real Postgres in
``tests/test_wall_alerts_db.py`` (integration). This file pins what can be
pinned without one: what an alert says (payload, tiers, regime, odds), who
receives it (target selection), what the API accepts, and how the routes are
mounted — in particular that the outbox is admin-gated and never mirrored to
``/api/v2``.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from src.alerts.wall_detector import ET, WallEvent
from src.alerts.wall_payload import (
    build_payload,
    regime_for,
    reportable_odds,
    strength_tier,
)
from src.alerts.wall_store import select_targets


def _event(**kw) -> WallEvent:
    ts = datetime(2026, 10, 13, 10, 15, tzinfo=ET)
    base = dict(
        symbol="SPX",
        profile="balanced",
        side="put",
        kind="hit",
        wall=7700.0,
        ordinal=2,
        session_date=date(2026, 10, 13),
        bar_ts=ts,
        price=7702.5,
        last_price=7703.0,
        hit_ts=ts,
        hit_price=7702.5,
        strength=3.1e9,
        walls_ts=ts,
        break_buffer=7.7,
        touch_band=3.85,
        hold_distance=11.55,
        confirm_minutes=5,
        minutes_since_hit=0.0,
        peak=7702.5,
        alerted_hit=True,
        approach_number=2,
    )
    base.update(kw)
    return WallEvent(**base)


# --------------------------------------------------------------------------
# Payload
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pctile,tier",
    [
        (None, None),
        (0.0, "light"),
        (24.9, "light"),
        (25.0, "moderate"),
        (50.0, "above_typical"),
        (74.9, "above_typical"),
        (75.0, "strong"),
        (90.0, "very_strong"),
        (100.0, "very_strong"),
    ],
)
def test_strength_tiers_follow_the_slider_stops(pctile, tier):
    assert strength_tier(pctile) == tier


def test_regime_is_the_sign_of_dealer_gamma():
    assert regime_for(2e9) == "positive"
    assert regime_for(-1.0) == "negative"
    assert regime_for(0.0) is None
    assert regime_for(None) is None


def test_payload_carries_everything_a_channel_renders():
    payload = build_payload(
        _event(),
        strength_pctile=88.24,
        net_gex_at_spot=-5e8,
        current_wall=7700.0,
        odds=[{"horizon_minutes": 60, "break_prob": 0.34}],
    )
    assert payload["v"] == 1
    assert payload["symbol"] == "SPX" and payload["side"] == "put" and payload["kind"] == "hit"
    # Put wall below: 7702.5 is still 2.5 above it, on the approach side.
    assert payload["distance"] == pytest.approx(2.5)
    assert payload["strength_pctile"] == 88.2
    assert payload["strength_tier"] == "strong"
    assert payload["regime"] == "negative"
    assert payload["approach_number"] == 2
    assert payload["odds"] == [{"horizon_minutes": 60, "break_prob": 0.34}]
    assert payload["bar_ts"].startswith("2026-10-13T10:15")


def test_call_distance_is_signed_toward_the_approach_side():
    payload = build_payload(
        _event(side="call", wall=7720.0, price=7722.0),
        strength_pctile=None,
        net_gex_at_spot=None,
        current_wall=None,
    )
    # 2 points PAST a call wall reads negative.
    assert payload["distance"] == pytest.approx(-2.0)
    assert payload["strength_tier"] is None


def test_only_reportable_odds_at_requested_horizons_are_quoted():
    rows = [
        {"horizon_minutes": 15, "break_prob": 0.1, "reportable": True},
        {
            "horizon_minutes": 30,
            "break_prob": 0.153,
            "ci_low": 0.11,
            "ci_high": 0.21,
            "n_tests": 234,
            "reportable": True,
        },
        {"horizon_minutes": 60, "break_prob": 0.344, "reportable": False},
        {"horizon_minutes": 45, "break_prob": None, "reportable": True},
    ]
    out = reportable_odds(rows, (30, 60))
    assert out == [
        {
            "horizon_minutes": 30,
            "break_prob": 0.153,
            "ci_low": 0.11,
            "ci_high": 0.21,
            "n_tests": 234,
        }
    ]


# --------------------------------------------------------------------------
# Who receives an event
# --------------------------------------------------------------------------


def test_targets_dedupe_per_member_and_channel_and_honor_the_floor():
    rules = [
        (1, "alice", {"in_app": True, "email": True}, 0),
        (2, "alice", {"in_app": True}, 0),  # second matching rule: no duplicate rows
        (3, "bob", {"in_app": True}, 90),  # event is p80: filtered
        (4, "carol", '{"email": true}', 50),  # JSON text is accepted too
        (5, "dave", {"in_app": True, "sms": True}, 0),  # sms not deliverable
    ]
    targets, filtered, capped = select_targets(
        rules,
        strength_pctile=80.0,
        used_today={},
        daily_cap=10,
        channels=("in_app", "email"),
    )
    assert targets == [
        (1, "alice", "in_app"),
        (1, "alice", "email"),
        (4, "carol", "email"),
        (5, "dave", "in_app"),
    ]
    assert filtered == 1 and capped == []


def test_unknown_strength_only_reaches_rules_without_a_floor():
    rules = [(1, "alice", {"in_app": True}, 0), (2, "bob", {"in_app": True}, 25)]
    targets, filtered, _ = select_targets(
        rules, strength_pctile=None, used_today={}, daily_cap=10, channels=("in_app",)
    )
    assert targets == [(1, "alice", "in_app")] and filtered == 1


def test_the_daily_cap_is_per_member():
    rules = [(1, "alice", {"in_app": True}, 0), (2, "bob", {"in_app": True}, 0)]
    targets, _, capped = select_targets(
        rules, strength_pctile=None, used_today={"alice": 3}, daily_cap=3, channels=("in_app",)
    )
    assert targets == [(2, "bob", "in_app")] and capped == ["alice"]


# --------------------------------------------------------------------------
# Rule validation
# --------------------------------------------------------------------------


def _validate(body, partial=False):
    from src.api.routers.wall_alerts import validate_rule

    return validate_rule(body, partial=partial)


def test_a_minimal_rule_gets_the_defaults():
    rule = _validate({"symbol": "spx", "side": "PUT"})
    assert rule == {
        "symbol": "SPX",
        "side": "put",
        "events": {"hit": True, "break": True, "hold": False},
        "min_strength_pctile": 0,
        "profile": "balanced",
        "channels": {"in_app": True, "email": False, "sms": False},
        "enabled": True,
    }


@pytest.mark.parametrize(
    "body,needle",
    [
        ({"symbol": "NQ", "side": "call"}, "SPX / NDX"),
        ({"symbol": "TSLA", "side": "call"}, "symbol must be one of"),
        ({"symbol": "SPY", "side": "up"}, "side"),
        ({"symbol": "SPY", "side": "call", "events": {"touch": True}}, "unknown event"),
        (
            {"symbol": "SPY", "side": "call", "events": {"hit": False, "break": False}},
            "at least one event",
        ),
        ({"symbol": "SPY", "side": "call", "channels": {"pager": True}}, "unknown channel"),
        ({"symbol": "SPY", "side": "call", "channels": {"sms": True}}, "not available"),
        ({"symbol": "SPY", "side": "call", "min_strength_pctile": "high"}, "number"),
        ({"symbol": "SPY", "side": "call", "min_strength_pctile": -5}, "0 to 95"),
        ({"symbol": "SPY", "side": "call", "enabled": "yes"}, "true or false"),
    ],
)
def test_bad_rules_are_refused_with_a_reason(body, needle):
    with pytest.raises(HTTPException) as exc:
        _validate(body)
    assert exc.value.status_code == 422
    assert needle in exc.value.detail


def test_sms_is_accepted_once_the_flag_is_on(monkeypatch):
    from src.alerts import config as alerts_config

    monkeypatch.setattr(alerts_config, "SMS_ENABLED", True)
    rule = _validate({"symbol": "SPY", "side": "call", "channels": {"sms": True}})
    assert rule["channels"]["sms"] is True


def test_a_patch_validates_only_what_it_carries():
    assert _validate({"enabled": False}, partial=True) == {"enabled": False}
    assert _validate({"min_strength_pctile": 74.6}, partial=True) == {"min_strength_pctile": 75}
    with pytest.raises(HTTPException):
        _validate({}, partial=True)


# --------------------------------------------------------------------------
# Route wiring
# --------------------------------------------------------------------------


@pytest.fixture()
def app_module(monkeypatch):
    for name in (
        "API_KEY",
        "ENVIRONMENT",
        "CORS_ALLOW_ORIGINS",
        "END_USER_TOKEN_SECRET",
        "KEY_ADMIN_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("API_KEY", "static-key")
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("KEY_ADMIN_TOKEN", "admin-secret")
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    from src.api import main as main_mod

    yield main_mod
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)


def _mounted_paths(app) -> set:
    """Every mounted path — FastAPI >=0.140 keeps included routers behind
    ``effective_route_contexts()`` instead of flattening them into
    ``app.routes`` (same walk as tests/test_market_data_scope_boundary.py)."""
    paths = set()
    for route in list(app.routes):
        contexts = getattr(route, "effective_route_contexts", None)
        if contexts is None:
            paths.add(getattr(route, "path", ""))
            continue
        paths.update(ctx.path for ctx in contexts())
    return paths


def test_routes_are_mounted_and_the_outbox_is_not_mirrored_to_v2(app_module):
    paths = _mounted_paths(app_module.app)
    for path in (
        "/api/alerts/walls/config",
        "/api/alerts/walls/rules",
        "/api/alerts/walls/rules/{rule_id}",
        "/api/alerts/walls/feed",
        "/api/alerts/walls/feed/read",
        "/api/alerts/walls/live",
        "/api/alerts/walls/internal/queued",
        "/api/alerts/walls/internal/mark",
        "/api/alerts/walls/internal/disable-channel",
        "/api/alerts/walls/internal/purge-user",
    ):
        assert path in paths, path
    assert "/api/v2/alerts/walls/rules" in paths
    assert not any(p.startswith("/api/v2/alerts/walls/internal") for p in paths)


def test_the_outbox_refuses_an_api_key_without_the_admin_token(app_module):
    from fastapi.testclient import TestClient

    with TestClient(app_module.app) as client:
        headers = {"Authorization": "Bearer static-key"}
        for method, path in (
            ("get", "/api/alerts/walls/internal/queued?channel=email"),
            ("post", "/api/alerts/walls/internal/mark"),
            ("post", "/api/alerts/walls/internal/disable-channel"),
            ("post", "/api/alerts/walls/internal/purge-user"),
        ):
            res = getattr(client, method)(
                path, headers=headers, **({"json": {}} if method == "post" else {})
            )
            assert res.status_code == 403, (path, res.status_code, res.text)


def test_member_writes_need_a_signed_in_member(app_module):
    from fastapi.testclient import TestClient

    with TestClient(app_module.app) as client:
        headers = {"Authorization": "Bearer static-key"}
        assert (
            client.post(
                "/api/alerts/walls/rules", json={"symbol": "SPY", "side": "call"}, headers=headers
            ).status_code
            == 401
        )
        assert client.delete("/api/alerts/walls/feed", headers=headers).status_code == 401
        assert (
            client.post("/api/alerts/walls/feed/read", json={}, headers=headers).status_code == 401
        )


def test_config_is_served_without_a_database(app_module):
    from fastapi.testclient import TestClient

    with TestClient(app_module.app) as client:
        cfg = client.get(
            "/api/alerts/walls/config", headers={"Authorization": "Bearer static-key"}
        ).json()
    assert cfg["symbols"] == ["SPY", "QQQ", "SPX", "NDX"]
    assert cfg["default_profile"] == "balanced"
    assert {c["id"]: c["available"] for c in cfg["channels"]} == {
        "in_app": True,
        "email": True,
        "sms": False,
    }
    assert cfg["max_strength_floor"] == 95
