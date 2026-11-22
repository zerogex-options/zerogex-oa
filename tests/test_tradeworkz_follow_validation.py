"""Regression: a malformed follow body can't freeze a bot or the alert queue.

``POST /api/tradeworkz/bots/{id}/follow`` stored ``channels`` as whatever JSON
the caller sent. The engine's fan-out runs inside every open, scale-out and
close of the bot and assumed a dict of known names with true/false values, so
``{"channels": ["in_app"]}`` (or ``7``, or a key longer than the 16-character
channel column) made each of those raise and roll back: the bot stopped
repricing, stopping out and opening, for good. A string value such as
``{"email": "abc"}`` broke the delivery queue's ``::boolean`` cast for every
follower. One request per bot froze the fleet.

The endpoint now refuses those bodies with a 422, and the fan-out reads rows
written before the fix without raising.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_api_tradeworkz_unfollow import (  # noqa: E402
    _build_app,
    _FakeConn,
    _headers,
    _inject_pool,
    _mint,
)


def _validate(body):
    from src.api.routers.tradeworkz import _validate_follow_body

    return _validate_follow_body(body)


@pytest.mark.parametrize(
    "body",
    [
        {"channels": ["in_app"]},
        {"channels": 7},
        {"channels": True},
        {"channels": "in_app"},
        {"channels": {"in_app": True, "x_channel_name_17": True}},
        {"channels": {"email": "abc"}},
        {"channels": {"email": 1}},
        {"channels": {"in_app": None}},
        {"min_confidence": "lots"},
        {"min_confidence": float("nan")},
        {"min_confidence": float("inf")},
        {"min_confidence": -0.1},
        {"min_confidence": 1.5},
        {"min_confidence": True},
    ],
)
def test_malformed_follow_bodies_are_refused(body):
    with pytest.raises(HTTPException) as exc:
        _validate(body)
    assert exc.value.status_code == 422


def test_well_formed_bodies_pass_through():
    assert _validate({}) == ({"in_app": True}, 0.0)
    assert _validate({"channels": {}}) == ({"in_app": True}, 0.0)
    assert _validate({"channels": None, "min_confidence": None}) == ({"in_app": True}, 0.0)
    # What the website sends: only the channels that are on.
    assert _validate({"channels": {"in_app": True, "email": True}, "min_confidence": 0.4}) == (
        {"in_app": True, "email": True},
        0.4,
    )
    assert _validate({"channels": {"webhook": True, "in_app": False}, "min_confidence": 1}) == (
        {"webhook": True, "in_app": False},
        1.0,
    )


def test_the_endpoint_refuses_a_list_and_stores_nothing(monkeypatch: pytest.MonkeyPatch):
    main_mod = _build_app(monkeypatch)
    conn = _FakeConn(fetchval_result=1)
    from fastapi.testclient import TestClient

    with TestClient(main_mod.app) as client:
        _inject_pool(main_mod, conn)
        r = client.post(
            "/api/tradeworkz/bots/gamma_flip_defender/follow",
            headers=_headers(_mint("acct-1")),
            json={"channels": ["in_app"]},
        )
    assert r.status_code == 422, r.text
    assert conn.calls == [], "nothing may be written for a refused body"


class _Cursor:
    def __init__(self, rows):
        self._rows = rows
        self.inserts: list[tuple] = []

    def execute(self, sql, params=None):
        if "INSERT INTO tw_notifications_log" in sql:
            self.inserts.append(params)

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self.cur = _Cursor(rows)

    def cursor(self):
        return self.cur


def test_fanout_reads_rows_stored_before_validation_without_raising(monkeypatch):
    from src.tradeworkz import config as tw_config
    from src.tradeworkz import notifications

    monkeypatch.setattr(tw_config, "NOTIFY_ENABLED", True)
    rows = [
        ("acct-list", ["in_app"], 0),  # a list: falls back to in-app
        ("acct-number", 7, 0),  # a number: falls back to in-app
        ("acct-string-json", json.dumps({"email": "abc"}), 0),  # non-boolean: skipped
        ("acct-unknown", {"x_channel_name_17": True, "in_app": True}, 0),  # unknown key skipped
        ("acct-bad-min", {"in_app": True}, "lots"),  # unreadable floor: treated as 0
        ("acct-ok", {"in_app": True, "email": True}, 0),
    ]
    conn = _Conn(rows)
    written = notifications.fanout_event(
        conn,
        bot_id="gamma_flip_defender",
        event_type="entry",
        payload={"conviction": 0.8, "underlying": "SPY"},
    )
    channels_by_user: dict[str, list[str]] = {}
    for params in conn.cur.inserts:
        channels_by_user.setdefault(params[0], []).append(params[5])
    assert channels_by_user == {
        "acct-list": ["in_app"],
        "acct-number": ["in_app"],
        "acct-unknown": ["in_app"],
        "acct-bad-min": ["in_app"],
        "acct-ok": ["in_app", "email"],
    }
    assert written == 6


def test_trade_filters_parse_iso_dates_and_refuse_garbage():
    from datetime import datetime, timezone

    from src.api.routers.tradeworkz import _parse_filter_instant

    assert _parse_filter_instant("since", "2026-11-01") == datetime(
        2026, 11, 1, tzinfo=timezone.utc
    )
    assert _parse_filter_instant("until", "2026-11-01T14:30:00Z") == datetime(
        2026, 11, 1, 14, 30, tzinfo=timezone.utc
    )
    with pytest.raises(HTTPException) as exc:
        _parse_filter_instant("since", "last tuesday")
    assert exc.value.status_code == 400
