"""Email-channel dust suppression on exit-event fanout.

Every exit with ``|payload.realized_pnl| < TRADEWORKZ_EMAIL_DUST_THRESHOLD``
must NOT queue an email row — unless the exit is a risk-off stop
(``reason IN ('stop', 'wall_break')``), which the user always wants
to hear about even at $0.01. The in_app channel is unaffected in
every case: the dashboard bell stays complete so nothing is silently
lost, only the inbox spam is trimmed.

This test drives the fanout_event helper directly with a fake cursor
that records every INSERT so we can assert exactly which channel rows
were and weren't written. If someone re-defaults the threshold to 0
in config.py, this test still holds because the pass-through case
(large win, no suppression) is covered too.
"""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from src.tradeworkz import notifications


class _FakeCursor:
    def __init__(self, follower_rows: List[tuple]):
        self._follower_rows = follower_rows
        self.inserts: List[tuple[str, tuple[Any, ...]]] = []
        self._next_fetchall = follower_rows

    def execute(self, sql: str, params: tuple = ()) -> None:
        if sql.strip().startswith("SELECT end_user"):
            self._pending = self._follower_rows
        elif "INSERT INTO tw_notifications_log" in sql:
            self.inserts.append((sql, params))

    def fetchall(self):
        return self._follower_rows


class _FakeConn:
    def __init__(self, follower_rows: List[tuple]):
        self._cur = _FakeCursor(follower_rows)

    def cursor(self):
        return self._cur


def _payload(realized_pnl: float, reason: str = "target") -> Dict[str, Any]:
    return {
        "underlying": "SPY",
        "direction": "bullish",
        "realized_pnl": realized_pnl,
        "reason": reason,
        "outcome": "win" if realized_pnl > 0 else "loss",
        "contracts": 4,
        "entry_price": 3.15,
        "exit_price": 3.15 + realized_pnl / 400.0,
        "conviction": 0.7,
    }


def _channels_written(cur: _FakeCursor) -> List[str]:
    """Return the channel argument passed to each INSERT, in order."""
    return [params[5] for _, params in cur.inserts]


@pytest.fixture(autouse=True)
def _enable_notifications(monkeypatch):
    monkeypatch.setattr(notifications.tw_config, "NOTIFY_ENABLED", True)
    monkeypatch.setattr(notifications.tw_config, "EMAIL_DUST_THRESHOLD", 10.0)


def test_dust_exit_suppresses_email_but_keeps_in_app():
    """|realized_pnl| = $2 < $10, reason=target → email skipped, in_app written."""
    follower_rows = [
        ("user_alice", {"in_app": True, "email": True}, 0.0),
    ]
    conn = _FakeConn(follower_rows)

    written = notifications.fanout_event(
        conn,
        bot_id="test_bot",
        event_type="exit",
        payload=_payload(2.00, reason="target"),
    )
    channels = _channels_written(conn.cursor())
    assert channels == ["in_app"], f"expected only in_app, got {channels}"
    assert written == 1


def test_dust_riskoff_stop_still_emails():
    """Small |P&L| but reason='stop' — user MUST get the email."""
    follower_rows = [
        ("user_alice", {"in_app": True, "email": True}, 0.0),
    ]
    conn = _FakeConn(follower_rows)

    notifications.fanout_event(
        conn,
        bot_id="test_bot",
        event_type="exit",
        payload=_payload(-3.00, reason="stop"),
    )
    channels = _channels_written(conn.cursor())
    assert "email" in channels, "stop-out at any size must still email"
    assert "in_app" in channels


def test_dust_riskoff_wall_break_still_emails():
    follower_rows = [
        ("user_alice", {"in_app": True, "email": True}, 0.0),
    ]
    conn = _FakeConn(follower_rows)

    notifications.fanout_event(
        conn,
        bot_id="test_bot",
        event_type="exit",
        payload=_payload(-4.50, reason="wall_break"),
    )
    channels = _channels_written(conn.cursor())
    assert "email" in channels


def test_material_exit_still_emails():
    """|P&L| ≥ threshold → normal fanout, both channels."""
    follower_rows = [
        ("user_alice", {"in_app": True, "email": True}, 0.0),
    ]
    conn = _FakeConn(follower_rows)

    notifications.fanout_event(
        conn,
        bot_id="test_bot",
        event_type="exit",
        payload=_payload(348.0, reason="target"),
    )
    channels = _channels_written(conn.cursor())
    assert sorted(channels) == ["email", "in_app"]


def test_entry_never_suppressed_by_dust_filter():
    """Dust filter only applies to exits — entries always fan out."""
    follower_rows = [
        ("user_alice", {"in_app": True, "email": True}, 0.0),
    ]
    conn = _FakeConn(follower_rows)

    notifications.fanout_event(
        conn,
        bot_id="test_bot",
        event_type="entry",
        payload={"conviction": 0.7, "realized_pnl": 0.0},
    )
    channels = _channels_written(conn.cursor())
    assert "email" in channels, "entry events must not be dust-suppressed"


def test_threshold_zero_disables_suppression(monkeypatch):
    """EMAIL_DUST_THRESHOLD=0 turns the filter off entirely."""
    monkeypatch.setattr(notifications.tw_config, "EMAIL_DUST_THRESHOLD", 0.0)
    follower_rows = [
        ("user_alice", {"in_app": True, "email": True}, 0.0),
    ]
    conn = _FakeConn(follower_rows)

    notifications.fanout_event(
        conn,
        bot_id="test_bot",
        event_type="exit",
        payload=_payload(1.50, reason="target"),
    )
    channels = _channels_written(conn.cursor())
    assert "email" in channels


def test_missing_realized_pnl_is_not_suppressed():
    """No realized_pnl on payload → we can't measure dust → send everything."""
    follower_rows = [
        ("user_alice", {"in_app": True, "email": True}, 0.0),
    ]
    conn = _FakeConn(follower_rows)

    payload = {"outcome": "win", "reason": "target", "conviction": 0.7}
    notifications.fanout_event(
        conn, bot_id="test_bot", event_type="exit", payload=payload,
    )
    channels = _channels_written(conn.cursor())
    assert "email" in channels
