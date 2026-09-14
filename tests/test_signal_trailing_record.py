"""GET /api/scorecard/signal-record — the cross-session signal record.

The daily scorecard is one calendar day. Nothing aggregated a signal's record
across sessions, so "is this signal actually any good?" — the question a
subscriber asks after it calls one session right — could not be answered from
the product at all.
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


def _payload(**overrides):
    base = {
        "symbol": "QQQ",
        "sessions_requested": 30,
        "window_start_utc": datetime(2026, 8, 3, 4, 0, tzinfo=timezone.utc),
        "window_end_utc": datetime(2026, 9, 14, 4, 0, tzinfo=timezone.utc),
        "horizon_minutes": 60,
        "signals": [
            {
                "name": "eod_pressure",
                "flips": 61,
                "scored": 12,
                "wins": 7,
                "losses": 5,
                "win_rate": 7 / 12,
                "avg_directional_return": 0.0009,
            },
            {
                "name": "trap_detection",
                "flips": 88,
                "scored": 88,
                "wins": 50,
                "losses": 38,
                "win_rate": 50 / 88,
                "avg_directional_return": 0.0012,
            },
        ],
    }
    base.update(overrides)
    return base


def test_signal_record_returns_rows_with_labels(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_signal_trailing_record = AsyncMock(return_value=_payload())
    with TestClient(app) as client:
        r = client.get("/api/scorecard/signal-record", params={"symbol": "QQQ", "sessions": 30})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["is_empty"] is False
    names = {row["name"]: row for row in body["signals"]}
    assert names["eod_pressure"]["label"] == "EOD Pressure"
    # scored < flips is the honest shape for a close-only signal, not a defect.
    assert names["eod_pressure"]["scored"] < names["eod_pressure"]["flips"]


def test_signal_record_rejects_an_unknown_signal(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_signal_trailing_record = AsyncMock(return_value=_payload())
    with TestClient(app) as client:
        r = client.get("/api/scorecard/signal-record", params={"signal": "not_a_signal"})
    assert r.status_code == 422


def test_signal_record_passes_a_single_signal_through(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    stub = AsyncMock(return_value=_payload(signals=[]))
    dbmod.DatabaseManager.get_signal_trailing_record = stub
    with TestClient(app) as client:
        r = client.get("/api/scorecard/signal-record", params={"signal": "eod_pressure"})
    assert r.status_code == 200
    assert r.json()["is_empty"] is True
    assert stub.await_args.kwargs["signal_names"] == ["eod_pressure"]


def test_signal_record_clamps_the_session_window(monkeypatch: pytest.MonkeyPatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_signal_trailing_record = AsyncMock(return_value=_payload())
    with TestClient(app) as client:
        assert client.get("/api/scorecard/signal-record", params={"sessions": 1}).status_code == 422
        assert client.get("/api/scorecard/signal-record", params={"sessions": 200}).status_code == 422


def test_win_rate_is_null_when_nothing_was_scorable(monkeypatch: pytest.MonkeyPatch):
    """A close-only signal with no gradable flips must not read as 0% wins."""
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_signal_trailing_record = AsyncMock(
        return_value=_payload(
            signals=[
                {
                    "name": "eod_pressure",
                    "flips": 9,
                    "scored": 0,
                    "wins": 0,
                    "losses": 0,
                    "win_rate": None,
                    "avg_directional_return": None,
                }
            ]
        )
    )
    with TestClient(app) as client:
        r = client.get("/api/scorecard/signal-record", params={"signal": "eod_pressure"})
    row = r.json()["signals"][0]
    assert row["win_rate"] is None
    assert row["avg_directional_return"] is None
    assert row["flips"] == 9
