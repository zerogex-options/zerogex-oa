"""The tick reads only the market it can act on.

Outside the opens window nothing can open, so only an underlying with an open
position needs its ~30-read snapshot (to mark, exit or settle the position).
Before 2026-11-18 the engine rebuilt every fleet symbol's snapshot every 5
seconds, all night; with the universe at every analytics symbol that was six
snapshots per tick with nothing to do.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from src.tradeworkz import engine


class _Cur:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, params=None):
        pass

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cur(self._rows)


_BOT = (
    "charm_close_magnet",
    "C",
    "CharmCloseMagnet",
    "0DTE",
    "context",
    "*",
    "",
    "",
    True,
    True,
    {},
)


@pytest.fixture
def wired(monkeypatch):
    built, held_calls = [], []

    @contextmanager
    def _db():
        yield _Conn([_BOT])

    monkeypatch.setattr(engine, "db_connection", _db)
    monkeypatch.setattr(engine.tw_config, "fleet_universes", lambda: ("SPY", "QQQ", "NVDA"))
    monkeypatch.setattr(engine, "build_snapshot", lambda conn, u: built.append(u) or object())
    monkeypatch.setattr(engine, "_run_bot", lambda *a, **k: {})
    monkeypatch.setattr(engine, "rollup_daily", lambda conn: None)

    def _held(held):
        def fn(conn):
            held_calls.append(1)
            return set(held)

        monkeypatch.setattr(engine, "_held_underlyings", fn)

    return built, held_calls, _held


def test_outside_the_window_with_nothing_held_no_snapshot_is_built(monkeypatch, wired):
    built, _calls, held = wired
    held(set())
    monkeypatch.setattr(engine, "_opens_allowed", lambda now: False)
    summary = engine.tick()
    assert built == []
    assert summary["opens_allowed"] is False


def test_outside_the_window_only_held_underlyings_are_read(monkeypatch, wired):
    built, _calls, held = wired
    held({"NVDA", "IWM"})  # IWM is not in the fleet: never read
    monkeypatch.setattr(engine, "_opens_allowed", lambda now: False)
    engine.tick()
    assert built == ["NVDA"]


def test_inside_the_window_every_symbol_is_read(monkeypatch, wired):
    built, calls, held = wired
    held(set())
    monkeypatch.setattr(engine, "_opens_allowed", lambda now: True)
    engine.tick()
    assert built == ["NVDA", "QQQ", "SPY"]
    assert calls == [], "the held-positions read is skipped while opens are allowed"
