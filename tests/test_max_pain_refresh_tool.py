"""src.tools.max_pain_refresh: the scheduled (off-process) refresh entrypoint.

Replaces the old 5-min in-process loop / inline recompute.  These checks
pin the wiring the systemd unit depends on: config-driven defaults, CLI
overrides, that it delegates to the exact tested
``DatabaseManager.refresh_max_pain_snapshots`` (no second copy of the
recompute SQL), connect/disconnect lifecycle, and the exit codes systemd
keys off (0 ok, 1 hard failure).

Pure/hermetic: DatabaseManager is faked; no DB, no event loop surprises.
"""

from __future__ import annotations

import pytest

from src import config
from src.tools import max_pain_refresh


class _FakeDB:
    connect_raises = False
    refresh_raises = False

    def __init__(self) -> None:
        self.calls: list = []
        _FakeDB.last = self

    async def connect(self) -> None:
        self.calls.append(("connect",))
        if _FakeDB.connect_raises:
            raise RuntimeError("pool down")

    async def disconnect(self) -> None:
        self.calls.append(("disconnect",))

    async def refresh_max_pain_snapshots(self, symbols, strike_limit, statement_timeout_ms):
        self.calls.append(("refresh", list(symbols), strike_limit, statement_timeout_ms))
        if _FakeDB.refresh_raises:
            raise RuntimeError("recompute blew up")


@pytest.fixture(autouse=True)
def _fake_db(monkeypatch):
    _FakeDB.connect_raises = False
    _FakeDB.refresh_raises = False
    _FakeDB.last = None
    monkeypatch.setattr(max_pain_refresh, "DatabaseManager", _FakeDB)


def test_defaults_from_config_and_delegates_to_refresh():
    rc = max_pain_refresh.main([])
    assert rc == 0
    db = _FakeDB.last
    assert db.calls == [
        ("connect",),
        (
            "refresh",
            list(config.MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS),
            config.MAX_PAIN_BACKGROUND_REFRESH_STRIKE_LIMIT,
            config.MAX_PAIN_BACKGROUND_REFRESH_STATEMENT_TIMEOUT_MS,
        ),
        ("disconnect",),
    ]


def test_cli_overrides_symbols_strike_and_timeout():
    rc = max_pain_refresh.main(
        ["--symbols", "spy", "qqq", "--strike-limit", "250", "--statement-timeout-ms", "60000"]
    )
    assert rc == 0
    assert _FakeDB.last.calls[1] == ("refresh", ["SPY", "QQQ"], 250, 60000)


def test_empty_symbol_set_is_a_clean_noop(monkeypatch):
    monkeypatch.setattr(config, "MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS", [])
    rc = max_pain_refresh.main([])
    assert rc == 0
    assert _FakeDB.last is None  # never even connected


def test_connect_failure_returns_nonzero_and_skips_refresh():
    _FakeDB.connect_raises = True
    rc = max_pain_refresh.main([])
    assert rc == 1
    assert _FakeDB.last.calls == [("connect",)]  # no refresh attempted


def test_refresh_failure_returns_nonzero_but_still_disconnects():
    _FakeDB.refresh_raises = True
    rc = max_pain_refresh.main([])
    assert rc == 1
    kinds = [c[0] for c in _FakeDB.last.calls]
    assert kinds == ["connect", "refresh", "disconnect"]
