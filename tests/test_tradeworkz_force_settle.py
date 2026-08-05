"""A 0DTE past its time_stop must never strand open just because it can't be priced.

The money bug (2026-08-04): two vwap_reversion_scalper long puts sat open ~13
hours past an 11:00 ET time_stop. Their option quotes had rolled off and the
intrinsic settlement was unavailable, so ``spread_price(action="close")``
returned ``None`` every tick. ``mark_position`` then returned ``None``, the
engine did ``if mark is None: continue`` — never reaching ``exit_criteria`` and
its time_stop — and ``close_position`` had the identical bail, so the positions
could be neither marked nor closed. Their stale unrealized P&L kept corrupting
NAV / heat.

The backstop: when a position is past its hard time_stop (or its legs have
expired) and cannot be priced, the engine force-settles it at the last observed
mark via ``close_position(..., fallback_fill=pos.current_price)`` instead of
skipping it. A position still inside its time_stop is skipped and retried, as
before.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

from src.tradeworkz import engine
from src.tradeworkz.engine import _force_settle_due
from src.tradeworkz.models import OpenPosition
from src.tradeworkz.reconciler import close_position

_NOW = datetime.now(timezone.utc)
_FUTURE_EXP = [{"option_symbol": "SPY 991231P500", "side": "long",
                "option_type": "put", "strike": 500, "expiration": "2099-12-31"}]


# ── _force_settle_due: the pure trigger ───────────────────────────────


@dataclass
class _Duck:
    """Minimal stand-in — _force_settle_due only reads these two fields."""
    time_stop_at: Optional[datetime]
    legs: List[Dict[str, Any]]


def test_force_settle_due_when_past_time_stop():
    pos = _Duck(time_stop_at=_NOW - timedelta(hours=13), legs=_FUTURE_EXP)
    assert _force_settle_due(pos, _NOW) is True


def test_not_due_before_time_stop():
    pos = _Duck(time_stop_at=_NOW + timedelta(hours=1), legs=_FUTURE_EXP)
    assert _force_settle_due(pos, _NOW) is False


def test_force_settle_due_when_legs_expired_even_without_time_stop():
    # time_stop unknown, but the legs themselves expired yesterday.
    yday = (_NOW.astimezone(engine._ET).date() - timedelta(days=1)).isoformat()
    pos = _Duck(time_stop_at=None, legs=[{"expiration": yday, "side": "long",
                                          "option_type": "put", "strike": 1}])
    assert _force_settle_due(pos, _NOW) is True


def test_not_due_when_no_time_stop_and_legs_unexpired():
    pos = _Duck(time_stop_at=None, legs=_FUTURE_EXP)
    assert _force_settle_due(pos, _NOW) is False


# ── close_position(fallback_fill=...) ─────────────────────────────────


class _FakeCursor:
    def __init__(self):
        self.captured = []

    def execute(self, sql, params=None):
        self.captured.append((sql, params))

    def fetchone(self):
        return (99,)


class _FakeConn:
    def __init__(self):
        self._cursor = _FakeCursor()

    def cursor(self):
        return self._cursor


def _pos(current_price: float = 0.55) -> OpenPosition:
    return OpenPosition(
        id=1, bot_id="vwap_reversion_scalper", underlying="QQQ",
        opened_at=_NOW - timedelta(hours=13), direction="bearish",
        strategy_type="BUY_PUT_DEBIT",
        legs=[{"option_symbol": "QQQ 260804P716", "side": "long",
               "option_type": "put", "strike": 716, "expiration": "2026-08-04"}],
        entry_price=1.00, current_price=current_price, quantity_open=14,
        unrealized_pnl=-119.84, stop_price=None, target_price=None,
        time_stop_at=_NOW - timedelta(hours=12), min_hold_until=None,
        wall_ref_price=None, wall_ref_side=None, entry_conviction=0.5,
        components_at_entry={}, entry_spot=717.0, quantity_initial=14,
    )


def _run_close(conn, pos, *, spread, fallback):
    with patch("src.tradeworkz.reconciler.spread_price", return_value=spread), patch(
        "src.tradeworkz.reconciler.notifications.fanout_event", return_value=0
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.load_state", return_value=MagicMock()
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.online_update", side_effect=lambda s, *a, **k: s
    ), patch(
        "src.tradeworkz.reconciler.ml_mod.save_state", return_value=None
    ):
        return close_position(conn, pos, reason="time_stop_settle", fallback_fill=fallback)


def test_fallback_settles_when_structure_unpriceable():
    # spread_price None (can't price the expired legs) but a last mark exists:
    # the position settles at that mark instead of stranding.
    conn = _FakeConn()
    rid = _run_close(conn, _pos(current_price=0.55), spread=None, fallback=0.55)
    assert rid == 99
    trade = next(c for c in conn.cursor().captured if "INSERT INTO tw_trades" in c[0])
    entry_price, exit_price, quantity, realized = (
        trade[1][7], trade[1][8], trade[1][9], trade[1][10]
    )
    # Settled at the fallback fill: realized = (0.55 - 1.00) * 14 * 100 = -630.
    assert exit_price == 0.55
    assert quantity == 14
    assert realized == (0.55 - 1.00) * 14 * 100
    # Invariant A holds even for a force-settle.
    assert (exit_price - entry_price) * quantity * 100 == realized


def test_live_price_still_wins_over_fallback():
    # When spread_price CAN value the structure, the fallback is ignored.
    conn = _FakeConn()
    _run_close(conn, _pos(current_price=0.55), spread=0.80, fallback=0.55)
    trade = next(c for c in conn.cursor().captured if "INSERT INTO tw_trades" in c[0])
    assert trade[1][8] == 0.80  # exit_price = the live fill, not the fallback


def test_no_fallback_still_bails_when_unpriceable():
    # A normal close (no fallback) is unchanged: an unpriceable structure
    # returns None and writes nothing.
    conn = _FakeConn()
    rid = _run_close(conn, _pos(), spread=None, fallback=None)
    assert rid is None
    assert not any("INSERT INTO tw_trades" in c[0] for c in conn.cursor().captured)


# ── _run_bot wiring: unmarkable + past time_stop -> force-settle ───────


@dataclass
class _MLState:
    pass


class _FakeBot:
    def __init__(self, spec, ml_state=None):
        self.exit_called = False

    def size_multiplier(self):
        return 1.0

    def exit_criteria(self, snap, pos):
        self.exit_called = True
        return MagicMock(should_close=False, should_cut=False)


class _NullConn:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Pos:
    def __init__(self, time_stop_at, current_price=0.55):
        self.id = 7
        self.underlying = "SPY"
        self.time_stop_at = time_stop_at
        self.current_price = current_price
        self.legs = _FUTURE_EXP


_ROW = (
    "vwap_reversion_scalper", "VWAP Reversion Scalper", "VwapReversionScalper",
    "0DTE", "context", "SPY", "tag", "desc", True, True, {},
)


def _wire(monkeypatch, pos):
    monkeypatch.setattr(engine, "db_connection", lambda: _NullConn())
    monkeypatch.setattr(engine, "load_capital", lambda conn, bid: object())
    monkeypatch.setattr(engine.ml_mod, "load_state", lambda conn, bid: _MLState())
    monkeypatch.setattr(
        engine, "get_bot_class", lambda cls: (lambda spec, ml_state=None: _FakeBot(spec))
    )
    monkeypatch.setattr(engine, "load_open_positions", lambda conn, bid: [pos])
    monkeypatch.setattr(engine, "mark_position", lambda conn, p: None)  # unpriceable
    settled = []
    monkeypatch.setattr(
        engine, "close_position",
        lambda conn, p, reason=None, fallback_fill=None: settled.append(
            (reason, fallback_fill)) or 123,
    )
    return settled


def test_run_bot_force_settles_unmarkable_past_time_stop(monkeypatch):
    pos = _Pos(time_stop_at=_NOW - timedelta(hours=13), current_price=0.55)
    settled = _wire(monkeypatch, pos)
    stats = engine._run_bot(_ROW, {"SPY": object()}, ("SPY",), opens_allowed=False)
    assert settled == [("time_stop_settle", 0.55)]
    assert stats["closed"] == 1


def test_run_bot_skips_unmarkable_before_time_stop(monkeypatch):
    pos = _Pos(time_stop_at=_NOW + timedelta(hours=1), current_price=0.55)
    settled = _wire(monkeypatch, pos)
    stats = engine._run_bot(_ROW, {"SPY": object()}, ("SPY",), opens_allowed=False)
    assert settled == []            # not past time_stop -> skip and retry
    assert stats["closed"] == 0
