"""Regular-trading-hours gate on NEW opens.

Root cause of the 2026-07-16 evening ``-$12.78K`` churn: ``gamma_flip_breaker``
and ``dealer_delta_pressure_rider`` gate entries only on
``minutes_since_open >= 30`` — a floor with no ceiling. At 20:00 ET
``minutes_since_open`` is 630, which sails through, so with an after-hours-
ticking feed the bots opened 0DTE positions. The reconciler then caps every
0DTE's ``time_stop`` at 15:55 ET (already ~4h in the past), so each position
was instantly eligible to time-stop and died ~90s later for a spread loss —
a churn loop, all ``time_stop``, all losers.

``engine._opens_allowed`` is the coarse market-hours chokepoint that fixes it:
no NEW opens outside the ET cash session or at/after the 0DTE time_stop cap.
Exits and marks on existing positions are never gated. These tests lock both
the pure gate logic and the ``_run_bot`` wiring (exits still run while opens
are suppressed).
"""

from __future__ import annotations

from dataclasses import dataclass

from src.market_calendar import ET
from src.tradeworkz import config as tw_config
from src.tradeworkz import engine


def _et(y, mo, d, h, mi):
    """A timezone-aware ET wall-clock instant (naive-localized via pytz)."""
    from datetime import datetime

    return ET.localize(datetime(y, mo, d, h, mi))


# 2026-07-16 is a Wednesday; 2026-07-18 is a Saturday. July => EDT (UTC-4).


# ----------------------------------------------------------------------
# _opens_allowed — the pure gate
# ----------------------------------------------------------------------


def test_opens_blocked_after_hours_the_money_bug():
    # 20:00 ET Wed — the exact instant the fleet churned. Must be blocked.
    assert engine._opens_allowed(_et(2026, 7, 16, 20, 0)) is False


def test_opens_allowed_midday_rth_weekday():
    assert engine._opens_allowed(_et(2026, 7, 16, 11, 0)) is True


def test_opens_blocked_before_open():
    # 09:00 ET — pre-market, before the 09:30 cash open.
    assert engine._opens_allowed(_et(2026, 7, 16, 9, 0)) is False


def test_opens_blocked_at_and_after_time_stop_cap():
    # 15:55 ET is the 0DTE time_stop cap; opening then (or later) would
    # instantly time-stop, so the gate is strict (< cap).
    assert engine._opens_allowed(_et(2026, 7, 16, 15, 55)) is False
    assert engine._opens_allowed(_et(2026, 7, 16, 15, 54)) is True


def test_opens_blocked_on_weekend():
    # Saturday midday — market closed even though the time is "in-window".
    assert engine._opens_allowed(_et(2026, 7, 18, 11, 0)) is False


def test_opens_allowed_when_gate_disabled(monkeypatch):
    monkeypatch.setattr(tw_config, "RTH_ONLY_OPENS", False)
    # Even at 8pm on a Saturday, the disabled gate lets opens through.
    assert engine._opens_allowed(_et(2026, 7, 18, 20, 0)) is True


def test_parse_et_hhmm_falls_back_on_garbage():
    from datetime import time

    assert engine._parse_et_hhmm("not-a-time", time(15, 55)) == time(15, 55)
    assert engine._parse_et_hhmm("09:30", time(15, 55)) == time(9, 30)


# ----------------------------------------------------------------------
# _run_bot wiring — exits run while opens are gated
# ----------------------------------------------------------------------


@dataclass
class _MLState:
    pass


class _ExitDecision:
    should_close = False
    reason = None


class _FakeBot:
    """Records whether the open/exit paths were reached."""

    def __init__(self, spec, ml_state=None):
        self.spec = spec
        self.open_called = False
        self.exit_called = False

    def size_multiplier(self):
        return 1.0

    def exit_criteria(self, snap, pos):
        self.exit_called = True
        return _ExitDecision()

    def open_criteria(self, snap):
        self.open_called = True
        return None


class _FakePos:
    underlying = "SPY"


class _NullConn:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _wire(monkeypatch):
    """Patch the engine's per-bot dependencies; return the shared _FakeBot."""
    the_bot = _FakeBot(spec=None)

    monkeypatch.setattr(engine, "db_connection", lambda: _NullConn())
    monkeypatch.setattr(engine, "load_capital", lambda conn, bid: object())
    monkeypatch.setattr(engine.ml_mod, "load_state", lambda conn, bid: _MLState())
    monkeypatch.setattr(engine, "get_bot_class", lambda cls: (lambda spec, ml_state=None: the_bot))
    monkeypatch.setattr(engine, "load_open_positions", lambda conn, bid: [_FakePos()])
    monkeypatch.setattr(engine, "mark_position", lambda conn, pos: object())
    closed = []
    opened = []
    monkeypatch.setattr(
        engine, "close_position", lambda conn, pos, reason=None: closed.append(reason)
    )
    monkeypatch.setattr(engine, "daily_realized_pnl", lambda conn, bid: 0.0)
    monkeypatch.setattr(engine, "open_position", lambda *a, **k: opened.append(1) or 1)
    return the_bot, closed, opened


# tw_bots row shape used by _run_bot (11 columns).
_ROW = (
    "gamma_flip_breaker",  # id
    "Gamma Flip Breaker",  # display_name
    "GammaFlipBreaker",  # strategy_class
    "0DTE",  # tier
    "context",  # direction_mode
    "SPY",  # universe
    "tag",  # tagline
    "desc",  # description
    True,  # is_public
    True,  # enabled
    {},  # params
)


def test_run_bot_skips_open_when_gated(monkeypatch):
    the_bot, closed, opened = _wire(monkeypatch)
    snap = object()
    stats = engine._run_bot(_ROW, {"SPY": snap}, ("SPY",), opens_allowed=False)
    # Exits were still evaluated; the open path was never reached.
    assert the_bot.exit_called is True
    assert the_bot.open_called is False
    assert opened == []
    assert stats["opened"] == 0


def test_run_bot_opens_when_allowed(monkeypatch):
    the_bot, closed, opened = _wire(monkeypatch)
    snap = object()
    engine._run_bot(_ROW, {"SPY": snap}, ("SPY",), opens_allowed=True)
    # With the gate open, the entry path IS reached (open_criteria consulted).
    assert the_bot.open_called is True
