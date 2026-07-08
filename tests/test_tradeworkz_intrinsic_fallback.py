"""Regression: pricing.spread_price(action='close') falls back to intrinsic
value on expired options with no fillable option_chains quote.

When a 0DTE option's expiration date is on or before today AND
option_chains has no row or the last row is older than
OPTION_QUOTE_MAX_AGE_SECONDS, spread_price previously returned None
and the reconciler couldn't close the position — it just sat "open"
for hours with a stale mark. That's what produced the -$2.82K "loss"
on `SPY 7/8 P744` when SPY had actually dropped to 743.55 in
after-hours (intrinsic = $0.45 → ~$120k win, not a -$2.82K loss).

The fallback:
  * only fires on action='close' — we never OPEN a position off
    intrinsic
  * only fires when the leg's expiration is <= today's UTC date
  * uses ``max(0, spot - strike)`` for calls, ``max(0, strike - spot)``
    for puts, negated for short legs
  * pulls spot from underlying_quotes (the equity's after-hours prints
    keep flowing even when options are shut)
  * does NOT apply slippage — intrinsic IS the settlement price

Also verifies: on unexpired options, the fallback is a no-op — a bot
must never open a position because it has no live quote yet.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

import pytest

from src.tradeworkz import pricing


class _FakeCursor:
    """Records calls; returns pre-scripted rows keyed by (table, key)."""

    def __init__(
        self,
        option_rows: Dict[str, Optional[Tuple[Any, ...]]],
        underlying_rows: Dict[str, Optional[Tuple[Any, ...]]],
    ):
        self._option_rows = option_rows
        self._underlying_rows = underlying_rows
        self._pending: Optional[Tuple[Any, ...]] = None

    def execute(self, sql: str, params: tuple = ()) -> None:
        if "FROM option_chains" in sql:
            sym = params[0]
            self._pending = self._option_rows.get(sym)
        elif "FROM underlying_quotes" in sql:
            sym = params[0]
            self._pending = self._underlying_rows.get(sym)
        else:
            self._pending = None

    def fetchone(self):
        return self._pending


class _FakeConn:
    def __init__(self, option_rows, underlying_rows):
        self._cursor = _FakeCursor(option_rows, underlying_rows)

    def cursor(self):
        return self._cursor


def _long_put_leg(strike: float, expiration_iso: str) -> Dict[str, Any]:
    return {
        "option_symbol": f"SPY {expiration_iso.replace('-', '')[-6:]}P{int(strike)}",
        "side": "long",
        "option_type": "put",
        "strike": strike,
        "expiration": expiration_iso,
    }


def _long_call_leg(strike: float, expiration_iso: str) -> Dict[str, Any]:
    return {
        "option_symbol": f"SPY {expiration_iso.replace('-', '')[-6:]}C{int(strike)}",
        "side": "long",
        "option_type": "call",
        "strike": strike,
        "expiration": expiration_iso,
    }


def test_close_expired_itm_put_falls_back_to_intrinsic():
    """Expired long put with no option_chains row → intrinsic from spot."""
    today = date.today().isoformat()
    leg = _long_put_leg(strike=744.0, expiration_iso=today)
    conn = _FakeConn(
        option_rows={leg["option_symbol"]: None},
        underlying_rows={"SPY": (743.55,)},
    )
    px = pricing.spread_price(conn, [leg], action="close")
    # intrinsic = max(0, 744 - 743.55) = 0.45
    assert px == pytest.approx(0.45, abs=1e-6)


def test_close_expired_otm_put_falls_back_to_zero():
    """Expired long put that's OTM → intrinsic 0, not None."""
    today = date.today().isoformat()
    leg = _long_put_leg(strike=740.0, expiration_iso=today)
    conn = _FakeConn(
        option_rows={leg["option_symbol"]: None},
        underlying_rows={"SPY": (743.55,)},
    )
    px = pricing.spread_price(conn, [leg], action="close")
    assert px == pytest.approx(0.0)


def test_close_expired_itm_call_falls_back_to_intrinsic():
    today = date.today().isoformat()
    leg = _long_call_leg(strike=740.0, expiration_iso=today)
    conn = _FakeConn(
        option_rows={leg["option_symbol"]: None},
        underlying_rows={"SPY": (743.55,)},
    )
    px = pricing.spread_price(conn, [leg], action="close")
    # intrinsic = max(0, 743.55 - 740) = 3.55
    assert px == pytest.approx(3.55, abs=1e-6)


def test_close_unexpired_option_no_quote_still_returns_none():
    """Fallback only applies to already-expired legs; live options with no
    quote must still refuse to fill."""
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    leg = _long_put_leg(strike=744.0, expiration_iso=tomorrow)
    conn = _FakeConn(
        option_rows={leg["option_symbol"]: None},
        underlying_rows={"SPY": (743.55,)},
    )
    px = pricing.spread_price(conn, [leg], action="close")
    assert px is None


def test_open_action_never_uses_intrinsic_even_when_expired():
    """We never OPEN on intrinsic — a bot must have a real live quote."""
    today = date.today().isoformat()
    leg = _long_put_leg(strike=744.0, expiration_iso=today)
    conn = _FakeConn(
        option_rows={leg["option_symbol"]: None},
        underlying_rows={"SPY": (743.55,)},
    )
    px = pricing.spread_price(conn, [leg], action="open")
    assert px is None


def test_close_expired_missing_spot_returns_none():
    """No underlying quote → we can't intrinsically settle, fail closed."""
    today = date.today().isoformat()
    leg = _long_put_leg(strike=744.0, expiration_iso=today)
    conn = _FakeConn(
        option_rows={leg["option_symbol"]: None},
        underlying_rows={"SPY": None},
    )
    px = pricing.spread_price(conn, [leg], action="close")
    assert px is None


def test_yesterday_expired_still_falls_back():
    """A leg that expired yesterday but is still open (edge case: engine
    restart, missed close) should also settle via intrinsic."""
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    leg = _long_put_leg(strike=744.0, expiration_iso=yesterday)
    conn = _FakeConn(
        option_rows={leg["option_symbol"]: None},
        underlying_rows={"SPY": (743.55,)},
    )
    px = pricing.spread_price(conn, [leg], action="close")
    assert px == pytest.approx(0.45, abs=1e-6)


def test_close_expired_with_live_quote_prefers_the_quote(monkeypatch):
    """If option_chains DOES have a fresh row, we use it — intrinsic is
    the fallback, not the primary. Guards against silently ignoring a
    real bid/ask on expiration morning."""
    today = date.today().isoformat()
    leg = _long_put_leg(strike=744.0, expiration_iso=today)
    now_utc = datetime.now(timezone.utc)
    conn = _FakeConn(
        # Fresh quote: (bid, ask, last, timestamp) tuple
        option_rows={leg["option_symbol"]: (0.20, 0.22, 0.21, now_utc)},
        underlying_rows={"SPY": (743.55,)},
    )
    # leg_fill_price is deterministic when we stub slippage to 0 — the
    # exact number matters less than "is it the quote-based path?".
    # Close action on a LONG leg fills at bid + slip. With slip=0 that's 0.20.
    monkeypatch.setattr(pricing.tw_config, "EXECUTION_SLIPPAGE_PCT", 0.0)
    px = pricing.spread_price(conn, [leg], action="close", slippage_pct=0.0)
    assert px == pytest.approx(0.20, abs=1e-6)
