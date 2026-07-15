"""SPX expired-leg settlement: reverse option-root -> underlying resolution.

A 0DTE SPX option carries the weekly root ``SPXW`` (``SPXW 260714P7530``),
but its price settles against the ``SPX`` cash-index close, and the
underlying price is stored in ``underlying_quotes`` under ``SPX``. The
expired-leg intrinsic fallback in ``pricing.spread_price(action='close')``
extracts the underlying from the option symbol — so it must map the
``SPXW`` root back to ``SPX`` or it queries a symbol that has no row,
returns None, and the position can't settle.

These lock in the reverse mapping and the end-to-end intrinsic fallback for
SPX, while confirming ETF settlement (SPY) is unchanged.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional, Tuple

import pytest

from src.symbols import resolve_underlying_from_option_root
from src.tradeworkz import pricing

# The documented SPX alias config (weekly chain -> SPXW root, monthly -> SPX).
SPX_ENV = {
    "SYMBOL_ALIASES": "SPX=$SPXW.X",
    "OPTION_ROOT_ALIASES": "$SPXW.X=SPXW,$SPX.X=SPX",
}


def _use_spx_env(monkeypatch):
    for k, v in SPX_ENV.items():
        monkeypatch.setenv(k, v)


# ----------------------------------------------------------------------
# resolve_underlying_from_option_root
# ----------------------------------------------------------------------


def test_reverse_root_maps_spxw_to_spx(monkeypatch):
    _use_spx_env(monkeypatch)
    assert resolve_underlying_from_option_root("SPXW") == "SPX"
    assert resolve_underlying_from_option_root("spxw") == "SPX"  # case-insensitive


def test_reverse_root_monthly_and_etfs_pass_through(monkeypatch):
    _use_spx_env(monkeypatch)
    # Monthly SPX root already equals its underlying_quotes symbol.
    assert resolve_underlying_from_option_root("SPX") == "SPX"
    # ETFs carry no alias.
    for etf in ("SPY", "QQQ", "IWM"):
        assert resolve_underlying_from_option_root(etf) == etf
    assert resolve_underlying_from_option_root("") == ""


def test_reverse_root_no_aliases_returns_root(monkeypatch):
    monkeypatch.delenv("SYMBOL_ALIASES", raising=False)
    monkeypatch.delenv("OPTION_ROOT_ALIASES", raising=False)
    # Nothing configured -> best-effort pass-through.
    assert resolve_underlying_from_option_root("SPXW") == "SPXW"
    assert resolve_underlying_from_option_root("SPY") == "SPY"


def test_underlying_from_option_symbol_reverse_maps(monkeypatch):
    _use_spx_env(monkeypatch)
    assert pricing._underlying_from_option_symbol("SPXW 260714P7530") == "SPX"
    assert pricing._underlying_from_option_symbol("SPY 260714P755") == "SPY"
    assert pricing._underlying_from_option_symbol(None) is None


# ----------------------------------------------------------------------
# End-to-end: expired SPX leg settles against the SPX cash close
# ----------------------------------------------------------------------


class _FakeCursor:
    """Serves option_chains / underlying_quotes rows keyed by symbol param."""

    def __init__(self, option_rows, underlying_rows):
        self._option_rows = option_rows
        self._underlying_rows = underlying_rows
        self._pending: Optional[Tuple[Any, ...]] = None

    def execute(self, sql: str, params: tuple = ()) -> None:
        if "FROM option_chains" in sql:
            self._pending = self._option_rows.get(params[0])
        elif "FROM underlying_quotes" in sql:
            self._pending = self._underlying_rows.get(params[0])
        else:
            self._pending = None

    def fetchone(self):
        return self._pending


class _FakeConn:
    def __init__(self, option_rows, underlying_rows):
        self._cursor = _FakeCursor(option_rows, underlying_rows)

    def cursor(self):
        return self._cursor


def _spx_put_leg(strike: float, expiration_iso: str) -> Dict[str, Any]:
    return {
        "option_symbol": f"SPXW {expiration_iso.replace('-', '')[-6:]}P{int(strike)}",
        "side": "long",
        "option_type": "put",
        "strike": strike,
        "expiration": expiration_iso,
    }


def test_expired_spx_put_settles_against_spx_close(monkeypatch):
    """No live SPXW quote -> intrinsic priced off the SPX cash close."""
    _use_spx_env(monkeypatch)
    today = date.today().isoformat()
    leg = _spx_put_leg(strike=7530.0, expiration_iso=today)
    conn = _FakeConn(
        option_rows={leg["option_symbol"]: None},   # expired, no fillable quote
        underlying_rows={"SPX": (7521.40,)},         # SPX cash close only under 'SPX'
    )
    px = pricing.spread_price(conn, [leg], action="close")
    # intrinsic = max(0, 7530 - 7521.40) = 8.60
    assert px == pytest.approx(8.60, abs=1e-6)


def test_expired_spx_settlement_would_be_none_without_reverse_map(monkeypatch):
    """Regression guard: the underlying row exists ONLY under 'SPX'.

    Pre-fix the code looked up 'SPXW' (the raw root), found no row, and
    returned None — the position could never settle. The reverse map makes
    the lookup hit 'SPX'.
    """
    _use_spx_env(monkeypatch)
    today = date.today().isoformat()
    leg = _spx_put_leg(strike=7530.0, expiration_iso=today)
    conn = _FakeConn(
        option_rows={leg["option_symbol"]: None},
        underlying_rows={"SPX": (7545.0,)},  # deliberately no 'SPXW' key
    )
    px = pricing.spread_price(conn, [leg], action="close")
    assert px is not None
    assert px == pytest.approx(0.0)  # 7530 - 7545 < 0 -> OTM put -> 0, not None
