"""Regression coverage for ``is_underlying_active_session``.

This predicate gates the session-aware staleness check in the analytics
engine. Symbol asymmetry is load-bearing:

* Stocks/ETFs (SPY, QQQ, AAPL): extended-hours cash session 04:00–20:00 ET
  — the freeze boundary for these symbols.
* Cash indexes (SPX, NDX): regular cash session 09:30–16:00 ET only —
  the index level itself does not print pre/after-hours even though its
  options can.

When this returns False, the analytics engine deliberately skips the
underlying-staleness gate: the resulting "stale gap" between the
latest option-chain timestamp and the cash-close underlying timestamp
is structural, not a fault, and refusing those cycles would block the
end-of-session snapshot from being re-anchored to the API consumers.
"""

from datetime import datetime

import pytz

from src.market_calendar import is_underlying_active_session

ET = pytz.timezone("US/Eastern")


def _et(y, mo, d, h, mi):
    return ET.localize(datetime(y, mo, d, h, mi))


# 2026-05-15 is a Friday; 2026-05-16 a Saturday; 2026-05-18 a Monday.
FRI_PRE_MARKET = _et(2026, 5, 15, 5, 0)
FRI_AT_OPEN = _et(2026, 5, 15, 9, 30)
FRI_REGULAR = _et(2026, 5, 15, 11, 0)
FRI_AT_CLOSE = _et(2026, 5, 15, 16, 0)
FRI_AFTER_HOURS = _et(2026, 5, 15, 18, 30)
FRI_AT_2000 = _et(2026, 5, 15, 20, 0)
FRI_LATE = _et(2026, 5, 15, 21, 0)
SAT = _et(2026, 5, 16, 11, 0)
MON_PRE_MARKET = _et(2026, 5, 18, 5, 0)


def test_etf_active_during_extended_hours():
    # SPY freezes at 20:00 ET — 04:00-20:00 weekdays is "active".
    assert is_underlying_active_session(FRI_PRE_MARKET, "SPY") is True
    assert is_underlying_active_session(FRI_AT_OPEN, "SPY") is True
    assert is_underlying_active_session(FRI_REGULAR, "SPY") is True
    assert is_underlying_active_session(FRI_AT_CLOSE, "SPY") is True
    assert is_underlying_active_session(FRI_AFTER_HOURS, "SPY") is True
    assert is_underlying_active_session(FRI_AT_2000, "SPY") is True


def test_etf_inactive_after_2000_and_before_0400():
    assert is_underlying_active_session(FRI_LATE, "SPY") is False
    # 03:59 ET on Monday is pre-pre-market.
    too_early = _et(2026, 5, 18, 3, 59)
    assert is_underlying_active_session(too_early, "SPY") is False
    # Exactly 04:00 ET is the boundary — active.
    assert is_underlying_active_session(MON_PRE_MARKET, "SPY") is True


def test_cash_index_active_only_during_regular_session():
    # SPX freezes at 16:00 ET and does not print pre/after-hours.
    assert is_underlying_active_session(FRI_PRE_MARKET, "SPX") is False
    assert is_underlying_active_session(FRI_AT_OPEN, "SPX") is True
    assert is_underlying_active_session(FRI_REGULAR, "SPX") is True
    assert is_underlying_active_session(FRI_AT_CLOSE, "SPX") is True
    assert is_underlying_active_session(FRI_AFTER_HOURS, "SPX") is False
    assert is_underlying_active_session(FRI_AT_2000, "SPX") is False


def test_cash_index_inactive_before_open_and_after_close():
    assert is_underlying_active_session(_et(2026, 5, 15, 9, 29), "SPX") is False
    # 16:01 ET is past cash close.
    assert is_underlying_active_session(_et(2026, 5, 15, 16, 1), "SPX") is False


def test_cash_index_detection_is_case_insensitive():
    # Canonical lower-/upper-case forms both resolve to the cash-index path.
    # (TradeStation-decorated ``$SPX.X`` is normalized to ``SPX`` upstream
    # by ``get_canonical_symbol``; this helper sees the canonical short form.)
    assert is_underlying_active_session(FRI_AFTER_HOURS, "spx") is False
    assert is_underlying_active_session(FRI_AFTER_HOURS, "SPX") is False
    assert is_underlying_active_session(FRI_AFTER_HOURS, "NDX") is False


def test_weekend_always_false():
    assert is_underlying_active_session(SAT, "SPY") is False
    assert is_underlying_active_session(SAT, "SPX") is False


def test_no_symbol_falls_back_to_extended_window():
    # When no symbol is supplied we default to the wider stocks/ETFs
    # window — caller is presumed to be in the equity path.
    assert is_underlying_active_session(FRI_AFTER_HOURS, None) is True
    assert is_underlying_active_session(FRI_LATE, None) is False


def test_unknown_symbol_uses_extended_window():
    # Anything that isn't a configured cash index gets the equity window.
    assert is_underlying_active_session(FRI_AFTER_HOURS, "AAPL") is True
    assert is_underlying_active_session(FRI_AFTER_HOURS, "QQQ") is True
