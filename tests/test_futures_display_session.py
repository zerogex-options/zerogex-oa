"""Regression coverage for the index→future DISPLAY swap timing.

The site shows a cash index (SPX/NDX/…) during the regular cash session and
through the post-close / maintenance evening hold, then swaps to the index's
CME future (SPX→@ES, …) only during the overnight futures window
(18:00 ET → next 09:30 ET) while the future is actually trading. This is
display-only and never touches persistence or the index analytics.

These predicates are load-bearing in two places that must agree: the futures
ingester (when to stream the future) and the API read swap (when to serve it).
"""

from datetime import datetime

import pytz

from src.market_calendar import (
    current_cash_close_reference,
    current_futures_session_start,
    is_futures_display_window,
    is_futures_session_open,
    should_display_future,
)
from src.symbols import get_index_futures, resolve_index_future

ET = pytz.timezone("US/Eastern")


def _et(y, mo, d, h, mi):
    return ET.localize(datetime(y, mo, d, h, mi))


# 2026-07-07 Tue, 07-08 Wed, 07-10 Fri, 07-11 Sat, 07-12 Sun, 07-13 Mon.


def test_resolve_index_future_defaults():
    assert resolve_index_future("SPX") == "@ES"
    assert resolve_index_future("NDX") == "@NQ"
    assert resolve_index_future("spy") is None  # ETF — no future
    assert resolve_index_future("") is None
    assert get_index_futures()["RUT"] == "@RTY"


def test_futures_session_open_boundaries():
    assert is_futures_session_open(_et(2026, 7, 7, 12, 0)) is True  # Tue midday
    assert is_futures_session_open(_et(2026, 7, 7, 17, 30)) is False  # maint break
    assert is_futures_session_open(_et(2026, 7, 7, 18, 0)) is True  # reopen
    assert is_futures_session_open(_et(2026, 7, 10, 17, 30)) is False  # Fri close
    assert is_futures_session_open(_et(2026, 7, 11, 12, 0)) is False  # Sat
    assert is_futures_session_open(_et(2026, 7, 12, 17, 59)) is False  # Sun pre-open
    assert is_futures_session_open(_et(2026, 7, 12, 18, 0)) is True  # Sun reopen


def test_show_index_during_cash_and_evening_hold():
    # Cash session and the 16:00-18:00 evening hold both stay on the index.
    for dt in [
        _et(2026, 7, 7, 9, 30),
        _et(2026, 7, 7, 12, 0),
        _et(2026, 7, 7, 15, 59),
        _et(2026, 7, 7, 16, 30),
        _et(2026, 7, 7, 17, 30),  # maintenance break
        _et(2026, 7, 7, 17, 59),
    ]:
        assert should_display_future("SPX", dt) is False, dt


def test_show_future_overnight():
    for dt in [
        _et(2026, 7, 7, 18, 0),  # reopen — flip to futures
        _et(2026, 7, 7, 23, 0),
        _et(2026, 7, 8, 3, 0),
        _et(2026, 7, 8, 9, 29),  # last minute before cash open
    ]:
        assert should_display_future("SPX", dt) is True, dt
    # Back to the index at the cash open.
    assert should_display_future("SPX", _et(2026, 7, 8, 9, 30)) is False


def test_weekend_stays_on_index():
    # Fri 17:00 -> Sun 18:00 the future is closed, so the frozen index shows.
    for dt in [
        _et(2026, 7, 10, 18, 0),  # Fri evening
        _et(2026, 7, 11, 3, 0),  # Sat overnight
        _et(2026, 7, 11, 20, 0),  # Sat evening
        _et(2026, 7, 12, 9, 0),  # Sun morning
        _et(2026, 7, 12, 17, 59),  # Sun just before reopen
    ]:
        assert should_display_future("SPX", dt) is False, dt
    # Sunday 18:00 reopen flips to futures.
    assert should_display_future("SPX", _et(2026, 7, 12, 18, 0)) is True


def test_only_cash_indexes_swap():
    overnight = _et(2026, 7, 7, 23, 0)
    assert should_display_future("SPX", overnight) is True
    assert should_display_future("SPY", overnight) is False  # ETF
    assert should_display_future("QQQ", overnight) is False  # ETF
    assert should_display_future(None, overnight) is False


def test_display_window_matches_should_display():
    # is_futures_display_window is the symbol-agnostic gate shared by the
    # ingester; it must agree with should_display_future for a mapped index.
    for dt in [
        _et(2026, 7, 7, 23, 0),
        _et(2026, 7, 7, 12, 0),
        _et(2026, 7, 12, 18, 0),
        _et(2026, 7, 10, 18, 0),
    ]:
        assert is_futures_display_window(dt) == should_display_future("SPX", dt), dt


def test_current_futures_session_start():
    # After 18:00 -> today's 18:00; in the 00:00-09:30 tail -> prior day's 18:00.
    def as_et(dt):
        return current_futures_session_start(dt).astimezone(ET).strftime("%Y-%m-%d %H:%M")

    assert as_et(_et(2026, 7, 7, 20, 0)) == "2026-07-07 18:00"
    assert as_et(_et(2026, 7, 8, 3, 0)) == "2026-07-07 18:00"
    assert as_et(_et(2026, 7, 8, 9, 0)) == "2026-07-07 18:00"
    assert as_et(_et(2026, 7, 8, 18, 30)) == "2026-07-08 18:00"


def test_current_cash_close_reference():
    # The overnight change is anchored to the session's 16:00 ET cash close:
    # today's 16:00 in the evening, the prior day's 16:00 in the morning tail.
    def as_et(dt):
        return current_cash_close_reference(dt).astimezone(ET).strftime("%Y-%m-%d %H:%M")

    assert as_et(_et(2026, 7, 7, 18, 30)) == "2026-07-07 16:00"  # Tue evening
    assert as_et(_et(2026, 7, 7, 23, 0)) == "2026-07-07 16:00"  # Tue late
    assert as_et(_et(2026, 7, 8, 3, 0)) == "2026-07-07 16:00"  # Wed overnight
    assert as_et(_et(2026, 7, 8, 9, 0)) == "2026-07-07 16:00"  # Wed pre-open
