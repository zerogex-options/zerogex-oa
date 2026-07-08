"""Regression: reconciler._cap_time_stop_at_expiration hard-caps a signal's
``time_stop_at`` at 15:55 ET on the earliest leg expiration.

The bots each set their own ``max_hold_minutes`` (60 for most, 90 for
the wall bouncer). An entry at 15:20 ET with max_hold=60 would put
time_stop_at at 16:20 ET, 20 minutes AFTER the options market closes
for that day's expirations. The reconciler would then try to close the
position on an option that no longer trades, spread_price would return
None (stale quote), and the position would sit "open" indefinitely
showing a stale mark.

This cap makes sure time_stop_at can never fire past the last fillable
moment on expiration day. The intrinsic-fallback in spread_price
still catches positions that slip through (e.g., engine restart), but
the primary defense is not letting them slip.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from src.tradeworkz.reconciler import (
    _cap_time_stop_at_expiration,
    _earliest_leg_expiration,
    _expiration_close_cap_utc,
)

_ET = ZoneInfo("America/New_York")


def _leg(strike: float, expiration_iso: str, opt_type: str = "put") -> dict:
    return {
        "option_symbol": (
            f"SPY {expiration_iso.replace('-', '')[-6:]}"
            f"{opt_type[0].upper()}{int(strike)}"
        ),
        "side": "long",
        "option_type": opt_type,
        "strike": strike,
        "expiration": expiration_iso,
    }


def test_expiration_cap_lands_at_1555_et_in_utc():
    """15:55 ET on 2026-07-08 → 19:55 UTC (EDT is UTC-4)."""
    cap = _expiration_close_cap_utc(date(2026, 7, 8))
    expected = datetime(2026, 7, 8, 15, 55, tzinfo=_ET).astimezone(timezone.utc)
    assert cap == expected
    # Sanity: during EDT, 15:55 ET = 19:55 UTC
    assert cap.hour == 19
    assert cap.minute == 55


def test_expiration_cap_handles_est_winter():
    """15:55 ET on 2026-01-15 (EST) → 20:55 UTC."""
    cap = _expiration_close_cap_utc(date(2026, 1, 15))
    assert cap.hour == 20
    assert cap.minute == 55


def test_earliest_leg_expiration_single_leg():
    legs = [_leg(744, "2026-07-08")]
    assert _earliest_leg_expiration(legs) == date(2026, 7, 8)


def test_earliest_leg_expiration_multi_leg_picks_soonest():
    legs = [
        _leg(744, "2026-07-15", "put"),
        _leg(750, "2026-07-08", "call"),
    ]
    assert _earliest_leg_expiration(legs) == date(2026, 7, 8)


def test_earliest_leg_expiration_empty_or_unparseable_returns_none():
    assert _earliest_leg_expiration([]) is None
    assert _earliest_leg_expiration([{"expiration": "not-a-date"}]) is None
    assert _earliest_leg_expiration([{"expiration": None}]) is None


def test_cap_time_stop_hairtriggers_when_signal_would_be_after_close():
    """max_hold=60 on a 15:20 ET entry → 16:20 ET → capped to 15:55 ET."""
    signal_ts = datetime(2026, 7, 8, 20, 20, tzinfo=timezone.utc)  # 16:20 ET
    legs = [_leg(744, "2026-07-08")]
    capped = _cap_time_stop_at_expiration(signal_ts, legs)
    expected = datetime(2026, 7, 8, 15, 55, tzinfo=_ET).astimezone(timezone.utc)
    assert capped == expected
    assert capped < signal_ts


def test_cap_time_stop_leaves_conservative_signal_alone():
    """A signal ts already before 15:55 ET must NOT be pushed later."""
    # 15:40 ET on expiration day
    signal_ts = datetime(2026, 7, 8, 15, 40, tzinfo=_ET).astimezone(timezone.utc)
    legs = [_leg(744, "2026-07-08")]
    capped = _cap_time_stop_at_expiration(signal_ts, legs)
    assert capped == signal_ts


def test_cap_time_stop_none_signal_becomes_cap():
    """No bot-provided time_stop_at → we still enforce the cap."""
    legs = [_leg(744, "2026-07-08")]
    capped = _cap_time_stop_at_expiration(None, legs)
    expected = datetime(2026, 7, 8, 15, 55, tzinfo=_ET).astimezone(timezone.utc)
    assert capped == expected


def test_cap_time_stop_swing_leg_next_month_uses_that_expiration():
    """Non-0DTE swing bots targeting 30 DTE should cap at expiration day,
    not today's 15:55 ET — otherwise a 30-DTE entry would force-close
    5 minutes before today's close and never see its intended horizon."""
    exp = date.today() + timedelta(days=30)
    signal_ts = datetime.now(timezone.utc) + timedelta(days=30, hours=6)
    legs = [_leg(744, exp.isoformat())]
    capped = _cap_time_stop_at_expiration(signal_ts, legs)
    expected = datetime.combine(exp, datetime.min.time(), tzinfo=_ET).replace(
        hour=15, minute=55
    ).astimezone(timezone.utc)
    assert capped == expected


def test_cap_time_stop_no_parseable_expiration_is_noop():
    """If we can't tell when the leg expires, don't guess — trust bot."""
    signal_ts = datetime(2026, 7, 8, 20, 20, tzinfo=timezone.utc)
    legs = [{"expiration": None}]
    capped = _cap_time_stop_at_expiration(signal_ts, legs)
    assert capped == signal_ts
