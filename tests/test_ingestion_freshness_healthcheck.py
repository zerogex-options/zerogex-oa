"""Per-symbol freshness detection: the check the QQQ outage did not have.

One ingestion worker (one child process per symbol) died at the Monday
session close and was never restarted, so QQQ wrote nothing until an operator
noticed by eye ~6 h into Tuesday's session. Every existing check asks "is the
process up" -- the parent was, so systemd, ``Restart=always``, ``OnFailure``
and the ``systemctl is-active`` liveness watchdog all reported healthy. None
asked "is the data arriving", which is the only question that distinguishes a
single dead worker from healthy siblings.

These tests pin the classification logic: stale inside the delivery window
alerts; silence outside it never does; and the staleness clock anchors at the
session OPEN so the first check after an open doesn't report the overnight gap
as a fault.
"""

from datetime import datetime, timedelta

import pytz

from src.tools.ingestion_freshness_healthcheck import evaluate

ET = pytz.timezone("US/Eastern")
TEMPLATE = "USEQ24Hour"  # 04:00-20:00 ET, as production runs
MAX_STALE = timedelta(minutes=15)


def _et(y, m, d, hh, mm):
    return ET.localize(datetime(y, m, d, hh, mm))


def test_stale_symbol_inside_session_is_flagged():
    """QQQ's actual outage shape: mid-session, hours since the last bar."""
    now = _et(2026, 8, 18, 10, 0)  # Tuesday, well inside 04:00-20:00
    last_bar = _et(2026, 8, 17, 19, 59)  # Monday's final bar

    result = evaluate("QQQ", last_bar, now, TEMPLATE, MAX_STALE)

    assert result.status == "stale"
    # Anchored at Tuesday's 04:00 open, not Monday's last bar -- the gap that
    # matters is "6h into a session with nothing", not "14h since a bar".
    assert result.stale_minutes == 360.0


def test_fresh_symbol_passes():
    now = _et(2026, 8, 18, 10, 0)
    result = evaluate("QQQ", _et(2026, 8, 18, 9, 58), now, TEMPLATE, MAX_STALE)

    assert result.status == "fresh"
    assert result.stale_minutes == 2.0


def test_silence_outside_the_delivery_window_never_alerts():
    """02:00 ET: the feed is legitimately dark, so staleness is meaningless."""
    now = _et(2026, 8, 18, 2, 0)
    result = evaluate("QQQ", _et(2026, 8, 17, 19, 59), now, TEMPLATE, MAX_STALE)

    assert result.status == "not_expected"
    assert result.stale_minutes is None


def test_weekend_never_alerts():
    now = _et(2026, 8, 15, 12, 0)  # Saturday
    result = evaluate("QQQ", _et(2026, 8, 14, 19, 59), now, TEMPLATE, MAX_STALE)

    assert result.status == "not_expected"


def test_cash_index_premarket_is_not_expected():
    """SPX prints no underlying level before 09:30 even under a 24h template.

    Without the cash-index clamp this fires a false alarm every weekday from
    04:00 to 09:30 on SPX and NDX -- four hours of daily noise, which is how
    an alert gets muted and stops being a detector at all.
    """
    now = _et(2026, 8, 18, 7, 0)
    result = evaluate("SPX", _et(2026, 8, 17, 15, 59), now, TEMPLATE, MAX_STALE)

    assert result.status == "not_expected"

    # ...but the same symbol IS checked once the cash session is open.
    open_now = _et(2026, 8, 18, 11, 0)
    stale = evaluate("SPX", _et(2026, 8, 17, 15, 59), open_now, TEMPLATE, MAX_STALE)
    assert stale.status == "stale"


def test_session_open_grace_avoids_a_spurious_alert_at_the_open():
    """At 04:05 with no bar yet, the gap is 5 min of open -- not 8 h overnight."""
    now = _et(2026, 8, 18, 4, 5)
    result = evaluate("QQQ", _et(2026, 8, 17, 19, 59), now, TEMPLATE, MAX_STALE)

    assert result.status == "fresh"
    assert result.stale_minutes == 5.0


def test_symbol_with_no_bars_at_all_is_measured_from_the_open():
    """A worker that never started writing still gets caught, not skipped."""
    now = _et(2026, 8, 18, 10, 0)
    result = evaluate("QQQ", None, now, TEMPLATE, MAX_STALE)

    assert result.status == "stale"
    assert result.stale_minutes == 360.0
    assert result.last_bar is None
