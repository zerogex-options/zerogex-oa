"""On an early-close day the end-of-day ramps count down to 13:00 ET.

EOD Pressure's time ramp, market_pressure's vanna/charm ramps and the 0DTE
imbalance time-of-day weight all counted to 16:00 every day. On 2026-11-27
(13:00 close) EOD Pressure read zero through the real last 90 minutes and
switched on from 14:30 to 16:00, after the close.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
import pytz

from src import market_calendar as mc
from src.signals.playbook.context_builder import ADVANCED_SIGNAL_NAMES  # noqa: F401 (import order)
from src.signals.advanced.eod_pressure import EODPressureSignal
from src.signals.components.utils import session_close_minute_et

ET = pytz.timezone("US/Eastern")
HALF_DAY = date(2026, 11, 27)


@pytest.fixture(autouse=True)
def _half_day(monkeypatch):
    monkeypatch.setattr(mc, "NYSE_HALF_DAYS", {HALF_DAY})


def _et(day: date, hour: int, minute: int) -> datetime:
    return ET.localize(datetime(day.year, day.month, day.day, hour, minute))


def test_the_close_minute_follows_the_calendar():
    assert session_close_minute_et(_et(HALF_DAY, 11, 0)) == 13 * 60
    assert session_close_minute_et(_et(date(2026, 11, 25), 11, 0)) == 16 * 60
    assert session_close_minute_et(None) == 16 * 60


def test_eod_ramp_runs_into_the_half_day_close_and_stops_after_it():
    ramp = EODPressureSignal._time_ramp
    # The last minutes before 13:00 are inside the window, as 15:5x are normally.
    assert ramp(_et(HALF_DAY, 12, 55)) == pytest.approx(ramp(_et(date(2026, 11, 25), 15, 55)))
    assert ramp(_et(HALF_DAY, 12, 55)) > 0.0
    # After the 13:00 close it is off, where it used to be ramping up.
    assert ramp(_et(HALF_DAY, 14, 45)) == 0.0
    assert ramp(_et(HALF_DAY, 13, 0)) == 0.0
