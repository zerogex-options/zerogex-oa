"""StreamManager session day-rollover hook regression tests.

The day-rollover detection in ``_update_session_volume_coverage`` is the
single per-day reset point in StreamManager.  As of #3 in the four-item
improvement plan it also invalidates the TradeStation strikes cache so
overnight chain changes (new weeklies listed) are picked up on the first
fetch after the date flips.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional
from unittest.mock import MagicMock

import pytz

from src.ingestion.stream_manager import StreamManager

ET = pytz.timezone("US/Eastern")


def _bare_stream_manager() -> StreamManager:
    sm = StreamManager.__new__(StreamManager)
    sm.client = MagicMock()
    sm.tracked_option_symbols = []
    sm._session_volume_symbols = set()
    sm._session_volume_date = None
    return sm


def test_first_observation_invalidates_cache_as_baseline():
    """The very first call (no prior date) triggers the hook with prev=None."""
    sm = _bare_stream_manager()
    now = ET.localize(datetime(2026, 6, 15, 10, 0, 0))
    sm._update_session_volume_coverage(changed_state={}, tracked_total=0, now_et=now)
    sm.client.invalidate_strikes_cache.assert_called_once()
    assert sm._session_volume_date == date(2026, 6, 15)


def test_same_day_does_not_reinvalidate():
    """Subsequent calls on the same ET day do not re-invalidate the cache."""
    sm = _bare_stream_manager()
    now = ET.localize(datetime(2026, 6, 15, 10, 0, 0))
    sm._update_session_volume_coverage(changed_state={}, tracked_total=0, now_et=now)
    sm.client.invalidate_strikes_cache.reset_mock()

    later_same_day = ET.localize(datetime(2026, 6, 15, 15, 30, 0))
    sm._update_session_volume_coverage(changed_state={}, tracked_total=0, now_et=later_same_day)
    sm.client.invalidate_strikes_cache.assert_not_called()


def test_day_rollover_invalidates_cache():
    """Crossing from one ET calendar day to the next triggers exactly one invalidation."""
    sm = _bare_stream_manager()
    # Day 1
    sm._update_session_volume_coverage(
        changed_state={},
        tracked_total=0,
        now_et=ET.localize(datetime(2026, 6, 15, 23, 59, 0)),
    )
    sm.client.invalidate_strikes_cache.reset_mock()
    # Day 2 (just after midnight ET)
    sm._update_session_volume_coverage(
        changed_state={},
        tracked_total=0,
        now_et=ET.localize(datetime(2026, 6, 16, 0, 1, 0)),
    )
    sm.client.invalidate_strikes_cache.assert_called_once()
    assert sm._session_volume_date == date(2026, 6, 16)


def test_invalidate_failure_does_not_crash_loop():
    """A cache-invalidation exception must NOT propagate out of the stream loop."""
    sm = _bare_stream_manager()
    sm.client.invalidate_strikes_cache.side_effect = RuntimeError("boom")
    # Should not raise.
    sm._update_session_volume_coverage(
        changed_state={},
        tracked_total=0,
        now_et=ET.localize(datetime(2026, 6, 15, 10, 0, 0)),
    )
    # Date marker still advanced -- next call won't loop on the same failure.
    assert sm._session_volume_date == date(2026, 6, 15)


def test_volume_symbol_set_still_cleared_on_rollover():
    """The pre-existing behaviour (clearing _session_volume_symbols) must not regress."""
    sm = _bare_stream_manager()
    sm._session_volume_date = date(2026, 6, 12)
    sm._session_volume_symbols = {"SPY 260619C600", "QQQ 260619C740"}
    sm._update_session_volume_coverage(
        changed_state={},
        tracked_total=0,
        now_et=ET.localize(datetime(2026, 6, 15, 10, 0, 0)),
    )
    assert sm._session_volume_symbols == set()
