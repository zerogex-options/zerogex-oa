"""Regression coverage for the session-cumulative option OI metric.

``volume_coverage`` was made session-cumulative because the per-cycle
accumulator it counted off is rebuilt without a REST re-seed on every strike
recalibration. ``oi_coverage`` sat two lines above it and never got the same
treatment -- it stayed:

    option_with_oi / option_count      # option_count = contracts drained THIS cycle

which is wrong twice over. The denominator is the changed subset, so the
ratio swings on how many contracts happened to tick rather than on data
quality; and the numerator reads accumulator state that the recalibration
resets, with OI only ever repopulated by a REST seed that
``OPTION_REST_SEED_ON_RECALC`` disables by default.

These tests pin the replacement: cumulative over the CURRENT tracked
universe, resilient to the accumulator reset, reset at the ET day rollover,
and bounded as the strike band drifts.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from src.ingestion.stream_manager import StreamManager

DAY = datetime(2026, 8, 24, 10, 0, 0)


class _StubClient:
    def __init__(self):
        self.invalidated = 0

    def invalidate_strikes_cache(self):
        self.invalidated += 1


def _bare_manager(tracked):
    mgr = object.__new__(StreamManager)
    mgr.client = _StubClient()
    mgr.tracked_option_symbols = list(tracked)
    mgr._session_volume_symbols = set()
    mgr._session_oi_symbols = set()
    mgr._session_volume_date = None
    return mgr


def _state(with_oi, zero_oi=(), key="DailyOpenInterest"):
    state = {s: {key: 1500} for s in with_oi}
    state.update({s: {key: 0} for s in zero_oi})
    return state


def test_coverage_survives_accumulator_reset():
    """The core bug: a recalc empties the accumulator; coverage must not drop."""
    tracked = [f"O{i}" for i in range(100)]
    mgr = _bare_manager(tracked)

    assert (
        mgr._update_session_oi_coverage(_state(tracked[:60], tracked[60:]), 100, now_et=DAY)
        == 0.60
    )

    # Strike recalc rebuilt the accumulator -> empty state, no REST re-seed.
    # The old accumulator-only count collapsed to 0.0 here.
    assert mgr._update_session_oi_coverage({}, 100, now_et=DAY) == 0.60

    # 20 *different* contracts report OI after the reset -> cumulative 80.
    assert mgr._update_session_oi_coverage(_state(tracked[60:80]), 100, now_et=DAY) == 0.80


def test_denominator_is_the_tracked_universe_not_the_drained_subset():
    """Kills the old formula directly.

    Three contracts drain, all carrying OI, against a 100-contract universe.
    The replaced expression (option_with_oi / option_count) reported 100%;
    the truth is 3%.
    """
    tracked = [f"O{i}" for i in range(100)]
    mgr = _bare_manager(tracked)
    assert mgr._update_session_oi_coverage(_state(tracked[:3]), 100, now_et=DAY) == 0.03


def test_a_quiet_cycle_does_not_erase_a_healthy_reading():
    tracked = [f"O{i}" for i in range(10)]
    mgr = _bare_manager(tracked)
    assert mgr._update_session_oi_coverage(_state(tracked[:9]), 10, now_et=DAY) == 0.90
    # One contract ticks with no OI field at all -> still 0.90, not 0.0.
    assert mgr._update_session_oi_coverage({"O0": {"Bid": 1.0}}, 10, now_et=DAY) == 0.90


def test_zero_and_missing_oi_do_not_count():
    tracked = ["A", "B", "C", "D"]
    mgr = _bare_manager(tracked)
    changed = {
        "A": {"DailyOpenInterest": 10},
        "B": {"DailyOpenInterest": 0},
        "C": {"OpenInterest": None},
        "D": {"Bid": 1.0},
    }
    assert mgr._update_session_oi_coverage(changed, 4, now_et=DAY) == 0.25


def test_either_oi_field_name_counts():
    mgr = _bare_manager(["A", "B"])
    changed = {"A": {"OpenInterest": 5}, "B": {"DailyOpenInterest": 5}}
    assert mgr._update_session_oi_coverage(changed, 2, now_et=DAY) == 1.0


def test_string_valued_oi_from_the_stream_counts():
    """TradeStation sends JSON numbers as strings on the quote stream."""
    mgr = _bare_manager(["A", "B", "C"])
    changed = {"A": {"DailyOpenInterest": "1200"}, "B": {"DailyOpenInterest": "0"},
               "C": {"DailyOpenInterest": "not-a-number"}}
    assert mgr._update_session_oi_coverage(changed, 3, now_et=DAY) == pytest.approx(1 / 3)
    assert mgr._session_oi_symbols == {"A"}


def test_drifted_out_symbols_do_not_inflate_coverage():
    """Contracts that leave the band must stop counting.

    Otherwise the union pins at 100% on a trending day while the current
    band's coverage is poor -- the same failure the volume metric hit.
    """
    tracked = ["A", "B", "C", "D"]
    mgr = _bare_manager(tracked)
    assert mgr._update_session_oi_coverage(_state(tracked), 4, now_et=DAY) == 1.0

    # Spot drifts; A and B leave the band, E and F enter it with no OI yet.
    mgr.tracked_option_symbols = ["C", "D", "E", "F"]
    assert mgr._update_session_oi_coverage({}, 4, now_et=DAY) == 0.50
    assert mgr._session_oi_symbols == {"C", "D"}


def test_coverage_resets_at_day_rollover():
    tracked = [f"O{i}" for i in range(10)]
    mgr = _bare_manager(tracked)
    assert mgr._update_session_oi_coverage(_state(tracked[:8]), 10, now_et=DAY) == 0.80

    next_day = datetime(2026, 8, 25, 10, 0, 0)
    assert mgr._update_session_oi_coverage({}, 10, now_et=next_day) == 0.0
    assert mgr._session_volume_date == next_day.date()


def test_rollover_observed_by_the_oi_path_also_clears_volume():
    """One day marker, two sets: whichever metric sees the boundary first
    must roll both, and must fire the rollover side effects exactly once."""
    mgr = _bare_manager(["A"])
    mgr._update_session_volume_coverage({"A": {"Volume": 5}}, 1, now_et=DAY)
    mgr._update_session_oi_coverage({"A": {"DailyOpenInterest": 5}}, 1, now_et=DAY)
    assert mgr._session_volume_symbols == {"A"}
    assert mgr._session_oi_symbols == {"A"}
    assert mgr.client.invalidated == 1

    next_day = datetime(2026, 8, 25, 10, 0, 0)
    mgr._update_session_oi_coverage({}, 1, now_et=next_day)
    assert mgr._session_oi_symbols == set()
    assert mgr._session_volume_symbols == set()
    assert mgr.client.invalidated == 2

    # The volume path observing the same day must not re-fire the rollover.
    mgr._update_session_volume_coverage({}, 1, now_et=next_day)
    assert mgr.client.invalidated == 2


def test_empty_tracked_universe_reports_zero():
    mgr = _bare_manager([])
    assert mgr._update_session_oi_coverage({}, 0, now_et=DAY) == 0.0
