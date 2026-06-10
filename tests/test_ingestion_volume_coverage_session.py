"""Regression coverage for the session-cumulative option volume metric.

Before the fix, ``volume_coverage`` was counted directly off the per-cycle
``OptionStreamAccumulator``. That accumulator is torn down and rebuilt WITHOUT
a REST re-seed every ``STRIKE_RECALC_INTERVAL`` (~60s at the default 5s poll),
zeroing its in-memory ``Volume`` on each recalc. The metric therefore only ever
reflected ~1 minute of trades and chronically tripped the 35% "Low option
volume coverage" alert (observed 0.4-16% while the persisted data showed ~80%).

The fix accumulates a session-scoped set of symbols-seen-with-volume on the
``StreamManager`` itself, which survives the accumulator swaps. These tests pin
that contract: coverage must not collapse when the accumulator state resets, it
must reset at the ET day rollover, and it must stay bounded as the strike band
drifts through the session.
"""

from __future__ import annotations

from datetime import datetime

from src.ingestion.stream_manager import StreamManager

DAY = datetime(2026, 5, 28, 10, 0, 0)


def _bare_manager(tracked):
    """A StreamManager with only the attributes the helper touches."""
    mgr = object.__new__(StreamManager)
    mgr.tracked_option_symbols = list(tracked)
    mgr._session_volume_symbols = set()
    mgr._session_volume_date = None
    return mgr


def _state(with_volume, zero_volume=()):
    state = {s: {"Volume": 100} for s in with_volume}
    state.update({s: {"Volume": 0} for s in zero_volume})
    return state


def test_coverage_survives_accumulator_reset():
    """The core bug: a recalc empties the accumulator; coverage must not drop."""
    tracked = [f"O{i}" for i in range(100)]
    mgr = _bare_manager(tracked)

    # Cycle 1: 60 of 100 contracts have traded.
    assert (
        mgr._update_session_volume_coverage(_state(tracked[:60], tracked[60:]), 100, now_et=DAY)
        == 0.60
    )

    # Cycle 2: strike recalc just rebuilt the accumulator -> empty state.
    # The old accumulator-only count would collapse to 0.0 here.
    assert mgr._update_session_volume_coverage({}, 100, now_et=DAY) == 0.60

    # Cycle 3: 20 *different* contracts trade after the reset -> cumulative 80.
    assert mgr._update_session_volume_coverage(_state(tracked[60:80]), 100, now_et=DAY) == 0.80


def test_coverage_resets_at_day_rollover():
    tracked = [f"O{i}" for i in range(10)]
    mgr = _bare_manager(tracked)
    assert mgr._update_session_volume_coverage(_state(tracked[:8]), 10, now_et=DAY) == 0.80

    next_day = datetime(2026, 5, 29, 10, 0, 0)
    # New session, accumulator empty at open -> coverage resets to 0.
    assert mgr._update_session_volume_coverage({}, 10, now_et=next_day) == 0.0
    assert mgr._session_volume_date == next_day.date()


def test_coverage_capped_at_one_as_band_drifts():
    # The tracked band re-centres on spot through the day, so the union of all
    # contracts that ever traded can exceed the current snapshot size; the
    # reported coverage saturates at 1.0 rather than overflowing.
    mgr = _bare_manager([f"O{i}" for i in range(10)])
    drifted = [f"D{i}" for i in range(15)]  # 15 distinct contracts over the session
    assert mgr._update_session_volume_coverage(_state(drifted), 10, now_et=DAY) == 1.0


def test_zero_tracked_total_is_safe():
    mgr = _bare_manager([])
    assert mgr._update_session_volume_coverage(_state(["X"]), 0, now_et=DAY) == 0.0
