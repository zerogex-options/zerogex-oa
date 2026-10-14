"""Wall-alert detection: the event model and every noise control.

Each scenario is a minute-by-minute tape against a fixed set of walls, fed
through :class:`WallAlertDetector` exactly as the service feeds it, with the
alert stream asserted in full — so a change that adds or drops an alert
anywhere shows up, not only at the line a test happens to look at.

The walls default to a 100.00 call wall / 90.00 put wall with a published
break buffer of 0.20, which under the balanced profile gives:
touch band 0.05 (5 bp), arm distance 0.30, hold distance 0.30, 5-close breaks.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable, List, Optional, Sequence, Tuple

import pytest

from src.alerts.wall_detector import (
    EVENT_BREAK,
    EVENT_HIT,
    EVENT_HOLD,
    ET,
    PROFILES,
    STATE_VERSION,
    Bar,
    WallAlertDetector,
    WallEvent,
    WallsInForce,
)

SESSION = date(2026, 10, 13)  # a Tuesday
OPEN = datetime(2026, 10, 13, 9, 30, tzinfo=ET)

WALLS = WallsInForce(
    ts=OPEN,
    call_wall=100.0,
    put_wall=90.0,
    call_strength=2.0e9,
    put_strength=1.5e9,
    break_buffer=0.20,
)


def at(minute: int) -> datetime:
    """Bar timestamp ``minute`` minutes after the 09:30 open."""
    return OPEN + timedelta(minutes=minute)


def flat(minute: int, price: float) -> Bar:
    return Bar(ts=at(minute), high=price, low=price, close=price)


def bar(minute: int, high: float, low: float, close: float) -> Bar:
    return Bar(ts=at(minute), high=high, low=low, close=close)


def run(
    tape: Iterable[Bar],
    *,
    profile: str = "balanced",
    walls: WallsInForce = WALLS,
    detector: Optional[WallAlertDetector] = None,
) -> Tuple[WallAlertDetector, List[WallEvent]]:
    det = detector or WallAlertDetector("SPY", PROFILES[profile])
    events: List[WallEvent] = []
    for b in tape:
        events.extend(det.observe(b, walls, complete=True))
    return det, events


def kinds(events: Sequence[WallEvent]) -> List[Tuple[str, str, float, int]]:
    """(kind, side, wall, minute-of-session) — the readable shape of a stream."""
    return [(e.kind, e.side, e.wall, int((e.bar_ts - OPEN).total_seconds() // 60)) for e in events]


def approach_call(start: int, *, from_price: float = 99.0, steps: int = 3) -> List[Bar]:
    """Quiet minutes well below the call wall: arms it without touching it."""
    return [flat(start + i, from_price) for i in range(steps)]


# --------------------------------------------------------------------------
# The basic lifecycle
# --------------------------------------------------------------------------


def test_hit_then_hold_is_one_hit_and_one_hold():
    tape = approach_call(5) + [
        bar(8, 99.97, 99.80, 99.90),  # within the 0.05 band of 100
        flat(9, 99.85),
        flat(10, 99.60),  # 0.40 below: past the 0.30 hold distance
    ]
    _, events = run(tape)
    assert kinds(events) == [("hit", "call", 100.0, 8), ("hold", "call", 100.0, 10)]
    hit, hold = events
    assert hit.price == pytest.approx(99.97)
    assert hit.strength == pytest.approx(2.0e9)
    assert hit.approach_number == 1 and hit.ordinal == 1
    assert hold.peak == pytest.approx(99.97)
    assert hold.minutes_since_hit == pytest.approx(2.0)
    assert hold.alerted_hit is True


def test_put_wall_mirrors_the_call_wall():
    tape = [flat(5 + i, 91.0) for i in range(3)] + [
        bar(8, 90.20, 90.03, 90.10),  # low within 0.05 of 90
        flat(9, 90.15),
        flat(10, 90.40),
    ]
    _, events = run(tape)
    assert kinds(events) == [("hit", "put", 90.0, 8), ("hold", "put", 90.0, 10)]


def test_break_needs_consecutive_closes_beyond_the_buffer():
    tape = approach_call(5) + [
        bar(8, 100.10, 99.90, 100.05),
        flat(9, 100.25),  # 1 beyond 100.20
        flat(10, 100.30),  # 2
        flat(11, 100.30),  # 3
        flat(12, 100.40),  # 4
        flat(13, 100.50),  # 5 -> confirmed
    ]
    _, events = run(tape)
    assert kinds(events) == [("hit", "call", 100.0, 8), ("break", "call", 100.0, 13)]
    brk = events[1]
    assert brk.price == pytest.approx(100.50)
    assert brk.confirm_minutes == 5
    assert brk.break_buffer == pytest.approx(0.20)


def test_failed_breakout_resets_the_run_and_is_not_a_break():
    # Four closes beyond the buffer, then one back inside: the run resets.
    tape = approach_call(5) + [
        bar(8, 100.10, 99.90, 100.05),
        flat(9, 100.25),
        flat(10, 100.30),
        flat(11, 100.30),
        flat(12, 100.30),
        flat(13, 100.10),  # back inside the buffer -> run resets
        flat(14, 100.30),
        flat(15, 100.30),
        flat(16, 100.30),
        flat(17, 100.30),
    ]
    _, events = run(tape)
    assert kinds(events) == [("hit", "call", 100.0, 8)]


def test_a_broken_wall_is_spent_for_the_session():
    tape = approach_call(5) + [bar(8, 100.10, 99.90, 100.05)]
    tape += [flat(9 + i, 100.40) for i in range(5)]  # break at minute 13
    tape += [flat(14 + i, 99.0) for i in range(5)]  # fall far back below: would arm
    tape += [bar(20, 100.0, 99.9, 99.95)]  # touch again
    _, events = run(tape)
    assert kinds(events) == [("hit", "call", 100.0, 8), ("break", "call", 100.0, 13)]


# --------------------------------------------------------------------------
# Chop
# --------------------------------------------------------------------------


def test_chop_at_the_wall_is_one_hit():
    """Forty minutes oscillating across the band: one alert, not forty."""
    tape = approach_call(5)
    for i in range(40):
        tape.append(bar(8 + i, 100.04 if i % 2 else 99.99, 99.85, 99.92 if i % 2 else 99.98))
    _, events = run(tape)
    assert kinds(events) == [("hit", "call", 100.0, 8)]


def test_a_retreat_short_of_the_arm_distance_does_not_rearm():
    tape = approach_call(5) + [
        bar(8, 99.98, 99.90, 99.95),  # hit
        flat(9, 99.75),  # 0.25 below: short of the 0.30 hold, short of the 0.30 arm
        flat(10, 99.80),
        bar(11, 99.99, 99.80, 99.90),  # back at the band: same test, no new hit
    ]
    tape += [flat(12 + i, 99.80) for i in range(30)]
    tape += [bar(42, 99.99, 99.85, 99.90)]
    _, events = run(tape)
    assert kinds(events) == [("hit", "call", 100.0, 8)]


def test_cooldown_suppresses_a_quick_retest_but_not_a_later_one():
    tape = approach_call(5) + [
        bar(8, 99.98, 99.90, 99.95),  # hit 1 (alerted)
        flat(9, 99.60),  # hold -> alerted, wall re-arms next bar
        flat(10, 99.60),
        bar(11, 99.98, 99.90, 99.95),  # hit 2 at +3 min: inside the 20-min cooldown
        flat(12, 99.60),  # its hold is quiet too (its hit was not alerted)
        flat(13, 99.60),
    ]
    tape += [flat(14 + i, 99.60) for i in range(14)]  # through minute 27
    tape += [bar(29, 99.98, 99.90, 99.95)]  # 21 min after hit 1: alerted again
    _, events = run(tape)
    assert kinds(events) == [
        ("hit", "call", 100.0, 8),
        ("hold", "call", 100.0, 9),
        ("hit", "call", 100.0, 29),
    ]
    assert events[2].approach_number == 3
    assert events[2].ordinal == 3


def test_cooldown_never_swallows_a_break():
    tape = approach_call(5) + [
        bar(8, 99.98, 99.90, 99.95),  # hit 1 (alerted)
        flat(9, 99.60),  # hold
        flat(10, 99.60),
        bar(11, 100.10, 99.90, 100.05),  # quiet hit inside the cooldown
    ]
    tape += [flat(12 + i, 100.40) for i in range(5)]  # break at minute 16
    _, events = run(tape)
    assert kinds(events) == [
        ("hit", "call", 100.0, 8),
        ("hold", "call", 100.0, 9),
        ("break", "call", 100.0, 16),
    ]
    assert events[2].alerted_hit is False


def test_responsive_alerts_earlier_and_conservative_later():
    approach = approach_call(5)
    near = [bar(8, 99.94, 99.90, 99.92)]  # 0.06 away: inside 8 bp, outside 5 bp
    _, responsive = run(approach + near, profile="responsive")
    _, balanced = run(approach + near, profile="balanced")
    assert kinds(responsive) == [("hit", "call", 100.0, 8)]
    assert balanced == []

    # Conservative needs ten closes for the break the balanced profile takes on five.
    tape = approach + [bar(8, 100.0, 99.9, 99.98)] + [flat(9 + i, 100.40) for i in range(10)]
    _, cons = run(tape, profile="conservative")
    _, bal = run(tape, profile="balanced")
    assert kinds(bal)[-1] == ("break", "call", 100.0, 13)
    assert kinds(cons)[-1] == ("break", "call", 100.0, 18)


# --------------------------------------------------------------------------
# Arming and re-picked walls
# --------------------------------------------------------------------------


def test_a_wall_that_was_never_approached_does_not_alert_a_hit():
    """Price opens AT the wall: nothing has arrived, so no hit."""
    tape = [bar(5 + i, 100.02, 99.97, 99.99) for i in range(10)]
    _, events = run(tape)
    assert events == []


def test_a_wall_that_chopped_all_morning_still_alerts_its_break():
    """The arming rule decides the hit, never the break."""
    tape = [bar(5 + i, 100.02, 99.97, 99.99) for i in range(30)]
    tape += [flat(35 + i, 100.40) for i in range(5)]
    _, events = run(tape)
    assert kinds(events) == [("break", "call", 100.0, 39)]
    assert events[0].alerted_hit is False
    assert events[0].approach_number is None


def test_a_wall_repicked_onto_price_starts_disarmed():
    det = WallAlertDetector("SPY", PROFILES["balanced"])
    events: List[WallEvent] = []
    # Call wall is 101 while price sits at 99.98, far below it: 101 arms.
    for i in range(5):
        events += det.observe(
            flat(5 + i, 99.98), WallsInForce(OPEN, 101.0, 90.0, break_buffer=0.2), complete=True
        )
    # The walls are re-picked onto 100.00 — where price already is.
    moved = WallsInForce(OPEN, 100.0, 90.0, break_buffer=0.2)
    events += det.observe(bar(10, 100.0, 99.95, 99.98), moved, complete=True)
    assert events == []
    # Price backs off, then returns: now it is an arrival.
    for i in range(3):
        events += det.observe(flat(11 + i, 99.50), moved, complete=True)
    events += det.observe(bar(14, 99.99, 99.90, 99.95), moved, complete=True)
    assert kinds(events) == [("hit", "call", 100.0, 14)]


def test_a_test_resolves_even_after_its_wall_is_repicked():
    """The break of 100 is still a break after the call wall moves up to 101."""
    det = WallAlertDetector("SPY", PROFILES["balanced"])
    events: List[WallEvent] = []
    for b in approach_call(5):
        events += det.observe(b, WALLS, complete=True)
    events += det.observe(bar(8, 100.10, 99.90, 100.05), WALLS, complete=True)
    moved = WallsInForce(OPEN, 101.0, 100.0, break_buffer=0.2)  # 100 flips to the put side
    for i in range(5):
        events += det.observe(flat(9 + i, 100.40), moved, complete=True)
    assert kinds(events) == [("hit", "call", 100.0, 8), ("break", "call", 100.0, 13)]


def test_open_grace_quiets_the_hit_but_not_the_break():
    tape = [flat(0, 99.0)] + [bar(1, 100.0, 99.0, 99.98)]  # touch at 09:31, inside 3-min grace
    tape += [flat(2 + i, 100.40) for i in range(5)]
    _, events = run(tape)
    assert kinds(events) == [("break", "call", 100.0, 6)]


def test_an_unresolved_test_lapses_silently_and_needs_a_new_approach():
    tape = approach_call(5) + [bar(8, 99.98, 99.90, 99.95)]  # hit
    tape += [bar(9 + i, 99.99, 99.85, 99.90) for i in range(70)]  # sit at the wall > 60 min
    _, events = run(tape)
    assert kinds(events) == [("hit", "call", 100.0, 8)]


# --------------------------------------------------------------------------
# The in-progress bar and replay safety
# --------------------------------------------------------------------------


def test_a_hit_is_seen_on_the_in_progress_bar_and_not_repeated_when_it_completes():
    det = WallAlertDetector("SPY", PROFILES["balanced"])
    for b in approach_call(5):
        det.observe(b, WALLS, complete=True)
    partial = det.observe(bar(8, 99.97, 99.90, 99.96), WALLS, complete=False)
    again = det.observe(bar(8, 99.98, 99.90, 99.96), WALLS, complete=False)
    final = det.observe(bar(8, 99.99, 99.90, 99.92), WALLS, complete=True)
    assert kinds(partial) == [("hit", "call", 100.0, 8)]
    assert again == [] and final == []


def test_completed_bars_are_applied_once():
    det, events = run(approach_call(5) + [bar(8, 99.98, 99.90, 99.95)])
    assert len(events) == 1
    # Re-feeding an overlapping window (as the service does every tick) is a no-op.
    _, replayed = run(approach_call(5) + [bar(8, 99.98, 99.90, 99.95)], detector=det)
    assert replayed == []


def test_state_round_trips_mid_test():
    tape = approach_call(5) + [bar(8, 100.10, 99.90, 100.05), flat(9, 100.30), flat(10, 100.30)]
    det, events = run(tape)
    assert kinds(events) == [("hit", "call", 100.0, 8)]

    restored = WallAlertDetector("SPY", PROFILES["balanced"])
    assert restored.restore(det.to_state()) is True
    assert restored.to_state() == det.to_state()
    _, rest = run([flat(11, 100.30), flat(12, 100.30), flat(13, 100.30)], detector=restored)
    assert kinds(rest) == [("break", "call", 100.0, 13)]


def test_restore_refuses_a_foreign_or_stale_state():
    det, _ = run(approach_call(5))
    state = det.to_state()
    other = WallAlertDetector("SPY", PROFILES["conservative"])
    assert other.restore(state) is False
    assert other.restore({**state, "version": STATE_VERSION + 1}) is False
    assert other.restore({"garbage": True}) is False


def test_a_new_session_starts_clean():
    det, events = run(approach_call(5) + [bar(8, 99.98, 99.90, 99.95)])
    assert len(events) == 1
    tomorrow = OPEN + timedelta(days=1)
    nxt = [Bar(tomorrow + timedelta(minutes=5 + i), 99.0, 99.0, 99.0) for i in range(3)]
    nxt.append(Bar(tomorrow + timedelta(minutes=8), 99.98, 99.90, 99.95))
    _, events = run(nxt, detector=det)
    assert [e.kind for e in events] == [EVENT_HIT]
    assert events[0].session_date == date(2026, 10, 14)
    assert events[0].ordinal == 1


def test_missing_break_buffer_falls_back_to_the_platform_floor():
    walls = WallsInForce(OPEN, 100.0, 90.0)  # pre-column row: no buffer
    tape = approach_call(5) + [bar(8, 100.0, 99.9, 99.98)] + [flat(9 + i, 100.15) for i in range(5)]
    _, events = run(tape, walls=walls)
    # Floor is 0.1% of price ~= 0.10, so 100.15 closes are beyond it.
    assert [e.kind for e in events] == [EVENT_HIT, EVENT_BREAK]
    assert events[1].break_buffer == pytest.approx(0.0999, abs=1e-3)


def test_event_keys_are_unique_within_a_session():
    tape = approach_call(5) + [bar(8, 99.98, 99.90, 99.95), flat(9, 99.60), flat(10, 99.60)]
    tape += [flat(11 + i, 99.60) for i in range(20)]
    tape += [bar(31, 99.98, 99.90, 99.95), flat(32, 99.60)]
    _, events = run(tape)
    assert [e.kind for e in events] == [EVENT_HIT, EVENT_HOLD, EVENT_HIT, EVENT_HOLD]
    assert len({e.key for e in events}) == len(events)
