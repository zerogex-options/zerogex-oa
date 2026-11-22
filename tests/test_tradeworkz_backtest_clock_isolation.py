"""Regression: a backtest's replay clock never reaches the live bots.

``bots.base._utcnow`` read one module-level clock. A member's backtest of a
bot-only strategy runs in an API worker's thread pool, and that worker may
also run the live TradeWorkz engine, so for the whole replay every live bot
read replay time: a position past its stop and its time stop was held as
"inside its minimum hold", and a bot opening meanwhile stamped a time stop in
the past. Two concurrent replays also shared one clock cell. The clock is now
a ContextVar, seen only where it was set.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from src.tradeworkz.bots import base as bot_base

REPLAY_TIME = datetime(2026, 3, 2, 15, 0, tzinfo=timezone.utc)


def test_a_clock_set_in_one_thread_is_not_seen_by_another():
    in_replay = threading.Event()
    done = threading.Event()
    seen_inside: list[datetime] = []

    def replay():
        bot_base.set_backtest_clock(lambda: REPLAY_TIME)
        try:
            seen_inside.append(bot_base._utcnow())
            in_replay.set()
            done.wait(5)
        finally:
            bot_base.set_backtest_clock(None)

    worker = threading.Thread(target=replay)
    worker.start()
    assert in_replay.wait(5)
    try:
        # The "live engine", meanwhile, on another thread: real time.
        live_now = bot_base._utcnow()
        assert abs(live_now - datetime.now(timezone.utc)) < timedelta(seconds=5)
    finally:
        done.set()
        worker.join(5)
    assert seen_inside == [REPLAY_TIME]


def test_two_concurrent_replays_keep_their_own_time():
    barrier = threading.Barrier(2)
    results: dict[str, datetime] = {}

    def replay(name: str, t: datetime):
        bot_base.set_backtest_clock(lambda: t)
        try:
            barrier.wait(5)  # both clocks installed at once
            results[name] = bot_base._utcnow()
        finally:
            bot_base.set_backtest_clock(None)

    a = threading.Thread(target=replay, args=("a", REPLAY_TIME))
    b = threading.Thread(target=replay, args=("b", REPLAY_TIME + timedelta(days=30)))
    a.start()
    b.start()
    a.join(5)
    b.join(5)
    assert results == {"a": REPLAY_TIME, "b": REPLAY_TIME + timedelta(days=30)}


def test_setting_and_clearing_in_the_same_thread_still_works():
    bot_base.set_backtest_clock(lambda: REPLAY_TIME)
    try:
        assert bot_base._utcnow() == REPLAY_TIME
    finally:
        bot_base.set_backtest_clock(None)
    assert abs(bot_base._utcnow() - datetime.now(timezone.utc)) < timedelta(seconds=5)
