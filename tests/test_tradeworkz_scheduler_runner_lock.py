"""Only one API worker runs TradeWorkz ticks.

``uvicorn --workers N`` runs the FastAPI lifespan, and so starts the TradeWorkz
scheduler, once per worker process. Nothing in a tick stops two workers from
opening the same position in the same second, so only the scheduler holding the
flock on ``scheduler._RUNNER_LOCK_PATH`` ticks.

Most tests use two schedulers in one process to stand in for two workers: a
flock belongs to the open file description, so two separate opens conflict
just as two processes do. One test uses a real second process anyway, since
that is the case production depends on.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

from src.tradeworkz import config as tw_config
from src.tradeworkz import scheduler as sched_mod

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def lock_path(tmp_path, monkeypatch):
    path = tmp_path / "runner.lock"
    monkeypatch.setattr(sched_mod, "_RUNNER_LOCK_PATH", str(path))
    return path


@pytest.fixture
def pair(lock_path):
    a = sched_mod.TradeWorkzScheduler()
    b = sched_mod.TradeWorkzScheduler()
    yield a, b
    a._release_runner_lock()
    b._release_runner_lock()


def test_only_one_scheduler_holds_the_lock(pair):
    a, b = pair
    assert a._holds_runner_lock() is True
    assert b._holds_runner_lock() is False
    # The holder keeps it, and the standby keeps being refused, on every check.
    assert a._holds_runner_lock() is True
    assert b._holds_runner_lock() is False


def test_a_standby_takes_over_when_the_runner_lets_go(pair):
    a, b = pair
    assert a._holds_runner_lock()
    assert not b._holds_runner_lock()
    a._release_runner_lock()  # what the kernel does when a worker process exits
    assert b._holds_runner_lock()
    assert not a._holds_runner_lock()


def test_a_second_process_is_refused_while_this_one_holds_it(pair, lock_path):
    a, _ = pair
    probe = (
        "import sys\n"
        "from src.tradeworkz import scheduler as s\n"
        "s._RUNNER_LOCK_PATH = sys.argv[1]\n"
        "print(s.TradeWorkzScheduler()._holds_runner_lock())\n"
    )

    def other_process_gets_it() -> str:
        result = subprocess.run(
            [sys.executable, "-c", probe, str(lock_path)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        )
        return result.stdout.strip().splitlines()[-1]

    assert a._holds_runner_lock()
    assert other_process_gets_it() == "False"
    a._release_runner_lock()
    assert other_process_gets_it() == "True"


def test_fails_open_when_the_lock_cannot_be_opened(tmp_path, monkeypatch):
    missing = tmp_path / "no-such-dir" / "runner.lock"
    monkeypatch.setattr(sched_mod, "_RUNNER_LOCK_PATH", str(missing))
    a = sched_mod.TradeWorkzScheduler()
    b = sched_mod.TradeWorkzScheduler()
    # Both tick, as every worker did before the lock: duplicate ticks are the
    # lesser failure next to TradeWorkz stopping without a word.
    assert a._holds_runner_lock() is True
    assert b._holds_runner_lock() is True


def test_only_the_lock_holder_ticks(pair, monkeypatch):
    a, b = pair
    monkeypatch.setattr(tw_config, "ENGINE_INTERVAL_SECONDS", 0.01)
    ticks = {"a": 0, "b": 0}

    async def tick_a():
        ticks["a"] += 1

    async def tick_b():
        ticks["b"] += 1

    a._tick_and_sweep = tick_a
    b._tick_and_sweep = tick_b

    async def run():
        a._stop_event = asyncio.Event()
        b._stop_event = asyncio.Event()
        loops = [asyncio.create_task(a._loop()), asyncio.create_task(b._loop())]
        await asyncio.sleep(0.1)
        a._stop_event.set()
        b._stop_event.set()
        await asyncio.gather(*loops)

    asyncio.run(run())
    assert ticks["a"] > 1
    assert ticks["b"] == 0


def test_stop_hands_the_lock_to_a_standby(pair, monkeypatch):
    a, b = pair
    monkeypatch.setattr(tw_config, "ENGINE_ENABLED", True)
    monkeypatch.setattr(tw_config, "ENGINE_INTERVAL_SECONDS", 0.01)

    async def no_tick():
        return None

    a._tick_and_sweep = no_tick

    async def run():
        a.start()
        await asyncio.sleep(0.05)
        assert not b._holds_runner_lock()
        await a.stop()
        assert b._holds_runner_lock()

    asyncio.run(run())
