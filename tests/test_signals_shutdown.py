"""A stop request must not have to wait out a sleep.

The service sleeps until the next run window when it is outside one — hours at
a weekend. PEP 475 makes `time.sleep()` RESUME after a signal handler returns,
so SIGTERM set `running = False` and then the process went on sleeping anyway:
systemd waited out TimeoutStopSec=30 and SIGKILLed it. Four such kills appear
in the journal for 2026-09-01.

SIGKILL is not a tidiness problem. It means no cleanup runs, every restart
costs the full stop timeout, and a deploy that restarts four workers spends two
minutes hard-killing them.
"""

from __future__ import annotations

import threading
import time

import pytest

from src.signals.main_engine import SignalEngineService


@pytest.fixture
def svc(monkeypatch):
    """A service object without the real engine or signal handlers."""
    s = object.__new__(SignalEngineService)
    s.underlying = "SPX"
    s.interval_seconds = 1
    s.running = True
    return s


def test_a_long_sleep_returns_promptly_once_stopped(svc):
    """The weekend case: asked to sleep for an hour, told to stop immediately."""

    def stop_soon():
        time.sleep(0.05)
        svc.running = False

    threading.Thread(target=stop_soon, daemon=True).start()
    started = time.monotonic()
    svc._sleep(3600)
    elapsed = time.monotonic() - started

    assert elapsed < 5, f"took {elapsed:.1f}s — systemd would have SIGKILLed it"


def test_a_sleep_that_is_not_interrupted_still_sleeps(svc):
    """The fix must not turn every sleep into a busy loop: a service that never
    naps would spin the CPU and re-enter its cycle far faster than intended."""
    started = time.monotonic()
    svc._sleep(0.3)
    elapsed = time.monotonic() - started
    assert 0.25 <= elapsed < 1.5, elapsed


def test_a_sleep_requested_after_stop_returns_at_once(svc):
    svc.running = False
    started = time.monotonic()
    svc._sleep(3600)
    assert time.monotonic() - started < 1


def test_the_poll_interval_is_inside_the_stop_timeout(svc):
    """TimeoutStopSec=30 in the unit file. The slice has to leave room for the
    cycle in flight to finish too, so it must be a small fraction of that."""
    assert SignalEngineService._SHUTDOWN_POLL_SECONDS <= 5.0
