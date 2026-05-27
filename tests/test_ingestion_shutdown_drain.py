"""Regression coverage for the symbol-worker SIGTERM drain.

Background
----------
``IngestionEngine._signal_handler`` used to only flip ``self.running = False``
and trust the consumer loop to notice. The producer
(``StreamManager.stream``) was unaware of the flag and would sit on
``self._wakeup.wait(timeout=max_wait)`` for the full poll interval —
``EXTENDED_HOURS_POLL_INTERVAL`` is 30s. That is exactly the systemd
``TimeoutStopSec``, so four of five extended-hours restarts on 2026-05-20
ended in ``status=9/KILL``. Regular-hours restarts (max_wait=5s with quotes
ticking constantly) drained cleanly — the giveaway that the wait, not the
HTTP/DB paths, was the dominant blocker.

The fix adds ``StreamManager.request_stop()`` which sets a stop latch *and*
the shared wakeup event, and wires the signal handler to call it via a
held reference to the active manager. These tests pin the contract:

  * ``request_stop`` sets both events idempotently (signal-handler safe).
  * The stream loop exits within seconds of ``request_stop``, regardless of
    how long ``max_wait`` would otherwise be — even when the session puts
    us on the 300s closed-hours wait, the loop must drain promptly.
  * The engine's signal handler propagates ``request_stop`` to whichever
    manager is currently active (and tolerates None when no stream is up).
"""

from __future__ import annotations

import signal
import threading
import time
from datetime import date, datetime

import pytz

from src.ingestion import stream_manager
from src.ingestion.main_engine import IngestionEngine
from src.ingestion.stream_manager import StreamManager

ET = pytz.timezone("US/Eastern")


# ----------------------------------------------------------------------
# request_stop primitive
# ----------------------------------------------------------------------


def test_request_stop_sets_both_events_and_is_idempotent():
    """A SIGTERM-safe stop signal: latches stop_event AND pokes wakeup so
    the idle ``_wakeup.wait`` returns immediately. Idempotent so a redelivery
    (systemd cgroup kill + parent's proc.terminate() both fire) is harmless.
    """
    mgr = object.__new__(StreamManager)
    mgr._wakeup = threading.Event()
    mgr._stop_event = threading.Event()

    assert not mgr._stop_event.is_set()
    assert not mgr._wakeup.is_set()

    mgr.request_stop()
    assert mgr._stop_event.is_set()
    assert mgr._wakeup.is_set()

    # Second call must not raise and must leave both flags set.
    mgr.request_stop()
    assert mgr._stop_event.is_set()
    assert mgr._wakeup.is_set()


# ----------------------------------------------------------------------
# stream() exits promptly on request_stop
# ----------------------------------------------------------------------


class _FakeOptionAcc:
    """Drop-in for OptionStreamAccumulator that yields nothing."""

    is_alive = True
    updates_received = 0

    def drain(self):
        return {}

    def snapshot(self):
        return {}

    def stop(self):
        pass


class _FakeUnderlyingAcc:
    """Drop-in for UnderlyingBarAccumulator that yields nothing."""

    is_alive = True
    updates_received = 0

    def drain(self):
        return None

    def stop(self):
        pass


def _stub_stream_manager_for_idle_loop(monkeypatch, session: str):
    """Build a StreamManager whose ``stream()`` enters the idle wakeup wait
    without touching any real I/O, and parks on the *session*'s ``max_wait``.

    ``after-hours`` -> 30s (the actual production failure mode), ``closed``
    -> 300s (a deliberately punishing wait that makes a regression
    unambiguous in test output).
    """
    monkeypatch.setattr(stream_manager, "get_market_session", lambda: session)
    # Cash-index off-hours: feed not expected -> no restart path, no REST.
    monkeypatch.setattr(stream_manager, "underlying_feed_expected", lambda *a, **kw: False)

    mgr = object.__new__(StreamManager)
    mgr._wakeup = threading.Event()
    mgr._stop_event = threading.Event()
    mgr.tracked_option_symbols = ["DUMMY"]
    mgr._symbol_metadata = {}
    mgr.target_expirations = [date.today()]
    mgr.last_expiration_refresh = datetime.now(ET)
    mgr.current_price = 100.0
    mgr.strike_count_max = 2
    mgr.strike_pct_range = 3.0
    mgr._accumulator = _FakeOptionAcc()
    mgr._underlying_accumulator = _FakeUnderlyingAcc()
    mgr.option_oi_coverage_alert_threshold = 0.35
    mgr.option_volume_coverage_alert_threshold = 0.35
    mgr.option_volume_warmup_minutes = 30
    mgr.option_oi_warmup_minutes = 5
    mgr.seed_rest_on_recalc = False
    # No-op hot-path helpers so stream() never reaches real I/O.
    mgr._start_accumulators = lambda *a, **kw: None
    mgr._should_refresh_expirations = lambda: False
    mgr._cleanup_expired_strikes = lambda: None
    return mgr


def _run_stream_in_thread(mgr) -> tuple[threading.Thread, threading.Event]:
    exited = threading.Event()

    def runner():
        try:
            for _ in mgr.stream(max_iterations=None):
                pass
        finally:
            exited.set()

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    return t, exited


def test_request_stop_interrupts_extended_hours_wait(monkeypatch):
    """The production failure mode: extended-hours poll interval is 30s
    (== systemd TimeoutStopSec). The loop must exit within a second of
    request_stop, NOT after the wait times out.
    """
    mgr = _stub_stream_manager_for_idle_loop(monkeypatch, session="after-hours")
    _, exited = _run_stream_in_thread(mgr)

    # Give the loop a moment to enter its _wakeup.wait().
    time.sleep(0.25)

    t0 = time.monotonic()
    mgr.request_stop()

    assert exited.wait(timeout=3.0), (
        "stream() did not exit within 3s of request_stop() — the loop is "
        "still parking on its extended-hours wait, which would get the "
        "worker SIGKILLed by systemd past TimeoutStopSec"
    )
    elapsed = time.monotonic() - t0
    # Should be milliseconds, but allow generous slack for CI scheduling.
    assert elapsed < 3.0, f"request_stop drain took {elapsed:.2f}s (expected <3s)"


def test_request_stop_interrupts_closed_hours_wait(monkeypatch):
    """Stronger version of the above: ``closed`` session sets ``max_wait``
    to 300s, so a regression here would deadlock the test for five minutes.
    Catching that explicitly keeps the failure signal loud.
    """
    mgr = _stub_stream_manager_for_idle_loop(monkeypatch, session="closed")
    _, exited = _run_stream_in_thread(mgr)

    time.sleep(0.25)

    t0 = time.monotonic()
    mgr.request_stop()

    assert exited.wait(timeout=3.0), (
        "stream() did not exit within 3s of request_stop() against a 300s "
        "closed-hours wait — the stop_event is not interrupting the loop"
    )
    elapsed = time.monotonic() - t0
    assert elapsed < 3.0, f"request_stop drain took {elapsed:.2f}s (expected <3s)"


def test_request_stop_before_loop_entry_exits_first_iteration(monkeypatch):
    """If shutdown lands BEFORE the loop ever enters its first wait (e.g.
    request_stop() called from a signal that fires while the generator is
    still being set up), the very first ``while`` predicate must short-
    circuit. No cycle of real work runs after stop is requested.
    """
    mgr = _stub_stream_manager_for_idle_loop(monkeypatch, session="after-hours")

    # Pre-arm the stop latch before stream() starts.
    mgr.request_stop()

    _, exited = _run_stream_in_thread(mgr)
    assert exited.wait(timeout=2.0), "stream() did not exit on a pre-armed stop"


# ----------------------------------------------------------------------
# IngestionEngine signal handler plumbs request_stop through
# ----------------------------------------------------------------------


class _RecordingStreamManager:
    def __init__(self):
        self.request_stop_calls = 0

    def request_stop(self):
        self.request_stop_calls += 1


def test_signal_handler_propagates_to_active_stream_manager():
    """The signal handler must (1) flip running=False and (2) wake the
    active stream manager. Without (2) the loop sits on its wakeup wait
    for up to 30s in extended hours, blowing past TimeoutStopSec."""
    engine = object.__new__(IngestionEngine)
    engine.running = True
    sm = _RecordingStreamManager()
    engine._active_stream_manager = sm

    engine._signal_handler(signal.SIGTERM, None)

    assert engine.running is False
    assert sm.request_stop_calls == 1


def test_signal_handler_tolerates_no_active_stream_manager():
    """Between run_streaming() calls the engine sits in its outer sleep
    loop with no active manager. A SIGTERM there must still flip running
    cleanly (the outer sleep will then exit on the next wake)."""
    engine = object.__new__(IngestionEngine)
    engine.running = True
    engine._active_stream_manager = None

    # Must not raise.
    engine._signal_handler(signal.SIGTERM, None)

    assert engine.running is False


def test_signal_handler_swallows_request_stop_exceptions():
    """Anything raised out of a signal handler kills the interpreter
    before the flush/pool-close finally blocks can run. A misbehaving
    StreamManager.request_stop must not be allowed to take the worker
    down before drain."""

    class _ExplodingManager:
        def request_stop(self):
            raise RuntimeError("boom")

    engine = object.__new__(IngestionEngine)
    engine.running = True
    engine._active_stream_manager = _ExplodingManager()

    # Must not raise even though request_stop did.
    engine._signal_handler(signal.SIGTERM, None)

    assert engine.running is False
