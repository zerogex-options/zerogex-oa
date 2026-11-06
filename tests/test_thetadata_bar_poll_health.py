"""What the ThetaData bar poller reports when the watchdog asks.

On 2026-11-06 two workers' underlying prices stopped at 09:41 ET. The
watchdog's lines said "data-starved" and blamed TradeStation, which no
longer serves the bars, and nothing said whether ThetaData was failing
calls, answering with nothing usable, or not answering at all. Those are
three different problems with three different owners, and
``_PollingBarStream.health`` is what tells them apart.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from src.ingestion.providers import thetadata
from src.ingestion.providers.base import Bar, BarStream
from src.ingestion.providers.thetadata import _PollingBarStream


def _wait_until(condition, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


def _bar() -> Bar:
    return Bar(
        symbol="SPY",
        timestamp=datetime.now(timezone.utc),
        open=680.0,
        high=680.0,
        low=680.0,
        close=680.0,
    )


def test_answered_with_nothing_usable_says_so():
    """An index print too old to count, a refused mark, an empty answer:
    ThetaData answers and fetch returns None. Calls are not failing."""
    stream = _PollingBarStream(lambda: None, "SPX", poll_interval=0.01)
    stream.start()
    try:
        assert _wait_until(lambda: stream._polls_answered >= 3)
    finally:
        stream.stop()

    text = stream.health()
    assert "poll(s) answered in" in text
    assert "0 with a usable price, 0 failed" in text
    assert "waiting" not in text


def test_failing_calls_carry_the_error():
    def refuse():
        raise RuntimeError("StatusCode.UNAVAILABLE: terminal unreachable")

    stream = _PollingBarStream(refuse, "SPY", poll_interval=0.01)
    stream.start()
    try:
        assert _wait_until(lambda: stream._polls_failed >= 2)
    finally:
        stream.stop()

    text = stream.health()
    assert text.startswith("0 poll(s) answered")
    assert "(last: RuntimeError: StatusCode.UNAVAILABLE: terminal unreachable)" in text


def test_a_later_bar_does_not_hide_the_failures_from_health():
    """``last_error`` is cleared by a bar, which the first-bar wait relies
    on. The failure count and text in health() are not: a stream that
    failed half its polls is worth knowing about even if the last one
    worked."""
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("deadline exceeded")
        return _bar()

    stream = _PollingBarStream(flaky, "SPY", poll_interval=0.01)
    stream.start()
    try:
        assert _wait_until(lambda: stream.updates_received >= 1)
    finally:
        stream.stop()

    assert stream.last_error is None
    text = stream.health()
    assert "1 failed (last: RuntimeError: deadline exceeded)" in text
    assert "0 with a usable price" not in text


def test_a_call_that_never_comes_back_is_named(monkeypatch):
    """The silent case: the poll thread is alive, nothing fails, nothing
    arrives. Only the age of the call in progress shows it."""
    monkeypatch.setattr(thetadata, "_CALL_WAITING_REPORT_SECONDS", 0.05)
    release = threading.Event()

    def hang():
        release.wait(5)
        return None

    stream = _PollingBarStream(hang, "NDX", poll_interval=0.01)
    stream.start()
    try:
        assert _wait_until(lambda: "waiting" in stream.health())
        text = stream.health()
        assert text.startswith("0 poll(s) answered")
        assert "a call has been waiting" in text
    finally:
        release.set()
        stream.stop()


def test_a_slow_store_is_not_a_waiting_call(monkeypatch):
    """The listener stores the bar on the poll thread. Time spent there is
    the database's, and must not read as ThetaData not answering."""
    monkeypatch.setattr(thetadata, "_CALL_WAITING_REPORT_SECONDS", 0.05)
    release = threading.Event()
    stored = threading.Event()

    def slow_store(_bar):
        stored.set()
        release.wait(5)

    stream = _PollingBarStream(_bar, "SPY", poll_interval=0.01)
    stream.set_listener(slow_store)
    stream.start()
    try:
        assert stored.wait(3)
        time.sleep(0.15)
        assert "waiting" not in stream.health()
    finally:
        release.set()
        stream.stop()


def test_the_base_stream_reports_nothing():
    """TradeStation's streams do not implement health(); the watchdog's line
    must then stand alone rather than carry an empty clause."""

    class _Minimal(BarStream):
        def start(self):
            pass

        def stop(self):
            pass

        def is_alive(self):
            return True

        def drain(self):
            return None

        @property
        def updates_received(self):
            return 0

    assert _Minimal().health() == ""
