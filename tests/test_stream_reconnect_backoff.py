"""Reconnect backoff + non-retryable 414 handling for the stream readers.

* B8: reader reconnects use exponential backoff with jitter (capped), not a
  flat 2s, so an outage doesn't make every reader thread across every process
  reconnect in lockstep at 2s and perpetuate cap exhaustion.
* B7: a 414 (over-sized chunk URL) is deterministic and is raised as a
  non-retryable error so the reader stops instead of hot-looping the identical
  doomed request forever.
* 429 stream responses honor X-RateLimit-Reset for the reconnect delay
  instead of falling into the exponential backoff that's sized for transient
  network blips — exponential 2/4/8s reconnects against a rate-limited
  endpoint waste budget and prolong the storm.
"""

import threading
import types

import requests

import src.ingestion.stream_manager as sm
from src.ingestion.stream_manager import (
    OptionStreamAccumulator,
    _NonRetryableStreamError,
    _RateLimitedStreamError,
    _sleep_interruptible,
    _stream_reconnect_delay,
    _stream_retry_delay_for_429,
)


def test_backoff_grows_and_caps():
    d1 = _stream_reconnect_delay(1)
    d2 = _stream_reconnect_delay(2)
    d3 = _stream_reconnect_delay(3)
    # Monotonic-ish growth (jitter is only +0-10%, so lower bounds increase).
    assert d1 < d2 < d3
    # Capped.
    assert _stream_reconnect_delay(50) <= sm._STREAM_RECONNECT_MAX_SECONDS * 1.1 + 1e-9


def test_sleep_interruptible_returns_early():
    import time

    flag = {"run": True}
    # Flip running off almost immediately from another thread.
    t = threading.Timer(0.05, lambda: flag.__setitem__("run", False))
    t.start()
    start = time.monotonic()
    _sleep_interruptible(10.0, lambda: flag["run"])
    elapsed = time.monotonic() - start
    t.cancel()
    assert elapsed < 2.0  # did not sleep the full 10s


class _FakeAuth:
    def get_headers(self):
        return {"Authorization": "Bearer x"}


class _FakeClient:
    base_url = "https://api.tradestation.com/v3"
    auth = _FakeAuth()


class _Resp414:
    status_code = 414

    def close(self):
        pass

    def raise_for_status(self):
        raise AssertionError("should not reach raise_for_status on 414")


def _accumulator():
    return OptionStreamAccumulator(
        _FakeClient(), ["SPY 260619C500"], max_symbols_per_connection=800
    )


def test_414_raises_non_retryable(monkeypatch):
    acc = _accumulator()
    acc._running = True
    monkeypatch.setattr(
        sm, "_requests", type("R", (), {"get": staticmethod(lambda *a, **k: _Resp414())})
    )
    try:
        acc._read_stream(0, ["SPY 260619C500"], "Option stream")
        raised = False
    except _NonRetryableStreamError:
        raised = True
    assert raised


def test_reader_loop_stops_on_non_retryable(monkeypatch):
    acc = _accumulator()
    acc._running = True
    calls = {"n": 0}

    def _boom(*a, **k):
        calls["n"] += 1
        raise _NonRetryableStreamError("414")

    monkeypatch.setattr(acc, "_read_stream", _boom)
    # Must return (break) rather than loop forever.
    acc._reader_loop(0, ["SPY 260619C500"])
    assert calls["n"] == 1


# --- 429 rate-limit handling ------------------------------------------------


class _Resp429:
    status_code = 429

    def __init__(self, headers=None):
        if headers is None:
            headers = {"X-RateLimit-Reset": "12", "X-RateLimit-Resource": "stream-quotes"}
        self.headers = headers

    def close(self):
        pass

    def raise_for_status(self):
        raise AssertionError("should not reach raise_for_status on 429")


def test_retry_delay_prefers_reset_header():
    # +0.5s cushion -> 12 + 0.5 = 12.5s
    delay = _stream_retry_delay_for_429(_Resp429())
    assert abs(delay - 12.5) < 1e-6


def test_retry_delay_caps_at_ten_minutes():
    resp = _Resp429(headers={"X-RateLimit-Reset": "99999"})
    assert _stream_retry_delay_for_429(resp) == 600.0


def test_retry_delay_falls_back_when_header_missing():
    resp = _Resp429(headers={})
    assert _stream_retry_delay_for_429(resp, default_seconds=42.0) == 42.0


def test_retry_delay_falls_back_when_header_malformed():
    resp = _Resp429(headers={"X-RateLimit-Reset": "not-a-number"})
    assert _stream_retry_delay_for_429(resp, default_seconds=30.0) == 30.0


def test_retry_delay_accepts_lowercase_header():
    resp = _Resp429(headers={"x-ratelimit-reset": "7"})
    assert abs(_stream_retry_delay_for_429(resp) - 7.5) < 1e-6


def test_429_raises_rate_limited(monkeypatch):
    acc = _accumulator()
    acc._running = True
    monkeypatch.setattr(
        sm, "_requests", type("R", (), {"get": staticmethod(lambda *a, **k: _Resp429())})
    )
    try:
        acc._read_stream(0, ["SPY 260619C500"], "Option stream")
        raised = None
    except _RateLimitedStreamError as e:
        raised = e
    assert raised is not None
    # Carries the deterministic delay from X-RateLimit-Reset (12 + 0.5).
    assert abs(raised.delay - 12.5) < 1e-6


def test_reader_loop_honors_rate_limit_delay(monkeypatch):
    """The 429 path sleeps the prescribed delay, NOT the exponential backoff,
    and does not increment consecutive_failures so an immediately-following
    transient error still gets a small first backoff (not the inflated nth)."""
    acc = _accumulator()
    acc._running = True
    sleeps = []
    calls = {"n": 0}

    def _read(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise _RateLimitedStreamError(7.5, "rate-limited")
        # Second iteration: stop the loop so the test terminates.
        acc._running = False

    monkeypatch.setattr(acc, "_read_stream", _read)
    monkeypatch.setattr(
        sm,
        "_sleep_interruptible",
        lambda secs, is_running: sleeps.append(secs),
    )

    acc._reader_loop(0, ["SPY 260619C500"])
    # First (and only) sleep is the rate-limit delay, not an exponential
    # backoff value (2.x with jitter).
    assert sleeps == [7.5]


# --- backoff resets after a healthy stream drops -----------------------------
#
# TradeStation ends long-lived streams by cutting them mid-chunk, which
# requests raises as ChunkedEncodingError("Response ended prematurely") rather
# than returning cleanly. The prod journal on 2026-10-12 showed one option
# reader counting 1 -> 2 -> 3 across connections that had each streamed for
# minutes, so every routine drop was on its way to the 60s cap.


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def monotonic(self):
        return self.t


class _DroppingResponse:
    """200 OK that streams one line, stays up ``lifetime`` seconds, then is cut."""

    status_code = 200
    headers = {}

    def __init__(self, clock, lifetime, line):
        self._clock = clock
        self._lifetime = lifetime
        self._line = line

    def raise_for_status(self):
        pass

    def iter_lines(self, decode_unicode=True):
        yield self._line
        self._clock.t += self._lifetime
        raise requests.exceptions.ChunkedEncodingError("Response ended prematurely")

    def close(self):
        pass


_QUOTE_LINE = '{"Symbol": "SPY 260619C500", "Last": "1.00"}'


def _drive(monkeypatch, acc, run_loop, attempts, max_sleeps=3):
    """Run ``run_loop`` against scripted attempts on a fake clock; return the
    reconnect delays it chose. Each attempt is a connection lifetime in seconds,
    or ``("connect_fail", seconds)`` for a connect that fails after hanging."""
    clock = _Clock()
    script = iter(attempts)

    def _get(*a, **k):
        attempt = next(script)
        if isinstance(attempt, tuple):
            clock.t += attempt[1]
            raise requests.exceptions.ConnectionError("connect timed out")
        line = _QUOTE_LINE if isinstance(acc, OptionStreamAccumulator) else "heartbeat"
        return _DroppingResponse(clock, attempt, line)

    sleeps = []

    def _sleep(secs, is_running):
        sleeps.append(secs)
        if len(sleeps) >= max_sleeps:
            acc._running = False

    monkeypatch.setattr(
        sm, "time", types.SimpleNamespace(monotonic=clock.monotonic, sleep=lambda s: None)
    )
    monkeypatch.setattr(sm, "_requests", types.SimpleNamespace(get=_get))
    monkeypatch.setattr(sm, "_sleep_interruptible", _sleep)
    acc._running = True
    acc._current_responses = [None]
    run_loop()
    return sleeps


def _is_first_backoff(delay):
    base = sm._STREAM_RECONNECT_BASE_SECONDS
    return base <= delay < base * 1.1 + 1e-9


def _escalated(sleeps):
    """Delays doubled at each step (2, 4, 8 ...), not restarted. Checked by
    size, not ordering: three jittered first-step delays can sort by chance."""
    base = sm._STREAM_RECONNECT_BASE_SECONDS
    return all(d >= base * 2**i for i, d in enumerate(sleeps))


def test_option_reader_restarts_backoff_after_each_healthy_stream_drops(monkeypatch):
    acc = _accumulator()
    sleeps = _drive(
        monkeypatch, acc, lambda: acc._reader_loop(0, ["SPY 260619C500"]), [300, 60, 300]
    )
    assert len(sleeps) == 3
    assert all(_is_first_backoff(d) for d in sleeps), sleeps


def test_option_reader_still_escalates_on_short_lived_streams(monkeypatch):
    """Streams that die within seconds (the per-account cap fingerprint) are a
    real streak and must keep backing off."""
    acc = _accumulator()
    sleeps = _drive(monkeypatch, acc, lambda: acc._reader_loop(0, ["SPY 260619C500"]), [5, 5, 5])
    assert _escalated(sleeps), sleeps


def test_option_reader_does_not_count_a_slow_failed_connect_as_healthy(monkeypatch):
    """A connect that hangs past the healthy threshold and then fails never
    opened a stream; neither it nor the earlier stream's open time may reset
    the streak."""
    acc = _accumulator()
    slow = sm._STREAM_RECONNECT_HEALTHY_SECONDS + 1
    sleeps = _drive(
        monkeypatch,
        acc,
        lambda: acc._reader_loop(0, ["SPY 260619C500"]),
        [300, ("connect_fail", slow), ("connect_fail", slow)],
    )
    assert _is_first_backoff(sleeps[0])
    assert _escalated(sleeps), sleeps


def test_underlying_reader_restarts_backoff_after_each_healthy_stream_drops(monkeypatch):
    acc = sm.UnderlyingBarAccumulator(_FakeClient(), "SPY", "SPY")
    sleeps = _drive(monkeypatch, acc, acc._reader_loop, [300, 60, 300])
    assert len(sleeps) == 3
    assert all(_is_first_backoff(d) for d in sleeps), sleeps


def test_underlying_reader_still_escalates_on_short_lived_streams(monkeypatch):
    acc = sm.UnderlyingBarAccumulator(_FakeClient(), "SPY", "SPY")
    sleeps = _drive(monkeypatch, acc, acc._reader_loop, [5, 5, 5])
    assert _escalated(sleeps), sleeps
