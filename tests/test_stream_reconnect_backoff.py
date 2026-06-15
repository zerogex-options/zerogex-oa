"""Reconnect backoff + non-retryable 414 handling for the stream readers.

* B8: reader reconnects use exponential backoff with jitter (capped), not a
  flat 2s, so an outage doesn't make every reader thread across every process
  reconnect in lockstep at 2s and perpetuate cap exhaustion.
* B7: a 414 (over-sized chunk URL) is deterministic and is raised as a
  non-retryable error so the reader stops instead of hot-looping the identical
  doomed request forever.
"""

import threading

import src.ingestion.stream_manager as sm
from src.ingestion.stream_manager import (
    OptionStreamAccumulator,
    _NonRetryableStreamError,
    _sleep_interruptible,
    _stream_reconnect_delay,
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
