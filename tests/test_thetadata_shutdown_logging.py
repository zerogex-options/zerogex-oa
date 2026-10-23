"""A call cut off by shutdown logs at DEBUG; a call that fails while running
stays a WARNING.

Every ``make services-restart`` used to lead "Recent Warnings" with the
stopping process's in-flight gRPC calls -- "thetadata ohlc snapshot failed
for NDX ...", "thetadata bar poll for SPX failed ..." -- cancelled as it went
down. Nothing was wrong with the feed, and a warning that shows up on every
deploy trains the reader to skip the one that matters.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import CancelledError
from datetime import date
from types import SimpleNamespace

import pytest

from src.ingestion.providers.thetadata import (
    ThetaDataProvider,
    _failure_level,
    _log_snapshot_failure,
    _PollingBarStream,
    _PollingOptionQuoteStream,
    build_occ_symbol,
)

LOGGER = "src.ingestion.providers.thetadata"
EXP = date(2026, 12, 18)


class _RpcError(Exception):
    """Shaped like grpc's ``_MultiThreadedRendezvous``: the status is on
    ``code()``. The message deliberately omits the status, so these tests
    exercise ``code()`` rather than the text fallback."""

    def __init__(self, status: str):
        super().__init__("<_MultiThreadedRendezvous of RPC that terminated with:")
        self._status = SimpleNamespace(name=status)

    def code(self):
        return self._status


@pytest.fixture
def records(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    return caplog


def _failures(caplog, text):
    return [r.levelno for r in caplog.records if r.name == LOGGER and text in r.getMessage()]


def _wait_for(predicate, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------


def test_a_failure_while_running_is_a_warning():
    assert _failure_level(RuntimeError("terminal down"), stopping=False) == logging.WARNING


def test_any_failure_while_stopping_is_debug():
    assert _failure_level(RuntimeError("terminal down"), stopping=True) == logging.DEBUG


def test_a_cancelled_grpc_call_is_debug_even_before_the_stop_flag():
    """The status is set by whoever abandoned the call, so it names a
    teardown even when it lands before stop() does."""
    assert _failure_level(_RpcError("CANCELLED"), stopping=False) == logging.DEBUG


def test_other_grpc_statuses_stay_warnings():
    for status in ("UNAVAILABLE", "DEADLINE_EXCEEDED", "UNAUTHENTICATED", "INVALID_ARGUMENT"):
        assert _failure_level(_RpcError(status), stopping=False) == logging.WARNING, status


def test_a_cancelled_status_is_read_from_the_text_when_code_is_absent():
    wrapped = RuntimeError(
        "<_MultiThreadedRendezvous of RPC that terminated with:\n"
        '\tstatus = StatusCode.CANCELLED\n\tdetails = "Channel closed!"\n>'
    )
    assert _failure_level(wrapped, stopping=False) == logging.DEBUG


def test_a_cancelled_future_is_debug():
    assert _failure_level(CancelledError(), stopping=False) == logging.DEBUG


def test_a_code_that_raises_does_not_break_the_log():
    class _Odd(Exception):
        def code(self):
            raise RuntimeError("no status")

    assert _failure_level(_Odd("boom"), stopping=False) == logging.WARNING


def test_snapshot_failure_level_follows_the_stop_flag(records):
    _log_snapshot_failure("ohlc", "NDX", EXP, RuntimeError("x"))
    _log_snapshot_failure("ohlc", "NDX", EXP, RuntimeError("x"), stopping=True)
    assert _failures(records, "ohlc snapshot failed for NDX") == [logging.WARNING, logging.DEBUG]


# ---------------------------------------------------------------------------
# _gather
# ---------------------------------------------------------------------------


class _Chain:
    """quote answers; ohlc runs ``on_ohlc`` and then raises."""

    def __init__(self, on_ohlc=lambda: None, error=None):
        self._on_ohlc = on_ohlc
        self._error = error or RuntimeError("ohlc refused")

    def option_snapshot_quote(self, **kw):
        return [{"strike": 650.0, "right": "CALL", "bid": 1.2, "ask": 1.3}]

    def option_snapshot_ohlc(self, **kw):
        self._on_ohlc()
        raise self._error


SYMBOL = build_occ_symbol("NDX", EXP, 650.0, "C")


@pytest.mark.parametrize("concurrency", [1, 4])
def test_a_snapshot_failing_while_running_is_a_warning(records, concurrency):
    provider = ThetaDataProvider(_Chain(), max_concurrency=concurrency)
    try:
        state = provider.fetch_chain_state([SYMBOL], include_open_interest=False)
    finally:
        provider.close()
    assert state[SYMBOL]["bid"] == pytest.approx(1.2)
    assert _failures(records, "ohlc snapshot failed") == [logging.WARNING]


@pytest.mark.parametrize("concurrency", [1, 4])
def test_a_snapshot_failing_after_the_caller_stops_is_debug(records, concurrency):
    stopped = threading.Event()
    provider = ThetaDataProvider(_Chain(on_ohlc=stopped.set), max_concurrency=concurrency)
    try:
        provider.fetch_chain_state([SYMBOL], include_open_interest=False, stopping=stopped.is_set)
    finally:
        provider.close()
    assert _failures(records, "ohlc snapshot failed") == [logging.DEBUG]


def test_a_snapshot_failing_after_close_is_debug(records):
    client = _Chain()
    provider = ThetaDataProvider(client, max_concurrency=1)
    client._on_ohlc = provider.close
    provider.fetch_chain_state([SYMBOL], include_open_interest=False)
    assert _failures(records, "ohlc snapshot failed") == [logging.DEBUG]


def test_a_cancelled_snapshot_is_debug_while_running(records):
    provider = ThetaDataProvider(_Chain(error=_RpcError("CANCELLED")), max_concurrency=4)
    try:
        provider.fetch_chain_state([SYMBOL], include_open_interest=False)
    finally:
        provider.close()
    assert _failures(records, "ohlc snapshot failed") == [logging.DEBUG]


# ---------------------------------------------------------------------------
# The pollers
# ---------------------------------------------------------------------------


def test_a_bar_poll_failing_while_running_is_a_warning(records):
    calls = []

    def fetch():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("terminal down")
        return None

    stream = _PollingBarStream(fetch, "SPX", poll_interval=0.01)
    stream.start()
    try:
        assert _wait_for(lambda: len(calls) >= 2)
    finally:
        stream.stop()
    assert _failures(records, "bar poll for SPX failed") == [logging.WARNING]


def test_a_bar_poll_cut_off_by_stop_is_debug(records):
    entered, release = threading.Event(), threading.Event()

    def fetch():
        entered.set()
        release.wait(5)
        raise RuntimeError("Channel closed!")

    stream = _PollingBarStream(fetch, "SPX", poll_interval=0.01)
    stream.start()
    assert entered.wait(3)
    stopper = threading.Thread(target=stream.stop)
    stopper.start()
    assert _wait_for(lambda: not stream._running)
    release.set()
    stopper.join(5)
    assert _failures(records, "bar poll for SPX failed") == [logging.DEBUG]


class _ChainSource:
    """Stands in for the provider behind an option stream."""

    def __init__(self, fetch):
        self._fetch = fetch

    def fetch_chain_state(self, symbols, include_open_interest=True, stopping=None):
        return self._fetch(stopping)


def test_an_option_poll_failing_while_running_is_a_warning(records):
    calls = []

    def fetch(stopping):
        calls.append(stopping())
        if len(calls) == 1:
            raise RuntimeError("terminal down")
        return {}

    stream = _PollingOptionQuoteStream(
        _ChainSource(fetch), [SYMBOL], poll_interval=0.01, oi_poll_interval=60
    )
    stream.start(seed_from_snapshot=False)
    try:
        assert _wait_for(lambda: len(calls) >= 2)
    finally:
        stream.stop()
    assert calls[0] is False, "a running stream must not report itself stopping"
    assert _failures(records, "thetadata poll failed") == [logging.WARNING]


def test_an_option_poll_cut_off_by_stop_is_debug(records):
    entered, release = threading.Event(), threading.Event()
    seen = []

    def fetch(stopping):
        entered.set()
        release.wait(5)
        seen.append(stopping())
        raise RuntimeError("Channel closed!")

    stream = _PollingOptionQuoteStream(
        _ChainSource(fetch), [SYMBOL], poll_interval=0.01, oi_poll_interval=60
    )
    stream.start(seed_from_snapshot=False)
    assert entered.wait(3)
    stopper = threading.Thread(target=stream.stop)
    stopper.start()
    assert _wait_for(lambda: stream._stopped)
    release.set()
    stopper.join(5)
    # The flag reaches the provider too, so _gather's per-endpoint lines
    # for the same cut-off call are quiet as well.
    assert seen == [True]
    assert _failures(records, "thetadata poll failed") == [logging.DEBUG]


def test_the_seed_poll_is_not_mistaken_for_shutdown(records):
    """start() seeds before raising the running flag; its failures are real."""
    seen = []

    def fetch(stopping):
        seen.append(stopping())
        return {}

    stream = _PollingOptionQuoteStream(
        _ChainSource(fetch), [SYMBOL], poll_interval=0.01, oi_poll_interval=60
    )
    stream.start(seed_from_snapshot=True)
    stream.stop()
    assert seen and seen[0] is False
