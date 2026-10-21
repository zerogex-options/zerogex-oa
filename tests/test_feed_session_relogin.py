"""A replaced feed login must bring the unit back on its own.

ThetaData keeps one session per account; a second login ends the first, and
every call on it then fails with UNAUTHENTICATED "Invalid session ID". The
session is shared by every ingestion worker (one login in the supervisor,
adopted across the fork), so no worker can log in alone without ending the
others'. On 2026-10-21 that happened at 00:00 ET and ingestion wrote nothing
-- 21,849 failed expiration refreshes, 477 failed SPY bar polls between 04:00
and 08:40 -- until a human restarted the unit at 08:44.

These tests pin the chain that now does what that restart did:

  * the provider notices a dead session on ANY call, and forgets it on the
    next call that works;
  * after the grace period the worker's stream loop, or its failing
    initialize(), raises FeedSessionLost instead of retrying the same dead
    session forever;
  * the worker exits with FEED_SESSION_LOST_EXIT_CODE (supervisor tests live
    in test_ingestion_worker_supervision.py).
"""

import threading
from unittest.mock import MagicMock

import pytest

from src.ingestion import main_engine as me
from src.ingestion.providers import thetadata as td
from src.ingestion.providers.base import FeedSessionLost
from src.ingestion.stream_manager import StreamManager

#: The text the journal showed, first line and details both.
_SESSION_GONE = RuntimeError(
    "<_MultiThreadedRendezvous of RPC that terminated with:\n"
    "\tstatus = StatusCode.UNAUTHENTICATED\n"
    '\tdetails = "Invalid session ID. This can occur if more than one terminal '
    "is running. If restarting your terminal does not resolve the issue, "
    'please contact support."'
)


@pytest.fixture(autouse=True)
def _fresh_session_state():
    td.reset_shared_clients()
    yield
    td.reset_shared_clients()


class _Client:
    """Answers one endpoint, or raises whatever ``fail`` holds."""

    def __init__(self):
        self.fail = None

    def option_list_expirations(self, **kw):
        if self.fail is not None:
            raise self.fail
        return []


def _provider(client):
    return td.ThetaDataProvider(client, stage="mv", market_value_endpoints=True)


# ---------------------------------------------------------------------------
# The provider notices
# ---------------------------------------------------------------------------


def test_a_replaced_login_is_reported_once_the_grace_period_has_passed(monkeypatch):
    client = _Client()
    provider = _provider(client)
    client.fail = _SESSION_GONE

    with pytest.raises(RuntimeError):
        provider.get_option_expirations("SPY")

    monkeypatch.setattr(td, "SESSION_LOST_RELOGIN_SECONDS", 3600.0)
    assert not provider.session_lost(), "inside the grace period: a blip, not a loss"
    monkeypatch.setattr(td, "SESSION_LOST_RELOGIN_SECONDS", 0.0)
    assert provider.session_lost()


def test_one_call_that_works_means_the_session_works(monkeypatch):
    monkeypatch.setattr(td, "SESSION_LOST_RELOGIN_SECONDS", 0.0)
    client = _Client()
    provider = _provider(client)

    client.fail = _SESSION_GONE
    with pytest.raises(RuntimeError):
        provider.get_option_expirations("SPY")
    assert provider.session_lost()

    client.fail = None
    provider.get_option_expirations("SPY")
    assert not provider.session_lost()
    assert td.session_lost_for() is None


def test_other_failures_are_not_a_lost_session(monkeypatch):
    """A restart for an outage or a bad request would only add a gap: the
    new login meets the same outage. Only a refused session is answered
    with a new one."""
    monkeypatch.setattr(td, "SESSION_LOST_RELOGIN_SECONDS", 0.0)
    client = _Client()
    provider = _provider(client)
    for error in (
        RuntimeError("status = StatusCode.UNAVAILABLE"),
        RuntimeError('details = "No data found for your request"'),
        # F13: NDX is unentitled, and a new login is just as unentitled.
        RuntimeError(
            "INVALID_ARGUMENT: Requesting data for NASDAQ GIDs symbols: [NDX] "
            "without proper permissions"
        ),
        RuntimeError('status = StatusCode.UNAUTHENTICATED\n\tdetails = "Bad credentials"'),
        TimeoutError("deadline exceeded"),
    ):
        client.fail = error
        with pytest.raises(type(error)):
            provider.get_option_expirations("SPY")
    assert not provider.session_lost()


def test_the_watch_hides_nothing_about_the_client():
    """_endpoint picks Market Value with getattr(..., None) and the probe
    lists endpoints with hasattr; both depend on a missing method still
    raising AttributeError through the watch."""
    client = _Client()
    provider = _provider(client)
    assert not hasattr(provider._client, "stock_snapshot_market_value")
    assert hasattr(provider._client, "option_list_expirations")
    assert provider.client is client, "the property still hands out the vendor client"


def test_providers_that_cannot_lose_a_session_never_say_so():
    from src.ingestion.providers.stub import StubProvider

    assert StubProvider().session_lost() is False


# ---------------------------------------------------------------------------
# The worker stops retrying the dead session
# ---------------------------------------------------------------------------


class _Lost:
    def session_lost(self):
        return True


class _Acc:
    def __init__(self):
        self.stopped = 0

    def stop(self):
        self.stopped += 1


def test_the_stream_loop_ends_on_a_lost_session():
    """Restarting a stream reuses the dead session, so the watchdog's own
    remedies cannot help. The loop has to end, and still close its streams
    on the way out."""
    mgr = object.__new__(StreamManager)
    mgr.tracked_option_symbols = ["SPY   261021C00770000"]
    mgr._start_accumulators = lambda *a, **k: None
    mgr._stop_event = threading.Event()
    mgr._wakeup = threading.Event()
    options, underlying = _Acc(), _Acc()
    mgr._accumulator = options
    mgr._underlying_accumulator = underlying
    mgr.underlying = "SPY"
    mgr.provider = _Lost()

    with pytest.raises(FeedSessionLost):
        next(mgr.stream())
    assert options.stopped == 1 and underlying.stopped == 1


def _engine(monkeypatch, provider):
    """A bare IngestionEngine whose StreamManager fails to initialize."""

    class _FailingManager:
        def __init__(self, **kw):
            pass

        def initialize(self):
            return False

    slept = []
    monkeypatch.setattr(me, "StreamManager", _FailingManager)
    monkeypatch.setattr(me, "is_engine_run_window", lambda: True)
    monkeypatch.setattr(me.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(me, "close_connection_pool", lambda: None)

    engine = me.IngestionEngine.__new__(me.IngestionEngine)
    engine.client = None
    engine.provider = provider
    engine.underlying = "SPY"
    engine.db_symbol = "SPY"
    engine.num_expirations = 3
    engine.strike_count_max = 40
    engine.strike_pct_range = 3.0
    engine.num_monthly_expirations = 0
    engine.monthly_underlying = None
    engine.running = True
    engine._initialize_failures = 0
    engine._active_stream_manager = None
    engine._flush_all_buffers = lambda: None
    engine.underlying_bars_stored = 0
    engine.option_quotes_stored = 0
    engine.greeks_calculated = 0
    engine.errors_count = 0
    return engine, slept


def test_a_failing_initialize_on_a_lost_session_is_not_retried(monkeypatch):
    """A unit started over a weekend, or after a restart that lost the
    race, fails in initialize() and never reaches the stream loop.
    Retrying initialize() reuses the dead session forever."""
    engine, slept = _engine(monkeypatch, _Lost())
    with pytest.raises(FeedSessionLost):
        engine.run_streaming()
    assert slept == [], "no backoff before giving the session up"


def test_a_failing_initialize_on_a_live_session_still_backs_off(monkeypatch):
    """The ordinary case is untouched. A MagicMock answers session_lost()
    with a truthy mock, which must not read as a lost session."""
    engine, slept = _engine(monkeypatch, MagicMock())
    engine.run_streaming()
    assert len(slept) == 1


def test_the_worker_exits_with_the_relogin_code(monkeypatch):
    engine, _ = _engine(monkeypatch, _Lost())
    with pytest.raises(SystemExit) as exit_info:
        engine.run()
    assert exit_info.value.code == me.FEED_SESSION_LOST_EXIT_CODE
