"""Regression coverage for the underlying-stream recovery primitive.

The TradeStation underlying bar stream can stay socket-alive (heartbeats
flowing) while delivering zero bars. Neither the socket read timeout nor
the dead-thread check catches that, so the supervisor force-reconnects via
``_restart_underlying_accumulator``. These tests pin its contract:

  * only the underlying accumulator is torn down/recreated — the options
    stream (and its expensive REST re-seed) is left untouched;
  * the replacement is constructed identically and started.
"""

from src.ingestion import stream_manager
from src.ingestion.stream_manager import StreamManager
from src.config import SESSION_TEMPLATE


class _FakeUnderlyingAcc:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = 0
        self.stopped = 0

    def start(self):
        self.started += 1

    def stop(self):
        self.stopped += 1


def _bare_manager(monkeypatch):
    """A StreamManager with only the attributes the helper touches."""
    created = []

    def _factory(**kwargs):
        acc = _FakeUnderlyingAcc(**kwargs)
        created.append(acc)
        return acc

    monkeypatch.setattr(stream_manager, "UnderlyingBarAccumulator", _factory)

    mgr = object.__new__(StreamManager)
    mgr.client = object()
    mgr.underlying = "$SPXW.X"
    mgr.db_underlying = "SPX"
    mgr._wakeup = object()
    mgr._accumulator = object()  # options stream sentinel — must NOT change
    return mgr, created


def test_restart_recreates_only_underlying_and_starts_it(monkeypatch):
    mgr, created = _bare_manager(monkeypatch)
    old = _FakeUnderlyingAcc()
    mgr._underlying_accumulator = old
    options_sentinel = mgr._accumulator

    mgr._restart_underlying_accumulator("data-starved 130s during after-hours")

    # Old underlying stream torn down exactly once.
    assert old.stopped == 1
    # A fresh accumulator was created, wired identically, and started.
    assert len(created) == 1
    new = created[0]
    assert mgr._underlying_accumulator is new
    assert new is not old
    assert new.started == 1
    assert new.kwargs["client"] is mgr.client
    assert new.kwargs["symbol"] == "$SPXW.X"
    assert new.kwargs["db_symbol"] == "SPX"
    assert new.kwargs["session_template"] == SESSION_TEMPLATE
    assert new.kwargs["wakeup"] is mgr._wakeup
    # Options stream object is left completely untouched.
    assert mgr._accumulator is options_sentinel


def test_restart_tolerates_missing_prior_accumulator(monkeypatch):
    mgr, created = _bare_manager(monkeypatch)
    mgr._underlying_accumulator = None

    mgr._restart_underlying_accumulator("reader thread is DEAD")

    assert len(created) == 1
    assert mgr._underlying_accumulator is created[0]
    assert created[0].started == 1
