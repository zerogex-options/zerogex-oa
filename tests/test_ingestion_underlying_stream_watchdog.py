"""Regression coverage for the underlying-stream recovery primitive.

The TradeStation underlying bar stream can stay socket-alive (heartbeats
flowing) while delivering zero bars. Neither the socket read timeout nor
the dead-thread check catches that, so the supervisor force-reconnects via
``_restart_underlying_accumulator``. These tests pin its contract:

  * only the underlying accumulator is torn down/recreated — the options
    stream (and its expensive REST re-seed) is left untouched;
  * the replacement is constructed identically and started.
"""

from datetime import datetime, timedelta, timezone

from src.ingestion import stream_manager
from src.ingestion.stream_manager import (
    StreamManager,
    _bar_timestamp_advanced,
    _stale_thresholds_for_session,
)
from src.config import (
    SESSION_TEMPLATE,
    UNDERLYING_STREAM_STALE_WARN_SECONDS,
    UNDERLYING_STREAM_STALE_RESTART_SECONDS,
    UNDERLYING_STREAM_STALE_WARN_SECONDS_EXTENDED,
    UNDERLYING_STREAM_STALE_RESTART_SECONDS_EXTENDED,
)


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


# --- session-aware staleness thresholds (fix #1) -------------------------
#
# Extended hours must use the wider thresholds: a thinly traded equity/ETF
# legitimately goes minutes between 1-minute bars after the close, so the
# dense regular-session thresholds produced false STALE/restart storms.


def test_regular_session_uses_base_thresholds():
    assert _stale_thresholds_for_session("regular") == (
        UNDERLYING_STREAM_STALE_WARN_SECONDS,
        UNDERLYING_STREAM_STALE_RESTART_SECONDS,
    )


def test_closed_session_falls_back_to_base_thresholds():
    # "closed" never reaches the watchdog (feed not expected) but the
    # mapping must still be total and safe.
    assert _stale_thresholds_for_session("closed") == (
        UNDERLYING_STREAM_STALE_WARN_SECONDS,
        UNDERLYING_STREAM_STALE_RESTART_SECONDS,
    )


def test_extended_hours_use_wider_thresholds():
    extended = (
        UNDERLYING_STREAM_STALE_WARN_SECONDS_EXTENDED,
        UNDERLYING_STREAM_STALE_RESTART_SECONDS_EXTENDED,
    )
    assert _stale_thresholds_for_session("pre-market") == extended
    assert _stale_thresholds_for_session("after-hours") == extended
    # The whole point of the fix: extended thresholds are strictly wider.
    assert UNDERLYING_STREAM_STALE_WARN_SECONDS_EXTENDED > UNDERLYING_STREAM_STALE_WARN_SECONDS
    assert (
        UNDERLYING_STREAM_STALE_RESTART_SECONDS_EXTENDED > UNDERLYING_STREAM_STALE_RESTART_SECONDS
    )


# --- barsback=1 replay must not count as liveness (fix #2) ---------------
#
# Every forced reconnect replays one historical bar with the same/older
# timestamp. Counting it as fresh reset the staleness clock + restart
# escalation, so a starved feed looped "restart -> replay -> reset" forever
# and never reached the backed-off upstream-outage state.

_T0 = datetime(2026, 5, 18, 16, 0, tzinfo=timezone.utc)


def test_first_ever_bar_is_fresh():
    assert _bar_timestamp_advanced(_T0, None) is True


def test_strictly_newer_bar_is_fresh():
    assert _bar_timestamp_advanced(_T0 + timedelta(minutes=1), _T0) is True


def test_replayed_same_timestamp_is_not_fresh():
    # barsback=1 replay of the exact last bar on reconnect.
    assert _bar_timestamp_advanced(_T0, _T0) is False


def test_older_timestamp_is_not_fresh():
    assert _bar_timestamp_advanced(_T0 - timedelta(minutes=5), _T0) is False


def test_non_datetime_payload_fails_safe_to_fresh():
    # Never silently suppress staleness detection on a malformed payload.
    assert _bar_timestamp_advanced(None, _T0) is True
    assert _bar_timestamp_advanced("not-a-datetime", _T0) is True


def test_naive_aware_mismatch_does_not_crash_and_fails_safe():
    naive = datetime(2026, 5, 18, 16, 1)
    # naive > aware would raise TypeError; helper must swallow it -> fresh.
    assert _bar_timestamp_advanced(naive, _T0) is True
