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


# --- staleness check must fire even when the stream re-emits stale bars ---
#
# Observed in production 2026-05-27 (pid 494066, $SPXW.X): TradeStation's
# bar stream stayed socket-alive across the overnight rollover and kept
# pushing bar events whose timestamp was stuck at yesterday's 16:00 close.
# The old watchdog ran its staleness ladder ONLY when ``drain()`` returned
# None — so the ``if underlying_data:`` branch absorbed every iteration,
# bar_advanced stayed False (correct), but no STALE warning ever fired and
# no force-reconnect was attempted. ``latest_underlying_timestamp`` froze
# at yesterday's close and the Greeks engine refused every option tick for
# the rest of the regular session.
#
# Fix: a non-advancing bar must drive the same staleness ladder as an
# empty drain. This test pins that contract by driving stream() with a
# stub accumulator that re-emits one stale bar on every drain, then
# asserting the watchdog escalates to a forced reconnect within a few
# real-time seconds.

import threading as _threading
import time as _time
from datetime import date as _date

import pytz as _pytz

from src.ingestion import stream_manager as _sm

_ET = _pytz.timezone("US/Eastern")


class _StaleRepeatingAcc:
    """Underlying accumulator stub that drains the same stale bar forever.

    Models the failure mode: socket-alive, ``bar_stream_updates`` climbing,
    but every drained bar carries the same prior-session timestamp.
    """

    is_alive = True

    def __init__(self):
        self.updates_received = 0
        self._stale_ts = datetime(2026, 5, 26, 16, 0, tzinfo=_pytz.UTC)

    def drain(self):
        self.updates_received += 1
        return {
            "symbol": "SPX",
            "timestamp": self._stale_ts,
            "open": 5800.0,
            "high": 5800.0,
            "low": 5800.0,
            "close": 5800.0,
            "volume": 0,
            "up_volume": 0,
            "down_volume": 0,
        }

    def snapshot(self):
        return {}

    def stop(self):
        pass


class _QuietOptionAcc:
    """Option accumulator stub that drains nothing — keeps the option
    branch quiet so the test observes only the underlying watchdog path."""

    is_alive = True
    updates_received = 0

    def drain(self):
        return {}

    def snapshot(self):
        return {}

    def stop(self):
        pass


def _stale_repeat_manager(monkeypatch) -> StreamManager:
    """Build a bare StreamManager whose underlying drain always returns a
    stale bar, all other hot-path I/O stubbed, in a regular-session window
    where the feed is expected to be live."""
    # Cash-index regular session, feed expected — the exact window where
    # the production bug manifested.
    monkeypatch.setattr(_sm, "get_market_session", lambda *a: "regular")
    monkeypatch.setattr(_sm, "underlying_feed_expected", lambda *a, **kw: True)

    # Tighten thresholds so a 3s real-time test elapses past both warn and
    # restart gates. Cooldown 0 so the first restart fires without a 90s
    # wait. The patches go on the stream_manager module's references —
    # _stale_thresholds_for_session reads them via that name.
    monkeypatch.setattr(_sm, "UNDERLYING_STREAM_STALE_WARN_SECONDS", 1)
    monkeypatch.setattr(_sm, "UNDERLYING_STREAM_STALE_RESTART_SECONDS", 2)
    monkeypatch.setattr(_sm, "UNDERLYING_STREAM_RESTART_COOLDOWN_SECONDS", 0)
    monkeypatch.setattr(_sm, "UNDERLYING_STREAM_MAX_RESTART_ATTEMPTS", 5)

    # Iterate fast so the body runs hundreds of times in a few seconds.
    monkeypatch.setattr(_sm, "MARKET_HOURS_POLL_INTERVAL", 0.02)
    # Don't recalibrate during the test — would trigger _start_accumulators.
    monkeypatch.setattr(_sm, "STRIKE_RECALC_INTERVAL", 10**9)

    mgr = object.__new__(StreamManager)
    mgr._wakeup = _threading.Event()
    mgr._stop_event = _threading.Event()
    mgr.underlying = "$SPXW.X"
    mgr.db_underlying = "SPX"
    mgr.tracked_option_symbols = ["DUMMY"]
    mgr._symbol_metadata = {}
    mgr.target_expirations = [_date.today()]
    mgr.last_expiration_refresh = datetime.now(_ET)
    mgr.current_price = 5800.0
    mgr.strike_count_max = 2
    mgr.strike_pct_range = 3.0
    mgr._accumulator = _QuietOptionAcc()
    mgr._underlying_accumulator = _StaleRepeatingAcc()
    mgr.option_oi_coverage_alert_threshold = 0.35
    mgr.option_volume_coverage_alert_threshold = 0.35
    mgr.option_volume_warmup_minutes = 30
    mgr.option_oi_warmup_minutes = 5
    mgr.seed_rest_on_recalc = False
    mgr._start_accumulators = lambda *a, **kw: None
    mgr._should_refresh_expirations = lambda: False
    mgr._cleanup_expired_strikes = lambda: None
    return mgr


def test_watchdog_escalates_when_stream_replays_stale_bars(monkeypatch, caplog):
    """The production failure mode: stream is socket-alive and pushing bar
    events, but every bar carries the same prior-session timestamp. The
    watchdog MUST still warn and force a reconnect — not absorb the
    replays silently while Greeks reject for the rest of the day.
    """
    mgr = _stale_repeat_manager(monkeypatch)

    restart_calls: list[str] = []
    mgr._restart_underlying_accumulator = lambda reason: restart_calls.append(reason)

    exited = _threading.Event()

    def _run():
        try:
            for _ in mgr.stream(max_iterations=None):
                pass
        finally:
            exited.set()

    import logging as _logging

    caplog.set_level(_logging.WARNING, logger="src.ingestion.stream_manager")

    t = _threading.Thread(target=_run, daemon=True)
    t.start()
    try:
        # Wait long enough for the staleness clock to pass the 2s restart
        # gate (tightened above) plus loop scheduling slack.
        _time.sleep(3.5)
        mgr.request_stop()
        assert exited.wait(timeout=3.0), "stream loop failed to exit"
    finally:
        mgr.request_stop()

    # The exact symptom from the production journal: a STALE warning
    # naming the non-advancing replays. Old code logged NOTHING here.
    stale_warnings = [r for r in caplog.records if "appears STALE" in r.getMessage()]
    assert stale_warnings, (
        "Watchdog did not warn on non-advancing bars. Before the fix the "
        "if underlying_data: branch absorbed every iteration and the "
        "staleness ladder was never entered — production saw 14k Greeks "
        "rejections per minute with the watchdog silent."
    )
    assert "empty/replay drains" in stale_warnings[0].getMessage(), (
        "Warning message must distinguish replay drains from empty drains "
        "so the post-incident log clearly names the new failure mode."
    )

    # And the watchdog must escalate to a forced reconnect once the
    # restart threshold is reached — otherwise the stuck upstream is
    # never given a chance to recover.
    assert restart_calls, (
        "Watchdog never forced a reconnect despite >2s of non-advancing "
        "bars. The Greeks engine would reject every option tick until "
        "16:00 ET."
    )
