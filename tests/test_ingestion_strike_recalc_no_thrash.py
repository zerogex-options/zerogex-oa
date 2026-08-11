"""Strike-recalc must not reopen the option streams when nothing changed.

The strike recalc in ``StreamManager.stream`` fires on an iteration counter.
Under event-driven wakeups iterations advance on every option tick, so the
block runs many times a minute — far more often than the "~60s" the interval
nominally models. On a calm tape the ±band trims to the same nearest-N strikes
each pass, so a *blind* ``_start_accumulators`` tears down and reopens both
option chunks for an IDENTICAL symbol set every recalc.

Those needless reopens are exactly the per-account concurrent-stream churn
that trips TradeStation's ~10-stream cap: each ingestion process reopens its
chunks ~every recalc, the reopens claim slots and evict another process's
chunks, which reopen and evict back — the short-lifetime disconnects the cap
classifier reports (option streams cut at ~22-26s while still delivering tens
of thousands of payloads).

These tests pin the guard: reopen only when the tracked universe actually
changed; otherwise keep the live streams. A transient empty rebuild (strike
fetch hiccup) is treated as "unchanged" so it never blacks out the universe.
"""

import threading
import time
from datetime import date, datetime

import pytz

from src.ingestion import stream_manager as sm
from src.ingestion.stream_manager import StreamManager

ET = pytz.timezone("US/Eastern")


class _QuietOptionAcc:
    """Option accumulator stub that drains nothing and is cheap to stop."""

    is_alive = True
    updates_received = 0

    def drain(self):
        return {}

    def snapshot(self):
        return {}

    def stop(self):
        pass


class _QuietUnderlyingAcc:
    """Underlying accumulator stub: alive, never produces a bar."""

    is_alive = True
    updates_received = 0

    def drain(self):
        return None

    def snapshot(self):
        return {}

    def stop(self):
        pass


def _recalc_manager(monkeypatch, build_returns):
    """Bare StreamManager whose recalc fires fast, with all hot-path I/O
    stubbed. ``build_returns`` is what ``_build_option_symbols`` yields each
    recalc (a list of option symbols)."""
    # Regular session, but the underlying feed is NOT expected, so the whole
    # underlying watchdog ladder takes its quiet "not expected" branch and
    # never restarts anything — the test observes only the recalc path.
    monkeypatch.setattr(sm, "get_market_session", lambda *a: "regular")
    monkeypatch.setattr(sm, "underlying_feed_expected", lambda *a, **kw: False)
    # Spin fast and recalc on every other iteration.
    monkeypatch.setattr(sm, "MARKET_HOURS_POLL_INTERVAL", 0.01)
    monkeypatch.setattr(sm, "STRIKE_RECALC_INTERVAL", 2)
    monkeypatch.setattr(sm, "STRIKE_CLEANUP_INTERVAL", 10**9)

    mgr = object.__new__(StreamManager)
    mgr._wakeup = threading.Event()
    mgr._stop_event = threading.Event()
    mgr.underlying = "$SPXW.X"
    mgr.db_underlying = "SPX"
    mgr.current_price = 100.0
    mgr.strike_count_max = 2
    mgr.strike_pct_range = 3.0
    # Initial tracked universe + the state _build_option_symbols overwrites.
    initial = ["SPXW 260101C100", "SPXW 260101P100"]
    mgr.tracked_option_symbols = list(initial)
    mgr._symbol_metadata = {s: {"strike": 100.0} for s in initial}
    mgr.tracked_strikes = {100.0}
    mgr.all_tracked_strikes = {date.today(): {100.0}}
    mgr.target_expirations = [date.today()]
    mgr.last_expiration_refresh = datetime.now(ET)
    mgr.seed_rest_on_recalc = False
    mgr._accumulator = _QuietOptionAcc()
    mgr._underlying_accumulator = _QuietUnderlyingAcc()

    mgr._should_refresh_expirations = lambda: False
    mgr._cleanup_expired_strikes = lambda: None
    mgr._get_underlying_price = lambda: 100.0
    mgr._build_option_symbols = lambda: list(build_returns)
    return mgr, initial


def _record_start_accumulators(mgr):
    """Replace ``_start_accumulators`` with a recorder of (args, kwargs)."""
    calls = []
    mgr._start_accumulators = lambda *a, **k: calls.append((a, k))
    return calls


def _drive(mgr, seconds=0.6):
    """Run ``stream()`` in a daemon thread for a bit, then stop it."""
    exited = threading.Event()

    def _run():
        try:
            for _ in mgr.stream(max_iterations=None):
                pass
        finally:
            exited.set()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    try:
        time.sleep(seconds)
        mgr.request_stop()
        assert exited.wait(timeout=3.0), "stream loop failed to exit"
    finally:
        mgr.request_stop()


def _recalc_reopens(calls):
    """Reopen calls made by the recalc/refresh path carry restart_underlying
    =False; the single initial start at stream() top passes no kwargs."""
    return [k for (_a, k) in calls if k.get("restart_underlying") is False]


def test_unchanged_strikes_do_not_reopen_option_streams(monkeypatch):
    """The core fix: when the recomputed strike set matches the live one,
    the recalc must NOT reopen the option chunks (which would re-enter the
    per-account stream-cap race for no benefit)."""
    # _build_option_symbols returns the SAME set as the initial universe
    # (reordered, to prove the guard compares sets not list identity).
    mgr, initial = _recalc_manager(
        monkeypatch, build_returns=["SPXW 260101P100", "SPXW 260101C100"]
    )
    calls = _record_start_accumulators(mgr)

    _drive(mgr)

    reopens = _recalc_reopens(calls)
    assert reopens == [], (
        "Recalc reopened the option streams despite an unchanged strike "
        f"universe — that is the cap-tripping churn this guard removes. "
        f"start_accumulators calls: {calls}"
    )
    # The live universe is preserved exactly.
    assert set(mgr.tracked_option_symbols) == set(initial)


def test_changed_strikes_reopen_option_streams(monkeypatch):
    """When the strike set genuinely moves, the recalc MUST reopen the option
    chunks (with restart_underlying=False) so the new contracts are streamed."""
    changed = ["SPXW 260101C105", "SPXW 260101P105"]
    mgr, initial = _recalc_manager(monkeypatch, build_returns=changed)
    calls = _record_start_accumulators(mgr)

    _drive(mgr)

    reopens = _recalc_reopens(calls)
    assert reopens, "Recalc did not reopen the option streams after the universe changed"
    # Reopen must leave the underlying bar stream alone.
    assert all(k.get("restart_underlying") is False for k in reopens)
    # The tracked universe advanced to the new set.
    assert set(mgr.tracked_option_symbols) == set(changed)


def test_empty_rebuild_keeps_existing_universe(monkeypatch):
    """A transient empty ``_build_option_symbols`` (strike-fetch hiccup) must
    NOT swap the accumulator to an empty universe, nor wipe the metadata the
    live accumulator's yields depend on."""
    mgr, initial = _recalc_manager(monkeypatch, build_returns=[])
    prev_metadata = mgr._symbol_metadata
    calls = _record_start_accumulators(mgr)

    _drive(mgr)

    reopens = _recalc_reopens(calls)
    assert reopens == [], "Empty rebuild must not reopen streams onto an empty universe"
    # Universe and metadata preserved (not blacked out).
    assert set(mgr.tracked_option_symbols) == set(initial)
    assert mgr._symbol_metadata is prev_metadata
    assert mgr._symbol_metadata, "metadata was wiped by a transient empty rebuild"
