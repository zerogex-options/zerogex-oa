"""Each underlying bar is stored the moment it is polled.

The collector's loop (``StreamManager.stream``) drains the bar stream once a
pass, and a pass also drains the option chain and waits while the engine
stores it. On 2026-10-28 a pass took 17 to 39 seconds for SPY, QQQ, SPX and
NDX, so the live price moved once a pass however often it was polled. The
bar stream now hands each bar to a listener on its own thread, and the
engine stores bars there and nowhere else. These tests pin the wiring:

  * the manager attaches the listener to every bar stream it opens, before
    the stream starts, restarts included;
  * a stream that cannot take a listener (TradeStation's) changes nothing;
  * while a listener is attached the engine does not also store the drained
    copies, which would write each bar twice and could put an older one after
    a newer one.
"""

import threading
from datetime import datetime, timezone

import src.ingestion.main_engine as me
from src.ingestion.providers.base import Bar
from src.ingestion.stream_manager import StreamManager


class _ListeningStream:
    """A bar stream that takes a listener, as ThetaData's poller does."""

    def __init__(self):
        self.listener = None
        self.listener_at_start = None
        self.started = 0
        self.stopped = 0

    def set_listener(self, listener):
        self.listener = listener
        return True

    def start(self):
        self.started += 1
        self.listener_at_start = self.listener

    def stop(self):
        self.stopped += 1


class _DeafStream:
    """A bar stream with no listener support, as TradeStation's has none."""

    def start(self):
        pass

    def stop(self):
        pass


class _OptionStream:
    def start(self, seed_from_snapshot=True):
        pass

    def stop(self):
        pass


def _manager(make_stream):
    """A StreamManager with only what opening its streams touches."""
    opened = []

    class _Provider:
        def stream_underlying_bars(self, symbol, *, db_symbol=None, wakeup=None, **kw):
            stream = make_stream()
            opened.append(stream)
            return stream

        def stream_option_quotes(self, symbols, *, wakeup=None, **kw):
            return _OptionStream()

    mgr = object.__new__(StreamManager)
    mgr.provider = _Provider()
    mgr.underlying = "SPY"
    mgr.db_underlying = "SPY"
    mgr.tracked_option_symbols = []
    mgr._wakeup = threading.Event()
    mgr._accumulator = None
    mgr._underlying_accumulator = None
    return mgr, opened


def test_the_listener_is_on_the_stream_before_it_starts():
    mgr, opened = _manager(_ListeningStream)
    rows = []
    mgr.set_underlying_listener(rows.append)
    assert mgr.underlying_listener_attached is False, "no stream is open yet"

    mgr._start_accumulators()

    stream = opened[0]
    assert stream.started == 1
    assert stream.listener_at_start is not None, "attached after start(): its first bar is lost"
    assert mgr.underlying_listener_attached is True

    # The engine gets the same dict the loop has always yielded.
    at = datetime(2026, 10, 28, 14, 0, 1, tzinfo=timezone.utc)
    stream.listener(Bar(symbol="SPY", timestamp=at, open=1.0, high=2.0, low=0.5, close=1.5))
    assert rows == [
        {
            "symbol": "SPY",
            "timestamp": at,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": None,
            "up_volume": None,
            "down_volume": None,
        }
    ]


def test_a_restarted_stream_gets_the_listener_too():
    """The watchdog replaces a stalled bar stream. A replacement without the
    listener would poll every second and store nothing, while the engine,
    still told a listener is attached, skipped the drained copies."""
    mgr, opened = _manager(_ListeningStream)
    mgr.set_underlying_listener(lambda row: None)
    mgr._start_accumulators()

    mgr._restart_underlying_accumulator("data-starved 130s")

    assert len(opened) == 2 and opened[0].stopped == 1
    assert opened[1].listener_at_start is not None
    assert mgr.underlying_listener_attached is True


def test_a_stream_that_cannot_listen_keeps_the_old_path():
    mgr, opened = _manager(_DeafStream)
    mgr.set_underlying_listener(lambda row: None)
    mgr._start_accumulators()

    assert opened and mgr.underlying_listener_attached is False


def test_clearing_the_listener_detaches_it():
    mgr, opened = _manager(_ListeningStream)
    mgr.set_underlying_listener(lambda row: None)
    mgr._start_accumulators()

    mgr.set_underlying_listener(None)

    assert opened[0].listener is None
    assert mgr.underlying_listener_attached is False


# ---------------------------------------------------------------------------
# The engine stores each bar once
# ---------------------------------------------------------------------------


class _Manager:
    """Stands in for StreamManager in IngestionEngine.run_streaming."""

    def __init__(self, *, can_listen, rows):
        self.can_listen = can_listen
        self.rows = rows
        self.listener = None
        self.underlying_listener_attached = False

    def initialize(self):
        return True

    def set_underlying_listener(self, listener):
        self.listener = listener
        self.underlying_listener_attached = self.can_listen

    def stream(self, max_iterations=None):
        for row in self.rows:
            if self.underlying_listener_attached:
                # The poll thread stores the bar the moment it lands...
                self.listener(row)
            # ...and the loop still yields the drained copy, for the watchdog.
            yield {"type": "underlying", "data": row}


def _engine(monkeypatch, manager):
    monkeypatch.setattr(me, "StreamManager", lambda **kw: manager)
    monkeypatch.setattr(me, "is_engine_run_window", lambda: True)

    engine = me.IngestionEngine.__new__(me.IngestionEngine)
    engine.client = None
    engine.provider = object()
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
    engine._check_buffer_flush_timeout = lambda: None
    engine._underlying_store_lock = threading.Lock()
    stored = []
    engine._store_underlying = stored.append
    return engine, stored


def test_with_a_listener_each_bar_is_stored_once(monkeypatch):
    rows = [{"close": 1.0}, {"close": 2.0}]
    manager = _Manager(can_listen=True, rows=rows)
    engine, stored = _engine(monkeypatch, manager)

    engine.run_streaming()

    assert manager.listener is not None, "the engine never attached a listener"
    assert stored == rows, "each bar must be stored once, by the listener"


def test_without_a_listener_the_drained_bars_are_stored(monkeypatch):
    rows = [{"close": 1.0}, {"close": 2.0}]
    manager = _Manager(can_listen=False, rows=rows)
    engine, stored = _engine(monkeypatch, manager)

    engine.run_streaming()

    assert stored == rows
