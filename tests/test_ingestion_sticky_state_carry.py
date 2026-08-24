"""Regression coverage for sticky option state surviving a stream restart.

Open interest enters ``OptionStreamAccumulator`` state from exactly one
place: the REST seed. Stream deltas send ``DailyOpenInterest=0`` (or omit
it) and are dropped by the accumulator's positive-only merge rule, because
OI settles once daily rather than per tick.

Strike recalibration constructs a BRAND NEW accumulator and starts it with
``seed_from_rest=OPTION_REST_SEED_ON_RECALC``, which defaults to false. So
before this fix, the first recalibration of the session permanently blanked
OI: every ``option_chains`` row written afterwards carried NULL
``open_interest``, and every gamma-exposure query multiplying by
``COALESCE(oc.open_interest, 0)`` (src/api/database.py) silently read zero.

These tests pin the hand-off: sticky fields cross the swap, volatile ones
do not, and the carry can neither resurrect dropped contracts nor publish a
contract that has no live quote.
"""

from __future__ import annotations

import threading

import pytest

from src.ingestion.stream_manager import OptionStreamAccumulator, StreamManager


def _acc(symbols, state=None):
    a = OptionStreamAccumulator(client=None, symbols=list(symbols))
    if state:
        a._state = {k: dict(v) for k, v in state.items()}
    return a


def test_sticky_fields_are_exported():
    a = _acc(["A"], {"A": {"Symbol": "A", "DailyOpenInterest": 1200, "Volume": 40, "IV": 0.21}})
    assert a.sticky_state() == {
        "A": {"DailyOpenInterest": 1200, "Volume": 40, "IV": 0.21}
    }


def test_prices_and_timestamps_are_not_exported():
    """Carrying a price forward would re-publish a stale quote as a fresh one.

    _yield_option_snapshot stamps a quote that has no TimeStamp with the
    receive time, so a carried Bid/Ask would land in option_chains as a
    current quote carrying pre-recalibration prices.
    """
    a = _acc(
        ["A"],
        {"A": {"Symbol": "A", "Bid": 1.0, "Ask": 1.2, "Mid": 1.1, "Last": 1.05,
               "TimeStamp": "2026-08-24T14:00:00Z", "DailyOpenInterest": 7}},
    )
    carried = a.sticky_state()["A"]
    assert carried == {"DailyOpenInterest": 7}
    for volatile in ("Bid", "Ask", "Mid", "Last", "TimeStamp"):
        assert volatile not in carried


def test_contracts_with_no_sticky_values_are_omitted():
    a = _acc(["A"], {"A": {"Symbol": "A", "Bid": 1.0}})
    assert a.sticky_state() == {}


def test_carry_populates_a_fresh_accumulator():
    """The core bug: OI must survive a recalcuation that does not re-seed."""
    old = _acc(["A", "B"], {
        "A": {"Symbol": "A", "DailyOpenInterest": 500, "Volume": 12},
        "B": {"Symbol": "B", "OpenInterest": 900},
    })
    new = _acc(["A", "B"])
    assert new.carry_sticky_state(old.sticky_state()) == 2
    assert new._state["A"]["DailyOpenInterest"] == 500
    assert new._state["A"]["Volume"] == 12
    assert new._state["B"]["OpenInterest"] == 900


def test_carry_skips_symbols_outside_the_new_tracked_set():
    """A recalibration that drops strikes must not resurrect them.

    Also what keeps the carried set bounded across a trending session.
    """
    old = _acc(["A", "DROPPED"], {
        "A": {"Symbol": "A", "DailyOpenInterest": 500},
        "DROPPED": {"Symbol": "DROPPED", "DailyOpenInterest": 700},
    })
    new = _acc(["A"])
    assert new.carry_sticky_state(old.sticky_state()) == 1
    assert "DROPPED" not in new._state


def test_carry_does_not_mark_symbols_dirty():
    """Carried state holds no price, so publishing it would yield nothing.

    drain() returns the dirty set; a carried-only contract has no bid/ask/mid
    and _yield_option_snapshot would drop it. It must become visible only once
    the contract genuinely ticks.
    """
    old = _acc(["A"], {"A": {"Symbol": "A", "DailyOpenInterest": 500}})
    new = _acc(["A"])
    new.carry_sticky_state(old.sticky_state())
    assert new._dirty == set()
    assert new.drain() == {}


def test_carry_does_not_overwrite_a_fresh_rest_seed():
    """setdefault semantics: a value already present wins over the carry."""
    old = _acc(["A"], {"A": {"Symbol": "A", "DailyOpenInterest": 500}})
    new = _acc(["A"], {"A": {"Symbol": "A", "DailyOpenInterest": 999}})
    new.carry_sticky_state(old.sticky_state())
    assert new._state["A"]["DailyOpenInterest"] == 999


def test_empty_carry_is_a_noop():
    new = _acc(["A"])
    assert new.carry_sticky_state({}) == 0
    assert new._state == {}


class _StubAccumulator:
    """Records the swap without opening sockets."""

    instances: list = []

    def __init__(self, client=None, symbols=(), wakeup=None):
        self.symbols = list(symbols)
        self.started_with_seed = None
        self.carried = None
        self.seeded_new = None
        type(self).instances.append(self)

    def start(self, seed_from_rest=True):
        self.started_with_seed = seed_from_rest

    def stop(self):
        pass

    def carry_sticky_state(self, carried):
        self.carried = carried
        return len(carried)

    def seed_new_symbols_from_rest(self, known):
        self.seeded_new = sorted(set(self.symbols) - set(known))
        return len(self.seeded_new)


@pytest.fixture
def _swap_manager(monkeypatch):
    _StubAccumulator.instances = []
    monkeypatch.setattr(
        "src.ingestion.stream_manager.OptionStreamAccumulator", _StubAccumulator
    )
    mgr = object.__new__(StreamManager)
    mgr.client = None
    mgr.tracked_option_symbols = ["A", "B"]
    mgr._wakeup = threading.Event()
    # Non-None so the underlying bar stream branch is skipped entirely.
    mgr._underlying_accumulator = object()
    return mgr


def test_recalibration_hands_sticky_state_to_the_successor(_swap_manager):
    """End-to-end wiring: the swap in _start_accumulators carries OI across."""
    mgr = _swap_manager
    mgr._accumulator = _acc(["A", "B"], {
        "A": {"Symbol": "A", "DailyOpenInterest": 500},
        "B": {"Symbol": "B", "DailyOpenInterest": 800},
    })

    mgr._start_accumulators(seed_option_rest=False, restart_underlying=False)

    successor = _StubAccumulator.instances[-1]
    assert successor.started_with_seed is False
    assert successor.carried == {
        "A": {"DailyOpenInterest": 500},
        "B": {"DailyOpenInterest": 800},
    }


def test_a_failed_handoff_never_breaks_the_restart(_swap_manager):
    """Losing the carry costs data quality; raising here would kill the feed."""
    mgr = _swap_manager

    class _Exploding:
        def sticky_state(self):
            raise RuntimeError("boom")

        def stop(self):
            pass

    mgr._accumulator = _Exploding()
    mgr._start_accumulators(seed_option_rest=False, restart_underlying=False)

    successor = _StubAccumulator.instances[-1]
    assert successor.started_with_seed is False
    assert successor.carried is None


def test_first_start_has_nothing_to_carry(_swap_manager):
    mgr = _swap_manager
    mgr._accumulator = None
    mgr._start_accumulators(seed_option_rest=True, restart_underlying=False)

    successor = _StubAccumulator.instances[-1]
    assert successor.started_with_seed is True
    assert successor.carried is None


def test_newly_tracked_strikes_are_rest_seeded(_swap_manager):
    """Arrivals have nothing to inherit; without a targeted seed they would
    write NULL open_interest until the next process restart."""
    mgr = _swap_manager
    mgr.tracked_option_symbols = ["A", "B", "NEW"]
    mgr._accumulator = _acc(["A", "B"], {
        "A": {"Symbol": "A", "DailyOpenInterest": 500},
        "B": {"Symbol": "B", "DailyOpenInterest": 800},
    })

    mgr._start_accumulators(seed_option_rest=False, restart_underlying=False)

    successor = _StubAccumulator.instances[-1]
    assert successor.seeded_new == ["NEW"]


def test_no_targeted_seed_when_a_full_rest_seed_is_already_running(_swap_manager):
    mgr = _swap_manager
    mgr._accumulator = _acc(["A", "B"], {"A": {"Symbol": "A", "DailyOpenInterest": 500}})

    mgr._start_accumulators(seed_option_rest=True, restart_underlying=False)

    successor = _StubAccumulator.instances[-1]
    assert successor.started_with_seed is True
    assert successor.seeded_new is None


def test_a_failed_arrival_seed_never_breaks_the_restart(_swap_manager):
    mgr = _swap_manager
    mgr._accumulator = _acc(["A"], {"A": {"Symbol": "A", "DailyOpenInterest": 500}})

    def _boom(self, known):
        raise RuntimeError("rate limited")

    _StubAccumulator.seed_new_symbols_from_rest = _boom
    try:
        mgr._start_accumulators(seed_option_rest=False, restart_underlying=False)
        successor = _StubAccumulator.instances[-1]
        assert successor.started_with_seed is False
        assert successor.carried == {"A": {"DailyOpenInterest": 500}}
    finally:
        del _StubAccumulator.seed_new_symbols_from_rest


def test_rest_seed_targets_only_the_requested_subset(monkeypatch):
    """The subset seed must not re-fetch the whole universe."""
    requested = []

    class _Client:
        def get_option_quotes(self, batch):
            requested.extend(batch)
            return {"Quotes": []}

    a = OptionStreamAccumulator(client=_Client(), symbols=["A", "B", "C"])
    monkeypatch.setattr("src.ingestion.stream_manager.DELAY_BETWEEN_BATCHES", 0)
    assert a.seed_new_symbols_from_rest({"A", "B"}) == 1
    assert requested == ["C"]


def test_nothing_new_means_no_rest_call(monkeypatch):
    class _Client:
        def get_option_quotes(self, batch):
            raise AssertionError("should not fetch when nothing is new")

    a = OptionStreamAccumulator(client=_Client(), symbols=["A", "B"])
    assert a.seed_new_symbols_from_rest({"A", "B"}) == 0
