"""StreamManager must handle provider OptionQuote objects everywhere.

Three production failures in a row came from the same root cause: the
manager took its streams from a provider, so everything it drains is now a
normalised OptionQuote, and several consumers still treated those values as
raw vendor dicts. Each one was found by deploying.

The unit tests missed them because each covered its own function with its
own fixture. Nothing asserted the one property that actually matters --
that EVERY consumer of drained state accepts what the provider hands it.
"""

from __future__ import annotations

import inspect
from datetime import datetime, timezone

import pytest

from src.ingestion.providers.base import OptionQuote
from src.ingestion.stream_manager import StreamManager, _has_positive_oi

NOW = datetime(2026, 9, 23, 13, 45, tzinfo=timezone.utc)


def _quote(symbol, **kw):
    return OptionQuote(option_symbol=symbol, timestamp=NOW, **kw)


def _manager() -> StreamManager:
    sm = StreamManager.__new__(StreamManager)
    sm.tracked_option_symbols = ["A", "B", "C"]
    sm._session_oi_symbols = set()
    sm._session_volume_symbols = set()
    sm._session_day = None
    sm._ensure_session_day = lambda *_a, **_kw: None
    return sm


def test_open_interest_coverage_reads_a_quote():
    sm = _manager()
    changed = {
        "A": _quote("A", open_interest=1200),
        "B": _quote("B", open_interest=0),
        "C": _quote("C"),
    }
    assert sm._update_session_oi_coverage(changed, 3) == pytest.approx(1 / 3)


def test_volume_coverage_reads_a_quote():
    sm = _manager()
    changed = {
        "A": _quote("A", volume=400),
        "B": _quote("B", volume=0),
        "C": _quote("C"),
    }
    assert sm._update_session_volume_coverage(changed, 3) == pytest.approx(1 / 3)


def test_coverage_accumulates_across_cycles():
    """The reason these live on the manager: the accumulator is rebuilt
    every strike recalibration, so counting off it directly collapses to
    one minute of trades and false-trips the alert."""
    sm = _manager()
    sm._update_session_volume_coverage({"A": _quote("A", volume=10)}, 3)
    assert sm._update_session_volume_coverage({"B": _quote("B", volume=10)}, 3) == pytest.approx(
        2 / 3
    )


def test_positive_open_interest_needs_a_positive_reading():
    assert _has_positive_oi(_quote("A", open_interest=5)) is True
    assert _has_positive_oi(_quote("A", open_interest=0)) is False
    assert _has_positive_oi(_quote("A")) is False


def test_no_consumer_of_drained_state_reaches_for_a_vendor_field_name():
    """The guard that would have caught all three failures at once.

    StreamManager is vendor-neutral orchestration now. Every quote it sees
    has been through a provider, so a TradeStation field name appearing
    anywhere in this class means some consumer is still expecting a raw
    dict and will raise on the first live cycle.

    Scoped to the class body, because the accumulators in the same module
    legitimately parse those names -- that is their job.
    """
    src = inspect.getsource(StreamManager)
    for vendor_field in (
        '"Bid"',
        '"Ask"',
        '"Last"',
        '"Mid"',
        '"Volume"',
        '"OpenInterest"',
        '"DailyOpenInterest"',
        '"ImpliedVolatility"',
    ):
        assert vendor_field not in src, (
            f"StreamManager still reads the vendor field {vendor_field}. It "
            f"receives normalised OptionQuote objects from the provider, so "
            f"this raises 'OptionQuote' object has no attribute 'get' in "
            f"production on the first cycle."
        )
