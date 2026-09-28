"""Forced-flow views price from ONE shared option book per symbol (no DB).

Every /forced-flow view reprices the same latest book, and loading it -- not the
math -- was the cost: each view loaded its own copy on every response-cache
miss, and misses were nearly every request because one viewer's polls alternate
between the two API workers. These pin the three properties the fix rests on:

* one load serves every view of a symbol while the book is fresh, including
  concurrent misses;
* a cached response ages from when its BOOK was loaded, so sharing can never make
  a served number older than the 30s cache already allowed;
* the session-surface warmer only runs while there is a session to warm.
"""

import asyncio
import importlib
import threading
import time
from datetime import datetime, timezone

import pytest

from src.analytics.forced_flow import ContractLeg

ff = importlib.import_module("src.api.routers.forced_flow")


def _book():
    legs = [
        ContractLeg(495, "C", 800, 0.18, 30 / 365),
        ContractLeg(500, "C", 1500, 0.18, 30 / 365),
        ContractLeg(505, "C", 1200, 0.18, 30 / 365),
        ContractLeg(490, "P", 700, 0.18, 30 / 365),
        ContractLeg(500, "P", 1400, 0.18, 30 / 365),
        ContractLeg(505, "P", 500, 0.18, 30 / 365),
    ]
    return {
        "legs": legs,
        "spot": 500.0,
        "timestamp": datetime(2026, 9, 28, 18, 0, tzinfo=timezone.utc),
        "r": 0.05,
        "q": 0.0,
        "session_days": 0.1,
    }


class _CountingLoader:
    def __init__(self, delay=0.0, results=None):
        self.calls = 0
        self._delay = delay
        self._results = list(results) if results is not None else None
        self._lock = threading.Lock()

    def __call__(self, symbol, expiry=None):
        with self._lock:
            self.calls += 1
        if self._delay:
            time.sleep(self._delay)
        if self._results is not None:
            return self._results.pop(0)
        return _book()


class _FakeDB:
    async def get_latest_gex_summary(self, symbol):
        return {"gamma_flip_point": 498.0}


@pytest.fixture(autouse=True)
def _clean_state():
    for state in (ff._cache, ff._book_cache, ff._book_tasks, ff._inflight_tasks):
        state.clear()
    yield
    for state in (ff._cache, ff._book_cache, ff._book_tasks, ff._inflight_tasks):
        state.clear()


def test_every_view_prices_from_one_load(monkeypatch):
    loader = _CountingLoader()
    monkeypatch.setattr(ff, "_load", loader)

    async def main():
        await ff._run(("curve", "SPY"), ff._curve_sync, "SPY", None, 0.02, 0.0, None)
        await ff._run(("charm-decay", "SPY"), ff._charm_decay_sync, "SPY", None, 26)
        await ff._run(("vanna", "SPY"), ff._vanna_ladder_sync, "SPY", None, -3.0, 3.0, 0.5)
        await ff._run(("surface", "SPY"), ff._surface_sync, "SPY", None, 0.02, 8)
        await ff._run(("scenario", "SPY"), ff._scenario_sync, "SPY", None, 0.01, 0.0, 0.0)
        return await ff.get_levels(symbol="SPY", db=_FakeDB())

    levels = asyncio.run(main())
    assert loader.calls == 1  # six views, one snapshot load
    assert levels.gamma_flip == 498.0 and levels.spot == 500.0


def test_concurrent_misses_share_one_load(monkeypatch):
    loader = _CountingLoader(delay=0.2)  # hold the load so the callers pile up
    monkeypatch.setattr(ff, "_load", loader)

    async def main():
        return await asyncio.gather(*[ff._shared_book("SPY") for _ in range(5)])

    books = asyncio.run(main())
    assert loader.calls == 1
    assert all(b is books[0] for b in books)
    assert not ff._book_tasks  # the in-flight registry drains once the load lands


def test_symbols_and_expiries_get_their_own_book(monkeypatch):
    loader = _CountingLoader()
    monkeypatch.setattr(ff, "_load", loader)

    async def main():
        await ff._shared_book("SPY")
        await ff._shared_book("spy")  # same book: the key is case-insensitive
        await ff._shared_book("QQQ")
        await ff._shared_book("SPY", "2026-09-28")

    asyncio.run(main())
    assert loader.calls == 3


def test_a_stale_book_is_reloaded(monkeypatch):
    loader = _CountingLoader()
    monkeypatch.setattr(ff, "_load", loader)
    stale = _book()
    stale["loaded_at"] = time.monotonic() - ff._BOOK_TTL_SECONDS - 1.0
    ff._book_cache[("SPY", None)] = stale

    book = asyncio.run(ff._shared_book("SPY"))
    assert loader.calls == 1
    assert book is not stale


def test_a_load_slower_than_the_ttl_is_still_shared(monkeypatch):
    # Aged from when the load FINISHED: a load that outlasts the TTL (a struggling
    # database) must still serve the callers after it, not arrive already expired
    # and send every request into another load.
    monkeypatch.setattr(ff, "_BOOK_TTL_SECONDS", 0.5)
    loader = _CountingLoader(delay=0.8)
    monkeypatch.setattr(ff, "_load", loader)

    async def main():
        await ff._shared_book("SPY")
        return await ff._shared_book("SPY")

    assert asyncio.run(main()) is not None
    assert loader.calls == 1


def test_a_degraded_snapshot_is_not_cached(monkeypatch):
    loader = _CountingLoader(results=[None, _book()])
    monkeypatch.setattr(ff, "_load", loader)

    async def main():
        return await ff._shared_book("SPY"), await ff._shared_book("SPY")

    first, second = asyncio.run(main())
    assert first is None and second is not None
    assert loader.calls == 2  # the None was retried, not served for 30s


def test_a_response_expires_with_the_book_it_was_priced_from(monkeypatch):
    # A view priced from a book that is already 29s old must be served for about
    # 1s more, not another full 30s.
    loader = _CountingLoader()
    monkeypatch.setattr(ff, "_load", loader)
    old = _book()
    old["loaded_at"] = time.monotonic() - (ff._BOOK_TTL_SECONDS - 1.0)
    ff._book_cache[("SPY", None)] = old

    key = ("scenario", "SPY")
    asyncio.run(ff._run(key, ff._scenario_sync, "SPY", None, 0.01, 0.0, 0.0))
    assert loader.calls == 0  # priced from the shared (still fresh) book
    assert ff._cache[key]["ts"] == old["loaded_at"]

    # And the aging itself: fresh inside the window, gone past it.
    ff._cache_put(("a",), {"v": 1}, as_of=time.monotonic() - (ff._RESPONSE_CACHE_TTL_SECONDS - 1))
    ff._cache_put(("b",), {"v": 2}, as_of=time.monotonic() - (ff._RESPONSE_CACHE_TTL_SECONDS + 1))
    assert ff._cache_get(("a",)) == {"v": 1}
    assert ff._cache_get(("b",)) is None


def test_the_book_cache_is_bounded(monkeypatch):
    # ``expiry`` is caller-supplied, so the keyspace is open-ended.
    monkeypatch.setattr(ff, "_BOOK_CACHE_MAX", 3)
    for i in range(6):
        book = _book()
        book["loaded_at"] = time.monotonic()
        ff._book_put(("SPY", f"2026-10-{i + 1:02d}"), book)
    assert len(ff._book_cache) == 3
    assert ("SPY", "2026-10-06") in ff._book_cache  # the newest survives


# ── The warmer only runs while there is a session to warm ─────────────────────


@pytest.mark.parametrize(
    "when, expected",
    [
        (datetime(2026, 9, 28, 9, 29), False),  # Monday, before the open
        (datetime(2026, 9, 28, 9, 30), True),
        (datetime(2026, 9, 28, 12, 0), True),
        (datetime(2026, 9, 28, 16, 15), True),  # the option close
        (datetime(2026, 9, 28, 16, 16), False),
        (datetime(2026, 9, 28, 23, 0), False),
        (datetime(2026, 9, 27, 12, 0), False),  # Sunday
    ],
)
def test_warm_window(when, expected):
    assert ff._warm_window_open(ff.ET.localize(when)) is expected


def test_warm_window_skips_market_holidays(monkeypatch):
    monkeypatch.setattr(ff, "is_trading_session", lambda day: False)
    assert ff._warm_window_open(ff.ET.localize(datetime(2026, 9, 28, 12, 0))) is False


def test_warm_cycle_warms_only_inside_the_window(monkeypatch):
    warmed = []

    async def fake_warm(sym):
        warmed.append(sym)

    monkeypatch.setattr(ff, "_warm_one", fake_warm)
    monkeypatch.setattr(ff, "_WARM_SYMBOLS", ["SPY", "SPX"])

    closed = asyncio.run(ff._warm_cycle(ff.ET.localize(datetime(2026, 9, 27, 12, 0))))
    assert closed == 0 and warmed == []

    opened = asyncio.run(ff._warm_cycle(ff.ET.localize(datetime(2026, 9, 28, 12, 0))))
    assert opened == 2 and warmed == ["SPY", "SPX"]
