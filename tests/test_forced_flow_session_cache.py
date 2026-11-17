"""Session-field per-column cache + price-band quantization (pure, no DB).

The ``/session-surface`` endpoint memoizes each immutable PAST column so a warm
session reprices only the live now/projection columns. These exercise that memo's
building blocks in isolation -- quantization stability, get/put round-trip,
day-scoped pruning, and insertion-order eviction -- without touching the DB or
the FastAPI app.
"""

import importlib
from datetime import datetime

ff = importlib.import_module("src.api.routers.forced_flow")


def test_quantize_band_snaps_out_and_is_stable_under_small_moves():
    spot = 778.0
    lo, hi = ff._quantize_band(770.13, 785.42, spot)
    # Snapped OUT: lo floored, hi ceiled to the lattice step.
    assert lo <= 770.13 and hi >= 785.42
    # A tiny new extreme inside the same step must NOT move the band -- that is
    # what keeps the past-column cache warm through the day.
    lo2, hi2 = ff._quantize_band(770.05, 785.46, spot)
    assert (lo2, hi2) == (lo, hi)
    # A move past the step DOES widen it (and legitimately re-grids).
    lo3, _ = ff._quantize_band(766.0, 785.42, spot)
    assert lo3 < lo


def test_grid_step_is_a_1_2_5_step_that_holds_across_a_wide_range():
    assert ff._grid_step(6800.0) == 20.0
    assert ff._grid_step(680.0) == 2.0
    assert ff._grid_step(25000.0) == 100.0
    assert ff._grid_step(190.0) == 0.5
    # The same step for every price a symbol trades through in a day, and far
    # beyond: SPX anywhere from 4,100 to 9,999 snaps to 20-point lines.
    assert {ff._grid_step(p) for p in (4100.0, 5000.0, 6800.0, 9999.0)} == {20.0}


def test_the_band_holds_still_while_spot_ticks():
    """The regression behind 2026-11-16's rebuild storm. On most days the band is
    spot +/- span, and the step used to be a fixed fraction of the same spot, so
    the snapped band moved on every tick and every build missed the past-column
    cache. Thirty one-cent ticks must give one band; a real move must re-grid."""
    bands = {
        ff._quantize_band(spot * 0.98, spot * 1.02, spot)
        for spot in (680.00 + i * 0.01 for i in range(31))
    }
    assert len(bands) == 1
    moved = ff._quantize_band(686.80 * 0.98, 686.80 * 1.02, 686.80)  # +1%
    assert moved not in bands


def _stub_session_build(monkeypatch, day_range):
    """Run _session_surface_sync with no DB and trivial math. Returns the list
    every as-of chain read appends to: one per past column built cold."""

    class _Engine:
        def __init__(self, *_args, **_kwargs):
            pass

        def _build_forced_flow_legs(self, options, timestamp):
            return ["leg"]

    loads = []

    def _load_asof(sym, tau):
        loads.append(tau)
        return {"timestamp": tau, "spot": 680.0, "options": [{}]}

    monkeypatch.setattr(ff, "AnalyticsEngine", _Engine)
    monkeypatch.setattr(ff, "_load_asof", _load_asof)
    monkeypatch.setattr(ff, "_session_day_range", lambda db_symbol, day_start: day_range)
    monkeypatch.setattr(ff, "_session_price_bars", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        ff,
        "session_column_flow",
        lambda spot, legs, mtc, prices, r, q, floor: ([0.0] * len(prices), None, None),
    )
    ff._past_col_cache.clear()
    return loads


def _session_ctx(spot):
    return {
        "timestamp": ff.ET.localize(datetime(2026, 11, 16, 14, 0)),
        "spot": spot,
        "r": 0.04,
        "q": 0.0,
        "legs": ["leg"],
    }


def test_a_tick_in_spot_reuses_every_past_column(monkeypatch):
    loads = _stub_session_build(monkeypatch, (676.80, 683.10))
    try:
        first = ff._session_surface_sync(_session_ctx(680.10), "SPY", 0.02, 30, 8)
        built = len(loads)
        second = ff._session_surface_sync(_session_ctx(680.17), "SPY", 0.02, 30, 8)
        assert built == 21  # 14:00 ET: the 21 past buckets from the open
        assert len(loads) == built, "a 7-cent tick re-read past columns from the DB"
        assert second["prices"] == first["prices"]
    finally:
        ff._past_col_cache.clear()


def test_a_real_move_still_regrids(monkeypatch):
    loads = _stub_session_build(monkeypatch, (676.80, 690.00))
    try:
        first = ff._session_surface_sync(_session_ctx(680.10), "SPY", 0.02, 30, 8)
        built = len(loads)
        second = ff._session_surface_sync(_session_ctx(687.00), "SPY", 0.02, 30, 8)
        assert second["prices"] != first["prices"]
        assert len(loads) == 2 * built
    finally:
        ff._past_col_cache.clear()


def test_past_col_cache_get_put_and_day_prune():
    ff._past_col_cache.clear()
    k_today = ("2026-08-13", "SPY", 195.0, (770.0, 786.0, 41))
    k_yday = ("2026-08-12", "SPY", 195.0, (770.0, 786.0, 41))
    ff._past_col_put(k_today, {"z": [1.0], "magnet": 778.0})
    ff._past_col_put(k_yday, {"z": [2.0], "magnet": 777.0})
    assert ff._past_col_get(k_today)["z"] == [1.0]
    # Pruning to today's date drops every other trading day's entry.
    ff._past_col_prune_to_day("2026-08-13")
    assert ff._past_col_get(k_today) is not None
    assert ff._past_col_get(k_yday) is None
    ff._past_col_cache.clear()


def test_past_col_cache_evicts_oldest_over_cap():
    ff._past_col_cache.clear()
    original_max = ff._PAST_COL_CACHE_MAX
    try:
        ff._PAST_COL_CACHE_MAX = 3
        for i in range(5):
            ff._past_col_put(("2026-08-13", "SPY", float(i), ()), {"z": [i]})
        # Only the last 3 survive; the two oldest went first (insertion order).
        assert len(ff._past_col_cache) == 3
        assert ff._past_col_get(("2026-08-13", "SPY", 0.0, ())) is None
        assert ff._past_col_get(("2026-08-13", "SPY", 1.0, ())) is None
        assert ff._past_col_get(("2026-08-13", "SPY", 4.0, ())) is not None
    finally:
        ff._PAST_COL_CACHE_MAX = original_max
        ff._past_col_cache.clear()


def _endpoint_key(sym):
    # The exact response-cache key get_session_surface builds for a default call.
    return (
        "session-surface",
        sym,
        ff._DEFAULT_VIEW_SPAN_PCT,
        ff._SESSION_PAST_STEPS_DEFAULT,
        ff._SESSION_FUTURE_STEPS_DEFAULT,
        None,
    )


def _stub_book(monkeypatch):
    # The build prices from the shared book, which would otherwise load a real
    # snapshot. Any dict stands in -- the stubbed builds below never read it.
    ff._book_cache.clear()
    monkeypatch.setattr(ff, "_load", lambda symbol, expiry=None: {"spot": 1.0})


def test_warm_one_caches_under_the_exact_endpoint_key(monkeypatch):
    # The whole point of the warmer is that on-demand GETs hit its cache -- so the
    # key _warm_one writes MUST equal the one get_session_surface reads. Stub the
    # (DB-backed) rebuild and assert the warmed value lands under that key.
    import asyncio

    ff._cache.clear()
    ff._inflight_tasks.clear()
    _stub_book(monkeypatch)
    fake = {"symbol": "SPY", "z": [[1.0]], "now_index": 0}
    monkeypatch.setattr(ff, "_session_surface_sync", lambda *a, **k: fake)
    asyncio.run(ff._warm_one("SPY"))
    assert ff._cache_get(_endpoint_key("SPY")) is fake
    ff._cache.clear()
    ff._inflight_tasks.clear()
    ff._book_cache.clear()


def test_warm_one_skips_cache_when_rebuild_returns_none(monkeypatch):
    # A degraded snapshot (None) must NOT poison the cache with an empty entry.
    import asyncio

    ff._cache.clear()
    ff._inflight_tasks.clear()
    _stub_book(monkeypatch)
    monkeypatch.setattr(ff, "_session_surface_sync", lambda *a, **k: None)
    asyncio.run(ff._warm_one("SPY"))
    assert ff._cache_get(_endpoint_key("SPY")) is None
    ff._cache.clear()
    ff._inflight_tasks.clear()
    ff._book_cache.clear()


def test_warm_loop_is_a_noop_when_disabled(monkeypatch):
    # With the warmer disabled it must return immediately without repricing.
    import asyncio

    monkeypatch.setattr(ff, "_WARM_ENABLED", False)
    called = []
    monkeypatch.setattr(ff, "_warm_one", lambda sym: called.append(sym))
    asyncio.run(ff.session_surface_warm_loop())
    assert called == []


def test_session_surface_async_coalesces_concurrent_builds(monkeypatch):
    # The stampede fix: N concurrent callers for the SAME field must trigger only
    # ONE underlying rebuild; the rest await the shared Task and get its result.
    import asyncio
    import time as _t

    ff._cache.clear()
    ff._inflight_tasks.clear()
    _stub_book(monkeypatch)
    builds = []

    def slow_build(book, sym, *args, **kwargs):
        builds.append(sym)
        _t.sleep(0.2)  # hold the build (in its worker thread) so callers pile up
        return {"symbol": sym, "z": [[1.0]], "now_index": 0}

    monkeypatch.setattr(ff, "_session_surface_sync", slow_build)

    async def main():
        return await asyncio.gather(
            *[ff._session_surface_async("SPY", 0.02, 30, 8, None) for _ in range(5)]
        )

    results = asyncio.run(main())
    assert len(builds) == 1  # exactly one rebuild ran, not five
    assert len(results) == 5 and all(r is not None and r["z"] == [[1.0]] for r in results)
    ff._cache.clear()
    ff._inflight_tasks.clear()
    ff._book_cache.clear()
