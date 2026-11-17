"""Session-field per-column cache + price-band quantization (pure, no DB).

The ``/session-surface`` endpoint memoizes each immutable PAST column so a warm
session reprices only the live now/projection columns. These exercise that memo's
building blocks in isolation -- quantization stability, get/put round-trip,
day-scoped pruning, and insertion-order eviction -- without touching the DB or
the FastAPI app.
"""

import importlib

ff = importlib.import_module("src.api.routers.forced_flow")


def test_quantize_band_snaps_out_and_is_stable_under_small_moves():
    ref = 778.0
    lo, hi = ff._quantize_band(770.13, 785.42, ref)
    # Snapped OUT: lo floored, hi ceiled to the step (a fraction of ref).
    assert lo <= 770.13 and hi >= 785.42
    # A tiny new extreme inside the same step must NOT move the band -- that is
    # what keeps the past-column cache warm through the day.
    lo2, hi2 = ff._quantize_band(770.05, 785.46, ref)
    assert (lo2, hi2) == (lo, hi)
    # A move past the step DOES widen it (and legitimately re-grids).
    lo3, _ = ff._quantize_band(766.0, 785.42, ref)
    assert lo3 < lo


# The day so far: low, high, and the session's first price (the step reference).
_DAY = (676.10, 681.90, 678.00)


def test_session_band_holds_still_while_spot_moves_inside_the_days_range():
    """The regression behind the 2026-11-16 RDS load: the old band was spot +/-
    span snapped to 0.1% of SPOT, so it moved with every cent and every refresh
    cold-missed every past column. Inside the day's range, spot must not matter."""
    bands = {ff._session_band(spot, 0.02, *_DAY) for spot in (676.10, 678.00, 680.03, 681.90)}
    assert len(bands) == 1


def test_session_band_covers_the_days_range_and_spot_plus_minus_span():
    lo, hi = ff._session_band(681.90, 0.02, *_DAY)
    assert lo <= 676.10 * 0.98 and hi >= 681.90 * 1.02
    # Still a band around the day, not the whole chain: within one step of it.
    step = 678.00 * ff._SESSION_GRID_QUANTUM_PCT
    assert lo > 676.10 * 0.98 - step and hi < 681.90 * 1.02 + step


def test_session_band_widens_only_when_a_new_extreme_crosses_a_step():
    lo, hi = ff._session_band(680.0, 0.02, *_DAY)
    # A new low that stays inside the snapped band's step: same band.
    assert ff._session_band(680.0, 0.02, 675.50, 681.90, 678.00) == (lo, hi)
    # One that crosses it re-grids, downward only.
    lo2, hi2 = ff._session_band(680.0, 0.02, 674.00, 681.90, 678.00)
    assert lo2 < lo and hi2 == hi


def test_session_band_falls_back_to_the_live_spot_without_a_day_range():
    lo, hi = ff._session_band(680.0, 0.02, None, None, None)
    assert lo <= 680.0 * 0.98 and hi >= 680.0 * 1.02
    assert hi - lo < 680.0 * (0.04 + 2 * ff._SESSION_GRID_QUANTUM_PCT) + 1e-9


class _FakeEngine:
    def __init__(self, *args, **kwargs):
        pass

    def _build_forced_flow_legs(self, options, timestamp):
        return [object()]


def _build(monkeypatch, spot, now, day=_DAY):
    """One _session_surface_sync build with the database stubbed out. Returns
    (as-of queries it ran, the response). The as-of query is the expensive part:
    one DISTINCT ON over option_chains per cold past column."""
    calls = []

    def load_asof(sym, tau):
        calls.append(tau)
        return {"timestamp": tau, "spot": spot, "options": [{}]}

    monkeypatch.setattr(ff, "_load_asof", load_asof)
    monkeypatch.setattr(ff, "AnalyticsEngine", _FakeEngine)
    monkeypatch.setattr(ff, "_session_day_range", lambda db_symbol, open_dt: day)
    monkeypatch.setattr(ff, "_session_price_bars", lambda *a, **k: [])
    monkeypatch.setattr(ff, "session_column_flow", lambda *a, **k: ([0.0] * 41, None, None))
    ctx = {"timestamp": now, "spot": spot, "r": 0.04, "q": 0.0, "legs": []}
    result = ff._session_surface_sync(ctx, "SPY", ff._DEFAULT_VIEW_SPAN_PCT, 30, 8)
    return len(calls), result


def _et(hh, mm):
    from datetime import datetime

    return ff.ET.localize(datetime(2026, 11, 16, hh, mm))


def test_a_few_cents_of_spot_reuses_every_past_column(monkeypatch):
    """At 14:00 ET there are 21 past columns. The first build runs their 21 as-of
    queries; refreshes with spot moving inside the day's range run none (the old
    grid ran all 21 again on a 3-cent move); a new low past a step re-runs them;
    and 13 minutes later only the one newly-past column is built."""
    ff._past_col_cache.clear()
    try:
        assert _build(monkeypatch, 680.00, _et(14, 0))[0] == 21
        assert _build(monkeypatch, 680.03, _et(14, 0))[0] == 0
        assert _build(monkeypatch, 681.40, _et(14, 0))[0] == 0
        assert _build(monkeypatch, 676.50, _et(14, 0))[0] == 0
        broken_low = (674.00, 681.90, 678.00)
        assert _build(monkeypatch, 674.20, _et(14, 0), day=broken_low)[0] == 21
        assert _build(monkeypatch, 675.10, _et(14, 13), day=broken_low)[0] == 1
    finally:
        ff._past_col_cache.clear()


def test_every_worker_builds_the_same_grid(monkeypatch):
    """Each uvicorn worker caches on its own, and a viewer's polls land on any of
    them. The grid depends only on the day's data, so two workers whose books
    caught spot at different prices still draw -- and cache -- the same axis."""
    ff._past_col_cache.clear()
    try:
        _, first_worker = _build(monkeypatch, 680.00, _et(14, 0))
        ff._past_col_cache.clear()
        _, second_worker = _build(monkeypatch, 677.25, _et(14, 0))
        assert first_worker["prices"] == second_worker["prices"]
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
