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
    spot = 778.0
    lo, hi = ff._quantize_band(770.13, 785.42, spot)
    # Snapped OUT: lo floored, hi ceiled to the ~0.1%-of-spot step.
    assert lo <= 770.13 and hi >= 785.42
    # A tiny new extreme inside the same step must NOT move the band -- that is
    # what keeps the past-column cache warm through the day.
    lo2, hi2 = ff._quantize_band(770.05, 785.46, spot)
    assert (lo2, hi2) == (lo, hi)
    # A move past the step DOES widen it (and legitimately re-grids).
    lo3, _ = ff._quantize_band(766.0, 785.42, spot)
    assert lo3 < lo


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


def test_warm_one_caches_under_the_exact_endpoint_key(monkeypatch):
    # The whole point of the warmer is that on-demand GETs hit its cache -- so the
    # key _warm_one writes MUST equal the one get_session_surface reads. Stub the
    # (DB-backed) rebuild and assert the warmed value lands under that key.
    ff._cache.clear()
    fake = {"symbol": "SPY", "z": [[1.0]], "now_index": 0}
    monkeypatch.setattr(ff, "_session_surface_sync", lambda *a, **k: fake)
    ff._warm_one("SPY")
    assert ff._cache_get(_endpoint_key("SPY")) is fake
    ff._cache.clear()


def test_warm_one_skips_cache_when_rebuild_returns_none(monkeypatch):
    # A degraded snapshot (None) must NOT poison the cache with an empty entry.
    ff._cache.clear()
    monkeypatch.setattr(ff, "_session_surface_sync", lambda *a, **k: None)
    ff._warm_one("SPY")
    assert ff._cache_get(_endpoint_key("SPY")) is None
    ff._cache.clear()


def test_warm_loop_is_a_noop_when_disabled(monkeypatch):
    # With the warmer disabled it must return immediately without repricing.
    import asyncio

    monkeypatch.setattr(ff, "_WARM_ENABLED", False)
    called = []
    monkeypatch.setattr(ff, "_warm_one", lambda sym: called.append(sym))
    asyncio.run(ff.session_surface_warm_loop())
    assert called == []
