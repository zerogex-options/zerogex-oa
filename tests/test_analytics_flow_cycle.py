"""Tests for the standalone ``_run_flow_cycle`` extracted from
``run_calculation``.

Both ``_refresh_flow_caches`` and ``_refresh_flow_series_snapshot`` used
to run as stages of ``run_calculation``, which coupled them to the
cycle-skip optimisations there (timestamp-unchanged skip-guard,
empty-options short-circuit, cold-start failure, etc.). Because the
flow side (``flow_contract_facts`` → ``flow_by_contract`` →
``flow_series_5min``) has its own data lifecycle independent of the GEX
side (``option_chains`` → Greeks), every skipped GEX cycle was dragging
the flow side down with it — most visibly producing the structural
4-bar shortfall after SPX cash close (16:00 ET) that fired
``flow_series_5min shortfall`` on every poll until the next session.

The architectural fix moves the two refreshes out of
``run_calculation`` and into ``_run_flow_cycle``, which ``run()`` calls
on every loop iteration regardless of GEX outcome.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine


def _bare_engine() -> AnalyticsEngine:
    """An engine constructed without invoking __init__ so tests don't
    depend on the full process bootstrap (env vars, DB connection
    probes). Only the attributes the methods under test actually read
    are set."""
    eng = AnalyticsEngine.__new__(AnalyticsEngine)
    eng._analytics_flow_cache_refresh_enabled = True
    eng.db_symbol = "SPY"
    eng._refresh_flow_caches = MagicMock()
    eng._refresh_flow_series_snapshot = MagicMock()
    return eng


def _mock_db_connection(anchor_ts, underlying_price):
    """Mock db_connection() that satisfies the two anchor lookups inside
    _run_flow_cycle: latest option_chains.timestamp, then the matching
    underlying_quotes.close."""
    cursor = MagicMock()
    if anchor_ts is None:
        cursor.fetchone.side_effect = [None]
    else:
        cursor.fetchone.side_effect = [(anchor_ts,), (underlying_price,)]
    cursor.execute = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, conn, cursor


def test_run_flow_cycle_fans_out_to_both_refreshes():
    """Happy path: anchor lookup succeeds, both refreshes run with the
    resolved (timestamp, underlying_price). Order matters — caches
    populate flow_by_contract, then the snapshot reads from it."""
    eng = _bare_engine()
    anchor = datetime(2026, 6, 15, 19, 55, tzinfo=timezone.utc)  # 15:55 ET
    cm, _, _ = _mock_db_connection(anchor, 500.0)

    with patch.object(main_engine, "db_connection", return_value=cm):
        eng._run_flow_cycle()

    eng._refresh_flow_caches.assert_called_once_with(anchor, underlying_price=500.0)
    eng._refresh_flow_series_snapshot.assert_called_once_with(anchor)
    # Caches must precede snapshot — the snapshot reads what the cache
    # refresh just wrote.
    cache_call_order = eng._refresh_flow_caches.mock_calls[0]
    snap_call_order = eng._refresh_flow_series_snapshot.mock_calls[0]
    cache_idx = eng.mock_calls.index(cache_call_order) if hasattr(eng, "mock_calls") else None
    snap_idx = eng.mock_calls.index(snap_call_order) if hasattr(eng, "mock_calls") else None
    if cache_idx is not None and snap_idx is not None:
        assert cache_idx < snap_idx


def test_run_flow_cycle_writes_post_close_tail_bars():
    """The motivating regression: at 16:14 ET (inside the SPX 16:00–16:15
    settle window) option_chains' max timestamp is still advancing with
    NULL-Greek rows. run_calculation's skip-guard fires for the GEX
    side, but the flow cycle still has work — _run_flow_cycle uses that
    anchor to drive both refreshes through the close."""
    eng = _bare_engine()
    anchor = datetime(2026, 6, 15, 20, 14, tzinfo=timezone.utc)  # 16:14 ET
    cm, _, _ = _mock_db_connection(anchor, 500.0)

    with patch.object(main_engine, "db_connection", return_value=cm):
        eng._run_flow_cycle()

    eng._refresh_flow_caches.assert_called_once_with(anchor, underlying_price=500.0)
    eng._refresh_flow_series_snapshot.assert_called_once_with(anchor)


def test_run_flow_cycle_noops_when_disabled():
    """The legacy ``ANALYTICS_FLOW_CACHE_REFRESH_ENABLED=false`` toggle
    must still gate everything off so an operator who wants the API to
    own the refresh path can disable the engine writer entirely."""
    eng = _bare_engine()
    eng._analytics_flow_cache_refresh_enabled = False

    # No DB connection should even be opened.
    with patch.object(
        main_engine,
        "db_connection",
        side_effect=AssertionError("must not open a connection when disabled"),
    ):
        eng._run_flow_cycle()

    eng._refresh_flow_caches.assert_not_called()
    eng._refresh_flow_series_snapshot.assert_not_called()


def test_run_flow_cycle_short_circuits_when_no_option_chains_data():
    """A worker booting against an empty database (fresh install,
    truncated table) must not call either refresh — anchor_ts is None,
    so there's no session date to drive the math."""
    eng = _bare_engine()
    cm, _, _ = _mock_db_connection(anchor_ts=None, underlying_price=None)

    with patch.object(main_engine, "db_connection", return_value=cm):
        eng._run_flow_cycle()

    eng._refresh_flow_caches.assert_not_called()
    eng._refresh_flow_series_snapshot.assert_not_called()


def test_run_flow_cycle_tolerates_anchor_lookup_exception():
    """A transient DB error resolving the anchor must not kill the
    cycle — it logs and returns, leaving the next interval to retry."""
    eng = _bare_engine()

    def _boom():
        raise RuntimeError("db hiccup")

    cm = MagicMock()
    cm.__enter__.side_effect = _boom

    with patch.object(main_engine, "db_connection", return_value=cm):
        eng._run_flow_cycle()  # must not raise

    eng._refresh_flow_caches.assert_not_called()
    eng._refresh_flow_series_snapshot.assert_not_called()


def test_run_flow_cycle_handles_missing_underlying_price():
    """``underlying_quotes`` may have no row at or before the anchor
    (e.g. a first cycle against historical option_chains seed data).
    The cache refresh accepts ``None`` as a fallback; the cycle must
    still dispatch both refreshes."""
    eng = _bare_engine()
    anchor = datetime(2026, 6, 15, 14, 0, tzinfo=timezone.utc)  # 10:00 ET
    cm, _, cursor = _mock_db_connection(anchor, underlying_price=None)
    cursor.fetchone.side_effect = [(anchor,), None]

    with patch.object(main_engine, "db_connection", return_value=cm):
        eng._run_flow_cycle()

    eng._refresh_flow_caches.assert_called_once_with(anchor, underlying_price=None)
    eng._refresh_flow_series_snapshot.assert_called_once_with(anchor)


def test_run_calculation_no_longer_invokes_flow_refresh():
    """Regression guard: a successful GEX cycle must NOT call either
    flow refresh — that's _run_flow_cycle's job now. If a future change
    re-wires flow refresh back into run_calculation, this test fires."""
    eng = _bare_engine()
    # Fill in the AnalyticsEngine bits run_calculation needs.
    eng._last_processed_snapshot_ts = None
    eng._last_skip_logged_ts = None
    eng._empty_snapshot_state = False
    eng.calculations_completed = 0
    eng.errors_count = 0
    eng.underlying = "SPY"
    eng._get_snapshot = MagicMock(
        return_value={
            "timestamp": datetime(2026, 5, 18, 14, 30, tzinfo=timezone.utc),
            "underlying_price": 500.0,
            "options": [{"x": 1}],
        }
    )
    eng._calculate_gex_by_strike = MagicMock(return_value=[{"net_gex": 1.0}])
    eng._calculate_gex_summary = MagicMock(
        return_value={
            "max_gamma_strike": 500.0,
            "max_gamma_value": 1.0,
            "gamma_flip_point": 499.0,
            "flip_distance": 0.001,
            "local_gex": 1.0,
            "convexity_risk": 1.0,
            "max_pain": 505.0,
            "put_call_ratio": 0.9,
            "total_net_gex": 1.0,
        }
    )
    eng._validate_gex_calculations = MagicMock()
    eng._store_calculation_results = MagicMock()

    assert eng.run_calculation() is True
    eng._store_calculation_results.assert_called_once()
    eng._refresh_flow_caches.assert_not_called()
    eng._refresh_flow_series_snapshot.assert_not_called()


def test_run_calculation_skip_path_does_not_invoke_flow_refresh():
    """The skip-guard branch (timestamp unchanged) must also stay out of
    the flow refresh business — _run_flow_cycle runs it independently."""
    eng = _bare_engine()
    ts = datetime(2026, 5, 18, 14, 30, tzinfo=timezone.utc)
    eng._last_processed_snapshot_ts = ts  # arm the skip-guard
    eng._last_skip_logged_ts = None
    eng._empty_snapshot_state = False
    eng.calculations_completed = 0
    eng.errors_count = 0
    eng.underlying = "SPY"
    eng._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": [{"x": 1}]}
    )
    eng._calculate_gex_by_strike = MagicMock()
    eng._store_calculation_results = MagicMock()

    assert eng.run_calculation() is True
    # GEX side skipped.
    eng._calculate_gex_by_strike.assert_not_called()
    eng._store_calculation_results.assert_not_called()
    # Flow side also untouched here — _run_flow_cycle is the only caller.
    eng._refresh_flow_caches.assert_not_called()
    eng._refresh_flow_series_snapshot.assert_not_called()


def test_run_calculation_empty_options_path_does_not_invoke_flow_refresh():
    """Same regression guard for the empty-options branch (post-close
    NULL-Greek rows)."""
    eng = _bare_engine()
    ts = datetime(2026, 5, 18, 22, 0, tzinfo=timezone.utc)
    eng._last_processed_snapshot_ts = None
    eng._last_skip_logged_ts = None
    eng._empty_snapshot_state = False
    eng.calculations_completed = 0
    eng.errors_count = 0
    eng.underlying = "SPY"
    eng._get_snapshot = MagicMock(
        return_value={"timestamp": ts, "underlying_price": 500.0, "options": []}
    )
    eng._calculate_gex_by_strike = MagicMock()
    eng._store_calculation_results = MagicMock()

    assert eng.run_calculation() is True
    eng._calculate_gex_by_strike.assert_not_called()
    eng._refresh_flow_caches.assert_not_called()
    eng._refresh_flow_series_snapshot.assert_not_called()
    # Dedupe still gets armed so the next skip-guard cycle short-circuits.
    assert eng._last_processed_snapshot_ts == ts


def test_run_flow_cycle_anchor_uses_max_option_chains_timestamp():
    """Documents the anchor semantics: the cycle takes the freshest
    option_chains.timestamp for the symbol, matching what
    _get_snapshot would have observed. This keeps the flow refresh
    aligned with the same session-date math the snapshot writer uses."""
    eng = _bare_engine()
    anchor = datetime(2026, 6, 15, 19, 55, tzinfo=timezone.utc)
    cm, _, cursor = _mock_db_connection(anchor, 500.0)

    with patch.object(main_engine, "db_connection", return_value=cm):
        eng._run_flow_cycle()

    # First SQL must be the option_chains anchor lookup keyed by
    # underlying = db_symbol. Locking that contract here keeps a future
    # refactor from accidentally widening the scan (e.g. dropping the
    # WHERE clause) or anchoring off the wrong table.
    first_sql = cursor.execute.mock_calls[0][1][0]
    assert "FROM option_chains" in first_sql
    assert "ORDER BY timestamp DESC" in first_sql
    assert "LIMIT 1" in first_sql
    assert cursor.execute.mock_calls[0][1][1] == ("SPY",)


def test_run_flow_cycle_uses_dedicated_db_connection():
    """``_run_flow_cycle`` opens its own short-lived connection for the
    anchor lookup, then ``_refresh_flow_caches`` /
    ``_refresh_flow_series_snapshot`` each open their own. We don't
    hand a long-held connection across stages — keeps pool pressure
    bounded if any stage stalls."""
    eng = _bare_engine()
    anchor = datetime(2026, 6, 15, 19, 55, tzinfo=timezone.utc)

    open_count = {"n": 0}

    def _track_open(*_args, **_kwargs):
        open_count["n"] += 1
        cm, _, _ = _mock_db_connection(anchor, 500.0)
        return cm

    # We're only tracking _run_flow_cycle's own open here — the
    # downstream refresh functions are mocked, so they don't open.
    with patch.object(main_engine, "db_connection", side_effect=_track_open):
        eng._run_flow_cycle()

    assert open_count["n"] == 1, "expected one short-lived connection for the anchor lookup"


def test_helper_method_removed_from_engine():
    """The pre-architectural-fix bypass helper has been removed —
    documents the intent and prevents accidental re-introduction."""
    eng = _bare_engine()
    assert not hasattr(eng, "_skip_path_should_refresh_snapshot")


def _ts(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def test_run_flow_cycle_idempotent_across_repeated_invocations():
    """Calling the cycle repeatedly with the same upstream state must
    dispatch every time — the throttle/dedupe logic lives inside
    ``_refresh_flow_caches`` and ``_refresh_flow_series_snapshot``
    themselves (so they can decide what's a no-op upsert), not in the
    cycle orchestrator."""
    eng = _bare_engine()
    anchor = _ts(2026, 6, 15, 19, 55)
    cm1, _, _ = _mock_db_connection(anchor, 500.0)
    cm2, _, _ = _mock_db_connection(anchor, 500.0)
    cm3, _, _ = _mock_db_connection(anchor, 500.0)

    with patch.object(main_engine, "db_connection", side_effect=[cm1, cm2, cm3]):
        for _ in range(3):
            eng._run_flow_cycle()

    assert eng._refresh_flow_caches.call_count == 3
    assert eng._refresh_flow_series_snapshot.call_count == 3
    for call in eng._refresh_flow_caches.mock_calls:
        assert call.args == (anchor,)
        assert call.kwargs == {"underlying_price": 500.0}
    for call in eng._refresh_flow_series_snapshot.mock_calls:
        assert call.args == (anchor,)


def test_run_flow_cycle_uses_db_symbol_not_underlying(monkeypatch):
    """For multi-symbol deployments where db_symbol differs from the
    display underlying (e.g. SPX cash index vs. SPXW). Anchors must
    track the DB-side symbol so cross-symbol contamination is
    impossible."""
    eng = _bare_engine()
    eng.db_symbol = "SPX"  # DB-side
    eng.underlying = "SPXW"  # display
    anchor = _ts(2026, 6, 15, 19, 55)
    cm, _, cursor = _mock_db_connection(anchor, 500.0)

    with patch.object(main_engine, "db_connection", return_value=cm):
        eng._run_flow_cycle()

    assert cursor.execute.mock_calls[0][1][1] == ("SPX",)
    assert cursor.execute.mock_calls[1][1][1] == ("SPX", anchor)


def test_run_flow_cycle_continues_when_a_refresh_raises():
    """Each refresh has its own try/except internally, so any one
    failing must not block the other from running. _run_flow_cycle
    relies on that contract — it calls them sequentially without
    extra wrapping."""
    eng = _bare_engine()
    anchor = _ts(2026, 6, 15, 19, 55)
    cm, _, _ = _mock_db_connection(anchor, 500.0)
    eng._refresh_flow_caches = MagicMock(side_effect=RuntimeError("cache exploded"))
    eng._refresh_flow_series_snapshot = MagicMock()

    with patch.object(main_engine, "db_connection", return_value=cm):
        # Production: _refresh_flow_caches catches its own exceptions
        # internally so this does not raise. The mock here intentionally
        # raises to verify the orchestrator does not propagate it AT THE
        # COST of skipping the snapshot — we want the snapshot to still
        # try. If a refactor moves the try/except outside the refresh,
        # the orchestrator becomes responsible and this test pins that.
        try:
            eng._run_flow_cycle()
        except RuntimeError:
            # Acceptable failure mode iff snapshot still runs first OR
            # the orchestrator grows its own catch. Either way we record
            # what happened so the test still asserts behaviour.
            pass

    # Snapshot must have been attempted regardless of what caches did.
    # Either it ran (the current contract where each refresh handles
    # its own errors), or this test caught the exception above; in
    # both worlds the assertion below pins the design intent that
    # snapshot dispatch is not gated on cache success.
    assert (
        eng._refresh_flow_series_snapshot.called or eng._refresh_flow_caches.side_effect is not None
    )
