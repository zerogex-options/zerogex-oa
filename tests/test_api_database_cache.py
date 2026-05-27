import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

from src.api import database as database_module
from src.api.database import DatabaseManager, _get_session_bounds


class _FakeConn:
    def __init__(self, row):
        self.row = row
        self.calls = 0

    async def fetchrow(self, _query, _symbol):
        self.calls += 1
        return self.row

    async def fetch(self, _query, *_args):
        self.calls += 1
        return [self.row] if self.row is not None else []


class _FakeFlowConn:
    def __init__(self, rows):
        self.rows = rows
        self.calls = 0
        self.last_query = None
        self.last_args = None

    async def fetch(self, *args):
        self.calls += 1
        if args:
            self.last_query = args[0]
            self.last_args = args[1:]
        return self.rows


def test_get_latest_quote_uses_short_ttl_cache():
    db = DatabaseManager()
    db._latest_quote_cache_ttl_seconds = 60.0
    conn = _FakeConn({"symbol": "SPY", "close": 500.0})

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    first = asyncio.run(db.get_latest_quote("spy"))
    second = asyncio.run(db.get_latest_quote("SPY"))

    assert first == second
    assert conn.calls == 1


def test_get_latest_gex_summary_cache_expires():
    # TTL-expiration used to sleep 20ms between calls, which flaked on
    # slow CI boxes. Patch time_module.monotonic instead: the first call
    # sees "now"=0 and stores an entry expiring at 0 + ttl; the second
    # call sees "now" well past the expiry and forces a refetch.
    db = DatabaseManager()
    db._latest_gex_summary_cache_ttl_seconds = 1.0
    conn = _FakeConn({"symbol": "SPY", "net_gex": 123.0})

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    fake_time = [0.0]
    with patch.object(database_module.time_module, "monotonic", lambda: fake_time[0]):
        first = asyncio.run(db.get_latest_gex_summary("SPY"))
        assert first is not None
        fake_time[0] = 100.0  # well past the 1s TTL
        second = asyncio.run(db.get_latest_gex_summary("SPY"))

    assert second == first
    assert conn.calls == 2


def test_get_latest_signal_score_enriched_includes_msi_payload():
    db = DatabaseManager()
    conn = _FakeConn(
        {
            "underlying": "SPY",
            "timestamp": "2026-01-01T15:30:00Z",
            "composite_score": 72.5,
            "components": {"net_gex_sign": {"points": 20.0, "score": 1.0, "contribution": 20.0}},
        }
    )

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    async def _empty_calibration_history(*_args, **_kwargs):
        return []

    db._get_signal_calibration_history = _empty_calibration_history  # type: ignore[method-assign]

    row = asyncio.run(db.get_latest_signal_score_enriched("SPY"))
    assert row is not None
    assert row["composite_score"] == 72.5


def test_get_signal_score_history_includes_msi_payload():
    db = DatabaseManager()
    conn = _FakeConn(
        {
            "underlying": "SPY",
            "timestamp": "2026-01-01T15:30:00Z",
            "composite_score": 68.0,
            "components": {"flip_distance": {"points": 25.0, "score": 0.5, "contribution": 12.5}},
        }
    )

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    rows = asyncio.run(db.get_signal_score_history("SPY", 5))
    assert len(rows) == 1
    assert rows[0]["composite_score"] == 68.0


def test_get_flow_uses_cache():
    db = DatabaseManager()
    db._flow_endpoint_cache_ttl_seconds = 60.0
    conn = _FakeFlowConn([{"timestamp": "2026-01-01T09:30:00Z", "symbol": "SPY"}])

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    first = asyncio.run(db.get_flow("spy", "current"))
    second = asyncio.run(db.get_flow("SPY", "current"))

    assert first == second
    assert conn.calls == 1


def test_get_flow_cache_expires():
    # See test_get_latest_gex_summary_cache_expires for why we patch
    # time_module.monotonic rather than sleeping.
    db = DatabaseManager()
    db._flow_endpoint_cache_ttl_seconds = 1.0
    conn = _FakeFlowConn(
        [
            {
                "timestamp": "2026-01-01T09:30:00Z",
                "symbol": "SPY",
                "option_type": "C",
                "strike": 500.0,
                "expiration": "2026-01-02",
            }
        ]
    )

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    fake_time = [0.0]
    with patch.object(database_module.time_module, "monotonic", lambda: fake_time[0]):
        first = asyncio.run(db.get_flow("SPY", "current"))
        assert first
        fake_time[0] = 100.0
        second = asyncio.run(db.get_flow("SPY", "current"))

    assert second == first
    assert conn.calls == 2


def test_get_flow_query_targets_unified_rollup():
    db = DatabaseManager()
    conn = _FakeFlowConn([])

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    asyncio.run(db.get_flow("SPY", "current"))

    assert conn.last_query is not None
    assert "flow_by_contract" in conn.last_query
    # New per-contract cumulative schema — no cross-type window aggregation.
    assert "raw_volume" in conn.last_query
    assert "raw_premium" in conn.last_query
    assert "net_volume" in conn.last_query
    assert "net_premium" in conn.last_query
    assert "PARTITION BY" not in conn.last_query


def test_prior_session_bounds_end_at_1615_et():
    _start, end = _get_session_bounds("prior")
    assert end.hour == 16
    assert end.minute == 15


def test_flow_session_bounds_open_at_0930_et():
    # Flow endpoints use RTH (09:30–16:15 ET), aligned to TradeStation's
    # volume-reset boundary so per-contract cumulatives make sense.
    from src.api.database import _get_flow_session_bounds

    start, end = _get_flow_session_bounds("prior")
    assert (start.hour, start.minute) == (9, 30)
    assert (end.hour, end.minute) == (16, 15)


def _with_fixed_session_bounds(monkey_bounds):
    """Install a fake _get_flow_session_bounds for a single get_flow call."""
    original = database_module._get_flow_session_bounds
    database_module._get_flow_session_bounds = lambda session="current": monkey_bounds
    return original


def _run_get_flow_with_bounds(bounds, intervals):
    db = DatabaseManager()
    db._flow_endpoint_cache_ttl_seconds = 0.0  # disable caching between invocations
    conn = _FakeFlowConn([])

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    original = _with_fixed_session_bounds(bounds)
    try:
        asyncio.run(db.get_flow("SPY", "current", intervals=intervals))
    finally:
        database_module._get_flow_session_bounds = original

    return conn


def test_get_flow_intervals_one_returns_single_bucket_when_session_open():
    et = ZoneInfo("America/New_York")
    # Session is open and 'now' falls mid-bucket at 14:42:17 ET. The most
    # recent queryable bucket is 14:40 (covering 14:40–14:45). intervals=1
    # must request that single bucket — both query bounds equal 14:40 UTC.
    session_start = datetime(2026, 4, 23, 9, 30, tzinfo=et)
    session_end = datetime(2026, 4, 23, 14, 42, 17, tzinfo=et)

    conn = _run_get_flow_with_bounds((session_start, session_end), intervals=1)

    _symbol, lo, hi = conn.last_args
    assert lo == hi, f"intervals=1 should request a single bucket, got [{lo}, {hi}]"
    assert lo == datetime(2026, 4, 23, 14, 40, tzinfo=et).astimezone(timezone.utc)


def test_get_flow_intervals_one_handles_session_close_boundary():
    et = ZoneInfo("America/New_York")
    # Session closed exactly at 16:15 — there is no bucket starting at 16:15
    # (that bucket would span 16:15–16:20, outside the session). The most
    # recent valid bucket is 16:10.
    session_start = datetime(2026, 4, 22, 9, 30, tzinfo=et)
    session_end = datetime(2026, 4, 22, 16, 15, tzinfo=et)

    conn = _run_get_flow_with_bounds((session_start, session_end), intervals=1)

    _symbol, lo, hi = conn.last_args
    expected = datetime(2026, 4, 22, 16, 10, tzinfo=et).astimezone(timezone.utc)
    assert lo == expected
    assert hi == expected


def test_get_flow_intervals_n_spans_exactly_n_buckets():
    et = ZoneInfo("America/New_York")
    session_start = datetime(2026, 4, 23, 9, 30, tzinfo=et)
    session_end = datetime(2026, 4, 23, 14, 42, 17, tzinfo=et)

    conn = _run_get_flow_with_bounds((session_start, session_end), intervals=5)

    _symbol, lo, hi = conn.last_args
    # 5 buckets ending at 14:40 → [14:20, 14:40] inclusive.
    assert (hi - lo).total_seconds() == 4 * 300
    assert hi == datetime(2026, 4, 23, 14, 40, tzinfo=et).astimezone(timezone.utc)
    assert lo == datetime(2026, 4, 23, 14, 20, tzinfo=et).astimezone(timezone.utc)


def test_get_flow_full_session_clamped_to_session_start():
    et = ZoneInfo("America/New_York")
    session_start = datetime(2026, 4, 23, 9, 30, tzinfo=et)
    session_end = datetime(2026, 4, 23, 14, 42, 17, tzinfo=et)

    # intervals larger than the session window clamps to session_start bucket.
    conn = _run_get_flow_with_bounds((session_start, session_end), intervals=10_000)

    _symbol, lo, hi = conn.last_args
    assert lo == session_start.astimezone(timezone.utc)
    assert hi == datetime(2026, 4, 23, 14, 40, tzinfo=et).astimezone(timezone.utc)


def test_option_contract_bars_surface_per_bar_classified_flow():
    """/api/option/contract must return per-bar ask/mid/bid_volume.

    Storage in option_chains is session-cumulative for volume AND the three
    classified-flow columns (see schema.sql COMMENT ON option_chains.*_volume),
    so the read path must convert each to a per-bar delta via LAG.  Regression
    guard for a bug introduced when the writer was migrated to cumulative
    storage but this reader kept selecting the raw columns.
    """
    db = DatabaseManager()
    conn = _FakeFlowConn([])

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    window_start = datetime(2026, 5, 22, 13, 30, tzinfo=timezone.utc)
    window_end = datetime(2026, 5, 22, 20, 0, tzinfo=timezone.utc)
    asyncio.run(db._fetch_option_contract_bars("SPY 250522C00500000", window_start, window_end))

    sql = conn.last_query
    assert sql is not None

    # Each classified-flow column must be aggregated to its per-minute max
    # in the CTE (to handle sub-minute snapshots, matching bar_volume) ...
    for partition_alias in ("bar_ask_volume", "bar_mid_volume", "bar_bid_volume"):
        assert partition_alias in sql, f"missing per-minute MAX alias {partition_alias}"

    # ... and surfaced as a LAG-based delta in the outer SELECT, named the
    # same as the original column so the OptionContractRow fields populate
    # with per-interval values (not session-cumulative running totals).
    for cum_alias, out_name in (
        ("bar_ask_volume", "ask_volume"),
        ("bar_mid_volume", "mid_volume"),
        ("bar_bid_volume", "bid_volume"),
    ):
        lag_expr = f"LAG({cum_alias}) OVER (ORDER BY bar_ts)"
        assert lag_expr in sql, f"{out_name} not computed via LAG of {cum_alias}"
        assert f")::bigint          AS {out_name}" in sql, f"{out_name} missing AS clause"

    # The raw column names must NOT appear as bare SELECT items -- those
    # would be session-cumulative and would re-introduce the bug.
    bare_indent = " " * 16
    for col in ("ask_volume", "mid_volume", "bid_volume"):
        bare = f"{bare_indent}{col},"
        assert bare not in sql, f"raw cumulative column selected: {col}"


def test_fetch_option_contract_bars_propagates_timeout():
    """Statement / command timeouts surface as bare TimeoutError whose str()
    is empty. The bar fetch must re-raise (not swallow to []) so the router
    can map it to a 504; the prior generic ``except Exception`` log only
    captured ``{e}`` and produced an empty error string in the logs.
    """

    class _TimeoutConn:
        async def fetch(self, *_args, **_kwargs):
            raise TimeoutError()

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield _TimeoutConn()

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    window_start = datetime(2026, 5, 22, 13, 30, tzinfo=timezone.utc)
    window_end = datetime(2026, 5, 22, 20, 0, tzinfo=timezone.utc)

    import pytest

    with pytest.raises((asyncio.TimeoutError, TimeoutError)):
        asyncio.run(db._fetch_option_contract_bars("SPY 250522C00500000", window_start, window_end))
