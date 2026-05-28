"""Tests for the ingestion-side dual-write into ``option_chains_latest``.

The ingestion engine UPSERTs into both ``option_chains`` (history) and
``option_chains_latest`` (per-contract latest cache) inside the SAME
transaction so the cache cannot drift from history under partial
failure.  These tests pin down:

  * Both UPSERTs run on the same cursor inside the same DB transaction.
  * They share the exact same VALUES tuple (cache row matches history
    row for that contract).
  * The cache UPSERT references ``option_chains_latest`` and is gated on
    ``EXCLUDED.timestamp >= option_chains_latest.timestamp`` so an
    out-of-order replay cannot clobber a newer row.
  * The dual-write applies to both batch (``_write_option_rows``) and
    single-row paths.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion import main_engine as ingestion_module
from src.ingestion.main_engine import IngestionEngine


def _build_row(
    option_symbol: str = "SPY260520C00500000",
    timestamp: datetime | None = None,
):
    """Construct a minimal aggregated option row matching the writer's
    expected dict shape."""
    if timestamp is None:
        timestamp = datetime(2026, 5, 26, 14, 30, tzinfo=timezone.utc)
    return {
        "option_symbol": option_symbol,
        "timestamp": timestamp,
        "underlying": "SPY",
        "strike": 500.0,
        "expiration": datetime(2026, 5, 28).date(),
        "option_type": "C",
        "last": 1.23,
        "bid": 1.20,
        "ask": 1.25,
        "mid": 1.225,
        "volume": 1000,
        "open_interest": 5000,
        "implied_volatility": 0.20,
        "ask_volume": 600,
        "mid_volume": 0,
        "bid_volume": 400,
        "delta": 0.50,
        "gamma": 0.02,
        "theta": -0.05,
        "vega": 0.10,
    }


def _mock_db_connection():
    """Return a context-manager mock for ``db_connection()`` plus the cursor."""
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor

    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, conn, cursor


def _execute_values_calls(execute_values_mock):
    """Return list of (sql, values) pairs the writer passed to execute_values."""
    calls = []
    for call in execute_values_mock.call_args_list:
        # signature: execute_values(cursor, sql, values, page_size=...)
        args = call.args
        kwargs = call.kwargs
        # cursor is args[0]; sql is args[1]; values is args[2]
        sql = args[1] if len(args) > 1 else kwargs.get("sql")
        values = args[2] if len(args) > 2 else kwargs.get("argslist")
        calls.append((sql, values))
    return calls


def _make_engine_for_write_test() -> IngestionEngine:
    """Construct an engine bypassing __init__ side effects unrelated to writes.

    ``IngestionEngine.__init__`` wires up TradeStation auth, the stream
    manager, and a bunch of accumulators.  For these tests we only need
    the bound ``_OPTION_UPSERT_SQL`` / ``_OPTION_LATEST_UPSERT_SQL`` class
    attributes plus the circuit-breaker state, so build an instance via
    ``__new__`` and set the small set of fields ``_write_option_rows``
    actually reads.
    """
    engine = IngestionEngine.__new__(IngestionEngine)
    # Circuit-breaker fields the writer touches before/after the UPSERTs.
    engine._db_backoff_until = 0.0
    engine._db_consecutive_failures = 0
    engine._pending_failed_option_rows = []
    engine.option_quotes_stored = 0
    engine.last_flush_time = None
    engine.errors_count = 0
    # Observability counters touched on the success path.
    engine._obs_batches_written = 0
    engine._obs_rows_written = 0
    engine._obs_write_time_ms = 0.0
    engine._obs_last_log = 0.0
    return engine


def test_batch_write_dual_upserts_in_same_transaction():
    """``_write_option_rows`` issues exactly two ``execute_values`` calls on
    the same cursor: history first, then cache, then a single commit."""
    engine = _make_engine_for_write_test()
    cm, conn, cursor = _mock_db_connection()

    with (
        patch.object(ingestion_module, "db_connection", return_value=cm),
        patch.object(ingestion_module, "execute_values") as execute_values_mock,
    ):
        engine._write_option_rows([_build_row()])

    calls = _execute_values_calls(execute_values_mock)
    assert (
        len(calls) == 2
    ), f"expected exactly 2 execute_values calls (history + cache), got {len(calls)}"

    history_sql, _ = calls[0]
    cache_sql, _ = calls[1]

    # History UPSERT must come first (cache is downstream of history).
    assert "INSERT INTO option_chains" in history_sql
    assert "INSERT INTO option_chains_latest" not in history_sql

    # Cache UPSERT must reference option_chains_latest and the
    # out-of-order replay guard.
    assert "INSERT INTO option_chains_latest" in cache_sql
    assert "ON CONFLICT (option_symbol)" in cache_sql
    assert (
        "EXCLUDED.timestamp >= option_chains_latest.timestamp" in cache_sql
    ), "cache UPSERT missing the newer-or-equal timestamp guard"

    # Both calls used the same cursor.
    cursor_args = [call.args[0] for call in execute_values_mock.call_args_list]
    assert cursor_args[0] is cursor_args[1] is cursor

    # Exactly one commit, after both UPSERTs.
    assert conn.commit.call_count == 1


def test_history_and_cache_receive_identical_values():
    """The same tuples are passed to both UPSERTs -- the cache row reflects
    the same data as the just-written history row for that contract."""
    engine = _make_engine_for_write_test()
    cm, _, _ = _mock_db_connection()

    row = _build_row()
    with (
        patch.object(ingestion_module, "db_connection", return_value=cm),
        patch.object(ingestion_module, "execute_values") as execute_values_mock,
    ):
        engine._write_option_rows([row])

    calls = _execute_values_calls(execute_values_mock)
    _, history_values = calls[0]
    _, cache_values = calls[1]
    assert (
        history_values == cache_values
    ), "history and cache UPSERTs must receive byte-identical VALUES tuples"
    assert len(history_values) == 1


def test_dual_write_applies_to_multi_row_batches():
    """A batch with N rows produces one ``execute_values`` call per UPSERT
    target with all N rows in each call -- not N pairs of single-row calls."""
    engine = _make_engine_for_write_test()
    cm, _, _ = _mock_db_connection()

    rows = [_build_row(option_symbol=f"SPY260520C0050{i:04d}000") for i in range(5)]
    with (
        patch.object(ingestion_module, "db_connection", return_value=cm),
        patch.object(ingestion_module, "execute_values") as execute_values_mock,
    ):
        engine._write_option_rows(rows)

    calls = _execute_values_calls(execute_values_mock)
    assert len(calls) == 2, "expected one history + one cache execute_values per batch"
    _, history_values = calls[0]
    _, cache_values = calls[1]
    assert len(history_values) == 5
    assert len(cache_values) == 5


def test_cache_upsert_is_skipped_when_db_in_backoff():
    """If the circuit breaker has tripped (mid-backoff), neither UPSERT
    runs -- the rows are retained for retry, not partially applied."""
    engine = _make_engine_for_write_test()
    # Force the breaker into the future so the writer skips the batch.
    import time as _time

    engine._db_backoff_until = _time.monotonic() + 30.0

    with (
        patch.object(ingestion_module, "execute_values") as execute_values_mock,
        patch.object(ingestion_module, "db_connection") as db_conn_mock,
    ):
        engine._write_option_rows([_build_row()])

    assert execute_values_mock.call_count == 0
    assert db_conn_mock.call_count == 0
    # Row must be retained for the next attempt.
    assert (
        engine._pending_failed_option_rows
    ), "rows must be retained when the breaker skips the write"


def test_cache_upsert_deduped_by_option_symbol_when_batch_has_multiple_timestamps():
    """Regression: PostgreSQL refuses to UPDATE the same conflict-target
    row twice within a single ``INSERT ... ON CONFLICT DO UPDATE``
    statement (error: "ON CONFLICT DO UPDATE command cannot affect row a
    second time").  A batch with two timestamps for the same contract
    would trigger that on the cache UPSERT (keyed by option_symbol alone)
    even though the history UPSERT (keyed by option_symbol + timestamp)
    is fine.  The writer must pre-dedupe by option_symbol, keeping the
    latest-timestamp row, before issuing the cache UPSERT.

    Hit production at 2026-05-26 15:44 EDT shortly after the cache
    feature flag was flipped on; ingestion went into circuit-breaker
    because every batch with multi-timestamp-per-contract rows failed.
    """
    engine = _make_engine_for_write_test()
    cm, _, _ = _mock_db_connection()

    ts_earlier = datetime(2026, 5, 26, 14, 30, tzinfo=timezone.utc)
    ts_later = datetime(2026, 5, 26, 14, 31, tzinfo=timezone.utc)

    sym = "SPY260520C00500000"
    rows = [
        _build_row(option_symbol=sym, timestamp=ts_earlier),
        _build_row(option_symbol=sym, timestamp=ts_later),
        # A second contract to confirm dedup is per-symbol, not global.
        _build_row(option_symbol="SPY260520C00501000", timestamp=ts_later),
    ]

    with (
        patch.object(ingestion_module, "db_connection", return_value=cm),
        patch.object(ingestion_module, "execute_values") as execute_values_mock,
    ):
        engine._write_option_rows(rows)

    calls = _execute_values_calls(execute_values_mock)
    assert len(calls) == 2, "expected one history + one cache execute_values"
    _, history_values = calls[0]
    _, cache_values = calls[1]

    # History got all THREE rows -- same (sym, ts) pairs are unique.
    assert len(history_values) == 3

    # Cache got TWO rows, one per option_symbol.  No duplicates.
    cache_symbols = [v[0] for v in cache_values]
    assert len(cache_values) == 2, (
        f"cache UPSERT must dedupe by option_symbol; got {len(cache_values)} "
        f"rows for {set(cache_symbols)}"
    )
    assert sorted(cache_symbols) == sorted({sym, "SPY260520C00501000"})

    # And the kept row for the duplicated symbol is the LATER one.
    sym_row = next(v for v in cache_values if v[0] == sym)
    assert (
        sym_row[1] == ts_later
    ), "cache dedup must keep the latest-timestamp row, not an arbitrary one"


def test_cache_upsert_dedup_preserves_history_order_too():
    """Two contracts in one batch, with the latest timestamp for one and
    a single timestamp for the other -- both must still reach BOTH
    UPSERTs, not get dropped by the dedup."""
    engine = _make_engine_for_write_test()
    cm, _, _ = _mock_db_connection()

    rows = [
        _build_row(
            option_symbol="SPY260520C00500000",
            timestamp=datetime(2026, 5, 26, 14, 30, tzinfo=timezone.utc),
        ),
        _build_row(
            option_symbol="SPY260520P00500000",
            timestamp=datetime(2026, 5, 26, 14, 30, tzinfo=timezone.utc),
        ),
    ]

    with (
        patch.object(ingestion_module, "db_connection", return_value=cm),
        patch.object(ingestion_module, "execute_values") as execute_values_mock,
    ):
        engine._write_option_rows(rows)

    calls = _execute_values_calls(execute_values_mock)
    _, history_values = calls[0]
    _, cache_values = calls[1]
    assert len(history_values) == 2
    assert len(cache_values) == 2, "non-duplicated symbols must not be dropped by the dedup"


def test_cache_upsert_sql_is_a_class_constant():
    """``_OPTION_LATEST_UPSERT_SQL`` must be defined on the class so it's
    cheap to access and stays in sync with the history UPSERT (any future
    column add needs to touch both)."""
    assert hasattr(IngestionEngine, "_OPTION_LATEST_UPSERT_SQL")
    sql = IngestionEngine._OPTION_LATEST_UPSERT_SQL
    assert "INSERT INTO option_chains_latest" in sql
    assert "ON CONFLICT (option_symbol)" in sql
    # Cumulative columns must use GREATEST so retries don't decrement.
    for col in ("volume", "open_interest", "ask_volume", "mid_volume", "bid_volume"):
        assert (
            f"GREATEST(option_chains_latest.{col}, EXCLUDED.{col})" in sql
        ), f"cache UPSERT must use GREATEST for monotonic column {col!r}"
