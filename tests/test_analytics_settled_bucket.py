"""A closed minute is final: nothing after the session may rewrite it.

After the close the snapshot stays on the session's last bucket until the next
session. Two things recomputed it there and overwrote the row the session had
published: ingestion writing overnight into rows that still carry that bucket's
timestamps (14 of 15 production sessions from 2026-09-08), and a restart, whose
first cycle recomputes the frozen bucket with nothing new written (2026-10-01,
06:32). A bucket past the settle window by the clock, whose row is already
stored, is now kept.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.analytics.main_engine import AnalyticsEngine
from tests.test_analytics_empty_snapshot_quiet import _stub_pipeline

CLOSE = datetime(2026, 9, 30, 19, 59, tzinfo=timezone.utc)  # 15:59 ET

# conftest stubs the lookup for every test; captured at import, before it does.
_REAL_ROW_EXISTS = AnalyticsEngine._gex_summary_row_exists


def _snapshot(written_after: timedelta) -> dict:
    return {
        "timestamp": CLOSE,
        "underlying_price": 6650.0,
        "options": [{"strike": 6650.0}],
        "spot_anchored": False,
        "data_updated_at": CLOSE + written_after,
    }


def _engine(row_exists: bool = True) -> AnalyticsEngine:
    engine = AnalyticsEngine(underlying="SPX")
    _stub_pipeline(engine)
    engine._gex_summary_row_exists = MagicMock(return_value=row_exists)
    return engine


def _run(engine: AnalyticsEngine, *, now_after: timedelta, written_after: timedelta) -> bool:
    engine._utcnow = lambda: CLOSE + now_after
    engine._get_snapshot = MagicMock(return_value=_snapshot(written_after))
    return engine.run_calculation()


def test_after_hours_writes_do_not_rewrite_the_published_closing_row():
    engine = _engine()
    assert _run(engine, now_after=timedelta(seconds=55), written_after=timedelta(seconds=50))
    assert engine._store_calculation_results.call_count == 1

    # 03:00 and the next morning: new writes into the closed minute's rows.
    assert _run(engine, now_after=timedelta(hours=11), written_after=timedelta(hours=11))
    assert _run(engine, now_after=timedelta(hours=17), written_after=timedelta(hours=17))
    assert engine._store_calculation_results.call_count == 1
    # The database is asked once per bucket, not on every later write.
    engine._gex_summary_row_exists.assert_called_once_with(CLOSE)


def test_a_restart_with_nothing_new_written_keeps_the_published_row():
    """2026-10-01: both services restarted at 06:32 and the first cycle rewrote
    the previous day's 15:59 row, with no chain write past the settle window."""
    engine = _engine()
    assert _run(
        engine, now_after=timedelta(hours=14, minutes=33), written_after=timedelta(seconds=59)
    )
    engine._store_calculation_results.assert_not_called()
    assert engine._last_processed_snapshot_ts == CLOSE


def test_a_settled_bucket_with_no_stored_row_is_still_published():
    engine = _engine(row_exists=False)
    assert _run(engine, now_after=timedelta(hours=14), written_after=timedelta(seconds=59))
    assert engine._store_calculation_results.call_count == 1


def test_a_live_minute_keeps_its_sub_minute_refinements():
    engine = _engine()
    assert _run(engine, now_after=timedelta(seconds=25), written_after=timedelta(seconds=20))
    # The cycle that runs just after the minute ends still refines it.
    assert _run(engine, now_after=timedelta(seconds=95), written_after=timedelta(seconds=90))
    assert engine._store_calculation_results.call_count == 2
    engine._gex_summary_row_exists.assert_not_called()


def test_settled_is_measured_by_the_clock_with_a_strict_boundary():
    engine = AnalyticsEngine(underlying="SPX")
    assert engine._bucket_settle_seconds == 120
    engine._utcnow = lambda: CLOSE + timedelta(seconds=120)
    assert engine._bucket_is_settled(CLOSE) is False
    engine._utcnow = lambda: CLOSE + timedelta(seconds=121)
    assert engine._bucket_is_settled(CLOSE) is True
    # A naive bucket is read as UTC, as the rest of the engine reads one.
    assert engine._bucket_is_settled(CLOSE.replace(tzinfo=None)) is True
    assert engine._bucket_is_settled(None) is False


def test_the_settled_log_line_names_the_symbol(caplog):
    engine = _engine()
    with caplog.at_level(logging.INFO):
        _run(engine, now_after=timedelta(hours=14), written_after=timedelta(seconds=59))
    assert any("[SPX]" in r.getMessage() and "is settled" in r.getMessage() for r in caplog.records)


def test_the_lookup_asks_for_this_underlying_and_minute():
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    conn = MagicMock()
    conn.cursor.return_value = cursor

    @contextmanager
    def fake_connection():
        yield conn

    engine = AnalyticsEngine(underlying="SPX")
    with patch("src.analytics.main_engine.db_connection", fake_connection):
        assert _REAL_ROW_EXISTS(engine, CLOSE) is True
    sql, params = cursor.execute.call_args.args
    assert "FROM gex_summary" in sql
    assert params == (engine.db_symbol, CLOSE)


def test_a_failed_lookup_recomputes_rather_than_blocking_the_publish():
    @contextmanager
    def broken_connection():
        raise RuntimeError("pool exhausted")
        yield  # pragma: no cover

    engine = AnalyticsEngine(underlying="SPX")
    with patch("src.analytics.main_engine.db_connection", broken_connection):
        assert _REAL_ROW_EXISTS(engine, CLOSE) is False
