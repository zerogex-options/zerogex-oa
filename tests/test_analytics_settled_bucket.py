"""A closed minute is final: after-hours writes must not rewrite it.

After the close the snapshot stays on the session's last bucket while
ingestion keeps writing into rows that still carry that bucket's timestamps.
Each write moved the write clock, so the sub-minute guard recomputed the closed
minute from after-hours data and overwrote the row the session had published
(14 of 15 production sessions from 2026-09-08). A bucket whose rows were
written long after its minute, and whose row is already stored, is now kept.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.analytics.main_engine import AnalyticsEngine
from tests.test_analytics_empty_snapshot_quiet import _stub_pipeline

CLOSE = datetime(2026, 9, 28, 19, 59, tzinfo=timezone.utc)  # 15:59 ET


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


def _run(engine: AnalyticsEngine, written_after: timedelta) -> bool:
    engine._get_snapshot = MagicMock(return_value=_snapshot(written_after))
    return engine.run_calculation()


def test_after_hours_writes_do_not_rewrite_the_published_closing_row():
    engine = _engine()
    assert _run(engine, timedelta(seconds=50)) is True
    assert engine._store_calculation_results.call_count == 1

    # 03:00 and the next morning: rows rewritten long after the minute.
    assert _run(engine, timedelta(hours=11)) is True
    assert _run(engine, timedelta(hours=17)) is True
    assert engine._store_calculation_results.call_count == 1
    # The database is asked once per bucket, not on every later write.
    engine._gex_summary_row_exists.assert_called_once_with(CLOSE)


def test_a_restarted_engine_keeps_the_published_row():
    engine = _engine()
    assert _run(engine, timedelta(hours=17)) is True
    engine._store_calculation_results.assert_not_called()
    assert engine._last_processed_snapshot_ts == CLOSE


def test_a_settled_bucket_with_no_stored_row_is_still_published():
    engine = _engine(row_exists=False)
    assert _run(engine, timedelta(hours=17)) is True
    assert engine._store_calculation_results.call_count == 1


def test_a_live_minute_keeps_its_sub_minute_refinements():
    engine = _engine()
    assert _run(engine, timedelta(seconds=20)) is True
    # A quote for the minute that lands just after it ends is still its own.
    assert _run(engine, timedelta(seconds=90)) is True
    assert engine._store_calculation_results.call_count == 2
    engine._gex_summary_row_exists.assert_not_called()


def test_settled_is_measured_from_the_write_clock_with_a_strict_boundary():
    engine = AnalyticsEngine(underlying="SPX")
    assert engine._bucket_settle_seconds == 120
    assert engine._bucket_is_settled(CLOSE, None) is False
    assert engine._bucket_is_settled(CLOSE, CLOSE + timedelta(seconds=120)) is False
    assert engine._bucket_is_settled(CLOSE, CLOSE + timedelta(seconds=121)) is True
    naive = CLOSE.replace(tzinfo=None) + timedelta(hours=17)
    assert engine._bucket_is_settled(CLOSE, naive) is False


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
        assert engine._gex_summary_row_exists(CLOSE) is True
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
        assert engine._gex_summary_row_exists(CLOSE) is False
