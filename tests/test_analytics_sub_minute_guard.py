"""The recompute-skip guard keyed on the chain's write clock.

The bucket timestamp moves once a minute. Keyed on it alone, an analytics
interval shorter than a minute skipped every other cycle as "unchanged", so a
30s cadence was a 60s cadence with extra snapshot queries. The chain's latest
table is rewritten in place every few seconds, and its write clock is now part
of the key: same bucket with fresher rows recomputes, same bucket with the same
rows does not.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from src.analytics.main_engine import AnalyticsEngine
from tests.test_analytics_empty_snapshot_quiet import _stub_pipeline

TS = datetime(2026, 9, 8, 14, 12, tzinfo=timezone.utc)


def _snapshot(updated: datetime) -> dict:
    return {
        "timestamp": TS,
        "underlying_price": 500.0,
        "options": [{"strike": 500.0}],
        "spot_anchored": False,
        "data_updated_at": updated,
    }


def test_same_bucket_recomputes_only_when_the_rows_were_rewritten():
    engine = AnalyticsEngine(underlying="SPY")
    _stub_pipeline(engine)
    first_write = TS + timedelta(seconds=20)
    later_write = TS + timedelta(seconds=50)

    engine._get_snapshot = MagicMock(return_value=_snapshot(first_write))
    assert engine.run_calculation() is True
    assert engine._store_calculation_results.call_count == 1

    # Identical input: skipped, as before.
    assert engine.run_calculation() is True
    assert engine._store_calculation_results.call_count == 1

    # Same minute, rows rewritten since: recomputed.
    engine._get_snapshot = MagicMock(return_value=_snapshot(later_write))
    assert engine.run_calculation() is True
    assert engine._store_calculation_results.call_count == 2


def test_a_snapshot_without_the_write_clock_behaves_as_before():
    """Fakes and older snapshot shapes carry no data_updated_at; the guard
    then falls back to the timestamp alone."""
    engine = AnalyticsEngine(underlying="SPY")
    _stub_pipeline(engine)
    bare = {"timestamp": TS, "underlying_price": 500.0, "options": [{"strike": 500.0}]}
    engine._get_snapshot = MagicMock(return_value=bare)
    assert engine.run_calculation() is True
    assert engine.run_calculation() is True
    assert engine._store_calculation_results.call_count == 1
