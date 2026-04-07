from datetime import datetime, timezone

from src.analytics.main_engine import AnalyticsEngine


def test_refresh_flow_caches_noops_when_disabled():
    engine = AnalyticsEngine.__new__(AnalyticsEngine)
    engine._analytics_flow_cache_refresh_enabled = False
    engine._last_flow_cache_ts = None
    engine._last_flow_cache_refresh_mono = 0.0
    engine._flow_cache_refresh_min_seconds = 0.0
    engine.db_symbol = "SPY"

    # Should return immediately without requiring DB access.
    engine._refresh_flow_caches(datetime.now(timezone.utc), 500.0)
