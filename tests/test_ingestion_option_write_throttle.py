from datetime import datetime

from src.ingestion.main_engine import IngestionEngine


def _build_engine_stub() -> IngestionEngine:
    engine = IngestionEngine.__new__(IngestionEngine)
    engine._option_bucket_last_write = {}
    return engine


def test_option_bucket_write_throttle_blocks_immediate_repeat():
    engine = _build_engine_stub()
    bucket = datetime(2026, 1, 1, 9, 30)

    first = engine._should_write_option_bucket("SPY250101C00500000", bucket)
    second = engine._should_write_option_bucket("SPY250101C00500000", bucket)

    assert first is True
    assert second is False


def test_option_bucket_write_throttle_force_overrides_rate_limit():
    engine = _build_engine_stub()
    bucket = datetime(2026, 1, 1, 9, 30)

    assert engine._should_write_option_bucket("SPY250101C00500000", bucket) is True
    assert engine._should_write_option_bucket("SPY250101C00500000", bucket) is False
    assert engine._should_write_option_bucket("SPY250101C00500000", bucket, force=True) is True
