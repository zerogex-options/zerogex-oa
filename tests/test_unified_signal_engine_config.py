from src.signals.unified_signal_engine import UnifiedSignalEngine


def test_iv_rank_disabled_by_default(monkeypatch):
    monkeypatch.delenv("SIGNAL_IV_RANK_ENABLED", raising=False)
    engine = UnifiedSignalEngine("SPY")
    assert engine._iv_rank_enabled is False


def test_iv_rank_can_be_enabled_via_env(monkeypatch):
    monkeypatch.setenv("SIGNAL_IV_RANK_ENABLED", "true")
    engine = UnifiedSignalEngine("SPY")
    assert engine._iv_rank_enabled is True
