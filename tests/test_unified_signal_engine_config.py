from src.signals.unified_signal_engine import UnifiedSignalEngine


def test_iv_rank_disabled_by_default(monkeypatch):
    monkeypatch.delenv("SIGNAL_IV_RANK_ENABLED", raising=False)
    engine = UnifiedSignalEngine("SPY")
    assert engine._iv_rank_enabled is False


def test_iv_rank_can_be_enabled_via_env(monkeypatch):
    monkeypatch.setenv("SIGNAL_IV_RANK_ENABLED", "true")
    engine = UnifiedSignalEngine("SPY")
    assert engine._iv_rank_enabled is True


def test_unified_engine_includes_intraday_regime_component():
    engine = UnifiedSignalEngine("SPY")
    names = {c.name for c in engine.scoring_engine.components}
    assert names == {
        "net_gex_sign",
        "flip_distance",
        "local_gamma",
        "put_call_ratio_state",
        "price_vs_max_gamma",
        "volatility_regime",
    }
