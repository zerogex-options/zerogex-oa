from src.signals.unified_signal_engine import UnifiedSignalEngine


def test_iv_rank_disabled_by_default(monkeypatch):
    monkeypatch.delenv("SIGNAL_IV_RANK_ENABLED", raising=False)
    engine = UnifiedSignalEngine("SPY")
    assert engine._iv_rank_enabled is False


def test_iv_rank_can_be_enabled_via_env(monkeypatch):
    monkeypatch.setenv("SIGNAL_IV_RANK_ENABLED", "true")
    engine = UnifiedSignalEngine("SPY")
    assert engine._iv_rank_enabled is True


def test_unified_engine_includes_market_state_components():
    engine = UnifiedSignalEngine("SPY")
    names = {c.name for c in engine.scoring_engine.components}
    # Phase 3.1 added two leading-indicator components to the MSI pipeline
    # and the existing six were rebalanced down to keep the composite total
    # at 100.  See ScoringEngine.COMPONENT_POINTS for the authoritative
    # weight table.
    assert names == {
        "net_gex_sign",
        "flip_distance",
        "local_gamma",
        "put_call_ratio",
        "price_vs_max_gamma",
        "volatility_regime",
        "order_flow_imbalance",
        "dealer_delta_pressure",
    }


def test_msi_component_weights_sum_to_one_hundred():
    """Phase 3.1 invariant: composite_score must remain bounded by [0, 100]."""
    from src.signals.scoring_engine import ScoringEngine

    total = sum(ScoringEngine.COMPONENT_POINTS.values())
    assert total == 100.0, f"COMPONENT_POINTS must total 100 pts, got {total}"
