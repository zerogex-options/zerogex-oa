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
    # Phase 2.1 final state: the three former gamma-cluster components
    # (flip_distance / local_gamma / price_vs_max_gamma) are no longer
    # registered as standalone MSI components — their logic lives inside
    # gamma_anchor which exposes the subscores via its `context` field.
    assert names == {
        "net_gex_sign",
        "gamma_anchor",
        "put_call_ratio",
        "volatility_regime",
        "order_flow_imbalance",
        "dealer_delta_pressure",
    }


def test_msi_component_weights_sum_to_one_hundred():
    """composite_score must remain bounded by [0, 100]."""
    from src.signals.scoring_engine import ScoringEngine

    total = sum(ScoringEngine.COMPONENT_POINTS.values())
    assert total == 100.0, f"COMPONENT_POINTS must total 100 pts, got {total}"


def test_former_gamma_cluster_components_no_longer_registered():
    """Phase 2.1 final state: the three former gamma-cluster components must
    not appear in COMPONENT_POINTS — they were collapsed into gamma_anchor."""
    from src.signals.scoring_engine import ScoringEngine

    for name in ("flip_distance", "local_gamma", "price_vs_max_gamma"):
        assert name not in ScoringEngine.COMPONENT_POINTS, (
            f"{name} must not be registered post-Phase-2.1 (logic lives in gamma_anchor)"
        )
