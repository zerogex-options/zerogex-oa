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
    # Phase 2.1 added the unified ``gamma_anchor`` component on top of the
    # post-Phase-3.1 set, and kept flip_distance / local_gamma /
    # price_vs_max_gamma as zero-weight stubs for API back-compat.
    assert names == {
        "net_gex_sign",
        "gamma_anchor",
        "flip_distance",
        "local_gamma",
        "price_vs_max_gamma",
        "put_call_ratio",
        "volatility_regime",
        "order_flow_imbalance",
        "dealer_delta_pressure",
    }


def test_msi_component_weights_sum_to_one_hundred():
    """composite_score must remain bounded by [0, 100].  Deprecated stubs
    sit at weight 0 so they don't contribute to the active total."""
    from src.signals.scoring_engine import ScoringEngine

    total = sum(ScoringEngine.COMPONENT_POINTS.values())
    assert total == 100.0, f"COMPONENT_POINTS must total 100 pts, got {total}"


def test_deprecated_stubs_have_zero_weight():
    """Phase 2.1 invariant: the three former gamma-cluster components are
    registered for API visibility but contribute nothing to the composite."""
    from src.signals.scoring_engine import ScoringEngine

    for name in ("flip_distance", "local_gamma", "price_vs_max_gamma"):
        assert ScoringEngine.COMPONENT_POINTS.get(name) == 0.0, (
            f"{name} must be a zero-weight stub post-Phase-2.1"
        )
