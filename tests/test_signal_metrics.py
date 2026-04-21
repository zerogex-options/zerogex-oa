from src.api.signal_metrics import calibrate_signal, classify_regime


def test_classify_regime_uses_gex_sign():
    # Legacy "value" key held raw net_gex (positive => long_gamma).
    assert classify_regime({"gex_regime": {"value": -10}}) == "short_gamma"
    assert classify_regime({"gex_regime": {"value": 10}}) == "long_gamma"
    assert classify_regime({"gex_regime": {"value": 0}}) == "neutral_gamma"
    # Scoring engine's "score" is -tanh(net_gex / norm), so its sign is the
    # OPPOSITE of net_gex. Positive score => net_gex negative => short_gamma.
    assert classify_regime({"gex_regime": {"weight": 0.15, "score": 0.6}}) == "short_gamma"
    assert classify_regime({"gex_regime": {"weight": 0.15, "score": -0.4}}) == "long_gamma"
    assert classify_regime({"gex_regime": {"weight": 0.15, "score": 0.0}}) == "neutral_gamma"


def test_classify_regime_prefers_explicit_regime_field():
    assert classify_regime({"gex_regime": {"score": 0.9, "regime": "long_gamma"}}) == "long_gamma"
    assert classify_regime({"gex_regime": {"score": -0.9, "regime": "short_gamma"}}) == "short_gamma"
    assert classify_regime({"gex_regime": {"score": -0.1, "regime": "neutral_gamma"}}) == "neutral_gamma"


def test_classify_regime_matches_positive_net_gex_as_long_gamma():
    # Regression: Net GEX = +$1.0B should classify as long_gamma, not short_gamma.
    # With GEX_NORM = 2.5e8, score = -tanh(1e9 / 2.5e8) ~= -0.9993.
    import math
    score = -math.tanh(1.0e9 / 2.5e8)
    assert classify_regime({"gex_regime": {"score": score}}) == "long_gamma"


def test_calibrate_signal_returns_enter_for_good_edge():
    history = []
    for _ in range(80):
        history.append({"composite_score": 0.72, "regime": "long_gamma", "fwd_return": 0.0020})
    for _ in range(20):
        history.append({"composite_score": 0.70, "regime": "long_gamma", "fwd_return": -0.0010})

    metrics = calibrate_signal(
        current_composite=0.74,
        current_normalized=0.74,
        current_regime="long_gamma",
        history_rows=history,
    )

    assert metrics["action"] == "enter"
    assert metrics["hit_rate"] is not None and metrics["hit_rate"] >= 0.75
    assert metrics["expected_move_bp"] is not None and metrics["expected_move_bp"] > 0
    assert metrics["sample_size"] >= 40


def test_calibrate_signal_handles_neutral_signal():
    metrics = calibrate_signal(
        current_composite=0.0,
        current_normalized=0.0,
        current_regime="unknown",
        history_rows=[],
    )
    assert metrics["action"] == "wait"
    assert metrics["hit_rate"] is None
