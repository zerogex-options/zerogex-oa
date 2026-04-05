from src.api.signal_metrics import calibrate_signal, classify_regime


def test_classify_regime_uses_gex_sign():
    assert classify_regime({"gex_regime": {"value": -10}}) == "short_gamma"
    assert classify_regime({"gex_regime": {"value": 10}}) == "long_gamma"
    assert classify_regime({"gex_regime": {"value": 0}}) == "neutral_gamma"


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
