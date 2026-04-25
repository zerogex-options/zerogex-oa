import math

import pytest

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
    assert (
        classify_regime({"gex_regime": {"score": -0.9, "regime": "short_gamma"}}) == "short_gamma"
    )
    assert (
        classify_regime({"gex_regime": {"score": -0.1, "regime": "neutral_gamma"}})
        == "neutral_gamma"
    )


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


# ---------------------------------------------------------------------------
# Edge cases: classify_regime with malformed / missing / NaN inputs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "components",
    [
        None,
        {},
        {"gex_regime": None},
        {"gex_regime": "not-a-dict"},
        {"gex_regime": {}},  # no known keys
        {"gex_regime": {"score": "not-a-float"}},
        {"gex_regime": {"score": None}},
        {"gex_regime": {"value": "N/A"}},
        {"gex_regime": {"regime": ""}},  # empty regime string
        {"other_component": {"score": 0.5}},  # wrong top-level key
    ],
    ids=[
        "none",
        "empty_dict",
        "regime_value_none",
        "regime_value_string",
        "regime_dict_empty",
        "score_unparseable",
        "score_none",
        "value_na",
        "regime_empty_string",
        "missing_gex_regime",
    ],
)
def test_classify_regime_unknown_on_malformed_input(components):
    # Every degenerate input should be rejected cleanly with "unknown",
    # not raised or silently misclassified.
    assert classify_regime(components) == "unknown"


def test_classify_regime_nan_score_returns_neutral():
    # NaN is a float, float(math.nan) succeeds, and NaN comparisons are
    # always False.  That cascades to the final "neutral_gamma" branch
    # and must not blow up.
    result = classify_regime({"gex_regime": {"score": math.nan}})
    assert result == "neutral_gamma"


def test_classify_regime_infinity_score_classified_by_sign():
    assert classify_regime({"gex_regime": {"score": math.inf}}) == "short_gamma"
    assert classify_regime({"gex_regime": {"score": -math.inf}}) == "long_gamma"


# ---------------------------------------------------------------------------
# Edge cases: calibrate_signal with degenerate history
# ---------------------------------------------------------------------------


def test_calibrate_signal_skips_history_rows_with_bad_fields():
    # Rows with missing/non-numeric composite_score or fwd_return must
    # be silently skipped, not crash the calibration.
    history = [
        {"composite_score": "bad", "fwd_return": 0.001, "regime": "long_gamma"},
        {"composite_score": 0.7, "fwd_return": None, "regime": "long_gamma"},
        {},  # missing every key
        {"composite_score": 0.7, "fwd_return": 0.001},  # missing regime (tolerated)
        {"composite_score": 0.7, "fwd_return": 0.001, "regime": "long_gamma"},
    ]
    metrics = calibrate_signal(
        current_composite=0.7,
        current_normalized=0.7,
        current_regime="long_gamma",
        history_rows=history,
    )
    # At most 2 rows survive; sample size is too small for 'enter' but
    # the call itself must not raise.
    assert metrics["action"] in {"wait", "watch", "enter"}
    assert isinstance(metrics["sample_size"], int)


def test_calibrate_signal_nan_composite_treated_as_neutral():
    # NaN has no sign → direction=0 → early-return "wait".
    metrics = calibrate_signal(
        current_composite=math.nan,
        current_normalized=0.0,
        current_regime="long_gamma",
        history_rows=[
            {"composite_score": 0.7, "fwd_return": 0.001, "regime": "long_gamma"} for _ in range(50)
        ],
    )
    assert metrics["action"] == "wait"
    assert metrics["hit_rate"] is None


def test_calibrate_signal_empty_history_with_strong_signal():
    # A strong signal with no history should wait (insufficient_history),
    # not fabricate an estimate.
    metrics = calibrate_signal(
        current_composite=0.9,
        current_normalized=0.9,
        current_regime="long_gamma",
        history_rows=[],
    )
    assert metrics["action"] == "wait"
    assert metrics["calibration_scope"] == "insufficient_history"
    assert metrics["hit_rate"] is None


def test_calibrate_signal_zero_sample_after_filtering():
    # Historical rows exist, but none match the direction (all opposite-signed).
    history = [
        {"composite_score": -0.7, "fwd_return": 0.001, "regime": "long_gamma"} for _ in range(100)
    ]
    metrics = calibrate_signal(
        current_composite=0.7,  # positive direction
        current_normalized=0.7,
        current_regime="long_gamma",
        history_rows=history,
    )
    # All 100 rows are direction=-1; none match current direction=+1.
    assert metrics["sample_size"] == 0
    assert metrics["action"] == "wait"
    assert metrics["calibration_scope"] == "insufficient_history"
