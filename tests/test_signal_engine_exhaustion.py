from datetime import datetime, timezone

from src.analytics.signal_engine import SignalContext, _compute_zes, _score_components


def _ctx(**overrides):
    base = SignalContext(
        timestamp=datetime(2026, 4, 1, 14, 30, tzinfo=timezone.utc),
        current_price=500.0,
        max_gamma_strike=500.5,
        net_gex=-1_200_000.0,
        gamma_flip=498.5,
        put_call_ratio=0.9,
        vwap=499.0,
        vwap_deviation_pct=0.2,
        price_change_5min=1.0,
        recent_closes=[
            490.0, 491.0, 492.0, 493.0, 494.0,
            495.0, 496.0, 497.0, 498.0, 499.0,
            499.6, 500.1, 500.4, 500.3, 499.7,
            499.4, 499.1, 498.9, 498.7, 498.6,
        ],
        recent_highs=[
            490.4, 491.4, 492.4, 493.4, 494.4,
            495.4, 496.4, 497.4, 498.4, 499.4,
            500.0, 500.5, 500.8, 500.6, 499.9,
            499.6, 499.3, 499.1, 498.8, 498.7,
        ],
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def test_compute_zes_returns_exhaustion_zone_when_reversal_risk_builds():
    zes, state, trap = _compute_zes(_ctx())
    assert zes >= 40
    assert state in {"Late Trend", "Exhaustion Zone", "Trap Triggered"}
    assert isinstance(trap, bool)


def test_score_components_includes_exhaustion_component():
    _, components = _score_components(_ctx(), "intraday")
    exhaustion = [c for c in components if c.name == "ZeroGEX Exhaustion Score"]
    assert len(exhaustion) == 1
    assert 0 <= float(exhaustion[0].value) <= 100
