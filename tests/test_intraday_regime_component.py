"""Tests for intraday_regime component."""
from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.basic.intraday_regime import IntradayRegimeComponent


def _ctx(hour=15, minute=0, **overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 14, hour, minute, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=0.0,
        gamma_flip=500.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[500.0] * 5,
        iv_rank=None,
    )
    defaults.update(overrides)
    return MarketContext(**defaults)


comp = IntradayRegimeComponent()


def test_pre_open_is_zero():
    assert comp.compute(_ctx(hour=12, minute=0)) == 0.0


def test_post_close_is_zero():
    assert comp.compute(_ctx(hour=21, minute=0)) == 0.0


def test_phase_opening_range():
    assert comp._phase(_ctx(hour=13, minute=45).timestamp) == "opening_range"


def test_phase_mid_session():
    assert comp._phase(_ctx(hour=16, minute=0).timestamp) == "mid_session"


def test_phase_power_hour():
    assert comp._phase(_ctx(hour=19, minute=0).timestamp) == "power_hour"


def test_phase_closing_pin():
    assert comp._phase(_ctx(hour=19, minute=50).timestamp) == "closing_pin"


def test_opening_range_uses_dampened_momentum():
    base = 500.0
    closes = [base] * 4 + [base * 1.006]  # big up move
    ctx = _ctx(hour=13, minute=45, recent_closes=closes)
    score = comp.compute(ctx)
    assert 0.0 < score < 1.0


def test_mid_session_trend_persistence():
    base = 500.0
    closes = [base] * 4 + [base * 1.006]
    ctx = _ctx(hour=16, minute=0, recent_closes=closes)
    score = comp.compute(ctx)
    assert score > 0.5


def test_power_hour_fades_stretched_move():
    """In power hour, price above VWAP should produce a negative (fade) bias."""
    ctx = _ctx(hour=19, minute=0, close=503.0, vwap=500.0)
    score = comp.compute(ctx)
    assert score < 0


def test_closing_pin_pulls_toward_max_gamma():
    ctx = _ctx(hour=19, minute=55, close=499.0)
    ctx.extra["max_gamma_strike"] = 502.0
    score = comp.compute(ctx)
    assert score > 0  # pulled up toward pin


def test_score_bounded():
    for h in range(13, 21):
        s = comp.compute(_ctx(hour=h, minute=30))
        assert -1.0 <= s <= 1.0


def test_context_values_has_phase():
    cv = comp.context_values(_ctx(hour=15, minute=0))
    assert cv["phase"] == "mid_session"


def test_context_values_exposes_market_state_index():
    ctx = _ctx(
        hour=16,
        minute=0,
        net_gex=-200_000_000.0,
        put_call_ratio=1.2,
    )
    ctx.extra["flip_distance"] = 0.002
    ctx.extra["local_gex"] = 10_000_000.0
    ctx.extra["max_gamma_strike"] = 506.0
    ctx.extra["normalizers"] = {"local_gex": 200_000_000.0}
    cv = comp.context_values(ctx)
    assert 0.0 <= cv["market_state_index"] <= 100.0
    assert cv["intraday_score"] == cv["market_state_index"]


def test_market_state_index_higher_in_volatile_setup_than_stable_setup():
    volatile = _ctx(
        hour=16,
        minute=0,
        close=505.0,
        net_gex=-300_000_000.0,
        put_call_ratio=1.25,
        recent_closes=[500.0, 501.5, 503.0, 504.0, 505.0],
    )
    volatile.extra.update(
        {
            "flip_distance": 0.001,  # near flip => higher risk
            "local_gex": 5_000_000.0,
            "max_gamma_strike": 495.0,  # far from spot
            "normalizers": {"local_gex": 200_000_000.0},
            "vix_level": 28.0,
        }
    )
    stable = _ctx(
        hour=16,
        minute=0,
        close=500.2,
        net_gex=300_000_000.0,
        put_call_ratio=1.0,
        recent_closes=[500.0, 500.05, 500.1, 500.15, 500.2],
    )
    stable.extra.update(
        {
            "flip_distance": 0.03,  # far from flip
            "local_gex": 250_000_000.0,
            "max_gamma_strike": 500.0,  # near pin
            "normalizers": {"local_gex": 200_000_000.0},
            "vix_level": 13.0,
        }
    )

    volatile_idx = comp.context_values(volatile)["market_state_index"]
    stable_idx = comp.context_values(stable)["market_state_index"]
    assert volatile_idx > stable_idx
