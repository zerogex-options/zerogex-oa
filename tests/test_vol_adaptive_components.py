"""Tests for Phase 2.2: vol-adaptive flip_distance and price_vs_max_gamma."""

from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.components.flip_distance import FlipDistanceComponent
from src.signals.components.price_vs_max_gamma import PriceVsMaxGammaComponent


def _ctx(
    *,
    close: float,
    gamma_flip: float = None,
    max_gamma_strike: float = None,
    recent_closes: list[float] = None,
) -> MarketContext:
    extra = {}
    if max_gamma_strike is not None:
        extra["max_gamma_strike"] = max_gamma_strike
    return MarketContext(
        timestamp=datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=close,
        net_gex=0.0,
        gamma_flip=gamma_flip,
        put_call_ratio=1.0,
        max_pain=None,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=recent_closes or [],
        iv_rank=None,
        extra=extra,
    )


def _quiet_closes(base: float = 500.0, n: int = 30, drift_pct: float = 0.001) -> list[float]:
    """Generate a synthetic 1-min close series with realized sigma ~ drift_pct/bar."""
    return [base * (1 + drift_pct * (i % 3 - 1)) for i in range(n)]


def _volatile_closes(base: float = 500.0, n: int = 30, drift_pct: float = 0.005) -> list[float]:
    return [base * (1 + drift_pct * ((-1) ** i)) for i in range(n)]


# ---------------------------------------------------------------------------
# flip_distance
# ---------------------------------------------------------------------------


class TestFlipDistanceVolAdaptive:
    def test_falls_back_to_fixed_pct_when_closes_sparse(self):
        cmp = FlipDistanceComponent()
        # Only 3 bars — well under SIGNAL_VOL_MIN_BARS (10).
        ctx = _ctx(close=500.0, gamma_flip=495.0, recent_closes=[499, 500, 501])
        payload = cmp.context_values(ctx)
        assert payload["saturation_source"] == "fallback_sparse"
        # Fixed 2% saturation.
        assert payload["saturation_pct"] == 0.02

    def test_uses_vol_adaptive_when_closes_sufficient(self):
        cmp = FlipDistanceComponent()
        ctx = _ctx(close=500.0, gamma_flip=495.0, recent_closes=_quiet_closes())
        payload = cmp.context_values(ctx)
        assert payload["saturation_source"] == "vol_adaptive"

    def test_volatile_regime_widens_saturation(self):
        cmp = FlipDistanceComponent()
        quiet = cmp.context_values(
            _ctx(close=500.0, gamma_flip=495.0, recent_closes=_quiet_closes())
        )
        loud = cmp.context_values(
            _ctx(close=500.0, gamma_flip=495.0, recent_closes=_volatile_closes())
        )
        # Both should be vol-adaptive but volatile regime saturates farther
        # from the flip strike (i.e. larger saturation_pct).
        assert loud["saturation_pct"] > quiet["saturation_pct"]

    def test_saturation_bounded_by_max_pct(self):
        cmp = FlipDistanceComponent()
        # Pathologically volatile: 5% per-bar drift would imply ~5% σ.
        ctx = _ctx(
            close=500.0,
            gamma_flip=495.0,
            recent_closes=_volatile_closes(drift_pct=0.05),
        )
        payload = cmp.context_values(ctx)
        # SIGNAL_FLIP_DISTANCE_MAX_PCT default is 0.05 (5%).
        assert payload["saturation_pct"] <= 0.05

    def test_saturation_bounded_by_min_pct(self):
        cmp = FlipDistanceComponent()
        # Effectively flat: zero σ.
        flat = [500.0] * 30
        ctx = _ctx(close=500.0, gamma_flip=495.0, recent_closes=flat)
        payload = cmp.context_values(ctx)
        # σ=0 forces fallback path, not the floor — the source label exposes that.
        assert payload["saturation_source"] == "fallback_zero_sigma"
        assert payload["saturation_pct"] == 0.02


# ---------------------------------------------------------------------------
# price_vs_max_gamma
# ---------------------------------------------------------------------------


class TestPriceVsMaxGammaVolAdaptive:
    def test_falls_back_to_fixed_pct_when_closes_sparse(self):
        cmp = PriceVsMaxGammaComponent()
        ctx = _ctx(close=500.0, max_gamma_strike=499.0, recent_closes=[499, 500, 501])
        payload = cmp.context_values(ctx)
        assert payload["saturation_source"] == "fallback_sparse"
        assert payload["saturation_pct"] == 0.01  # original 1% fallback

    def test_uses_vol_adaptive_when_closes_sufficient(self):
        cmp = PriceVsMaxGammaComponent()
        ctx = _ctx(close=500.0, max_gamma_strike=499.0, recent_closes=_quiet_closes())
        payload = cmp.context_values(ctx)
        assert payload["saturation_source"] == "vol_adaptive"

    def test_volatile_regime_widens_saturation(self):
        cmp = PriceVsMaxGammaComponent()
        quiet = cmp.context_values(
            _ctx(close=500.0, max_gamma_strike=499.0, recent_closes=_quiet_closes())
        )
        loud = cmp.context_values(
            _ctx(close=500.0, max_gamma_strike=499.0, recent_closes=_volatile_closes())
        )
        assert loud["saturation_pct"] > quiet["saturation_pct"]

    def test_score_clamped_to_unit_interval(self):
        cmp = PriceVsMaxGammaComponent()
        # Price 10% from max gamma strike (well past saturation under any vol).
        ctx = _ctx(close=550.0, max_gamma_strike=500.0, recent_closes=_quiet_closes())
        score = cmp.compute(ctx)
        assert score == 1.0  # max "free" score
