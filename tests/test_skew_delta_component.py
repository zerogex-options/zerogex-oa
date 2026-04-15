"""Tests for skew_delta component."""
from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.components.skew_delta import (
    SkewDeltaComponent,
    _SKEW_BASELINE,
    _SKEW_SATURATION,
)


def _ctx(**overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 14, 14, 0, tzinfo=timezone.utc),
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


comp = SkewDeltaComponent()


def test_no_data_is_neutral():
    assert comp.compute(_ctx()) == 0.0


def test_baseline_spread_is_neutral():
    ctx = _ctx()
    ctx.extra["skew"] = {
        "otm_put_iv": 0.2 + _SKEW_BASELINE,
        "otm_call_iv": 0.2,
    }
    assert abs(comp.compute(ctx)) < 1e-9


def test_elevated_put_skew_is_bearish():
    ctx = _ctx()
    ctx.extra["skew"] = {
        "otm_put_iv": 0.2 + _SKEW_BASELINE + _SKEW_SATURATION,
        "otm_call_iv": 0.2,
    }
    assert comp.compute(ctx) <= -1.0 + 1e-9


def test_compressed_skew_is_bullish():
    ctx = _ctx()
    ctx.extra["skew"] = {
        "otm_put_iv": 0.2,
        "otm_call_iv": 0.2 + _SKEW_BASELINE,
    }
    # put-call = -baseline ... deviation = -2*baseline ...
    assert comp.compute(ctx) > 0


def test_partial_data_is_neutral():
    ctx = _ctx()
    ctx.extra["skew"] = {"otm_put_iv": 0.2}  # missing call
    assert comp.compute(ctx) == 0.0


def test_context_values_populated():
    ctx = _ctx()
    ctx.extra["skew"] = {"otm_put_iv": 0.22, "otm_call_iv": 0.18}
    cv = comp.context_values(ctx)
    assert cv["otm_put_iv"] == 0.22
    assert cv["otm_call_iv"] == 0.18
    assert cv["spread"] is not None
