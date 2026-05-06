"""Tests for the regime_tilt + ensure_non_zero spectrum helpers."""

from __future__ import annotations

from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.components.spectrum import (
    _ABSTAIN_THRESHOLD,
    ensure_non_zero,
    regime_tilt,
)


def _ctx(**overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 5, 6, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=-100_000_000.0,
        gamma_flip=498.0,
        put_call_ratio=1.0,
        max_pain=None,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[],
        iv_rank=None,
    )
    defaults.update(overrides)
    return MarketContext(**defaults)


def test_regime_tilt_returns_value_in_magnitude_band():
    tilt = regime_tilt(_ctx(), magnitude=0.20)
    assert -0.20 <= tilt <= 0.20


def test_regime_tilt_is_strictly_non_zero():
    tilt = regime_tilt(_ctx(), magnitude=0.10)
    assert tilt != 0.0


def test_regime_tilt_responds_to_flip_position():
    above = regime_tilt(_ctx(close=510.0, gamma_flip=500.0), magnitude=0.25)
    below = regime_tilt(_ctx(close=490.0, gamma_flip=500.0), magnitude=0.25)
    # Above flip leans bullish, below flip leans bearish.
    assert above > 0
    assert below < 0


def test_regime_tilt_responds_to_put_call_ratio():
    high_pcr = regime_tilt(_ctx(put_call_ratio=1.4), magnitude=0.25)
    low_pcr = regime_tilt(_ctx(put_call_ratio=0.7), magnitude=0.25)
    assert high_pcr < 0  # bearish
    assert low_pcr > 0  # bullish


def test_regime_tilt_respects_magnitude_clamp():
    # Even with strong cues the tilt is bounded by the requested magnitude.
    tilt = regime_tilt(
        _ctx(close=600.0, gamma_flip=500.0, put_call_ratio=0.5, net_gex=-1.0e10),
        magnitude=0.10,
    )
    assert -0.10 <= tilt <= 0.10


def test_regime_tilt_falls_back_to_dither_when_context_empty():
    ctx = _ctx(
        close=500.0,
        net_gex=0.0,  # zero GEX → tanh contributes 0 but cue still counted
        gamma_flip=None,
        put_call_ratio=0.0,  # filtered out (>0 guard)
    )
    tilt = regime_tilt(ctx, magnitude=0.10)
    # Still non-zero — net_gex=0 → tanh(0) = 0, average = 0, last-resort
    # min kicks in to keep the result strictly non-zero.
    assert tilt != 0.0
    assert -0.10 <= tilt <= 0.10


def test_ensure_non_zero_passes_through_real_scores():
    score = 0.42
    assert ensure_non_zero(score, _ctx()) == 0.42


def test_ensure_non_zero_replaces_abstain_zero():
    replaced = ensure_non_zero(0.0, _ctx(), magnitude=0.15)
    assert replaced != 0.0
    assert -0.15 <= replaced <= 0.15


def test_ensure_non_zero_replaces_sub_threshold_scores():
    half_threshold = _ABSTAIN_THRESHOLD / 2.0
    replaced = ensure_non_zero(half_threshold, _ctx())
    assert abs(replaced) >= _ABSTAIN_THRESHOLD or replaced != half_threshold
