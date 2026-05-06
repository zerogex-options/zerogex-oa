"""Tests for the component_normalizer_cache refresh tool."""

from __future__ import annotations

import math

import pytest

from src.tools.normalizer_cache_refresh import (
    FIELD_SPECS,
    MIN_SAMPLES,
    Distribution,
    _summarize,
)


def test_summarize_returns_none_below_min_samples():
    assert _summarize([1.0, -2.0, 3.0]) is None
    assert _summarize([float(i) for i in range(MIN_SAMPLES - 1)]) is None


def test_summarize_uses_abs_for_percentiles_signed_for_std():
    # 100 evenly-spaced signed samples -50..49.  |x| percentiles look only
    # at magnitude; std should reflect the signed dispersion.
    samples = [float(i) for i in range(-50, 50)]
    dist = _summarize(samples)
    assert dist is not None
    # 95th percentile of |x| over [-50..49] is around 47-48.
    assert 45 <= dist.p95 <= 50
    # Median magnitude of evenly-spaced symmetric data is ~25.
    assert 20 <= dist.p50 <= 30
    # Signed std ≈ 29 for uniform [-50, 49].
    assert 25 <= dist.std <= 35
    assert dist.sample_size == 100


def test_summarize_skips_nan_and_none():
    samples = [1.0, 2.0, float("nan"), None, 3.0] + [float(i) for i in range(MIN_SAMPLES)]
    dist = _summarize(samples)
    assert dist is not None
    # NaN + None dropped; finite count = 3 + MIN_SAMPLES.
    assert dist.sample_size == 3 + MIN_SAMPLES


def test_summarize_zero_only_yields_zero_distribution():
    dist = _summarize([0.0] * MIN_SAMPLES)
    assert dist is not None
    assert dist.p05 == 0.0
    assert dist.p50 == 0.0
    assert dist.p95 == 0.0
    assert dist.std == 0.0


def test_field_specs_cover_all_normalizer_consumers():
    """Every name read from ``ctx.extra['normalizers']`` in the signal code
    should have a matching FieldSpec — otherwise the populator silently
    leaves that field unscaled."""
    expected = {
        "dealer_vanna_exposure",
        "dealer_charm_exposure",
        "local_gex",
        "net_gex_delta",
        "call_flow_delta",
        "put_flow_delta",
    }
    actual = {spec.name for spec in FIELD_SPECS}
    assert expected == actual, f"missing: {expected - actual}, extra: {actual - expected}"


def test_field_specs_have_two_query_placeholders():
    """Each spec's SQL must accept (symbol, window_days) as parameters."""
    for spec in FIELD_SPECS:
        # Two placeholders the runner will bind: symbol + window_days.
        assert spec.query.count("%s") == 2, (
            f"{spec.name}: expected 2 %s placeholders, "
            f"got {spec.query.count('%s')}"
        )


def test_distribution_is_immutable():
    dist = Distribution(p05=1.0, p50=2.0, p95=3.0, std=0.5, sample_size=10)
    with pytest.raises(Exception):
        dist.p95 = 99.0  # frozen dataclass


def test_summarize_reflects_realistic_spy_magnitudes():
    """SPY-scale dealer_vanna_exposure samples produce p95 in the
    hundreds-of-millions to billions range — the calibration goal that
    the static _VC_NORM default failed to hit."""
    rng_samples = [
        # Mix of positive/negative around 0 with magnitudes ~$200M-$1B.
        ((-1) ** i) * (2.0e8 + (i % 7) * 1.0e8)
        for i in range(200)
    ]
    dist = _summarize(rng_samples)
    assert dist is not None
    assert dist.p95 >= 5e8, f"p95={dist.p95:.2e} too small for SPY-scale samples"
    assert math.isfinite(dist.std) and dist.std > 0
