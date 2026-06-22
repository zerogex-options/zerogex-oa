"""Tests for the _historical_context_for helper that backs
``/api/gex/historical-context``.

The endpoint joins the live ``gex_summary`` value against a pre-aggregated
``gex_historical_stats`` row, then derives a percentile, z-score, and
regime label from the stored quantiles + mean/std.  These tests pin the
regime-bucket boundaries (z-score based) and the record-high override so
the badge color on the live MetricCards has a stable contract.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.api.database import _historical_context_for, _tod_bucket_for_timestamp


def _stats(
    *,
    p05: float,
    p25: float,
    p50: float,
    p75: float,
    p95: float,
    mean: float,
    std: float,
    minv: float,
    maxv: float,
    n: int = 1000,
):
    return {
        "p05": p05,
        "p25": p25,
        "p50": p50,
        "p75": p75,
        "p95": p95,
        "mean": mean,
        "std": std,
        "min_value": minv,
        "max_value": maxv,
        "sample_size": n,
    }


def test_normal_regime_within_one_sigma():
    ctx = _historical_context_for(
        current=0.0,
        stats=_stats(p05=-2, p25=-1, p50=0, p75=1, p95=2, mean=0, std=1, minv=-3, maxv=3),
        tod_bucket_used=10,
    )
    assert ctx["regime"] == "normal"
    assert ctx["z_score"] == 0.0
    # current sits exactly at p50 -> percentile 50.
    assert ctx["percentile"] == 50.0
    assert ctx["tod_bucket_used"] == 10


def test_elevated_regime_above_one_sigma():
    ctx = _historical_context_for(
        current=1.5,
        stats=_stats(p05=-2, p25=-1, p50=0, p75=1, p95=2, mean=0, std=1, minv=-3, maxv=3),
        tod_bucket_used=0,
    )
    assert ctx["regime"] == "elevated"
    assert 1.0 <= ctx["z_score"] < 2.0


def test_extreme_high_regime_above_two_sigma():
    ctx = _historical_context_for(
        current=2.5,
        stats=_stats(p05=-2, p25=-1, p50=0, p75=1, p95=2, mean=0, std=1, minv=-3, maxv=3),
        tod_bucket_used=0,
    )
    assert ctx["regime"] == "extreme_high"
    assert ctx["z_score"] >= 2.0


def test_low_regime_below_one_sigma():
    ctx = _historical_context_for(
        current=-1.5,
        stats=_stats(p05=-2, p25=-1, p50=0, p75=1, p95=2, mean=0, std=1, minv=-3, maxv=3),
        tod_bucket_used=0,
    )
    assert ctx["regime"] == "low"
    assert -2.0 < ctx["z_score"] <= -1.0


def test_extreme_low_below_minus_two_sigma():
    ctx = _historical_context_for(
        current=-2.5,
        stats=_stats(p05=-2, p25=-1, p50=0, p75=1, p95=2, mean=0, std=1, minv=-3, maxv=3),
        tod_bucket_used=0,
    )
    assert ctx["regime"] == "extreme_low"


def test_record_high_overrides_z_score():
    """A live value past the stored max should be flagged record_high
    immediately, regardless of where the z-score lands.  This is the
    "today set a new record before tonight's refresh" branch."""
    ctx = _historical_context_for(
        current=5.0,
        stats=_stats(p05=-2, p25=-1, p50=0, p75=1, p95=2, mean=0, std=1, minv=-3, maxv=3),
        tod_bucket_used=0,
    )
    assert ctx["regime"] == "record_high"
    # We still surface the z-score so consumers can render it on the page.
    assert ctx["z_score"] is not None


def test_record_low_overrides_z_score():
    ctx = _historical_context_for(
        current=-5.0,
        stats=_stats(p05=-2, p25=-1, p50=0, p75=1, p95=2, mean=0, std=1, minv=-3, maxv=3),
        tod_bucket_used=0,
    )
    assert ctx["regime"] == "record_low"


def test_percentile_interpolation_between_quantile_anchors():
    """Halfway between p25 (-1) and p50 (0) → ~37.5 percentile."""
    ctx = _historical_context_for(
        current=-0.5,
        stats=_stats(p05=-2, p25=-1, p50=0, p75=1, p95=2, mean=0, std=1, minv=-3, maxv=3),
        tod_bucket_used=0,
    )
    assert ctx["percentile"] is not None
    assert 35.0 <= ctx["percentile"] <= 40.0


def test_unknown_when_std_zero_and_no_anchors():
    """Degenerate distribution (single value) — no regime can be inferred."""
    ctx = _historical_context_for(
        current=0.0,
        stats={
            "p05": None,
            "p25": None,
            "p50": None,
            "p75": None,
            "p95": None,
            "mean": 0.0,
            "std": 0.0,
            "min_value": 0.0,
            "max_value": 0.0,
            "sample_size": 1,
        },
        tod_bucket_used=None,
    )
    assert ctx["regime"] == "unknown"
    assert ctx["z_score"] is None
    assert ctx["percentile"] is None


def test_current_below_all_anchors_clamps_to_lowest_percentile():
    """Below p05 we report 5 (the lowest stored anchor) — we don't try to
    extrapolate beyond the captured quantiles."""
    ctx = _historical_context_for(
        current=-10.0,
        stats=_stats(p05=-2, p25=-1, p50=0, p75=1, p95=2, mean=0, std=1, minv=-3, maxv=3),
        tod_bucket_used=0,
    )
    # record_low overrides since -10 < min=-3, but percentile is still 5 (clamped).
    assert ctx["percentile"] == 5.0


def test_tod_bucket_for_rth_open_close():
    # 09:30 ET (DST/EDT) -> bucket 0
    ts = datetime(2026, 6, 16, 13, 30, tzinfo=timezone.utc)  # 13:30 UTC = 09:30 ET in summer
    assert _tod_bucket_for_timestamp(ts) == 0
    # 15:55 ET -> bucket 77 (last)
    ts = datetime(2026, 6, 16, 19, 55, tzinfo=timezone.utc)
    assert _tod_bucket_for_timestamp(ts) == 77
    # 16:00 ET -> -1 (past last RTH bucket)
    ts = datetime(2026, 6, 16, 20, 0, tzinfo=timezone.utc)
    assert _tod_bucket_for_timestamp(ts) == -1
