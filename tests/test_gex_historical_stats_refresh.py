"""Tests for the gex_historical_stats refresh tool."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.tools.gex_historical_stats_refresh import (
    METRICS,
    MIN_BUCKET_SAMPLES,
    MIN_FLAT_SAMPLES,
    RTH_BUCKETS,
    Distribution,
    WINDOWS,
    _summarize,
    _tod_bucket_for,
)


def test_summarize_returns_none_for_empty_input():
    assert _summarize([]) is None
    assert _summarize([None, float("nan")]) is None


def test_summarize_percentile_and_stats():
    samples = [float(i) for i in range(100)]  # 0..99
    dist = _summarize(samples)
    assert dist is not None
    assert dist.sample_size == 100
    # p05 ≈ 4.95, p50 ≈ 49.5, p95 ≈ 94.05 — np.percentile uses linear interp.
    assert 4 <= dist.p05 <= 6
    assert 48 <= dist.p50 <= 51
    assert 93 <= dist.p95 <= 96
    assert dist.min_value == 0.0
    assert dist.max_value == 99.0


def test_summarize_handles_single_sample():
    dist = _summarize([42.0])
    assert dist is not None
    assert dist.sample_size == 1
    assert dist.p05 == dist.p50 == dist.p95 == 42.0
    # ddof=1 with n=1 would be undefined — we fall back to 0.0.
    assert dist.std == 0.0


def test_tod_bucket_for_rth_window():
    # 09:30 ET → bucket 0
    et = datetime(2026, 6, 16, 9, 30, tzinfo=timezone.utc).replace(
        tzinfo=timezone(timedelta(hours=-4))  # ET in summer (DST)
    )
    assert _tod_bucket_for(et) == 0
    # 09:34 ET → still bucket 0
    et = et + timedelta(minutes=4)
    assert _tod_bucket_for(et) == 0
    # 09:35 ET → bucket 1
    et = datetime(2026, 6, 16, 9, 35, tzinfo=timezone(timedelta(hours=-4)))
    assert _tod_bucket_for(et) == 1
    # 15:55 ET → bucket 77 (last)
    et = datetime(2026, 6, 16, 15, 55, tzinfo=timezone(timedelta(hours=-4)))
    assert _tod_bucket_for(et) == 77


def test_tod_bucket_outside_rth_returns_minus_one():
    # Pre-market 04:00 ET
    et = datetime(2026, 6, 16, 4, 0, tzinfo=timezone(timedelta(hours=-4)))
    assert _tod_bucket_for(et) == -1
    # Exactly 16:00 ET — bucket 78 is past the last RTH 5-min window.
    et = datetime(2026, 6, 16, 16, 0, tzinfo=timezone(timedelta(hours=-4)))
    assert _tod_bucket_for(et) == -1
    # After-hours 18:00 ET
    et = datetime(2026, 6, 16, 18, 0, tzinfo=timezone(timedelta(hours=-4)))
    assert _tod_bucket_for(et) == -1


def test_metric_set_matches_endpoint_contract():
    """The endpoint hard-codes which metric names it reads back; keep both
    sides in lockstep so the refresh tool can't silently drop a metric."""
    metric_names = {name for name, _column in METRICS}
    assert metric_names == {"net_gex_at_spot", "total_net_gex"}


def test_window_labels_are_unique_and_known():
    """The schema's PRIMARY KEY treats window_label as part of the key; the
    refresh / endpoint pair must agree on the exact labels."""
    labels = {w.label for w in WINDOWS}
    assert labels == {"30d", "all_time"}
    assert len(labels) == len(WINDOWS)


def test_rth_buckets_is_six_point_five_hours():
    # 09:30 ET to 16:00 ET = 390 minutes = 78 5-min buckets.
    assert RTH_BUCKETS == 78


def test_min_thresholds_are_sensible():
    # Per-bucket distribution is only meaningful with at least a couple of
    # weeks of session repeats at the same TOD; the flat fallback needs
    # roughly a full trading day of samples to be representative.
    assert MIN_BUCKET_SAMPLES >= 5
    assert MIN_FLAT_SAMPLES >= MIN_BUCKET_SAMPLES * 5
