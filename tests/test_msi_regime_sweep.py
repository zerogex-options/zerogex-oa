"""Tests for the MSI regime-validity sweep (pure / DB-free logic)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from src.backtesting.msi_regime_sweep import (
    MsiRow,
    _band_for,
    _row_from_payload,
    evaluate_msi_regime,
)

_BAND_LABELS = ["reversal <20", "chop 20-40", "controlled 40-70", "trend >=70"]


def _series(closes, msi, day=17):
    base = datetime(2026, 7, day, 14, 0, tzinfo=timezone.utc)
    return [
        MsiRow(timestamp=base + timedelta(minutes=i), close=c, msi=msi)
        for i, c in enumerate(closes)
    ]


def _stat(stats, band, h):
    return next(s for s in stats if s.band == band and s.horizon_min == h)


def test_trending_series_is_all_continuation():
    # Monotonic rise => every split window continues; msi 75 => trend band.
    rows = _series([100 + 0.1 * i for i in range(8)], msi=75.0)
    s = _stat(evaluate_msi_regime(rows, [4]), "trend >=70", 4)
    assert s.n > 0
    assert s.continuation_rate == 1.0
    assert s.avg_abs_move_bps is not None and s.avg_abs_move_bps > 0


def test_alternating_series_is_mean_reverting():
    # Zigzag => the second half reverses the first => continuation ~ 0; msi 30 => chop.
    rows = _series([100 + (i % 2) for i in range(8)], msi=30.0)
    s = _stat(evaluate_msi_regime(rows, [2]), "chop 20-40", 2)
    assert s.n > 0
    assert s.continuation_rate == 0.0


def test_band_routing():
    for msi, band in [(10.0, _BAND_LABELS[0]), (30.0, _BAND_LABELS[1]),
                      (55.0, _BAND_LABELS[2]), (85.0, _BAND_LABELS[3])]:
        stats = evaluate_msi_regime(_series([100 + 0.1 * i for i in range(8)], msi=msi), [4])
        assert _stat(stats, band, 4).n > 0
        for other in _BAND_LABELS:
            if other != band:
                assert _stat(stats, other, 4).n == 0


def test_none_msi_skipped():
    rows = _series([100 + 0.1 * i for i in range(8)], msi=None)
    assert _stat(evaluate_msi_regime(rows, [4]), "ALL", 4).n == 0


def test_band_for_boundaries():
    assert _band_for(0.0) == "reversal <20"
    assert _band_for(19.999) == "reversal <20"
    assert _band_for(20.0) == "chop 20-40"
    assert _band_for(69.999) == "controlled 40-70"
    assert _band_for(70.0) == "trend >=70"
    assert _band_for(100.0) == "trend >=70"
    assert _band_for(None) is None


def test_row_from_payload_reads_msi_from_inputs():
    payload = {
        "timestamp": "2026-07-17T18:55:00+00:00",
        "close": 680.0,
        "inputs": {"msi": 24.04, "net_gex": 50},
    }
    row = _row_from_payload("2026-07-17T18:55:00+00:00", payload)
    assert row is not None
    assert row.close == 680.0
    assert row.msi == 24.04


def test_row_from_payload_handles_missing_inputs():
    payload = {"timestamp": "2026-07-17T18:55:00+00:00", "close": 100.0}
    row = _row_from_payload(None, json.dumps(payload))
    assert row is not None
    assert row.msi is None
