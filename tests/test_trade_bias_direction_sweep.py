"""Tests for the Trade Bias direction hit-rate sweep (pure / DB-free logic)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.backtesting.trade_bias_direction_sweep import (
    DirRow,
    _row_from_payload,
    evaluate_direction,
)


def _rows(n=12, start=100.0, step=0.1, bias_score=40.0, confidence=60.0, day=17):
    base = datetime(2026, 7, day, 14, 0, tzinfo=timezone.utc)
    return [
        DirRow(
            timestamp=base + timedelta(minutes=i),
            close=round(start + i * step, 4),
            bias_score=bias_score,
            confidence=confidence,
        )
        for i in range(n)
    ]


def _stat(stats, bucket, h):
    return next(s for s in stats if s.bucket == bucket and s.horizon_min == h)


def test_long_lean_on_rising_tape_is_all_hits():
    # Rising closes + positive bias_score => every scored row is directionally right.
    stats = evaluate_direction(_rows(12, step=0.1, bias_score=40.0), [5])
    s = _stat(stats, "ALL", 5)
    assert s.n_scored == 7  # rows 0..6 each have a +5m forward return
    assert s.hit_rate == 1.0
    assert s.avg_edge_bps is not None and s.avg_edge_bps > 0
    assert s.avg_abs_move_bps is not None and s.avg_abs_move_bps > 0


def test_long_lean_on_falling_tape_is_all_misses():
    # Falling closes + positive bias_score => wrong direction every time.
    stats = evaluate_direction(_rows(12, step=-0.1, bias_score=40.0), [5])
    s = _stat(stats, "ALL", 5)
    assert s.hit_rate == 0.0
    assert s.avg_edge_bps is not None and s.avg_edge_bps < 0


def test_short_lean_on_falling_tape_is_all_hits():
    # Falling closes + negative bias_score => the short lean is right every time.
    stats = evaluate_direction(_rows(12, step=-0.1, bias_score=-40.0), [5])
    s = _stat(stats, "ALL", 5)
    assert s.hit_rate == 1.0
    assert s.avg_edge_bps is not None and s.avg_edge_bps > 0


def test_deadband_excludes_flat_rows():
    # |bias_score| == deadband => treated as flat, not scored.
    stats = evaluate_direction(_rows(12, bias_score=1.0), [5], deadband=1.0)
    s = _stat(stats, "ALL", 5)
    assert s.n_scored == 0
    assert s.hit_rate is None
    assert s.avg_edge_bps is None


def test_confidence_bucketing_routes_rows():
    # confidence 80 => everything lands in the "75-100" bucket and in ALL.
    stats = evaluate_direction(_rows(12, confidence=80.0), [5])
    assert _stat(stats, "75-100", 5).n_scored == 7
    assert _stat(stats, "0-25", 5).n_scored == 0
    assert _stat(stats, "ALL", 5).n_scored == 7


def test_none_confidence_only_in_all_bucket():
    stats = evaluate_direction(_rows(12, confidence=None), [5])
    assert _stat(stats, "ALL", 5).n_scored == 7
    for b in ("0-25", "25-50", "50-75", "75-100"):
        assert _stat(stats, b, 5).n_scored == 0


def test_forward_return_none_across_days():
    # Two 6-row sessions: within each, only the first row has a +5m match; no row
    # borrows the next session's open. => 2 scored total in ALL.
    rows = _rows(6, day=17) + _rows(6, day=18)
    s = _stat(evaluate_direction(rows, [5]), "ALL", 5)
    assert s.n_scored == 2


def test_row_from_payload_parses_contract():
    payload = {
        "timestamp": "2026-07-17T18:55:00+00:00",
        "close": 680.0,
        "bias_score": 47.5,
        "confidence": 35.0,
    }
    row = _row_from_payload("2026-07-17T18:55:00+00:00", payload)
    assert row is not None
    assert row.close == 680.0
    assert row.bias_score == 47.5
    assert row.confidence == 35.0


def test_row_from_payload_handles_json_string():
    import json

    payload = {
        "timestamp": "2026-07-17T18:55:00+00:00",
        "close": 100.0,
        "bias_score": -12.0,
        "confidence": None,
    }
    row = _row_from_payload(None, json.dumps(payload))
    assert row is not None
    assert row.bias_score == -12.0
    assert row.confidence is None
