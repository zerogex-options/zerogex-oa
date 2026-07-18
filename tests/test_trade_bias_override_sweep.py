"""Tests for the override-threshold backtest sweep (pure / DB-free logic)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.backtesting.trade_bias_override_sweep import (
    BiasRow,
    _forward_returns,
    _row_from_payload,
    count_with_forward,
    evaluate_sweep,
)


def _rows(n=12, start=100.0, step=0.1, trend="bearish", pillars=(0.6, 0.6, 0.5, 0.1), day=17):
    base = datetime(2026, 7, day, 14, 0, tzinfo=timezone.utc)
    out = []
    for i in range(n):
        out.append(
            BiasRow(
                timestamp=base + timedelta(minutes=i),
                close=round(start + i * step, 4),
                pillars={
                    "price_action": pillars[0],
                    "flow": pillars[1],
                    "tape": pillars[2],
                    "momentum": pillars[3],
                },
                struct_trend=trend,
                struct_confidence=4.0,
                struct_max_confidence=10.0,
            )
        )
    return out


def _stat(stats, d, h):
    return next(s for s in stats if s.override_d == d and s.horizon_min == h)


def test_lower_threshold_fires_more_overrides():
    # pillars => tactical D ~ 0.475 with 3 aligned; bearish structural is opposed.
    stats = evaluate_sweep(_rows(12), "swing", [0.30, 0.40, 0.50, 0.60], [5])
    assert _stat(stats, 0.30, 5).n_override > 0
    assert _stat(stats, 0.50, 5).n_override == 0  # 0.475 < 0.50 => no override
    assert _stat(stats, 0.30, 5).n_override >= _stat(stats, 0.50, 5).n_override


def test_override_hit_rate_and_edge_positive_on_rising_tape():
    # Rising closes + bullish override (long) => every override is directionally right.
    stats = evaluate_sweep(_rows(12), "swing", [0.30], [5])
    s = _stat(stats, 0.30, 5)
    assert s.hit_rate == 1.0
    assert s.avg_edge_bps is not None and s.avg_edge_bps > 0
    assert s.avg_abs_move_bps is not None and s.avg_abs_move_bps > 0


def test_high_threshold_has_no_overrides_and_null_hit_rate():
    stats = evaluate_sweep(_rows(12), "swing", [0.90], [5])
    s = _stat(stats, 0.90, 5)
    assert s.n_override == 0
    assert s.hit_rate is None
    assert s.avg_edge_bps is None


def test_forward_return_none_at_series_end_and_across_days():
    rows = _rows(6, day=17) + _rows(6, day=18)  # two sessions
    fwd = _forward_returns(rows, [5])[5]
    # Last row overall has no future row => None.
    assert fwd[-1] is None
    # The last row of day 17 (index 5) must NOT borrow day 18's open.
    assert fwd[5] is None


def test_count_with_forward_data():
    # A single row (Saturday's frozen upsert) has no later row => 0 scorable.
    assert count_with_forward(_rows(1), [5]) == 0
    assert count_with_forward([], [5]) == 0
    # 12 one-minute rows => rows 0..6 have a +5m match => 7 scorable.
    assert count_with_forward(_rows(12), [5]) == 7


def test_row_from_payload_parses_contract():
    payload = {
        "timestamp": "2026-07-17T18:55:00+00:00",
        "close": 680.0,
        "tactical": {"pillars": {"price_action": 0.6, "flow": 0.6, "tape": 0.5, "momentum": 0.1}},
        "structural_bias": {"trend": "bearish"},
        "confidence_raw": 4.0,
        "max_confidence_raw": 10.0,
    }
    row = _row_from_payload("2026-07-17T18:55:00+00:00", payload)
    assert row is not None
    assert row.close == 680.0
    assert row.struct_trend == "bearish"
    assert row.pillars["price_action"] == 0.6


def test_row_from_payload_handles_json_string():
    import json

    payload = {
        "timestamp": "2026-07-17T18:55:00+00:00",
        "close": 100.0,
        "tactical": {"pillars": {"price_action": None, "flow": 0.2, "tape": None, "momentum": 0.1}},
        "structural_bias": {"trend": "neutral"},
    }
    row = _row_from_payload(None, json.dumps(payload))
    assert row is not None
    assert row.pillars["price_action"] is None
    assert row.struct_max_confidence == 10.0  # default when absent
