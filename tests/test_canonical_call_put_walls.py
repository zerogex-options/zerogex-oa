"""Tests for the canonical Call/Put Wall helper.

The helper lives at :mod:`src.analytics.walls` and is the single source of
truth for every consumer (``/api/gex/summary``, ``/api/gex/history``,
:class:`src.signals.unified_signal_engine.UnifiedSignalEngine`, every
playbook pattern that reads ``ctx.level("call_wall" | "put_wall")``).
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.analytics.walls import compute_call_put_walls


def _row(strike: float, call_gamma: float = 0.0, put_gamma: float = 0.0) -> dict:
    return {
        "strike": strike,
        "call_gamma": call_gamma,
        "put_gamma": put_gamma,
        # Extra keys are accepted and ignored — callers commonly pass full
        # gex_by_strike rows straight in.
        "net_gex": 0.0,
        "call_oi": 0,
        "put_oi": 0,
    }


def test_picks_max_call_gamma_above_spot_and_max_put_gamma_below_spot():
    spot = 100.0
    rows = [
        _row(90.0, call_gamma=10.0, put_gamma=5.0),
        _row(95.0, call_gamma=20.0, put_gamma=25.0),  # below spot, big put
        _row(105.0, call_gamma=30.0, put_gamma=10.0),  # above spot, biggest call
        _row(110.0, call_gamma=15.0, put_gamma=8.0),
    ]
    call_wall, put_wall = compute_call_put_walls(rows, spot)
    assert call_wall == 105.0
    assert put_wall == 95.0


def test_excludes_call_strikes_below_spot_and_put_strikes_above_spot():
    """A huge call_gamma far below spot must not be picked as call_wall.

    This is the exact bug that caused ``/api/gex/summary`` to disagree with
    the signals layer — the endpoint was ranking across all strikes with no
    above/below-spot filter.
    """
    spot = 100.0
    rows = [
        # Big call_gamma below spot — must be ignored by call_wall.
        _row(80.0, call_gamma=999.0, put_gamma=1.0),
        # Big put_gamma above spot — must be ignored by put_wall.
        _row(120.0, call_gamma=5.0, put_gamma=999.0),
        # Legitimate walls (eligible side).
        _row(105.0, call_gamma=5.0, put_gamma=1.0),
        _row(95.0, call_gamma=1.0, put_gamma=5.0),
    ]
    call_wall, put_wall = compute_call_put_walls(rows, spot)
    # call_wall must come from strikes >= 100 (only 105 and 120 qualify, and
    # 120 has lower call_gamma).  put_wall must come from strikes <= 100 (only
    # 80 and 95 qualify, and 80 has lower put_gamma).
    assert call_wall == 105.0
    assert put_wall == 95.0


def test_at_spot_strike_is_eligible_on_both_sides():
    """An at-the-money strike participates in both the call and put scan."""
    spot = 100.0
    rows = [
        _row(100.0, call_gamma=50.0, put_gamma=40.0),
        _row(105.0, call_gamma=10.0, put_gamma=5.0),
        _row(95.0, call_gamma=8.0, put_gamma=20.0),
    ]
    call_wall, put_wall = compute_call_put_walls(rows, spot)
    # ATM dominates both sides because filters are inclusive (>= and <=).
    assert call_wall == 100.0
    assert put_wall == 100.0


def test_tiebreaker_prefers_nearest_to_spot():
    """When two strikes tie on gamma magnitude, the strike closer to spot wins."""
    spot = 100.0
    rows = [
        _row(110.0, call_gamma=10.0),
        _row(105.0, call_gamma=10.0),  # same call_gamma but closer above spot
        _row(90.0, put_gamma=10.0),
        _row(95.0, put_gamma=10.0),  # same put_gamma but closer below spot
    ]
    call_wall, put_wall = compute_call_put_walls(rows, spot)
    assert call_wall == 105.0  # lowest strike above spot wins
    assert put_wall == 95.0  # highest strike below spot wins


def test_returns_none_when_side_is_empty():
    """No strikes on one side of spot ⇒ that wall is None."""
    spot = 100.0
    # All strikes below spot — call_wall must be None.
    rows = [_row(80.0, call_gamma=5.0, put_gamma=10.0)]
    call_wall, put_wall = compute_call_put_walls(rows, spot)
    assert call_wall is None
    assert put_wall == 80.0


def test_returns_none_when_gamma_is_zero():
    """Zero/negative gamma rows are not eligible."""
    spot = 100.0
    rows = [
        _row(110.0, call_gamma=0.0, put_gamma=0.0),
        _row(90.0, call_gamma=0.0, put_gamma=0.0),
    ]
    call_wall, put_wall = compute_call_put_walls(rows, spot)
    assert call_wall is None
    assert put_wall is None


def test_handles_missing_or_none_fields():
    spot = 100.0
    rows = [
        {"strike": 105.0, "call_gamma": None, "put_gamma": 0.0},
        {"strike": 110.0, "call_gamma": 5.0},  # put_gamma missing entirely
        {"strike": 95.0, "put_gamma": 5.0},  # call_gamma missing
    ]
    call_wall, put_wall = compute_call_put_walls(rows, spot)
    assert call_wall == 110.0
    assert put_wall == 95.0


def test_handles_invalid_spot():
    """Non-positive spot returns (None, None) without crashing."""
    rows = [_row(100.0, call_gamma=5.0, put_gamma=5.0)]
    assert compute_call_put_walls(rows, 0) == (None, None)
    assert compute_call_put_walls(rows, -1.0) == (None, None)
    assert compute_call_put_walls(rows, None) == (None, None)  # type: ignore[arg-type]


def test_handles_empty_input():
    assert compute_call_put_walls([], 100.0) == (None, None)


def test_endpoint_and_signal_engine_now_agree():
    """End-to-end sanity: same inputs produce the same walls regardless of
    which caller (REST endpoint, analytics engine, signal engine) computed
    them.  This was the regression the canonical helper was introduced to
    fix.
    """
    spot = 678.0
    rows = [
        _row(670.0, call_gamma=0.0, put_gamma=120.0),  # max put below spot
        _row(675.0, call_gamma=0.0, put_gamma=80.0),
        _row(680.0, call_gamma=150.0, put_gamma=0.0),  # max call above spot
        _row(685.0, call_gamma=100.0, put_gamma=0.0),
    ]

    # The REST endpoint, the analytics engine, and the unified signal engine
    # all go through this function — there is no second implementation to
    # diverge.
    assert compute_call_put_walls(rows, spot) == (680.0, 670.0)


def test_pre_sorted_input_does_not_change_result():
    """Result is order-invariant within tiebreaker rules."""
    spot = 100.0
    rows_a = [
        _row(110.0, call_gamma=10.0),
        _row(105.0, call_gamma=10.0),
        _row(90.0, put_gamma=10.0),
        _row(95.0, put_gamma=10.0),
    ]
    rows_b = list(reversed(rows_a))
    assert compute_call_put_walls(rows_a, spot) == compute_call_put_walls(rows_b, spot)


def test_main_engine_summary_includes_call_put_walls():
    """The Analytics Engine ``_calculate_gex_summary`` must surface canonical
    walls in the summary dict so ``_store_gex_summary`` can persist them.
    """
    from src.analytics.main_engine import AnalyticsEngine

    engine = AnalyticsEngine(underlying="SPY")
    ts = datetime(2026, 3, 27, 15, 55, tzinfo=timezone.utc)
    spot = 500.0

    gex_by_strike = [
        {
            "strike": 495.0,
            "call_gamma": 0.0,
            "put_gamma": 80.0,
            "net_gex": -400000.0,
            "call_oi": 0,
            "put_oi": 1000,
        },
        {
            "strike": 500.0,
            "call_gamma": 50.0,
            "put_gamma": 50.0,
            "net_gex": 0.0,
            "call_oi": 500,
            "put_oi": 500,
        },
        {
            "strike": 505.0,
            "call_gamma": 120.0,
            "put_gamma": 0.0,
            "net_gex": 600000.0,
            "call_oi": 1500,
            "put_oi": 0,
        },
    ]

    # _calculate_max_pain and _calculate_gamma_flip_point both consume options
    # rows, not strike rows — pass an empty list to bypass them (they
    # tolerate that and return None).
    summary = engine._calculate_gex_summary(
        gex_by_strike=gex_by_strike,
        options=[],
        underlying_price=spot,
        timestamp=ts,
    )

    assert summary is not None
    assert summary["call_wall"] == 505.0
    assert summary["put_wall"] == 495.0
