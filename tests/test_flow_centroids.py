"""Tests for src.analytics.flow_centroids.build_centroid_series.

The call / put centroid is the volume-weighted average strike of the contracts
traded on that side. The session line covers the open through each bar; the
recent line covers only the last ``window_minutes``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.analytics.flow_centroids import build_centroid_series

UTC = timezone.utc
OPEN = datetime(2026, 10, 19, 13, 30, tzinfo=UTC)


def _bar(n: int) -> datetime:
    return OPEN + timedelta(minutes=5 * n)


def _row(n: int, side: str, trades: list[tuple[float, int]]) -> dict:
    """One (bar, side) aggregate from (strike, contracts) pairs."""
    return {
        "bar_start": _bar(n),
        "option_type": side,
        "volume": sum(v for _, v in trades),
        "strike_volume": sum(s * v for s, v in trades),
    }


def test_empty_input_is_an_empty_series():
    assert build_centroid_series([]) == []


def test_centroid_is_weighted_by_contracts_traded():
    rows = [
        _row(0, "C", [(7700, 10), (7710, 30)]),
        _row(0, "P", [(7650, 20), (7690, 20)]),
    ]

    [bar] = build_centroid_series(rows)

    assert bar["bar_start"] == _bar(0)
    assert bar["call_centroid"] == pytest.approx(7707.5)
    assert bar["put_centroid"] == pytest.approx(7670.0)
    # One bar in: the recent window and the session are the same trades.
    assert bar["call_centroid_recent"] == pytest.approx(7707.5)
    assert bar["put_centroid_recent"] == pytest.approx(7670.0)
    assert (bar["call_volume"], bar["put_volume"]) == (40, 40)


def test_session_accumulates_and_recent_keeps_only_the_window():
    rows = [
        _row(0, "C", [(7760, 100)]),
        _row(1, "C", [(7740, 100)]),
        _row(2, "C", [(7720, 100)]),
        _row(3, "C", [(7700, 100)]),
    ]

    series = build_centroid_series(rows, window_minutes=15)

    assert [b["call_centroid"] for b in series] == pytest.approx([7760, 7750, 7740, 7730])
    # 15 minutes = this bar and the two before it.
    assert [b["call_centroid_recent"] for b in series] == pytest.approx([7760, 7750, 7740, 7720])
    assert [b["call_volume"] for b in series] == [100, 200, 300, 400]


def test_a_five_minute_window_is_the_bar_alone():
    rows = [_row(0, "C", [(7760, 100)]), _row(1, "C", [(7740, 300)])]

    series = build_centroid_series(rows, window_minutes=5)

    assert [b["call_centroid_recent"] for b in series] == pytest.approx([7760, 7740])
    assert series[-1]["call_centroid"] == pytest.approx(7745)


def test_the_recent_window_is_measured_in_time_not_in_rows():
    # Nothing traded in bars 1-3, so at bar 4 the 15-minute window holds bar 4
    # only. Counting rows instead would reach back to bar 0.
    rows = [_row(0, "C", [(7800, 100)]), _row(4, "C", [(7700, 100)])]

    series = build_centroid_series(rows, window_minutes=15)

    assert [b["bar_start"] for b in series] == [_bar(0), _bar(4)]
    assert series[-1]["call_centroid_recent"] == pytest.approx(7700)
    assert series[-1]["call_centroid"] == pytest.approx(7750)


def test_a_side_with_no_trades_reads_none_and_the_session_carries_forward():
    rows = [
        _row(0, "C", [(7760, 100)]),
        _row(1, "P", [(7640, 50)]),
        _row(5, "P", [(7660, 50)]),
    ]

    series = build_centroid_series(rows, window_minutes=10)

    assert series[0]["put_centroid"] is None
    assert series[0]["put_centroid_recent"] is None
    # No calls after bar 0: the session line holds, the recent line empties.
    assert series[1]["call_centroid"] == pytest.approx(7760)
    assert series[1]["call_centroid_recent"] == pytest.approx(7760)
    assert series[2]["call_centroid"] == pytest.approx(7760)
    assert series[2]["call_centroid_recent"] is None
    assert series[2]["put_centroid"] == pytest.approx(7650)
    assert series[2]["put_centroid_recent"] == pytest.approx(7660)


def test_input_order_does_not_matter_and_split_rows_add_up():
    rows = [
        _row(1, "C", [(7740, 100)]),
        _row(0, "C", [(7760, 50)]),
        _row(0, "C", [(7760, 50)]),
        {"bar_start": _bar(0), "option_type": "X", "volume": 999, "strike_volume": 1.0},
    ]

    series = build_centroid_series(rows)

    assert [b["bar_start"] for b in series] == [_bar(0), _bar(1)]
    assert series[0]["call_volume"] == 100
    assert series[1]["call_centroid"] == pytest.approx(7750)


@pytest.mark.parametrize("window", [0, 3, 7, -5])
def test_a_window_that_is_not_a_multiple_of_the_bar_is_refused(window):
    with pytest.raises(ValueError):
        build_centroid_series([], window_minutes=window)
