"""The cadence report behind ``make quote-cadence`` (src/tools/quote_cadence.py)."""

import math

from src.tools.quote_cadence import summarize


def test_a_steady_one_second_tape_reads_as_sixty_a_minute():
    [row] = summarize({"SPY": [float(s) for s in range(60)]}, seconds=60)
    assert row.updates == 60
    assert row.per_minute == 60
    assert row.median_gap == 1.0
    assert row.max_gap == 1.0
    assert row.slow_gaps == 0


def test_a_stall_shows_as_the_worst_gap_and_a_slow_gap():
    times = [0.0, 1.0, 2.0, 6.5, 7.5, 8.5]
    [row] = summarize({"QQQ": times}, seconds=10)
    assert row.max_gap == 4.5
    assert row.slow_gaps == 1
    assert row.median_gap == 1.0


def test_arrival_order_does_not_matter_and_busiest_symbol_comes_first():
    rows = summarize({"SPX": [2.0, 0.0, 1.0], "SPY": [0.0, 0.5, 1.0, 1.5]}, seconds=2)
    assert [r.symbol for r in rows] == ["SPY", "SPX"]
    assert rows[1].median_gap == 1.0


def test_a_single_update_has_no_gap_to_measure():
    [row] = summarize({"IWM": [3.0]}, seconds=60)
    assert row.updates == 1
    assert math.isnan(row.median_gap)
    assert row.slow_gaps == 0
