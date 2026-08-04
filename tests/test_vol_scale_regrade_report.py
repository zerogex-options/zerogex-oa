"""Tests for the read-only vol-scale regrade backtest report."""

from __future__ import annotations

from datetime import date

from src.jobs.forecast_range_model import RANGE_OVER_SIGMA
from src.tools.vol_scale_regrade_report import analyze


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        self._params = params

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _FakeCursor(self._rows)


# Row tuple order matches the tool's SELECT:
# (symbol, date, open_spot, actual_low, actual_high, implied_move,
#  expected_vol_state, vol_state_correct)
def _row(symbol, actual_low, actual_high, implied_move,
         expected_vol_state, committed_correct):
    return (symbol, date(2026, 8, 1), 600.0, actual_low, actual_high,
            implied_move, expected_vol_state, committed_correct)


def _normal_range(implied: float) -> float:
    """A statistically ordinary day's high-low range for a given 1-σ move."""
    return RANGE_OVER_SIGMA * implied


def test_old_scale_mislabels_normal_day_the_fix_recovers_it():
    # A perfectly ordinary day (range = √(8/π)·implied) whose morning call was
    # "normal". OLD ÷implied buckets it "expansion" (wrong); FIXED ÷(1.6·impl)
    # buckets it "normal" (right) — the whole point of the fix.
    implied = 5.0
    rng = _normal_range(implied)
    rows = [
        _row("SPY", 600 - rng / 2, 600 + rng / 2, implied,
             expected_vol_state="normal", committed_correct=False)
        for _ in range(6)
    ]
    s = analyze(_FakeConn(rows), None)["SPY"]
    assert s.n_gradeable == 6
    assert s.old_rate == 0.0          # every normal day mislabeled expansion
    assert s.fixed_rate == 1.0        # re-centered → normal
    assert s.calib_rate == 1.0        # median day already ~1.0, basis ~1.0
    assert s.old_buckets == {"expansion": 6}
    assert s.fixed_buckets == {"normal": 6}


def test_calibrated_basis_absorbs_variance_risk_premium():
    # Realized ranges persistently run at 0.8× the Parkinson expectation (a VRP
    # regime). FIXED (basis 1.0) still marks these "compression" vs a "normal"
    # call; CALIB learns basis≈0.8 and re-centers them to "normal".
    implied = 5.0
    rng = 0.8 * _normal_range(implied)
    rows = [
        _row("SPY", 600 - rng / 2, 600 + rng / 2, implied,
             expected_vol_state="normal", committed_correct=False)
        for _ in range(15)
    ]
    s = analyze(_FakeConn(rows), None)["SPY"]
    assert s.calib_basis == 0.8
    assert s.fixed_rate == 0.0        # 0.8× reads compression under pure Parkinson
    assert s.calib_rate == 1.0        # basis 0.8 re-centers it to normal
    assert s.fixed_buckets == {"compression": 15}
    assert s.calib_buckets == {"normal": 15}


def test_rows_without_implied_move_are_not_vol_gradeable():
    # No implied move → the vol call can't be graded, but the row's committed
    # verdict still counts toward the committed (as-stored) reference rate.
    rows = [
        _row("QQQ", 595.0, 605.0, None,
             expected_vol_state="normal", committed_correct=True),
    ]
    s = analyze(_FakeConn(rows), None)["QQQ"]
    assert s.n_rows == 1
    assert s.n_gradeable == 0
    assert s.old_rate is None
    assert s.fixed_rate is None
    assert s.committed_scored == 1
    assert s.committed_rate == 1.0


def test_analyze_empty():
    assert analyze(_FakeConn([]), None) == {}
