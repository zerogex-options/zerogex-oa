"""Tests for the read-only vol-scale regrade backtest report."""

from __future__ import annotations

from datetime import date, timedelta

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
#  expected_vol_state, vol_state_correct, forecast_inputs)
def _row(symbol, actual_low, actual_high, implied_move,
         expected_vol_state, committed_correct, day=date(2026, 8, 1), fi=None):
    return (symbol, day, 600.0, actual_low, actual_high,
            implied_move, expected_vol_state, committed_correct, fi)


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


def test_reworked_prediction_beats_static_normal_via_persistence():
    # A regime that CLUSTERS: 8 quiet days then 8 active days. The committed v1.4
    # model called "normal" every day. On the calibrated scale the window splits
    # into compression + expansion (0 normal), so the static "normal" call never
    # hits — but the causal persistence anchor tracks each cluster and recovers a
    # chunk of them, i.e. the rework strictly beats the shipped model here.
    implied = 5.0
    rows = []
    for i in range(16):
        raw = 0.40 if i < 8 else 0.90  # low-vol cluster, then high-vol cluster
        rng = raw * RANGE_OVER_SIGMA * implied
        rows.append(_row(
            "SPY", 600 - rng / 2, 600 + rng / 2, implied,
            expected_vol_state="normal", committed_correct=False,
            day=date(2026, 8, 1) + timedelta(days=i),
        ))
    s = analyze(_FakeConn(rows), None)["SPY"]
    assert s.calib_rate == 0.0                       # static "normal" hits nothing
    assert s.reworked_rate is not None
    assert s.reworked_rate > s.calib_rate            # persistence recovers days
    assert s.baseline_rate == 0.5                    # 8 comp / 8 exp
    assert s.reworked_scored == 16


def test_atr_over_implied_diagnostic_collected():
    # The implied_move sanity ratio is surfaced from the forecast_inputs blob.
    implied = 5.0
    rng = RANGE_OVER_SIGMA * implied
    rows = [
        _row("SPY", 600 - rng / 2, 600 + rng / 2, implied,
             expected_vol_state="normal", committed_correct=True,
             day=date(2026, 8, 1) + timedelta(days=i), fi={"atr_5d": 6.5})
        for i in range(3)
    ]
    s = analyze(_FakeConn(rows), None)["SPY"]
    assert s.median_atr_over_implied == 1.3          # 6.5 / 5.0


def test_analyze_empty():
    assert analyze(_FakeConn([]), None) == {}
