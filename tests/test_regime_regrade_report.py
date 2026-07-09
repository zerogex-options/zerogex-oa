"""Tests for the read-only regime regrade backtest report."""

from __future__ import annotations

from datetime import date

from src.tools.regime_regrade_report import _regrade, analyze


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
# (symbol, date, open_spot, gamma_flip, regime, threshold, actual_close, regime_correct)
def _row(symbol, open_spot, gamma_flip, actual_close, committed_correct,
         committed_regime="long_gamma", threshold=0.02):
    return (symbol, date(2026, 6, 29), open_spot, gamma_flip, committed_regime,
            threshold, actual_close, committed_correct)


def test_regrade_matches_receipt_logic():
    # long_gamma correct on a chop day, wrong on a trend day; short is inverse.
    assert _regrade("long_gamma", 100.0, 100.5, 0.02) is True    # 0.5% <= 2%
    assert _regrade("long_gamma", 100.0, 103.0, 0.02) is False   # 3% > 2%
    assert _regrade("short_gamma", 100.0, 103.0, 0.02) is True
    assert _regrade("short_gamma", 100.0, 100.5, 0.02) is False
    assert _regrade("transition", 100.0, 103.0, 0.02) is None
    assert _regrade("long_gamma", 0.0, 100.0, 0.02) is None      # guard div/0


def test_analyze_recomputes_and_flips_labels():
    rows = [
        # Committed long_gamma, spot ABOVE flip → recompute also long_gamma (no flip).
        # Chop day (0.48% move) → both graded correct.
        _row("SPY", open_spot=105.0, gamma_flip=100.0, actual_close=105.5, committed_correct=True),
        # Committed long_gamma (the bug), but spot BELOW flip → recompute short_gamma (FLIP).
        # Trend day (3.16% move) → committed long_gamma WRONG, recomputed short_gamma RIGHT.
        _row("SPY", open_spot=95.0, gamma_flip=100.0, actual_close=98.0, committed_correct=False),
    ]
    stats = analyze(_FakeConn(rows), None)
    s = stats["SPY"]
    assert s.n_rows == 2
    # Committed: 1 of 2 correct; every committed label was long_gamma (the bug).
    assert s.committed_scored == 2
    assert s.committed_rate == 0.5
    assert s.committed_regime_counts == {"long_gamma": 2}
    # Recomputed: both correct under the corrected classifier; one label flipped.
    assert s.recomputed_scored == 2
    assert s.recomputed_rate == 1.0
    assert s.recomputed_regime_counts == {"long_gamma": 1, "short_gamma": 1}
    assert s.label_flips == 1


def test_analyze_skips_ungraded_committed_rows():
    # A committed row graded None (transition) shouldn't count toward the
    # committed scored denominator.
    rows = [
        _row("QQQ", open_spot=100.0, gamma_flip=100.0, actual_close=100.0,
             committed_correct=None, committed_regime="transition"),
    ]
    stats = analyze(_FakeConn(rows), None)
    s = stats["QQQ"]
    assert s.n_rows == 1
    assert s.committed_scored == 0
    assert s.committed_rate is None


def test_analyze_empty():
    assert analyze(_FakeConn([]), None) == {}
