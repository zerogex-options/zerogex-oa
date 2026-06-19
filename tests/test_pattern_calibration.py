"""Tests for the playbook pattern-calibration feedback loop.

Covers the store builder's gates (sample size, freshness, clamp), the
pattern-wide weighted fallback, and the behavior-preserving consult
(``calibrated_base`` returns the prior when disabled / absent).
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from src import config
from src.signals.playbook import calibration as cal


@pytest.fixture(autouse=True)
def _reset_store():
    """Each test starts with no active store; restore afterward."""
    cal.set_active_store(None)
    yield
    cal.set_active_store(None)


@pytest.fixture
def _enabled(monkeypatch):
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_ENABLED", True)
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_MIN_SAMPLES", 20)
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_MAX_AGE_DAYS", 45)
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_FLOOR", 0.40)
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_CEIL", 0.85)


def _row(pattern, underlying, days_ago, n_resolved, proposed_base):
    return (pattern, underlying, date.today() - timedelta(days=days_ago), n_resolved, proposed_base)


# ----------------------------------------------------------------------
# Store builder gates
# ----------------------------------------------------------------------


def test_build_store_trusts_sufficient_fresh_window(_enabled):
    store = cal.build_store_from_rows([_row("gamma_flip_break", "SPY", 5, 40, 0.62)])
    assert store.by_pair[("gamma_flip_break", "SPY")] == pytest.approx(0.62)
    assert store.by_pattern["gamma_flip_break"] == pytest.approx(0.62)


def test_build_store_drops_undersampled_window(_enabled):
    store = cal.build_store_from_rows([_row("call_wall_fade", "SPY", 5, 5, 0.70)])
    assert store.by_pair == {}
    assert store.by_pattern == {}


def test_build_store_drops_stale_window(_enabled):
    store = cal.build_store_from_rows([_row("call_wall_fade", "SPY", 120, 50, 0.70)])
    assert store.by_pair == {}


def test_build_store_clamps_to_band(_enabled):
    store = cal.build_store_from_rows(
        [
            _row("hot", "SPY", 1, 50, 0.99),   # above ceil
            _row("cold", "SPY", 1, 50, 0.10),  # below floor
        ]
    )
    assert store.by_pair[("hot", "SPY")] == pytest.approx(0.85)
    assert store.by_pair[("cold", "SPY")] == pytest.approx(0.40)


def test_pattern_wide_fallback_is_sample_weighted(_enabled):
    # 100 trades @ 0.60 and 20 trades @ 0.45 → weighted mean ≈ 0.575.
    store = cal.build_store_from_rows(
        [
            _row("p", "SPY", 1, 100, 0.60),
            _row("p", "QQQ", 1, 20, 0.45),
        ]
    )
    assert store.by_pattern["p"] == pytest.approx((0.60 * 100 + 0.45 * 20) / 120)


# ----------------------------------------------------------------------
# Lookup + consult
# ----------------------------------------------------------------------


def test_lookup_prefers_pair_then_pattern_wide(_enabled):
    store = cal.CalibrationStore(
        by_pair={("p", "SPY"): 0.62},
        by_pattern={"p": 0.55},
    )
    assert store.lookup("p", "SPY") == 0.62      # exact pair
    assert store.lookup("p", "QQQ") == 0.55      # falls back to pattern-wide
    assert store.lookup("other", "SPY") is None  # unknown


def test_calibrated_base_returns_prior_when_disabled(monkeypatch):
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_ENABLED", False)
    cal.set_active_store(cal.CalibrationStore(by_pair={("p", "SPY"): 0.62}))
    # Disabled ⇒ prior returned unchanged regardless of the store.
    assert cal.calibrated_base("p", "SPY", fallback=0.50) == 0.50


def test_calibrated_base_returns_prior_when_no_store(_enabled):
    assert cal.active_store() is None
    assert cal.calibrated_base("p", "SPY", fallback=0.50) == 0.50


def test_calibrated_base_uses_store_when_enabled(_enabled):
    cal.set_active_store(cal.CalibrationStore(by_pair={("p", "SPY"): 0.62}))
    assert cal.calibrated_base("p", "SPY", fallback=0.50) == 0.62
    # Unknown pattern still falls back to the prior.
    assert cal.calibrated_base("unknown", "SPY", fallback=0.50) == 0.50


def test_load_store_uses_distinct_on_query(_enabled):
    class _Cur:
        def __init__(self):
            self.sql = ""

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [_row("p", "SPY", 1, 30, 0.58)]

    class _Conn:
        def cursor(self):
            return _Cur()

    store = cal.load_store(_Conn())
    assert store.by_pair[("p", "SPY")] == pytest.approx(0.58)
