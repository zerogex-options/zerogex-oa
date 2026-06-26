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


class _Cur:
    """Fake cursor returning per-source scripted rows, capturing the params."""

    def __init__(self, rows_by_source):
        self._rows_by_source = rows_by_source
        self.calls: list = []
        self._last_source = None

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        self._last_source = params[0] if params else None

    def fetchall(self):
        return self._rows_by_source.get(self._last_source, [])


class _Conn:
    def __init__(self, rows_by_source):
        self._cur = _Cur(rows_by_source)

    def cursor(self):
        return self._cur


def test_load_store_filters_by_source(monkeypatch, _enabled):
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_SOURCE", "underlying_touch")
    conn = _Conn({"underlying_touch": [_row("p", "SPY", 1, 30, 0.58)]})
    store = cal.load_store(conn)
    assert store.by_pair[("p", "SPY")] == pytest.approx(0.58)
    # The query was scoped to the configured source.
    assert conn._cur.calls[0][1] == ("underlying_touch",)
    assert "source = %s" in " ".join(conn._cur.calls[0][0].split())


def test_load_store_option_pnl_source(monkeypatch, _enabled):
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_SOURCE", "option_pnl")
    conn = _Conn({"option_pnl": [_row("p", "SPY", 1, 30, 0.71)]})
    store = cal.load_store(conn)
    assert store.by_pair[("p", "SPY")] == pytest.approx(0.71)
    assert conn._cur.calls[0][1] == ("option_pnl",)


def test_load_store_unknown_source_falls_back_to_touch(monkeypatch, _enabled):
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_SOURCE", "bogus")
    conn = _Conn({"underlying_touch": [_row("p", "SPY", 1, 30, 0.55)]})
    store = cal.load_store(conn)
    assert store.by_pair[("p", "SPY")] == pytest.approx(0.55)
    assert conn._cur.calls[0][1] == ("underlying_touch",)


def test_load_store_auto_prefers_pnl_with_touch_fallback(monkeypatch, _enabled):
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_SOURCE", "auto")
    conn = _Conn(
        {
            # 'p'/SPY measured by both — option_pnl must win.
            # 'q'/SPY measured only by touch — must fall back.
            "underlying_touch": [
                _row("p", "SPY", 1, 40, 0.55),
                _row("q", "SPY", 1, 40, 0.48),
            ],
            "option_pnl": [_row("p", "SPY", 1, 40, 0.72)],
        }
    )
    store = cal.load_store(conn)
    assert store.by_pair[("p", "SPY")] == pytest.approx(0.72)  # P&L preferred
    assert store.by_pair[("q", "SPY")] == pytest.approx(0.48)  # touch fallback


def test_merge_prefer_overlays_preferred():
    base = cal.CalibrationStore(
        by_pair={("a", "SPY"): 0.5, ("b", "SPY"): 0.6}, by_pattern={"a": 0.5}
    )
    pref = cal.CalibrationStore(by_pair={("a", "SPY"): 0.8}, by_pattern={"a": 0.8})
    merged = cal._merge_prefer(base, pref)
    assert merged.by_pair[("a", "SPY")] == 0.8   # preferred wins
    assert merged.by_pair[("b", "SPY")] == 0.6   # base retained
    assert merged.by_pattern["a"] == 0.8
