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


def test_band_for_source_uses_pnl_override(monkeypatch, _enabled):
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_FLOOR_OPTION_PNL", 0.25)
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_CEIL_OPTION_PNL", 0.90)
    assert cal._band_for_source("option_pnl") == (0.25, 0.90)
    assert cal._band_for_source("underlying_touch") == (0.40, 0.85)
    assert cal._band_for_source(None) == (0.40, 0.85)


def test_option_pnl_band_clamps_independently(monkeypatch, _enabled):
    # A genuinely-losing 0.13 base: the global band pins it at 0.40, but the
    # option_pnl band (floor lowered to 0.20) marks it down further.
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_FLOOR_OPTION_PNL", 0.20)
    touch = cal.build_store_from_rows(
        [_row("p", "SPY", 1, 50, 0.13)], source="underlying_touch"
    )
    pnl = cal.build_store_from_rows(
        [_row("p", "SPY", 1, 50, 0.13)], source="option_pnl"
    )
    assert touch.by_pair[("p", "SPY")] == pytest.approx(0.40)
    assert pnl.by_pair[("p", "SPY")] == pytest.approx(0.20)


def test_load_store_auto_applies_per_source_band(monkeypatch, _enabled):
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_SOURCE", "auto")
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_FLOOR_OPTION_PNL", 0.20)
    conn = _Conn(
        {
            "underlying_touch": [_row("p", "SPY", 1, 40, 0.13)],
            "option_pnl": [_row("p", "SPY", 1, 40, 0.13)],
        }
    )
    store = cal.load_store(conn)
    # option_pnl wins under 'auto' AND is clamped with its own lower floor.
    assert store.by_pair[("p", "SPY")] == pytest.approx(0.20)


def test_compare_report_marks_gate_and_delta():
    from src.tools.pattern_calibration_refresh import _compare_report

    today = date.today()
    out = _compare_report(
        {"p": 0.5},
        [("p", "SPY", today, 1000, 0.04)],
        [("p", "SPY", today, 120, 0.33)],
        min_samples=20,
    )
    assert "option_pnl" in out
    assert "+0.290" in out  # Δ = 0.33 − 0.04
    # A below-gate cell (n < min_samples) is flagged with '·'.
    gated = _compare_report(
        {"q": 0.5}, [("q", "SPY", today, 10, 0.70)], [], min_samples=20
    )
    assert "·" in gated


def test_compare_report_auto_column(_enabled):
    from src.tools.pattern_calibration_refresh import _compare_report

    today = date.today()
    out = _compare_report(
        {"a": 0.5, "b": 0.5},
        # 'a' measured by both (pnl trustworthy ⇒ auto picks P); 'b' touch-only.
        [("a", "SPY", today, 1000, 0.04), ("b", "SPY", today, 1000, 0.62)],
        [("a", "SPY", today, 72, 0.585)],
        min_samples=20,
    )
    assert "0.585 P" in out   # auto prefers the trustworthy option_pnl base
    assert "0.620 T" in out   # auto falls back to touch where no P&L window


def test_compare_report_auto_below_gate_falls_back(_enabled):
    from src.tools.pattern_calibration_refresh import _compare_report

    today = date.today()
    # option_pnl present but below the gate (n=3) ⇒ auto must not pick it.
    out = _compare_report(
        {"a": 0.5},
        [("a", "SPY", today, 1000, 0.50)],
        [("a", "SPY", today, 3, 0.90)],
        min_samples=20,
    )
    assert "0.500 T" in out
    assert "0.900 P" not in out


def test_explain_report_outcome_distribution():
    from types import SimpleNamespace as NS
    from datetime import datetime

    from src.tools.pattern_calibration_refresh import _explain_report

    # 4 target_hit (1 profitable) + 2 time_exit (1 profitable) — a theta trap:
    # the pattern hits its target but the option mostly loses money.
    trades = [
        NS(seq=i, entered_at=datetime(2026, 5, 1, 10, i), exited_at=None,
           hold_minutes=30, strike=500, option_type="C", entry_premium=2.0,
           exit_premium=0.5, contracts=1,
           net_pnl=(80 if i in (0, 5) else -150),
           return_pct=(40.0 if i in (0, 5) else -75.0),
           outcome=("time_exit" if i in (0, 3) else "target_hit"))
        for i in range(6)
    ]
    out = _explain_report(NS(trades=trades), pattern="overnight_trap", underlying="SPY")
    assert "realized win rate: 2/6" in out
    assert "target_hit" in out
    assert "profitable    1 (25%)" in out  # 1 of 4 target_hits made money
    # Economics block: 2×80 win, 4×−150 loss → net −440, PF (160/600)=0.27.
    assert "net P&L $-440" in out
    assert "profit factor 0.27" in out


def test_structures_report_single_vs_vertical():
    from src.tools.pattern_calibration_refresh import _structures_report

    single = {"p": {"n": 75, "win_rate": 0.13, "pf": 0.10, "expectancy": -1417.0}}
    vert = {"p": {"n": 70, "win_rate": 0.41, "pf": 0.95, "expectancy": -95.0}}
    out = _structures_report({"single": single, "vertical": vert}, underlying="SPY")
    assert "single" in out and "vertical" in out
    assert "13% ( 75)" in out      # single win% + n
    assert "41% ( 70)" in out      # vertical win% + n
    assert "pf  0.95" in out


def test_explain_report_empty():
    from types import SimpleNamespace as NS

    from src.tools.pattern_calibration_refresh import _explain_report

    out = _explain_report(NS(trades=[]), pattern="pin_risk_premium_sell", underlying="SPY")
    assert "no priced entries" in out


def test_merge_prefer_overlays_preferred():
    base = cal.CalibrationStore(
        by_pair={("a", "SPY"): 0.5, ("b", "SPY"): 0.6}, by_pattern={"a": 0.5}
    )
    pref = cal.CalibrationStore(by_pair={("a", "SPY"): 0.8}, by_pattern={"a": 0.8})
    merged = cal._merge_prefer(base, pref)
    assert merged.by_pair[("a", "SPY")] == 0.8   # preferred wins
    assert merged.by_pair[("b", "SPY")] == 0.6   # base retained
    assert merged.by_pattern["a"] == 0.8


# ----------------------------------------------------------------------
# Auto-source soft-pnl disagreement veto
# ----------------------------------------------------------------------


@pytest.fixture
def _veto_defaults(monkeypatch):
    """Veto enabled with documented defaults — covers the live config path."""
    monkeypatch.setattr(
        config, "SIGNALS_PATTERN_CALIBRATION_AUTO_DISAGREEMENT_THRESHOLD", 0.15
    )
    monkeypatch.setattr(
        config, "SIGNALS_PATTERN_CALIBRATION_AUTO_PNL_SOFT_MIN_SAMPLES", 8
    )


def test_auto_vetoed_pairs_drops_inflated_touch(_enabled, _veto_defaults):
    # The caveat case: touch n=40 gives 0.850 (inflated), option_pnl n=10
    # (below the hard gate of 20) gives 0.30 — a 0.55 disagreement. Veto.
    touch_rows = [_row("p", "SPX", 1, 40, 0.85)]
    pnl_rows = [_row("p", "SPX", 1, 10, 0.30)]
    assert cal.auto_vetoed_pairs(touch_rows, pnl_rows) == {("p", "SPX")}


def test_auto_vetoed_pairs_respects_soft_min(_enabled, _veto_defaults):
    # Same disagreement, but pnl n=3 — below the soft minimum, single trade
    # would have outsized weight, so no veto.
    touch_rows = [_row("p", "SPX", 1, 40, 0.85)]
    pnl_rows = [_row("p", "SPX", 1, 3, 0.30)]
    assert cal.auto_vetoed_pairs(touch_rows, pnl_rows) == set()


def test_auto_vetoed_pairs_is_one_directional(_enabled, _veto_defaults):
    # Touch UNDER-rates the pattern relative to pnl — the safe direction
    # (lower confidence). No veto; touch base stays.
    touch_rows = [_row("p", "SPY", 1, 40, 0.40)]
    pnl_rows = [_row("p", "SPY", 1, 10, 0.80)]
    assert cal.auto_vetoed_pairs(touch_rows, pnl_rows) == set()


def test_auto_vetoed_pairs_respects_threshold(_enabled, _veto_defaults):
    # Disagreement under the threshold (post-clamp) — no veto. Touch 0.55
    # vs pnl 0.45 = 0.10 < 0.15 threshold.
    touch_rows = [_row("p", "SPY", 1, 40, 0.55)]
    pnl_rows = [_row("p", "SPY", 1, 10, 0.45)]
    assert cal.auto_vetoed_pairs(touch_rows, pnl_rows) == set()


def test_auto_vetoed_pairs_skips_stale_pnl(_enabled, _veto_defaults):
    # A six-month-old pnl row shouldn't be allowed to veto a fresh touch reading
    # — its window is stale by the same MAX_AGE_DAYS gate the store uses.
    touch_rows = [_row("p", "SPY", 1, 40, 0.85)]
    pnl_rows = [_row("p", "SPY", 200, 10, 0.30)]
    assert cal.auto_vetoed_pairs(touch_rows, pnl_rows) == set()


def test_auto_vetoed_pairs_disabled_when_threshold_zero(_enabled, monkeypatch):
    monkeypatch.setattr(
        config, "SIGNALS_PATTERN_CALIBRATION_AUTO_DISAGREEMENT_THRESHOLD", 0.0
    )
    monkeypatch.setattr(
        config, "SIGNALS_PATTERN_CALIBRATION_AUTO_PNL_SOFT_MIN_SAMPLES", 8
    )
    touch_rows = [_row("p", "SPY", 1, 40, 0.85)]
    pnl_rows = [_row("p", "SPY", 1, 50, 0.30)]
    assert cal.auto_vetoed_pairs(touch_rows, pnl_rows) == set()


def test_auto_vetoed_pairs_disabled_when_soft_min_zero(_enabled, monkeypatch):
    monkeypatch.setattr(
        config, "SIGNALS_PATTERN_CALIBRATION_AUTO_DISAGREEMENT_THRESHOLD", 0.15
    )
    monkeypatch.setattr(
        config, "SIGNALS_PATTERN_CALIBRATION_AUTO_PNL_SOFT_MIN_SAMPLES", 0
    )
    touch_rows = [_row("p", "SPY", 1, 40, 0.85)]
    pnl_rows = [_row("p", "SPY", 1, 50, 0.30)]
    assert cal.auto_vetoed_pairs(touch_rows, pnl_rows) == set()


def test_build_store_honors_exclude_pairs(_enabled):
    # Vetoed pair must drop out of both by_pair AND the by_pattern weighted mean
    # — otherwise the touch overshoot leaks through the pattern-wide fallback.
    rows = [
        _row("p", "SPY", 1, 40, 0.50),
        _row("p", "SPX", 1, 40, 0.85),
    ]
    full = cal.build_store_from_rows(rows, source="underlying_touch")
    assert full.by_pair[("p", "SPX")] == pytest.approx(0.85)
    excluded = cal.build_store_from_rows(
        rows, source="underlying_touch", exclude_pairs={("p", "SPX")}
    )
    assert ("p", "SPX") not in excluded.by_pair
    # by_pattern weighted mean now reflects only the non-vetoed SPY sample.
    assert excluded.by_pattern["p"] == pytest.approx(0.50)


def test_load_store_auto_vetoes_inflated_touch_pair(
    monkeypatch, _enabled, _veto_defaults
):
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_SOURCE", "auto")
    conn = _Conn(
        {
            # SPX: inflated touch (n=40) vs sub-gate pnl that strongly disagrees
            #      ⇒ vetoed. Live falls through to the pattern-wide / prior.
            # SPY: pnl is hard-gated and provides its own honest reading ⇒
            #      pnl wins on the merge as today.
            "underlying_touch": [
                _row("p", "SPX", 1, 40, 0.85),
                _row("p", "SPY", 1, 40, 0.62),
            ],
            "option_pnl": [
                _row("p", "SPX", 1, 10, 0.30),     # sub-gate disagreement
                _row("p", "SPY", 1, 40, 0.55),     # hard-gated
            ],
        }
    )
    store = cal.load_store(conn)
    # SPX touch base is gone; SPY pnl base remains.
    assert ("p", "SPX") not in store.by_pair
    assert store.by_pair[("p", "SPY")] == pytest.approx(0.55)
    # Merge prefers pnl by_pattern (the hard-gated SPY pnl row), so the
    # cross-underlying fallback reflects the honest pnl measurement, not the
    # vetoed touch overshoot. (build_store_from_rows already excluded SPX from
    # touch.by_pattern as well — verified by test_build_store_honors_exclude_pairs.)
    assert store.by_pattern["p"] == pytest.approx(0.55)


def test_compare_report_auto_veto_marker(_enabled, _veto_defaults):
    from src.tools.pattern_calibration_refresh import _compare_report

    today = date.today()
    out = _compare_report(
        {"p": 0.5},
        # touch n=40 at 0.85 (inflated)
        [("p", "SPX", today, 40, 0.85)],
        # sub-gate pnl at 0.30 — 0.55 disagreement, veto fires
        [("p", "SPX", today, 10, 0.30)],
        min_samples=20,
    )
    # Auto column shows 'veto→prior' (no pattern-wide fallback in this fixture).
    assert "veto" in out
    # The auto-veto footer summarizes which pairs were dropped.
    assert "p/SPX" in out
    assert "auto-veto" in out


def test_compare_report_pnl_wins_display_even_when_pair_vetoed(
    _enabled, _veto_defaults
):
    from src.tools.pattern_calibration_refresh import _compare_report

    today = date.today()
    # touch overshoots inflated; pnl is HARD-gated AND disagrees by a lot.
    # The veto fires (touch removed from by_pair), but pnl's pair entry wins
    # on the merge anyway, so the auto cell must show 'P' (the value live
    # actually returns) — NOT a misleading by_pattern fall-through 'v'.
    out = _compare_report(
        {"p": 0.5},
        [("p", "SPY", today, 100, 0.85)],
        [("p", "SPY", today, 60, 0.30)],     # n=60 ≥ MIN_SAMPLES=20
        min_samples=20,
    )
    assert "0.400 P" in out          # pnl 0.30 clamped to the global floor 0.40
    assert "0.400 v" not in out      # the veto did not fall through here
    # The footer still records that the veto fired (its by_pattern effect ran).
    assert "p/SPY" in out
    assert "auto-veto" in out


def test_compare_report_no_veto_marker_when_disabled(_enabled, monkeypatch):
    from src.tools.pattern_calibration_refresh import _compare_report

    monkeypatch.setattr(
        config, "SIGNALS_PATTERN_CALIBRATION_AUTO_DISAGREEMENT_THRESHOLD", 0.0
    )
    monkeypatch.setattr(
        config, "SIGNALS_PATTERN_CALIBRATION_AUTO_PNL_SOFT_MIN_SAMPLES", 8
    )
    today = date.today()
    out = _compare_report(
        {"p": 0.5},
        [("p", "SPX", today, 40, 0.85)],
        [("p", "SPX", today, 10, 0.30)],
        min_samples=20,
    )
    # With the veto off, the inflated touch base comes through unchanged.
    assert "0.850 T" in out
    # The legend line always mentions the v marker, but with no vetoes fired
    # the per-pair veto cell and the dropped-pairs footer must both be absent.
    assert "veto→prior" not in out
    assert "touch pair(s) dropped" not in out
