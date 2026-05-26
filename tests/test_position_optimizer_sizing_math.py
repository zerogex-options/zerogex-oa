"""Regression tests for ``_build_sizing_profiles``.

Pins down two CRITICAL audit findings:

1. **Stop-distance sizing**: per-contract risk for sizing is the
   configured stop distance (``|SIGNALS_INTRADAY_STOP_LOSS_PCT| × premium``),
   not full ``max_loss``. Previously the optimizer sized against
   ``max_loss``, silently producing positions ~4× smaller than the
   configured heat budget intended.

2. **Kelly-floor removal**: ``kelly_fraction`` is no longer floored at
   0.10 of the budget, so weak-edge candidates legitimately downsize to
   zero contracts instead of being force-sized at 10% of the heat
   budget regardless of edge.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from src.signals.position_optimizer_engine import PositionOptimizerEngine


def _engine() -> PositionOptimizerEngine:
    return PositionOptimizerEngine(underlying="SPY")


@dataclass
class _Cand:
    """Minimal SpreadCandidate substitute for sizing-only tests.

    SpreadCandidate is a frozen-style dataclass with many required
    fields; we duck-type the four fields ``_build_sizing_profiles``
    reads to keep the test focused.
    """

    max_loss: float
    entry_debit: float
    entry_credit: float
    kelly_fraction: float
    expected_value: float = 100.0  # positive so sizing is not edge-filtered


def _optimal(profiles):
    """Return the ``optimal`` SizingProfile from the engine's output list."""
    for p in profiles:
        if p.profile == "optimal":
            return p
    raise AssertionError("no optimal sizing profile in result")


# ---------------------------------------------------------------------------
# Stop-distance sizing
# ---------------------------------------------------------------------------


def test_long_debit_uses_stop_distance_not_max_loss(monkeypatch):
    """Stop at -25% of premium → effective risk = 0.25 × premium.

    Premium $500, max_loss $500 (full debit at risk in the worst case),
    but the stop fires at $375 → real per-contract risk = $125.

    With the default $1M portfolio and 2% optimal heat budget = $20k,
    sizing produces $20k ÷ $125 = 160 contracts. Under the pre-fix
    max_loss-based math the same heat budget would have sized just
    $20k ÷ $500 = 40 contracts — exactly the documented 4× shortfall.
    """
    monkeypatch.setattr(
        "src.signals.position_optimizer_engine.SIGNALS_INTRADAY_STOP_LOSS_PCT", -0.25
    )
    cand = _Cand(max_loss=500.0, entry_debit=500.0, entry_credit=0.0, kelly_fraction=1.0)
    profiles = _engine()._build_sizing_profiles(cand)
    opt = _optimal(profiles)
    assert opt.contracts == 160
    assert opt.max_risk_dollars == pytest.approx(160 * 125.0)


def test_credit_spread_uses_stop_distance_on_credit_premium(monkeypatch):
    """For a credit spread the entry premium is the credit collected.

    Credit $100, max_loss $400 (width minus credit), stop fires at
    cost-to-close = credit × (1 + stop_pct) → realized loss per contract
    = $25. Sizing should reflect $25, not $400.

    $20k optimal budget ÷ $25 = 800 contracts. The portfolio heat-cap
    layer (downstream of sizing) is what catches catastrophic risk.
    """
    monkeypatch.setattr(
        "src.signals.position_optimizer_engine.SIGNALS_INTRADAY_STOP_LOSS_PCT", -0.25
    )
    cand = _Cand(max_loss=400.0, entry_debit=0.0, entry_credit=100.0, kelly_fraction=1.0)
    profiles = _engine()._build_sizing_profiles(cand)
    opt = _optimal(profiles)
    assert opt.contracts == 800


def test_no_stop_falls_back_to_max_loss(monkeypatch):
    """When ``SIGNALS_INTRADAY_STOP_LOSS_PCT = 0`` (stops disabled),
    sizing falls back to ``max_loss`` so risk is never underestimated.

    $20k ÷ $500 = 40 contracts (the prior behavior, preserved as fallback).
    """
    monkeypatch.setattr("src.signals.position_optimizer_engine.SIGNALS_INTRADAY_STOP_LOSS_PCT", 0.0)
    cand = _Cand(max_loss=500.0, entry_debit=500.0, entry_credit=0.0, kelly_fraction=1.0)
    profiles = _engine()._build_sizing_profiles(cand)
    opt = _optimal(profiles)
    assert opt.contracts == 40


def test_zero_premium_falls_back_to_max_loss(monkeypatch):
    """Degenerate candidate with no entry premium falls back to max_loss
    so we never produce ``effective_risk == 0`` and unbounded sizing."""
    monkeypatch.setattr(
        "src.signals.position_optimizer_engine.SIGNALS_INTRADAY_STOP_LOSS_PCT", -0.25
    )
    cand = _Cand(max_loss=500.0, entry_debit=0.0, entry_credit=0.0, kelly_fraction=1.0)
    profiles = _engine()._build_sizing_profiles(cand)
    opt = _optimal(profiles)
    # Same as the no-stop fallback: $20k ÷ $500 = 40 contracts.
    assert opt.contracts == 40


# ---------------------------------------------------------------------------
# Kelly downsizing actually engages
# ---------------------------------------------------------------------------


def test_weak_kelly_now_downsizes_to_zero(monkeypatch):
    """kelly_fraction = 0.001 (0.1% edge) with $125 effective risk:
    kelly_adjusted_budget = $20k × 0.001 = $20, which is below $125 →
    zero contracts. Previously the ``max(kelly, 0.10)`` floor force-sized
    weak-edge candidates at 10% of the heat budget regardless of actual
    edge — the Kelly downsizer was effectively dead."""
    monkeypatch.setattr(
        "src.signals.position_optimizer_engine.SIGNALS_INTRADAY_STOP_LOSS_PCT", -0.25
    )
    cand = _Cand(max_loss=500.0, entry_debit=500.0, entry_credit=0.0, kelly_fraction=0.001)
    profiles = _engine()._build_sizing_profiles(cand)
    opt = _optimal(profiles)
    assert opt.contracts == 0


def test_full_kelly_takes_full_budget(monkeypatch):
    """Kelly = 1.0 uses the entire heat budget. Same as the
    stop-distance test above — verifies the no-floor branch."""
    monkeypatch.setattr(
        "src.signals.position_optimizer_engine.SIGNALS_INTRADAY_STOP_LOSS_PCT", -0.25
    )
    cand = _Cand(max_loss=500.0, entry_debit=500.0, entry_credit=0.0, kelly_fraction=1.0)
    profiles = _engine()._build_sizing_profiles(cand)
    opt = _optimal(profiles)
    assert opt.contracts == 160


def test_moderate_kelly_proportionally_sizes(monkeypatch):
    """Kelly = 0.50 → 50% of the heat budget."""
    monkeypatch.setattr(
        "src.signals.position_optimizer_engine.SIGNALS_INTRADAY_STOP_LOSS_PCT", -0.25
    )
    cand = _Cand(max_loss=500.0, entry_debit=500.0, entry_credit=0.0, kelly_fraction=0.50)
    profiles = _engine()._build_sizing_profiles(cand)
    opt = _optimal(profiles)
    # $20k × 0.50 = $10k ÷ $125 = 80 contracts.
    assert opt.contracts == 80


def test_negative_ev_still_rejected(monkeypatch):
    """Edge filter still wins over sizing math: expected_value <= 0
    forces contracts = 0 regardless of Kelly or stop distance."""
    monkeypatch.setattr(
        "src.signals.position_optimizer_engine.SIGNALS_INTRADAY_STOP_LOSS_PCT", -0.25
    )
    cand = _Cand(
        max_loss=500.0,
        entry_debit=500.0,
        entry_credit=0.0,
        kelly_fraction=1.0,
        expected_value=-50.0,
    )
    profiles = _engine()._build_sizing_profiles(cand)
    opt = _optimal(profiles)
    assert opt.contracts == 0
    assert opt.constrained_by == "edge filter"
