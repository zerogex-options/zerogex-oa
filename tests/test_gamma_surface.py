"""The Gamma Surface model (src/analytics/gamma_surface.py).

The load-bearing check is the first one: the surface's first row IS the raw
(unweighted) spot-shift profile the engine already builds, priced from the
same legs, so the new surface cannot quietly drift from the GEX convention
every other chart uses.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import numpy as np
import pytest

from src.analytics.forced_flow import ContractLeg
from src.analytics.gamma_surface import (
    AUTO_SPAN_MAX,
    AUTO_SPAN_MIN,
    MIN_TTE_YEARS,
    auto_span_pct,
    compute_gamma_surface,
)
from src.analytics.main_engine import AnalyticsEngine

TS = datetime(2026, 10, 14, 15, 0, tzinfo=timezone.utc)  # 11:00 ET
TODAY = date(2026, 10, 14)
FRIDAY = date(2026, 10, 16)


def _opt(strike, otype, *, oi=1000, iv=0.20, exp=FRIDAY):
    return {
        "option_symbol": None,
        "strike": strike,
        "option_type": otype,
        "open_interest": oi,
        "implied_volatility": iv,
        "expiration": exp,
        "gamma": 0.0,
        "volume": 0,
    }


def _book():
    return [
        _opt(98.0, "P", oi=4000, iv=0.22, exp=TODAY),
        _opt(100.0, "C", oi=6000, iv=0.18, exp=TODAY),
        _opt(102.0, "C", oi=3000, iv=0.17, exp=TODAY),
        _opt(95.0, "P", oi=9000, iv=0.25, exp=FRIDAY),
        _opt(105.0, "C", oi=7000, iv=0.16, exp=FRIDAY),
    ]


def test_first_row_is_the_raw_spot_shift_profile():
    engine = AnalyticsEngine(underlying="SPY")
    spot = 100.0
    options = _book()
    legs = engine._build_forced_flow_legs(options, TS)
    assert len(legs) == len(options)

    # 95..105 in 0.25 steps: the grid the engine's profile builds for
    # span_pct=0.05 / step_pct=0.0025, so the two line up point for point.
    surface = compute_gamma_surface(
        legs,
        spot,
        engine.risk_free_rate,
        engine.dividend_yield,
        span_pct=0.05,
        price_points=41,
        time_steps=5,
    )
    profile = engine._gamma_exposure_profile(
        options, spot, TS, span_pct=0.05, step_pct=0.0025, apply_dte_weight=False
    )
    assert len(profile) == 41
    np.testing.assert_allclose(surface["grid"], [s for s, _ in profile], rtol=0, atol=1e-9)
    np.testing.assert_allclose(surface["profiles"][0], [v for _, v in profile], rtol=1e-9)


def test_time_axis_runs_to_the_first_settlement():
    legs = [
        ContractLeg(100.0, "C", 1000, 0.20, 4.0 / 24 / 365),  # settles in 4h
        ContractLeg(100.0, "P", 1000, 0.20, 52.0 / 24 / 365),
    ]
    surface = compute_gamma_surface(legs, 100.0, 0.04, 0.0, time_steps=9)
    times = surface["times_years"]
    assert len(times) == 9 and len(surface["profiles"]) == 9
    assert times[0] == 0.0
    assert times[-1] == pytest.approx(4.0 / 24 / 365)
    assert surface["settles_in_years"] == pytest.approx(4.0 / 24 / 365)
    assert all(a < b for a, b in zip(times, times[1:]))
    for row in surface["profiles"]:
        assert len(row) == len(surface["grid"])


def test_last_row_holds_the_settling_contract_at_the_floor():
    """At the settlement instant the same-day leg is priced at 30 minutes, not
    at zero (a spike no grid can sample) and not dropped."""
    same_day = ContractLeg(100.0, "C", 1000, 0.20, 2.0 / 24 / 365)
    alone = compute_gamma_surface([same_day], 100.0, 0.0, 0.0, span_pct=0.01, time_steps=3)
    held = compute_gamma_surface(
        [ContractLeg(100.0, "C", 1000, 0.20, MIN_TTE_YEARS)],
        100.0,
        0.0,
        0.0,
        span_pct=0.01,
        time_steps=2,
    )
    np.testing.assert_allclose(alone["profiles"][-1], held["profiles"][0], rtol=1e-12)


def test_same_day_gamma_sharpens_into_settlement():
    leg = ContractLeg(100.0, "C", 1000, 0.20, 4.0 / 24 / 365)
    surface = compute_gamma_surface([leg], 100.0, 0.0, 0.0, span_pct=0.02, time_steps=5)
    grid = np.array(surface["grid"])
    at_strike = int(np.argmin(np.abs(grid - 100.0)))
    peaks = [row[at_strike] for row in surface["profiles"]]
    assert all(a < b for a, b in zip(peaks, peaks[1:]))
    # sqrt(4h / 30min) ~ 2.83x taller at the floor than now.
    assert peaks[-1] / peaks[0] == pytest.approx(np.sqrt(8.0), rel=0.02)


def test_dealer_sign_convention_calls_positive_puts_negative():
    t = 3.0 / 365
    calls = compute_gamma_surface([ContractLeg(100.0, "C", 500, 0.2, t)], 100.0, 0.0, 0.0)
    puts = compute_gamma_surface([ContractLeg(100.0, "P", 500, 0.2, t)], 100.0, 0.0, 0.0)
    assert min(min(row) for row in calls["profiles"]) >= 0.0
    assert max(max(row) for row in puts["profiles"]) <= 0.0
    np.testing.assert_allclose(calls["profiles"], -np.array(puts["profiles"]), rtol=1e-12)


def test_nothing_to_price_returns_none():
    assert compute_gamma_surface([], 100.0, 0.0) is None
    assert compute_gamma_surface([ContractLeg(100.0, "C", 1, 0.2, 0.01)], 0.0, 0.0) is None
    unusable = [
        ContractLeg(100.0, "C", 0, 0.2, 0.01),  # no open interest
        ContractLeg(100.0, "C", 10, 0.0, 0.01),  # no implied vol
        ContractLeg(100.0, "C", 10, 0.2, 0.0),  # already settled
    ]
    assert compute_gamma_surface(unusable, 100.0, 0.0) is None


def test_auto_span_frames_the_strikes():
    assert auto_span_pct(100.0, [98.0, 103.0]) == pytest.approx(0.035)
    assert auto_span_pct(100.0, [100.0]) == AUTO_SPAN_MIN
    assert auto_span_pct(100.0, [40.0, 100.0]) == AUTO_SPAN_MAX
    surface = compute_gamma_surface(
        [ContractLeg(103.0, "C", 10, 0.2, 0.01), ContractLeg(98.0, "P", 10, 0.2, 0.01)],
        100.0,
        0.0,
    )
    assert surface["span_pct"] == pytest.approx(0.035)
    assert surface["grid"][0] == pytest.approx(96.5)
    assert surface["grid"][-1] == pytest.approx(103.5)
    # Odd point count: spot sits on the middle grid point.
    assert surface["grid"][len(surface["grid"]) // 2] == pytest.approx(100.0)
