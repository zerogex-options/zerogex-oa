"""Tests for the gex_gradient scoring component."""

from datetime import datetime, timezone

import pytest

from src.signals.components.base import MarketContext
from src.signals.basic.gex_gradient import GexGradientComponent


def _ctx(rows=None, net_gex=-1.4e9, **overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 14, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=net_gex,
        gamma_flip=500.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[500.0] * 5,
        iv_rank=None,
    )
    defaults.update(overrides)
    ctx = MarketContext(**defaults)
    if rows is not None:
        ctx.extra["gex_by_strike"] = rows
    return ctx


comp = GexGradientComponent()


def test_no_rows_is_neutral():
    assert comp.compute(_ctx()) == 0.0


def test_empty_rows_is_neutral():
    assert comp.compute(_ctx(rows=[])) == 0.0


def test_thin_rows_below_min_total_are_neutral():
    """If total notional gamma is below the minimum, abstain."""
    rows = [
        {"strike": 502.0, "net_gex": 1.0e6},
        {"strike": 498.0, "net_gex": 1.0e6},
    ]
    assert comp.compute(_ctx(rows=rows)) == 0.0


def test_above_heavy_in_negative_gex_is_bullish():
    """Dealers short above-spot gamma (negative net_gex) must buy into a rally."""
    rows = [
        {"strike": 502.0, "net_gex": 1.4e9},
        {"strike": 504.0, "net_gex": 7.0e8},
        {"strike": 498.0, "net_gex": 7.0e7},
    ]
    score = comp.compute(_ctx(rows=rows, net_gex=-2.1e9))
    assert score > 0


def test_below_heavy_in_negative_gex_is_bearish():
    rows = [
        {"strike": 498.0, "net_gex": 1.4e9},
        {"strike": 496.0, "net_gex": 7.0e8},
        {"strike": 502.0, "net_gex": 7.0e7},
    ]
    score = comp.compute(_ctx(rows=rows, net_gex=-2.1e9))
    assert score < 0


def test_sign_flips_with_dealer_regime():
    """Same asymmetry should score opposite in positive vs negative net_gex."""
    rows = [
        {"strike": 502.0, "net_gex": 1.4e9},
        {"strike": 504.0, "net_gex": 7.0e8},
        {"strike": 498.0, "net_gex": 7.0e7},
    ]
    neg = comp.compute(_ctx(rows=rows, net_gex=-2.1e9))
    pos = comp.compute(_ctx(rows=rows, net_gex=2.1e9))
    assert neg * pos < 0  # opposite sign
    assert abs(pos) < abs(neg)  # long-gamma side is intentionally damped


def test_score_bounded():
    rows = [{"strike": 550.0, "net_gex": 1.0e12}]
    score = comp.compute(_ctx(rows=rows, net_gex=-1.0e12))
    assert -1.0 <= score <= 1.0


def test_context_values_populated_when_available():
    rows = [
        {"strike": 502.0, "net_gex": 1.4e9},
        {"strike": 498.0, "net_gex": 7.0e8},
    ]
    cv = comp.context_values(_ctx(rows=rows))
    assert cv["source"] == "gex_by_strike"
    assert cv["above_spot_gamma_abs"] == pytest.approx(1.4e9)
    assert cv["below_spot_gamma_abs"] == pytest.approx(7.0e8)
    assert cv["strike_count"] == 2


def test_context_values_unavailable_returns_nones():
    cv = comp.context_values(_ctx())
    assert cv["source"] == "unavailable"
    assert cv["above_spot_gamma_abs"] is None


# ---------------------------------------------------------------------------
# Wing reach: "no wing gamma" vs "no wing data"
#
# Both produce wing_fraction == 0.0. Before the reach check they also produced
# the same confidence (1.0), so a chain truncated short of the wing window
# earned the FULL confidence of a genuinely wing-free book. These pin the two
# apart.
# ---------------------------------------------------------------------------

_WING_PCT = 0.04


def _rows_within(pct: float, spot: float = 500.0, gex: float = 1.4e9):
    """Two rows straddling spot, both strictly inside ``pct`` of it."""
    return [
        {"strike": spot * (1 + pct), "net_gex": gex},
        {"strike": spot * (1 - pct), "net_gex": gex * 0.5},
    ]


def test_reach_short_of_wing_window_is_flagged_unavailable():
    rows = _rows_within(_WING_PCT / 2)  # ±2%, well inside a ±4% wing window
    cv = comp.context_values(_ctx(rows=rows))
    assert cv["wing_data_available"] is False
    assert cv["wing_fraction"] == 0.0
    assert cv["wing_reach_pct"] == pytest.approx(_WING_PCT / 2, abs=1e-6)


def test_reach_past_wing_window_is_flagged_available():
    rows = _rows_within(_WING_PCT * 2)  # ±8%, clears the window on both sides
    cv = comp.context_values(_ctx(rows=rows))
    assert cv["wing_data_available"] is True
    assert cv["wing_reach_pct"] == pytest.approx(_WING_PCT * 2, abs=1e-6)


def test_unmeasured_wings_score_lower_than_measured_empty_wings():
    """The regression this guards.

    Same book shape either side; the only difference is whether any strike
    reached the wing window. Unmeasured must not score as confidently as
    measured-and-empty, or a truncated chain reads as a clean one.
    """
    spot = 500.0
    truncated = _rows_within(_WING_PCT / 2, spot=spot)
    # Measured and genuinely empty: strikes reach past the window, but the
    # gamma out there is negligible, so wing_fraction stays ~0.
    measured = _rows_within(_WING_PCT / 2, spot=spot) + [
        {"strike": spot * (1 + _WING_PCT * 2), "net_gex": 1.0},
        {"strike": spot * (1 - _WING_PCT * 2), "net_gex": 1.0},
    ]

    s_truncated = comp.compute(_ctx(rows=truncated, close=spot))
    s_measured = comp.compute(_ctx(rows=measured, close=spot))

    assert comp.context_values(_ctx(rows=truncated, close=spot))["wing_data_available"] is False
    assert comp.context_values(_ctx(rows=measured, close=spot))["wing_data_available"] is True
    assert abs(s_truncated) < abs(s_measured)


def test_heavy_wing_gamma_still_dampens_when_measured():
    """The pre-existing damping path must survive the change."""
    spot = 500.0
    rows = _rows_within(_WING_PCT / 2, spot=spot) + [
        {"strike": spot * (1 + _WING_PCT * 2), "net_gex": 5.0e9},
        {"strike": spot * (1 - _WING_PCT * 2), "net_gex": 5.0e9},
    ]
    cv = comp.context_values(_ctx(rows=rows, close=spot))
    assert cv["wing_data_available"] is True
    assert cv["wing_fraction"] > 0.5


def test_unavailable_context_carries_the_new_keys():
    cv = comp.context_values(_ctx())
    assert cv["wing_reach_pct"] is None
    assert cv["wing_data_available"] is None
