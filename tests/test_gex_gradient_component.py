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
# The wing damper is only meaningful if the chain reaches the wing window
# ---------------------------------------------------------------------------


def _chain(close: float, half_width_pct: float, n: int = 21, gex: float = 3.0e8):
    """Strikes spread evenly across +/- half_width_pct of ``close``."""
    span = close * half_width_pct
    return [{"strike": close - span + (2 * span) * i / (n - 1), "net_gex": gex} for i in range(n)]


def test_a_chain_that_never_reaches_the_wings_says_so():
    """Production ingests +/-3% capped at 40 strikes and recalibrates about
    once a minute, so the widest SPX strike sits ~1.3% from spot and nothing
    can satisfy the >= 4% wing test. wing_fraction is then 0.0 because the
    bucket was UNREACHABLE, not because the wings were empty -- and read as
    a measurement it pins wing_confidence at its maximum, which is the
    overconfident direction.
    """
    ctx = _ctx(rows=_chain(500.0, 0.03))
    values = comp.context_values(ctx)

    assert values["wing_window_reached"] is False
    assert values["wing_fraction"] == 0.0
    assert values["max_strike_distance_pct"] == pytest.approx(0.03, abs=1e-3)


def test_a_wide_chain_measures_the_wings_for_real():
    ctx = _ctx(rows=_chain(500.0, 0.06))
    values = comp.context_values(ctx)

    assert values["wing_window_reached"] is True
    assert values["wing_fraction"] > 0.0, "strikes past 4% must land in the wing bucket"
    assert values["max_strike_distance_pct"] == pytest.approx(0.06, abs=1e-3)


def test_the_unreachable_case_is_logged_not_silently_scored(caplog):
    """A signal quietly stuck at full confidence is worse than one reporting
    low confidence: the first is invisible."""
    with caplog.at_level("WARNING", logger="src.signals.basic.gex_gradient"):
        comp.compute(_ctx(rows=_chain(500.0, 0.03)))
    messages = [r.getMessage() for r in caplog.records if "wing window" in r.getMessage()]
    assert messages, "an inert damper must be visible"
    assert "unreachable" in messages[0]
    assert "inert" in messages[0]

    caplog.clear()
    with caplog.at_level("WARNING", logger="src.signals.basic.gex_gradient"):
        comp.compute(_ctx(rows=_chain(500.0, 0.06)))
    assert not [r for r in caplog.records if "wing window" in r.getMessage()]


def test_the_damper_still_bites_when_the_wings_are_real():
    """Sanity: the mechanism works, it just never fires on production config."""
    narrow = comp.compute(_ctx(rows=_chain(500.0, 0.03)))
    wide = comp.compute(_ctx(rows=_chain(500.0, 0.06)))
    # Same symmetric chain either way, so asymmetry ~0 and both scores are
    # near zero -- what matters is that the wide chain populated the bucket.
    assert comp.context_values(_ctx(rows=_chain(500.0, 0.06)))["wing_fraction"] > 0
    assert comp.context_values(_ctx(rows=_chain(500.0, 0.03)))["wing_fraction"] == 0
    assert isinstance(narrow, float) and isinstance(wide, float)
