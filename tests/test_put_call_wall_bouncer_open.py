"""Entry-logic tests for PutCallWallBouncer.open_criteria.

The Bouncer fades the nearest call/put wall in a positive-gamma regime.
Until now its ``open_criteria`` had no direct coverage — only the shared
exit/premium-stop paths were tested. These lock in:

* the bearish call-wall fade and the symmetric bullish put-wall bounce fire
  with the right structure, target, stop, and wall reference;
* the regime / proximity / conviction gates still reject the setups they
  should (negative gamma, price too far from the wall, weak gamma);
* the late-session conviction dead-zone is gone — a strong-gamma wall touch
  in the final hour of the session now opens where it used to be
  mathematically unable to (regression for the ``_quality`` session floor).

All timestamps are ET→UTC for July (EDT, UTC-4): 11:30 ET == 15:30 UTC.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.tradeworkz.bots.put_call_wall_bouncer import (
    _SESSION_SCORE_FLOOR,
    PutCallWallBouncer,
)
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec

# 11:30 ET (midday) and 15:30 ET (final hour) as UTC instants.
MIDDAY = datetime(2026, 7, 14, 15, 30, tzinfo=timezone.utc)  # mins_since_open = 120
LATE = datetime(2026, 7, 14, 19, 30, tzinfo=timezone.utc)  # mins_since_open = 360

STRONG_GEX = 3.0e9  # gex_score saturates at 1.0
WEAK_GEX = 4.0e8  # positive regime, but not enough conviction


def _bot(**params) -> PutCallWallBouncer:
    spec = BotSpec(
        id="put_call_wall_bouncer",
        display_name="Put/Call Wall Bouncer",
        strategy_class="PutCallWallBouncer",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params={
            "wall_proximity_pct": 0.002,
            "wall_break_pct": 0.003,
            "max_hold_minutes": 90,
            **params,
        },
    )
    # ml_state=None -> confidence_base and threshold default to 0.55.
    return PutCallWallBouncer(spec, ml_state=None)


def _snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MIDDAY,
        spot=755.0,
        call_wall=755.0,
        put_wall=750.0,
        gamma_flip=749.0,
        max_pain=752.0,
        net_gex=STRONG_GEX,
    )
    base.update(over)
    return MarketSnapshot(**base)


# ----------------------------------------------------------------------
# Firing setups
# ----------------------------------------------------------------------


def test_bearish_call_wall_fade_fires():
    """Spot pinned to the call wall in strong positive gamma -> short via put."""
    sig = _bot().open_criteria(_snap(spot=755.0, call_wall=755.0))
    assert sig is not None
    assert sig.direction == "bearish"
    assert sig.strategy_type == "BUY_PUT_DEBIT"
    assert sig.legs[0].option_type == "put"
    assert sig.legs[0].strike == 755  # round(spot); SPY strikes are 1-wide
    assert sig.wall_ref_side == "call"
    assert sig.wall_ref_price == 755.0
    assert sig.target_price == 752.0  # max_pain, below spot
    # Wall-break stop is now dynamic (vol-scaled + confirmed) in the base
    # exit path, keyed off wall_ref_side — no static spot-level stop here.
    assert sig.stop_price is None
    assert sig.conviction >= _bot().confidence_threshold()


def test_bullish_put_wall_bounce_fires():
    """Spot pinned to the put wall in strong positive gamma -> long via call."""
    sig = _bot().open_criteria(_snap(spot=750.0, put_wall=750.0, call_wall=755.0, max_pain=753.0))
    assert sig is not None
    assert sig.direction == "bullish"
    assert sig.strategy_type == "BUY_CALL_DEBIT"
    assert sig.legs[0].option_type == "call"
    assert sig.wall_ref_side == "put"
    assert sig.wall_ref_price == 750.0
    assert sig.target_price == 753.0
    assert sig.stop_price is None  # dynamic vol-scaled wall-break stop instead


# ----------------------------------------------------------------------
# Symbol-relative gamma (percentile-driven quality)
# ----------------------------------------------------------------------


def test_quality_fires_on_high_pctile_small_absolute_gamma():
    """net_gex tiny by the absolute 3e9 scale, but strong for THIS symbol."""
    sig = _bot().open_criteria(
        _snap(spot=755.0, call_wall=755.0, net_gex=3.0e8, net_gex_pctile=92.0)
    )
    assert sig is not None
    assert sig.direction == "bearish"


def test_quality_declines_on_low_pctile_large_absolute_gamma():
    """net_gex huge (would saturate the old absolute score) but weak for its symbol."""
    sig = _bot().open_criteria(
        _snap(spot=755.0, call_wall=755.0, net_gex=2.0e10, net_gex_pctile=12.0)
    )
    assert sig is None


# ----------------------------------------------------------------------
# Regression: the late-session conviction dead-zone
# ----------------------------------------------------------------------


def test_late_session_strong_setup_still_fires():
    """Final-hour strong-gamma wall touch must open.

    Pre-fix: at 15:30 ET the session term decayed to 0, dragging conviction
    (0.52) under the 0.55 threshold — the Bouncer could not open at all in
    the last hour. With the session floor it clears the bar again.
    """
    sig = _bot().open_criteria(_snap(timestamp=LATE, spot=755.0, call_wall=755.0))
    assert sig is not None, "strong wall setup must still fire in the final hour"
    assert sig.direction == "bearish"


def test_quality_session_term_is_floored_not_zeroed():
    """The session component never falls below the floor, at any time of day."""
    bot = _bot()
    # net_gex=0 isolates the session term: quality == 0.5 * session_score.
    for ts in (LATE, datetime(2026, 7, 14, 20, 45, tzinfo=timezone.utc)):
        q = bot._quality(_snap(timestamp=ts, net_gex=0.0), "call")
        assert q == pytest.approx(0.5 * _SESSION_SCORE_FLOOR)


def test_strong_setup_clears_threshold_at_every_session_minute():
    """A maximal-gamma wall touch is never vetoed by time-of-day alone."""
    bot = _bot()
    open_utc = datetime(2026, 7, 14, 13, 30, tzinfo=timezone.utc)  # 09:30 ET
    for m in range(30, 389, 5):  # 10:00 ET .. 15:59 ET, every 5 min
        ts = open_utc + timedelta(minutes=m)
        snap = _snap(timestamp=ts, spot=755.0, call_wall=755.0, net_gex=STRONG_GEX)
        assert bot.open_criteria(snap) is not None, f"declined at +{m} min"


# ----------------------------------------------------------------------
# Gates that must still reject
# ----------------------------------------------------------------------


def test_no_trade_when_far_from_either_wall():
    """Spot outside the proximity band of both walls -> no signal."""
    # spot 752: 0.40% from the 755 call wall, 0.93% from the 745 put wall.
    sig = _bot().open_criteria(_snap(spot=752.0, call_wall=755.0, put_wall=745.0))
    assert sig is None


def test_no_trade_in_negative_gamma_regime():
    """Wall touch but net_gex negative -> regime gate rejects."""
    sig = _bot().open_criteria(_snap(spot=755.0, call_wall=755.0, net_gex=-8.0e8))
    assert sig is None


def test_no_trade_on_weak_gamma_setup():
    """Positive but weak gamma at the wall -> conviction below threshold.

    Confirms the session floor did not neuter the conviction gate: a thin
    wall is still declined.
    """
    sig = _bot().open_criteria(_snap(spot=755.0, call_wall=755.0, net_gex=WEAK_GEX))
    assert sig is None


def test_no_trade_in_opening_noise_window():
    """Before 30 minutes since the open the bot stays flat."""
    early = datetime(2026, 7, 14, 13, 45, tzinfo=timezone.utc)  # 09:45 ET, mins=15
    sig = _bot().open_criteria(_snap(timestamp=early, spot=755.0, call_wall=755.0))
    assert sig is None
