"""Entry-logic tests for PutWallMagnetReversal.open_criteria.

The negative-gamma mega-put-wall reversal bot — the risk-inverted sibling
of the Bouncer. These lock in:

* it fires ONLY in negative_weak gamma (never positive, never deep
  negative_strong) on a historically massive put wall approached from above;
* the "massive enough" gate (put_wall_strength_pctile) and the
  wall-collapse / proximity gates reject what they should;
* the structure is a defined-risk bull call debit spread with target = the
  gamma flip and NO static stop (the tight confirmed wall-break stop owns it);
* the inverted-risk defaults travel with the class, and the wall percentile
  drives sizing (not stop-widening).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.tradeworkz.bots.put_wall_magnet_reversal import PutWallMagnetReversal
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec

MIDDAY = datetime(2026, 7, 16, 15, 30, tzinfo=timezone.utc)  # 11:30 ET, m=120
LATE = datetime(2026, 7, 16, 19, 30, tzinfo=timezone.utc)  # 15:30 ET, m=360


def _bot(**params) -> PutWallMagnetReversal:
    spec = BotSpec(
        id="put_wall_magnet_reversal",
        display_name="Put Wall Magnet Reversal",
        strategy_class="PutWallMagnetReversal",
        tier="0DTE",
        direction_mode="bullish",
        universe="SPY",
        tagline="",
        description="",
        params=dict(params),
    )
    return PutWallMagnetReversal(spec, ml_state=None)


def _snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MIDDAY,
        spot=749.0,
        put_wall=748.0,
        prior_put_wall=748.0,
        call_wall=752.0,
        gamma_flip=751.75,  # above spot -> negative gamma structure
        max_pain=751.0,
        net_gex=-5.0e8,  # negative & shallow -> negative_weak (absolute fallback)
        put_wall_strength_pctile=95.0,
        put_wall_strength=2.0e9,
    )
    base.update(over)
    return MarketSnapshot(**base)


# ----------------------------------------------------------------------
# Fires on the intended setup
# ----------------------------------------------------------------------


def test_fires_on_massive_put_wall_in_negative_weak():
    sig = _bot().open_criteria(_snap())
    assert sig is not None
    assert sig.direction == "bullish"
    assert sig.strategy_type == "BULL_CALL_DEBIT_SPREAD"
    assert sig.wall_ref_side == "put"
    assert sig.wall_ref_price == 748.0
    assert sig.target_price == 751.75  # gamma flip
    assert sig.stop_price is None  # dynamic confirmed wall-break stop
    # Defined-risk spread: long ATM call + short higher call.
    assert len(sig.legs) == 2
    assert all(leg.option_type == "call" for leg in sig.legs)
    longs = [leg for leg in sig.legs if leg.side == "long"]
    shorts = [leg for leg in sig.legs if leg.side == "short"]
    assert len(longs) == 1 and len(shorts) == 1
    assert longs[0].strike < shorts[0].strike
    # Percentile drives sizing.
    assert sig.components_at_entry["wall_strength"] == pytest.approx(0.95)


def test_spread_width_is_capped():
    # A far-away flip must not create an unbounded-width spread.
    sig = _bot().open_criteria(_snap(gamma_flip=800.0))
    assert sig is not None
    longs = [leg for leg in sig.legs if leg.side == "long"][0]
    shorts = [leg for leg in sig.legs if leg.side == "short"][0]
    # max_spread_width_pct default 0.004 * 749 ~= 3 -> short within ~3 of long.
    assert shorts.strike - longs.strike <= 4.0


# ----------------------------------------------------------------------
# Regime gate — the inverse of the Bouncer
# ----------------------------------------------------------------------


def test_no_trade_in_positive_gamma():
    assert _bot().open_criteria(_snap(net_gex=1.0e9)) is None


def test_no_trade_in_deep_negative_strong():
    # net_gex < -1.5e9 -> negative_strong (too big a cascade to fade).
    assert _bot().open_criteria(_snap(net_gex=-2.0e9)) is None


def test_no_trade_when_regime_unknown():
    assert _bot().open_criteria(_snap(net_gex=None)) is None


# ----------------------------------------------------------------------
# "Massive enough" gate
# ----------------------------------------------------------------------


def test_no_trade_without_wall_strength_history():
    """No percentile (new column, no history) -> cannot confirm 'massive' -> skip."""
    assert _bot().open_criteria(_snap(put_wall_strength_pctile=None)) is None


def test_no_trade_on_ordinary_wall():
    assert _bot().open_criteria(_snap(put_wall_strength_pctile=70.0)) is None


def test_threshold_is_configurable():
    # An 85th-pct wall fires only if the operator lowers the gate.
    assert _bot().open_criteria(_snap(put_wall_strength_pctile=85.0)) is None
    assert (
        _bot(min_put_wall_pctile=80.0).open_criteria(_snap(put_wall_strength_pctile=85.0))
        is not None
    )


# ----------------------------------------------------------------------
# Proximity / approach-from-above / collapse gates
# ----------------------------------------------------------------------


def test_no_trade_when_spot_too_far_above_wall():
    # spot 752 vs wall 748 -> 0.53% > 0.2% proximity.
    assert _bot().open_criteria(_snap(spot=752.0)) is None


def test_no_trade_when_spot_below_wall():
    # Already broken below the wall -> not a bounce setup.
    assert _bot().open_criteria(_snap(spot=747.5, put_wall=748.0)) is None


def test_no_trade_when_wall_is_collapsing():
    # Put wall slid down from 752 -> 748 (support evaporating).
    assert _bot().open_criteria(_snap(prior_put_wall=752.0)) is None


def test_no_trade_in_opening_noise_window():
    early = datetime(2026, 7, 16, 13, 45, tzinfo=timezone.utc)  # 09:45 ET, m=15
    assert _bot().open_criteria(_snap(timestamp=early)) is None


# ----------------------------------------------------------------------
# Late-session magnet tilt (inverse of the Bouncer's earlier-is-better)
# ----------------------------------------------------------------------


def test_late_session_scores_higher_than_midday():
    bot = _bot()
    mid = bot.open_criteria(_snap(timestamp=MIDDAY))
    late = bot.open_criteria(_snap(timestamp=LATE))
    assert mid is not None and late is not None
    assert late.conviction > mid.conviction


# ----------------------------------------------------------------------
# Inverted-risk defaults travel with the class
# ----------------------------------------------------------------------


def test_risk_defaults_are_self_contained():
    p = _bot().params
    assert p["wall_break_min_pct"] == 0.0015  # tighter than the fade bot's 0.003
    assert p["wall_break_max_pct"] == 0.005
    assert p["wall_break_confirm_bars"] == 1  # fast, single-bar confirm
    assert p["wall_break_strength_widen"] == 0.0  # big wall does NOT loosen the stop
    assert p["max_premium_loss_pct"] == 0.25  # tighter dollar cap than 0.40


def test_stop_buffer_does_not_widen_with_wall_strength():
    """The whole point: a massive wall sizes UP but the stop stays tight."""
    bot = _bot()
    snap = _snap()
    small = bot._wall_break_buffer_pct(snap, 0.0)
    huge = bot._wall_break_buffer_pct(snap, 1.0)
    assert huge == pytest.approx(small)  # strength_widen=0 -> no loosening
    assert small <= 0.005  # capped tight
