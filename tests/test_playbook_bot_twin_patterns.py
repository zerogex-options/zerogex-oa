"""Tests for the 5 new playbook patterns that give every TradeWorkz bot a
backtestable twin: max_pain_gravitation, dealer_delta_pressure, vwap_reversion,
vix_regime_breakout, opening_range_break.

Each: fires with the correct direction when its thesis holds, and stands down
when a key gate fails.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.dealer_delta_pressure import PATTERN as DEALER_DELTA
from src.signals.playbook.patterns.max_pain_gravitation import PATTERN as MAX_PAIN
from src.signals.playbook.patterns.opening_range_break import PATTERN as ORB
from src.signals.playbook.patterns.vix_regime_breakout import PATTERN as VIX_BREAKOUT
from src.signals.playbook.patterns.vwap_reversion import PATTERN as VWAP_REV

# 18:30 UTC = 14:30 ET (EDT); 14:30 UTC = 10:30 ET.
TS_AFTERNOON = datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc)
TS_MID_MORNING = datetime(2026, 5, 1, 14, 30, tzinfo=timezone.utc)


def _ctx(
    *,
    timestamp: datetime = TS_AFTERNOON,
    regime: str = "chop_range",
    close: float = 678.4,
    net_gex: float = 7.1e9,
    max_pain: Optional[float] = 675.0,
    vwap: Optional[float] = None,
    recent_highs: Optional[list] = None,
    recent_lows: Optional[list] = None,
    dealer_delta_score: Optional[float] = None,
    vix_level: Optional[float] = None,
) -> PlaybookContext:
    extra = {}
    if vix_level is not None:
        extra["vix_level"] = vix_level
    market = MarketContext(
        timestamp=timestamp,
        underlying="SPY",
        close=close,
        net_gex=net_gex,
        gamma_flip=676.5,
        put_call_ratio=0.9,
        max_pain=max_pain,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[close],
        iv_rank=None,
        vwap=vwap,
        recent_highs=recent_highs or [],
        recent_lows=recent_lows or [],
        extra=extra,
    )
    basic = {}
    if dealer_delta_score is not None:
        basic["dealer_delta_pressure"] = SignalSnapshot(
            name="dealer_delta_pressure",
            score=dealer_delta_score,
            clamped_score=dealer_delta_score / 100.0,
            triggered=True,
        )
    return PlaybookContext(
        market=market,
        msi_score=0.0,
        msi_regime=regime,
        msi_components={},
        advanced_signals={},
        basic_signals=basic,
        levels={"max_pain": max_pain},
        open_positions=[],
        recently_emitted={},
    )


# ---- max_pain_gravitation ------------------------------------------------


def test_max_pain_fades_toward_max_pain():
    # Spot 678.4 sits 0.5% above max pain 675 in long gamma → fade down (puts).
    card = MAX_PAIN.match(_ctx(close=678.4, max_pain=675.0, net_gex=5e9))
    assert card is not None
    assert card.direction == "bearish"
    assert card.target.ref_price == 675.0


def test_max_pain_stands_down_in_short_gamma():
    assert MAX_PAIN.match(_ctx(close=678.4, max_pain=675.0, net_gex=-5e9)) is None


def test_max_pain_stands_down_when_not_displaced():
    assert MAX_PAIN.match(_ctx(close=675.3, max_pain=675.0, net_gex=5e9)) is None


# ---- dealer_delta_pressure ----------------------------------------------


def test_dealer_delta_rides_the_lean():
    card = DEALER_DELTA.match(_ctx(net_gex=-5e9, dealer_delta_score=45.0, regime="trend_expansion"))
    assert card is not None
    assert card.direction == "bullish"  # positive score ⇒ bullish lean


def test_dealer_delta_stands_down_in_long_gamma():
    assert DEALER_DELTA.match(_ctx(net_gex=5e9, dealer_delta_score=45.0)) is None


def test_dealer_delta_stands_down_when_weak():
    assert DEALER_DELTA.match(_ctx(net_gex=-5e9, dealer_delta_score=10.0)) is None


# ---- vwap_reversion ------------------------------------------------------


def test_vwap_reversion_fades_stretch():
    # Spot 675.5 is 0.37% below VWAP 678 in long gamma, midday → revert up (calls).
    card = VWAP_REV.match(_ctx(close=675.5, vwap=678.0, net_gex=5e9))
    assert card is not None
    assert card.direction == "bullish"
    assert card.target.ref_price == 678.0


def test_vwap_reversion_stands_down_outside_window():
    # 19:45 UTC = 15:45 ET, past the 10:00–15:00 window → no trade even though
    # stretched in long gamma.
    late = datetime(2026, 5, 1, 19, 45, tzinfo=timezone.utc)
    assert VWAP_REV.match(_ctx(timestamp=late, close=675.5, vwap=678.0, net_gex=5e9)) is None


def test_vwap_reversion_stands_down_in_short_gamma():
    assert VWAP_REV.match(_ctx(close=675.5, vwap=678.0, net_gex=-5e9)) is None


def test_vwap_reversion_stands_down_when_not_stretched():
    assert VWAP_REV.match(_ctx(close=677.9, vwap=678.0, net_gex=5e9)) is None


# ---- vix_regime_breakout -------------------------------------------------


def test_vix_breakout_rides_the_break():
    highs = [677.0, 677.5, 678.0, 678.2, 678.1]
    lows = [676.0, 676.2, 676.1, 676.5, 676.3]
    card = VIX_BREAKOUT.match(
        _ctx(
            close=679.0,
            net_gex=-5e9,
            vix_level=22.0,
            recent_highs=highs,
            recent_lows=lows,
            regime="trend_expansion",
        )
    )
    assert card is not None
    assert card.direction == "bullish"  # broke above the recent high


def test_vix_breakout_stands_down_when_calm():
    highs = [677.0, 677.5, 678.0, 678.2, 678.1]
    lows = [676.0, 676.2, 676.1, 676.5, 676.3]
    assert (
        VIX_BREAKOUT.match(
            _ctx(close=679.0, net_gex=-5e9, vix_level=12.0, recent_highs=highs, recent_lows=lows)
        )
        is None
    )


# ---- opening_range_break -------------------------------------------------


def test_orb_trades_the_break_in_window():
    highs = [677.0, 677.5, 678.0, 678.2, 678.1]
    lows = [676.0, 676.2, 676.1, 676.5, 676.3]
    card = ORB.match(
        _ctx(
            timestamp=TS_MID_MORNING,
            close=679.0,
            recent_highs=highs,
            recent_lows=lows,
            regime="trend_expansion",
        )
    )
    assert card is not None
    assert card.direction == "bullish"


def test_orb_stands_down_outside_window():
    highs = [677.0, 677.5, 678.0, 678.2, 678.1]
    lows = [676.0, 676.2, 676.1, 676.5, 676.3]
    # Afternoon timestamp is outside the 10:00–11:30 ET window.
    assert (
        ORB.match(_ctx(timestamp=TS_AFTERNOON, close=679.0, recent_highs=highs, recent_lows=lows))
        is None
    )
