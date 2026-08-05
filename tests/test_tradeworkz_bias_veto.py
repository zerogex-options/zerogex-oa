"""No bot may trade against the fleet's confident fused directional bias.

The Signals Engine persists a bias (bias_code + long/short/neutral + confidence)
to trade_bias_scores; context.build_snapshot reads it into snap.trade_bias_code
/ trade_bias_trend / trade_bias_confidence (preferring the intraday tenor). The
engine calls _bias_veto on every signal and drops one that opposes a confident
DIRECTIONAL read. RANGE_FADE / WAIT never veto — they carry a nominal long/short
that is not a directional call. The intraday RTH confidence distribution runs
BUY_DIPS / SELL_RIPS medians ~36-40, FADE_* medians ~24-26; the default floor
is 30 (BIAS_VETO_MIN_CONFIDENCE).
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec


def _bot(params=None) -> BaseBot:
    spec = BotSpec(
        id="t", display_name="T", strategy_class="BaseBot", tier="0DTE",
        direction_mode="context", universe="SPY", tagline="", description="",
        params=params or {},
    )
    return BaseBot(spec)


def _snap(code, trend, conf) -> MarketSnapshot:
    return MarketSnapshot(
        underlying="SPY", timestamp=datetime(2026, 7, 31, 18, 0, tzinfo=timezone.utc),
        spot=745.0, trade_bias_code=code, trade_bias_trend=trend, trade_bias_confidence=conf,
    )


def test_veto_bullish_against_confident_bearish_bias():
    assert _bot()._bias_veto(_snap("SELL_RIPS", "bearish", 60.0), "bullish") is True


def test_veto_bearish_against_confident_bullish_bias():
    # The incident: a bearish fade on a day the bias called BUY_DIPS / long.
    assert _bot()._bias_veto(_snap("BUY_DIPS", "bullish", 60.0), "bearish") is True


def test_no_veto_when_aligned_with_bias():
    assert _bot()._bias_veto(_snap("BUY_DIPS", "bullish", 90.0), "bullish") is False
    assert _bot()._bias_veto(_snap("SELL_RIPS", "bearish", 90.0), "bearish") is False


def test_range_fade_never_vetoes_even_with_nominal_direction():
    # RANGE_FADE carries a nominal short/long at tiny conviction — not a
    # directional call. Even at high confidence it must not veto.
    assert _bot()._bias_veto(_snap("RANGE_FADE", "bearish", 95.0), "bullish") is False
    assert _bot()._bias_veto(_snap("WAIT", "bullish", 95.0), "bearish") is False


def test_no_veto_when_bias_neutral_trend():
    assert _bot()._bias_veto(_snap("BUY_DIPS", "neutral", 90.0), "bearish") is False


def test_no_veto_when_bias_unavailable():
    assert _bot()._bias_veto(_snap(None, None, None), "bearish") is False


def test_no_veto_below_confidence_floor():
    # Opposing BUY_DIPS but only 2 confidence (< 30 default) -> don't veto.
    assert _bot()._bias_veto(_snap("BUY_DIPS", "bullish", 2.0), "bearish") is False


def test_disabled_per_bot():
    assert _bot(params={"bias_veto_enabled": False})._bias_veto(
        _snap("BUY_DIPS", "bullish", 90.0), "bearish") is False


def test_neutral_signal_direction_never_vetoed():
    assert _bot()._bias_veto(_snap("BUY_DIPS", "bullish", 90.0), "neutral") is False
