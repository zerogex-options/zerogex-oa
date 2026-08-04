"""No bot may trade against the fleet's confident fused directional bias.

The Signals Engine persists a directional bias (long/short/neutral + confidence)
to trade_bias_scores; context.build_snapshot reads it into
snap.trade_bias_trend / trade_bias_confidence. The engine calls _bias_veto on
every signal and drops one that opposes a confident bias — the live incident was
reversion bots shorting a day the bias called long.
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


def _snap(trend, conf) -> MarketSnapshot:
    return MarketSnapshot(
        underlying="SPY", timestamp=datetime(2026, 7, 31, 18, 0, tzinfo=timezone.utc),
        spot=745.0, trade_bias_trend=trend, trade_bias_confidence=conf,
    )


def test_veto_bullish_signal_against_confident_bearish_bias():
    assert _bot()._bias_veto(_snap("bearish", 70.0), "bullish") is True


def test_veto_bearish_signal_against_confident_bullish_bias():
    # The incident: a bearish fade on a day the bias called long/buy-dips.
    assert _bot()._bias_veto(_snap("bullish", 70.0), "bearish") is True


def test_no_veto_when_aligned_with_bias():
    assert _bot()._bias_veto(_snap("bullish", 90.0), "bullish") is False
    assert _bot()._bias_veto(_snap("bearish", 90.0), "bearish") is False


def test_no_veto_when_bias_neutral():
    assert _bot()._bias_veto(_snap("neutral", 90.0), "bearish") is False


def test_no_veto_when_bias_unavailable():
    assert _bot()._bias_veto(_snap(None, None), "bearish") is False


def test_no_veto_below_confidence_floor():
    # Opposing but only 30 confidence (< 50 default) -> don't veto.
    assert _bot()._bias_veto(_snap("bullish", 30.0), "bearish") is False


def test_disabled_per_bot():
    assert _bot(params={"bias_veto_enabled": False})._bias_veto(
        _snap("bullish", 90.0), "bearish") is False


def test_neutral_signal_direction_never_vetoed():
    assert _bot()._bias_veto(_snap("bullish", 90.0), "neutral") is False
