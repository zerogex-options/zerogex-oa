"""Reversion bots must not fade a strongly trending tape.

The live incident: on a gamma-positive but up-TRENDING day, the reversion bots
(max_pain / wall-bouncer / VWAP scalper) went net short and the puts got run
over (-50% to -70%). _trend_veto blocks a fade whose direction opposes a strong
recent move in snap.recent_closes (1-minute closes).
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


def _snap(closes) -> MarketSnapshot:
    return MarketSnapshot(
        underlying="SPY",
        timestamp=datetime(2026, 7, 31, 18, 0, tzinfo=timezone.utc),
        spot=closes[-1] if closes else 745.0,
        recent_closes=list(closes),
    )


_UP = [744.0 + i * 0.2 for i in range(12)]     # +~0.24% over the last 10 bars
_DOWN = [746.0 - i * 0.2 for i in range(12)]   # -~0.24%
_FLAT = [745.0 + (0.01 if i % 2 else -0.01) for i in range(12)]


def test_veto_bearish_fade_into_uptrend():
    assert _bot()._trend_veto(_snap(_UP), "bearish") is True


def test_veto_bullish_fade_into_downtrend():
    assert _bot()._trend_veto(_snap(_DOWN), "bullish") is True


def test_no_veto_bearish_with_the_downtrend():
    # A bearish fade in a down-move is WITH the trend — allowed.
    assert _bot()._trend_veto(_snap(_DOWN), "bearish") is False


def test_no_veto_bullish_with_the_uptrend():
    assert _bot()._trend_veto(_snap(_UP), "bullish") is False


def test_no_veto_when_flat():
    assert _bot()._trend_veto(_snap(_FLAT), "bearish") is False
    assert _bot()._trend_veto(_snap(_FLAT), "bullish") is False


def test_no_veto_with_insufficient_history():
    assert _bot()._trend_veto(_snap([745.0, 745.6, 746.2]), "bearish") is False


def test_disabled_when_pct_zero():
    assert _bot(params={"trend_veto_pct": 0.0})._trend_veto(_snap(_UP), "bearish") is False


def test_per_bot_threshold_override():
    # A 1% threshold is not met by the ~0.24% move, so no veto.
    assert _bot(params={"trend_veto_pct": 0.01})._trend_veto(_snap(_UP), "bearish") is False
