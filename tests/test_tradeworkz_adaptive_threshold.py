"""The adaptive confidence threshold must not lock the fleet out of trading.

recalibrate_bot gates entries with

    confidence_threshold = clamp(1 - win_30d + OFFSET, FLOOR, CEIL)

and a bot's conviction (base.compute_conviction) tops out near 0.76 even on a
flawless setup. An earlier hard-coded CEIL of 0.75 therefore sat at the very
top of the reachable conviction range: once a losing streak dragged every
bot's win_30d below ~0.55, the whole fleet pinned at 0.75 and nothing could
score high enough to open — 2.5 trading days of zero entries with the engine
ticking and bots enabled. These tests lock the fix: CEIL is configurable and
defaults to 0.60, which a clean setup clears while a weak one can't.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.tradeworkz import config as tw_config
from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.ml import recalibrate_bot
from src.tradeworkz.models import BotSpec


def _spec() -> BotSpec:
    return BotSpec(
        id="t",
        display_name="t",
        strategy_class="X",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params={},
    )


# ----------------------------------------------------------------------
# The unlock: a clean setup clears the new ceiling, not the old one
# ----------------------------------------------------------------------


def test_clean_setup_clears_new_ceiling_but_was_blocked_by_old():
    # base 0.40 (floored — a poorly-calibrated bot), a clean setup (quality
    # 0.9), no ML overlay (components=None): conviction = 0.4*0.40 + 0.6*0.9.
    bot = BaseBot(_spec(), ml_state={"confidence_base": 0.40})
    conviction = bot.compute_conviction(None, quality_score=0.9, components=None)
    assert conviction == pytest.approx(0.70)
    # Clears the new default ceiling -> the bot CAN open on a clean setup.
    assert conviction >= tw_config.ADAPTIVE_THRESHOLD_CEIL
    # But would have been rejected by the old 0.75 ceiling -> the lockout.
    assert conviction < 0.75


def test_default_ceiling_sits_below_the_reachable_conviction_ceiling():
    # A flawless setup on a floored-base bot tops out here; the gate must sit
    # strictly below it or nothing ever opens.
    bot = BaseBot(_spec(), ml_state={"confidence_base": tw_config.CALIBRATION_FLOOR})
    max_conviction = bot.compute_conviction(None, quality_score=1.0, components=None)
    assert tw_config.ADAPTIVE_THRESHOLD_CEIL < max_conviction


# ----------------------------------------------------------------------
# recalibrate_bot honors the configurable ceiling
# ----------------------------------------------------------------------


class _Cur:
    def __init__(self, trades):
        self._trades = trades
        self._pending = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self._pending = []
        self.rowcount = 0
        if "FROM tw_ml_state" in s:
            self._pending = []  # no prior state -> load_state default
        elif "FROM tw_trades" in s:
            self._pending = list(self._trades)
        elif "UPDATE tw_bots" in s:
            self.rowcount = 1

    def fetchone(self):
        return self._pending[0] if self._pending else None

    def fetchall(self):
        return list(self._pending)


class _Conn:
    def __init__(self, trades):
        self._c = _Cur(trades)

    def cursor(self):
        return self._c


def _losses(n):
    now = datetime.now(timezone.utc)
    return [("loss", -100.0, now - timedelta(hours=i + 1)) for i in range(n)]


def test_recalibrate_threshold_uses_configurable_ceiling(monkeypatch):
    monkeypatch.setattr(tw_config, "ADAPTIVE_THRESHOLD_CEIL", 0.55)
    # 25 straight losses -> win_30d 0.0 -> 1 - 0 + 0.30 = 1.30, clamped to CEIL.
    state = recalibrate_bot(_Conn(_losses(25)), "t")
    assert state.confidence_threshold == pytest.approx(0.55)


def test_recalibrate_threshold_default_ceiling_is_0_60():
    # Same all-loss bot, default config: pins at the 0.60 ceiling, not 0.75.
    state = recalibrate_bot(_Conn(_losses(25)), "t")
    assert state.confidence_threshold == pytest.approx(0.60)
