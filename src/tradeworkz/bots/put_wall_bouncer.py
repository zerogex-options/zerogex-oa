"""PutWallBouncer — fade a confirmed bounce off the put wall (bullish).

The bullish half of the split flagship wall strategy. Fires when spot is pressed
just above a historically large put wall in a positive-γ regime AND price has
tagged the wall and bounced (a confirmed defense, not a fresh break lower), AND
fresh order flow is not piercing down through it. Longs via a defined-risk bull
call vertical toward the mean (max_pain / flip). See
``src/tradeworkz/bots/wall_reversion.py`` for the shared engine and the
rationale for splitting the retired ``PutCallWallBouncer``.
"""

from __future__ import annotations

from typing import Optional

from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.bots.wall_reversion import wall_reversion_signal
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class PutWallBouncer(BaseBot):
    tier = "0DTE"
    direction_mode = "bullish"
    tagline = "Confirmed bounce off a big put wall. Fade the dip."
    description = (
        "Bullish half of the split wall strategy. Fades a CONFIRMED bounce at "
        "a historically large put wall in positive γ — price tagged the wall "
        "and turned up, flow is not piercing it — via a defined-risk bull call "
        "vertical toward max_pain / flip."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        return wall_reversion_signal(self, snap, "put")
