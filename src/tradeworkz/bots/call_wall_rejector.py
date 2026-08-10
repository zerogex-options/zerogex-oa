"""CallWallRejector — fade a confirmed rejection at the call wall (bearish).

The bearish half of the split flagship wall strategy. Fires when spot is pressed
just under a historically large call wall in a positive-γ regime AND price has
tagged the wall and rolled back (a confirmed rejection, not a fresh push), AND
fresh order flow is not piercing up through it. Shorts via a defined-risk bear
put vertical toward the mean (max_pain / flip). See
``src/tradeworkz/bots/wall_reversion.py`` for the shared engine and the
rationale for splitting the retired ``PutCallWallBouncer``.
"""

from __future__ import annotations

from typing import Optional

from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.bots.wall_reversion import wall_credit_exit, wall_reversion_signal
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, OpenPosition, TradeSignal


class CallWallRejector(BaseBot):
    tier = "0DTE"
    direction_mode = "bearish"
    tagline = "Confirmed rejection at a big call wall. Fade the rally."
    description = (
        "Bearish half of the split wall strategy. Fades a CONFIRMED rejection "
        "at a historically large call wall in positive γ — price tagged the "
        "wall and rolled back, flow is not piercing it. Structure is "
        "param-selected (debit vertical / single debit / credit spread)."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        return wall_reversion_signal(self, snap, "call")

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
        dec = wall_credit_exit(self, snap, position)
        if dec is not None:
            return dec
        return super().exit_criteria(snap, position)
