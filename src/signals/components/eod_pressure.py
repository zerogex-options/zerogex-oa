"""End-of-day pressure scoring component.

Predicts directional dealer flow in the final ~75 minutes of the US cash
session by combining three mechanical effects:

  * **Charm-at-spot**: sum of signed dealer charm exposure for strikes
    within a vol-scaled band around spot, weighted by expiry bucket so
    0DTE charm (which dominates into the close) counts most.
  * **Pin gravity**: (pin - spot) / spot, gated by the dealer gamma
    regime. In a positive-gamma regime dealers damp moves toward the
    heavy-OI strike; in a negative-gamma regime they amplify moves
    *away* from it, so the sign flips. max_pain is preferred when
    available (it's the dollar-weighted OI max-pain, more reliable than
    max_gamma for EOD pin mechanics).
  * **Calendar amplifier**: OpEx Fridays (3rd Friday of the month) and
    quad-witching days (3rd Friday of Mar/Jun/Sep/Dec) roughly double
    charm magnitude as dealers unwind expiring hedges.

The score is **gated off** before 14:30 ET (T-90min) — EOD dynamics do
not meaningfully drive tape earlier in the session.  Uses the ET-native
minute-of-day helper so the ramp is DST-correct year-round.

Sign convention (matches vanna_charm_flow):
  * Positive => bullish EOD pressure (dealer buying into close)
  * Negative => bearish EOD pressure (dealer selling into close)
"""
from __future__ import annotations

import math
import os
from datetime import datetime

from src.signals.independent.eod_pressure import (
    EODPressureSignal as EODPressureComponent,
    _CHARM_NORM,
)

__all__ = ["EODPressureComponent", "_CHARM_NORM"]
