"""Size a Card's exits to the move price can make before its hold runs out.

In the graded record through 2026-09-24, three in four max_pain_gravitation
and vwap_reversion ideas, and most call_wall_fade and gamma_flip_bounce
ideas, "went nowhere": neither target nor stop was touched before the hold
ran out. Those patterns aimed at a structural level at whatever distance it
happened to sit (max pain, VWAP, a wall), put the stop a fixed percent away,
and fire only in long-gamma, pinned markets, which is exactly when price
moves least. gamma_flip_break, which already sizes its target to recent
volatility with a tight stop, almost never timed out.

This module measures how far price typically moves over the Card's usable
hold and sizes both exits to that:

* ``expected_move``: one standard deviation of price over the hold,
  close x sigma_1min x sqrt(minutes). Sigma comes from the last hour of
  1-minute closes with the most extreme 5% of minutes dropped, so one
  overnight gap or opening print can't inflate it.
* The target is the pattern's level when it sits within ``TARGET_MULT``
  expected moves; otherwise the Card aims that far toward the level.
* The stop sits ``STOP_MULT`` expected moves the other way, or at the
  pattern's own structural stop when that is closer, and never on the wrong
  side of the structure it protects (a wall, the gamma flip).
* A Card whose target would be under ``MIN_MOVE_PCT`` of price is not
  issued: the market is too quiet for the trade to beat its costs.

With both exits within three-quarters of a typical move, a driftless price
leaves that band before the hold ends about 86% of the time, against about
one Card in four resolving under the old exits.

With too little history to measure volatility (fewer than ``MIN_BARS``
closes), ``expected_move`` returns None and patterns keep their original
exits. Production always has two hours of bars; the fallback covers a data
gap and thin test fixtures.
"""

from __future__ import annotations

import math
import os
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

import pytz

from src.market_calendar import regular_session_close


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


# Target and stop distances, in expected moves over the usable hold.
TARGET_MULT = _env_float("PLAYBOOK_REACH_TARGET_MULT", 0.75)
STOP_MULT = _env_float("PLAYBOOK_REACH_STOP_MULT", 0.75)
# Smallest target worth a Card, as a fraction of price (0.10%).
MIN_MOVE_PCT = _env_float("PLAYBOOK_REACH_MIN_MOVE_PCT", 0.0010)
# Gap between a stop and the structure it sits beyond (0.05%).
STRUCTURE_BUFFER_PCT = _env_float("PLAYBOOK_REACH_STRUCTURE_BUFFER_PCT", 0.0005)
# Closes needed to measure volatility, and how many to use (an hour).
MIN_BARS = 21
SIGMA_BARS = 61

_ET = pytz.timezone("America/New_York")


def minute_sigma(closes: Optional[Sequence[float]], bars: int = SIGMA_BARS) -> Optional[float]:
    """Standard deviation of 1-minute returns over the last ``bars`` closes,
    or None with fewer than ``MIN_BARS``.

    The most extreme 5% of returns (at least one) are dropped first: an
    overnight gap or a stale opening print is one return that would otherwise
    dominate an hour of minutes. A median-based estimate would be more robust
    still, but collapses toward zero when prices step evenly.
    """
    usable = [float(c) for c in (closes or ()) if c and c > 0][-bars:]
    if len(usable) < MIN_BARS:
        return None
    rets = [usable[i] / usable[i - 1] - 1.0 for i in range(1, len(usable))]
    med = statistics.median(rets)
    trim = max(1, len(rets) // 20)
    kept = sorted(rets, key=lambda r: abs(r - med))[: len(rets) - trim]
    return statistics.pstdev(kept)


def expected_move(
    close: float, closes: Optional[Sequence[float]], minutes: float
) -> Optional[float]:
    """One standard deviation of price over ``minutes``, in price units.

    None when there isn't enough history to measure volatility.
    """
    sigma = minute_sigma(closes)
    if sigma is None:
        return None
    if close <= 0 or minutes <= 0:
        return 0.0
    return close * sigma * math.sqrt(minutes)


def minutes_to_close(ts: datetime) -> int:
    """Minutes from ``ts`` to that day's regular close (13:00 on a half day)."""
    if ts.tzinfo is None:
        ts = pytz.UTC.localize(ts)
    et = ts.astimezone(_ET)
    close = _ET.localize(datetime.combine(et.date(), regular_session_close(et.date())))
    return int((close - et).total_seconds() // 60)


def usable_hold(ts: datetime, hold_minutes: int, tier: str) -> int:
    """The hold a Card can actually use: a 0DTE Card's options expire at the close."""
    if tier != "0DTE":
        return int(hold_minutes)
    return max(0, min(int(hold_minutes), minutes_to_close(ts)))


@dataclass(frozen=True)
class Exits:
    target: float
    target_name: str
    stop: float
    move: float  # the expected move the exits were sized to

    def move_pct(self, price: float) -> float:
        return self.move / price * 100.0 if price > 0 else 0.0


def size_exits(
    *,
    direction: str,
    entry: float,
    move: float,
    level: Optional[float],
    level_name: Optional[str],
    structural_stop: Optional[float] = None,
    beyond: Optional[float] = None,
) -> Optional[Exits]:
    """Target and stop for a Card entering at ``entry``, or None when the
    target would be too small to trade.

    ``level`` is what the pattern aims at (max pain, VWAP, a wall); None aims
    at the move itself. ``structural_stop`` is the pattern's own stop, used
    when it is closer than ``STOP_MULT`` moves. ``beyond`` is a structure the
    stop must stay on the far side of: for a bullish Card the stop is kept
    below it, for a bearish Card above it, by ``STRUCTURE_BUFFER_PCT``.
    """
    if entry <= 0 or move < 0:
        return None
    sign = 1.0 if direction == "bullish" else -1.0
    reach = TARGET_MULT * move
    level_distance = sign * (level - entry) if level is not None else None
    if level_distance is not None and 0 < level_distance <= reach:
        target, target_name = float(level), str(level_name or "level")
    else:
        target = entry + sign * reach
        if level_distance is not None and level_distance > 0 and level_name:
            target_name = f"toward_{level_name}"
        else:
            target_name = "expected_move"
    if sign * (target - entry) < MIN_MOVE_PCT * entry:
        return None

    stop_distance = STOP_MULT * move
    if structural_stop is not None:
        structural_distance = sign * (entry - structural_stop)
        if 0 < structural_distance < stop_distance:
            stop_distance = structural_distance
    stop = entry - sign * stop_distance
    if beyond is not None:
        edge = beyond * (1.0 - sign * STRUCTURE_BUFFER_PCT)
        stop = min(stop, edge) if sign > 0 else max(stop, edge)
    if sign * (entry - stop) <= 0:
        return None
    return Exits(target=target, target_name=target_name, stop=stop, move=move)


@dataclass(frozen=True)
class Sizing:
    """A pattern's exits for one Card, or why it has none.

    ``move`` None: too little bar history; the pattern keeps its original
    exits. ``exits`` None with a ``move``: too quiet to trade; no Card.
    """

    hold: int
    move: Optional[float]
    exits: Optional[Exits]

    @property
    def measured(self) -> bool:
        return self.move is not None

    @property
    def too_quiet(self) -> bool:
        return self.move is not None and self.exits is None


def plan(
    *,
    ts: datetime,
    close: float,
    closes: Optional[Sequence[float]],
    hold_minutes: int,
    tier: str,
    direction: str,
    entry: float,
    level: Optional[float],
    level_name: Optional[str],
    structural_stop: Optional[float] = None,
    beyond: Optional[float] = None,
) -> Sizing:
    """Usable hold, expected move and exits for a Card, in one call."""
    hold = usable_hold(ts, hold_minutes, tier)
    move = expected_move(close, closes, hold)
    if move is None:
        return Sizing(hold=hold, move=None, exits=None)
    exits = size_exits(
        direction=direction,
        entry=entry,
        move=move,
        level=level,
        level_name=level_name,
        structural_stop=structural_stop,
        beyond=beyond,
    )
    return Sizing(hold=hold, move=move, exits=exits)


def too_quiet_reason(close: float, move: Optional[float], hold: int) -> str:
    """Near-miss text for a Card the market is too quiet to support."""
    pct = (move or 0.0) / close * 100.0 if close > 0 else 0.0
    return (
        f"too quiet: price typically moves {pct:.2f}% in {hold}m, so a target "
        f"within reach would be under {MIN_MOVE_PCT * 100:.2f}%"
    )
