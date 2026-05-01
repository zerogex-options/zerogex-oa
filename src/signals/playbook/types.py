"""Action Card and supporting value types for the Playbook Engine.

The Action Card is the unified output of a playbook cycle: a single
trade instruction (or STAND_DOWN) with explicit instrument, entry,
target, stop, sizing hint, confidence, and rationale.

See ``docs/playbook_catalog.md`` §2 for the schema this module mirrors.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class ActionEnum(str, Enum):
    BUY_CALL_DEBIT = "BUY_CALL_DEBIT"
    BUY_PUT_DEBIT = "BUY_PUT_DEBIT"
    BUY_CALL_SPREAD = "BUY_CALL_SPREAD"
    BUY_PUT_SPREAD = "BUY_PUT_SPREAD"
    SELL_CALL_SPREAD = "SELL_CALL_SPREAD"
    SELL_PUT_SPREAD = "SELL_PUT_SPREAD"
    BUY_IRON_CONDOR = "BUY_IRON_CONDOR"
    SELL_IRON_CONDOR = "SELL_IRON_CONDOR"
    BUY_BUTTERFLY = "BUY_BUTTERFLY"
    BUY_CALENDAR = "BUY_CALENDAR"
    BUY_DIAGONAL = "BUY_DIAGONAL"
    TAKE_PROFIT = "TAKE_PROFIT"
    TIGHTEN_STOP = "TIGHTEN_STOP"
    CLOSE = "CLOSE"
    STAND_DOWN = "STAND_DOWN"


# Tier ordering for conflict resolution (see engine.resolve_conflicts).
TIER_INTRADAY = ("0DTE", "1DTE", "swing")
TIER_END_OF_DAY = ("1DTE", "swing", "0DTE")
TIER_AFTER_CLOSE = ("swing", "1DTE", "0DTE")


@dataclass
class Leg:
    expiry: str  # ISO date "YYYY-MM-DD"
    strike: float
    right: str  # "C" | "P"
    side: str  # "BUY" | "SELL"
    qty: int = 1


@dataclass
class Entry:
    ref_price: float
    limit_premium: Optional[float] = None
    trigger: str = "at_market"  # at_market | at_touch | on_break | at_close | at_open_next


@dataclass
class Target:
    ref_price: Optional[float]
    exit_premium: Optional[float] = None
    kind: str = "level"  # level | premium_pct | time | signal_event
    level_name: Optional[str] = None


@dataclass
class Stop:
    ref_price: Optional[float]
    exit_premium: Optional[float] = None
    kind: str = "level"
    level_name: Optional[str] = None


@dataclass
class NearMiss:
    pattern: str
    missing: list[str] = field(default_factory=list)


@dataclass
class Alternative:
    pattern: str
    reason: str


@dataclass
class ActionCard:
    """Unified trade instruction emitted by the Playbook Engine."""

    underlying: str
    timestamp: datetime
    action: ActionEnum
    pattern: str
    tier: str  # "0DTE" | "1DTE" | "swing" | "n/a" (STAND_DOWN)
    direction: str  # "bullish" | "bearish" | "non_directional" | "context_dependent"
    confidence: float  # clamped [0.20, 0.95]; STAND_DOWN uses 0.0
    size_multiplier: float = 1.0
    max_hold_minutes: Optional[int] = None
    legs: list[Leg] = field(default_factory=list)
    entry: Optional[Entry] = None
    target: Optional[Target] = None
    stop: Optional[Stop] = None
    rationale: str = ""
    context: dict[str, Any] = field(default_factory=dict)
    alternatives_considered: list[Alternative] = field(default_factory=list)
    near_misses: list[NearMiss] = field(default_factory=list)  # populated only for STAND_DOWN

    def to_dict(self) -> dict[str, Any]:
        """JSON-serializable dict matching the API shape."""
        d = asdict(self)
        d["action"] = self.action.value
        d["timestamp"] = (
            self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else self.timestamp
        )
        if self.action == ActionEnum.STAND_DOWN:
            # STAND_DOWN cards drop the trade-instruction fields entirely.
            for key in ("legs", "entry", "target", "stop", "size_multiplier", "max_hold_minutes"):
                d.pop(key, None)
        else:
            # Trade cards drop near_misses (those are only meaningful for STAND_DOWN).
            d.pop("near_misses", None)
        return d


def clamp_confidence(value: float) -> float:
    """Clamp a confidence to the spec's [0.20, 0.95] range."""
    if value != value:  # NaN
        return 0.20
    return max(0.20, min(0.95, float(value)))
