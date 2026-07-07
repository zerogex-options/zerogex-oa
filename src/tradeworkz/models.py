"""Domain dataclasses used across the TradeWorkz engine, bots, and API.

These are intentionally thin — a bot returns a ``TradeSignal`` (or ``None``),
the engine turns it into a row insert. Nothing here talks to the DB.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class Leg:
    """One leg of the option structure a bot is trading."""

    option_symbol: str
    side: str  # 'long' | 'short'
    option_type: str  # 'call' | 'put'
    strike: float
    expiration: str  # ISO date
    delta: Optional[float] = None


@dataclass
class TradeSignal:
    """A bot's decision to open (or add to) a position on the current tick.

    The engine consumes this, computes size against the bot's capital sleeve,
    fills the legs at the current bid/ask (with slippage), and writes rows
    into ``tw_positions``. ``target_price`` / ``stop_price`` / ``time_stop_at``
    are absolute per-share prices / timestamps; the engine copies them into
    the position row so the reconciler can exit without re-consulting the bot.
    """

    bot_id: str
    underlying: str
    direction: str  # 'bullish' | 'bearish' | 'neutral'
    strategy_type: str
    legs: List[Leg]
    entry_price: float
    conviction: float  # [0, 1]
    target_price: Optional[float] = None
    stop_price: Optional[float] = None
    time_stop_at: Optional[datetime] = None
    wall_ref_price: Optional[float] = None
    wall_ref_side: Optional[str] = None
    rationale: str = ""
    components_at_entry: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExitDecision:
    """A bot's per-tick answer to 'should I close the open position?'."""

    should_close: bool
    reason: str = ""
    should_add: bool = False
    should_cut: bool = False
    add_fraction: float = 0.0
    cut_fraction: float = 0.0


@dataclass
class BotSpec:
    """Static registry entry — one row of ``tw_bots``."""

    id: str
    display_name: str
    strategy_class: str
    tier: str
    direction_mode: str
    universe: str
    tagline: str
    description: str
    params: Dict[str, Any] = field(default_factory=dict)
    is_public: bool = True
    enabled: bool = True


@dataclass
class BotCapital:
    """Live capital sleeve — one row of ``tw_bot_capital``."""

    bot_id: str
    starting_capital: float
    current_capital: float
    peak_capital: float
    max_heat_pct: float
    kelly_fraction: float
    daily_kill_pct: float


@dataclass
class OpenPosition:
    """A row from ``tw_positions``, materialized for the reconciler."""

    id: int
    bot_id: str
    underlying: str
    opened_at: datetime
    direction: str
    strategy_type: str
    legs: List[Dict[str, Any]]
    entry_price: float
    current_price: float
    quantity_open: int
    unrealized_pnl: float
    stop_price: Optional[float]
    target_price: Optional[float]
    time_stop_at: Optional[datetime]
    min_hold_until: Optional[datetime]
    wall_ref_price: Optional[float]
    wall_ref_side: Optional[str]
    entry_conviction: Optional[float]
    components_at_entry: Dict[str, Any]
