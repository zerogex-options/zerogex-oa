"""Per-bot position sizing.

The formula:

    contracts = floor(
        (capital × max_heat_pct × conviction × size_multiplier × wall_scale)
        / (entry_price × 100 × kelly_denominator)
    )

Where ``wall_scale`` (0.5..1.5) scales with the bot's read of wall strength
when the signal references a wall — bots that don't reference walls just
pass 1.0. ``size_multiplier`` is the ML-learned per-bot knob. A minimum of
1 contract is enforced when the ordinary Kelly-scaled number rounds to 0
but the bot's conviction is above threshold — this keeps small-capital
sleeves from being locked out of trading.
"""

from __future__ import annotations

import math
from typing import Optional

from src.tradeworkz.models import BotCapital


def compute_contracts(
    *,
    capital: BotCapital,
    entry_price: float,
    conviction: float,
    size_multiplier: float,
    wall_strength: Optional[float] = None,
    daily_realized_pnl: float = 0.0,
) -> int:
    """Return the integer number of contracts the bot should open.

    ``entry_price`` is per-share premium (dollars), converted to
    per-contract by ×100 inside the formula. ``wall_strength`` is a bot-
    supplied 0..1 read of the wall's dollar-gamma weight.
    """
    if entry_price <= 0:
        return 0

    heat_dollars = capital.current_capital * capital.max_heat_pct
    kelly_denom = max(0.05, 1.0 - capital.kelly_fraction * conviction)

    if capital.daily_kill_pct > 0:
        loss_kill_dollars = -capital.daily_kill_pct * capital.starting_capital
        if daily_realized_pnl <= loss_kill_dollars:
            return 0

    wall_scale = 1.0
    if wall_strength is not None:
        wall_scale = 0.5 + max(0.0, min(1.0, wall_strength))

    dollars_per_contract = entry_price * 100.0
    if dollars_per_contract <= 0:
        return 0

    raw = (heat_dollars * conviction * size_multiplier * wall_scale) / (
        dollars_per_contract * kelly_denom
    )
    contracts = int(math.floor(raw))
    if contracts <= 0 and conviction >= 0.55:
        contracts = 1
    return contracts
