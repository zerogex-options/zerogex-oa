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
from typing import Any, Dict, List, Optional

from src.tradeworkz.models import BotCapital


def structure_max_loss_per_share(legs: List[Dict[str, Any]], entry_price: float) -> float:
    """True per-share max loss (dollars) of a defined-risk options structure.

    Risk-sizing must divide the heat budget by the position's real max loss per
    contract, NOT the net premium. For long-only debit structures (single,
    straddle/strangle, debit vertical) those are the same — max loss == the
    debit paid == ``entry_price``. But for structures with SHORT legs (iron
    condor, credit spreads) the max loss is bounded by the wings and can be many
    multiples of the near-zero net: an iron condor entered at a $0.06 net debit
    can lose the full wing width (~$2/share = $200/contract). Sizing on the
    $0.06 net over-levered a live position ~34x — the root of the -$6.8K
    single-trade loss.

    Computed from the expiry payoff evaluated at every strike (and the tails):
    the structure's intrinsic value at spot ``S`` is
    ``Σ long_leg intrinsic − Σ short_leg intrinsic``; net P&L per share is that
    minus ``entry_price`` (the debit paid); max loss is the worst such P&L. This
    is exact for all of these piecewise-linear payoffs. Falls back to
    ``max(0, entry_price)`` if the legs lack usable strikes.
    """
    try:
        strikes = sorted({float(lg["strike"]) for lg in legs if lg.get("strike") is not None})
    except (TypeError, ValueError, KeyError):
        return max(0.0, float(entry_price))
    if not strikes:
        return max(0.0, float(entry_price))

    test_points = [0.0] + strikes + [strikes[-1] * 2.0]
    worst_pnl: Optional[float] = None
    for s in test_points:
        payoff = 0.0
        for leg in legs:
            try:
                k = float(leg["strike"])
            except (TypeError, ValueError, KeyError):
                continue
            typ = str(leg.get("option_type", "")).lower()
            side = str(leg.get("side", "")).lower()
            intrinsic = max(0.0, s - k) if typ.startswith("c") else max(0.0, k - s)
            payoff += intrinsic if side == "long" else -intrinsic
        pnl = payoff - float(entry_price)
        if worst_pnl is None or pnl < worst_pnl:
            worst_pnl = pnl
    if worst_pnl is None:
        return max(0.0, float(entry_price))
    return max(0.0, -worst_pnl)


def compute_contracts(
    *,
    capital: BotCapital,
    entry_price: float,
    conviction: float,
    size_multiplier: float,
    wall_strength: Optional[float] = None,
    daily_realized_pnl: float = 0.0,
    max_loss_per_share: Optional[float] = None,
) -> int:
    """Return the integer number of contracts the bot should open.

    ``entry_price`` is per-share premium (dollars), converted to
    per-contract by ×100 inside the formula. ``wall_strength`` is a bot-
    supplied 0..1 read of the wall's dollar-gamma weight. ``max_loss_per_share``
    is the structure's true per-share max loss (see
    :func:`structure_max_loss_per_share`); when given it is the risk
    denominator instead of ``entry_price`` — identical for long-debit
    structures, but far larger (correct) for defined-risk credit structures.

    Credit structures: ``entry_price`` may be <= 0 (a net credit). Sizing then
    relies entirely on ``max_loss_per_share`` (the wing width − credit); a
    non-positive risk denominator still returns 0.
    """
    heat_dollars = capital.current_capital * capital.max_heat_pct
    kelly_denom = max(0.05, 1.0 - capital.kelly_fraction * conviction)

    if capital.daily_kill_pct > 0:
        loss_kill_dollars = -capital.daily_kill_pct * capital.starting_capital
        if daily_realized_pnl <= loss_kill_dollars:
            return 0

    wall_scale = 1.0
    if wall_strength is not None:
        wall_scale = 0.5 + max(0.0, min(1.0, wall_strength))

    # Risk denominator = the structure's true per-contract max loss. Falls back
    # to the debit (entry_price) when not supplied, which is correct for every
    # long-debit structure and preserves prior behavior.
    risk_per_share = (
        max_loss_per_share
        if (max_loss_per_share is not None and max_loss_per_share > 0)
        else entry_price
    )
    dollars_per_contract = risk_per_share * 100.0
    if dollars_per_contract <= 0:
        return 0

    raw = (heat_dollars * conviction * size_multiplier * wall_scale) / (
        dollars_per_contract * kelly_denom
    )
    contracts = int(math.floor(raw))
    if contracts <= 0 and conviction >= 0.55:
        contracts = 1
    return contracts
