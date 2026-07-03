"""Static registry of the default TradeWorkz bot roster.

Each :class:`BotSpec` here corresponds to one row of ``tw_bots`` after
provisioning. The class ids are used to look up the strategy class at
runtime — do NOT rename a bot id once it has traded; :meth:`get_bot_class`
resolves the class from ``bot.strategy_class`` on each engine tick.
"""

from __future__ import annotations

from typing import Dict, Iterable, Type

from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.bots.dealer_delta_pressure_rider import DealerDeltaPressureRider
from src.tradeworkz.bots.eod_pin_drifter import EodPinDrifter
from src.tradeworkz.bots.gamma_flip_breaker import GammaFlipBreaker
from src.tradeworkz.bots.gamma_flip_defender import GammaFlipDefender
from src.tradeworkz.bots.max_pain_gravitator import MaxPainGravitator
from src.tradeworkz.bots.opening_range_hunter import OpeningRangeHunter
from src.tradeworkz.bots.put_call_wall_bouncer import PutCallWallBouncer
from src.tradeworkz.bots.vix_regime_breakout import VixRegimeBreakout
from src.tradeworkz.bots.vwap_reversion_scalper import VwapReversionScalper
from src.tradeworkz.models import BotSpec

STRATEGY_CLASSES: Dict[str, Type[BaseBot]] = {
    "PutCallWallBouncer": PutCallWallBouncer,
    "GammaFlipDefender": GammaFlipDefender,
    "GammaFlipBreaker": GammaFlipBreaker,
    "EodPinDrifter": EodPinDrifter,
    "DealerDeltaPressureRider": DealerDeltaPressureRider,
    "VwapReversionScalper": VwapReversionScalper,
    "VixRegimeBreakout": VixRegimeBreakout,
    "OpeningRangeHunter": OpeningRangeHunter,
    "MaxPainGravitator": MaxPainGravitator,
}


DEFAULT_ROSTER: tuple[BotSpec, ...] = (
    BotSpec(
        id="put_call_wall_bouncer",
        display_name="Put/Call Wall Bouncer",
        strategy_class="PutCallWallBouncer",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="Fade the wall. Ride the mean reversion. Cut if the wall breaks.",
        description=(
            "Enters mean-reversion trades against the nearest call or put wall "
            "in a positive-gamma regime. Size scales with dollar-gamma at the "
            "wall; risk is capped by a wall-break stop and a wall-migration cut."
        ),
        params={"wall_proximity_pct": 0.002, "wall_break_pct": 0.003, "max_hold_minutes": 90},
    ),
    BotSpec(
        id="gamma_flip_defender",
        display_name="Gamma Flip Defender",
        strategy_class="GammaFlipDefender",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="Sell rallies into the flip. Buy dips into the flip. Positive-γ only.",
        description=(
            "Mean-reversion at the gamma flip line inside a positive-gamma regime. "
            "Exits on flip break or the far wall."
        ),
        params={"flip_proximity_pct": 0.0015, "flip_break_pct": 0.003, "max_hold_minutes": 60},
    ),
    BotSpec(
        id="gamma_flip_breaker",
        display_name="Gamma Flip Breaker",
        strategy_class="GammaFlipBreaker",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="Ride the breakout. Dealer hedging turns momentum into a wave.",
        description=(
            "Enters directional trades when price breaks through the gamma "
            "flip in a thin or negative-gamma regime."
        ),
        params={"cross_min_pct": 0.0015, "flip_reentry_pct": 0.0008, "max_hold_minutes": 60},
    ),
    BotSpec(
        id="eod_pin_drifter",
        display_name="EOD Pin Drifter",
        strategy_class="EodPinDrifter",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="Ride the last-hour pin. Dealers push spot toward max-pain.",
        description=(
            "0DTE afternoon drift bot: enters when the underlying is off "
            "max_pain enough to expect a pin into the close."
        ),
        params={"min_drift_pct": 0.001, "max_hold_minutes": 60},
    ),
    BotSpec(
        id="dealer_delta_pressure_rider",
        display_name="Dealer Delta Pressure Rider",
        strategy_class="DealerDeltaPressureRider",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="Ride dealer delta pressure. Momentum + short-γ regime = follow-through.",
        description=(
            "Enters directional trades in a negative-gamma regime when "
            "dealer net delta is heavily one-sided and recent price agrees."
        ),
        params={"delta_threshold": 5.0e8, "max_hold_minutes": 60},
    ),
    BotSpec(
        id="vwap_reversion_scalper",
        display_name="VWAP Reversion Scalper",
        strategy_class="VwapReversionScalper",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="Fade the stretch to VWAP. Positive-γ magnet.",
        description=(
            "Small-size 0DTE scalps that fade over-extension away from "
            "session VWAP in a positive-gamma regime."
        ),
        params={"min_stretch_pct": 0.0025, "max_hold_minutes": 30},
    ),
    BotSpec(
        id="vix_regime_breakout",
        display_name="VIX Regime Breakout",
        strategy_class="VixRegimeBreakout",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="Vol is expanding. Ride the trend before the wall catches it.",
        description=(
            "Trend continuation when VIX is elevated and dealer gamma has "
            "flipped short. Targets the far wall."
        ),
        params={"min_vix": 16.0, "min_move_pct": 0.003, "max_hold_minutes": 60},
    ),
    BotSpec(
        id="opening_range_hunter",
        display_name="Opening Range Hunter",
        strategy_class="OpeningRangeHunter",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="Break the opening range. Trend + thin γ = clean legs.",
        description=(
            "Enters trend continuation on breaks of the opening range in "
            "regimes where dealer gamma does not choke the move."
        ),
        params={"break_buffer_pct": 0.0005, "max_hold_minutes": 90},
    ),
    BotSpec(
        id="max_pain_gravitator",
        display_name="Max Pain Gravitator",
        strategy_class="MaxPainGravitator",
        tier="1DTE",
        direction_mode="context",
        universe="SPY",
        tagline="Distant max_pain in a pin regime. Ride the drift back.",
        description=(
            "1-2 day debit trades in the direction of max_pain when the "
            "underlying is materially displaced and gamma is positive."
        ),
        params={"min_drift_pct": 0.004, "max_hold_minutes": 1440},
    ),
)


def get_bot_class(name: str) -> type[BaseBot]:
    """Resolve a ``strategy_class`` string to the concrete class."""
    try:
        return STRATEGY_CLASSES[name]
    except KeyError:
        raise KeyError(
            f"Unknown TradeWorkz strategy_class {name!r}; "
            f"known: {sorted(STRATEGY_CLASSES)}"
        ) from None


def default_roster() -> Iterable[BotSpec]:
    return DEFAULT_ROSTER
