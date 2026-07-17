"""Static registry of the default TradeWorkz bot roster.

Each :class:`BotSpec` here corresponds to one row of ``tw_bots`` after
provisioning. The class ids are used to look up the strategy class at
runtime — do NOT rename a bot id once it has traded; :meth:`get_bot_class`
resolves the class from ``bot.strategy_class`` on each engine tick.
"""

from __future__ import annotations

from typing import Dict, Iterable, Type

from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.bots.bull_momentum_climber import BullMomentumClimber
from src.tradeworkz.bots.dealer_delta_pressure_rider import DealerDeltaPressureRider
from src.tradeworkz.bots.eod_pin_drifter import EodPinDrifter
from src.tradeworkz.bots.gamma_flip_breaker import GammaFlipBreaker
from src.tradeworkz.bots.gamma_flip_defender import GammaFlipDefender
from src.tradeworkz.bots.max_pain_gravitator import MaxPainGravitator
from src.tradeworkz.bots.opening_range_hunter import OpeningRangeHunter
from src.tradeworkz.bots.put_call_wall_bouncer import PutCallWallBouncer
from src.tradeworkz.bots.put_wall_magnet_reversal import PutWallMagnetReversal
from src.tradeworkz.bots.range_iron_condor import RangeIronCondor
from src.tradeworkz.bots.vix_regime_breakout import VixRegimeBreakout
from src.tradeworkz.bots.vol_expansion_straddle import VolExpansionStraddle
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
    # v2: multi-strategy expansion — verticals, iron condors, straddles.
    "BullMomentumClimber": BullMomentumClimber,
    "RangeIronCondor": RangeIronCondor,
    "VolExpansionStraddle": VolExpansionStraddle,
    # v3: negative-γ mega-put-wall reversal. Registered (runnable /
    # backtestable) but intentionally NOT in DEFAULT_ROSTER. SHELVED: the
    # thesis backtest (src/tools/put_wall_magnet_backtest.py, 90d SPY/QQQ,
    # 2026-07-16) did NOT support it. "Bigger wall bounces better" failed —
    # on SPY expectancy INVERTED with wall size (90-95th pct +0.30R vs
    # 99-100th -0.15R) and was ~breakeven overall; QQQ was noisy and
    # non-monotone. And that is BEFORE 0DTE theta on a ~65%-timeout
    # distribution, which almost certainly turns it net-negative. Kept for
    # the record; do NOT enable without a theta-aware spread-P&L backtest
    # that shows a real edge.
    "PutWallMagnetReversal": PutWallMagnetReversal,
}

# Roster entry for PutWallMagnetReversal, deliberately kept OUT of the live
# DEFAULT_ROSTER. SHELVED (see the note above) — the thesis backtest did not
# support the edge, so this is NOT ready to enable. Left as scaffolding in
# case a future, theta-aware backtest revives the idea; the bot's
# inverted-risk params live on the class (_DEFAULTS), so enabling would only
# need the identity fields here.
PUT_WALL_MAGNET_REVERSAL_SPEC = BotSpec(
    id="put_wall_magnet_reversal",
    display_name="Put Wall Magnet Reversal",
    strategy_class="PutWallMagnetReversal",
    tier="0DTE",
    direction_mode="bullish",
    universe="*",
    tagline="Negative-γ knife into a massive put wall. Fade the magnet, cut fast on a break.",
    description=(
        "Fades a historically large put wall in a negative-gamma regime "
        "(max-pain / liquidity-node magnet, not a dealer-defended wall). Bull "
        "call debit spread caps risk; a tight, single-bar-confirmed wall-break "
        "stop cuts fast because a break in negative gamma cascades. Sizes up "
        "into the biggest walls, never loosens the stop."
    ),
    params={"min_put_wall_pctile": 90.0},
)


DEFAULT_ROSTER: tuple[BotSpec, ...] = (
    BotSpec(
        id="put_call_wall_bouncer",
        display_name="Put/Call Wall Bouncer",
        strategy_class="PutCallWallBouncer",
        tier="0DTE",
        direction_mode="context",
        universe="*",
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
        universe="*",
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
        universe="*",
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
        universe="*",
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
        universe="*",
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
        universe="*",
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
        universe="*",
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
        universe="*",
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
        universe="*",
        tagline="Distant max_pain in a pin regime. Ride the drift back.",
        description=(
            "1-2 day debit trades in the direction of max_pain when the "
            "underlying is materially displaced and gamma is positive."
        ),
        params={"min_drift_pct": 0.004, "max_hold_minutes": 1440},
    ),
    # ── v2 additions ────────────────────────────────────────────────
    # Purposefully differentiated by:
    #   * DIRECTION — bull_momentum_climber is the fleet's first
    #     dedicated bullish bot; range_iron_condor is neutral-theta;
    #     vol_expansion_straddle is neutral-vega. Together they
    #     rebalance the fleet's average delta away from short-only.
    #   * STRUCTURE — verticals cap risk vs naked debits; iron condor
    #     and straddle both use multi-leg structures the pricing
    #     layer already supports via build_iron_condor / build_straddle.
    # Every bot is symbol-agnostic (universe='*') — the engine loops
    # each bot over the full fleet universe (TRADEWORKZ_UNIVERSE) at
    # tick time and hands it the matching per-underlying snapshot.
    BotSpec(
        id="bull_momentum_climber",
        display_name="Bull Momentum Climber",
        strategy_class="BullMomentumClimber",
        tier="0DTE",
        direction_mode="bullish",
        universe="*",
        tagline="Positive-γ climbing above VWAP and flip. Buy the call debit spread.",
        description=(
            "Fires only when spot has cleared VWAP AND gamma_flip in a "
            "positive-γ regime with confirming 5-bar momentum. Enters a "
            "narrow bull call debit spread targeting the call wall; stop "
            "is a break back below the flip."
        ),
        params={
            "min_trend_pct": 0.0015,
            "min_wall_room_pct": 0.003,
            "max_hold_minutes": 60,
            "dte_target": 0,
        },
    ),
    BotSpec(
        id="range_iron_condor",
        display_name="Range Iron Condor",
        strategy_class="RangeIronCondor",
        tier="0DTE",
        direction_mode="neutral",
        universe="*",
        tagline="Positive-γ pin, low VIX. Sell the condor between the walls.",
        description=(
            "Sells a symmetric iron condor when spot is between the put "
            "and call walls in positive-γ with subdued VIX. Non-"
            "directional theta capture with wing-capped downside."
        ),
        params={
            "max_vix": 18.0,
            "min_wall_span_pct": 0.008,
            "wing_width": 2,
            "short_buffer": 2,
            "max_hold_minutes": 180,
            "dte_target": 0,
        },
    ),
    BotSpec(
        id="vol_expansion_straddle",
        display_name="Vol Expansion Straddle",
        strategy_class="VolExpansionStraddle",
        tier="0DTE",
        direction_mode="neutral",
        universe="*",
        tagline="Compressed vol + tight range + short γ. Buy the straddle.",
        description=(
            "Buys an ATM straddle when VIX is compressed, SPY has ranged "
            "tightly, and gamma is negative — the setup where a breakout "
            "in either direction reinforces via dealer hedging."
        ),
        params={
            "max_vix": 16.0,
            "max_range_pct": 0.006,
            "max_hold_minutes": 120,
            "dte_target": 0,
        },
    ),
)


# ── Retired bots ────────────────────────────────────────────────────
# Bots that were once shipped but no longer belong in the fleet. The
# engine calls ``retire_stale_bots`` on provision so historical trade
# rows survive (audit stays intact) but the sleeve is zeroed and the
# bot flipped to enabled=false, freeing capital for the active roster
# on the next re-balance. Do NOT delete the id itself — foreign keys
# from tw_trades / tw_positions point at it and the audit UI needs it.
RETIRED_BOT_IDS: tuple[str, ...] = (
    # Symbol-specific variants collapsed into their symbol-agnostic
    # parents when the fleet universe expanded from SPY-only to CSV.
    "qqq_gamma_flip_breaker",
    "qqq_dealer_delta_pressure_rider",
)


def get_bot_class(name: str) -> type[BaseBot]:
    """Resolve a ``strategy_class`` string to the concrete class."""
    try:
        return STRATEGY_CLASSES[name]
    except KeyError:
        raise KeyError(
            f"Unknown TradeWorkz strategy_class {name!r}; " f"known: {sorted(STRATEGY_CLASSES)}"
        ) from None


def default_roster() -> Iterable[BotSpec]:
    return DEFAULT_ROSTER
