"""Static registry of the default TradeWorkz bot roster.

Each :class:`BotSpec` here corresponds to one row of ``tw_bots`` after
provisioning. The class ids are used to look up the strategy class at
runtime — do NOT rename a bot id once it has traded; :meth:`get_bot_class`
resolves the class from ``bot.strategy_class`` on each engine tick.
"""

from __future__ import annotations

from typing import Dict, Iterable, Type

from src.tradeworkz.bots.aggressor_flow_divergence import AggressorFlowDivergence
from src.tradeworkz.bots.base import BaseBot
from src.tradeworkz.bots.bull_momentum_climber import BullMomentumClimber
from src.tradeworkz.bots.charm_close_magnet import CharmCloseMagnet
from src.tradeworkz.bots.dealer_delta_pressure_rider import DealerDeltaPressureRider
from src.tradeworkz.bots.eod_pin_drifter import EodPinDrifter
from src.tradeworkz.bots.gamma_flip_breaker import GammaFlipBreaker
from src.tradeworkz.bots.gamma_flip_defender import GammaFlipDefender
from src.tradeworkz.bots.gamma_regime_shift_rider import GammaRegimeShiftRider
from src.tradeworkz.bots.max_pain_gravitator import MaxPainGravitator
from src.tradeworkz.bots.opening_range_hunter import OpeningRangeHunter
from src.tradeworkz.bots.put_call_wall_bouncer import PutCallWallBouncer
from src.tradeworkz.bots.put_wall_magnet_reversal import PutWallMagnetReversal
from src.tradeworkz.bots.range_iron_condor import RangeIronCondor
from src.tradeworkz.bots.vanna_vol_crush_rider import VannaVolCrushRider
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
    # v4: edge-metric candidates. Each trades a data layer the retired fleet
    # never touched — aggressor order flow, second-order forced dealer flow
    # (vanna/charm), and the gamma-restoring Pin Strike. Registered (runnable /
    # backtestable) but intentionally NOT in DEFAULT_ROSTER (see CANDIDATE_SPECS
    # and the shelving note below): they go live only after clearing
    # `make tradeworkz-backtest`.
    "CharmCloseMagnet": CharmCloseMagnet,
    "VannaVolCrushRider": VannaVolCrushRider,
    "AggressorFlowDivergence": AggressorFlowDivergence,
    "GammaRegimeShiftRider": GammaRegimeShiftRider,
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


# ── Shelved catalog ─────────────────────────────────────────────────
# The full set of bot specs that HAVE shipped. As of 2026-08-09 none are
# in the active DEFAULT_ROSTER (below) — the fleet was shelved after the
# backtest screen found no edge. These definitions are preserved verbatim
# so a strategy can be revived (moved back into DEFAULT_ROSTER) the moment
# it clears `make tradeworkz-backtest`. They are also what
# RETIRED_BOT_IDS is built from, so every shipped bot is retired in the DB.
SHELVED_SPECS: tuple[BotSpec, ...] = (
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


# ── Candidate roster (edge-metric bots, NOT yet live) ───────────────
# The v4 strategies. These are the first bots to trade the data layers the
# shelved fleet never used — aggressor order flow, second-order forced dealer
# flow (vanna/charm), the modeled close-charm flow, and the gamma-restoring Pin
# Strike (see docs/design/tradeworkz-edge-strategies.md). They are deliberately
# kept OUT of DEFAULT_ROSTER: the whole point of the 2026-08-09 shelving was
# that nothing goes live on a thesis alone. Each is registered in
# STRATEGY_CLASSES so `make tradeworkz-backtest --bots <id>` can screen it; move
# a spec into DEFAULT_ROSTER (and out of here) ONLY after it clears the gate —
# PF >= 1.1, positive expectancy, >= 20 trades. Until then they never
# provision, never size, never open.
CANDIDATE_SPECS: tuple[BotSpec, ...] = (
    BotSpec(
        id="charm_close_magnet",
        display_name="Charm Close Magnet",
        strategy_class="CharmCloseMagnet",
        tier="0DTE",
        direction_mode="context",
        universe="*",
        tagline="Ride the forced charm flow into the pin. Quantified, not folklore.",
        description=(
            "Final-window drift toward the gamma-restoring Pin Strike, but only "
            "when the modeled close_charm_flow (dollars dealers must trade by the "
            "close) actually points at the pin. Positive-γ, confident pin, "
            "defined-risk vertical. Supersedes EodPinDrifter / MaxPainGravitator, "
            "which drifted on displacement + folklore."
        ),
        params={
            "max_minutes_to_close": 120,
            "min_pin_confidence": 0.55,
            "min_drift_pct": 0.001,
            "max_drift_pct": 0.010,
            "max_hold_minutes": 90,
            "dte_target": 0,
        },
    ),
    BotSpec(
        id="vanna_vol_crush_rider",
        display_name="Vanna Vol-Crush Rider",
        strategy_class="VannaVolCrushRider",
        tier="0DTE",
        direction_mode="context",
        universe="*",
        tagline="Vol is moving. Ride the vanna hedging flow it forces on dealers.",
        description=(
            "Trades the sign+size of dealer_vanna_total × ΔVIX — a short-vanna "
            "book into a vol crush must buy (melt-up), and the mirror for a vol "
            "spike. Defined-risk vertical. Supersedes VixRegimeBreakout, which "
            "used only the VIX level and never vanna or the vol change."
        ),
        params={
            "min_vix_change": 0.30,
            "min_dealer_vanna": 4.0e7,
            "target_pct": 0.004,
            "stop_pct": 0.003,
            "max_hold_minutes": 60,
            "dte_target": 0,
        },
    ),
    BotSpec(
        id="aggressor_flow_divergence",
        display_name="Aggressor Flow Divergence",
        strategy_class="AggressorFlowDivergence",
        tier="0DTE",
        direction_mode="context",
        universe="*",
        tagline="Aggressive option flow leans hard; price hasn't caught up. Lead it.",
        description=(
            "Leads price with aggressor-classified net option premium "
            "(flow_series_5min) when it is strong, still accelerating, confirmed "
            "on volume, and price has NOT yet moved to match. Stands down in a "
            "strong positive-γ pin. The first bot to use option order flow at "
            "all — no retired bot did."
        ),
        params={
            "min_net_premium": 5.0e5,
            "max_price_move_pct": 0.0025,
            "target_pct": 0.004,
            "stop_pct": 0.003,
            "max_hold_minutes": 60,
            "dte_target": 0,
        },
    ),
    BotSpec(
        id="gamma_regime_shift_rider",
        display_name="Gamma Regime Shift Rider",
        strategy_class="GammaRegimeShiftRider",
        tier="0DTE",
        direction_mode="context",
        universe="*",
        tagline="Dealer gamma is flipping short at the flip. Ride the regime break.",
        description=(
            "Trades the long→short gamma TRANSITION — net_gex collapsing tick "
            "over tick with spot at the flip and convexity elevated — not a "
            "static level break. Flow-confirmed; stop is a reclaim of the flip. "
            "Supersedes GammaFlipBreaker / DealerDeltaPressureRider, which read a "
            "static level / sign."
        ),
        params={
            "min_net_gex_drop": 5.0e8,
            "max_flip_distance_pct": 0.003,
            "min_break_trend_pct": 0.0010,
            "target_pct": 0.006,
            "max_hold_minutes": 60,
            "dte_target": 0,
        },
    ),
)


def candidate_specs() -> Iterable[BotSpec]:
    """Backtest-gated edge candidates — registered/runnable, NOT auto-provisioned."""
    return CANDIDATE_SPECS


# ── Active roster ───────────────────────────────────────────────────
# EMPTY. The entire fleet was shelved on 2026-08-09 after the backtest
# harness (make tradeworkz-backtest) screened every bot over 45 days /
# 2,065 trades and found NONE with edge — the best profit factor in the
# fleet was 0.78, and four bots never fired at all. The live record
# (PF ~0.42) and the backtest agree. Rather than keep paper capital on a
# negative-edge fleet, every bot is retired (below) while the engine keeps
# running so a future, edge-positive strategy can be added.
#
# To REVIVE a strategy: move its spec from SHELVED_SPECS into this tuple
# and drop its id from RETIRED_BOT_IDS — but ONLY after it clears the
# screen (PF >= 1.1 with positive expectancy over >= 20 trades on
# `make tradeworkz-backtest`). The harness is the gate; nothing goes live
# on hope again.
DEFAULT_ROSTER: tuple[BotSpec, ...] = ()


# ── Retired bots ────────────────────────────────────────────────────
# Bots retired from the live fleet. On provision the engine flips each to
# enabled=false and zeroes its sleeve, so historical trade rows survive
# (audit + leaderboard stay intact) but no capital rides on them and they
# never open. Do NOT delete an id — foreign keys from tw_trades /
# tw_positions point at it and the audit UI needs it. This is currently the
# WHOLE shipped catalog (see the shelving note above) plus the two legacy
# symbol-specific variants collapsed into their parents when the fleet
# universe expanded from SPY-only to CSV.
RETIRED_BOT_IDS: tuple[str, ...] = tuple(spec.id for spec in SHELVED_SPECS) + (
    "qqq_gamma_flip_breaker",
    "qqq_dealer_delta_pressure_rider",
)


def known_specs() -> Dict[str, BotSpec]:
    """Every registry spec by id — active roster + candidates + shelved + the
    standalone magnet spec.

    The backtest harness (``_load_backtest_bots``) uses this to screen a bot
    that has NEVER been provisioned into ``tw_bots`` — every candidate and
    every shelved bot. That is what keeps the promotion gate honest: a
    ``CANDIDATE_SPECS`` bot is deliberately never seeded into the DB (it must
    not provision, size, or open), yet ``make tradeworkz-backtest --bots <id>``
    still needs to measure its edge before anyone moves it into
    ``DEFAULT_ROSTER``. Without this fallback the harness would report "no bots
    found" for an un-provisioned candidate.

    Ordering: shelved first, then candidates, then the (currently empty) active
    roster last so a revived id would win — ids do not collide across groups
    today, so ordering is only a future-proofing detail.
    """
    specs: Dict[str, BotSpec] = {}
    for group in (SHELVED_SPECS, CANDIDATE_SPECS, DEFAULT_ROSTER):
        for s in group:
            specs[s.id] = s
    specs.setdefault(PUT_WALL_MAGNET_REVERSAL_SPEC.id, PUT_WALL_MAGNET_REVERSAL_SPEC)
    return specs


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
