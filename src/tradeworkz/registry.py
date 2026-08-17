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
from src.tradeworkz.bots.call_wall_rejector import CallWallRejector
from src.tradeworkz.bots.charm_close_magnet import CharmCloseMagnet
from src.tradeworkz.bots.climax_flow_fade import ClimaxFlowFade
from src.tradeworkz.bots.dealer_delta_pressure_rider import DealerDeltaPressureRider
from src.tradeworkz.bots.dual_flip_dislocation import DualFlipDislocation
from src.tradeworkz.bots.eod_pin_drifter import EodPinDrifter
from src.tradeworkz.bots.fresh_flow_momentum import FreshFlowMomentum
from src.tradeworkz.bots.hedge_impulse_quiet_tape import HedgeImpulseQuietTape
from src.tradeworkz.bots.gamma_flip_breaker import GammaFlipBreaker
from src.tradeworkz.bots.gamma_flip_defender import GammaFlipDefender
from src.tradeworkz.bots.gamma_regime_shift_rider import GammaRegimeShiftRider
from src.tradeworkz.bots.max_pain_gravitator import MaxPainGravitator
from src.tradeworkz.bots.opening_range_hunter import OpeningRangeHunter
from src.tradeworkz.bots.profile_shelf_breaker import ProfileShelfBreaker
from src.tradeworkz.bots.put_call_wall_bouncer import PutCallWallBouncer
from src.tradeworkz.bots.put_capitulation_credit_fade import PutCapitulationCreditFade
from src.tradeworkz.bots.put_wall_bouncer import PutWallBouncer
from src.tradeworkz.bots.put_wall_magnet_reversal import PutWallMagnetReversal
from src.tradeworkz.bots.range_iron_condor import RangeIronCondor
from src.tradeworkz.bots.settlement_flow_snap import SettlementFlowSnap
from src.tradeworkz.bots.vanna_vol_crush_rider import VannaVolCrushRider
from src.tradeworkz.bots.vix_regime_breakout import VixRegimeBreakout
from src.tradeworkz.bots.vol_expansion_straddle import VolExpansionStraddle
from src.tradeworkz.bots.vwap_reversion_scalper import VwapReversionScalper
from src.tradeworkz.bots.weekly_charm_grind import WeeklyCharmGrind
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
    # v5: fresh-flow successor to the screened-out cumulative-flow bot.
    "FreshFlowMomentum": FreshFlowMomentum,
    # v6: contrarian read — FADE the flow climax (both follow-bots failed).
    "ClimaxFlowFade": ClimaxFlowFade,
    # v7: the flagship wall strategy, split into two directional bots with
    # rejection-confirmation + wall-strength + flow-no-pierce filters.
    "CallWallRejector": CallWallRejector,
    "PutWallBouncer": PutWallBouncer,
    # v8 (the "v5 fleet", docs/design/tradeworkz-v5-strategies.md): six
    # candidates trading data layers no bot tier ever consumed — the
    # forced-flow raw/smooth settlement residual, the dual-flip disagreement
    # band, gex_profile curve geometry, the delta-weighted pending hedge
    # obligation, the per-type put-capitulation split (first credit-exit
    # bot), and the bucketed weekly charm ladder (first 1DTE candidate).
    "SettlementFlowSnap": SettlementFlowSnap,
    "DualFlipDislocation": DualFlipDislocation,
    "ProfileShelfBreaker": ProfileShelfBreaker,
    "HedgeImpulseQuietTape": HedgeImpulseQuietTape,
    "PutCapitulationCreditFade": PutCapitulationCreditFade,
    "WeeklyCharmGrind": WeeklyCharmGrind,
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


# Aggressor Flow Divergence — SCREENED OUT (edge test failed). Registered and
# backtestable for the record (see known_specs), but deliberately NOT in
# CANDIDATE_SPECS. The `make tradeworkz-backtest` screen on 2026-08-09 (45d,
# 404 trades) returned PF 0.31 / expectancy -$36.63 — a decisive no-edge, a
# monotonic bleed to -$14.8K, with wins SMALLER than losses at a 31% win rate
# (a directional-prediction failure, not a stop/target tuning issue). The
# cumulative day-to-date net premium it keyed on is a lagging signal. Do NOT
# revive this exact design; a "fresh-flow" variant (keying on the per-bucket
# flow delta rather than the cumulative) would be a NEW hypothesis, screened
# from scratch.
AGGRESSOR_FLOW_DIVERGENCE_SPEC = BotSpec(
    id="aggressor_flow_divergence",
    display_name="Aggressor Flow Divergence",
    strategy_class="AggressorFlowDivergence",
    tier="0DTE",
    direction_mode="context",
    universe="*",
    tagline="Aggressive option flow leans hard; price hasn't caught up. Lead it.",
    description=(
        "SCREENED OUT (PF 0.31 / 404 trades, 2026-08-09). Led price with "
        "aggressor-classified net option premium (flow_series_5min); the "
        "cumulative signal did not predict direction. Kept for the record."
    ),
    params={
        "min_net_premium": 5.0e5,
        "max_price_move_pct": 0.0025,
        "target_pct": 0.004,
        "stop_pct": 0.003,
        "max_hold_minutes": 60,
        "dte_target": 0,
    },
)


# Fresh Flow Momentum — SCREENED OUT (edge test failed). The fresh-flow
# successor to AggressorFlowDivergence: it keyed on the recent WINDOWED flow +
# acceleration instead of the day-to-date cumulative, precisely to fix the
# lagging-signal diagnosis. It did not help. The screen on 2026-08-10 (45d, 461
# trades) returned PF 0.331 / expectancy -$34.68 — the same monotonic bleed and
# the same signature as its predecessor (33% win rate, wins SMALLER than losses).
# Two decisive failures settle the "aggressor flow LEADS price" thesis on 0DTE:
# following the burst systematically buys a local extreme that reverts. Any
# future work on this data axis should test the CONTRARIAN read (fade the flow
# extreme), which would be a NEW hypothesis screened from scratch — not another
# variant of "follow the flow". Registered + backtestable for the record.
FRESH_FLOW_MOMENTUM_SPEC = BotSpec(
    id="fresh_flow_momentum",
    display_name="Fresh Flow Momentum",
    strategy_class="FreshFlowMomentum",
    tier="0DTE",
    direction_mode="context",
    universe="*",
    tagline="Fresh aggressor-flow burst leads price. Ride the pulse.",
    description=(
        "SCREENED OUT (PF 0.331 / 461 trades, 2026-08-10). Rode a fresh, "
        "accelerating aggressor-flow burst; following the burst on 0DTE buys a "
        "local extreme that reverts. Kept for the record."
    ),
    params={
        "min_recent_premium": 2.0e5,
        "accel_mult": 1.15,
        "max_price_move_pct": 0.004,
        "target_pct": 0.004,
        "stop_pct": 0.003,
        "max_hold_minutes": 45,
        "dte_target": 0,
    },
)


# Quiet-Tape Hedge Impulse — SCREENED OUT (edge test failed). The strongest
# formulation the flow-direction axis will ever get: it weighted the flow by
# per-contract DELTA (the hedge OBLIGATION, not premium sentiment), required a
# FLAT tape (entering ahead of the hedge instead of chasing it — the exact
# inverse of the entry state that killed the first two flow bots), conditioned
# on NEGATIVE gamma (amplification), and required sign persistence. The
# 2026-08-15 screen (60d, 383 trades) returned PF 0.32 / expectancy -$11.66 /
# 22% win rate — the SAME signature as aggressor_flow_divergence (0.31) and
# fresh_flow_momentum (0.33). Four independent formulations of "trade the
# direction of recent option flow on 0DTE" have now failed identically:
# cumulative premium, windowed premium + acceleration, and delta-weighted
# obligation both behind and AHEAD of the tape. The axis is closed — recent
# flow direction does not predict 0DTE price direction on these underlyings at
# this cadence, full stop. Do not build a fifth variant; the one live read of
# this data that remains open is the CONTRARIAN one (climax_flow_fade,
# screening). Registered + backtestable for the record.
HEDGE_IMPULSE_QUIET_TAPE_SPEC = BotSpec(
    id="hedge_impulse_quiet_tape",
    display_name="Quiet-Tape Hedge Impulse",
    strategy_class="HedgeImpulseQuietTape",
    tier="0DTE",
    direction_mode="context",
    universe="*",
    tagline="A hedge obligation is staged and the tape hasn't moved. Front-run it.",
    description=(
        "SCREENED OUT (PF 0.32 / 383 trades, 2026-08-15). Front-ran the "
        "delta-weighted pending dealer hedge on a flat tape in negative "
        "gamma — the fourth and strongest flow-direction formulation, and it "
        "failed with the same signature as the first three. The flow-follow "
        "axis is closed. Kept for the record."
    ),
    params={
        "min_impulse_ratio": 0.08,
        "max_flat_move_pct": 0.0015,
        "flip_exit_ratio": 0.04,
        "no_move_expiry_minutes": 45,
        "target_pct": 0.004,
        "stop_pct": 0.003,
        "max_hold_minutes": 75,
        "scale_out_enabled": False,
        "dte_target": 0,
    },
)


# Split flagship wall bots — SCREENED OUT (edge test failed). The 2026-08-10
# screen (45d) gave call_wall_rejector PF 0.193 / 33 trades and put_wall_bouncer
# PF 0.577 / 29 trades — a THIRD wall-fade failure after the retired
# PutCallWallBouncer (0.53). The rejection-confirmation + wall-strength +
# flow-no-pierce filters cut frequency hard but did not create edge; the
# surviving fades still lost, and the bearish call-fade (PF 0.19, 15% win) was
# far worse than the bullish put-bounce — the result was dominated by the
# window's up-drift, not a wall edge. STRUCTURAL diagnosis: a DEBIT vertical
# needs price to MOVE to the target, but "the wall holds" is a boundary, not a
# target — the right structure to monetize it is a CREDIT spread beyond the wall
# (profit if the wall holds), which is a NEW hypothesis, screened from scratch
# (and cautioned by the retired iron condor, which sold premium at walls and
# also failed). Registered + backtestable for the record.
_WALL_FILTER_PARAMS = {
    "wall_proximity_pct": 0.002,
    "wall_reject_margin_pct": 0.0007,
    "min_wall_strength_pctile": 50.0,
    "pierce_premium": 6.0e5,
    "max_hold_minutes": 90,
    "dte_target": 0,
}
CALL_WALL_REJECTOR_SPEC = BotSpec(
    id="call_wall_rejector",
    display_name="Call Wall Rejector",
    strategy_class="CallWallRejector",
    tier="0DTE",
    direction_mode="bearish",
    universe="*",
    tagline="Confirmed rejection at a big call wall. Fade the rally.",
    description=(
        "SCREENED OUT (PF 0.193 / 33 trades, 2026-08-10). Faded confirmed "
        "call-wall rejections with a debit put vertical; the debit structure is "
        "wrong for a boundary thesis and shorting fought the up-drift. Kept for "
        "the record."
    ),
    params=dict(_WALL_FILTER_PARAMS),
)
PUT_WALL_BOUNCER_SPEC = BotSpec(
    id="put_wall_bouncer",
    display_name="Put Wall Bouncer",
    strategy_class="PutWallBouncer",
    tier="0DTE",
    direction_mode="bullish",
    universe="*",
    tagline="Confirmed bounce off a big put wall. Fade the dip.",
    description=(
        "SCREENED OUT (PF 0.577 / 29 trades, 2026-08-10). Faded confirmed "
        "put-wall bounces with a debit call vertical; a boundary thesis wants a "
        "credit structure, not a debit that needs a move. Kept for the record."
    ),
    params=dict(_WALL_FILTER_PARAMS),
)


# ── Candidate roster (edge-metric bots, NOT yet live) ───────────────
# The v4 strategies still under evaluation. These trade the data layers the
# shelved fleet never used — second-order forced dealer flow (vanna/charm), the
# modeled close-charm flow, and the gamma-restoring Pin Strike / regime velocity
# (see docs/design/tradeworkz-edge-strategies.md). They are deliberately kept
# OUT of DEFAULT_ROSTER: the whole point of the 2026-08-09 shelving was that
# nothing goes live on a thesis alone. Each is registered in STRATEGY_CLASSES so
# `make tradeworkz-backtest --bots <id>` can screen it; move a spec into
# DEFAULT_ROSTER (and out of here) ONLY after it clears the gate — PF >= 1.1,
# positive expectancy, >= 20 trades. Until then they never provision, never
# size, never open. (Screened out so far — see the standalone specs above and
# design doc §8: both flow-follow bots (aggressor / fresh_flow) and both split
# wall bots (call_wall_rejector / put_wall_bouncer). Remaining candidates:
# charm / gamma are underpowered on available history; vanna is VIX-history-
# gated; climax_flow_fade is the live contrarian test.)
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
            "min_shed_frac": 0.10,
            "max_flip_distance_pct": 0.012,
            "min_break_trend_pct": 0.0010,
            "target_pct": 0.006,
            "max_hold_minutes": 60,
            "dte_target": 0,
        },
    ),
    BotSpec(
        id="climax_flow_fade",
        display_name="Climax Flow Fade",
        strategy_class="ClimaxFlowFade",
        tier="0DTE",
        direction_mode="context",
        universe="*",
        tagline="Aggressive flow burst spiked price into a pin. Fade the climax.",
        description=(
            "The CONTRARIAN read of the two screened-out follow-the-flow bots: "
            "their 33% win rate with wins < losses showed that chasing a 0DTE "
            "flow burst buys a local extreme that reverts. This FADES a large, "
            "volume-confirmed fresh flow burst that has overshot price, in a "
            "positive-γ (mean-reverting) regime. Flow burst = the exhaustion "
            "trigger the retired VWAP-reversion bots lacked."
        ),
        params={
            "min_recent_premium": 3.0e5,
            "min_extension_pct": 0.0015,
            "target_pct": 0.003,
            "stop_pct": 0.004,
            "max_hold_minutes": 45,
            "dte_target": 0,
        },
    ),
    # ── The v5 fleet (docs/design/tradeworkz-v5-strategies.md) ─────────
    # Six candidates, each on a data axis no bot tier has ever consumed.
    # Same discipline as every candidate above: registered + screenable by
    # id, never provisioned until it clears PF >= 1.1 / positive
    # expectancy / >= 20 trades on `make tradeworkz-backtest`.
    BotSpec(
        id="settlement_flow_snap",
        display_name="Settlement Residual Snap",
        strategy_class="SettlementFlowSnap",
        tier="0DTE",
        direction_mode="context",
        universe="*",
        tagline="Ride the pure settlement-unwind hedge into the close.",
        description=(
            "Trades the raw-minus-smooth close_charm_flow residual — the "
            "dollars of the by-close dealer hedge that come from 0DTE strikes "
            "resolving to intrinsic, not from smooth charm drift. Fires only "
            "when that settlement leg dominates the drift (keeping it "
            "disjoint from charm_close_magnet) and rivals the local gamma "
            "pool in size. Defined-risk vertical, rides to ~15:52 ET."
        ),
        params={
            "min_minutes_to_close": 30,
            "max_minutes_to_close": 90,
            # First-screen calibration (2026-08-15): the 0.25 prior blocked
            # 1,607 in-window ticks (miss_reason=residual_small) and let ONE
            # trade through in 60d — the residual is real but runs smaller
            # relative to local_gex than the prior assumed. The dominance
            # gate (vs the smooth drift) stays as the qualitative filter.
            "min_residual_local_gex_frac": 0.10,
            "min_residual_dominance": 1.5,
            "target_pct": 0.003,
            "stop_pct": 0.002,
            "max_premium_loss_pct": 0.45,
            "dte_target": 0,
        },
    ),
    BotSpec(
        id="dual_flip_dislocation",
        display_name="Dual-Flip Dislocation",
        strategy_class="DualFlipDislocation",
        tier="0DTE",
        direction_mode="context",
        universe="*",
        tagline="Two flip conventions disagree. Ride the fast book across the band.",
        description=(
            "Fires on a fresh, momentum-confirmed price entry into the band "
            "between gamma_flip_raw (where the un-DTE-weighted 0DTE fast book "
            "flips — an unconsumed column) and the structural DTE-weighted "
            "flip: the state where intraday hedgers are short gamma while "
            "single-flip dashboards still read positive. Targets the band's "
            "far edge; stops on a re-cross. Defined-risk vertical."
        ),
        params={
            "min_band_pct": 0.002,
            "min_momentum_pct": 0.0005,
            # First-screen calibration (2026-08-15): the fresh-cross test
            # compared against the last 1-MINUTE close, but the replay (and
            # the engine) evaluate every ~5 minutes — a cross older than a
            # minute never registered, so valid bands produced only 2 entries
            # in 60d (miss: outside_band 1455 / no_fresh_cross 115). The
            # cross is now detected over the last N one-minute closes.
            "cross_lookback_bars": 6,
            "stop_buffer_pct": 0.001,
            "band_collapse_pct": 0.001,
            "max_hold_minutes": 75,
            "dte_target": 0,
        },
    ),
    BotSpec(
        id="profile_shelf_breaker",
        display_name="Gamma Shelf Cascade",
        strategy_class="ProfileShelfBreaker",
        tier="0DTE",
        direction_mode="context",
        universe="*",
        tagline="A gamma cliff sits just off spot and price is sliding into it.",
        description=(
            "Reads the persisted gex_profile spot-shift curve — consumed by "
            "one frontend overlay and zero bots — for a steep one-sided "
            "negative-gamma shelf within ~0.5% of spot, and buys a vertical "
            "spanning it when the tape slides that way. The dealers' own "
            "hedge schedule accelerates the move; the trough bottom (a "
            "computed, unpublished level) is the target."
        ),
        params={
            # First-screen calibration (2026-08-15): PF 2.28 / +$450 on 5
            # trades with the 0.75/0.5 priors — the best early read in the
            # fleet, but the geometry gates (no_shelf 3495 / not_on_shoulder
            # 3329) throttle it far below the 20-trade bar, and the screen
            # window cannot grow (chain history starts 2026-06-15). Depth
            # and shoulder are widened modestly to buy sample; this is a
            # recalibrated hypothesis screened from scratch, NOT a validated
            # PF 2.28 — the asymmetry (2:1) and live-slide gates that define
            # the mechanism are unchanged.
            "min_shelf_depth_local_frac": 0.60,
            "min_asymmetry_ratio": 2.0,
            "max_spot_gex_local_frac": 0.60,
            "min_trigger_pct": 0.001,
            "stop_pct": 0.0015,
            "max_hold_minutes": 100,
            "dte_target": 0,
        },
    ),
    BotSpec(
        id="put_capitulation_credit_fade",
        display_name="Put Capitulation Credit Fade",
        strategy_class="PutCapitulationCreditFade",
        tier="0DTE",
        direction_mode="context",
        universe="*",
        tagline="Panic put buying into a full-strength long-gamma book. Sell it the IV.",
        description=(
            "When a put-only aggressor burst (3x session baseline, put-"
            "dominant) hits a top-quartile positive-gamma book with spot "
            "still above the put wall, sells a 0DTE put credit vertical "
            "below the dip low — short the IV the capitulators spiked, long "
            "the mechanical dealer dip-buy. First credit-exit bot "
            "(credit_take/credit_stop); the put wall is only the "
            "invalidation floor, never the trigger."
        ),
        params={
            "min_burst_multiple": 3.0,
            "min_put_dominance": 0.65,
            "min_displacement_pct": 0.0035,
            # First-screen bug fix (2026-08-15): the 30-bar displacement
            # window needed 31 closes but build_snapshot fetches only 30, so
            # every tick that cleared regime+window died at no_history
            # (1,300 of them) and the bot could never fire. 25 bars fits the
            # snapshot's history with margin.
            "displacement_bars": 25,
            "min_wall_room_pct": 0.0025,
            "short_strike_offset_pct": 0.0035,
            "credit_take_frac": 0.55,
            "credit_stop_mult": 1.75,
            "max_hold_minutes": 120,
            "dte_target": 0,
        },
    ),
    BotSpec(
        id="weekly_charm_grind",
        display_name="Weekly Charm Grind",
        strategy_class="WeeklyCharmGrind",
        tier="1DTE",
        direction_mode="context",
        universe="*",
        tagline="Quiet positive-gamma midday: weekly-book time decay is the only flow.",
        description=(
            "Rides the deterministic delta-decay rebalance of the 1-7 DTE "
            "weekly dealer book through the 11:00-14:00 lull, gated on the "
            "weekly charm bucket dominating the 0DTE bucket (a ladder "
            "ZeroGEX computes and nobody consumes) and a compressed session. "
            "1DTE debit vertical — half the theta of 0DTE over the hold — "
            "hard exit 15:00 ET, never overnight."
        ),
        params={
            # First-screen calibration (2026-08-15): 0 trades — only 3 ticks
            # in 60d survived the gate stack and all 3 then died at
            # conviction (quality saturated too high for setups that had
            # already cleared every hard gate). The intersection of
            # range<=0.6% AND dominance>=2x AND flow>=0.10*local_gex was
            # near-empty (range_wide 2100 / odte_dominates 347 / flow_small
            # 41); each gate is relaxed one notch and the quality scale is
            # matched to the entry floor so a passing setup can also pass
            # conviction.
            "min_bucket_dominance": 1.5,
            "min_flow_local_gex_frac": 0.08,
            "max_session_range_pct": 0.008,
            "quality_flow_saturation": 0.15,
            "target_pct": 0.0035,
            "stop_pct": 0.0025,
            "scale_out_enabled": False,
            "dte_target": 1,
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
    # Screened-out but backtestable-for-the-record (mirror the magnet spec).
    for screened in (
        AGGRESSOR_FLOW_DIVERGENCE_SPEC,
        FRESH_FLOW_MOMENTUM_SPEC,
        CALL_WALL_REJECTOR_SPEC,
        PUT_WALL_BOUNCER_SPEC,
        HEDGE_IMPULSE_QUIET_TAPE_SPEC,
    ):
        specs.setdefault(screened.id, screened)
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
