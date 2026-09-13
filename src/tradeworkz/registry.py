"""TradeWorkz bot roster — a derived view over the strategy catalog.

This module used to BE the roster: ~900 lines of hand-maintained ``BotSpec``
literals that drifted from the playbook patterns describing the same theses.
It is now a thin projection of ``src/strategies`` — the single source of truth
shared with Backtesting and Pattern Insights.

What a bot contributes on top of the catalog is **execution and tuning**: the
catalog says what the strategy is and what its general parameters are, and the
bot pulls that and layers its own ``bot_params`` over it
(:meth:`StrategyEntry.effective_bot_params`). Nothing here re-describes a
strategy.

Two ids, both preserved forever:

* ``BotSpec.id`` is the legacy ``tw_bots.id`` — ``tw_trades`` /
  ``tw_positions`` have foreign keys onto it and the audit UI needs it, so it
  is never renamed.
* the catalog's canonical id is what the other two surfaces show. Where they
  differ, :func:`known_specs` accepts either, so
  ``make tradeworkz-backtest --bots <id>`` works with whichever the operator
  has to hand.

Live capital is gated on evidence, not on this module: a bot is provisioned
with a sleeve only when its catalog entry is ``VALIDATED`` **and** carries a
bot binding (:attr:`StrategyEntry.is_provisionable`). Everything else is
disabled in ``tw_bots`` with a zeroed sleeve while it stays in research — it
remains fully runnable and backtestable, it just risks nothing.
"""

from __future__ import annotations

from typing import Dict, Iterable, Tuple, Type

from src.strategies import Stage, StrategyEntry, all_strategies, bot_bound, by_stage
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
from src.tradeworkz.bots.gamma_flip_breaker import GammaFlipBreaker
from src.tradeworkz.bots.gamma_flip_defender import GammaFlipDefender
from src.tradeworkz.bots.gamma_regime_shift_rider import GammaRegimeShiftRider
from src.tradeworkz.bots.gex_gradient_drift import GexGradientDrift
from src.tradeworkz.bots.hedge_impulse_quiet_tape import HedgeImpulseQuietTape
from src.tradeworkz.bots.max_pain_gravitator import MaxPainGravitator
from src.tradeworkz.bots.opening_range_hunter import OpeningRangeHunter
from src.tradeworkz.bots.profile_shelf_breaker import ProfileShelfBreaker
from src.tradeworkz.bots.put_call_wall_bouncer import PutCallWallBouncer
from src.tradeworkz.bots.put_capitulation_credit_fade import PutCapitulationCreditFade
from src.tradeworkz.bots.put_wall_bouncer import PutWallBouncer
from src.tradeworkz.bots.put_wall_magnet_reversal import PutWallMagnetReversal
from src.tradeworkz.bots.range_iron_condor import RangeIronCondor
from src.tradeworkz.bots.settlement_flow_snap import SettlementFlowSnap
from src.tradeworkz.bots.spec_strategy import SpecStrategyBot
from src.tradeworkz.bots.vanna_vol_crush_rider import VannaVolCrushRider
from src.tradeworkz.bots.vix_regime_breakout import VixRegimeBreakout
from src.tradeworkz.bots.vol_expansion_straddle import VolExpansionStraddle
from src.tradeworkz.bots.vwap_reversion_scalper import VwapReversionScalper
from src.tradeworkz.bots.weekly_charm_grind import WeeklyCharmGrind
from src.tradeworkz.models import BotSpec

# ── Class resolution ───────────────────────────────────────────────────
# The one mapping this module still owns: catalog ``bot_class`` strings to the
# concrete implementations. Import-time explicit rather than dynamically
# discovered, so a typo in a catalog binding fails fast at startup (and in the
# catalog integrity test) instead of on the tick that first needs the class.
STRATEGY_CLASSES: Dict[str, Type[BaseBot]] = {
    "AggressorFlowDivergence": AggressorFlowDivergence,
    "BullMomentumClimber": BullMomentumClimber,
    "CallWallRejector": CallWallRejector,
    "CharmCloseMagnet": CharmCloseMagnet,
    "ClimaxFlowFade": ClimaxFlowFade,
    "DealerDeltaPressureRider": DealerDeltaPressureRider,
    "DualFlipDislocation": DualFlipDislocation,
    "EodPinDrifter": EodPinDrifter,
    "FreshFlowMomentum": FreshFlowMomentum,
    "GammaFlipBreaker": GammaFlipBreaker,
    "GammaFlipDefender": GammaFlipDefender,
    "GammaRegimeShiftRider": GammaRegimeShiftRider,
    "GexGradientDrift": GexGradientDrift,
    "HedgeImpulseQuietTape": HedgeImpulseQuietTape,
    "MaxPainGravitator": MaxPainGravitator,
    "OpeningRangeHunter": OpeningRangeHunter,
    "ProfileShelfBreaker": ProfileShelfBreaker,
    "PutCallWallBouncer": PutCallWallBouncer,
    "PutCapitulationCreditFade": PutCapitulationCreditFade,
    "PutWallBouncer": PutWallBouncer,
    "PutWallMagnetReversal": PutWallMagnetReversal,
    "RangeIronCondor": RangeIronCondor,
    "SettlementFlowSnap": SettlementFlowSnap,
    "VannaVolCrushRider": VannaVolCrushRider,
    "VixRegimeBreakout": VixRegimeBreakout,
    "VolExpansionStraddle": VolExpansionStraddle,
    "VwapReversionScalper": VwapReversionScalper,
    "WeeklyCharmGrind": WeeklyCharmGrind,
    # Not a catalog strategy: the data-driven backtest->live bridge bot. Its
    # behavior comes from a saved BacktestSpec.strategy carried in
    # ``tw_bots.params``, so it has no fixed thesis to catalog. Registered here
    # so the fleet loader can instantiate a user-deployed bot row.
    # See docs/design/tradeworkz-backtest-bridge.md.
    "spec_strategy": SpecStrategyBot,
}

#: ``STRATEGY_CLASSES`` keys that are infrastructure rather than catalog
#: strategies, and so are exempt from the catalog-coverage integrity check.
NON_CATALOG_STRATEGY_CLASSES: Tuple[str, ...] = ("spec_strategy",)


# ── Spec projection ────────────────────────────────────────────────────


def spec_for(entry: StrategyEntry) -> BotSpec:
    """Project one catalog entry onto the ``tw_bots`` row shape.

    ``description`` carries the catalog thesis: the market mechanism, stated
    once, so the bot card and the Backtesting picker cannot disagree about
    what a strategy claims. ``enabled`` follows the evidence gate, never a
    hand-set flag.
    """
    if entry.bot_class is None:
        raise ValueError(f"{entry.id} has no bot binding — cannot build a BotSpec")
    bot_id = entry.bot_id
    assert bot_id is not None  # implied by bot_class, narrows for type checkers
    return BotSpec(
        id=bot_id,
        display_name=entry.name,
        strategy_class=entry.bot_class,
        tier=entry.tier,
        direction_mode=entry.direction_mode,
        universe=entry.universe,
        tagline=entry.tagline,
        description=entry.thesis,
        params=entry.effective_bot_params(),
        is_public=entry.is_public,
        enabled=entry.is_provisionable,
    )


def _specs(entries: Iterable[StrategyEntry]) -> Tuple[BotSpec, ...]:
    return tuple(spec_for(e) for e in entries if e.bot_class is not None)


#: Every bot the catalog binds, whatever its stage. This is the full shipped
#: roster — what a "does every bot still satisfy X" invariant should check.
ALL_BOT_SPECS: Tuple[BotSpec, ...] = _specs(bot_bound())

#: Bots that HAVE shipped (a ``tw_bots`` row exists) and are no longer
#: live-eligible — the set the 2026-08-09 shelving created. Preserved under its
#: original name and meaning; :data:`ALL_BOT_SPECS` is the full roster.
SHELVED_SPECS: Tuple[BotSpec, ...] = _specs(
    e for e in bot_bound() if e.provisioned_history and not e.is_provisionable
)

#: Bots whose catalog entry is CANDIDATE — a screen shows promise but has not
#: cleared the promotion gate. Runnable and backtestable; never provisioned.
CANDIDATE_SPECS: Tuple[BotSpec, ...] = _specs(by_stage(Stage.CANDIDATE))

#: Bots cleared for a live capital sleeve: catalog stage VALIDATED with a bot
#: binding. EMPTY today — the one validated strategy in the catalog
#: (``gex_gradient_trend``) has no bot to execute it, which the catalog audit
#: reports as the highest-value gap to close. Nothing goes live on hope.
DEFAULT_ROSTER: Tuple[BotSpec, ...] = _specs(e for e in all_strategies() if e.is_provisionable)

#: Bot ids that must be force-disabled with a zeroed sleeve on provision:
#: bots that HAVE been provisioned at some point (so a ``tw_bots`` row exists)
#: and are no longer live-eligible. ``provision_defaults`` flips these to
#: ``enabled=false`` and zeroes their capital, so historical ``tw_trades``
#: rows survive (audit + leaderboard stay intact) but no capital rides on
#: them. Never delete an id — foreign keys point at it.
#:
#: A bot that never shipped is deliberately NOT here: it has no row to turn
#: off, and listing it would mean an operator who hand-inserts a candidate row
#: to trial it gets it disabled out from under them on the next provision.
#:
#: NOTE this is a DB-provisioning state, not the catalog's ``RETIRED`` stage.
#: Catalog retirement is terminal and requires five years of history under
#: ``src/strategies/policy.py``; nothing has reached it. A bot here is simply
#: not funded yet.
DISABLED_BOT_IDS: Tuple[str, ...] = tuple(
    e.bot_id for e in bot_bound() if e.provisioned_history and not e.is_provisionable and e.bot_id
) + (
    # Legacy symbol-specific variants, collapsed into their parents when the
    # fleet universe went from SPY-only to CSV. No catalog entry; the ids
    # survive only so their historical rows keep a valid foreign key.
    "qqq_gamma_flip_breaker",
    "qqq_dealer_delta_pressure_rider",
)

#: Deprecated alias for :data:`DISABLED_BOT_IDS`, kept for existing imports.
RETIRED_BOT_IDS: Tuple[str, ...] = DISABLED_BOT_IDS


def known_specs() -> Dict[str, BotSpec]:
    """Every bot-bound strategy, keyed by BOTH its legacy bot id and its
    canonical catalog id.

    The backtest harness (``_load_backtest_bots``) uses this to screen a bot
    that was never provisioned into ``tw_bots`` — which, while the fleet is
    unfunded, is all of them. Accepting either id is what lets an operator run
    ``--bots call_wall_fade`` (the id Backtesting and Insights show) or
    ``--bots call_wall_rejector`` (the id ``tw_trades`` holds) and get the same
    strategy.
    """
    specs: Dict[str, BotSpec] = {}
    for entry in bot_bound():
        spec = spec_for(entry)
        specs[spec.id] = spec
        specs.setdefault(entry.id, spec)
    return specs


def get_bot_class(name: str) -> Type[BaseBot]:
    """Resolve a ``strategy_class`` string to the concrete class."""
    try:
        return STRATEGY_CLASSES[name]
    except KeyError:
        raise KeyError(
            f"Unknown TradeWorkz strategy_class {name!r}; known: {sorted(STRATEGY_CLASSES)}"
        ) from None


def default_roster() -> Iterable[BotSpec]:
    return DEFAULT_ROSTER


def candidate_specs() -> Iterable[BotSpec]:
    """Backtest-gated edge candidates — registered/runnable, NOT provisioned."""
    return CANDIDATE_SPECS
