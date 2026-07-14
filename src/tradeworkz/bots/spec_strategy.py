"""SpecStrategyBot — a TradeWorkz bot defined by a saved backtest strategy.

This is the technical heart of the Backtesting↔TradeWorkz bridge. Every other
bot is a hand-written Python class; this one is **data-driven** — its behavior
comes from a stored ``BacktestSpec.strategy`` (the same condition rule a user
built and backtested). It evaluates those conditions against the *live*
:class:`MarketSnapshot` using the **exact same** ``_passes`` logic the backtest
uses over historical bars, so a strategy that was backtested behaves
identically when deployed live. That equivalence is the whole point: the
backtest is the promise, the live bot is the receipt.

Deploy flow (persistence / API / UI live elsewhere — see
``docs/design/tradeworkz-backtest-bridge.md``): a completed backtest's
``strategy`` block is stored in ``BotSpec.params``; the fleet engine ticks this
bot like any other, and it shows up on the leaderboard next to its backtest.

Scope (v1): **directional** structures (single ATM long, or a defined-risk
vertical) that exit on the base class's spot-level target/stop/time policy.
Neutral structures (straddle/strangle/condor) exit on option premium, which the
base exit policy doesn't model — those return no signal here and are a
follow-up. A condition on a field the live snapshot can't supply (e.g. MSI when
the context didn't attach it) makes the rule fail closed — the bot simply won't
fire — so a strategy is never traded on a half-evaluated rule.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

from src.backtesting.models import Condition
from src.backtesting.strategy import _passes
from src.tradeworkz.bots.base import BaseBot, _utcnow
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal

logger = logging.getLogger(__name__)

# Strategy fields that the live MarketSnapshot can populate. A saved strategy is
# only deployable-live when ALL its condition fields are in here (validated at
# deploy time); anything else can't be evaluated tick-to-tick yet.
LIVE_MAPPABLE_FIELDS = {
    "price",
    "net_gex",
    "net_gex_at_spot",
    "net_gex_sign",
    "gamma_flip_point",
    "flip_distance_pct",
    "call_wall",
    "put_wall",
    "dist_to_call_wall_pct",
    "dist_to_put_wall_pct",
    "put_call_ratio",
    "max_pain",
    "msi",
    "msi_regime",
}


def _snap_to_bar(snap: MarketSnapshot) -> dict:
    """Map a live snapshot to the field names the strategy conditions use.

    Mirrors the historical ``bar`` that ``src/backtesting/strategy.py`` builds,
    so ``_passes`` evaluates identically. Fields the snapshot can't supply are
    left out (a condition on a missing field then fails closed).
    """
    bar: dict = {"price": snap.spot}
    if snap.net_gex is not None:
        bar["net_gex"] = snap.net_gex
        # The live snapshot exposes one net-GEX figure; use it for the at-spot
        # field too (the analytics engine's net_gex is the regime-correct one).
        bar["net_gex_at_spot"] = snap.net_gex
        bar["net_gex_sign"] = (
            "positive" if snap.net_gex > 0 else "negative" if snap.net_gex < 0 else "zero"
        )
    if snap.gamma_flip is not None:
        bar["gamma_flip_point"] = snap.gamma_flip
        if snap.spot > 0:
            bar["flip_distance_pct"] = abs(snap.spot - snap.gamma_flip) / snap.spot * 100.0
    if snap.call_wall is not None:
        bar["call_wall"] = snap.call_wall
        if snap.spot > 0:
            bar["dist_to_call_wall_pct"] = (snap.call_wall - snap.spot) / snap.spot * 100.0
    if snap.put_wall is not None:
        bar["put_wall"] = snap.put_wall
        if snap.spot > 0:
            bar["dist_to_put_wall_pct"] = (snap.spot - snap.put_wall) / snap.spot * 100.0
    if snap.put_call_ratio is not None:
        bar["put_call_ratio"] = snap.put_call_ratio
    if snap.max_pain is not None:
        bar["max_pain"] = snap.max_pain
    # MSI rides the snapshot's `extra` escape hatch when the live context
    # attaches it (from signal_scores); absent otherwise.
    extra = snap.extra or {}
    if extra.get("msi") is not None:
        bar["msi"] = extra["msi"]
    if extra.get("msi_regime") is not None:
        bar["msi_regime"] = extra["msi_regime"]
    return bar


def strategy_field_names(strategy: dict) -> set:
    """The set of condition field names a saved strategy references."""
    return {str(c.get("field")) for c in (strategy.get("conditions") or []) if c.get("field")}


def is_live_deployable(strategy: dict) -> bool:
    """Whether every condition field can be evaluated against the live snapshot.

    Directional single/vertical only (neutral structures exit on premium, which
    the live exit policy doesn't model yet).
    """
    structure = str(strategy.get("structure") or "single")
    if structure not in ("single", "vertical"):
        return False
    if str(strategy.get("direction") or "") not in ("bullish", "bearish"):
        return False
    return strategy_field_names(strategy).issubset(LIVE_MAPPABLE_FIELDS)


class SpecStrategyBot(BaseBot):
    """A bot whose entries come from a saved backtest strategy's conditions."""

    tier = "0DTE"
    direction_mode = "context"
    tagline = "Your backtested strategy, trading live."
    description = (
        "Evaluates a saved custom-strategy rule against the live market and "
        "trades it forward — the same conditions you backtested, now out-of-sample."
    )

    def _strategy(self) -> dict:
        return dict(self.params.get("strategy") or {})

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        strategy = self._strategy()
        if not is_live_deployable(strategy):
            return None
        direction = strategy["direction"]  # 'bullish' | 'bearish' (validated above)

        # Same condition evaluation as the backtest — fail closed on any field
        # the snapshot couldn't supply.
        try:
            conditions = [Condition.from_dict(c) for c in strategy.get("conditions") or []]
        except Exception:  # pragma: no cover - a stored spec should already be valid
            logger.warning("spec_strategy: bad conditions in %s", self.spec.id, exc_info=True)
            return None
        if not conditions or not _passes(conditions, _snap_to_bar(snap)):
            return None

        if snap.spot <= 0:
            return None
        opt_type = "call" if direction == "bullish" else "put"
        strike = snap.round_to_strike(snap.spot)
        dte = int(strategy.get("entry", {}).get("dte", 0) or 0)
        expiration = (snap.et_date + timedelta(days=dte)).isoformat()

        structure = str(strategy.get("structure") or "single")
        if structure == "vertical":
            width = float(strategy.get("width") or 5.0)
            short = strike + width if direction == "bullish" else strike - width
            legs = self.build_vertical(snap.underlying, opt_type, strike, short, expiration)
            strategy_type = "BUY_CALL_SPREAD" if direction == "bullish" else "BUY_PUT_SPREAD"
        else:
            legs = self.build_atm_debit(snap.underlying, opt_type, strike, expiration, 0.0)
            strategy_type = "BUY_CALL_DEBIT" if direction == "bullish" else "BUY_PUT_DEBIT"

        # Spot-level target/stop from the strategy's fractional offsets (the base
        # class's exit policy compares these to spot).
        target = stop = None
        tgt_off = strategy.get("target_offset_pct")
        stop_off = strategy.get("stop_offset_pct")
        if tgt_off:
            target = (
                snap.spot * (1 + tgt_off) if direction == "bullish" else snap.spot * (1 - tgt_off)
            )
        if stop_off:
            stop = (
                snap.spot * (1 - stop_off) if direction == "bullish" else snap.spot * (1 + stop_off)
            )

        quality = float(self.params.get("base_quality", 0.6))
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None

        max_hold = int(self.params.get("max_hold_minutes", 120))
        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=direction,
            strategy_type=strategy_type,
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=target,
            stop_price=stop,
            time_stop_at=_utcnow() + timedelta(minutes=max_hold),
            rationale="Saved strategy conditions met live",
            components_at_entry={
                "net_gex": snap.net_gex,
                "gamma_flip": snap.gamma_flip,
                "spot": snap.spot,
            },
        )
