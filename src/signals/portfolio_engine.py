"""
Layer 2 of the ZeroGEX Signal Engine.

PortfolioEngine reads the current composite ScoreSnapshot and the current
state of open signal_trades, computes the optimal target portfolio, and
executes the minimum set of transactions to reach that target.

Key design principles:
- 100% cash is a first-class target state, not a fallback.
- Decisions are made holistically (what should the full portfolio look like?)
  not reactively (did this individual trade hit a stop?).
- Each transaction is recorded independently to signal_trades.
- Every reconciliation cycle is snapshotted to portfolio_snapshots.
"""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from src.config import (
    SIGNALS_ADVANCED_MIN_BASIC_CONFIRM,
    SIGNALS_ADVANCED_MIN_MSI_CONFIRM,
    SIGNALS_ADVANCED_REQUIRE_CONFIRMATION,
    SIGNALS_BREAKOUT_SIGNAL_SOURCES,
    SIGNALS_BREAKOUT_SIZE_MULTIPLIER,
    SIGNALS_BREAKOUT_TARGET_PCT,
    SIGNALS_CONFLUENCE_ADVANCED_WEIGHT,
    SIGNALS_CONFLUENCE_ENABLED,
    SIGNALS_CONFLUENCE_MIN_AGREE,
    SIGNALS_CONFLUENCE_MIN_NET_RATIO,
    SIGNALS_CONFLUENCE_MIN_OPINIONATED,
    SIGNALS_CONFLUENCE_MIN_STRENGTH,
    SIGNALS_CONVICTION_FLOOR,
    SIGNALS_DRS_CALL_ENTRY_MIN,
    SIGNALS_DRS_FRESH_CROSS_BOOST,
    SIGNALS_DRS_HARD_GATES_ENABLED,
    SIGNALS_DRS_OVERRIDE_ENABLED,
    SIGNALS_DRS_OVERRIDE_THRESHOLD,
    SIGNALS_DRS_PUT_ENTRY_MAX,
    SIGNALS_EXIT_THRESHOLD,
    SIGNALS_MAX_OPEN_TRADES,
    SIGNALS_MAX_PORTFOLIO_HEAT_PCT,
    SIGNALS_MIN_HOLD_SECONDS,
    SIGNALS_NO_0DTE_MORNING_MINUTES,
    SIGNALS_PORTFOLIO_SIZE,
    SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES,
    SIGNALS_SCALP_SIZE_MULTIPLIER,
    SIGNALS_STOP_LOSS_PCT,
    SIGNALS_TARGET_PCT,
    SIGNALS_TIME_STOP_MINUTES,
    SIGNALS_TRIGGER_THRESHOLD,
)
from src.signals import regime_filter
from src.database import db_connection
from src.signals.execution import leg_fill_price
from src.signals.position_optimizer_engine import (
    PositionOptimizerContext,
    PositionOptimizerEngine,
    TARGET_DTE_WINDOWS,
    fetch_option_snapshot,
)
from src.signals.advanced import AdvancedSignalResult
from src.signals.scoring_engine import ScoreSnapshot
from src.signals.strategy_builder import StrategyBuilder
from src.symbols import get_canonical_symbol
from src.utils import get_logger
from src.validation import ET, NYSE_HOLIDAYS

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class TargetPosition:
    direction: str  # 'bullish', 'bearish', 'neutral'
    strategy_type: str  # from optimizer, or 'cash' if no position
    contracts: int  # 0 = full cash
    option_symbol: str
    option_type: str
    expiration: date
    strike: float
    entry_mark: float
    probability_of_profit: float
    expected_value: float
    kelly_fraction: float
    optimizer_payload: dict


@dataclass
class PortfolioTarget:
    underlying: str
    timestamp: datetime
    composite_score: float
    normalized_score: float
    direction: str
    target_positions: list[TargetPosition]  # empty = 100% cash
    total_target_contracts: int
    target_heat_pct: float
    rationale: str  # human-readable explanation of why this target was chosen
    source: str = "composite"  # composite or advanced:<signal_name>


# ---------------------------------------------------------------------------
# PortfolioEngine
# ---------------------------------------------------------------------------


class PortfolioEngine:
    def __init__(self, underlying: str):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        self.position_optimizer = PositionOptimizerEngine(self.underlying)
        self.strategy_builder = StrategyBuilder(self.underlying)

        # Config constants
        self.max_open_trades = SIGNALS_MAX_OPEN_TRADES
        self.max_heat_pct = SIGNALS_MAX_PORTFOLIO_HEAT_PCT
        self.cooldown_minutes = SIGNALS_SAME_DIRECTION_COOLDOWN_MINUTES
        self.drs_hard_gates_enabled = SIGNALS_DRS_HARD_GATES_ENABLED
        self.drs_call_entry_min = SIGNALS_DRS_CALL_ENTRY_MIN
        self.drs_put_entry_max = SIGNALS_DRS_PUT_ENTRY_MAX
        self.drs_override_enabled = SIGNALS_DRS_OVERRIDE_ENABLED
        self.drs_override_threshold = SIGNALS_DRS_OVERRIDE_THRESHOLD
        self.drs_fresh_cross_boost = SIGNALS_DRS_FRESH_CROSS_BOOST
        self.scalp_size_multiplier = SIGNALS_SCALP_SIZE_MULTIPLIER
        self.entry_threshold = SIGNALS_TRIGGER_THRESHOLD
        self.exit_threshold = SIGNALS_EXIT_THRESHOLD
        self.conviction_floor = SIGNALS_CONVICTION_FLOOR
        self.min_hold_seconds = SIGNALS_MIN_HOLD_SECONDS
        self.target_pct = SIGNALS_TARGET_PCT
        self.time_stop_minutes = SIGNALS_TIME_STOP_MINUTES
        self.stop_loss_pct = SIGNALS_STOP_LOSS_PCT

    _ADVANCED_SIGNAL_DIRECTION_MAP = {
        "squeeze_setup": {
            "bullish_squeeze": "bullish",
            "bearish_squeeze": "bearish",
        },
        "trap_detection": {
            "bullish_fade": "bullish",
            "bearish_fade": "bearish",
        },
        "zero_dte_position_imbalance": {
            "call_heavy": "bullish",
            "put_heavy": "bearish",
        },
        "gamma_vwap_confluence": {
            "bullish_confluence": "bullish",
            "bearish_confluence": "bearish",
        },
        "vol_expansion": {
            "bullish_expansion": "bullish",
            "bearish_expansion": "bearish",
        },
        "eod_pressure": {
            "bullish": "bullish",
            "bearish": "bearish",
        },
        "range_break_imminence": {
            "bullish_break_imminent": "bullish",
            "bearish_break_imminent": "bearish",
        },
    }

    _ADVANCED_MIN_ABS_SCORE = 0.25

    @staticmethod
    def _market_status(dt: Optional[datetime] = None) -> str:
        """Return OPEN only during regular options trading hours (09:30-16:15 ET)."""
        if dt is None:
            dt = datetime.now(ET)
        elif dt.tzinfo is None:
            dt = ET.localize(dt)
        else:
            dt = dt.astimezone(ET)

        if dt.weekday() > 4 or dt.date() in NYSE_HOLIDAYS:
            return "CLOSED"

        current_time = dt.time()
        market_open = datetime.strptime("09:30:00", "%H:%M:%S").time()
        market_close = datetime.strptime("16:15:00", "%H:%M:%S").time()
        return "OPEN" if market_open <= current_time <= market_close else "CLOSED"

    # ------------------------------------------------------------------
    # Connection helper
    # ------------------------------------------------------------------

    @staticmethod
    @contextmanager
    def _use_conn(conn=None):
        if conn is not None:
            yield conn
        else:
            with db_connection() as new_conn:
                yield new_conn

    # ------------------------------------------------------------------
    # compute_target — pure function of score + market context
    # ------------------------------------------------------------------

    def compute_target(
        self, score: ScoreSnapshot, market_ctx: dict, conn=None, cached_option_rows=None
    ) -> PortfolioTarget:
        """Return the optimal portfolio state given the current score.

        This method never reads signal_trades — it is purely a function of
        the score and market context.  All trade-awareness lives in reconcile().
        """
        msi = float(score.composite_score or 0.0)
        regime = self._resolve_regime(score)
        trade_direction = self._resolve_trade_direction(score, market_ctx, regime)
        # score.normalized_score stores MSI/100 in the new scoring engine.
        conviction = float(score.normalized_score or 0.0)

        # Hard wait regime.
        if regime == "high_risk_reversal":
            return self._cash_target(
                score,
                f"MSI {msi:.1f} in high-risk reversal regime: wait",
            )

        if trade_direction not in {"bullish", "bearish"}:
            return self._cash_target(
                score,
                f"MSI {msi:.1f} regime={regime} lacks directional trend signal",
            )

        # Asymmetric trigger / hysteresis: require strict entry threshold to
        # open a fresh position; once we already hold the same direction, fall
        # back to the looser exit threshold so a 1-2 point MSI dip doesn't
        # whipsaw us out. Implementation tolerates MagicMock conns (tests) by
        # treating any failure as "no open position".
        #
        # Advanced-signal and confluence entry paths bypass this gate — they
        # are independent triggers that self-gate via min-score and confluence
        # ratio filters before reaching compute_target.
        agg = score.aggregation or {}
        is_signal_driven = bool(
            agg.get("advanced_trigger") or agg.get("confluence_trigger")
        )
        if not is_signal_driven:
            held_direction = self._current_position_direction(conn)
            if held_direction == trade_direction:
                effective_threshold = self.exit_threshold
                threshold_label = "exit"
            else:
                effective_threshold = self.entry_threshold
                threshold_label = "entry"
            if conviction < effective_threshold:
                return self._cash_target(
                    score,
                    f"Conviction {conviction:.2f} below {threshold_label} threshold "
                    f"{effective_threshold:.2f} (held={held_direction})",
                )

        # Determine sizing mode by regime.
        if regime == "trend_expansion":
            tier_label = "ride"
            size_multiplier = 1.0
        elif regime == "controlled_trend":
            tier_label = "intraday"
            size_multiplier = 0.75
        else:  # chop_range
            tier_label = "scalp"
            size_multiplier = min(self.scalp_size_multiplier, 0.6)
        is_scalp = tier_label == "scalp"

        # --- CASE 2: trend confirmation ---
        if regime in {"trend_expansion", "controlled_trend"} and not self._score_trend_confirmation(
            score,
            market_ctx,
            conn=conn,
        ):
            return self._cash_target(
                score,
                f"Trend confirmation failed for regime={regime}",
            )

        # Keep DRS gates for directional entries.
        drs_override_active = (
            self.drs_override_enabled and not is_scalp and conviction >= self.drs_override_threshold
        )
        if is_scalp or drs_override_active:
            passes_drs_gate, drs_reason = True, (
                "DRS hard gate bypassed: scalp tier"
                if is_scalp
                else f"DRS hard gate bypassed: conviction {conviction:.3f} "
                f">= override {self.drs_override_threshold:.2f}"
            )
        else:
            passes_drs_gate, drs_reason = self._passes_dealer_regime_gates(score, market_ctx)
        if not passes_drs_gate:
            return self._cash_target(score, drs_reason)

        # Regime-aware timeframe selection.
        if regime == "trend_expansion":
            forced_timeframe = "swing"
        elif regime == "controlled_trend":
            forced_timeframe = "intraday"
        else:
            forced_timeframe = "intraday"
        candidate_result = self._select_optimizer_candidate(
            score,
            market_ctx,
            signal_direction=trade_direction,
            conn=conn,
            cached_option_rows=cached_option_rows,
            forced_timeframe=forced_timeframe,
        )
        if not candidate_result:
            return self._cash_target(
                score,
                "No positive-EV structure available",
            )

        candidate = candidate_result["candidate"]
        sizing = next((p for p in candidate.sizing_profiles if p.profile == "optimal"), None)
        if not sizing or sizing.contracts <= 0:
            return self._cash_target(
                score,
                "No positive-EV structure available",
            )

        # Combined kelly × conviction floor: rejects technically-positive-EV
        # candidates whose effective sizing edge is microscopic. Without this
        # we fire on every weak signal and pay slippage for ~zero edge.
        # Advanced/confluence-driven scores carry their own gating upstream;
        # don't double-block them here.
        if not is_signal_driven:
            edge_proxy = float(candidate.kelly_fraction or 0.0) * conviction
            if edge_proxy < self.conviction_floor:
                return self._cash_target(
                    score,
                    f"Edge proxy kelly*conviction {edge_proxy:.3f} below floor "
                    f"{self.conviction_floor:.3f}",
                )

        # --- CASE 4: compute target contracts via Kelly sizing ---
        fresh_cross = self._fresh_drs_cross(trade_direction, market_ctx)
        cross_multiplier = (
            1.0 + self.drs_fresh_cross_boost
            if fresh_cross and self.drs_fresh_cross_boost > 0
            else 1.0
        )
        base_contracts = sizing.contracts
        contracts = max(
            1,
            int(base_contracts * conviction * size_multiplier * cross_multiplier),
        )

        entry_price = (candidate.entry_debit or candidate.entry_credit) / 100.0
        # Resolve option symbol for the primary leg
        legs = list(candidate.legs or [])
        if not legs:
            legs = self._legs_from_candidate(
                {
                    "strategy_type": candidate.strategy_type,
                    "strikes": candidate.strikes,
                    "expiry": candidate.expiry,
                }
            )
        enriched_legs = []
        with self._use_conn(conn) as c:
            for leg in legs:
                option_symbol = self._resolve_option_symbol_for_leg(score.timestamp, leg, conn=c)
                enriched_legs.append({**leg, "option_symbol": option_symbol})

        primary_symbol = (
            enriched_legs[0]["option_symbol"] if enriched_legs else f"{self.db_symbol}-SYNTHETIC"
        )
        primary_type = (
            str(enriched_legs[0].get("option_type", "")).upper()
            if enriched_legs
            else ("C" if trade_direction == "bullish" else "P")
        )

        optimizer_payload = {
            "strategy_type": candidate.strategy_type,
            "pricing_mode": "debit" if candidate.entry_debit > 0 else "credit",
            "strikes": candidate.strikes,
            "expiry": str(candidate.expiry),
            "legs": enriched_legs,
            "probability_of_profit": candidate.probability_of_profit,
            "expected_value": candidate.expected_value,
            "signal_timeframe": candidate_result["signal_timeframe"],
            "signal_strength": candidate_result["signal_strength"],
            "trade_type": candidate_result.get("trade_type"),
            "strategy_regime": candidate_result.get("strategy_regime"),
            "strategy_regime_score": candidate_result.get("strategy_regime_score"),
            "strategy_diagnostics": candidate_result.get("strategy_diagnostics") or {},
            "tier": tier_label,
            "size_multiplier": size_multiplier,
        }

        target_heat = abs(entry_price) * contracts * 100 / max(SIGNALS_PORTFOLIO_SIZE, 1.0)

        tp = TargetPosition(
            direction=trade_direction,
            strategy_type=candidate.strategy_type,
            contracts=contracts,
            option_symbol=primary_symbol,
            option_type=primary_type,
            expiration=candidate.expiry,
            strike=(
                round(float(enriched_legs[0]["strike"]), 4)
                if enriched_legs
                else round(float(market_ctx["close"]), 4)
            ),
            entry_mark=entry_price,
            probability_of_profit=candidate.probability_of_profit,
            expected_value=candidate.expected_value,
            kelly_fraction=candidate.kelly_fraction,
            optimizer_payload=optimizer_payload,
        )

        rationale = f"MSI {msi:.1f} regime={regime} dir={trade_direction} [{tier_label}], " f"{candidate.strategy_type} {contracts}c Kelly={candidate.kelly_fraction:.1%}" + (
            f" regime={candidate_result.get('strategy_regime')}:{candidate_result.get('strategy_regime_score', 0):.2f}"
            if candidate_result.get("strategy_regime")
            else ""
        ) + (
            " (DRS override)" if drs_override_active else ""
        ) + (
            " (fresh-cross boost)" if fresh_cross and cross_multiplier > 1.0 else ""
        )

        return PortfolioTarget(
            underlying=self.db_symbol,
            timestamp=score.timestamp,
            composite_score=score.composite_score,
            normalized_score=score.normalized_score,
            direction=trade_direction,
            target_positions=[tp],
            total_target_contracts=contracts,
            target_heat_pct=round(target_heat, 6),
            rationale=rationale,
        )

    def compute_target_with_advanced_signals(
        self,
        score: ScoreSnapshot,
        market_ctx: dict,
        advanced_results: list[AdvancedSignalResult],
        basic_results: Optional[list[AdvancedSignalResult]] = None,
        conn=None,
        cached_option_rows=None,
    ) -> PortfolioTarget:
        """Primary MSI target with signal-based opportunity gating.

        Entry paths, in priority order:
          1. Strongest individual advanced signal (discrete trigger fires).
          2. Cross-signal confluence across Basic + Advanced families: if
             enough signals agree on direction with sufficient aggregated
             strength, that consensus is itself an entry signal.
        """
        composite_target = self.compute_target(
            score,
            market_ctx,
            conn=conn,
            cached_option_rows=cached_option_rows,
        )
        entry_target = self._build_advanced_target(
            score,
            market_ctx,
            advanced_results=advanced_results,
            basic_results=basic_results or [],
            conn=conn,
            cached_option_rows=cached_option_rows,
        )

        if entry_target is None:
            entry_target = self._build_confluence_target(
                score,
                market_ctx,
                advanced_results=advanced_results,
                basic_results=basic_results or [],
                conn=conn,
                cached_option_rows=cached_option_rows,
            )

        # Require either a triggered advanced signal or a strong confluence.
        if entry_target is None:
            return self._cash_target(
                score,
                "No advanced signal setup or confluence confirmed; stay in cash",
            )

        # Phase 2.4: regime/event filter on *new* entries. Same-direction
        # holds bypass — only fresh entries (no held position OR opposite
        # direction) are subject to lunch chop / late-close / event windows.
        held_direction = self._current_position_direction(conn)
        is_fresh_entry = held_direction != entry_target.direction
        if is_fresh_entry:
            decision = regime_filter.evaluate(
                timestamp=score.timestamp,
                msi_conviction=float(score.normalized_score or 0.0),
                signal_source=entry_target.source,
            )
            if decision.skip:
                return self._cash_target(
                    score,
                    f"Regime filter: {decision.reason}",
                )

        # Hard do-not-fade policy in destabilizing, no-anchor conditions.
        if self._do_not_fade(market_ctx):
            trend_dir = self._msi_trend_direction(market_ctx)
            if (
                trend_dir in {"bullish", "bearish"}
                and entry_target.direction in {"bullish", "bearish"}
                and trend_dir != entry_target.direction
            ):
                return self._cash_target(
                    score,
                    "Do-not-fade policy active; skipping counter-trend setup",
                )

        # In expansion regime, prefer ride-move alignment with MSI trend.
        if score.composite_score >= 70.0:
            trend_dir = self._msi_trend_direction(market_ctx)
            if (
                trend_dir in {"bullish", "bearish"}
                and entry_target.direction in {"bullish", "bearish"}
                and trend_dir != entry_target.direction
            ):
                return self._cash_target(
                    score,
                    "Expansion regime: advanced setup opposes prevailing trend",
                )

        return entry_target

    def _build_advanced_target(
        self,
        score: ScoreSnapshot,
        market_ctx: dict,
        advanced_results: list[AdvancedSignalResult],
        basic_results: Optional[list[AdvancedSignalResult]] = None,
        conn=None,
        cached_option_rows=None,
    ) -> Optional[PortfolioTarget]:
        strongest = self._strongest_advanced_signal(advanced_results)
        if strongest is None:
            return None

        signal_result, signal_direction = strongest

        # Phase 2.3: a single advanced trigger isn't enough. Require at least
        # one independent confirmation (another triggered advanced, a basic
        # above the basic cutoff, or MSI conviction above the MSI cutoff)
        # in the same direction before sizing a position.  Confluence-driven
        # entries already self-gate via SIGNALS_CONFLUENCE_MIN_AGREE.
        confirmation = self._evaluate_advanced_confirmation(
            primary=signal_result,
            primary_direction=signal_direction,
            advanced_results=advanced_results,
            basic_results=basic_results or [],
            score=score,
            market_ctx=market_ctx,
        )
        if not confirmation["passed"]:
            return None

        synthetic_score = self._build_signal_snapshot_for_advanced(
            base=score,
            signal_result=signal_result,
            direction=signal_direction,
        )
        if synthetic_score.direction == "neutral":
            return None

        target = self.compute_target(
            synthetic_score,
            market_ctx,
            conn=conn,
            cached_option_rows=cached_option_rows,
        )
        if not target.target_positions:
            return None

        target.source = f"advanced:{signal_result.name}"
        confirm_label = confirmation.get("label") or "no-confirm-required"

        boost_label = self._apply_breakout_boost(target, signal_result.name)

        target.rationale = (
            f"Advanced {signal_result.name} score={signal_result.score:.3f} triggered "
            f"[{confirm_label}]"
            + (f" [{boost_label}]" if boost_label else "")
            + ", "
            + target.rationale
        )
        return target

    @staticmethod
    def _apply_breakout_boost(
        target: PortfolioTarget, signal_name: str
    ) -> Optional[str]:
        """Apply Phase 3.2 sizing/target boost when the trigger is a directional-
        expansion advanced signal (range_break_imminence, squeeze_setup, etc.).

        Mutates ``target`` in place: scales contracts and stamps a per-trade
        target_pct override into the optimizer payload so the risk plan widens
        the take-profit on this specific trade.  Returns a short human-readable
        label for the rationale, or ``None`` when no boost was applied.
        """
        eligible = {name.lower() for name in SIGNALS_BREAKOUT_SIGNAL_SOURCES}
        if signal_name.lower() not in eligible:
            return None
        if not target.target_positions:
            return None

        size_mult = float(SIGNALS_BREAKOUT_SIZE_MULTIPLIER)
        widen_target_pct = float(SIGNALS_BREAKOUT_TARGET_PCT)

        boosted_total = 0
        for tp in target.target_positions:
            scaled = max(int(round(tp.contracts * size_mult)), tp.contracts)
            tp.contracts = scaled
            payload = dict(tp.optimizer_payload or {})
            risk_overrides = dict(payload.get("risk_overrides") or {})
            if widen_target_pct > 0:
                risk_overrides["target_pct"] = widen_target_pct
            risk_overrides["breakout_boost"] = {
                "signal": signal_name,
                "size_multiplier": size_mult,
                "target_pct": widen_target_pct,
            }
            payload["risk_overrides"] = risk_overrides
            tp.optimizer_payload = payload
            boosted_total += scaled

        target.total_target_contracts = boosted_total
        if target.target_positions:
            primary = target.target_positions[0]
            target.target_heat_pct = round(
                abs(primary.entry_mark) * boosted_total * 100
                / max(SIGNALS_PORTFOLIO_SIZE, 1.0),
                6,
            )
        return f"breakout-boost x{size_mult:.2f} target={widen_target_pct:.0%}"

    @classmethod
    def _evaluate_advanced_confirmation(
        cls,
        *,
        primary: AdvancedSignalResult,
        primary_direction: str,
        advanced_results: list[AdvancedSignalResult],
        basic_results: list[AdvancedSignalResult],
        score: ScoreSnapshot,
        market_ctx: dict,
    ) -> dict:
        """Decide whether the primary advanced signal has independent backing.

        Returns ``{"passed": bool, "label": str}``.  When confirmation isn't
        required (env knob off) the gate auto-passes with label="disabled".
        """
        if not SIGNALS_ADVANCED_REQUIRE_CONFIRMATION:
            return {"passed": True, "label": "confirm-disabled"}

        confirmations: list[str] = []

        # Another advanced signal triggered in the same direction.
        for result in advanced_results or []:
            if result is primary:
                continue
            triggered = bool((result.context or {}).get("triggered", False))
            if not triggered:
                continue
            if cls._resolve_advanced_direction(result) != primary_direction:
                continue
            if abs(float(result.score)) < 0.25:
                continue
            confirmations.append(f"adv:{result.name}")
            break

        # Basic signal score in same direction above the basic cutoff.
        basic_cutoff = float(SIGNALS_ADVANCED_MIN_BASIC_CONFIRM)
        for result in basic_results or []:
            score_val = float(result.score or 0.0)
            if abs(score_val) < basic_cutoff:
                continue
            direction = cls._resolve_basic_direction(result)
            if direction != primary_direction:
                continue
            confirmations.append(f"basic:{result.name}")
            break

        # MSI trend agrees and conviction clears the MSI cutoff.
        msi_cutoff = float(SIGNALS_ADVANCED_MIN_MSI_CONFIRM)
        msi_conviction = float(score.normalized_score or 0.0)
        msi_direction = cls._msi_trend_direction(market_ctx)
        if msi_conviction >= msi_cutoff and msi_direction == primary_direction:
            confirmations.append(f"msi:{msi_conviction:.2f}")

        if not confirmations:
            return {"passed": False, "label": "no-confirmation"}
        return {"passed": True, "label": "confirm=" + "+".join(confirmations)}

    def _strongest_advanced_signal(
        self,
        advanced_results: list[AdvancedSignalResult],
    ) -> Optional[tuple[AdvancedSignalResult, str]]:
        ranked: list[tuple[AdvancedSignalResult, str]] = []
        for result in advanced_results or []:
            direction = self._resolve_advanced_direction(result)
            if direction == "neutral":
                continue
            triggered = bool((result.context or {}).get("triggered", False))
            if not triggered:
                continue
            if abs(float(result.score)) < 0.25:
                continue
            ranked.append((result, direction))

        if not ranked:
            return None

        ranked.sort(
            key=lambda item: abs(item[0].score),
            reverse=True,
        )
        return ranked[0]

    def _build_confluence_target(
        self,
        score: ScoreSnapshot,
        market_ctx: dict,
        advanced_results: list[AdvancedSignalResult],
        basic_results: list[AdvancedSignalResult],
        conn=None,
        cached_option_rows=None,
    ) -> Optional[PortfolioTarget]:
        """Build an entry target from cross-signal confluence.

        Fires when enough Basic + Advanced signals agree on direction with
        sufficient aggregated strength, even if no single advanced signal
        individually triggered.  Uses advanced signals' explicit direction
        map and basic signals' score sign to vote.
        """
        confluence = self._signal_confluence(advanced_results, basic_results)
        if confluence is None:
            return None

        direction = confluence["direction"]
        magnitude = confluence["magnitude"]
        synthetic_score = ScoreSnapshot(
            timestamp=score.timestamp,
            underlying=score.underlying,
            composite_score=round(score.composite_score, 6),
            normalized_score=round(magnitude, 6),
            direction=direction,
            components=dict(score.components or {}),
            aggregation={
                **(score.aggregation or {}),
                "confluence_trigger": True,
                "confluence_direction": direction,
                "confluence_agree": confluence["agree"],
                "confluence_disagree": confluence["disagree"],
                "confluence_strength": round(confluence["strength"], 6),
                "confluence_net_ratio": round(confluence["net_ratio"], 6),
                "confluence_contributors": confluence["contributors"],
            },
        )

        target = self.compute_target(
            synthetic_score,
            market_ctx,
            conn=conn,
            cached_option_rows=cached_option_rows,
        )
        if not target.target_positions:
            return None

        target.source = "confluence"
        contributors = ",".join(confluence["contributors"])
        target.rationale = (
            f"Confluence {direction} agree={confluence['agree']}/"
            f"{confluence['opinionated']} net={confluence['net_ratio']:.2f} "
            f"strength={confluence['strength']:.2f} [{contributors}], " + target.rationale
        )
        return target

    @classmethod
    def _signal_confluence(
        cls,
        advanced_results: list[AdvancedSignalResult],
        basic_results: list[AdvancedSignalResult],
    ) -> Optional[dict]:
        """Compute direction/strength agreement across Basic + Advanced signals.

        Returns ``None`` when confluence is disabled, no signals are
        opinionated, or the agreement doesn't clear the configured thresholds.
        """
        if not SIGNALS_CONFLUENCE_ENABLED:
            return None

        min_opinion = float(SIGNALS_CONFLUENCE_MIN_OPINIONATED)
        adv_weight = float(SIGNALS_CONFLUENCE_ADVANCED_WEIGHT)

        votes: dict[str, dict] = {
            "bullish": {"count": 0, "strength": 0.0, "names": []},
            "bearish": {"count": 0, "strength": 0.0, "names": []},
        }
        opinionated = 0

        def _ingest(result, direction_resolver, weight: float) -> None:
            nonlocal opinionated
            score_abs = abs(float(result.score))
            if score_abs < min_opinion:
                return
            direction = direction_resolver(result)
            if direction not in {"bullish", "bearish"}:
                return
            opinionated += 1
            bucket = votes[direction]
            bucket["count"] += 1
            bucket["strength"] += score_abs * weight
            bucket["names"].append(result.name)

        for result in advanced_results or []:
            _ingest(result, cls._resolve_advanced_direction, adv_weight)
        for result in basic_results or []:
            _ingest(result, cls._resolve_basic_direction, 1.0)

        if opinionated == 0:
            return None

        bull = votes["bullish"]
        bear = votes["bearish"]
        if bull["count"] >= bear["count"]:
            winner, direction = bull, "bullish"
            disagree = int(bear["count"])
        else:
            winner, direction = bear, "bearish"
            disagree = int(bull["count"])

        agree = int(winner["count"])
        strength = float(winner["strength"])
        net_ratio = (agree - disagree) / opinionated if opinionated > 0 else 0.0

        if agree < SIGNALS_CONFLUENCE_MIN_AGREE:
            return None
        if net_ratio < SIGNALS_CONFLUENCE_MIN_NET_RATIO:
            return None
        if strength < SIGNALS_CONFLUENCE_MIN_STRENGTH:
            return None

        magnitude = max(0.0, min(1.0, strength / max(float(agree), 1.0)))
        return {
            "direction": direction,
            "agree": agree,
            "disagree": disagree,
            "opinionated": opinionated,
            "strength": strength,
            "net_ratio": net_ratio,
            "magnitude": magnitude,
            "contributors": list(winner["names"]),
        }

    # Backward compatibility for callers still using legacy naming.
    def compute_target_with_independents(
        self,
        score: ScoreSnapshot,
        market_ctx: dict,
        independent_results: list[AdvancedSignalResult],
        basic_results: Optional[list[AdvancedSignalResult]] = None,
        conn=None,
        cached_option_rows=None,
    ) -> PortfolioTarget:
        return self.compute_target_with_advanced_signals(
            score=score,
            market_ctx=market_ctx,
            advanced_results=independent_results,
            basic_results=basic_results,
            conn=conn,
            cached_option_rows=cached_option_rows,
        )

    @classmethod
    def _resolve_advanced_direction(cls, result: AdvancedSignalResult) -> str:
        signal_name = result.name
        signal_value = str((result.context or {}).get("signal", "")).lower()
        mapping = cls._ADVANCED_SIGNAL_DIRECTION_MAP.get(signal_name, {})
        if signal_value in mapping:
            return mapping[signal_value]
        if result.score > 0:
            return "bullish"
        if result.score < 0:
            return "bearish"
        return "neutral"

    @staticmethod
    def _resolve_basic_direction(result: AdvancedSignalResult) -> str:
        """Basic signals are continuous: direction = sign of score."""
        score = float(result.score or 0.0)
        if score > 0:
            return "bullish"
        if score < 0:
            return "bearish"
        return "neutral"

    @staticmethod
    def _build_signal_snapshot_for_advanced(
        base: ScoreSnapshot,
        signal_result: AdvancedSignalResult,
        direction: str,
    ) -> ScoreSnapshot:
        magnitude = max(0.0, min(1.0, abs(float(signal_result.score))))
        components = dict(base.components or {})
        components[f"advanced:{signal_result.name}"] = {
            "weight": 0.0,
            "effective_weight": 0.0,
            "score": float(signal_result.score),
        }
        return ScoreSnapshot(
            timestamp=base.timestamp,
            underlying=base.underlying,
            composite_score=round(base.composite_score, 6),
            normalized_score=round(magnitude, 6),
            direction=direction,
            components=components,
            aggregation={
                **(base.aggregation or {}),
                "advanced_trigger": signal_result.name,
                "advanced_score": round(float(signal_result.score), 6),
                "advanced_direction": direction,
            },
        )

    @staticmethod
    def _do_not_fade(market_ctx: dict) -> bool:
        net_gex = float(market_ctx.get("net_gex") or 0.0)
        close = float(market_ctx.get("close") or 0.0)
        gamma_flip = market_ctx.get("gamma_flip")
        max_gamma = market_ctx.get("max_gamma_strike")
        if close <= 0:
            return False
        if net_gex >= 0:
            return False

        far_from_max = False
        if max_gamma is not None:
            try:
                far_from_max = abs((close - float(max_gamma)) / close) >= 0.012
            except (TypeError, ValueError, ZeroDivisionError):
                far_from_max = False

        flip_not_near = True
        if gamma_flip is not None:
            try:
                flip_not_near = abs((close - float(gamma_flip)) / close) >= 0.006
            except (TypeError, ValueError, ZeroDivisionError):
                flip_not_near = True

        return bool(far_from_max and flip_not_near)

    @staticmethod
    def _msi_trend_direction(market_ctx: dict) -> str:
        closes = market_ctx.get("recent_closes") or []
        if len(closes) < 3:
            return "neutral"
        try:
            start = float(closes[-3])
            end = float(closes[-1])
        except (TypeError, ValueError):
            return "neutral"
        if start <= 0:
            return "neutral"
        move = (end - start) / start
        if move > 0.0005:
            return "bullish"
        if move < -0.0005:
            return "bearish"
        return "neutral"

    # ------------------------------------------------------------------
    # reconcile — reads actual state, computes delta, executes
    # ------------------------------------------------------------------

    def reconcile(self, target: PortfolioTarget, conn=None) -> str:
        """Compare target portfolio to actual holdings and execute the delta.

        All writes (close/open/mark/snapshot) are issued without intermediate
        commits so the entire reconciliation is atomic.  When this function
        opens its own connection, ``db_connection()`` commits once on success
        and rolls back on exception.  When a caller supplies ``conn``, the
        caller owns the transaction boundary.

        Returns an action string for logging.
        """
        with self._use_conn(conn) as c:
            open_trades = self._fetch_open_trades(c)
            actual_contracts = sum(t["quantity_open"] for t in open_trades)
            actual_direction = self._majority_direction(open_trades)
            open_trade_count = len(open_trades)

            market_status = self._market_status()
            if market_status != "OPEN":
                for trade in open_trades:
                    self._update_trade_mark(trade, target.timestamp, c)
                action = "held_market_closed"
                action_detail = {
                    "market_status": market_status,
                    "reason": "Signal trades are only allowed during OPEN market status (09:30-16:15 ET).",
                    "target_direction": target.direction,
                    "target_contracts": target.total_target_contracts,
                    "actual_contracts": actual_contracts,
                    "actual_direction": actual_direction,
                }
                self.snapshot(target, action, action_detail, open_trades, c)
                return action

            # Phase 1.2: per-trade stop/target/time-stop sweep.  Plan exits
            # ALWAYS fire — they precede the target-driven CASE ladder so a
            # mid-cycle target flip can't pre-empt a hit stop or take-profit.
            plan_closed = self._sweep_plan_exits(open_trades, target.timestamp, c)
            if plan_closed:
                # Refresh state so direction-reversal/trim logic operates on
                # what's actually still open after plan exits.
                open_trades = self._fetch_open_trades(c)
                actual_contracts = sum(t["quantity_open"] for t in open_trades)
                actual_direction = self._majority_direction(open_trades)
                open_trade_count = len(open_trades)

            target_contracts = target.total_target_contracts
            target_direction = target.direction if target.target_positions else "neutral"

            action = "held"
            action_detail: dict = {}

            # CASE A: target is 100% cash
            if not target.target_positions:
                if open_trades:
                    closed_count = 0
                    held_min_hold = 0
                    closed_pnl = 0.0
                    for trade in open_trades:
                        if self._min_hold_active(trade, target.timestamp):
                            self._update_trade_mark(trade, target.timestamp, c)
                            held_min_hold += 1
                            continue
                        pnl = self._close_trade(trade, target.timestamp, c)
                        closed_pnl += pnl
                        closed_count += 1
                    if closed_count == 0:
                        action = "held_min_hold"
                    elif held_min_hold > 0:
                        action = "closed_partial_min_hold"
                    else:
                        action = "closed_all"
                    action_detail = {
                        "closed_count": closed_count,
                        "min_hold_protected": held_min_hold,
                        "realized_pnl": round(closed_pnl, 4),
                        "reason": target.rationale,
                    }
                else:
                    action = "cash"
                    action_detail = {"reason": target.rationale}

            # CASE B: direction reversal
            elif actual_contracts > 0 and actual_direction != target_direction:
                # Min-hold blocks the entire reversal — half-flipping the book
                # creates conflicting net deltas.  We hold until protected
                # trades age out, then reverse on the next cycle.
                if any(self._min_hold_active(t, target.timestamp) for t in open_trades):
                    for trade in open_trades:
                        self._update_trade_mark(trade, target.timestamp, c)
                    action = "held_min_hold_reversal"
                    action_detail = {
                        "current_direction": actual_direction,
                        "target_direction": target_direction,
                        "current_contracts": actual_contracts,
                        "target_contracts": target_contracts,
                    }
                else:
                    closed_pnl = 0.0
                    for trade in open_trades:
                        pnl = self._close_trade(trade, target.timestamp, c)
                        closed_pnl += pnl
                    if target_contracts > 0:
                        self._open_position(target.target_positions[0], target, c)
                    action = "reversed"
                    action_detail = {
                        "old_direction": actual_direction,
                        "new_direction": target_direction,
                        "closed_contracts": actual_contracts,
                        "opened_contracts": target_contracts,
                        "closed_pnl": round(closed_pnl, 4),
                    }

            # CASE C: same direction, adjust size
            elif actual_contracts > 0 and actual_direction == target_direction:
                contracts_delta = target_contracts - actual_contracts
                if contracts_delta > 0:
                    if open_trade_count >= self.max_open_trades:
                        action = "held_max_open_trades"
                        action_detail = {
                            "reason": "Max open trade slots reached",
                            "max_open_trades": self.max_open_trades,
                            "open_trade_count": open_trade_count,
                            "target_contracts": target_contracts,
                            "actual_contracts": actual_contracts,
                        }
                    else:
                        self._open_position(target.target_positions[0], target, c)
                        action = "added"
                        action_detail = {
                            "added_contracts": contracts_delta,
                            "new_total": target_contracts,
                        }
                elif contracts_delta < 0:
                    # Partially close oldest trades first; skip those still
                    # inside their min-hold window so reconcile churn can't
                    # round-trip a freshly-opened position.
                    to_close = abs(contracts_delta)
                    requested = to_close
                    closed_pnl = 0.0
                    skipped_min_hold = 0
                    for trade in open_trades:
                        if to_close <= 0:
                            break
                        if self._min_hold_active(trade, target.timestamp):
                            skipped_min_hold += trade["quantity_open"]
                            continue
                        open_qty = trade["quantity_open"]
                        close_qty = min(open_qty, to_close)
                        pnl = self._close_trade(trade, target.timestamp, c, partial_qty=close_qty)
                        closed_pnl += pnl
                        to_close -= close_qty
                    actually_trimmed = requested - to_close
                    action = "trimmed" if actually_trimmed > 0 else "held_min_hold_trim"
                    action_detail = {
                        "trimmed_contracts": actually_trimmed,
                        "deferred_contracts": to_close,
                        "min_hold_protected": skipped_min_hold,
                        "new_total": target_contracts + to_close,
                        "realized_pnl": round(closed_pnl, 4),
                    }
                else:
                    # Hold — just update marks
                    for trade in open_trades:
                        self._update_trade_mark(trade, target.timestamp, c)
                    action = "held"
                    action_detail = {
                        "contracts": actual_contracts,
                        "direction": actual_direction,
                    }

            # CASE D: no open trades, target has position
            elif actual_contracts == 0 and target.target_positions:
                if open_trade_count >= self.max_open_trades:
                    action = "held_max_open_trades"
                    action_detail = {
                        "reason": "Max open trade slots reached",
                        "max_open_trades": self.max_open_trades,
                        "open_trade_count": open_trade_count,
                        "target_contracts": target_contracts,
                    }
                else:
                    self._open_position(target.target_positions[0], target, c)
                    action = "opened"
                    action_detail = {
                        "contracts": target_contracts,
                        "direction": target_direction,
                        "strategy": target.target_positions[0].strategy_type,
                    }

            # Re-fetch to get accurate state for snapshot
            open_trades_after = self._fetch_open_trades(c)
            self.snapshot(target, action, action_detail, open_trades_after, c)

            return action

    # ------------------------------------------------------------------
    # _close_trade
    # ------------------------------------------------------------------

    def _close_trade(
        self,
        trade: dict,
        as_of: datetime,
        conn,
        partial_qty: Optional[int] = None,
    ) -> float:
        """Close (fully or partially) an open trade at current mark.

        If ``partial_qty`` is None or >= ``quantity_open``, close the whole
        trade.  Otherwise decrement ``quantity_open`` by ``partial_qty``,
        add the proportional realized PnL, and leave ``status = 'open'``.
        Returns realized PnL for the portion that was closed.
        """
        mark, pricing_mode = self._spread_mark(trade, as_of, conn)
        if mark is None:
            mark = trade["current_price"]

        entry = trade["entry_price"]
        open_qty = trade["quantity_open"]
        if open_qty <= 0:
            return 0.0

        if partial_qty is None or partial_qty >= open_qty:
            close_qty = open_qty
            fully_closing = True
        elif partial_qty <= 0:
            return 0.0
        else:
            close_qty = partial_qty
            fully_closing = False

        realized_pnl = self._spread_pnl(entry, mark, close_qty, pricing_mode)

        cur = conn.cursor()
        if fully_closing:
            cur.execute(
                """
                UPDATE signal_trades
                SET status = 'closed',
                    closed_at = %s,
                    current_price = %s,
                    quantity_open = 0,
                    realized_pnl = realized_pnl + %s,
                    unrealized_pnl = 0,
                    total_pnl = realized_pnl + %s,
                    pnl_percent = CASE
                        WHEN entry_price > 0
                        THEN ROUND(((realized_pnl + %s) / (entry_price * quantity_initial * 100)) * 100, 4)
                        ELSE 0
                    END,
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'open'
                """,
                (as_of, mark, realized_pnl, realized_pnl, realized_pnl, trade["id"]),
            )
        else:
            cur.execute(
                """
                UPDATE signal_trades
                SET current_price = %s,
                    quantity_open = quantity_open - %s,
                    realized_pnl = realized_pnl + %s,
                    total_pnl = (realized_pnl + %s) + unrealized_pnl,
                    pnl_percent = CASE
                        WHEN entry_price > 0 AND quantity_initial > 0
                        THEN ROUND(
                            (((realized_pnl + %s) + unrealized_pnl)
                             / (entry_price * quantity_initial * 100)) * 100,
                            4
                        )
                        ELSE 0
                    END,
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'open'
                """,
                (mark, close_qty, realized_pnl, realized_pnl, realized_pnl, trade["id"]),
            )
            # Keep the in-memory dict in sync for callers that iterate over
            # the same trade list after a partial close.
            trade["quantity_open"] = open_qty - close_qty
        # Commit is deferred to reconcile() so the entire cycle is atomic.
        return realized_pnl

    # ------------------------------------------------------------------
    # _open_position
    # ------------------------------------------------------------------

    def _open_position(self, tp: TargetPosition, target: PortfolioTarget, conn) -> bool:
        """Insert a new row to signal_trades. Returns True if inserted."""
        components_at_entry = {
            "optimizer": tp.optimizer_payload,
            "risk": self._build_risk_plan(tp, target.timestamp),
        }

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO signal_trades (
                underlying, signal_timestamp, opened_at, updated_at, status,
                direction, score_at_entry, option_symbol, option_type, expiration, strike,
                entry_price, current_price, quantity_initial, quantity_open,
                realized_pnl, unrealized_pnl, total_pnl, pnl_percent,
                components_at_entry
            ) VALUES (
                %s, %s, %s, NOW(), 'open',
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                0, 0, 0, 0,
                %s::jsonb
            )
            """,
            (
                self.db_symbol,
                target.timestamp,
                target.timestamp,
                tp.direction,
                target.composite_score,
                tp.option_symbol,
                tp.option_type,
                tp.expiration,
                tp.strike,
                tp.entry_mark,
                tp.entry_mark,
                tp.contracts,
                tp.contracts,
                json.dumps(components_at_entry, default=str),
            ),
        )
        # Commit is deferred to reconcile() so the entire cycle is atomic.
        return cur.rowcount > 0

    # ------------------------------------------------------------------
    # _update_trade_mark
    # ------------------------------------------------------------------

    def _update_trade_mark(self, trade: dict, as_of: datetime, conn) -> None:
        """Refresh unrealized PnL for an open trade."""
        mark, pricing_mode = self._spread_mark(trade, as_of, conn)
        if mark is None:
            return

        entry = trade["entry_price"]
        qty = trade["quantity_open"]
        unrealized = self._spread_pnl(entry, mark, qty, pricing_mode)
        realized = trade["realized_pnl"]
        total = realized + unrealized
        basis_qty = max(trade["quantity_initial"], 1)
        pnl_pct = ((total / (entry * basis_qty * 100)) * 100) if entry > 0 else 0

        cur = conn.cursor()
        cur.execute(
            """
            UPDATE signal_trades
            SET current_price = %s,
                unrealized_pnl = %s,
                total_pnl = %s,
                pnl_percent = %s,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'open'
            """,
            (mark, round(unrealized, 4), round(total, 4), round(pnl_pct, 4), trade["id"]),
        )
        # Commit is deferred to reconcile() so the entire cycle is atomic.

    # ------------------------------------------------------------------
    # snapshot — written every cycle regardless of action
    # ------------------------------------------------------------------

    def snapshot(
        self,
        target: PortfolioTarget,
        action: str,
        action_detail: dict,
        open_trades: list[dict],
        conn,
    ) -> None:
        """Insert a portfolio_snapshots row after each reconciliation cycle."""
        actual_contracts = sum(t["quantity_open"] for t in open_trades)
        actual_direction = self._majority_direction(open_trades) if open_trades else "neutral"
        target_strategy = (
            target.target_positions[0].strategy_type if target.target_positions else None
        )
        heat = sum(abs(t["entry_price"]) * t["quantity_open"] * 100 for t in open_trades) / max(
            SIGNALS_PORTFOLIO_SIZE, 1.0
        )

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO portfolio_snapshots (
                underlying, timestamp,
                composite_score, normalized_score, direction,
                target_contracts, target_direction, target_strategy,
                actual_contracts, actual_direction,
                heat_pct, action_taken, action_detail
            ) VALUES (
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s::jsonb
            )
            ON CONFLICT (underlying, timestamp) DO UPDATE SET
                composite_score = EXCLUDED.composite_score,
                normalized_score = EXCLUDED.normalized_score,
                direction = EXCLUDED.direction,
                target_contracts = EXCLUDED.target_contracts,
                target_direction = EXCLUDED.target_direction,
                target_strategy = EXCLUDED.target_strategy,
                actual_contracts = EXCLUDED.actual_contracts,
                actual_direction = EXCLUDED.actual_direction,
                heat_pct = EXCLUDED.heat_pct,
                action_taken = EXCLUDED.action_taken,
                action_detail = EXCLUDED.action_detail
            """,
            (
                self.db_symbol,
                target.timestamp,
                target.composite_score,
                target.normalized_score,
                target.direction,
                target.total_target_contracts,
                target.direction if target.target_positions else "neutral",
                target_strategy,
                actual_contracts,
                actual_direction,
                round(heat, 6),
                action,
                json.dumps(action_detail, default=str),
            ),
        )
        # Commit is deferred to reconcile() so the entire cycle is atomic.

    # ------------------------------------------------------------------
    # Internal helpers (ported from UnifiedSignalEngine)
    # ------------------------------------------------------------------

    def _fetch_open_trades(self, conn) -> list[dict]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, option_symbol, entry_price, current_price, quantity_open,
                   quantity_initial, status, direction, realized_pnl, components_at_entry
            FROM signal_trades
            WHERE underlying = %s
              AND status = 'open'
            ORDER BY opened_at ASC
            """,
            (self.db_symbol,),
        )
        return [
            {
                "id": r[0],
                "option_symbol": r[1],
                "entry_price": float(r[2]),
                "current_price": float(r[3] or r[2]),
                "quantity_open": int(r[4]),
                "quantity_initial": int(r[5]),
                "status": r[6],
                "direction": r[7],
                "realized_pnl": float(r[8] or 0.0),
                "components_at_entry": r[9] or {},
            }
            for r in cur.fetchall()
        ]

    def _latest_option_mark(self, option_symbol: str, as_of: datetime, conn) -> Optional[float]:
        """Mid-price for a single option at-or-before ``as_of``.

        Used as the legacy fallback when a trade has no per-leg metadata and we
        can't apply a side-aware exit fill.
        """
        quote = self._latest_option_quote(option_symbol, as_of, conn)
        if quote is None:
            return None
        bid, ask, last = quote
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        return max(last, ask, bid, 0.0) or None

    def _latest_option_quote(
        self, option_symbol: str, as_of: datetime, conn
    ) -> Optional[tuple[float, float, float]]:
        """Latest (bid, ask, last) for a single option at-or-before ``as_of``."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT bid, ask, last
            FROM option_chains
            WHERE option_symbol = %s
              AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (option_symbol, as_of),
        )
        row = cur.fetchone()
        if not row:
            return None
        bid, ask, last = row
        return (float(bid or 0.0), float(ask or 0.0), float(last or 0.0))

    def _spread_mark(self, trade: dict, as_of: datetime, conn) -> tuple[Optional[float], str]:
        """Mark the spread at its realistic *liquidation* price per share.

        Each leg is priced with a side-aware exit fill: the long leg is sold at
        the bid, the short leg is bought back at the ask.  The optional
        slippage knob (SIGNALS_EXECUTION_SLIPPAGE_PCT) widens both sides.

        Returns (value_per_share, pricing_mode). ``pricing_mode`` is "debit" or
        "credit"; on missing/partial chain data returns ``(None, pricing_mode)``.
        """
        payload = (trade.get("components_at_entry") or {}).get("optimizer") or {}
        legs = payload.get("legs") or []
        pricing_mode = payload.get("pricing_mode") or "debit"

        if not legs:
            mark = self._latest_option_mark(trade["option_symbol"], as_of, conn)
            return mark, pricing_mode

        long_sum = 0.0
        short_sum = 0.0
        for leg in legs:
            symbol = leg.get("option_symbol")
            if not symbol:
                return None, pricing_mode
            quote = self._latest_option_quote(symbol, as_of, conn)
            if quote is None:
                return None, pricing_mode
            bid, ask, last = quote
            side = str(leg.get("side") or "").lower()
            if side not in {"long", "short"}:
                return None, pricing_mode
            fill = leg_fill_price(bid=bid, ask=ask, last=last, side=side, action="close")
            if side == "long":
                long_sum += fill
            else:
                short_sum += fill

        if pricing_mode == "credit":
            # Cost-to-close a credit spread: buy shorts back, sell longs out.
            return short_sum - long_sum, pricing_mode
        return long_sum - short_sum, pricing_mode

    @staticmethod
    def _spread_pnl(entry: float, mark: float, qty: int, pricing_mode: str) -> float:
        """Signed dollar P&L for the spread at ``mark`` vs ``entry`` per-share.

        Debit spreads are held long: value up => profit. Credit spreads are held
        short: cost-to-close down => profit, so the sign inverts.
        """
        if pricing_mode == "credit":
            per_share = entry - mark
        else:
            per_share = mark - entry
        return per_share * qty * 100

    def _score_trend_confirmation(
        self,
        score: ScoreSnapshot,
        market_ctx: dict,
        conn=None,
    ) -> bool:
        """Confirm recent signal_scores history agrees with current direction."""
        try:
            with self._use_conn(conn) as c:
                cur = c.cursor()
                lookback = 6
                if lookback <= 0:
                    return True

                direction = score.direction
                min_match = 3
                cur.execute(
                    """
                    SELECT direction
                    FROM signal_scores
                    WHERE underlying = %s
                      AND timestamp < %s
                      AND direction != 'high_risk_reversal'
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    (self.db_symbol, score.timestamp, lookback),
                )
                rows = cur.fetchall()
                if not rows:
                    return True
                matching = sum(1 for r in rows if r[0] == direction)
                required = min(min_match, len(rows))
                return matching >= required
        except Exception as exc:
            logger.warning(
                "PortfolioEngine[%s]: trend confirmation lookup failed (%s); treating as unconfirmed",
                self.db_symbol,
                exc,
            )
            return False

    def _passes_dealer_regime_gates(
        self, score: ScoreSnapshot, market_ctx: dict
    ) -> tuple[bool, str]:
        # Under MSI architecture, direction validity is handled by regime +
        # do-not-fade + advanced-signal gating. Keep this hook as pass-through.
        return True, "MSI regime gate passed"

    def _build_risk_plan(self, tp: TargetPosition, opened_at: datetime) -> dict:
        """Compute and serialize the per-trade stop/target/time-stop/min-hold plan.

        Stop and target are expressed as **per-share spread prices** (matching
        ``signal_trades.entry_price`` and the value returned by ``_spread_mark``)
        so the reconcile loop can compare directly without rebuilding fills.

        For debit spreads, profit accrues as mark > entry; stop fires below
        entry, target fires above. For credit spreads the inequalities invert
        (cost-to-close drops below entry on a winner).
        """
        entry = float(tp.entry_mark or 0.0)
        payload = tp.optimizer_payload or {}
        pricing_mode = (
            payload.get("pricing_mode")
            or ("credit" if "credit" in (tp.strategy_type or "") else "debit")
        )

        # SIGNALS_STOP_LOSS_PCT is signed negative (-0.25 = 25% loss tolerance);
        # SIGNALS_TARGET_PCT is unsigned (+0.50 = +50% profit target).
        # Per-trade target_pct override (Phase 3.2 breakout-boost) wins over
        # the default when present.
        stop_drift = float(self.stop_loss_pct or 0.0)
        risk_overrides = payload.get("risk_overrides") or {}
        if "target_pct" in risk_overrides:
            target_drift = float(risk_overrides.get("target_pct") or 0.0)
        else:
            target_drift = float(self.target_pct or 0.0)
        if pricing_mode == "credit":
            # Credit: winner = mark falls; loser = mark rises.
            stop_price = entry * (1.0 + abs(stop_drift)) if entry > 0 else 0.0
            target_price = entry * max(1.0 - target_drift, 0.0) if entry > 0 else 0.0
        else:
            stop_price = entry * (1.0 + stop_drift) if entry > 0 else 0.0
            target_price = entry * (1.0 + target_drift) if entry > 0 else 0.0

        # Normalize opened_at to UTC iso for downstream string comparison.
        if opened_at.tzinfo is None:
            opened_at_utc = opened_at.replace(tzinfo=timezone.utc)
        else:
            opened_at_utc = opened_at.astimezone(timezone.utc)

        time_stop = (
            opened_at_utc + timedelta(minutes=self.time_stop_minutes)
            if self.time_stop_minutes > 0
            else None
        )
        min_hold_until = (
            opened_at_utc + timedelta(seconds=self.min_hold_seconds)
            if self.min_hold_seconds > 0
            else None
        )

        return {
            "pricing_mode": pricing_mode,
            "entry_price": round(entry, 6),
            "stop_loss_pct": stop_drift,
            "target_pct": target_drift,
            "stop_price": round(stop_price, 6) if stop_price else None,
            "target_price": round(target_price, 6) if target_price else None,
            "min_hold_seconds": int(self.min_hold_seconds),
            "min_hold_until": min_hold_until.isoformat() if min_hold_until else None,
            "time_stop_minutes": int(self.time_stop_minutes),
            "time_stop_at": time_stop.isoformat() if time_stop else None,
        }

    @staticmethod
    def _parse_iso(value) -> Optional[datetime]:
        """Best-effort ISO timestamp parse; returns None on any failure."""
        if not value:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        try:
            parsed = datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    def _evaluate_exit_plan(
        self, trade: dict, as_of: datetime, conn
    ) -> Optional[tuple[str, float, str]]:
        """Check the per-trade stop/target/time-stop. Returns (kind, mark, reason)
        when an exit fires, else None.

        ``kind`` is one of {"stop_loss", "take_profit", "time_stop"} so callers
        can label the resulting reconcile action distinctly.
        """
        risk = (trade.get("components_at_entry") or {}).get("risk") or {}
        if not risk:
            return None

        time_stop_at = self._parse_iso(risk.get("time_stop_at"))
        if as_of.tzinfo is None:
            as_of_utc = as_of.replace(tzinfo=timezone.utc)
        else:
            as_of_utc = as_of.astimezone(timezone.utc)

        if time_stop_at and as_of_utc >= time_stop_at:
            mark, _ = self._spread_mark(trade, as_of, conn)
            return ("time_stop", mark or 0.0, f"time stop at {time_stop_at.isoformat()}")

        stop_price = risk.get("stop_price")
        target_price = risk.get("target_price")
        if stop_price is None and target_price is None:
            return None

        mark, _ = self._spread_mark(trade, as_of, conn)
        if mark is None:
            return None

        pricing_mode = risk.get("pricing_mode") or "debit"
        if pricing_mode == "credit":
            # Credit: stop above entry (cost-to-close ballooned), target below.
            if stop_price is not None and mark >= float(stop_price):
                return ("stop_loss", mark, f"credit stop {mark:.4f} >= {stop_price:.4f}")
            if target_price is not None and mark <= float(target_price):
                return ("take_profit", mark, f"credit target {mark:.4f} <= {target_price:.4f}")
        else:
            if stop_price is not None and mark <= float(stop_price):
                return ("stop_loss", mark, f"debit stop {mark:.4f} <= {stop_price:.4f}")
            if target_price is not None and mark >= float(target_price):
                return ("take_profit", mark, f"debit target {mark:.4f} >= {target_price:.4f}")
        return None

    def _sweep_plan_exits(self, open_trades: list[dict], as_of: datetime, conn) -> int:
        """Close every open trade whose stored risk plan fired (stop/target/time).

        Returns the count of trades that were closed.  Plan exits ignore the
        min-hold window — a stop hit at second 30 is still a stop.
        """
        if not open_trades:
            return 0
        closed = 0
        for trade in open_trades:
            try:
                exit_decision = self._evaluate_exit_plan(trade, as_of, conn)
            except Exception as exc:
                logger.warning(
                    "PortfolioEngine[%s]: plan-exit evaluation failed for trade %s (%s)",
                    self.db_symbol,
                    trade.get("id"),
                    exc,
                )
                continue
            if exit_decision is None:
                continue
            kind, _mark, reason = exit_decision
            try:
                self._close_trade(trade, as_of, conn)
                closed += 1
                logger.info(
                    "PortfolioEngine[%s]: plan-exit %s on trade %s (%s)",
                    self.db_symbol,
                    kind,
                    trade.get("id"),
                    reason,
                )
            except Exception as exc:
                logger.warning(
                    "PortfolioEngine[%s]: plan-exit close failed for trade %s (%s)",
                    self.db_symbol,
                    trade.get("id"),
                    exc,
                )
        return closed

    def _min_hold_active(self, trade: dict, as_of: datetime) -> bool:
        """True if the trade is inside its protected min-hold window."""
        risk = (trade.get("components_at_entry") or {}).get("risk") or {}
        min_hold_until = self._parse_iso(risk.get("min_hold_until"))
        if not min_hold_until:
            return False
        if as_of.tzinfo is None:
            as_of_utc = as_of.replace(tzinfo=timezone.utc)
        else:
            as_of_utc = as_of.astimezone(timezone.utc)
        return as_of_utc < min_hold_until

    def _current_position_direction(self, conn=None) -> str:
        """Return the majority direction of currently-open trades.

        Used by compute_target to apply asymmetric entry/exit thresholds
        (hysteresis). Returns "neutral" when no positions are open OR when the
        underlying lookup fails — that conservative fallback forces the strict
        entry threshold and is safe under MagicMock conns in unit tests.
        """
        try:
            with self._use_conn(conn) as c:
                trades = self._fetch_open_trades(c)
            if not trades:
                return "neutral"
            return self._majority_direction(trades)
        except Exception:
            return "neutral"

    def _fresh_drs_cross(self, direction: str, market_ctx: dict) -> bool:
        # Legacy DRS cross boost disabled under MSI-first logic.
        return False

    def _select_optimizer_candidate(
        self,
        score: ScoreSnapshot,
        ctx: dict,
        conn=None,
        cached_option_rows=None,
        forced_timeframe: Optional[str] = None,
        signal_direction: Optional[str] = None,
    ) -> Optional[dict]:
        signal_timeframe = forced_timeframe or self._infer_signal_timeframe(score.composite_score)
        signal_strength = self._infer_signal_strength(score.composite_score)
        dte_min, dte_max = self._resolve_dte_window(signal_timeframe, score.timestamp)

        option_rows = None
        if cached_option_rows is not None:
            cache_key, rows = cached_option_rows
            if cache_key == (score.timestamp, dte_min, dte_max) and rows:
                option_rows = rows
        if option_rows is None:
            with self._use_conn(conn) as c:
                option_rows = fetch_option_snapshot(
                    c, self.db_symbol, score.timestamp, score.timestamp.date(), dte_min, dte_max
                )
        if not option_rows:
            return None

        # signal_direction (when provided by the regime-aware MSI path) takes
        # precedence over score.direction so the strategy builder + optimizer
        # follow the resolved trade_direction rather than the raw MSI sign.
        effective_direction = signal_direction or score.direction

        strategy_decision = self.strategy_builder.decide(
            score_direction=effective_direction,
            score_normalized=score.normalized_score,
            market_ctx={
                **ctx,
                "timestamp": score.timestamp,
            },
            option_rows=option_rows,
        )

        optimizer_ctx = PositionOptimizerContext(
            timestamp=score.timestamp,
            signal_timestamp=score.timestamp,
            signal_timeframe=signal_timeframe,
            signal_direction=strategy_decision.optimizer_direction,
            signal_strength=signal_strength,
            trade_type=strategy_decision.trade_type,
            current_price=ctx["close"],
            net_gex=ctx["net_gex"],
            gamma_flip=ctx["gamma_flip"],
            put_call_ratio=ctx["put_call_ratio"],
            max_pain=ctx["max_pain"],
            smart_call_premium=ctx["smart_call"],
            smart_put_premium=ctx["smart_put"],
            dealer_net_delta=0.0,
            target_dte_min=dte_min,
            target_dte_max=dte_max,
            iv_rank=ctx.get("iv_rank"),
            preferred_strategies=strategy_decision.preferred_strategies,
            regime=strategy_decision.regime,
            regime_score=strategy_decision.regime_score,
            strategy_diagnostics=strategy_decision.diagnostics,
            option_rows=option_rows,
        )
        candidates = self.position_optimizer._generate_candidates(optimizer_ctx)
        if not candidates:
            return None

        for candidate in candidates:
            profiles = {p.profile: p for p in candidate.sizing_profiles}
            optimal = profiles.get("optimal")
            if optimal and optimal.contracts > 0:
                return {
                    "candidate": candidate,
                    "signal_timeframe": signal_timeframe,
                    "signal_strength": signal_strength,
                    "trade_type": strategy_decision.trade_type,
                    "strategy_regime": strategy_decision.regime,
                    "strategy_regime_score": strategy_decision.regime_score,
                    "strategy_diagnostics": strategy_decision.diagnostics,
                }
        return None

    @staticmethod
    def _resolve_regime(score: ScoreSnapshot) -> str:
        regime = str(score.direction or "").strip().lower()
        if regime in {"trend_expansion", "controlled_trend", "chop_range", "high_risk_reversal"}:
            return regime
        msi = float(score.composite_score or 0.0)
        if msi >= 70.0:
            return "trend_expansion"
        if msi >= 40.0:
            return "controlled_trend"
        if msi >= 20.0:
            return "chop_range"
        return "high_risk_reversal"

    @staticmethod
    def _resolve_trade_direction(score: ScoreSnapshot, market_ctx: dict, regime: str) -> str:
        if regime == "high_risk_reversal":
            return "neutral"
        trend = PortfolioEngine._msi_trend_direction(market_ctx)
        if trend in {"bullish", "bearish"}:
            return trend
        # Fallback for tests/legacy callers that still pass explicit bullish/bearish labels.
        legacy = str(score.direction or "").strip().lower()
        if legacy in {"bullish", "bearish"}:
            return legacy
        return "neutral"

    @staticmethod
    def _majority_direction(trades: list[dict]) -> str:
        if not trades:
            return "neutral"
        bull = sum(t["quantity_open"] for t in trades if t["direction"] == "bullish")
        bear = sum(t["quantity_open"] for t in trades if t["direction"] == "bearish")
        if bull > bear:
            return "bullish"
        if bear > bull:
            return "bearish"
        return "neutral"

    @staticmethod
    def _infer_signal_strength(msi_score: float) -> str:
        if msi_score >= 80.0:
            return "high"
        if msi_score >= 55.0:
            return "medium"
        return "low"

    @staticmethod
    def _infer_signal_timeframe(msi_score: float) -> str:
        if msi_score >= 70.0:
            return "swing"
        if msi_score >= 40.0:
            return "intraday"
        return "intraday"

    @staticmethod
    def _resolve_dte_window(
        signal_timeframe: str, timestamp: datetime
    ) -> tuple[int, int]:
        """Return the (dte_min, dte_max) window for the optimizer fetch.

        Phase 3.3: prevent 0DTE fills early in the session.  In the first
        ``SIGNALS_NO_0DTE_MORNING_MINUTES`` minutes after the open, gamma is
        non-stationary and 0DTE prices are dominated by overnight risk
        premium repricing — a coin-flip window.  Bump dte_min from 0 to 1
        in that window so the optimizer reaches for 1-2 DTE structures
        with theta protection.
        """
        base_min, base_max = TARGET_DTE_WINDOWS.get(signal_timeframe, (1, 7))
        no_zero_minutes = int(SIGNALS_NO_0DTE_MORNING_MINUTES)
        if no_zero_minutes <= 0 or base_min > 0:
            return base_min, base_max

        if timestamp.tzinfo is None:
            ts_et = ET.localize(timestamp)
        else:
            ts_et = timestamp.astimezone(ET)

        market_open_dt = datetime.combine(
            ts_et.date(), datetime.strptime("09:30", "%H:%M").time(), tzinfo=ts_et.tzinfo,
        )
        cutoff_dt = market_open_dt + timedelta(minutes=no_zero_minutes)
        if market_open_dt <= ts_et < cutoff_dt:
            # Bump dte_min to 1 but keep dte_max so we still get 1-2 DTE.
            return max(base_min, 1), max(base_max, 1)
        return base_min, base_max

    @staticmethod
    def _legs_from_candidate(candidate: dict) -> list[dict]:
        payload_legs = candidate.get("legs")
        if isinstance(payload_legs, list) and payload_legs:
            return payload_legs
        strikes = candidate.get("strikes", "")
        strategy = candidate.get("strategy_type")
        expiry = candidate.get("expiry")
        parts = [float(p) for p in re.findall(r"(\d+(?:\.\d+)?)", strikes)]
        if strategy == "bull_call_debit" and len(parts) >= 2:
            return [
                {"side": "long", "option_type": "C", "strike": parts[0], "expiry": expiry},
                {"side": "short", "option_type": "C", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "bear_put_debit" and len(parts) >= 2:
            return [
                {"side": "long", "option_type": "P", "strike": parts[0], "expiry": expiry},
                {"side": "short", "option_type": "P", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "bull_put_credit" and len(parts) >= 2:
            return [
                {"side": "short", "option_type": "P", "strike": parts[0], "expiry": expiry},
                {"side": "long", "option_type": "P", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "bear_call_credit" and len(parts) >= 2:
            return [
                {"side": "short", "option_type": "C", "strike": parts[0], "expiry": expiry},
                {"side": "long", "option_type": "C", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "iron_condor" and len(parts) >= 4:
            return [
                {"side": "short", "option_type": "P", "strike": parts[0], "expiry": expiry},
                {"side": "short", "option_type": "C", "strike": parts[1], "expiry": expiry},
                {"side": "long", "option_type": "P", "strike": parts[2], "expiry": expiry},
                {"side": "long", "option_type": "C", "strike": parts[3], "expiry": expiry},
            ]
        if strategy == "long_straddle" and len(parts) >= 2:
            return [
                {"side": "long", "option_type": "C", "strike": parts[0], "expiry": expiry},
                {"side": "long", "option_type": "P", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "long_strangle" and len(parts) >= 2:
            return [
                {"side": "long", "option_type": "P", "strike": parts[0], "expiry": expiry},
                {"side": "long", "option_type": "C", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "short_strangle" and len(parts) >= 2:
            return [
                {"side": "short", "option_type": "P", "strike": parts[0], "expiry": expiry},
                {"side": "short", "option_type": "C", "strike": parts[1], "expiry": expiry},
            ]
        if strategy == "iron_butterfly" and len(parts) >= 4:
            return [
                {"side": "long", "option_type": "P", "strike": parts[0], "expiry": expiry},
                {"side": "short", "option_type": "P", "strike": parts[1], "expiry": expiry},
                {"side": "short", "option_type": "C", "strike": parts[2], "expiry": expiry},
                {"side": "long", "option_type": "C", "strike": parts[3], "expiry": expiry},
            ]
        if strategy == "calendar" and len(parts) >= 2:
            option_type = str(candidate.get("option_type") or "C").upper()
            return [
                {"side": "short", "option_type": option_type, "strike": parts[0], "expiry": expiry},
                {"side": "long", "option_type": option_type, "strike": parts[1], "expiry": expiry},
            ]
        return []

    def _resolve_option_symbol_for_leg(
        self, as_of: datetime, leg: dict, conn=None
    ) -> Optional[str]:
        with self._use_conn(conn) as c:
            cur = c.cursor()
            cur.execute(
                """
                SELECT option_symbol
                FROM option_chains
                WHERE underlying = %s
                  AND timestamp <= %s
                  AND expiration = %s
                  AND option_type = %s
                  AND ABS(strike - %s) < 0.01
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (self.db_symbol, as_of, leg["expiry"], leg["option_type"], leg["strike"]),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def _cash_target(self, score: ScoreSnapshot, rationale: str) -> PortfolioTarget:
        """Convenience: build a 100% cash PortfolioTarget."""
        return PortfolioTarget(
            underlying=self.db_symbol,
            timestamp=score.timestamp,
            composite_score=score.composite_score,
            normalized_score=score.normalized_score,
            direction=score.direction,
            target_positions=[],
            total_target_contracts=0,
            target_heat_pct=0.0,
            rationale=rationale,
        )
