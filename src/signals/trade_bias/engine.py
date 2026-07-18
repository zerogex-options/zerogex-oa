"""Trade Bias engine — compute + persist, mirroring ``ScoringEngine``.

Runs inside the same per-symbol cycle as the Market State Index
(``UnifiedSignalEngine.run_cycle``). It reads the signal scores the cycle has
already computed — no recompute — assembles the nine directional inputs on the
``-100..+100`` scale the front end uses, synthesizes the bias via
``compute_bias``, and upserts a row into ``trade_bias_scores``.

Phase 1 persists a single ``swing`` (multi-day / structural) row per cycle;
``state`` is ``"baseline"`` and ``override.active`` is always ``False``. The
graded override, first-class momentum, swing bounce/reject detector, and the
``intraday`` (0DTE) tenor arrive in later phases without changing this contract.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from src.signals.components.base import MarketContext
from src.signals.components.momentum import MomentumComponent
from src.signals.components.order_flow_imbalance import OrderFlowImbalanceComponent
from src.signals.components.swing_reversal import SwingReversalComponent
from src.signals.trade_bias.bias import BiasInput, BiasResult, compute_bias
from src.signals.trade_bias.fusion import (
    SWING_PROFILE,
    FusedBias,
    TacticalRead,
    TenorProfile,
    compute_tactical,
    fuse,
    profile_for,
)
from src.utils import get_logger

logger = get_logger(__name__)

# Signal ``.score`` in the cycle results is the persisted ``clamped_score`` on
# the ``[-1, 1]`` scale; the API multiplies it by 100 for the front end, which
# is the scale ``compute_bias`` expects — so we do the same.
_TREND_SCALE = 100.0

TENOR_SWING = "swing"
TENOR_INTRADAY = "intraday"

_TREND_TO_DIRECTION = {"bullish": "long", "bearish": "short", "neutral": "neutral"}
_DIRECTION_TO_TREND = {"long": "bullish", "short": "bearish", "neutral": "neutral"}


@dataclass
class TradeBiasSnapshot:
    timestamp: datetime
    underlying: str
    tenor: str
    bias_score: float  # signed [-100, 100]
    direction: str  # long / short / neutral
    bias_code: str  # BUY_DIPS / SELL_RIPS / FADE_* / RANGE_FADE / WAIT
    market_state: str
    state: str  # baseline (Phase 1); confirmed / divergent / override later
    confidence: float  # [0, 100]
    override_active: bool
    payload: dict


class TradeBiasEngine:
    """Compute and persist the directional Trade Bias each cycle."""

    def __init__(self, underlying: str):
        self.underlying = underlying.upper()
        # Tactical-pillar components. Stateless — instantiate once per engine.
        # swing_reversal + momentum are new here; order_flow_imbalance is the
        # existing MSI component, read directly for its raw directional value.
        self._swing = SwingReversalComponent()
        self._momentum = MomentumComponent()
        self._order_flow = OrderFlowImbalanceComponent()

    # ------------------------------------------------------------------
    # Input assembly
    # ------------------------------------------------------------------
    @staticmethod
    def _scores_by_name(results: Any) -> dict[str, float]:
        out: dict[str, float] = {}
        for result in results or []:
            name = getattr(result, "name", None)
            raw = getattr(result, "score", None)
            if name is None or raw is None:
                continue
            try:
                out[name] = float(raw) * _TREND_SCALE
            except (TypeError, ValueError):
                continue
        return out

    def build_inputs(
        self,
        ctx: MarketContext,
        score: Any,
        advanced_results: Any,
        basic_results: Any,
    ) -> BiasInput:
        basic = self._scores_by_name(basic_results)
        advanced = self._scores_by_name(advanced_results)

        # net GEX collapses to its sign (±50) — magnitude is discarded, exactly
        # as the dashboard does. Anything <= 0 reads short-gamma (matches the
        # front end's ``net_gex > 0 ? 50 : -50``).
        net_gex = getattr(ctx, "net_gex", None)
        net_gex_input: Optional[float] = None
        if isinstance(net_gex, (int, float)):
            net_gex_input = 50.0 if net_gex > 0 else -50.0

        composite = getattr(score, "composite_score", None)
        msi = float(composite) if isinstance(composite, (int, float)) else None

        return BiasInput(
            netGEX=net_gex_input,
            gexGradient=basic.get("gex_gradient"),
            tapeFlow=basic.get("tape_flow_bias"),
            vannaCharm=basic.get("vanna_charm_flow"),
            odtePositioning=advanced.get("zero_dte_position_imbalance"),
            positioningTrap=basic.get("positioning_trap"),
            trapDetection=advanced.get("trap_detection"),
            gammaVWAP=advanced.get("gamma_vwap_confluence"),
            msi=msi,
        )

    # ------------------------------------------------------------------
    # Snapshot assembly
    # ------------------------------------------------------------------
    @staticmethod
    def _gamma_regime(ctx: MarketContext) -> Optional[str]:
        net_gex = getattr(ctx, "net_gex", None)
        if not isinstance(net_gex, (int, float)):
            return None
        return "negative" if net_gex < 0 else "positive"

    @staticmethod
    def _volatility_regime(ctx: MarketContext) -> Optional[str]:
        vix = (getattr(ctx, "extra", None) or {}).get("vix_level")
        try:
            from src.signals.advanced.base import vix_regime

            regime = vix_regime(vix)
        except Exception:
            return None
        return None if regime == "unknown" else regime

    def build_tactical(
        self, ctx: MarketContext, inputs: BiasInput, profile: TenorProfile = SWING_PROFILE
    ) -> TacticalRead:
        """Read the four directional pillars off the live context.

        price_action = swing bounce/reject; flow = smart-money order-flow
        imbalance (swing) or the 0DTE positioning tilt (intraday); tape = the
        premium tape lean (tape_flow_bias, one of the nine inputs, rescaled to
        [-1, 1]); momentum = vol-normalized momentum.
        """
        price_action = self._swing.compute(ctx)
        momentum = self._momentum.compute(ctx)
        order_flow = self._order_flow.compute(ctx)
        odte = (inputs.odtePositioning / 100.0) if inputs.odtePositioning is not None else None
        # Intraday's flow pillar is the same-day 0DTE positioning; swing's is the
        # all-expiry smart-money flow. Each falls back to the other if missing.
        if profile.name == "intraday":
            flow = odte if odte is not None else order_flow
        else:
            flow = order_flow
        tape = (inputs.tapeFlow / 100.0) if inputs.tapeFlow is not None else None
        return compute_tactical(
            price_action=price_action, flow=flow, tape=tape, momentum=momentum, profile=profile
        )

    def build_snapshot(
        self,
        ctx: MarketContext,
        inputs: BiasInput,
        structural: BiasResult,
        tactical: TacticalRead,
        fused: FusedBias,
        tenor: str = TENOR_SWING,
    ) -> TradeBiasSnapshot:
        # The fused layer owns the signed bias / direction / state / override;
        # the structural baseline still owns the regime label, checklist, and
        # the CHOP "watching" chips.
        bias_trend = _DIRECTION_TO_TREND.get(fused.direction, "neutral")
        pillars = {
            k: (round(v, 4) if isinstance(v, (int, float)) else None)
            for k, v in tactical.pillars.items()
        }

        payload: dict[str, Any] = {
            "tenor": tenor,
            "bias_score": fused.bias_score,
            "direction": fused.direction,
            "state": fused.state,
            "override": {
                "active": fused.override_active,
                "reason": fused.override_reason,
                "overruled_posture": fused.overruled_posture,
            },
            "confidence": fused.confidence,
            "confidence_raw": structural.confidence,
            "max_confidence_raw": structural.maxConfidence,
            "regime": {
                "gamma": self._gamma_regime(ctx),
                "volatility": self._volatility_regime(ctx),
            },
            # Underlying close at this tick — lets the override-threshold
            # backtest compute forward returns straight from the dense
            # trade_bias_scores rows, no external bars join.
            "close": getattr(ctx, "close", None),
            "market_state": structural.marketState,
            "regime_label": structural.regimeLabel,
            "regime_desc": structural.regimeDesc,
            "bias": {
                "code": fused.bias_code,
                "label": fused.bias_label,
                "trend": bias_trend,
            },
            "setup": fused.setup,
            "playbook": list(fused.playbook),
            "expected_behavior": list(fused.expected_behavior),
            "checklist": [{"label": c.label, "passed": c.passed} for c in structural.checklist],
            "conviction_driven": structural.convictionDriven,
            "watching": [
                {"key": w.key, "label": w.label, "direction": w.direction}
                for w in structural.watching
            ],
            "has_data": structural.hasData,
            # The tactical layer that confirmed / diverged from / overrode the
            # structural baseline — the four pillars plus the fused vector.
            "tactical": {
                "direction": round(tactical.direction, 4),
                "conviction": round(tactical.conviction, 4),
                "aligned_count": tactical.aligned_count,
                "available_count": tactical.available_count,
                "pillars": pillars,
            },
            "structural_bias": {
                "code": structural.bias,
                "label": structural.biasLabel,
                "trend": structural.trend,
            },
            "inputs": {
                "net_gex": inputs.netGEX,
                "gex_gradient": inputs.gexGradient,
                "tape_flow": inputs.tapeFlow,
                "vanna_charm": inputs.vannaCharm,
                "odte_positioning": inputs.odtePositioning,
                "positioning_trap": inputs.positioningTrap,
                "trap_detection": inputs.trapDetection,
                "gamma_vwap": inputs.gammaVWAP,
                "msi": inputs.msi,
            },
        }
        ts = getattr(ctx, "timestamp", None)
        if ts is not None:
            payload["timestamp"] = ts.isoformat() if hasattr(ts, "isoformat") else ts

        return TradeBiasSnapshot(
            timestamp=ctx.timestamp,
            underlying=getattr(ctx, "underlying", self.underlying),
            tenor=tenor,
            bias_score=fused.bias_score,
            direction=fused.direction,
            bias_code=fused.bias_code,
            market_state=structural.marketState,
            state=fused.state,
            confidence=fused.confidence,
            override_active=fused.override_active,
            payload=payload,
        )

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def persist(self, snapshot: TradeBiasSnapshot, conn=None) -> None:
        if conn is None:
            from src.database import db_connection

            with db_connection() as local_conn:
                self._persist_inner(snapshot, local_conn)
        else:
            self._persist_inner(snapshot, conn)

    def _persist_inner(self, snapshot: TradeBiasSnapshot, conn) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO trade_bias_scores (
                underlying, timestamp, tenor, bias_score, direction, bias_code,
                market_state, state, confidence, override_active, payload
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (underlying, timestamp, tenor) DO UPDATE SET
                bias_score = EXCLUDED.bias_score,
                direction = EXCLUDED.direction,
                bias_code = EXCLUDED.bias_code,
                market_state = EXCLUDED.market_state,
                state = EXCLUDED.state,
                confidence = EXCLUDED.confidence,
                override_active = EXCLUDED.override_active,
                payload = EXCLUDED.payload,
                updated_at = NOW()
            """,
            (
                snapshot.underlying,
                snapshot.timestamp,
                snapshot.tenor,
                snapshot.bias_score,
                snapshot.direction,
                snapshot.bias_code,
                snapshot.market_state,
                snapshot.state,
                snapshot.confidence,
                snapshot.override_active,
                json.dumps(snapshot.payload, default=str),
            ),
        )
        conn.commit()

    def compute_and_persist(
        self,
        ctx: MarketContext,
        score: Any,
        advanced_results: Any,
        basic_results: Any,
        conn=None,
        tenor: str = TENOR_SWING,
    ) -> TradeBiasSnapshot:
        profile = profile_for(tenor)
        inputs = self.build_inputs(ctx, score, advanced_results, basic_results)
        structural = compute_bias(inputs)
        tactical = self.build_tactical(ctx, inputs, profile)
        fused = fuse(structural, tactical, profile)
        snapshot = self.build_snapshot(ctx, inputs, structural, tactical, fused, tenor=tenor)
        self.persist(snapshot, conn=conn)
        return snapshot
