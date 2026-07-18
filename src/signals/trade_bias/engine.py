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
from src.signals.trade_bias.bias import BiasInput, BiasResult, compute_bias
from src.utils import get_logger

logger = get_logger(__name__)

# Signal ``.score`` in the cycle results is the persisted ``clamped_score`` on
# the ``[-1, 1]`` scale; the API multiplies it by 100 for the front end, which
# is the scale ``compute_bias`` expects — so we do the same.
_TREND_SCALE = 100.0

TENOR_SWING = "swing"
TENOR_INTRADAY = "intraday"

_TREND_TO_DIRECTION = {"bullish": "long", "bearish": "short", "neutral": "neutral"}


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

    def build_snapshot(
        self,
        ctx: MarketContext,
        inputs: BiasInput,
        result: BiasResult,
        tenor: str = TENOR_SWING,
    ) -> TradeBiasSnapshot:
        # Signed bias on -100..100. Phase 1 has no fused directional score yet,
        # so derive it from the regime's trend and confidence; Phase 3 replaces
        # this with the fused price-action/flow/tape/momentum vector.
        trend_sign = {"bullish": 1.0, "bearish": -1.0}.get(result.trend, 0.0)
        confidence_pct = (
            (result.confidence / result.maxConfidence) * 100.0 if result.maxConfidence else 0.0
        )
        bias_score = round(trend_sign * confidence_pct, 4)
        direction = _TREND_TO_DIRECTION.get(result.trend, "neutral")

        payload: dict[str, Any] = {
            "tenor": tenor,
            "bias_score": bias_score,
            "direction": direction,
            # Phase 1: no fusion yet, so the read is always the structural
            # baseline and never an override. The keys are here now so the
            # contract stays stable when Phase 3 populates them.
            "state": "baseline",
            "override": {"active": False},
            "confidence": round(confidence_pct, 2),
            "confidence_raw": result.confidence,
            "max_confidence_raw": result.maxConfidence,
            "regime": {
                "gamma": self._gamma_regime(ctx),
                "volatility": self._volatility_regime(ctx),
            },
            "market_state": result.marketState,
            "regime_label": result.regimeLabel,
            "regime_desc": result.regimeDesc,
            "bias": {
                "code": result.bias,
                "label": result.biasLabel,
                "trend": result.trend,
            },
            "setup": result.setup,
            "playbook": list(result.playbook),
            "expected_behavior": list(result.expectedBehavior),
            "checklist": [{"label": c.label, "passed": c.passed} for c in result.checklist],
            "conviction_driven": result.convictionDriven,
            "watching": [
                {"key": w.key, "label": w.label, "direction": w.direction} for w in result.watching
            ],
            "has_data": result.hasData,
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
            bias_score=bias_score,
            direction=direction,
            bias_code=result.bias,
            market_state=result.marketState,
            state="baseline",
            confidence=round(confidence_pct, 2),
            override_active=False,
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
        inputs = self.build_inputs(ctx, score, advanced_results, basic_results)
        result = compute_bias(inputs)
        snapshot = self.build_snapshot(ctx, inputs, result, tenor=tenor)
        self.persist(snapshot, conn=conn)
        return snapshot
