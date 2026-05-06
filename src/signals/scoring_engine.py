"""Market State Index scoring engine (0-100)."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.spectrum import ensure_non_zero

# Soft-saturation scale for the composite.  ``composite = 50 + 50 *
# tanh(sum_offset / _COMPOSITE_SAT_SCALE)`` so the index asymptotically
# approaches 0 and 100 instead of clamping to them.  At SCALE=50 the
# regime-label boundaries (40 / 70) line up with the same component-sum
# offsets the previous linear formula required, so existing thresholds
# carry over cleanly.
_COMPOSITE_SAT_SCALE = 50.0


@dataclass
class ScoreSnapshot:
    timestamp: datetime
    underlying: str
    composite_score: float  # Market State Index [0, 100]
    normalized_score: float  # same as composite_score / 100
    direction: str  # market state regime label
    components: dict
    aggregation: dict = field(default_factory=dict)


class ScoringEngine:
    """Compute and persist the 0-100 Market State Index."""

    # Weights total 100 pts; composite_score = 50 + sum(weight * score)
    # clamped to [0, 100].
    #
    # Phase 2.1 collapsed the three correlated gamma-anchor components
    # (flip_distance / local_gamma / price_vs_max_gamma) into a single
    # 30-pt ``gamma_anchor`` that internally blends them.  Their three
    # sub-scores remain visible nested inside gamma_anchor's `context`
    # field in the API response.  The 11 pts freed by the collapse went
    # to the two leading-indicator components added in Phase 3.1
    # (order_flow_imbalance + dealer_delta_pressure).
    COMPONENT_POINTS: dict[str, float] = {
        "net_gex_sign": 16.0,
        "gamma_anchor": 30.0,
        "put_call_ratio": 12.0,
        "volatility_regime": 6.0,
        "order_flow_imbalance": 19.0,  # Phase 3.1 13 -> 19 (+6)
        "dealer_delta_pressure": 17.0,  # Phase 3.1 12 -> 17 (+5)
    }

    def __init__(self, underlying: str, components: list[ComponentBase]):
        self.underlying = underlying
        self.components = components

    @staticmethod
    def _regime_label(msi: float) -> str:
        if msi >= 70.0:
            return "trend_expansion"
        if msi >= 40.0:
            return "controlled_trend"
        if msi >= 20.0:
            return "chop_range"
        return "high_risk_reversal"

    def score(
        self, ctx: MarketContext, conn=None
    ) -> tuple[ScoreSnapshot, list[tuple[ComponentBase, float]]]:
        component_results: list[tuple[ComponentBase, float]] = []
        payload: dict[str, dict] = {}

        sum_offset = 0.0
        for component in self.components:
            raw = component.compute(ctx)
            clamped = max(-1.0, min(1.0, float(raw)))
            # Replace abstain-zero scores with a regime-derived tilt so
            # the component contribution lands on a continuous spectrum.
            clamped = ensure_non_zero(clamped, ctx)
            points = self.COMPONENT_POINTS.get(component.name, float(component.weight) * 100.0)
            contribution = points * clamped
            sum_offset += contribution
            component_results.append((component, clamped))
            entry: dict = {
                "score": round(clamped, 6),
                "max_points": round(points, 2),
                "contribution": round(contribution, 6),
            }
            # Components may emit diagnostic sub-fields via context_values()
            # (e.g. gamma_anchor exposes its three subscores + blend weights).
            # Surface them under `context` so the API can render the same
            # detail without separate component entries.  Failures here are
            # non-fatal — the score itself is the contract.
            try:
                ctx_payload = component.context_values(ctx) or {}
            except Exception:
                ctx_payload = {}
            if ctx_payload:
                entry["context"] = ctx_payload
            payload[component.name] = entry

        # Soft tanh saturation in place of a hard [0, 100] clamp.  Sum of
        # weighted component contributions can mathematically run from
        # -100 to +100; mapping through tanh keeps the composite in
        # (0, 100) open-interval — exact 0 / 100 become asymptotic
        # extremes instead of common saturation points.
        composite = 50.0 + 50.0 * math.tanh(sum_offset / _COMPOSITE_SAT_SCALE)
        composite = max(0.0, min(100.0, composite))
        normalized = composite / 100.0
        direction = self._regime_label(composite)

        snapshot = ScoreSnapshot(
            timestamp=ctx.timestamp,
            underlying=ctx.underlying,
            composite_score=round(composite, 6),
            normalized_score=round(normalized, 6),
            direction=direction,
            components=payload,
            aggregation={"mode": "market_state_index"},
        )
        return snapshot, component_results

    def persist(
        self,
        score: ScoreSnapshot,
        component_results: list[tuple[ComponentBase, float]],
        ctx: MarketContext,
        conn=None,
    ) -> None:
        if conn is None:
            from src.database import db_connection

            with db_connection() as local_conn:
                self._persist_inner(score, component_results, ctx, local_conn)
        else:
            self._persist_inner(score, component_results, ctx, conn)

    def _persist_inner(
        self,
        score: ScoreSnapshot,
        component_results: list[tuple[ComponentBase, float]],
        ctx: MarketContext,
        conn,
    ) -> None:
        cur = conn.cursor()
        components_payload = dict(score.components)
        components_payload["__aggregation__"] = dict(score.aggregation)
        cur.execute(
            """
            INSERT INTO signal_scores (
                underlying, timestamp, composite_score, normalized_score, direction, components
            ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (underlying, timestamp) DO UPDATE SET
                composite_score = EXCLUDED.composite_score,
                normalized_score = EXCLUDED.normalized_score,
                direction = EXCLUDED.direction,
                components = EXCLUDED.components,
                updated_at = NOW()
            """,
            (
                score.underlying,
                score.timestamp,
                score.composite_score,
                score.normalized_score,
                score.direction,
                json.dumps(components_payload, default=str),
            ),
        )

        for component, clamped_score in component_results:
            points = float(
                self.COMPONENT_POINTS.get(component.name, float(component.weight) * 100.0)
            )
            context_vals = component.context_values(ctx)
            weighted = round(points * clamped_score, 6)
            cur.execute(
                """
                INSERT INTO signal_component_scores (
                    underlying, timestamp, component_name, clamped_score, weighted_score, weight, context_values
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (underlying, timestamp, component_name) DO UPDATE SET
                    clamped_score = EXCLUDED.clamped_score,
                    weighted_score = EXCLUDED.weighted_score,
                    weight = EXCLUDED.weight,
                    context_values = EXCLUDED.context_values
                """,
                (
                    score.underlying,
                    score.timestamp,
                    component.name,
                    clamped_score,
                    weighted,
                    points,
                    json.dumps(context_vals, default=str),
                ),
            )
        conn.commit()

    def score_and_persist(self, ctx: MarketContext, conn=None) -> ScoreSnapshot:
        snapshot, component_results = self.score(ctx, conn=conn)
        self.persist(snapshot, component_results, ctx, conn=conn)
        return snapshot
