"""Market State Index scoring engine (0-100)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime

from src.signals.components.base import ComponentBase, MarketContext


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

    COMPONENT_POINTS: dict[str, float] = {
        "net_gex_sign": 20.0,
        "flip_distance": 25.0,
        "local_gamma": 20.0,
        "put_call_ratio": 15.0,
        "price_vs_max_gamma": 10.0,
        "volatility_regime": 10.0,
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

        total_points = 50.0
        for component in self.components:
            raw = component.compute(ctx)
            clamped = max(-1.0, min(1.0, float(raw)))
            points = self.COMPONENT_POINTS.get(component.name, float(component.weight) * 100.0)
            contribution = points * clamped
            total_points += contribution
            component_results.append((component, clamped))
            payload[component.name] = {
                "score": round(clamped, 6),
                "max_points": round(points, 2),
                "contribution": round(contribution, 6),
            }

        composite = max(0.0, min(100.0, total_points))
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
