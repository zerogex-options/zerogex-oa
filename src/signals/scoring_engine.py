"""
Layer 1 of the ZeroGEX Signal Engine.

ScoringEngine maintains a registry of ComponentBase instances, calls each
every cycle with the current MarketContext, computes a weighted composite
score, and persists both the composite and individual component scores.

Adding a new signal source = instantiate its ComponentBase subclass and
pass it to ScoringEngine's constructor. Nothing else changes.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from src.signals.components.base import ComponentBase, MarketContext
from src.utils import get_logger

logger = get_logger(__name__)


@dataclass
class ScoreSnapshot:
    timestamp: datetime
    underlying: str
    composite_score: float
    normalized_score: float
    direction: str
    components: dict


class ScoringEngine:
    def __init__(self, underlying: str, components: list[ComponentBase]):
        self.underlying = underlying
        self.components = components
        self._weight_sum = sum(c.weight for c in components)
        if abs(self._weight_sum - 1.0) > 0.001:
            raise ValueError(
                f"Component weights must sum to 1.0 (got {self._weight_sum:.6f})"
            )

    @staticmethod
    def _direction(score: float) -> str:
        if score > 0:
            return "bullish"
        if score < 0:
            return "bearish"
        return "neutral"

    def score(self, ctx: MarketContext, conn=None) -> tuple[ScoreSnapshot, list[tuple[ComponentBase, float]]]:
        """Compute composite score from all components.

        Returns (ScoreSnapshot, component_results) where component_results is
        a list of (component, clamped_score) tuples.
        """
        component_results: list[tuple[ComponentBase, float]] = []
        weighted_components: dict = {}

        for component in self.components:
            raw = component.compute(ctx)
            clamped = max(-1.0, min(1.0, raw))
            component_results.append((component, clamped))
            weighted_components[component.name] = {
                "weight": component.weight,
                "score": clamped,
            }

        composite = sum(c.weight * score for c, score in component_results)
        normalized = abs(composite)

        snapshot = ScoreSnapshot(
            timestamp=ctx.timestamp,
            underlying=ctx.underlying,
            composite_score=round(composite, 6),
            normalized_score=round(normalized, 6),
            direction=self._direction(composite),
            components=weighted_components,
        )
        return snapshot, component_results

    def persist(
        self,
        score: ScoreSnapshot,
        component_results: list[tuple[ComponentBase, float]],
        ctx: MarketContext,
        conn=None,
    ) -> None:
        """Write composite score to signal_scores and per-component scores to signal_component_scores."""
        if conn is None:
            from src.database import db_connection
            with db_connection() as conn:
                self._persist_inner(score, component_results, ctx, conn)
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
        # Upsert into signal_scores
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
                json.dumps(score.components, default=str),
            ),
        )

        # Insert per-component scores
        for component, clamped_score in component_results:
            context_vals = component.context_values(ctx)
            cur.execute(
                """
                INSERT INTO signal_component_scores (
                    underlying, timestamp, component_name, raw_score, weighted_score, weight, context_values
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (underlying, timestamp, component_name) DO UPDATE SET
                    raw_score = EXCLUDED.raw_score,
                    weighted_score = EXCLUDED.weighted_score,
                    weight = EXCLUDED.weight,
                    context_values = EXCLUDED.context_values
                """,
                (
                    score.underlying,
                    score.timestamp,
                    component.name,
                    clamped_score,
                    round(component.weight * clamped_score, 6),
                    component.weight,
                    json.dumps(context_vals, default=str),
                ),
            )

        conn.commit()

    def score_and_persist(self, ctx: MarketContext, conn=None) -> ScoreSnapshot:
        """Convenience method: compute score, persist, and return snapshot."""
        snapshot, component_results = self.score(ctx, conn=conn)
        self.persist(snapshot, component_results, ctx, conn=conn)
        return snapshot
