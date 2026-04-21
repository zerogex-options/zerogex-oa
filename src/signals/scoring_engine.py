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
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from src.config import (
    SIGNALS_CONTRARIAN_OVERRIDE_ENABLED,
    SIGNALS_CONTRARIAN_OVERRIDE_MIN_COMPOSITE,
    SIGNALS_CONTRARIAN_OVERRIDE_THRESHOLD,
    SIGNALS_CONVICTION_ABSTAIN_EPSILON,
    SIGNALS_CONVICTION_AGGREGATION_ENABLED,
    SIGNALS_CONVICTION_AGREEMENT_MAX_MULT,
    SIGNALS_CONVICTION_EXTREMITY_MAX_MULT,
)
from src.signals.components.base import ComponentBase, MarketContext
from src.utils import get_logger

# Components whose scores express a contra-trend thesis. When their combined
# weighted score points the opposite way from the composite with enough
# conviction, the scoring engine flips direction (flush / squeeze setup).
_CONTRARIAN_COMPONENT_NAMES = frozenset({"exhaustion", "skew_delta", "positioning_trap"})

logger = get_logger(__name__)


@dataclass
class ScoreSnapshot:
    timestamp: datetime
    underlying: str
    composite_score: float
    normalized_score: float
    direction: str
    components: dict
    # Conviction aggregation diagnostics (populated when enabled)
    aggregation: dict = field(default_factory=dict)


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

        raw_composite = sum(c.weight * score for c, score in component_results)
        composite, aggregation = self._aggregate(component_results, raw_composite)
        composite, aggregation = self._apply_contrarian_override(
            composite, component_results, aggregation
        )
        normalized = abs(composite)

        snapshot = ScoreSnapshot(
            timestamp=ctx.timestamp,
            underlying=ctx.underlying,
            composite_score=round(composite, 6),
            normalized_score=round(normalized, 6),
            direction=self._direction(composite),
            components=weighted_components,
            aggregation=aggregation,
        )
        return snapshot, component_results

    # ------------------------------------------------------------------
    # Conviction aggregation
    # ------------------------------------------------------------------

    @staticmethod
    def _aggregate(
        component_results: list[tuple[ComponentBase, float]],
        raw_composite: float,
    ) -> tuple[float, dict]:
        """Optionally reweight the composite to fight abstention dilution.

        When SIGNALS_CONVICTION_AGGREGATION_ENABLED:
          1. Renormalize against the sum of weights of *active* components
             (|score| >= epsilon) so 6 silent components don't drag the
             composite toward 0.
          2. Amplify by an agreement factor -- a strong majority in one
             direction scales the composite up (up to AGREEMENT_MAX_MULT);
             a tied signal scales it down.
          3. Amplify by an extremity factor -- when the loudest active
             component is near the rails, scale the composite further.

        Always clamped to [-1, 1]. Returns (composite, diagnostics_dict).
        """
        # Preserve legacy behavior when the flag is off.
        if not SIGNALS_CONVICTION_AGGREGATION_ENABLED:
            return raw_composite, {"mode": "legacy_linear"}

        eps = SIGNALS_CONVICTION_ABSTAIN_EPSILON
        active = [(c, s) for c, s in component_results if abs(s) >= eps]
        active_weight = sum(c.weight for c, _ in active)

        if not active or active_weight <= 0:
            return 0.0, {
                "mode": "conviction",
                "active_count": 0,
                "active_weight": 0.0,
                "agreement_multiplier": 0.0,
                "extremity_multiplier": 0.0,
                "raw_composite": round(raw_composite, 6),
                "renormalized": 0.0,
            }

        # Renormalize against active weight only.
        active_numerator = sum(c.weight * s for c, s in active)
        renormalized = active_numerator / active_weight  # in [-1, 1]

        # Agreement: weighted mass of the majority direction over total
        # weighted mass of *all* active components (both signs). 0.5 means
        # a dead tie; 1.0 means unanimous.
        pos_mass = sum(c.weight * s for c, s in active if s > 0)
        neg_mass = sum(c.weight * abs(s) for c, s in active if s < 0)
        total_mass = pos_mass + neg_mass
        if total_mass <= 0:
            agreement = 0.5
        else:
            agreement = max(pos_mass, neg_mass) / total_mass

        # Map agreement -> multiplier.
        #   0.50 (tie)        -> 0.50  (dampen)
        #   0.70              -> ~1.00 (neutral)
        #   1.00 (unanimous)  -> AGREEMENT_MAX_MULT
        max_agree = SIGNALS_CONVICTION_AGREEMENT_MAX_MULT
        if agreement <= 0.5:
            agreement_mult = 0.5
        else:
            # Linear ramp from (0.5, 0.5) to (1.0, max_agree).
            agreement_mult = 0.5 + (max_agree - 0.5) * ((agreement - 0.5) / 0.5)

        # Extremity: how loud is the single strongest active component?
        max_abs = max(abs(s) for _, s in active)
        max_extreme = SIGNALS_CONVICTION_EXTREMITY_MAX_MULT
        if max_abs <= 0.70:
            extremity_mult = 1.0
        else:
            extremity_mult = 1.0 + (max_extreme - 1.0) * ((max_abs - 0.70) / 0.30)

        composite = renormalized * agreement_mult * extremity_mult
        composite = max(-1.0, min(1.0, composite))

        diagnostics = {
            "mode": "conviction",
            "active_count": len(active),
            "active_weight": round(active_weight, 4),
            "raw_composite": round(raw_composite, 6),
            "renormalized": round(renormalized, 6),
            "agreement": round(agreement, 4),
            "agreement_multiplier": round(agreement_mult, 4),
            "max_abs_component": round(max_abs, 4),
            "extremity_multiplier": round(extremity_mult, 4),
        }
        return composite, diagnostics

    @staticmethod
    def _apply_contrarian_override(
        composite: float,
        component_results: list[tuple[ComponentBase, float]],
        diagnostics: dict,
    ) -> tuple[float, dict]:
        """Flip composite sign when the contrarian consensus strongly opposes it.

        Sizing magnitude is preserved; only the direction changes. This lets
        mean-reversion setups trade against the trend-driven majority instead
        of just being dampened by it.
        """
        contrarian_total_weight = 0.0
        contrarian_weighted = 0.0
        for comp, clamped in component_results:
            if comp.name in _CONTRARIAN_COMPONENT_NAMES:
                contrarian_total_weight += comp.weight
                contrarian_weighted += comp.weight * clamped
        contrarian_consensus = (
            contrarian_weighted / contrarian_total_weight
            if contrarian_total_weight > 0
            else 0.0
        )
        diagnostics = {
            **diagnostics,
            "contrarian_consensus": round(contrarian_consensus, 6),
            "contrarian_override": False,
        }

        if not SIGNALS_CONTRARIAN_OVERRIDE_ENABLED:
            return composite, diagnostics
        if abs(composite) < SIGNALS_CONTRARIAN_OVERRIDE_MIN_COMPOSITE:
            return composite, diagnostics
        if abs(contrarian_consensus) < SIGNALS_CONTRARIAN_OVERRIDE_THRESHOLD:
            return composite, diagnostics
        # Require opposite signs -- if the contrarians agree with the trend
        # there is nothing to override.
        if (contrarian_consensus > 0) == (composite > 0):
            return composite, diagnostics

        flipped = -composite
        diagnostics["contrarian_override"] = True
        diagnostics["pre_override_composite"] = round(composite, 6)
        return flipped, diagnostics

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
        # Store aggregation diagnostics inside components JSON for persistence
        # (no schema change needed). The API layer extracts it to a top-level key.
        components_payload = dict(score.components)
        if score.aggregation:
            components_payload["__aggregation__"] = score.aggregation

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
                json.dumps(components_payload, default=str),
            ),
        )

        # Insert per-component scores
        for component, clamped_score in component_results:
            context_vals = component.context_values(ctx)
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
