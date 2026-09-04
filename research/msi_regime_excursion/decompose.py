"""Split the MSI into what it measures, and rebuild the obvious alternatives.

The composite is a signed sum: ``sum_offset = SUM(points_i * score_i)``, mapped
through ``composite = 50 + 50 * tanh(sum_offset / 50)``
(:mod:`src.signals.scoring_engine`). Every component contributes on a signed
[-1, +1] axis -- but the components do not all point along the *same* axis, and
each one's own docstring says which one it is on:

============================ ==== =========== =================================
component                    pts  axis        what its own docstring says +1 is
============================ ==== =========== =================================
``gamma_anchor``             30   magnitude   "price is 'free' ... expect
                                              movement"; -1 "anchored ->
                                              expect chop/pinning"
``net_gex_sign``             16   magnitude   short gamma, "amplifies
                                              volatility"
``volatility_regime``        6    magnitude   ``(vix - 20) / 10``
``order_flow_imbalance``     19   DIRECTION   "call premium dominates
                                              (**bullish** model output)"
``dealer_delta_pressure``    17   DIRECTION   dealers short delta, "**bullish**
                                              for price"
``put_call_ratio``           12   ambiguous   code comment claims "larger
                                              potential move"; the formula
                                              ``(pcr - 1) / sat`` is the
                                              standard bearish-sentiment gauge
============================ ==== =========== =================================

So at least 36 of the 100 points are a *direction* read, on a scale whose bands
are labelled as a *regime*. ``frontend/core/impliedDirection.ts`` states the
opposite in as many words -- "It is deliberately directionless."

That is a hypothesis about what the number does, and it is testable two ways:
without data, by driving the production engine (see :mod:`structural`), and
with data, by rebuilding the alternatives here and scoring them against the
same forward excursion:

``msi``
    the composite as shipped.
``msi_folded``
    ``|msi - 50| * 2``. The cheapest possible repair: fold the scale so that
    distance from neutral, in either direction, is what the gauge reports.
``msi_magnitude``
    recomputed from the magnitude components only.
``msi_magnitude_pcr``
    the same, plus the ambiguous ``put_call_ratio``.
``msi_direction``
    recomputed from the directional components only. This is the **negative
    control**: if it predicts excursion magnitude as well as the shipped MSI
    does, then the shipped MSI's apparent regime content is direction wearing a
    regime label.

Every variant is rebuilt with the engine's own renormalization rule -- a
partial component set is scaled back onto the full 100-point scale exactly as
``ScoringEngine.score`` does when components abstain -- so a variant is "what
the MSI would read if only these components existed", not a new formula.

**Reconstruction fidelity.** The persisted payload stores each component's
*display* score, which differs from its composite input only when the component
abstained (``|raw| < 1e-3``, replaced by a small regime tilt for display while
being excluded from the composite entirely). So a row reconstructs exactly
unless something abstained, and :func:`reconstruct` reports the error against
the persisted ``composite_score`` so rows that do not reconstruct can be
counted, reported, and excluded from the variant arms.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional

__all__ = [
    "AXIS",
    "MAGNITUDE_COMPONENTS",
    "DIRECTION_COMPONENTS",
    "AMBIGUOUS_COMPONENTS",
    "VARIANTS",
    "ComponentRead",
    "read_components",
    "reconstruct",
    "variant_scores",
    "SAT_SCALE",
    "FULL_SCALE_POINTS",
]

#: ``_COMPOSITE_SAT_SCALE`` in ``src/signals/scoring_engine.py``.
SAT_SCALE = 50.0
#: Total component points on the full scale.
FULL_SCALE_POINTS = 100.0

AXIS: dict[str, str] = {
    "gamma_anchor": "magnitude",
    "net_gex_sign": "magnitude",
    "volatility_regime": "magnitude",
    "order_flow_imbalance": "direction",
    "dealer_delta_pressure": "direction",
    "put_call_ratio": "ambiguous",
}

MAGNITUDE_COMPONENTS = tuple(k for k, v in AXIS.items() if v == "magnitude")
DIRECTION_COMPONENTS = tuple(k for k, v in AXIS.items() if v == "direction")
AMBIGUOUS_COMPONENTS = tuple(k for k, v in AXIS.items() if v == "ambiguous")

#: Fallback points per component, from ``ScoringEngine.COMPONENT_POINTS``. Used
#: only when the persisted payload carries no ``max_points`` for a component.
FALLBACK_POINTS: dict[str, float] = {
    "net_gex_sign": 16.0,
    "gamma_anchor": 30.0,
    "put_call_ratio": 12.0,
    "volatility_regime": 6.0,
    "order_flow_imbalance": 19.0,
    "dealer_delta_pressure": 17.0,
}

#: Which components make up each variant. ``None`` means "not rebuilt from
#: components" (the shipped score, and the folded transform of it).
VARIANTS: dict[str, Optional[tuple[str, ...]]] = {
    "msi": None,
    "msi_folded": None,
    "msi_magnitude": MAGNITUDE_COMPONENTS,
    "msi_magnitude_pcr": MAGNITUDE_COMPONENTS + AMBIGUOUS_COMPONENTS,
    "msi_direction": DIRECTION_COMPONENTS,
}


@dataclass(frozen=True)
class ComponentRead:
    name: str
    score: float      # signed [-1, +1]
    points: float     # max points on the 100-point scale


def read_components(payload: Any) -> dict[str, ComponentRead]:
    """Pull ``{name: ComponentRead}`` out of a persisted ``components`` blob."""
    out: dict[str, ComponentRead] = {}
    if not isinstance(payload, dict):
        return out
    for name, entry in payload.items():
        if name.startswith("__") or not isinstance(entry, dict):
            continue
        raw = entry.get("score")
        if raw is None:
            continue
        try:
            score = float(raw)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(score):
            continue
        points_raw = entry.get("max_points")
        try:
            points = float(points_raw) if points_raw is not None else FALLBACK_POINTS.get(name, 0.0)
        except (TypeError, ValueError):
            points = FALLBACK_POINTS.get(name, 0.0)
        # Zero-weight entries are the retired gamma-cluster components, kept
        # in the payload for front-ends that hard-coded their keys. They are
        # not part of the composite and must not be counted here.
        if points <= 0:
            continue
        out[name] = ComponentRead(name=name, score=max(-1.0, min(1.0, score)), points=points)
    return out


def _composite(sum_offset: float, active_points: float) -> Optional[float]:
    """The production mapping, including the abstention renormalization."""
    if active_points <= 0:
        return None
    full = sum_offset * (FULL_SCALE_POINTS / active_points)
    value = 50.0 + 50.0 * math.tanh(full / SAT_SCALE)
    return max(0.0, min(100.0, value))


def reconstruct(components: dict[str, ComponentRead]) -> Optional[float]:
    """Rebuild the shipped composite from every component in the payload."""
    if not components:
        return None
    sum_offset = sum(c.points * c.score for c in components.values())
    active = sum(c.points for c in components.values())
    return _composite(sum_offset, active)


def variant_scores(
    components: dict[str, ComponentRead],
    msi: float,
) -> dict[str, Optional[float]]:
    """Every variant's 0-100 reading for one row."""
    out: dict[str, Optional[float]] = {
        "msi": msi,
        # Fold the scale about neutral: 50 -> 0, and both 0 and 100 -> 100.
        "msi_folded": min(100.0, abs(msi - 50.0) * 2.0),
    }
    for name, members in VARIANTS.items():
        if members is None:
            continue
        subset = {k: v for k, v in components.items() if k in members}
        if not subset:
            out[name] = None
            continue
        sum_offset = sum(c.points * c.score for c in subset.values())
        active = sum(c.points for c in subset.values())
        out[name] = _composite(sum_offset, active)
    return out
