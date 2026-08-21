"""Inventory reconstruction confidence.

Every MM-attributed number must be readable in the context of how complete the
inferred inventory actually is.  Without that, a null result is ambiguous
between "participant attribution carries no extra information" and "we only
reconstructed a third of the gamma".  This module makes the difference
measurable.

Two levels of score:

**Per series** (:func:`score_position`) — a 0..1 confidence for one contract,
built from five factors that each answer a specific failure mode:

===================== ===============================================================
factor                what it protects against
===================== ===============================================================
``origin``            inventory that starts from an unknown pre-window position
``continuity``        sessions missing from the middle of the contract's life
``effect_coverage``   volume whose open/close designation is unknown
``breach``            closing volume exceeding observed opens (proof of censoring)
``maturity``          a contract observed for too little of its life to trust
===================== ===============================================================

The factors multiply.  A product, not an average, because these are
*conjunctive* requirements: a perfect continuity score cannot rescue a series
whose starting position is unknown.  Averaging would let one strong factor mask
a fatal one.

**Per snapshot** (:func:`score_universe`) — the number that actually gates
interpretation: what share of the *gamma that matters* is carried by
well-reconstructed contracts.  Weighting by |dollar gamma| rather than by
contract count is the point: 2,000 cleanly reconstructed far-OTM series that
carry no gamma do not make a snapshot trustworthy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from research.mm_attributed_gex.inventory import MMPosition

__all__ = [
    "ConfidenceWeights",
    "PositionConfidence",
    "UniverseConfidence",
    "score_position",
    "score_universe",
    "confidence_band",
]


@dataclass(frozen=True)
class ConfidenceWeights:
    """Tunable knobs for the per-series score.

    Defaults are deliberately conservative — a left-censored series scores
    ``0.15``, not ``0.5``, because an unknown starting position is close to
    fatal for a *level* even though the *changes* remain informative.
    """

    censored_origin: float = 0.15
    #: Credit for a series that started inside the window but whose listing
    #: date could not be confirmed independently.
    inferred_origin: float = 0.85
    confirmed_origin: float = 1.0
    #: Each missing session inside the contract's life costs this much,
    #: multiplicatively.
    missing_session_decay: float = 0.75
    #: Share of volume with an UNKNOWN position effect at which the
    #: effect-coverage factor reaches its floor.
    effect_unknown_full_penalty: float = 0.5
    effect_coverage_floor: float = 0.4
    #: Each floor breach (closing volume in excess of observed opens).
    breach_decay: float = 0.6
    #: Sessions of observed life at which the maturity factor saturates.
    maturity_saturation_sessions: int = 3
    maturity_floor: float = 0.5


@dataclass
class PositionConfidence:
    """Per-series confidence with its factor decomposition kept visible."""

    key: tuple
    score: float
    origin: float
    continuity: float
    effect_coverage: float
    breach: float
    maturity: float
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "score": round(self.score, 6),
            "origin": round(self.origin, 6),
            "continuity": round(self.continuity, 6),
            "effect_coverage": round(self.effect_coverage, 6),
            "breach": round(self.breach, 6),
            "maturity": round(self.maturity, 6),
            "reason": self.reason,
        }


def score_position(
    pos: MMPosition,
    weights: ConfidenceWeights = ConfidenceWeights(),
) -> PositionConfidence:
    """Confidence in ``pos``'s reconstructed inventory, in ``[0, 1]``."""
    # Origin — was the inventory started from a known zero?
    if pos.left_censored:
        origin = weights.censored_origin
    elif pos.left_censor_reason == "observed_from_listing_date":
        origin = weights.confirmed_origin
    else:
        origin = weights.inferred_origin

    # Continuity — sessions missing from inside the contract's observed life.
    n_missing = len(pos.missing_sessions)
    continuity = weights.missing_session_decay**n_missing if n_missing else 1.0

    # Effect coverage — how much volume had no usable open/close designation.
    share = pos.unknown_effect_share
    if weights.effect_unknown_full_penalty <= 0:
        effect_coverage = 1.0
    else:
        ramp = min(1.0, share / weights.effect_unknown_full_penalty)
        effect_coverage = 1.0 - ramp * (1.0 - weights.effect_coverage_floor)

    # Breach — direct proof that inventory predates the data.
    breaches = pos.long_floor_breaches + pos.short_floor_breaches
    breach = weights.breach_decay**breaches if breaches else 1.0

    # Maturity — a contract seen for one session carries less signal than one
    # tracked across its whole life.
    sat = max(1, weights.maturity_saturation_sessions)
    observed = len(pos.active_sessions)
    maturity = weights.maturity_floor + (1.0 - weights.maturity_floor) * min(1.0, observed / sat)

    score = origin * continuity * effect_coverage * breach * maturity
    reason = pos.left_censor_reason if pos.left_censored else "clean"
    return PositionConfidence(
        key=pos.key,
        score=max(0.0, min(1.0, score)),
        origin=origin,
        continuity=continuity,
        effect_coverage=effect_coverage,
        breach=breach,
        maturity=maturity,
        reason=reason,
    )


def confidence_band(score: Optional[float]) -> str:
    """Coarse label for reporting: ``high`` / ``medium`` / ``low`` / ``none``."""
    if score is None:
        return "none"
    if score >= 0.70:
        return "high"
    if score >= 0.40:
        return "medium"
    if score > 0.0:
        return "low"
    return "none"


@dataclass
class UniverseConfidence:
    """Snapshot-level completeness — the number that gates interpretation."""

    n_series: int = 0
    n_clean_series: int = 0
    contracts_total: float = 0.0
    contracts_clean: float = 0.0
    gamma_universe_abs: float = 0.0
    gamma_reconstructed_abs: float = 0.0
    gamma_weighted_confidence: float = 0.0
    mean_position_confidence: float = 0.0
    pct_gamma_universe_reconstructed: float = 0.0
    per_key: Mapping[tuple, PositionConfidence] = field(default_factory=dict)

    @property
    def band(self) -> str:
        return confidence_band(self.gamma_weighted_confidence)

    def as_dict(self) -> dict[str, Any]:
        return {
            "n_series": self.n_series,
            "n_clean_series": self.n_clean_series,
            "clean_series_share": (self.n_clean_series / self.n_series if self.n_series else 0.0),
            "contracts_total": self.contracts_total,
            "contracts_clean": self.contracts_clean,
            "gamma_universe_abs": self.gamma_universe_abs,
            "gamma_reconstructed_abs": self.gamma_reconstructed_abs,
            "pct_gamma_universe_reconstructed": self.pct_gamma_universe_reconstructed,
            "gamma_weighted_confidence": self.gamma_weighted_confidence,
            "mean_position_confidence": self.mean_position_confidence,
            "band": self.band,
        }


def score_universe(
    positions: Sequence[MMPosition],
    *,
    gamma_by_key: Optional[Mapping[tuple, float]] = None,
    universe_gamma_abs: Optional[float] = None,
    weights: ConfidenceWeights = ConfidenceWeights(),
) -> UniverseConfidence:
    """Aggregate per-series confidence into a snapshot-level view.

    :param positions: reconstructed MM positions live at the snapshot.
    :param gamma_by_key: ``|dollar gamma|`` each series contributes, keyed the
        same way as :attr:`MMPosition.key`.  Supplied by the GEX layer, which
        is the only place that knows the gamma.  When omitted, contract count
        stands in — a strictly worse proxy, and flagged as such by the caller.
    :param universe_gamma_abs: total ``|dollar gamma|`` of the FULL production
        option universe at this snapshot (from ZeroGEX's own chain).  This is
        the denominator that turns "we reconstructed 800 contracts" into "we
        reconstructed 34% of the gamma that exists".
    """
    out = UniverseConfidence(n_series=len(positions))
    if not positions:
        return out

    per_key: dict[tuple, PositionConfidence] = {}
    weight_sum = 0.0
    weighted_conf = 0.0
    conf_sum = 0.0
    recon_gamma = 0.0

    for pos in positions:
        conf = score_position(pos, weights)
        per_key[pos.key] = conf
        conf_sum += conf.score
        if not pos.left_censored:
            out.n_clean_series += 1
            out.contracts_clean += abs(pos.net_contracts)
        out.contracts_total += abs(pos.net_contracts)

        weight = (
            abs(gamma_by_key.get(pos.key, 0.0))
            if gamma_by_key is not None
            else abs(pos.net_contracts)
        )
        weight_sum += weight
        weighted_conf += weight * conf.score
        recon_gamma += weight

    out.per_key = per_key
    out.mean_position_confidence = conf_sum / len(positions)
    out.gamma_weighted_confidence = weighted_conf / weight_sum if weight_sum > 0 else 0.0
    out.gamma_reconstructed_abs = recon_gamma
    if universe_gamma_abs and universe_gamma_abs > 0:
        out.gamma_universe_abs = universe_gamma_abs
        out.pct_gamma_universe_reconstructed = min(1.0, recon_gamma / universe_gamma_abs)
    return out
