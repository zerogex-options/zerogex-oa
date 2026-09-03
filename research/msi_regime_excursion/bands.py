"""The regime bands under test, and the claims each one makes.

Mirrored from two places that must agree, and are asserted to agree by
``tests/test_msi_excursion_bands.py``:

* ``src.signals.scoring_engine.ScoringEngine._regime_label`` (backend, writes
  ``signal_scores.direction``)
* ``frontend/core/regime.ts`` (``REGIMES``, the customer-facing copy)

``claim`` is the falsifiable reading of each band's copy, expressed as the sign
of the expected difference from the unconditional base rate for a forward
excursion measure. It is what the study scores each band against, so that a
verdict is a statement about the copy rather than about an unlabelled number.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

__all__ = ["Band", "BANDS", "BAND_KEYS", "band_for", "ALL_BUCKET", "claim_for"]

#: Label used for the pooled, unconditional bucket -- the base rate every
#: conditional bucket is compared against.
ALL_BUCKET = "ALL"


@dataclass(frozen=True)
class Band:
    key: str
    lo: float  # inclusive
    hi: float  # exclusive (the top band's hi is open)
    label: str
    copy: str
    #: +1 = copy claims LARGER excursion than the base rate,
    #: -1 = copy claims SMALLER excursion,
    #:  0 = copy makes no excursion claim.
    claim: int


BANDS: tuple[Band, ...] = (
    Band(
        key="high_risk_reversal",
        lo=0.0,
        hi=20.0,
        label="High-Risk Reversal",
        copy="Mean-reversion only — extreme move risk elevated.",
        # "Mean-reversion only" is a claim that moves do not run: directional
        # follow-through should be BELOW the base rate. ("extreme move risk
        # elevated" is a tail claim, scored separately via the tail measures.)
        claim=-1,
    ),
    Band(
        key="chop_range",
        lo=20.0,
        hi=40.0,
        label="Chop / Range",
        copy="Range-bound — fade extremes, avoid trend trades.",
        claim=-1,
    ),
    Band(
        key="controlled_trend",
        lo=40.0,
        hi=70.0,
        label="Controlled Trend",
        copy="Moderate directional edge — trade with reduced size.",
        claim=+1,
    ),
    Band(
        key="trend_expansion",
        lo=70.0,
        hi=100.0,
        label="Trend / Expansion",
        copy="Strong directional regime — favor trades in the prevailing bias.",
        claim=+1,
    ),
)

#: Bands weakest-to-strongest. A valid regime gauge orders excursion this way.
BAND_KEYS: tuple[str, ...] = tuple(b.key for b in BANDS)


def band_for(msi: Optional[float]) -> Optional[str]:
    """Return the band key for an MSI reading, or None if it has none.

    Boundary handling matches ``_regime_label`` exactly: the comparison there
    is ``>=`` walking DOWN from 70, so 70.0 is trend_expansion, 40.0 is
    controlled_trend and 20.0 is chop_range.
    """
    if msi is None:
        return None
    try:
        value = float(msi)
    except (TypeError, ValueError):
        return None
    if value != value:  # NaN
        return None
    if value >= 70.0:
        return "trend_expansion"
    if value >= 40.0:
        return "controlled_trend"
    if value >= 20.0:
        return "chop_range"
    if value >= 0.0:
        return "high_risk_reversal"
    return None


def claim_for(band_key: str) -> int:
    for band in BANDS:
        if band.key == band_key:
            return band.claim
    return 0
