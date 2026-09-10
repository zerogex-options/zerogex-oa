"""The experiment: does the MSI regime read order realized forward excursion?

One study run produces, for every instrument and every horizon:

1. **Rank correlation** between the score and each excursion measure, with a
   session-level block-bootstrap interval. This is the headline: it needs no
   bucketing and no threshold, so it cannot be tuned into a result.
2. **Each regime band scored against the unconditional base rate** for the same
   instrument, horizon and measure -- the baseline ``content/methodology.md``
   names. Effect size (Cliff's delta) and a block-bootstrap interval, not just
   means.
3. **Score deciles**, which catch a monotone relationship that the four bands
   are too coarse to show, and an ordering test across the bands.
4. **Point-target hit rates** -- P(a 4 / 8 / 10 point run) by band against the
   same base rate, because that is the question in the units a scalper uses.
5. **The variants** from :mod:`decompose` scored the same way, so "the shipped
   MSI" can be compared head to head against "fold it about neutral" and
   "magnitude components only".

Multiplicity is real -- instruments x horizons x measures x bands is several
hundred tests -- so every p-value in the run goes through Benjamini-Hochberg
together and the report shows what survives.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Sequence

from research.msi_regime_excursion import stats
from research.msi_regime_excursion.bands import BAND_KEYS, band_for
from research.msi_regime_excursion.decompose import (
    VARIANTS,
    read_components,
    reconstruct,
    variant_scores,
)
from research.msi_regime_excursion.excursion import (
    ET,
    REST_OF_SESSION,
    BarSeries,
    compute_excursion,
)
from research.msi_regime_excursion.sources import Instrument, Reading

__all__ = [
    "Row",
    "MEASURES",
    "build_rows",
    "StudyResult",
    "run_study",
]

#: Excursion measures scored against the bands. ``claim_sign`` says which way
#: the copy claims the measure should move for a "trend" band relative to base.
MEASURES: dict[str, str] = {
    "max_up_bps": "largest upward excursion",
    "max_down_bps": "largest downward excursion",
    "range_bps": "high-to-low range",
    "abs_ret_bps": "absolute net move at the horizon",
    "mfe_bps": "excursion WITH the prevailing bias",
    "mae_bps": "excursion AGAINST the prevailing bias",
}

#: How closely a rebuilt composite must match the persisted one for the row to
#: count as exactly reconstructible (see decompose.reconstruct).
RECONSTRUCTION_TOLERANCE = 0.05


@dataclass
class Row:
    """One reading joined to the price action that followed it."""

    timestamp: datetime
    session: Any                 # ET date -- the block-bootstrap unit
    msi: float
    band: Optional[str]
    persisted_band: Optional[str]
    variants: dict[str, Optional[float]] = field(default_factory=dict)
    reconstruction_error: Optional[float] = None
    bias: int = 0
    measures: dict[tuple[str, object], Optional[float]] = field(default_factory=dict)
    points: dict[tuple[str, object], Optional[float]] = field(default_factory=dict)

    def value(self, measure: str, horizon: object) -> Optional[float]:
        return self.measures.get((measure, horizon))


def build_rows(
    readings: Sequence[Reading],
    series: BarSeries,
    *,
    horizons: Sequence[int],
    bias_lookback_min: int = 30,
    include_rest_of_session: bool = True,
) -> list[Row]:
    """Join each reading to its forward excursion. Readings with no bar are dropped."""
    rows: list[Row] = []
    for reading in readings:
        exc = compute_excursion(
            series,
            reading.timestamp,
            horizons=horizons,
            bias_lookback_min=bias_lookback_min,
            include_rest_of_session=include_rest_of_session,
        )
        if exc is None:
            continue
        components = read_components(reading.components)
        rebuilt = reconstruct(components)
        err = abs(rebuilt - reading.msi) if rebuilt is not None else None
        row = Row(
            timestamp=reading.timestamp,
            session=reading.timestamp.astimezone(ET).date(),
            msi=reading.msi,
            band=band_for(reading.msi),
            persisted_band=reading.persisted_band,
            variants=variant_scores(components, reading.msi),
            reconstruction_error=err,
            bias=exc.bias,
        )
        keys: list[object] = list(horizons)
        if include_rest_of_session:
            keys.append(REST_OF_SESSION)
        for h in keys:
            row.measures[("max_up_bps", h)] = exc.max_up_bps.get(h)
            row.measures[("max_down_bps", h)] = exc.max_down_bps.get(h)
            row.measures[("range_bps", h)] = exc.range_bps.get(h)
            row.measures[("abs_ret_bps", h)] = exc.abs_ret_bps.get(h)
            row.measures[("ret_bps", h)] = exc.ret_bps.get(h)
            row.measures[("mfe_bps", h)] = exc.mfe_bps.get(h)
            row.measures[("mae_bps", h)] = exc.mae_bps.get(h)
            row.points[("max_up_pts", h)] = exc.max_up_pts.get(h)
            row.points[("max_down_pts", h)] = exc.max_down_pts.get(h)
        rows.append(row)
    return rows


@dataclass
class BucketFinding:
    instrument: str
    horizon: object
    measure: str
    bucket_kind: str            # "band" | "decile"
    bucket: str
    comparison: stats.Comparison
    survives_bh: bool = False

    def as_dict(self) -> dict:
        d = {
            "instrument": self.instrument, "horizon": self.horizon,
            "measure": self.measure, "bucket_kind": self.bucket_kind,
            "bucket": self.bucket, "survives_bh": self.survives_bh,
        }
        d.update(self.comparison.as_dict())
        return d


@dataclass
class TargetFinding:
    instrument: str
    horizon: object
    target_pts: float
    side: str                   # "up" | "down" | "either"
    bucket: str
    n: int
    hits: int
    result: dict
    survives_bh: bool = False

    def as_dict(self) -> dict:
        d = {
            "instrument": self.instrument, "horizon": self.horizon,
            "target_pts": self.target_pts, "side": self.side,
            "bucket": self.bucket, "n": self.n, "hits": self.hits,
            "survives_bh": self.survives_bh,
        }
        d.update(self.result)
        return d


@dataclass
class CorrelationFinding:
    instrument: str
    horizon: object
    measure: str
    score: str                  # variant name
    correlation: stats.RankCorrelation
    survives_bh: bool = False

    def as_dict(self) -> dict:
        d = {
            "instrument": self.instrument, "horizon": self.horizon,
            "measure": self.measure, "score": self.score,
            "survives_bh": self.survives_bh,
        }
        d.update(self.correlation.as_dict())
        return d


@dataclass
class StudyResult:
    instrument: str
    n_rows: int
    n_sessions: int
    first: Optional[datetime]
    last: Optional[datetime]
    band_counts: dict[str, int]
    reconstruction_ok: int
    reconstruction_total: int
    correlations: list[CorrelationFinding] = field(default_factory=list)
    buckets: list[BucketFinding] = field(default_factory=list)
    targets: list[TargetFinding] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _assign_deciles(values: Sequence[Optional[float]]) -> list[Optional[int]]:
    """Decile index 0-9 by rank, so bucket sizes are equal regardless of shape."""
    indexed = [(v, i) for i, v in enumerate(values) if v is not None]
    out: list[Optional[int]] = [None] * len(values)
    n = len(indexed)
    if n < 10:
        return out
    indexed.sort(key=lambda t: t[0])
    for rank, (_, i) in enumerate(indexed):
        out[i] = min(9, rank * 10 // n)
    return out


def run_study(
    inst: Instrument,
    rows: Sequence[Row],
    *,
    horizons: Sequence[object],
    measures: Sequence[str] = tuple(MEASURES),
    variants: Sequence[str] = tuple(VARIANTS),
    iterations: int = 2000,
    seed: int = 20260903,
    min_bucket: int = 30,
) -> StudyResult:
    """Score one instrument end to end."""
    sessions = sorted({r.session for r in rows})
    band_counts: dict[str, int] = {k: 0 for k in BAND_KEYS}
    for r in rows:
        if r.band:
            band_counts[r.band] = band_counts.get(r.band, 0) + 1

    recon_total = sum(1 for r in rows if r.reconstruction_error is not None)
    recon_ok = sum(
        1 for r in rows
        if r.reconstruction_error is not None
        and r.reconstruction_error <= RECONSTRUCTION_TOLERANCE
    )

    result = StudyResult(
        instrument=inst.key,
        n_rows=len(rows),
        n_sessions=len(sessions),
        first=rows[0].timestamp if rows else None,
        last=rows[-1].timestamp if rows else None,
        band_counts=band_counts,
        reconstruction_ok=recon_ok,
        reconstruction_total=recon_total,
    )
    if inst.inherits_score:
        result.notes.append(
            f"{inst.key} has no MSI of its own: the score is {inst.score_symbol}'s, "
            f"read against {inst.key} bars. That is what the product shows an "
            f"{inst.key} user (src/jobs/futures_projection.py: scores are not projected)."
        )
    if not rows:
        result.notes.append("No rows: no readings joined to bars in this window.")
        return result

    session_col = [r.session for r in rows]

    for horizon in horizons:
        for measure in measures:
            values = [r.value(measure, horizon) for r in rows]
            if sum(1 for v in values if v is not None) < min_bucket:
                continue

            # 1. Rank correlation, per score variant.
            for variant in variants:
                score_col = [r.variants.get(variant) for r in rows]
                if sum(1 for v in score_col if v is not None) < min_bucket:
                    continue
                rc = stats.spearman_block(
                    score_col, values, session_col,
                    iterations=max(200, iterations // 4), seed=seed,
                )
                result.correlations.append(
                    CorrelationFinding(inst.key, horizon, measure, variant, rc)
                )

            # 2. Each regime band against the unconditional base rate.
            for band_key in BAND_KEYS:
                flags = [r.band == band_key for r in rows]
                if sum(1 for v, f in zip(values, flags) if f and v is not None) < min_bucket:
                    continue
                cmp_ = stats.compare(
                    values, session_col, flags, iterations=iterations, seed=seed
                )
                result.buckets.append(
                    BucketFinding(inst.key, horizon, measure, "band", band_key, cmp_)
                )

            # 3. Deciles of the shipped score.
            deciles = _assign_deciles([r.msi for r in rows])
            for d in range(10):
                flags = [x == d for x in deciles]
                if sum(1 for v, f in zip(values, flags) if f and v is not None) < min_bucket:
                    continue
                cmp_ = stats.compare(
                    values, session_col, flags, iterations=max(500, iterations // 4), seed=seed
                )
                result.buckets.append(
                    BucketFinding(inst.key, horizon, measure, "decile", f"d{d}", cmp_)
                )

        # 4. Point targets, by band, against the pooled base rate.
        for target in inst.point_targets:
            for side, key in (("up", "max_up_pts"), ("down", "max_down_pts")):
                col = [r.points.get((key, horizon)) for r in rows]
                usable = [(v, r) for v, r in zip(col, rows) if v is not None]
                if len(usable) < min_bucket:
                    continue
                base_hits = sum(1 for v, _ in usable if v >= target)
                base_n = len(usable)
                for band_key in BAND_KEYS:
                    sub = [(v, r) for v, r in usable if r.band == band_key]
                    if len(sub) < min_bucket:
                        continue
                    hits = sum(1 for v, _ in sub if v >= target)
                    res = stats.compare_proportions(hits, len(sub), base_hits, base_n)
                    result.targets.append(
                        TargetFinding(
                            inst.key, horizon, target, side, band_key,
                            len(sub), hits, res,
                        )
                    )

    _apply_bh(result)
    return result


def _apply_bh(result: StudyResult, alpha: float = 0.05) -> None:
    """One BH pass over every p-value the instrument produced."""
    findings: list[Any] = []
    p_values: list[Optional[float]] = []
    for f in result.correlations:
        findings.append(f)
        p_values.append(f.correlation.p_block)
    for f in result.buckets:
        findings.append(f)
        p_values.append(f.comparison.p_block)
    for f in result.targets:
        findings.append(f)
        p_values.append(f.result.get("p"))
    for f, keep in zip(findings, stats.benjamini_hochberg(p_values, alpha)):
        f.survives_bh = keep
