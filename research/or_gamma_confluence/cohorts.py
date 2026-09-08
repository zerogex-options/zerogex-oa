"""Cohort definitions and the statistics reported for each.

The cohorts are the brief's eleven, expressed as predicates over event rows so
that adding one is a three-line change and so that every cohort is
reconstructible from a saved dataset without a rebuild.  Thresholds
(confluence distance, extension depth) are ARGUMENTS here rather than baked in,
which is what makes the parameter-stability sweep possible: the same dataset is
re-cohorted at 2/5/10/15/20 points instead of relabelled five times.

No statistics are implemented here.  Everything comes from
``research/msi_regime_excursion/stats.py``, which already knows the two things
that matter for this data shape:

* **Rows sharing a session are not independent.**  A session-level feature
  (net GEX, the day's regime) is identical for every event that day, and those
  events share the day's path.  A textbook two-proportion z-test treats them as
  independent and is measurably anti-conservative for it — ``wall_break_odds``
  measured a naive z-test at a 0.110 false-positive rate versus 0.055 for the
  session-clustered bootstrap, at nominal α=0.05.  ``session_block_bootstrap_diff``
  resamples SESSIONS.
* **Many comparisons.**  Eleven cohorts times five lead times times five
  distance buckets is a multiple-comparisons machine; ``benjamini_hochberg``
  is applied across a report's family of tests, not per test.

Two reporting rules, both enforced in :func:`summarize`:

* ``n`` and the number of distinct SESSIONS are always exposed.  Twelve events
  from one session is not twelve observations of anything.
* A cohort below :data:`MIN_REPORTABLE_N` is labelled ``thin`` and its point
  estimates are still shown but must be read as such.  Suppressing them would
  hide exactly the "we tried and could not tell" answers the brief asks for.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence

from research.msi_regime_excursion import stats
from research.or_gamma_confluence.events import (
    OUTCOME_AMBIGUOUS,
    OUTCOME_CENSORED,
    OUTCOME_REVERSAL,
    RESOLVED_OUTCOMES,
)

__all__ = [
    "MIN_REPORTABLE_N",
    "Cohort",
    "build_cohorts",
    "summarize",
    "compare_to_baseline",
    "confluence_count",
    "has_confluence",
]

#: Below this many resolved events a cohort is labelled ``thin``.  Not a
#: suppression threshold — a null with n=14 is still worth printing, provided
#: nobody mistakes it for a measurement.
MIN_REPORTABLE_N = 30


def confluence_count(row: Mapping[str, Any], distance: float, *, kinds: bool = False) -> int:
    """Levels (or distinct kinds) within ``distance`` points of the extension.

    Reads the bucket columns the dataset wrote.  Falls back to ``0`` when the
    bucket was not built, which is the correct reading: a row with no gamma
    frame has no confluence, and must join the "no confluence" cohort rather
    than vanish from the comparison.
    """
    key = f"{distance:g}".replace(".", "p")
    field = f"gamma_confluence_{'kinds' if kinds else 'count'}_{key}"
    value = row.get(field)
    return int(value) if value is not None else 0


def has_confluence(row: Mapping[str, Any], distance: float) -> bool:
    return confluence_count(row, distance) > 0


def _regime(row: Mapping[str, Any]) -> Optional[int]:
    v = row.get("gamma_regime_sign")
    return int(v) if v is not None else None


def _trend_favours_reversion(row: Mapping[str, Any]) -> Optional[bool]:
    """Is the selected trend read pointing the way the reversion trade wants?

    Above the range the reversion is short, so a ``down`` trend favours it.
    ``None`` when no trend read is available — never silently ``False``, which
    would put "unknown" into the "against" cohort.
    """
    trend = row.get("trend_selected")
    if trend not in ("up", "down"):
        return None
    return trend == ("down" if row.get("side") == "up" else "up")


@dataclass(frozen=True)
class Cohort:
    key: str
    label: str
    predicate: Callable[[Mapping[str, Any]], bool]
    description: str = ""

    def select(self, rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        return [r for r in rows if self.predicate(r)]


def build_cohorts(
    *,
    confluence_distance: float,
    min_extension: float,
    min_broken: int,
) -> list[Cohort]:
    """The brief's eleven cohorts, parameterised by the thresholds under test."""
    d = confluence_distance

    def conf(r: Mapping[str, Any]) -> bool:
        return has_confluence(r, d)

    return [
        Cohort(
            "all",
            "1. All OR extension touches",
            lambda r: True,
            "The baseline every other cohort is measured against.",
        ),
        Cohort(
            "no_confluence",
            "2. No pre-existing gamma confluence",
            lambda r: not conf(r),
            f"No gamma level within {d:g} pts that was already there.",
        ),
        Cohort(
            "confluence",
            "3. Pre-existing gamma confluence",
            conf,
            f"At least one gamma level within {d:g} pts, available before the touch.",
        ),
        Cohort(
            "confluence_pos_gex",
            "4. Confluence + positive GEX",
            lambda r: conf(r) and _regime(r) == 1,
        ),
        Cohort(
            "confluence_neg_gex",
            "5. Confluence + negative GEX",
            lambda r: conf(r) and _regime(r) == -1,
        ),
        Cohort(
            "confluence_above_flip",
            "6. Confluence + above gamma flip",
            lambda r: conf(r) and r.get("spot_above_flip") is True,
        ),
        Cohort(
            "confluence_below_flip",
            "7. Confluence + below gamma flip",
            lambda r: conf(r) and r.get("spot_above_flip") is False,
        ),
        Cohort(
            "confluence_trend_with",
            "8. Confluence + trend favours reversion",
            lambda r: conf(r) and _trend_favours_reversion(r) is True,
        ),
        Cohort(
            "confluence_trend_against",
            "9. Confluence + trend against reversion",
            lambda r: conf(r) and _trend_favours_reversion(r) is False,
        ),
        Cohort(
            "prior_respected",
            "10. Multiple prior extensions respected",
            lambda r: (r.get("prior_extensions_touched") or 0) >= min_broken
            and (r.get("consecutive_extensions_broken") or 0) == 0,
            "Inner rungs produced a retrace before this touch.",
        ),
        Cohort(
            "prior_broken",
            "11. Multiple prior extensions broken",
            lambda r: (r.get("consecutive_extensions_broken") or 0) >= min_broken,
            f"{min_broken}+ consecutive inner rungs sliced through without a retrace.",
        ),
        # Not numbered in the brief, but the control its central claim needs:
        # if the effect only exists on levels that CHASE spot, it is probably
        # the re-centring artefact rather than structure.
        Cohort(
            "confluence_static_only",
            "12. Confluence on non-chasing levels only",
            lambda r: conf(r) and r.get("nearest_gamma_recenters") is False,
            "Max pain / flip / pin — levels that do not re-centre on spot.",
        ),
        Cohort(
            "extreme",
            f"13. Extension depth >= {min_extension:g}R",
            lambda r: abs(float(r.get("extension_k") or 0.0)) >= min_extension,
        ),
        Cohort(
            "extreme_confluence",
            f"14. Depth >= {min_extension:g}R + confluence",
            lambda r: conf(r) and abs(float(r.get("extension_k") or 0.0)) >= min_extension,
        ),
    ]


def _rate(rows: Sequence[Mapping[str, Any]]) -> tuple[int, int]:
    """``(reversals, resolved)`` — ambiguous and censored excluded from both."""
    resolved = [r for r in rows if r.get("outcome") in RESOLVED_OUTCOMES]
    reversals = sum(1 for r in resolved if r.get("outcome") == OUTCOME_REVERSAL)
    return reversals, len(resolved)


def _summary_dict(label: str, values: Sequence[Any]) -> dict[str, Any]:
    s = stats.describe(values)
    return {
        f"{label}_n": s.n,
        f"{label}_mean": s.mean,
        f"{label}_median": s.median,
    }


def summarize(
    rows: Sequence[Mapping[str, Any]],
    *,
    horizons: Sequence[int] = (5, 15, 30),
) -> dict[str, Any]:
    """Every reported statistic for one cohort.

    ``n`` is the raw event count; ``n_resolved`` is the denominator of the
    reversal rate.  Both are reported because their gap IS a result: a cohort
    that is mostly censored is telling you the level sits where the session
    runs out, not that it reverts.
    """
    k, n = _rate(rows)
    lo, hi = stats.wilson_ci(k, n) if n else (None, None)
    sessions = {(r.get("symbol"), r.get("session")) for r in rows}
    outcomes = [r.get("outcome") for r in rows]

    out: dict[str, Any] = {
        "n": len(rows),
        "n_resolved": n,
        "n_sessions": len(sessions),
        "reversal_first": k,
        "reversal_rate": (k / n) if n else None,
        "reversal_ci_low": lo,
        "reversal_ci_high": hi,
        "continuation_rate": ((n - k) / n) if n else None,
        "n_continuation_same_bar": sum(1 for o in outcomes if o == "continuation_same_bar"),
        "n_ambiguous": sum(1 for o in outcomes if o == OUTCOME_AMBIGUOUS),
        "n_censored": sum(1 for o in outcomes if o == OUTCOME_CENSORED),
        "censored_rate": (
            (sum(1 for o in outcomes if o == OUTCOME_CENSORED) / len(rows)) if rows else None
        ),
        "thin": n < MIN_REPORTABLE_N,
        "median_minutes_to_prev": stats.describe([r.get("minutes_to_prev") for r in rows]).median,
        "median_minutes_to_next": stats.describe([r.get("minutes_to_next") for r in rows]).median,
    }
    for h in horizons:
        out.update(_summary_dict(f"mfe_{h}", [r.get(f"mfe_pts_{h}") for r in rows]))
        out.update(_summary_dict(f"mae_{h}", [r.get(f"mae_pts_{h}") for r in rows]))
        out.update(_summary_dict(f"ret_{h}", [r.get(f"ret_reversion_bp_{h}") for r in rows]))
    out.update(_summary_dict("mfe_eod", [r.get("mfe_pts_eod") for r in rows]))
    out.update(_summary_dict("mae_eod", [r.get("mae_pts_eod") for r in rows]))
    return out


def compare_to_baseline(
    baseline_rows: Sequence[Mapping[str, Any]],
    predicate: Callable[[Mapping[str, Any]], bool],
    *,
    iterations: int = 2000,
    seed: int = 20260908,
) -> dict[str, Any]:
    """Effect of a cohort against the unconditional baseline, session-clustered.

    ``baseline_rows`` is the WHOLE pool and ``predicate`` selects the cohort
    inside it.  Membership is recomputed rather than passed as a second list,
    so the comparison cannot silently degrade if a caller hands over rows that
    were copied, re-read from JSON, or reordered.

    The baseline is the unconditional pool INCLUDING the cohort's own rows —
    that is what ``session_block_bootstrap_diff`` measures, and it is the
    harder and more honest question ("does this beat the base rate?") than
    cohort-versus-complement.

    Reports BOTH a naive two-proportion test and the session-clustered
    bootstrap.  Keeping the naive number visible is deliberate: where the two
    disagree, the gap IS the clustering, and a reader who only ever sees the
    conservative number cannot tell how much of a headline was independence
    being assumed.
    """
    resolved = [r for r in baseline_rows if r.get("outcome") in RESOLVED_OUTCOMES]
    in_bucket = [bool(predicate(r)) for r in resolved]

    k_b = sum(1 for r in resolved if r.get("outcome") == OUTCOME_REVERSAL)
    n_b = len(resolved)
    k_a = sum(
        1 for r, flag in zip(resolved, in_bucket) if flag and r.get("outcome") == OUTCOME_REVERSAL
    )
    n_a = sum(1 for flag in in_bucket if flag)

    rate_a = (k_a / n_a) if n_a else None
    rate_b = (k_b / n_b) if n_b else None
    # The point estimate is computed here, not taken from the bootstrap, which
    # returns ``(ci_lo, ci_hi, p)`` and no point value.
    diff = (rate_a - rate_b) if (rate_a is not None and rate_b is not None) else None

    naive = (
        stats.compare_proportions(k_a, n_a, k_b - k_a, n_b - n_a) if n_a and (n_b - n_a) > 0 else {}
    )

    lo = hi = pval = None
    if any(in_bucket) and not all(in_bucket):
        values = [1.0 if r.get("outcome") == OUTCOME_REVERSAL else 0.0 for r in resolved]
        sessions = [(r.get("symbol"), r.get("session")) for r in resolved]
        lo, hi, pval = stats.session_block_bootstrap_diff(
            values, sessions, in_bucket, iterations=iterations, seed=seed
        )
    return {
        "rate": rate_a,
        "baseline_rate": rate_b,
        "clustered_diff": diff,
        "clustered_ci_low": lo,
        "clustered_ci_high": hi,
        "clustered_p": pval,
        "naive_diff": naive.get("difference"),
        "naive_p": naive.get("p_value"),
        "n": n_a,
        "n_baseline": n_b,
    }
