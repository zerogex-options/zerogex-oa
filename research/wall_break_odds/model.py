"""Fitting and — more importantly — honest evaluation.

The statistical primitives are reused from
``research.mm_attributed_gex.stats`` (IRLS logit, AUC, Brier, log-loss,
reliability bins, Benjamini-Hochberg) rather than re-implemented, so both
studies are graded by the same code and a change to the yardstick cannot make
one of them look better than the other by accident.

Three rules this module enforces, because a break-probability model is very
easy to fool yourself with:

1. **Nothing is reported in-sample.**  Every headline number comes from
   walk-forward folds whose train sets end before their test sets begin, and
   the split boundaries are snapped to session edges so no session ever
   contributes rows to both sides.  An overlapping session leaks the day's
   regime — the single strongest confounder here — straight into the fit.
2. **Standardisation is fit on train only.**  Centring on the full sample's
   mean is a small leak that reliably flatters a model on short samples.
3. **The baseline is the base rate.**  A model that cannot beat an
   intercept-only predictor out-of-sample on log-loss has found nothing, no
   matter what its coefficients' p-values say.  :func:`evaluate` reports the
   comparison as ``skill``, and the report leads with it.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping, Optional, Sequence

from research.mm_attributed_gex.stats import (
    auc,
    benjamini_hochberg,
    brier_score,
    calibration_bins,
    log_loss,
    logistic_regression,
)
from research.wall_break_odds.features import FEATURE_NAMES, substantive_features

__all__ = [
    "MIN_EVENTS_FOR_MODEL",
    "MIN_EVENTS_FOR_RATE",
    "Row",
    "wilson_interval",
    "base_rate",
    "univariate_screen",
    "session_walk_forward",
    "evaluate",
    "fit_full",
    "replication",
]

#: Below this many resolved events, no coefficient is reported at all. Twenty
#: features on eighty events is not a model, it is a memoriser; the report says
#: "not enough data" instead, which is a usable answer.
MIN_EVENTS_FOR_MODEL = 200
#: Below this, not even a break rate is quoted for a bucket.
MIN_EVENTS_FOR_RATE = 30


@dataclass
class Row:
    """One resolved event, ready to model."""

    session: date
    side: str
    broke: int
    features: dict[str, Optional[float]] = field(default_factory=dict)


def wilson_interval(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval for a proportion.

    Used rather than the normal approximation because the interesting buckets
    here are small and often near 0 or 1, exactly where the textbook interval
    produces bounds outside [0, 1] and quietly overstates precision.
    """
    if n <= 0:
        return (0.0, 1.0)
    p = k / n
    denom = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(max(p * (1 - p) / n + z * z / (4 * n * n), 0.0)) / denom
    return (max(centre - half, 0.0), min(centre + half, 1.0))


def base_rate(rows: Sequence[Row]) -> dict[str, Any]:
    """Break rate with a Wilson interval, overall and by side."""

    def one(subset: Sequence[Row]) -> dict[str, Any]:
        n = len(subset)
        k = sum(r.broke for r in subset)
        lo, hi = wilson_interval(k, n)
        return {
            "n": n,
            "breaks": k,
            "rate": (k / n) if n else None,
            "ci95": [lo, hi] if n else None,
            "reportable": n >= MIN_EVENTS_FOR_RATE,
        }

    return {
        "overall": one(rows),
        "call": one([r for r in rows if r.side == "call"]),
        "put": one([r for r in rows if r.side == "put"]),
    }


#: Bootstrap resamples for the session-clustered screen. 500 is enough to
#: resolve a p-value against a 5% threshold without making a 20-feature screen
#: over a multi-month dataset take minutes.
CLUSTER_BOOTSTRAP_N = 500


def _best_split(values: Sequence[float]) -> Optional[float]:
    """Threshold ``t`` splitting into ``>= t`` and ``< t`` as evenly as possible.

    A plain median split is wrong for the two shapes this dataset actually
    contains. A BINARY feature (``spot_above_flip``) whose majority value is 1
    has median 1, so ``> median`` is empty and the feature reads as "not enough
    coverage" when the data are perfectly adequate. A MOSTLY-ZERO feature
    (``wall_migration_toward_break`` — walls hold still most minutes) has median
    0, and ``> 0`` keeps only the rare migrations.

    ``n_hi(t)`` is monotone decreasing in ``t``, so the most balanced cut is
    found by binary search over the distinct values and checking the immediate
    neighbours of the crossing — O(n log n), dominated by the sort. Scanning
    every distinct value instead is O(distinct x n), which on a continuous
    column is quadratic; the bootstrap re-splits 500 times per feature, so that
    difference is the difference between a screen that returns and one that
    does not.
    """
    clean = sorted(v for v in values if v is not None and math.isfinite(v))
    n = len(clean)
    if n < 2:
        return None
    uniq = sorted(set(clean))
    if len(uniq) < 2:
        return None
    target = n / 2.0
    lo, hi = 0, len(uniq) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if n - bisect.bisect_left(clean, uniq[mid]) > target:
            lo = mid + 1
        else:
            hi = mid - 1
    best: Optional[float] = None
    best_balance = -1
    for idx in range(max(0, hi - 1), min(len(uniq), lo + 2)):
        t = uniq[idx]
        n_hi = n - bisect.bisect_left(clean, t)
        balance = min(n_hi, n - n_hi)
        if balance > best_balance:
            best_balance, best = balance, t
    return best


def _cluster_bootstrap_delta(
    pairs: Sequence[tuple[Any, float, int]],
    *,
    n_boot: int = CLUSTER_BOOTSTRAP_N,
    seed: int = 12345,
) -> tuple[Optional[float], Optional[float]]:
    """Above-median minus below-median break rate, with a SESSION-CLUSTERED p.

    Rows in this dataset are not independent: a session-level feature like wall
    strength is identical for every test that day, and those tests share the
    day's regime, so their outcomes are correlated too. A textbook two-
    proportion z-test treats them as independent and is measurably
    anti-conservative for it — on synthetic data with realistic clustering its
    false-positive rate at alpha=0.05 runs around 11%, not 5%.

    Resampling whole SESSIONS with replacement reproduces that correlation in
    the null distribution, so the resulting p-value is honest about how much
    independent information the sample really carries. The median is recomputed
    inside each resample rather than fixed from the full sample, so the
    uncertainty in the split point is priced in too.

    Measured false-positive rate at alpha=0.05, on synthetic null data with
    300 sessions and 1-3 events each, 200 trials per cell, both arms using the
    same balanced split so only the p-value differs:

        design                     naive z-test    this
        independent rows               0.045       0.035
        session-clustered rows         0.110       0.055

    The clustered row is the one that matters — it is what the real dataset
    looks like — and it is the one the naive test gets wrong, at better than
    twice the nominal rate. Reproduce with
    ``research/wall_break_odds/README.md`` -> "Checking the screen's
    calibration".
    """
    import random as _random

    by_session: dict[Any, list[tuple[float, int]]] = {}
    for session, value, broke in pairs:
        by_session.setdefault(session, []).append((value, broke))
    sessions = list(by_session)
    if len(sessions) < 5:
        return None, None

    def delta(sample: Sequence[Any]) -> Optional[float]:
        vals = [vb for s in sample for vb in by_session[s]]
        if len(vals) < 2 * MIN_EVENTS_FOR_RATE:
            return None
        split = _best_split([v for v, _ in vals])
        if split is None:
            return None
        hi = [b for v, b in vals if v >= split]
        lo = [b for v, b in vals if v < split]
        if not hi or not lo:
            return None
        return sum(hi) / len(hi) - sum(lo) / len(lo)

    observed = delta(sessions)
    if observed is None:
        return None, None
    rng = _random.Random(seed)
    draws: list[float] = []
    for _ in range(n_boot):
        resample = [sessions[rng.randrange(len(sessions))] for _ in range(len(sessions))]
        d = delta(resample)
        if d is not None:
            draws.append(d)
    if len(draws) < n_boot // 2:
        return observed, None
    # Two-sided: how often does the resampled effect cross zero? Centring on
    # the observed effect makes this the standard bootstrap hypothesis test.
    centred = [d - observed for d in draws]
    extreme = sum(1 for c in centred if abs(c) >= abs(observed))
    p = (extreme + 1) / (len(centred) + 1)
    return observed, min(max(p, 0.0), 1.0)


def univariate_screen(
    rows: Sequence[Row], feature_names: Sequence[str] = FEATURE_NAMES
) -> list[dict[str, Any]]:
    """Break rate above vs below each feature's median, session-clustered.

    A screen, not a result: these are marginal associations and they are
    correlated with each other. It exists to show which columns carry any
    signal at all before the multivariate fit, and to make it obvious when
    the answer is "none of them do".

    Two corrections are applied, and both are load-bearing. The p-value per
    feature comes from a SESSION-clustered bootstrap (see
    :func:`_cluster_bootstrap_delta`) because rows within a day are not
    independent. Benjamini-Hochberg then controls the false-discovery rate
    across the whole feature family, because screening twenty columns at
    alpha=0.05 yields one spurious hit per screen by construction.
    """
    out: list[dict[str, Any]] = []
    for name in feature_names:
        vals = [(r.features.get(name), r.broke) for r in rows]
        usable = [(float(v), b) for v, b in vals if v is not None and math.isfinite(float(v))]
        if len(usable) < 2 * MIN_EVENTS_FOR_RATE:
            out.append({"feature": name, "n": len(usable), "reportable": False})
            continue
        split = _best_split([v for v, _ in usable])
        if split is None:
            out.append({"feature": name, "n": len(usable), "reportable": False})
            continue
        hi = [b for v, b in usable if v >= split]
        lo = [b for v, b in usable if v < split]
        if len(hi) < MIN_EVENTS_FOR_RATE or len(lo) < MIN_EVENTS_FOR_RATE:
            out.append(
                {
                    "feature": name,
                    "n": len(usable),
                    "reportable": False,
                    "reason": "no split leaves both groups above the reporting floor",
                }
            )
            continue
        p_hi, p_lo = sum(hi) / len(hi), sum(lo) / len(lo)
        pairs = [
            (r.session, float(r.features[name]), r.broke)
            for r in rows
            if r.features.get(name) is not None and math.isfinite(float(r.features[name]))
        ]
        delta, p_value = _cluster_bootstrap_delta(pairs)
        if p_value is None:
            out.append({"feature": name, "n": len(usable), "reportable": False})
            continue
        out.append(
            {
                "feature": name,
                "n": len(usable),
                "n_sessions": len({s for s, _, _ in pairs}),
                "split_at": split,
                "rate_above": p_hi,
                "rate_below": p_lo,
                "delta": delta,
                "p_value": p_value,
                "reportable": True,
            }
        )
    tested = [o for o in out if o.get("reportable")]
    flags = benjamini_hochberg([o["p_value"] for o in tested]) if tested else []
    for o, keep in zip(tested, flags):
        o["significant_fdr_05"] = bool(keep)
    return out


def session_walk_forward(
    rows: Sequence[Row], *, n_folds: int = 5, min_train_frac: float = 0.4
) -> list[tuple[list[int], list[int]]]:
    """Expanding-window splits whose boundaries fall on session edges.

    ``stats.walk_forward_splits`` slices on row index, which for this dataset
    can cut a session in half and put the morning's tests in train and the
    afternoon's in test. Same day, same regime, same walls — that is a leak.
    Here the sessions are ordered, split, and rows mapped back.
    """
    sessions = sorted({r.session for r in rows})
    if len(sessions) < n_folds + 1:
        return []
    idx_by_session: dict[date, list[int]] = {}
    for i, r in enumerate(rows):
        idx_by_session.setdefault(r.session, []).append(i)

    min_train = max(int(len(sessions) * min_train_frac), 1)
    remaining = len(sessions) - min_train
    if remaining < n_folds:
        n_folds = max(remaining, 1)
    fold_size = max(remaining // n_folds, 1)

    splits: list[tuple[list[int], list[int]]] = []
    for fold in range(n_folds):
        train_end = min_train + fold * fold_size
        test_end = train_end + fold_size if fold < n_folds - 1 else len(sessions)
        if train_end >= len(sessions) or test_end <= train_end:
            break
        train_sessions = sessions[:train_end]
        test_sessions = sessions[train_end:test_end]
        train_idx = [i for s in train_sessions for i in idx_by_session.get(s, [])]
        test_idx = [i for s in test_sessions for i in idx_by_session.get(s, [])]
        if train_idx and test_idx:
            splits.append((train_idx, test_idx))
    return splits


def _usable_features(rows: Sequence[Row], names: Sequence[str], min_coverage: float) -> list[str]:
    """Keep only columns present on enough rows to be worth fitting."""
    keep: list[str] = []
    for name in names:
        present = sum(
            1
            for r in rows
            if r.features.get(name) is not None and math.isfinite(float(r.features[name]))
        )
        if present >= max(int(len(rows) * min_coverage), MIN_EVENTS_FOR_RATE):
            keep.append(name)
    return keep


def _complete(rows: Sequence[Row], names: Sequence[str]) -> list[int]:
    out = []
    for i, r in enumerate(rows):
        vals = [r.features.get(n) for n in names]
        if all(v is not None and math.isfinite(float(v)) for v in vals):
            out.append(i)
    return out


def _standardise(
    train: Sequence[Sequence[float]], apply_to: Sequence[Sequence[float]]
) -> tuple[list[list[float]], list[list[float]]]:
    """Z-score using TRAIN moments only, applied to both matrices."""
    n_cols = len(train[0]) if train else 0
    means, sds = [], []
    for j in range(n_cols):
        col = [row[j] for row in train]
        m = sum(col) / len(col)
        var = sum((x - m) ** 2 for x in col) / max(len(col) - 1, 1)
        means.append(m)
        sds.append(math.sqrt(var) if var > 0 else 1.0)

    def apply(mat: Sequence[Sequence[float]]) -> list[list[float]]:
        return [[(row[j] - means[j]) / sds[j] for j in range(n_cols)] for row in mat]

    return apply(train), apply(apply_to)


def _predict(coef: Sequence[float], x: Sequence[float]) -> float:
    z = coef[0] + sum(c * v for c, v in zip(coef[1:], x))
    z = max(min(z, 35.0), -35.0)
    return 1.0 / (1.0 + math.exp(-z))


def evaluate(
    rows: Sequence[Row],
    *,
    feature_names: Sequence[str] = FEATURE_NAMES,
    n_folds: int = 5,
    min_coverage: float = 0.6,
) -> dict[str, Any]:
    """Walk-forward out-of-sample evaluation against the base-rate baseline.

    Returns a dict carrying ``skill`` — the log-loss improvement over always
    predicting the training base rate. Negative skill means the model is worse
    than knowing nothing but the average, which is a legitimate and reportable
    finding.
    """
    if len(rows) < MIN_EVENTS_FOR_MODEL:
        return {
            "status": "insufficient_data",
            "n": len(rows),
            "required": MIN_EVENTS_FOR_MODEL,
        }
    names = _usable_features(rows, feature_names, min_coverage)
    if not names:
        return {"status": "no_usable_features", "n": len(rows)}
    keep = _complete(rows, names)
    fitted_rows = [rows[i] for i in keep]
    if len(fitted_rows) < MIN_EVENTS_FOR_MODEL:
        # Naming the bottleneck matters: with enough events but sparse
        # columns, the fix is dropping a feature rather than waiting months
        # for more sessions, and there is no way to tell which from a bare
        # count. Each entry is how many complete cases dropping that ONE
        # feature would recover.
        gains: list[dict[str, Any]] = []
        for name in names:
            without = [n for n in names if n != name]
            gained = len(_complete(rows, without)) - len(fitted_rows)
            if gained > 0:
                gains.append({"feature": name, "rows_gained_if_dropped": gained})
        gains.sort(key=lambda g: -g["rows_gained_if_dropped"])
        return {
            "status": "insufficient_complete_cases",
            "n": len(fitted_rows),
            "n_resolved": len(rows),
            "required": MIN_EVENTS_FOR_MODEL,
            "features": names,
            "bottleneck": gains[:5],
        }

    splits = session_walk_forward(fitted_rows, n_folds=n_folds)
    if not splits:
        return {"status": "insufficient_sessions", "n": len(fitted_rows), "features": names}

    oos_y: list[int] = []
    oos_p: list[float] = []
    oos_base: list[float] = []
    fold_reports: list[dict[str, Any]] = []
    for train_idx, test_idx in splits:
        y_tr = [fitted_rows[i].broke for i in train_idx]
        if len(set(y_tr)) < 2:
            continue
        X_tr = [[float(fitted_rows[i].features[n]) for n in names] for i in train_idx]
        X_te = [[float(fitted_rows[i].features[n]) for n in names] for i in test_idx]
        Z_tr, Z_te = _standardise(X_tr, X_te)
        fit = logistic_regression(y_tr, {n: [row[j] for row in Z_tr] for j, n in enumerate(names)})
        if fit is None or not fit.converged:
            continue
        prior = sum(y_tr) / len(y_tr)
        for i, z in zip(test_idx, Z_te):
            oos_y.append(fitted_rows[i].broke)
            oos_p.append(_predict(fit.coef, z))
            oos_base.append(prior)
        fold_reports.append(
            {
                "train_sessions": len({fitted_rows[i].session for i in train_idx}),
                "test_sessions": len({fitted_rows[i].session for i in test_idx}),
                "n_train": len(train_idx),
                "n_test": len(test_idx),
                "train_base_rate": prior,
            }
        )

    if not oos_y:
        return {"status": "no_converged_folds", "n": len(fitted_rows), "features": names}

    model_ll = log_loss(oos_y, oos_p)
    base_ll = log_loss(oos_y, oos_base)
    skill = (
        1.0 - (model_ll / base_ll) if model_ll is not None and base_ll not in (None, 0) else None
    )
    return {
        "status": "ok",
        "n": len(oos_y),
        "features": names,
        "folds": fold_reports,
        "oos": {
            "auc": auc(oos_y, oos_p),
            "brier_model": brier_score(oos_y, oos_p),
            "brier_baseline": brier_score(oos_y, oos_base),
            "log_loss_model": model_ll,
            "log_loss_baseline": base_ll,
            "skill": skill,
            "calibration": calibration_bins(oos_y, oos_p, n_bins=5),
        },
    }


def fit_full(
    rows: Sequence[Row],
    *,
    feature_names: Sequence[str] = FEATURE_NAMES,
    min_coverage: float = 0.6,
) -> Optional[dict[str, Any]]:
    """One in-sample fit, for COEFFICIENT DIRECTION only.

    Reported beside the walk-forward numbers and never instead of them: the
    signs and relative magnitudes are what answer "which inputs matter", while
    anything resembling accuracy must come from :func:`evaluate`.
    """
    if len(rows) < MIN_EVENTS_FOR_MODEL:
        return None
    names = _usable_features(rows, feature_names, min_coverage)
    keep = _complete(rows, names)
    if not names or len(keep) < MIN_EVENTS_FOR_MODEL:
        return None
    subset = [rows[i] for i in keep]
    X = [[float(r.features[n]) for n in names] for r in subset]
    Z, _ = _standardise(X, X)
    fit = logistic_regression(
        [r.broke for r in subset], {n: [row[j] for row in Z] for j, n in enumerate(names)}
    )
    if fit is None:
        return None
    d = fit.as_dict()
    d["standardised"] = True
    d["note"] = "in-sample; coefficient DIRECTION only, never a performance claim"
    return d


def replication(screens: Mapping[str, Sequence[Mapping[str, Any]]]) -> Optional[dict[str, Any]]:
    """Do two independent samples agree about which features matter?

    When the pooling check rejects, the screens cannot be combined — but the
    two samples can still be asked whether they tell the same story, and that
    question is more informative than either screen alone.

    A significance test asks "could this delta be noise?". Replication asks
    the harder version: "does it show up again, in data it has never seen?".
    Nineteen features screened twice give nineteen chances to agree; if the
    rank correlation is ~0 and signs agree at ~50%, the deltas are noise
    regardless of how large the biggest ones look.

    Note that mechanical features (time of day, minutes to close, test
    ordinal) will tend to agree in ANY two samples, because their relationship
    to the resolution window is structural rather than about markets. Agreement
    concentrated there is not evidence of a finding.
    """
    names = [k for k in screens]
    if len(names) != 2:
        return None
    a_map = {
        s["feature"]: s["delta"]
        for s in screens[names[0]]
        if s.get("reportable") and s.get("delta") is not None
    }
    b_map = {
        s["feature"]: s["delta"]
        for s in screens[names[1]]
        if s.get("reportable") and s.get("delta") is not None
    }
    shared = sorted(set(a_map) & set(b_map))
    if len(shared) < 5:
        return None
    a = [a_map[f] for f in shared]
    b = [b_map[f] for f in shared]

    def _rank(v: Sequence[float]) -> list[float]:
        order = sorted(range(len(v)), key=lambda i: v[i])
        out = [0.0] * len(v)
        for pos, i in enumerate(order):
            out[i] = pos + 1.0
        return out

    def _pearson(x: Sequence[float], y: Sequence[float]) -> float:
        mx, my = sum(x) / len(x), sum(y) / len(y)
        num = sum((p - mx) * (q - my) for p, q in zip(x, y))
        den = (sum((p - mx) ** 2 for p in x) * sum((q - my) ** 2 for q in y)) ** 0.5
        return num / den if den else 0.0

    nonzero = [(x, y) for x, y in zip(a, b) if x * y != 0]
    agree = sum(1 for x, y in nonzero if x * y > 0)

    # The same calculation over the SUBSTANTIVE columns only. Mechanical
    # features agree across any two samples because their link to the
    # resolution window is structural, so a pair whose agreement is entirely
    # the clock scores identically to one that agrees about gamma. This is
    # the number to read.
    sub = substantive_features(shared)
    sub_stats: dict[str, Any] = {"n_features": len(sub)}
    if len(sub) >= 5:
        sa = [a_map[f] for f in sub]
        sb = [b_map[f] for f in sub]
        sub_nonzero = [(x, y) for x, y in zip(sa, sb) if x * y != 0]
        sub_stats.update(
            {
                "spearman": _pearson(_rank(sa), _rank(sb)),
                "sign_agreement": (
                    sum(1 for x, y in sub_nonzero if x * y > 0) / len(sub_nonzero)
                    if sub_nonzero
                    else None
                ),
            }
        )
    return {
        "substantive": sub_stats,
        "symbols": names,
        "n_features": len(shared),
        "spearman": _pearson(_rank(a), _rank(b)),
        "pearson": _pearson(a, b),
        "sign_agreement": (agree / len(nonzero)) if nonzero else None,
        "n_signed": len(nonzero),
        "rows": sorted(
            ({"feature": f, names[0]: a_map[f], names[1]: b_map[f]} for f in shared),
            key=lambda r: -abs(r[names[0]]),
        ),
    }
