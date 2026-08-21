"""Statistics toolkit for the experiment.

Deliberately small, explicit and interpretable.  Requirement 11 asks for simple
models before complex ones, and the reason is not aesthetics: the question is
whether participant attribution carries *information*, and a black-box score
cannot answer that in a way anyone can act on.

Two choices here matter more than the rest.

**Overlapping windows are handled, not ignored.**  A 60-minute forward return
sampled every 5 minutes shares 55 minutes with its neighbour.  Ordinary
standard errors treat those as independent and overstate significance by
roughly ``sqrt(overlap)`` — enough to turn noise into a "result".  Every
regression here reports Newey-West HAC standard errors with a lag chosen from
the overlap, and the resampling routines use a *block* bootstrap that keeps
contiguous runs intact.

**Effect sizes travel with p-values.**  A t-statistic on 20,000 overlapping
minute observations will clear any threshold; whether the effect is worth
anything is a separate question, so ``describe``/``compare_means`` always
return Cohen's d and a confidence interval alongside.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence, Union

import numpy as np

#: Anything the toolkit will treat as a numeric column. NumPy arrays are the
#: common case (the backtest builds its design matrices with NumPy), plain
#: sequences the convenient one for tests.
NumericInput = Union[Sequence[float], "np.ndarray"]

try:
    from scipy import stats as _scipy_stats

    _HAVE_SCIPY = True
except Exception:  # pragma: no cover - scipy is a declared dependency
    _HAVE_SCIPY = False

#: Public capability flag. scipy is a declared dependency, so this is True in
#: any supported install; callers branch on it only to stay importable in a
#: stripped environment.
HAVE_SCIPY = _HAVE_SCIPY

__all__ = [
    "HAVE_SCIPY",
    "Summary",
    "MeanComparison",
    "ProportionComparison",
    "RegressionResult",
    "LogitResult",
    "describe",
    "compare_means",
    "compare_proportions",
    "bootstrap_ci",
    "block_bootstrap_diff",
    "permutation_test",
    "correlation",
    "ols_hac",
    "logistic_regression",
    "auc",
    "brier_score",
    "log_loss",
    "calibration_bins",
    "walk_forward_splits",
    "nested_model_test",
    "benjamini_hochberg",
]


def _clean(values: Iterable[float]) -> np.ndarray:
    arr = np.asarray(list(values), dtype=float)
    if arr.size == 0:
        return arr
    return arr[np.isfinite(arr)]


def _t_sf(t: float, df: float) -> float:
    """Two-sided p-value for a t statistic."""
    if df <= 0 or not math.isfinite(t):
        return float("nan")
    if _HAVE_SCIPY:
        return float(2.0 * _scipy_stats.t.sf(abs(t), df))
    # Normal approximation fallback; adequate for the df this study produces.
    return float(math.erfc(abs(t) / math.sqrt(2.0)))


def _norm_sf(z: float) -> float:
    if not math.isfinite(z):
        return float("nan")
    return float(math.erfc(abs(z) / math.sqrt(2.0)))


# ---------------------------------------------------------------------------
# Descriptive
# ---------------------------------------------------------------------------


@dataclass
class Summary:
    n: int
    mean: Optional[float] = None
    median: Optional[float] = None
    std: Optional[float] = None
    se: Optional[float] = None
    ci_low: Optional[float] = None
    ci_high: Optional[float] = None
    p25: Optional[float] = None
    p75: Optional[float] = None
    min: Optional[float] = None
    max: Optional[float] = None
    positive_rate: Optional[float] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "n": self.n,
            "mean": self.mean,
            "median": self.median,
            "std": self.std,
            "se": self.se,
            "ci95": [self.ci_low, self.ci_high],
            "p25": self.p25,
            "p75": self.p75,
            "min": self.min,
            "max": self.max,
            "positive_rate": self.positive_rate,
        }


def describe(values: Iterable[float], *, confidence: float = 0.95) -> Summary:
    """Mean/median/spread with a confidence interval on the mean."""
    arr = _clean(values)
    n = int(arr.size)
    if n == 0:
        return Summary(n=0)
    mean = float(arr.mean())
    if n == 1:
        return Summary(
            n=1,
            mean=mean,
            median=mean,
            std=0.0,
            se=0.0,
            min=mean,
            max=mean,
            positive_rate=float(mean > 0),
        )
    std = float(arr.std(ddof=1))
    se = std / math.sqrt(n)
    if _HAVE_SCIPY:
        crit = float(_scipy_stats.t.ppf(0.5 + confidence / 2.0, n - 1))
    else:  # pragma: no cover
        crit = 1.959963985
    return Summary(
        n=n,
        mean=mean,
        median=float(np.median(arr)),
        std=std,
        se=se,
        ci_low=mean - crit * se,
        ci_high=mean + crit * se,
        p25=float(np.percentile(arr, 25)),
        p75=float(np.percentile(arr, 75)),
        min=float(arr.min()),
        max=float(arr.max()),
        positive_rate=float((arr > 0).mean()),
    )


# ---------------------------------------------------------------------------
# Two-sample comparisons
# ---------------------------------------------------------------------------


@dataclass
class MeanComparison:
    n_a: int
    n_b: int
    mean_a: Optional[float] = None
    mean_b: Optional[float] = None
    diff: Optional[float] = None
    t_stat: Optional[float] = None
    df: Optional[float] = None
    p_value: Optional[float] = None
    cohens_d: Optional[float] = None
    hedges_g: Optional[float] = None
    ci_low: Optional[float] = None
    ci_high: Optional[float] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "n_a": self.n_a,
            "n_b": self.n_b,
            "mean_a": self.mean_a,
            "mean_b": self.mean_b,
            "diff": self.diff,
            "t_stat": self.t_stat,
            "df": self.df,
            "p_value": self.p_value,
            "cohens_d": self.cohens_d,
            "hedges_g": self.hedges_g,
            "diff_ci95": [self.ci_low, self.ci_high],
        }


def compare_means(a: Iterable[float], b: Iterable[float]) -> MeanComparison:
    """Welch's t-test (unequal variances) with effect sizes.

    Welch rather than Student because the two regimes being compared —
    long-gamma vs short-gamma sessions, say — differ in variance almost by
    construction, which is the case Student's pooled estimate handles worst.
    """
    x, y = _clean(a), _clean(b)
    na, nb = int(x.size), int(y.size)
    out = MeanComparison(n_a=na, n_b=nb)
    if na < 2 or nb < 2:
        return out
    ma, mb = float(x.mean()), float(y.mean())
    va, vb = float(x.var(ddof=1)), float(y.var(ddof=1))
    out.mean_a, out.mean_b, out.diff = ma, mb, ma - mb
    se2 = va / na + vb / nb
    if se2 <= 0:
        return out
    se = math.sqrt(se2)
    out.t_stat = (ma - mb) / se
    denom = (va / na) ** 2 / (na - 1) + (vb / nb) ** 2 / (nb - 1)
    out.df = se2**2 / denom if denom > 0 else float(na + nb - 2)
    out.p_value = _t_sf(out.t_stat, out.df)
    pooled_sd = math.sqrt(((na - 1) * va + (nb - 1) * vb) / (na + nb - 2))
    if pooled_sd > 0:
        out.cohens_d = (ma - mb) / pooled_sd
        correction = 1.0 - 3.0 / (4.0 * (na + nb) - 9.0)
        out.hedges_g = out.cohens_d * correction
    crit = (
        float(_scipy_stats.t.ppf(0.975, out.df)) if _HAVE_SCIPY else 1.959963985
    )  # pragma: no branch
    out.ci_low = out.diff - crit * se
    out.ci_high = out.diff + crit * se
    return out


@dataclass
class ProportionComparison:
    n_a: int
    n_b: int
    rate_a: Optional[float] = None
    rate_b: Optional[float] = None
    diff: Optional[float] = None
    z_stat: Optional[float] = None
    p_value: Optional[float] = None
    ci_low: Optional[float] = None
    ci_high: Optional[float] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "n_a": self.n_a,
            "n_b": self.n_b,
            "rate_a": self.rate_a,
            "rate_b": self.rate_b,
            "diff": self.diff,
            "z_stat": self.z_stat,
            "p_value": self.p_value,
            "diff_ci95": [self.ci_low, self.ci_high],
        }


def compare_proportions(k_a: int, n_a: int, k_b: int, n_b: int) -> ProportionComparison:
    """Two-proportion z-test with a Wald interval on the difference."""
    out = ProportionComparison(n_a=n_a, n_b=n_b)
    if n_a <= 0 or n_b <= 0:
        return out
    pa, pb = k_a / n_a, k_b / n_b
    out.rate_a, out.rate_b, out.diff = pa, pb, pa - pb
    pooled = (k_a + k_b) / (n_a + n_b)
    se_pool = math.sqrt(pooled * (1 - pooled) * (1 / n_a + 1 / n_b))
    if se_pool > 0:
        out.z_stat = (pa - pb) / se_pool
        out.p_value = _norm_sf(out.z_stat)
    se_wald = math.sqrt(pa * (1 - pa) / n_a + pb * (1 - pb) / n_b)
    out.ci_low = out.diff - 1.959963985 * se_wald
    out.ci_high = out.diff + 1.959963985 * se_wald
    return out


# ---------------------------------------------------------------------------
# Resampling
# ---------------------------------------------------------------------------


def bootstrap_ci(
    values: Iterable[float],
    statistic: Callable[[np.ndarray], float] = lambda a: float(a.mean()),
    *,
    n_boot: int = 2000,
    confidence: float = 0.95,
    block_size: int = 1,
    seed: int = 20260821,
) -> dict[str, Any]:
    """Percentile bootstrap CI, optionally with contiguous blocks.

    ``block_size > 1`` resamples contiguous runs instead of individual
    observations, which is what preserves the serial dependence that
    overlapping forward windows create.  Set it to roughly the overlap length.
    """
    arr = _clean(values)
    n = int(arr.size)
    if n == 0:
        return {"n": 0, "point": None, "ci_low": None, "ci_high": None, "n_boot": 0}
    rng = np.random.default_rng(seed)
    point = float(statistic(arr))
    block = max(1, int(block_size))
    if block >= n:
        block = 1
    n_blocks = int(math.ceil(n / block))
    samples = np.empty(n_boot, dtype=float)
    if block == 1:
        idx = rng.integers(0, n, size=(n_boot, n))
        for i in range(n_boot):
            samples[i] = statistic(arr[idx[i]])
    else:
        starts = rng.integers(0, n - block + 1, size=(n_boot, n_blocks))
        offsets = np.arange(block)
        for i in range(n_boot):
            pick = (starts[i][:, None] + offsets[None, :]).ravel()[:n]
            samples[i] = statistic(arr[pick])
    alpha = (1.0 - confidence) / 2.0
    return {
        "n": n,
        "point": point,
        "ci_low": float(np.percentile(samples, 100 * alpha)),
        "ci_high": float(np.percentile(samples, 100 * (1 - alpha))),
        "n_boot": int(n_boot),
        "block_size": block,
    }


def block_bootstrap_diff(
    a: Iterable[float],
    b: Iterable[float],
    *,
    n_boot: int = 2000,
    block_size: int = 1,
    seed: int = 20260821,
) -> dict[str, Any]:
    """Bootstrap CI and two-sided p for ``mean(a) − mean(b)``.

    The p-value inverts the interval: the share of resamples on the opposite
    side of zero from the point estimate, doubled.  Reported alongside the
    interval rather than instead of it.
    """
    x, y = _clean(a), _clean(b)
    if x.size < 2 or y.size < 2:
        return {"n_a": int(x.size), "n_b": int(y.size), "diff": None}
    rng = np.random.default_rng(seed)
    diff = float(x.mean() - y.mean())

    def _resample(arr: np.ndarray) -> np.ndarray:
        n = arr.size
        block = max(1, min(int(block_size), n))
        if block == 1:
            return arr[rng.integers(0, n, size=n)]
        n_blocks = int(math.ceil(n / block))
        starts = rng.integers(0, n - block + 1, size=n_blocks)
        pick = (starts[:, None] + np.arange(block)[None, :]).ravel()[:n]
        return arr[pick]

    diffs = np.empty(n_boot, dtype=float)
    for i in range(n_boot):
        diffs[i] = _resample(x).mean() - _resample(y).mean()
    centered = diffs - diffs.mean()
    p = float(2.0 * min((centered >= abs(diff)).mean(), (centered <= -abs(diff)).mean()))
    return {
        "n_a": int(x.size),
        "n_b": int(y.size),
        "diff": diff,
        "ci_low": float(np.percentile(diffs, 2.5)),
        "ci_high": float(np.percentile(diffs, 97.5)),
        "p_value": min(1.0, p),
        "n_boot": int(n_boot),
        "block_size": int(block_size),
    }


def permutation_test(
    a: Iterable[float],
    b: Iterable[float],
    *,
    n_perm: int = 5000,
    seed: int = 20260821,
) -> dict[str, Any]:
    """Monte-Carlo permutation test on the difference in means.

    Assumption-free about the distributions; the exchangeability it does assume
    is the null being tested.
    """
    x, y = _clean(a), _clean(b)
    if x.size < 2 or y.size < 2:
        return {"n_a": int(x.size), "n_b": int(y.size), "p_value": None}
    rng = np.random.default_rng(seed)
    observed = float(x.mean() - y.mean())
    pool = np.concatenate([x, y])
    na = x.size
    count = 0
    for _ in range(n_perm):
        rng.shuffle(pool)
        if abs(pool[:na].mean() - pool[na:].mean()) >= abs(observed):
            count += 1
    return {
        "n_a": int(x.size),
        "n_b": int(y.size),
        "observed_diff": observed,
        "p_value": (count + 1) / (n_perm + 1),
        "n_perm": int(n_perm),
    }


def correlation(x: Iterable[float], y: Iterable[float]) -> dict[str, Any]:
    """Pearson and Spearman with p-values, on the pairwise-complete rows."""
    xa = np.asarray(list(x), dtype=float)
    ya = np.asarray(list(y), dtype=float)
    n = min(xa.size, ya.size)
    xa, ya = xa[:n], ya[:n]
    mask = np.isfinite(xa) & np.isfinite(ya)
    xa, ya = xa[mask], ya[mask]
    if xa.size < 3:
        return {"n": int(xa.size), "pearson": None, "spearman": None}
    out: dict[str, Any] = {"n": int(xa.size)}
    if _HAVE_SCIPY:
        pr = _scipy_stats.pearsonr(xa, ya)
        sr = _scipy_stats.spearmanr(xa, ya)
        out["pearson"] = float(pr[0])
        out["pearson_p"] = float(pr[1])
        out["spearman"] = float(sr[0])
        out["spearman_p"] = float(sr[1])
    else:  # pragma: no cover
        out["pearson"] = float(np.corrcoef(xa, ya)[0, 1])
        rx = np.argsort(np.argsort(xa)).astype(float)
        ry = np.argsort(np.argsort(ya)).astype(float)
        out["spearman"] = float(np.corrcoef(rx, ry)[0, 1])
    return out


# ---------------------------------------------------------------------------
# Regression
# ---------------------------------------------------------------------------


@dataclass
class RegressionResult:
    names: list[str]
    coef: list[float]
    se: list[float]
    t_stat: list[float]
    p_value: list[float]
    n: int
    k: int
    r_squared: float
    adj_r_squared: float
    hac_lags: int
    residual_std: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "n": self.n,
            "k": self.k,
            "r_squared": self.r_squared,
            "adj_r_squared": self.adj_r_squared,
            "hac_lags": self.hac_lags,
            "residual_std": self.residual_std,
            "terms": [
                {
                    "name": name,
                    "coef": c,
                    "se": s,
                    "t": t,
                    "p": p,
                }
                for name, c, s, t, p in zip(
                    self.names, self.coef, self.se, self.t_stat, self.p_value
                )
            ],
        }


def ols_hac(
    y: NumericInput,
    X: Mapping[str, NumericInput],
    *,
    hac_lags: Optional[int] = None,
    add_intercept: bool = True,
) -> Optional[RegressionResult]:
    """OLS with Newey-West (Bartlett) HAC standard errors.

    ``hac_lags=None`` uses the common ``floor(4·(n/100)^(2/9))`` rule.  When
    the dependent variable is an overlapping forward return, pass the overlap
    length instead — that is the dependence that actually needs absorbing, and
    it is usually larger than the automatic rule suggests.
    """
    yv = np.asarray(list(y), dtype=float)
    cols = {k: np.asarray(list(v), dtype=float) for k, v in X.items()}
    if not cols:
        return None
    n_rows = min([yv.size] + [c.size for c in cols.values()])
    yv = yv[:n_rows]
    cols = {k: v[:n_rows] for k, v in cols.items()}

    mask = np.isfinite(yv)
    for v in cols.values():
        mask &= np.isfinite(v)
    yv = yv[mask]
    design = [cols[k][mask] for k in cols]
    names = list(cols.keys())
    if add_intercept:
        design.insert(0, np.ones_like(yv))
        names.insert(0, "intercept")
    if yv.size == 0:
        return None
    Xm = np.column_stack(design)
    n, k = Xm.shape
    if n <= k + 1:
        return None

    XtX = Xm.T @ Xm
    try:
        XtX_inv = np.linalg.pinv(XtX)
    except np.linalg.LinAlgError:  # pragma: no cover
        return None
    beta = XtX_inv @ (Xm.T @ yv)
    resid = yv - Xm @ beta

    lags = hac_lags
    if lags is None:
        lags = int(math.floor(4.0 * (n / 100.0) ** (2.0 / 9.0)))
    lags = max(0, min(int(lags), n - 2))

    u = resid[:, None] * Xm
    S = u.T @ u
    for lag in range(1, lags + 1):
        weight = 1.0 - lag / (lags + 1.0)
        G = u[lag:].T @ u[:-lag]
        S += weight * (G + G.T)
    V = XtX_inv @ S @ XtX_inv
    se = np.sqrt(np.clip(np.diag(V), 0.0, None))

    with np.errstate(divide="ignore", invalid="ignore"):
        t_stat = np.where(se > 0, beta / se, np.nan)
    df = max(1, n - k)
    p_values = [_t_sf(float(t), df) if math.isfinite(t) else float("nan") for t in t_stat]

    ss_res = float(resid @ resid)
    ss_tot = float(((yv - yv.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    adj = 1.0 - (1.0 - r2) * (n - 1) / max(1, n - k) if ss_tot > 0 else 0.0

    return RegressionResult(
        names=names,
        coef=[float(b) for b in beta],
        se=[float(s) for s in se],
        t_stat=[float(t) for t in t_stat],
        p_value=[float(p) for p in p_values],
        n=n,
        k=k,
        r_squared=r2,
        adj_r_squared=adj,
        hac_lags=lags,
        residual_std=float(math.sqrt(ss_res / max(1, n - k))),
    )


def nested_model_test(
    y: NumericInput,
    baseline: Mapping[str, NumericInput],
    enhanced: Mapping[str, NumericInput],
    *,
    hac_lags: Optional[int] = None,
) -> dict[str, Any]:
    """Does adding the MM variables buy anything over the baseline?

    Fits both models on the *same rows* and reports the change in adjusted R²
    plus an F-test on the added terms.  ``enhanced`` must be a superset of
    ``baseline`` — this is the incremental-value question of requirement 11,
    not a horse race between two unrelated specifications.

    The F statistic uses homoskedastic sums of squares; the HAC-corrected
    per-coefficient t-statistics in ``enhanced_model.terms`` are the more
    trustworthy read when the forward windows overlap, and both are reported so
    a disagreement between them is visible.
    """
    missing = [k for k in baseline if k not in enhanced]
    if missing:
        raise ValueError(f"enhanced model must contain every baseline term; missing {missing}")
    added = [k for k in enhanced if k not in baseline]
    if not added:
        raise ValueError("enhanced model adds no terms")

    base = ols_hac(y, baseline, hac_lags=hac_lags)
    full = ols_hac(y, enhanced, hac_lags=hac_lags)
    if base is None or full is None:
        return {"ok": False, "reason": "insufficient data"}
    if base.n != full.n:
        return {
            "ok": False,
            "reason": f"models fit on different row counts ({base.n} vs {full.n}); "
            "align the inputs before comparing",
        }

    n = full.n
    ss_base = base.residual_std**2 * max(1, n - base.k)
    ss_full = full.residual_std**2 * max(1, n - full.k)
    q = full.k - base.k
    df_denom = max(1, n - full.k)
    f_stat = None
    p_value = None
    if q > 0 and ss_full > 0:
        f_stat = ((ss_base - ss_full) / q) / (ss_full / df_denom)
        if _HAVE_SCIPY and f_stat is not None and math.isfinite(f_stat):
            p_value = float(_scipy_stats.f.sf(max(0.0, f_stat), q, df_denom))
    return {
        "ok": True,
        "n": n,
        "added_terms": added,
        "baseline_r2": base.r_squared,
        "enhanced_r2": full.r_squared,
        "baseline_adj_r2": base.adj_r_squared,
        "enhanced_adj_r2": full.adj_r_squared,
        "delta_adj_r2": full.adj_r_squared - base.adj_r_squared,
        "f_stat": f_stat,
        "f_p_value": p_value,
        "baseline_model": base.as_dict(),
        "enhanced_model": full.as_dict(),
    }


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


@dataclass
class LogitResult:
    names: list[str]
    coef: list[float]
    se: list[float]
    z_stat: list[float]
    p_value: list[float]
    n: int
    log_likelihood: float
    null_log_likelihood: float
    mcfadden_r2: float
    converged: bool
    fitted: np.ndarray = field(repr=False, default_factory=lambda: np.array([]))

    def as_dict(self) -> dict[str, Any]:
        return {
            "n": self.n,
            "log_likelihood": self.log_likelihood,
            "mcfadden_r2": self.mcfadden_r2,
            "converged": self.converged,
            "terms": [
                {"name": nm, "coef": c, "se": s, "z": z, "p": p}
                for nm, c, s, z, p in zip(self.names, self.coef, self.se, self.z_stat, self.p_value)
            ],
        }


def logistic_regression(
    y: NumericInput,
    X: Mapping[str, NumericInput],
    *,
    max_iter: int = 100,
    tol: float = 1e-8,
    ridge: float = 1e-6,
) -> Optional[LogitResult]:
    """Binary logit by IRLS — interpretable coefficients, no ML dependency.

    A tiny ridge term keeps the Hessian invertible when a predictor separates
    the outcome perfectly (which happens with regime dummies on short samples);
    without it the fit diverges and reports spurious certainty.
    """
    yv = np.asarray(list(y), dtype=float)
    cols = {k: np.asarray(list(v), dtype=float) for k, v in X.items()}
    if not cols:
        return None
    n_rows = min([yv.size] + [c.size for c in cols.values()])
    yv = yv[:n_rows]
    cols = {k: v[:n_rows] for k, v in cols.items()}
    mask = np.isfinite(yv)
    for v in cols.values():
        mask &= np.isfinite(v)
    yv = yv[mask]
    names = ["intercept"] + list(cols.keys())
    Xm = np.column_stack([np.ones(yv.size)] + [cols[k][mask] for k in cols])
    n, k = Xm.shape
    if n <= k + 1 or yv.size == 0:
        return None
    uniq = np.unique(yv)
    if uniq.size < 2:
        return None

    beta = np.zeros(k)
    converged = False
    W = np.ones(n)
    for _ in range(max_iter):
        eta = np.clip(Xm @ beta, -35.0, 35.0)
        mu = 1.0 / (1.0 + np.exp(-eta))
        W = np.clip(mu * (1.0 - mu), 1e-10, None)
        z = eta + (yv - mu) / W
        XtW = Xm.T * W
        H = XtW @ Xm + ridge * np.eye(k)
        try:
            new_beta = np.linalg.solve(H, XtW @ z)
        except np.linalg.LinAlgError:  # pragma: no cover
            return None
        if np.max(np.abs(new_beta - beta)) < tol:
            beta = new_beta
            converged = True
            break
        beta = new_beta

    eta = np.clip(Xm @ beta, -35.0, 35.0)
    mu = 1.0 / (1.0 + np.exp(-eta))
    eps = 1e-12
    ll = float(np.sum(yv * np.log(mu + eps) + (1 - yv) * np.log(1 - mu + eps)))
    p_bar = float(yv.mean())
    ll0 = float(n * (p_bar * math.log(p_bar + eps) + (1 - p_bar) * math.log(1 - p_bar + eps)))

    W = np.clip(mu * (1.0 - mu), 1e-10, None)
    H = (Xm.T * W) @ Xm + ridge * np.eye(k)
    cov = np.linalg.pinv(H)
    se = np.sqrt(np.clip(np.diag(cov), 0.0, None))
    with np.errstate(divide="ignore", invalid="ignore"):
        zstat = np.where(se > 0, beta / se, np.nan)
    return LogitResult(
        names=names,
        coef=[float(b) for b in beta],
        se=[float(s) for s in se],
        z_stat=[float(t) for t in zstat],
        p_value=[_norm_sf(float(t)) if math.isfinite(t) else float("nan") for t in zstat],
        n=n,
        log_likelihood=ll,
        null_log_likelihood=ll0,
        mcfadden_r2=(1.0 - ll / ll0) if ll0 != 0 else 0.0,
        converged=converged,
        fitted=mu,
    )


def auc(y_true: NumericInput, scores: NumericInput) -> Optional[float]:
    """Rank-based ROC AUC (Mann-Whitney), ties handled by average ranks."""
    yv = np.asarray(list(y_true), dtype=float)
    sv = np.asarray(list(scores), dtype=float)
    n = min(yv.size, sv.size)
    yv, sv = yv[:n], sv[:n]
    mask = np.isfinite(yv) & np.isfinite(sv)
    yv, sv = yv[mask], sv[mask]
    n_pos = float((yv == 1).sum())
    n_neg = float((yv == 0).sum())
    if n_pos == 0 or n_neg == 0:
        return None
    order = np.argsort(sv, kind="mergesort")
    ranks = np.empty(sv.size, dtype=float)
    sorted_scores = sv[order]
    i = 0
    while i < sorted_scores.size:
        j = i
        while j + 1 < sorted_scores.size and sorted_scores[j + 1] == sorted_scores[i]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        ranks[order[i : j + 1]] = avg
        i = j + 1
    sum_pos = float(ranks[yv == 1].sum())
    return (sum_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


def brier_score(y_true: NumericInput, probs: NumericInput) -> Optional[float]:
    yv = np.asarray(list(y_true), dtype=float)
    pv = np.asarray(list(probs), dtype=float)
    n = min(yv.size, pv.size)
    yv, pv = yv[:n], pv[:n]
    mask = np.isfinite(yv) & np.isfinite(pv)
    if not mask.any():
        return None
    return float(np.mean((pv[mask] - yv[mask]) ** 2))


def log_loss(y_true: NumericInput, probs: NumericInput) -> Optional[float]:
    yv = np.asarray(list(y_true), dtype=float)
    pv = np.clip(np.asarray(list(probs), dtype=float), 1e-12, 1 - 1e-12)
    n = min(yv.size, pv.size)
    yv, pv = yv[:n], pv[:n]
    mask = np.isfinite(yv) & np.isfinite(pv)
    if not mask.any():
        return None
    yv, pv = yv[mask], pv[mask]
    return float(-np.mean(yv * np.log(pv) + (1 - yv) * np.log(1 - pv)))


def calibration_bins(
    y_true: NumericInput, probs: NumericInput, *, n_bins: int = 10
) -> list[dict[str, Any]]:
    """Reliability table: predicted vs realized rate per probability bin."""
    yv = np.asarray(list(y_true), dtype=float)
    pv = np.asarray(list(probs), dtype=float)
    n = min(yv.size, pv.size)
    yv, pv = yv[:n], pv[:n]
    mask = np.isfinite(yv) & np.isfinite(pv)
    yv, pv = yv[mask], pv[mask]
    if yv.size == 0:
        return []
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    out: list[dict[str, Any]] = []
    for i in range(n_bins):
        lo, hi = edges[i], edges[i + 1]
        sel = (pv >= lo) & (pv < hi if i < n_bins - 1 else pv <= hi)
        count = int(sel.sum())
        if count == 0:
            continue
        out.append(
            {
                "bin_low": float(lo),
                "bin_high": float(hi),
                "n": count,
                "predicted": float(pv[sel].mean()),
                "observed": float(yv[sel].mean()),
            }
        )
    return out


# ---------------------------------------------------------------------------
# Validation splits
# ---------------------------------------------------------------------------


def walk_forward_splits(
    n: int, *, n_folds: int = 5, min_train: int = 100, embargo: int = 0
) -> list[tuple[range, range]]:
    """Expanding-window walk-forward splits, oldest data always in train.

    ``embargo`` drops observations between train and test.  With overlapping
    forward windows the last few training rows share their outcome window with
    the first test rows; without the embargo that is a direct leak of the test
    period into the fit.  Set it to the forward horizon in observations.
    """
    if n <= min_train or n_folds < 1:
        return []
    test_size = (n - min_train) // n_folds
    if test_size <= 0:
        return []
    splits: list[tuple[range, range]] = []
    for fold in range(n_folds):
        train_end = min_train + fold * test_size
        test_start = train_end + embargo
        test_end = test_start + test_size if fold < n_folds - 1 else n
        if test_start >= n or test_end <= test_start:
            break
        splits.append((range(0, train_end), range(test_start, min(test_end, n))))
    return splits


def benjamini_hochberg(p_values: Sequence[float], alpha: float = 0.05) -> list[bool]:
    """BH false-discovery-rate control over a family of tests.

    This study runs dozens of tests across horizons, regimes and universes.
    Without an FDR correction, roughly one in twenty "findings" at α=0.05 is
    noise by construction — which for a go/no-go decision is the difference
    between a real result and an artifact of how many questions were asked.
    """
    finite = [(i, p) for i, p in enumerate(p_values) if p is not None and math.isfinite(p)]
    out = [False] * len(p_values)
    if not finite:
        return out
    finite.sort(key=lambda kv: kv[1])
    m = len(finite)
    threshold_rank = 0
    for rank, (_, p) in enumerate(finite, start=1):
        if p <= alpha * rank / m:
            threshold_rank = rank
    for rank, (idx, _) in enumerate(finite, start=1):
        if rank <= threshold_rank:
            out[idx] = True
    return out
