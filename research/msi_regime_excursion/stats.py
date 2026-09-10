"""Statistics for the excursion study. Stdlib only.

Two things about this data drive every choice here.

**The observations are not independent.** Readings land about once a minute and
the forward windows overlap, so a 30-minute measure at 10:00 shares 29 of its
30 minutes with the one at 10:01. A textbook t-test over 100k such rows will
report p < 1e-50 for a difference of no practical size, because it believes it
has 100k independent samples when it has closer to one per session. So the
headline test is a **session-level block bootstrap**: sessions are resampled
whole, which preserves the within-session correlation instead of assuming it
away. The parametric p-value is computed too, and reported beside it, purely so
the gap between the two is visible.

**The distributions are skewed and heavy-tailed.** Excursion is a non-negative
quantity with a long right tail. Means are reported because the copy is a claim
about typical travel, but the effect size is **Cliff's delta**, which is
rank-based and needs no distributional assumption, and medians are reported
alongside means.

Nothing here is novel; it is the standard kit, implemented without numpy so the
study runs on a production host with no extra packages.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

__all__ = [
    "Summary",
    "describe",
    "Comparison",
    "compare",
    "cliffs_delta",
    "mann_whitney_u",
    "session_block_bootstrap_diff",
    "wilson_ci",
    "compare_proportions",
    "benjamini_hochberg",
]


# ---------------------------------------------------------------------------
# Distributions
# ---------------------------------------------------------------------------

def _norm_sf(z: float) -> float:
    """Upper-tail normal survival function."""
    return 0.5 * math.erfc(z / math.sqrt(2.0))


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the incomplete beta function (Lentz's method)."""
    tiny = 1e-30
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < tiny:
        d = tiny
    d = 1.0 / d
    h = d
    for m in range(1, 201):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + aa / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + aa / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 3e-12:
            break
    return h


def _betai(a: float, b: float, x: float) -> float:
    """Regularized incomplete beta function I_x(a, b)."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    lbeta = (
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
        + a * math.log(x) + b * math.log1p(-x)
    )
    front = math.exp(lbeta)
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x) / a
    return 1.0 - math.exp(
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
        + b * math.log1p(-x) + a * math.log(x)
    ) * _betacf(b, a, 1.0 - x) / b


def _t_sf_two_sided(t: float, df: float) -> float:
    """Two-sided p-value for a t statistic."""
    if df <= 0 or not math.isfinite(t):
        return float("nan")
    return _betai(0.5 * df, 0.5, df / (df + t * t))


# ---------------------------------------------------------------------------
# Descriptives
# ---------------------------------------------------------------------------

def _clean(values: Iterable[Optional[float]]) -> list[float]:
    out: list[float] = []
    for v in values:
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            out.append(f)
    return out


def _quantile(sorted_vals: Sequence[float], q: float) -> Optional[float]:
    """Linear-interpolation quantile on an already-sorted sequence."""
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return sorted_vals[0]
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


@dataclass
class Summary:
    n: int
    mean: Optional[float]
    sd: Optional[float]
    median: Optional[float]
    p25: Optional[float]
    p75: Optional[float]
    p90: Optional[float]

    def as_dict(self) -> dict:
        return {
            "n": self.n, "mean": self.mean, "sd": self.sd, "median": self.median,
            "p25": self.p25, "p75": self.p75, "p90": self.p90,
        }


def describe(values: Iterable[Optional[float]]) -> Summary:
    vals = _clean(values)
    n = len(vals)
    if n == 0:
        return Summary(0, None, None, None, None, None, None)
    mean = sum(vals) / n
    if n > 1:
        var = sum((v - mean) ** 2 for v in vals) / (n - 1)
        sd = math.sqrt(var)
    else:
        sd = None
    s = sorted(vals)
    return Summary(
        n=n, mean=mean, sd=sd, median=_quantile(s, 0.5),
        p25=_quantile(s, 0.25), p75=_quantile(s, 0.75), p90=_quantile(s, 0.90),
    )


# ---------------------------------------------------------------------------
# Rank statistics
# ---------------------------------------------------------------------------

def _rank_sum_and_ties(a: Sequence[float], b: Sequence[float]) -> tuple[float, float, int, int]:
    """Mid-rank sum for ``a`` within the pooled sample, plus a tie correction."""
    n_a, n_b = len(a), len(b)
    pooled = sorted(
        [(v, 0) for v in a] + [(v, 1) for v in b], key=lambda t: t[0]
    )
    rank_sum_a = 0.0
    tie_term = 0.0
    i = 0
    total = n_a + n_b
    while i < total:
        j = i
        while j + 1 < total and pooled[j + 1][0] == pooled[i][0]:
            j += 1
        # Ranks i+1 .. j+1 are tied; assign each the mid-rank.
        mid = (i + 1 + j + 1) / 2.0
        group = j - i + 1
        for k in range(i, j + 1):
            if pooled[k][1] == 0:
                rank_sum_a += mid
        if group > 1:
            tie_term += group ** 3 - group
        i = j + 1
    return rank_sum_a, tie_term, n_a, n_b


def mann_whitney_u(
    a: Sequence[float], b: Sequence[float]
) -> tuple[Optional[float], Optional[float]]:
    """Return ``(U_a, two-sided p)`` under the normal approximation with ties.

    ``U_a`` counts (pairs where a > b) + 0.5 * (ties), so ``U_a / (n_a*n_b)``
    is P(a > b) + 0.5 P(a == b).
    """
    n_a, n_b = len(a), len(b)
    if n_a == 0 or n_b == 0:
        return None, None
    rank_sum_a, tie_term, _, _ = _rank_sum_and_ties(a, b)
    u_a = rank_sum_a - n_a * (n_a + 1) / 2.0
    n = n_a + n_b
    mu = n_a * n_b / 2.0
    var = (n_a * n_b / 12.0) * ((n + 1) - tie_term / (n * (n - 1))) if n > 1 else 0.0
    if var <= 0:
        return u_a, None
    # Continuity correction toward the mean.
    z_cc = (abs(u_a - mu) - 0.5) / math.sqrt(var)
    p = 2.0 * _norm_sf(max(0.0, z_cc))
    return u_a, min(1.0, p)


def cliffs_delta(a: Sequence[float], b: Sequence[float]) -> Optional[float]:
    """Cliff's delta: P(a > b) - P(a < b), in [-1, 1].

    Computed from the Mann-Whitney U rather than the naive O(n*m) double loop,
    so a hundred thousand rows is still fast. Conventional (Romano et al.)
    magnitude labels: |d| < 0.147 negligible, < 0.33 small, < 0.474 medium,
    else large.
    """
    n_a, n_b = len(a), len(b)
    if n_a == 0 or n_b == 0:
        return None
    rank_sum_a, _, _, _ = _rank_sum_and_ties(a, b)
    u_a = rank_sum_a - n_a * (n_a + 1) / 2.0
    return 2.0 * u_a / (n_a * n_b) - 1.0


def delta_magnitude(delta: Optional[float]) -> str:
    if delta is None:
        return "n/a"
    d = abs(delta)
    if d < 0.147:
        return "negligible"
    if d < 0.33:
        return "small"
    if d < 0.474:
        return "medium"
    return "large"


# ---------------------------------------------------------------------------
# Comparison against a baseline
# ---------------------------------------------------------------------------

@dataclass
class Comparison:
    """One conditional bucket scored against the unconditional base rate."""

    n: int
    n_base: int
    mean: Optional[float]
    mean_base: Optional[float]
    diff: Optional[float]              # mean - mean_base
    ratio: Optional[float]             # mean / mean_base
    median: Optional[float]
    median_base: Optional[float]
    cliffs_delta: Optional[float]
    effect_label: str
    hedges_g: Optional[float]
    p_naive: Optional[float]           # Welch t, treats rows as independent
    p_rank: Optional[float]            # Mann-Whitney, still row-level
    p_block: Optional[float]           # session block bootstrap -- the headline
    ci_lo_block: Optional[float]
    ci_hi_block: Optional[float]
    n_sessions: int

    def as_dict(self) -> dict:
        return dict(self.__dict__)


def _welch(a: Sequence[float], b: Sequence[float]) -> tuple[Optional[float], Optional[float]]:
    n_a, n_b = len(a), len(b)
    if n_a < 2 or n_b < 2:
        return None, None
    m_a, m_b = sum(a) / n_a, sum(b) / n_b
    v_a = sum((v - m_a) ** 2 for v in a) / (n_a - 1)
    v_b = sum((v - m_b) ** 2 for v in b) / (n_b - 1)
    se2 = v_a / n_a + v_b / n_b
    if se2 <= 0:
        return None, None
    t = (m_a - m_b) / math.sqrt(se2)
    num = se2 ** 2
    den = (v_a / n_a) ** 2 / (n_a - 1) + (v_b / n_b) ** 2 / (n_b - 1)
    df = num / den if den > 0 else float("nan")
    return t, _t_sf_two_sided(t, df)


def _hedges_g(a: Sequence[float], b: Sequence[float]) -> Optional[float]:
    n_a, n_b = len(a), len(b)
    if n_a < 2 or n_b < 2:
        return None
    m_a, m_b = sum(a) / n_a, sum(b) / n_b
    v_a = sum((v - m_a) ** 2 for v in a) / (n_a - 1)
    v_b = sum((v - m_b) ** 2 for v in b) / (n_b - 1)
    pooled = ((n_a - 1) * v_a + (n_b - 1) * v_b) / (n_a + n_b - 2)
    if pooled <= 0:
        return None
    d = (m_a - m_b) / math.sqrt(pooled)
    # Small-sample bias correction.
    j = 1.0 - 3.0 / (4.0 * (n_a + n_b) - 9.0)
    return d * j


def session_block_bootstrap_diff(
    values: Sequence[float],
    sessions: Sequence[object],
    in_bucket: Sequence[bool],
    *,
    iterations: int = 2000,
    seed: int = 20260903,
    confidence: float = 0.95,
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """Bootstrap the bucket-minus-base mean difference by resampling SESSIONS.

    Resampling whole sessions is what keeps the intraday autocorrelation in the
    resample instead of destroying it, so the interval reflects the number of
    independent days the study actually has rather than the number of minutes.

    Each session is reduced ONCE to four sufficient statistics -- bucket sum
    and count, base sum and count -- so a resample costs one pass over the
    sessions rather than over every row. The result is identical to resampling
    the rows themselves; a 90-day study is simply the difference between
    thousands of operations per iteration and millions.

    Returns ``(ci_lo, ci_hi, p)`` where ``p`` is the two-sided proportion of
    resamples whose difference falls on the other side of zero from the point
    estimate (a bootstrap p-value, floored at ``1/iterations``).
    """
    if not (len(values) == len(sessions) == len(in_bucket)):
        raise ValueError("values, sessions and in_bucket must be the same length")

    # session -> [bucket_sum, bucket_n, base_sum, base_n]
    agg: dict[object, list[float]] = {}
    for v, s, flag in zip(values, sessions, in_bucket):
        cell = agg.get(s)
        if cell is None:
            cell = [0.0, 0.0, 0.0, 0.0]
            agg[s] = cell
        # The baseline is the UNCONDITIONAL pool: every row, including the
        # bucket's own. That is the comparison methodology.md asks for --
        # "beat the unconditional base rate" -- not bucket vs not-bucket,
        # which is an easier and different question.
        cell[2] += v
        cell[3] += 1.0
        if flag:
            cell[0] += v
            cell[1] += 1.0

    cells = list(agg.values())
    n_keys = len(cells)
    if n_keys < 2:
        return None, None, None

    def diff_of(draw: Sequence[list[float]]) -> Optional[float]:
        bs = bn = qs = qn = 0.0
        for c in draw:
            bs += c[0]
            bn += c[1]
            qs += c[2]
            qn += c[3]
        if bn == 0.0 or qn == 0.0:
            return None
        return bs / bn - qs / qn

    point = diff_of(cells)
    if point is None:
        return None, None, None

    rng = random.Random(seed)
    randrange = rng.randrange
    diffs: list[float] = []
    for _ in range(iterations):
        bs = bn = qs = qn = 0.0
        for _ in range(n_keys):
            c = cells[randrange(n_keys)]
            bs += c[0]
            bn += c[1]
            qs += c[2]
            qn += c[3]
        if bn > 0.0 and qn > 0.0:
            diffs.append(bs / bn - qs / qn)
    if len(diffs) < 20:
        return None, None, None
    diffs.sort()
    alpha = (1.0 - confidence) / 2.0
    lo = _quantile(diffs, alpha)
    hi = _quantile(diffs, 1.0 - alpha)
    if point >= 0:
        tail = sum(1 for d in diffs if d <= 0.0)
    else:
        tail = sum(1 for d in diffs if d >= 0.0)
    p = min(1.0, 2.0 * tail / len(diffs))
    p = max(p, 1.0 / len(diffs))
    return lo, hi, p


def compare(
    values: Sequence[Optional[float]],
    sessions: Sequence[object],
    in_bucket: Sequence[bool],
    *,
    iterations: int = 2000,
    seed: int = 20260903,
) -> Comparison:
    """Score one bucket against the unconditional base rate.

    All three arguments are aligned row-for-row over the FULL sample for one
    instrument, horizon and measure. ``in_bucket`` flags the rows belonging to
    the conditional bucket under test; the baseline is every row, bucket
    included -- the unconditional base rate, which is the baseline
    ``methodology.md`` names ("the unconditional base rate, a simpler
    construction, or the existing production method"). Comparing against
    not-the-bucket instead would be an easier and different question.

    Because the bucket is a subset of the base, the two samples are not
    independent. The block bootstrap handles that correctly by resampling the
    same sessions for both sides of each difference; the parametric and rank
    p-values do not, which is one more reason they are reported as secondary.
    """
    if not (len(values) == len(sessions) == len(in_bucket)):
        raise ValueError("values, sessions and in_bucket must be the same length")

    vals: list[float] = []
    sess: list[object] = []
    flags: list[bool] = []
    for v, s, flag in zip(values, sessions, in_bucket):
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(fv):
            continue
        vals.append(fv)
        sess.append(s)
        flags.append(bool(flag))

    a = [v for v, f in zip(vals, flags) if f]
    b = vals
    sum_a = describe(a)
    sum_b = describe(b)

    diff = ratio = None
    if sum_a.mean is not None and sum_b.mean is not None:
        diff = sum_a.mean - sum_b.mean
        ratio = (sum_a.mean / sum_b.mean) if sum_b.mean not in (None, 0) else None

    _, p_naive = _welch(a, b)
    _, p_rank = mann_whitney_u(a, b)
    delta = cliffs_delta(a, b)

    ci_lo, ci_hi, p_block = session_block_bootstrap_diff(
        vals, sess, flags, iterations=iterations, seed=seed
    )

    return Comparison(
        n=sum_a.n, n_base=sum_b.n,
        mean=sum_a.mean, mean_base=sum_b.mean, diff=diff, ratio=ratio,
        median=sum_a.median, median_base=sum_b.median,
        cliffs_delta=delta, effect_label=delta_magnitude(delta),
        hedges_g=_hedges_g(a, b),
        p_naive=p_naive, p_rank=p_rank, p_block=p_block,
        ci_lo_block=ci_lo, ci_hi_block=ci_hi, n_sessions=len(set(sess)),
    )


# ---------------------------------------------------------------------------
# Proportions (point-target hit rates)
# ---------------------------------------------------------------------------

def wilson_ci(
    k: int, n: int, confidence: float = 0.95
) -> tuple[Optional[float], Optional[float]]:
    """Wilson score interval -- well behaved for rates near 0 and 1."""
    if n <= 0:
        return None, None
    z = 1.959963984540054 if abs(confidence - 0.95) < 1e-9 else _z_for(confidence)
    p = k / n
    denom = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return max(0.0, centre - half), min(1.0, centre + half)


def _z_for(confidence: float) -> float:
    """Invert the normal CDF by bisection -- exact enough, and dependency-free."""
    target = (1.0 + confidence) / 2.0
    lo, hi = 0.0, 10.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        if (1.0 - _norm_sf(mid)) < target:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def compare_proportions(k_a: int, n_a: int, k_b: int, n_b: int) -> dict:
    """Two-proportion comparison: rate, difference, and a pooled-z p-value."""
    if n_a <= 0 or n_b <= 0:
        return {"rate": None, "rate_base": None, "diff": None, "p": None,
                "ci_lo": None, "ci_hi": None}
    p_a, p_b = k_a / n_a, k_b / n_b
    lo, hi = wilson_ci(k_a, n_a)
    pooled = (k_a + k_b) / (n_a + n_b)
    se = math.sqrt(pooled * (1 - pooled) * (1 / n_a + 1 / n_b))
    p = 2.0 * _norm_sf(abs(p_a - p_b) / se) if se > 0 else None
    return {"rate": p_a, "rate_base": p_b, "diff": p_a - p_b, "p": p,
            "ci_lo": lo, "ci_hi": hi}


def benjamini_hochberg(p_values: Sequence[Optional[float]], alpha: float = 0.05) -> list[bool]:
    """BH step-up. Returns a rejection flag per input position.

    The study runs one test per (instrument x horizon x band x measure), which
    is enough comparisons that an uncorrected 5% would manufacture findings.
    ``None`` p-values never reject and never count toward ``m``.
    """
    indexed = [(p, i) for i, p in enumerate(p_values) if p is not None and math.isfinite(p)]
    out = [False] * len(p_values)
    m = len(indexed)
    if m == 0:
        return out
    indexed.sort(key=lambda t: t[0])
    k_max = 0
    for rank, (p, _) in enumerate(indexed, start=1):
        if p <= alpha * rank / m:
            k_max = rank
    for rank, (_, i) in enumerate(indexed, start=1):
        if rank <= k_max:
            out[i] = True
    return out


# ---------------------------------------------------------------------------
# Rank correlation
# ---------------------------------------------------------------------------

def _mid_ranks(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        mid = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = mid
        i = j + 1
    return ranks


def spearman(x: Sequence[float], y: Sequence[float]) -> Optional[float]:
    """Spearman's rho -- Pearson correlation of mid-ranks, tie-safe."""
    n = len(x)
    if n < 3 or n != len(y):
        return None
    rx, ry = _mid_ranks(x), _mid_ranks(y)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    dx = math.sqrt(sum((a - mx) ** 2 for a in rx))
    dy = math.sqrt(sum((b - my) ** 2 for b in ry))
    if dx <= 0 or dy <= 0:
        return None
    return num / (dx * dy)


@dataclass
class RankCorrelation:
    n: int
    n_sessions: int
    rho: Optional[float]
    ci_lo: Optional[float]
    ci_hi: Optional[float]
    p_block: Optional[float]

    def as_dict(self) -> dict:
        return dict(self.__dict__)


def spearman_block(
    x: Sequence[Optional[float]],
    y: Sequence[Optional[float]],
    sessions: Sequence[object],
    *,
    iterations: int = 1000,
    seed: int = 20260903,
    confidence: float = 0.95,
) -> RankCorrelation:
    """Spearman's rho with a session-level block-bootstrap interval.

    The headline number for "does the score order forward excursion at all":
    it needs no bucketing, no threshold, and no distributional assumption, and
    the session-level resample keeps the interval honest about how many
    independent days the sample really contains.

    Ranks are computed ONCE on the full sample and the resample is then a
    Pearson correlation of those fixed ranks -- the standard rank-transform
    bootstrap. Each session is reduced to six sufficient statistics, so an
    iteration costs one pass over the sessions rather than a re-sort of every
    row; without that a six-instrument study does not finish.
    """
    xs: list[float] = []
    ys: list[float] = []
    ss: list[object] = []
    for a, b, s in zip(x, y, sessions):
        if a is None or b is None:
            continue
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(fa) and math.isfinite(fb)):
            continue
        xs.append(fa)
        ys.append(fb)
        ss.append(s)

    n = len(xs)
    n_sessions = len(set(ss))
    rho = spearman(xs, ys)
    if rho is None or n_sessions < 2:
        return RankCorrelation(n, n_sessions, rho, None, None, None)

    rx, ry = _mid_ranks(xs), _mid_ranks(ys)
    # session -> [n, Sx, Sy, Sxx, Syy, Sxy] over the fixed ranks
    agg: dict[object, list[float]] = {}
    for i, s in enumerate(ss):
        cell = agg.get(s)
        if cell is None:
            cell = [0.0] * 6
            agg[s] = cell
        a, b = rx[i], ry[i]
        cell[0] += 1.0
        cell[1] += a
        cell[2] += b
        cell[3] += a * a
        cell[4] += b * b
        cell[5] += a * b

    cells = list(agg.values())
    n_keys = len(cells)

    def pearson_of(draw: Sequence[list[float]]) -> Optional[float]:
        t = [0.0] * 6
        for c in draw:
            for j in range(6):
                t[j] += c[j]
        cnt, sx, sy, sxx, syy, sxy = t
        if cnt < 3:
            return None
        num = cnt * sxy - sx * sy
        dx = cnt * sxx - sx * sx
        dy = cnt * syy - sy * sy
        if dx <= 0 or dy <= 0:
            return None
        return num / math.sqrt(dx * dy)

    rng = random.Random(seed)
    randrange = rng.randrange
    draws: list[float] = []
    for _ in range(iterations):
        r = pearson_of([cells[randrange(n_keys)] for _ in range(n_keys)])
        if r is not None:
            draws.append(r)
    if len(draws) < 20:
        return RankCorrelation(n, n_sessions, rho, None, None, None)
    draws.sort()
    alpha = (1.0 - confidence) / 2.0
    lo = _quantile(draws, alpha)
    hi = _quantile(draws, 1.0 - alpha)
    tail = sum(1 for d in draws if (d <= 0.0 if rho >= 0 else d >= 0.0))
    p = max(1.0 / len(draws), min(1.0, 2.0 * tail / len(draws)))
    return RankCorrelation(n, n_sessions, rho, lo, hi, p)
