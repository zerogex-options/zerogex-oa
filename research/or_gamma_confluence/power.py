"""How far is an effect from resolving?  Power from the study's own intervals.

Every effect in this study is a difference of session-clustered rates, and its
uncertainty is already measured: the session-block bootstrap interval in
:func:`.cohorts.compare_to_baseline`.  So power is not modeled from a
textbook binomial here, which would assume independent touches and overstate
it by the design effect.  It is read straight off that interval:

* ``se``  -- the interval's half-width over ``z(0.975)``;
* power   -- two-sided, for a stated true effect, at that ``se``;
* MDE     -- the smallest effect this sample resolves at 80% power;
* sessions needed -- how many CALENDAR sessions bring ``se`` down to the size
  that resolves a stated effect, scaling ``se`` as ``1 / sqrt(sessions)``.

The scaling is the one assumption.  It holds when a cohort's share of touches
and its touches per session stay roughly what they are, which is the only
honest extrapolation available from one window.  The September findings
scaled H2 from the POOLED sample's power curve (59 sessions resolve 5 points);
H2's cohort held about a ninth of the touches, so that borrowed precision it
never had.  Its own interval (-6.0 points at p=0.256, so ``se`` about 5.3)
puts a 6-point effect at roughly 300 sessions, not the ~40 that curve implies.
Reading ``se`` from the cohort's own interval is the correction.

Stdlib only: :class:`statistics.NormalDist` supplies the quantiles.
"""

from __future__ import annotations

import math
from statistics import NormalDist
from typing import Any, Mapping, Optional

__all__ = [
    "TRADING_SESSIONS_PER_MONTH",
    "se_from_ci",
    "power_two_sided",
    "min_detectable_effect",
    "sessions_needed",
    "power_report",
]

_N = NormalDist()

#: For turning a session count into calendar time.  252 / 12.
TRADING_SESSIONS_PER_MONTH = 21.0


def _z(p: float) -> float:
    return _N.inv_cdf(p)


def se_from_ci(
    lo: Optional[float], hi: Optional[float], confidence: float = 0.95
) -> Optional[float]:
    """Standard error implied by a two-sided interval."""
    if lo is None or hi is None or hi <= lo:
        return None
    return (hi - lo) / (2.0 * _z(0.5 + confidence / 2.0))


def power_two_sided(effect: float, se: Optional[float], alpha: float = 0.05) -> Optional[float]:
    """P(a two-sided test at ``alpha`` rejects) when the true effect is ``effect``."""
    if se is None or se <= 0:
        return None
    z = _z(1.0 - alpha / 2.0)
    shift = abs(effect) / se
    return _N.cdf(shift - z) + _N.cdf(-shift - z)


def min_detectable_effect(
    se: Optional[float], power: float = 0.8, alpha: float = 0.05
) -> Optional[float]:
    """The effect resolved with probability ``power`` at this ``se``."""
    if se is None or se <= 0:
        return None
    return (_z(1.0 - alpha / 2.0) + _z(power)) * se


def sessions_needed(
    n_sessions: int,
    se: Optional[float],
    effect: float,
    power: float = 0.8,
    alpha: float = 0.05,
) -> Optional[int]:
    """Calendar sessions at which ``effect`` is resolved with ``power``.

    ``se`` is assumed to shrink as ``1 / sqrt(sessions)``; see the module
    docstring for when that holds.
    """
    if se is None or se <= 0 or n_sessions <= 0 or effect == 0:
        return None
    target = abs(effect) / (_z(1.0 - alpha / 2.0) + _z(power))
    return int(math.ceil(n_sessions * (se / target) ** 2))


def power_report(
    diff: Optional[float],
    ci_low: Optional[float],
    ci_high: Optional[float],
    n_sessions: int,
    *,
    effects: Mapping[str, float],
    power: float = 0.8,
    alpha: float = 0.05,
) -> dict[str, Any]:
    """Everything the report prints about one effect's power.

    ``effects`` names the true effects to evaluate, in the same units as
    ``diff`` (rate differences, so 0.06 is six points).  ``"observed"`` is
    added automatically when ``diff`` is known.
    """
    se = se_from_ci(ci_low, ci_high)
    mde = min_detectable_effect(se, power, alpha)
    out: dict[str, Any] = {
        "se": se,
        "mde": mde,
        "n_sessions": n_sessions,
        "power_target": power,
        "alpha": alpha,
        "scenarios": [],
    }
    named = dict(effects)
    if diff is not None and diff != 0:
        named = {"observed": diff, **named}
    for name, eff in named.items():
        need = sessions_needed(n_sessions, se, eff, power, alpha)
        out["scenarios"].append(
            {
                "name": name,
                "effect": eff,
                "power_now": power_two_sided(eff, se, alpha),
                "sessions_needed": need,
                "more_sessions": (max(0, need - n_sessions) if need is not None else None),
                "more_months": (
                    max(0.0, (need - n_sessions) / TRADING_SESSIONS_PER_MONTH)
                    if need is not None
                    else None
                ),
            }
        )
    return out
