"""The feature vector, measured strictly at the moment of the test.

Every value here is computed from data timestamped **at or before**
``WallTest.tested_at``.  That is the whole discipline of this module: a
feature that peeks one minute past the test produces a model that looks
brilliant in backtest and is worthless live, and the failure is invisible in
every summary statistic.  :func:`build_features` therefore takes the test
timestamp as a hard cutoff and every helper slices against it;
``tests/test_wall_break_odds.py`` pins the property by feeding a series whose
post-test values are poisoned and asserting the features do not move.

What is deliberately NOT a feature
----------------------------------
* **Distance to the wall.**  It is ~0 by construction — that is what "tested"
  means.  Distance drives ``P(touch)``, which the production forecast already
  models with the reflection principle; it carries almost nothing once you
  condition on the touch having happened.  Conflating the two is the most
  common way this question gets answered wrongly.
* **Anything from the resolution window** — ``excursion_pct``,
  ``minutes_to_resolve``, the outcome itself.  These are outcomes.
* **Realised forward volatility.**  Same reason.

What replaces distance is *room and energy*: how much of the session is left,
how much travel the prevailing volatility buys in that time, and whether the
tape is actually pushing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Mapping, Optional, Sequence

from research.wall_break_odds.events import (
    ET,
    SESSION_END,
    PriceBar,
    StepSeries,
    WallTest,
)

__all__ = [
    "FEATURE_NAMES",
    "SummarySeries",
    "FlowWindow",
    "build_features",
]

#: Lookback for every "is this changing?" feature, in minutes.  One value for
#: all of them so the model cannot be tuned by quietly giving one feature a
#: friendlier window than the others.
TREND_WINDOW_MIN = 30

_EPS = 1e-9

#: The model's feature vector, in a fixed order.  The report prints them in
#: this order and the dataset writes them under these keys, so a column added
#: here shows up everywhere or nowhere — never in one place only.
FEATURE_NAMES: tuple[str, ...] = (
    # — the variable the question is usually anchored on —
    "wall_strength_log",
    "wall_strength_share",
    "wall_strength_pctile_trailing",
    # — is the wall being consumed while it is tested? —
    "wall_strength_trend",
    "wall_migration_toward_break",
    "wall_age_minutes",
    # — regime —
    "net_gex_log_signed",
    "net_gex_trend",
    "flip_distance",
    "spot_above_flip",
    "convexity_risk_log",
    "local_gex_share",
    # — the tape —
    "flow_toward_break",
    "flow_acceleration",
    # — room and energy —
    "minutes_to_close",
    "travel_budget",
    "realized_sigma_ratio",
    "vix_change_intraday",
    # — session context —
    "minutes_since_open",
    "test_ordinal",
)


def _signed_log(x: Optional[float]) -> Optional[float]:
    """``sign(x)·log10(1+|x|)`` — tames a heavy-tailed dollar quantity while
    keeping its sign, which for net GEX is the regime itself."""
    if x is None or not math.isfinite(x):
        return None
    return math.copysign(math.log10(1.0 + abs(x)), x)


def _pct_change(now: Optional[float], then: Optional[float]) -> Optional[float]:
    if now is None or then is None:
        return None
    denom = abs(then)
    if denom < _EPS:
        return None
    return (now - then) / denom


@dataclass
class SummarySeries:
    """The ``gex_summary`` path for one session, as step functions.

    Built once per session and shared by every event in it — the alternative
    is re-scanning the day's frames per event, which turns a multi-month study
    quadratic.
    """

    call_wall: StepSeries
    put_wall: StepSeries
    call_wall_strength: StepSeries
    put_wall_strength: StepSeries
    total_net_gex: StepSeries
    flip: StepSeries
    flip_distance: StepSeries
    local_gex: StepSeries
    convexity_risk: StepSeries

    @classmethod
    def from_rows(cls, rows: Sequence[Mapping[str, Any]]) -> "SummarySeries":
        def series(key: str) -> StepSeries:
            return StepSeries((r["timestamp"], r.get(key)) for r in rows)

        return cls(
            call_wall=series("call_wall"),
            put_wall=series("put_wall"),
            call_wall_strength=series("call_wall_strength"),
            put_wall_strength=series("put_wall_strength"),
            total_net_gex=series("total_net_gex"),
            flip=series("gamma_flip_point"),
            flip_distance=series("flip_distance"),
            local_gex=series("local_gex"),
            convexity_risk=series("convexity_risk"),
        )

    def wall(self, side: str) -> StepSeries:
        return self.call_wall if side == "call" else self.put_wall

    def strength(self, side: str) -> StepSeries:
        return self.call_wall_strength if side == "call" else self.put_wall_strength


#: Canonical option-type codes, and every spelling seen in the wild mapped
#: onto them. ``flow_by_contract`` stores 'C'/'P' (see
#: src/flow_series_sql.py), but callers and fixtures naturally write
#: 'call'/'put'. Matching on the wrong spelling silently drops EVERY row and
#: the feature reads as "no flow" — which is how the first real run of this
#: study produced an empty flow column against a fully populated table.
_OPTION_TYPE_CANON = {
    "c": "C",
    "call": "C",
    "calls": "C",
    "p": "P",
    "put": "P",
    "puts": "P",
}


def canonical_option_type(value: Any) -> Optional[str]:
    """'call' / 'C' / 'CALL' -> 'C'. None when unrecognised."""
    if value is None:
        return None
    return _OPTION_TYPE_CANON.get(str(value).strip().lower())


@dataclass
class FlowWindow:
    """Signed aggressor premium at the wall strike, as a cumulative series.

    ``flow_by_contract.net_premium`` is **day-to-date cumulative per contract**
    and resets at 09:30 ET, so a window figure is a DIFFERENCE of two
    cumulatives, never a sum over buckets.  Summing would double-count the
    whole morning into every window and make the feature grow monotonically
    through the day regardless of what the tape did.

    The difference is taken per contract and then summed, because a contract
    whose first print lands mid-window has no earlier row at all: its
    cumulative before that is implicitly zero, and differencing pre-summed
    totals would book its entire day-to-date figure as window activity.
    """

    #: ``{(canonical_type, strike, expiration): [(bucket_ts, cumulative)]}``
    series: dict[tuple[str, float, Any], list[tuple[datetime, float]]]
    #: Option-type values that could not be canonicalised, with their counts.
    #: Non-empty means the encoding changed underneath this module, and the
    #: flow features are silently degraded — the caller is expected to make
    #: noise about it rather than let the column quietly read as "no flow".
    unrecognised: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_rows(cls, rows: Sequence[Mapping[str, Any]]) -> "FlowWindow":
        acc: dict[tuple[str, float, Any], list[tuple[datetime, float]]] = {}
        bad: dict[str, int] = {}
        for r in rows:
            prem = r.get("net_premium")
            ts = r.get("timestamp")
            if prem is None or ts is None:
                continue
            opt = canonical_option_type(r.get("option_type"))
            if opt is None:
                raw = str(r.get("option_type"))
                bad[raw] = bad.get(raw, 0) + 1
                continue
            key = (opt, float(r.get("strike") or 0.0), r.get("expiration"))
            acc.setdefault(key, []).append((ts, float(prem)))
        for v in acc.values():
            v.sort(key=lambda p: p[0])
        return cls(series=acc, unrecognised=bad)

    @staticmethod
    def _cum_at(points: Sequence[tuple[datetime, float]], ts: datetime) -> float:
        """Cumulative as of ``ts`` — the last bucket at or before it, else 0."""
        import bisect

        stamps = [p[0] for p in points]
        idx = bisect.bisect_right(stamps, ts) - 1
        return points[idx][1] if idx >= 0 else 0.0

    def window_premium(
        self, option_type: str, strikes: Sequence[float], start: datetime, end: datetime
    ) -> Optional[float]:
        """Net aggressor premium over ``(start, end]`` for the given strikes.

        Returns None when no contract in the neighbourhood traded at all, so a
        genuinely empty tape is distinguishable from a balanced one. Both the
        stored and the requested option type are canonicalised first, so a
        caller saying 'call' and a table storing 'C' agree.
        """
        want_type = canonical_option_type(option_type)
        if want_type is None:
            return None
        wanted = {round(float(s), 4) for s in strikes}
        total = 0.0
        seen = False
        for (opt, strike, _exp), points in self.series.items():
            if opt != want_type or round(strike, 4) not in wanted:
                continue
            seen = True
            total += self._cum_at(points, end) - self._cum_at(points, start)
        return total if seen else None

    def coverage(self) -> dict[str, int]:
        """Contracts held per canonical option type — a cheap sanity probe.

        A window built from a populated table that reports ``{}`` here is the
        signature of an encoding mismatch, not a quiet tape.
        """
        out: dict[str, int] = {}
        for opt, _strike, _exp in self.series:
            out[opt] = out.get(opt, 0) + 1
        return out


def _realized_sigma(bars: Sequence[PriceBar], end: datetime, minutes: int) -> Optional[float]:
    """Close-to-close stdev of minute returns over the trailing window, as a
    per-minute fraction.  None when the window is too thin to estimate."""
    start = end - timedelta(minutes=minutes)
    window = [b for b in bars if start <= b.ts <= end]
    if len(window) < 10:
        return None
    rets = [
        (window[i].close - window[i - 1].close) / window[i - 1].close
        for i in range(1, len(window))
        if window[i - 1].close > 0
    ]
    if len(rets) < 5:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(max(var, 0.0))


def _minutes_since_open(ts: datetime) -> float:
    et = ts.astimezone(ET)
    return max((et.hour * 60 + et.minute) - (9 * 60 + 30), 0)


def _session_open(ts: datetime) -> datetime:
    """09:30 ET on ``ts``'s session, as an instant."""
    et = ts.astimezone(ET)
    return et.replace(hour=9, minute=30, second=0, microsecond=0)


def _minutes_to_close(ts: datetime) -> float:
    et = ts.astimezone(ET)
    close = SESSION_END.hour * 60 + SESSION_END.minute
    return max(close - (et.hour * 60 + et.minute), 0)


def build_features(
    event: WallTest,
    summary: SummarySeries,
    bars: Sequence[PriceBar],
    flow: Optional[FlowWindow] = None,
    vix: Optional[StepSeries] = None,
    strength_percentile: Optional[float] = None,
    strike_step: float = 5.0,
) -> dict[str, Optional[float]]:
    """The feature vector for one event.  Missing inputs yield None, not zero.

    A zero would be a *claim* — "there was no flow at the wall" — where the
    truth is "we do not have the flow".  The model layer drops rows with
    missing features per fit and reports the coverage, so a sparsely populated
    column narrows the sample instead of silently poisoning the coefficients.

    ``strength_percentile`` is supplied by the caller because it is a
    TRAILING-sessions quantity: computing it over the whole dataset would rank
    today's wall against walls that had not happened yet, which is leakage of
    exactly the kind this module exists to prevent.
    """
    t = event.tested_at
    then = t - timedelta(minutes=TREND_WINDOW_MIN)
    prior = then - timedelta(minutes=TREND_WINDOW_MIN)
    side = event.side
    toward = 1.0 if side == "call" else -1.0

    strength_now = summary.strength(side).at(t)
    strength_then = summary.strength(side).at(then)
    net_gex_now = summary.total_net_gex.at(t)
    net_gex_then = summary.total_net_gex.at(then)
    wall_then = summary.wall(side).at(then)
    flip = summary.flip.at(t)
    local = summary.local_gex.at(t)

    # Room and energy. ``travel_budget`` is how many break-buffers the
    # prevailing volatility buys in the time left — the honest replacement for
    # "distance", which is ~0 once a test has happened.
    sigma_min = _realized_sigma(bars, t, TREND_WINDOW_MIN)
    mins_left = _minutes_to_close(t)
    buffer_pct = 0.0005
    travel_budget: Optional[float] = None
    if sigma_min is not None and mins_left > 0:
        travel_budget = (sigma_min * math.sqrt(mins_left)) / buffer_pct

    # Vol-of-the-day context: trailing realised vol against the session's own
    # average pace, so "compressing into the test" is visible.
    sigma_session = _realized_sigma(bars, t, max(int(_minutes_since_open(t)), 1))
    sigma_ratio = sigma_min / sigma_session if sigma_min is not None and sigma_session else None

    # Falling VIX through the session is the vanna tailwind — the mechanism
    # that lifts a tape without any of it showing up in the gamma at the wall.
    # Measured open-to-now so it reads as "what has vol done today", not as a
    # level (which would just proxy the vol regime).
    vix_now = vix.at(t) if vix is not None else None
    vix_open = vix.at(_session_open(t)) if vix is not None else None
    vix_change = (
        float(vix_now) - float(vix_open) if vix_now is not None and vix_open is not None else None
    )

    flow_now = flow_prior = None
    if flow is not None:
        neighbourhood = [event.wall, event.wall + strike_step * toward]
        flow_now = flow.window_premium(side, neighbourhood, then, t)
        flow_prior = flow.window_premium(side, neighbourhood, prior, then)

    feats: dict[str, Optional[float]] = {
        "wall_strength_log": _signed_log(strength_now),
        "wall_strength_share": (
            abs(float(strength_now)) / (abs(float(net_gex_now)) + _EPS)
            if strength_now is not None and net_gex_now is not None
            else None
        ),
        "wall_strength_pctile_trailing": strength_percentile,
        "wall_strength_trend": _pct_change(strength_now, strength_then),
        "wall_migration_toward_break": (
            (event.wall - float(wall_then)) / event.wall * toward * 100.0
            if wall_then is not None and event.wall
            else None
        ),
        "wall_age_minutes": summary.wall(side).age_minutes(t),
        "net_gex_log_signed": _signed_log(net_gex_now),
        "net_gex_trend": _pct_change(net_gex_now, net_gex_then),
        "flip_distance": (
            float(summary.flip_distance.at(t)) if summary.flip_distance.at(t) is not None else None
        ),
        "spot_above_flip": (
            1.0
            if flip is not None and event.spot_at_test > float(flip)
            else 0.0 if flip is not None else None
        ),
        "convexity_risk_log": _signed_log(summary.convexity_risk.at(t)),
        "local_gex_share": (
            abs(float(local)) / (abs(float(net_gex_now)) + _EPS)
            if local is not None and net_gex_now is not None
            else None
        ),
        # Positive always means "pressure in the direction that breaks this
        # wall", on BOTH sides: net_premium is buy-minus-sell aggressor
        # premium, and customers buying the wall-side option is what shortens
        # dealers in it. Orienting both sides the same way is what lets the
        # call and put fits be read against each other.
        "flow_toward_break": None if flow_now is None else flow_now,
        "flow_acceleration": (
            None if flow_now is None or flow_prior is None else flow_now - flow_prior
        ),
        "minutes_to_close": mins_left,
        "travel_budget": travel_budget,
        "realized_sigma_ratio": sigma_ratio,
        "vix_change_intraday": vix_change,
        "minutes_since_open": _minutes_since_open(t),
        "test_ordinal": float(event.test_ordinal),
    }
    return {k: feats.get(k) for k in FEATURE_NAMES}
