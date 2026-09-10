"""Direct test of the aggressor assumption against exchange-classified MM activity.

The question this module answers is narrower than the market-outcome battery
and comes before it: *when ZeroGEX's tape classification says a trade was
buyer-initiated, how often was the exchange-classified Market Maker population
actually selling that contract?*  It is the assumption criticised in the
"aggressor side ≠ participant identity" argument, put to a number.

Two inputs, one grid
--------------------
* **Model B** — :class:`~research.mm_attributed_gex.aggressor.AggressorBucket`
  minute rows: ZeroGEX's Lee-Ready classification with the passive side
  *assumed* to be a market maker.
* **Model C** — :class:`~research.mm_attributed_gex.schema.ParticipantActivity`
  records tagged ``MARKET_MAKER`` by the exchange.

Both are folded onto the exchange feed's own interval (1-minute, 10-minute or
session) so a comparison is never made across mismatched buckets.  For each
``(bucket end, expiration, strike, call/put)`` cell::

    B signed change = seller-initiated − buyer-initiated     (assumed MM buys − sells)
    C signed change = MM buys − MM sells                     (all position effects)

Coverage discipline
-------------------
A cell is only compared when both feeds covered the series on that session.
A series the exchange file never mentions cannot be scored as "MM did nothing"
— it may simply not be in the delivery — and a series ZeroGEX never quoted
cannot be scored as "no aggressor flow".  Those are counted and excluded, not
silently zeroed.  Within a covered series-session, an absent cell on either
side IS a real zero: the exchange reports no MM activity, or the tape shows no
volume, in that bucket.

Zero handling
-------------
Most cells have little or no attributed activity.  A naive agreement rate over
all cells would be dominated by "both zero" and read as accuracy.  So every
metric is reported twice — over all matched cells and over *active* cells
whose gross attributed MM activity clears a predeclared floor — and the floor
is swept over sensitivity bands rather than chosen after looking.

Stratification is fixed in advance (call/put, DTE bands, moneyness, time of
day, activity size).  Simple-vs-complex is reported only when the delivered
data actually identifies it; otherwise it is stated as unavailable.  Intervals
are resampled by session for confidence intervals, because cells inside a
session are not independent.
"""

from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

import numpy as np

from research.mm_attributed_gex import stats as st
from research.mm_attributed_gex.aggressor import AggressorBucket, aggregate_to_interval
from research.mm_attributed_gex.gex import CONTRACT_MULTIPLIER
from research.mm_attributed_gex.schema import (
    Interval,
    ParticipantActivity,
    SeriesKey,
    Side,
)

__all__ = [
    "AttributionConfig",
    "MatchedCell",
    "MatchDiagnostics",
    "AttributionResult",
    "match_cells",
    "attribution_metrics",
    "compare_attribution",
    "render_attribution_markdown",
    "write_attribution_report",
    "write_matched_cells_csv",
]

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    import pytz

    ET = pytz.timezone("US/Eastern")  # type: ignore[assignment]


@dataclass(frozen=True)
class AttributionConfig:
    """Predeclared thresholds and bins.  Change them before a run, never after."""

    #: Gross attributed MM activity (buys + sells) for a cell to count as active.
    min_attributed_activity: float = 10.0
    #: Sensitivity bands the headline floor is swept over.
    activity_sensitivity: tuple[float, ...] = (1.0, 5.0, 10.0, 25.0, 50.0)
    #: Fixed activity-size bins on gross attributed MM contracts.
    volume_bins: tuple[tuple[str, float, float], ...] = (
        ("small", 0.0, 25.0),
        ("medium", 25.0, 250.0),
        ("large", 250.0, math.inf),
    )
    #: |K/S − 1| at or below which a cell is at-the-money.
    atm_band: float = 0.005
    #: Inclusive DTE bins.
    dte_bins: tuple[tuple[str, int, int], ...] = (
        ("0dte", 0, 0),
        ("1_5dte", 1, 5),
        ("6plus_dte", 6, 10_000),
    )
    #: Session-minute bins (minutes after 09:30 ET, end exclusive).
    time_of_day_bins: tuple[tuple[str, int, int], ...] = (
        ("first_30m", 0, 30),
        ("midday", 30, 330),
        ("final_hour", 330, 100_000),
    )
    #: Below this many cells a stratum is reported but flagged insufficient.
    min_stratum_n: int = 30
    n_boot: int = 1000
    seed: int = 20260907


@dataclass
class MatchedCell:
    """One ``(bucket end, series)`` cell with both sides of the comparison."""

    bucket_end: datetime
    trading_date: date
    key: SeriesKey
    option_symbol: str
    b_buyer: float = 0.0
    b_seller: float = 0.0
    b_unclassified: float = 0.0
    c_buys: float = 0.0
    c_sells: float = 0.0
    has_b: bool = False
    has_c: bool = False
    gamma: Optional[float] = None
    spot: Optional[float] = None
    b_extrapolated: bool = False
    complexity: Optional[str] = None

    @property
    def expiration(self) -> date:
        return self.key[1]

    @property
    def strike(self) -> float:
        return self.key[2]

    @property
    def option_type(self) -> str:
        return self.key[3]

    @property
    def b_signed(self) -> float:
        return self.b_seller - self.b_buyer

    @property
    def c_signed(self) -> float:
        return self.c_buys - self.c_sells

    @property
    def c_gross(self) -> float:
        return self.c_buys + self.c_sells

    @property
    def b_classified(self) -> float:
        return self.b_buyer + self.b_seller

    @property
    def signed_error(self) -> float:
        return self.b_signed - self.c_signed

    @property
    def abs_error(self) -> float:
        return abs(self.signed_error)

    @property
    def dollar_gamma_per_contract(self) -> Optional[float]:
        """``γ·100·S²·0.01`` when spot is known, else raw ``|γ|``."""
        if self.gamma is None:
            return None
        if self.spot:
            return abs(self.gamma) * CONTRACT_MULTIPLIER * self.spot * self.spot * 0.01
        return abs(self.gamma)

    @property
    def sign_agreement(self) -> Optional[bool]:
        """Same sign when both sides are non-zero; ``None`` otherwise."""
        if self.b_signed == 0.0 or self.c_signed == 0.0:
            return None
        return (self.b_signed > 0) == (self.c_signed > 0)

    @property
    def sign_agreement_with_zeros(self) -> bool:
        """Zero treated as its own sign, so both-zero counts as agreement."""
        return np.sign(self.b_signed) == np.sign(self.c_signed)

    def dte(self) -> int:
        return (self.expiration - self.trading_date).days

    def session_minute(self) -> int:
        et = self.bucket_end.astimezone(ET)
        return (et.hour - 9) * 60 + (et.minute - 30)

    def moneyness(self, atm_band: float) -> Optional[str]:
        if not self.spot:
            return None
        rel = self.strike / self.spot - 1.0
        if abs(rel) <= atm_band:
            return "atm"
        above = rel > 0
        if self.option_type == "C":
            return "otm" if above else "itm"
        return "itm" if above else "otm"

    def as_dict(self) -> dict[str, Any]:
        return {
            "bucket_end": self.bucket_end.isoformat(),
            "trading_date": self.trading_date.isoformat(),
            "expiration": self.expiration.isoformat(),
            "strike": self.strike,
            "option_type": self.option_type,
            "option_symbol": self.option_symbol,
            "b_buyer_initiated": self.b_buyer,
            "b_seller_initiated": self.b_seller,
            "b_unclassified": self.b_unclassified,
            "b_signed_change": self.b_signed,
            "c_mm_buys": self.c_buys,
            "c_mm_sells": self.c_sells,
            "c_signed_change": self.c_signed,
            "c_gross_activity": self.c_gross,
            "has_b": self.has_b,
            "has_c": self.has_c,
            "sign_agreement": self.sign_agreement,
            "abs_error": self.abs_error,
            "signed_error": self.signed_error,
            "gamma": self.gamma,
            "spot": self.spot,
            "dollar_gamma_per_contract": self.dollar_gamma_per_contract,
            "b_gamma_weighted_change": (
                None
                if self.dollar_gamma_per_contract is None
                else self.b_signed * self.dollar_gamma_per_contract
            ),
            "c_gamma_weighted_change": (
                None
                if self.dollar_gamma_per_contract is None
                else self.c_signed * self.dollar_gamma_per_contract
            ),
            "b_extrapolated": self.b_extrapolated,
            "complexity": self.complexity,
        }


@dataclass
class MatchDiagnostics:
    interval: str = "unknown"
    b_rows: int = 0
    c_records: int = 0
    c_mm_records: int = 0
    b_cells: int = 0
    c_cells: int = 0
    matched_cells: int = 0
    cells_b_only: int = 0
    cells_c_only: int = 0
    cells_both: int = 0
    sessions_matched: int = 0
    sessions_b_only: int = 0
    sessions_c_only: int = 0
    series_days_matched: int = 0
    series_days_b_only: int = 0
    series_days_c_only: int = 0
    c_activity_excluded_no_b_coverage: float = 0.0
    b_activity_excluded_no_c_coverage: float = 0.0
    cells_with_gamma: int = 0
    cells_with_spot: int = 0
    intraday_identification_testable: bool = True
    complexity_available: bool = False
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _detect_interval(records: Sequence[ParticipantActivity]) -> Interval:
    counts = Counter(r.interval for r in records)
    if not counts:
        return Interval.UNKNOWN
    if len(counts) > 1:
        raise ValueError(
            f"attributed records carry mixed intervals {sorted(c.value for c in counts)}; "
            "compare one cadence at a time"
        )
    return next(iter(counts))


def match_cells(
    aggressor: Iterable[AggressorBucket],
    attributed: Iterable[ParticipantActivity],
    *,
    spot_provider: Optional[Callable[[datetime], Optional[float]]] = None,
    gamma_provider: Optional[Callable[[datetime, SeriesKey], Optional[float]]] = None,
    interval: Optional[Interval] = None,
) -> tuple[list[MatchedCell], MatchDiagnostics]:
    """Join Model B and Model C on the exchange interval.  See the module notes."""
    diag = MatchDiagnostics()
    b_rows = list(aggressor)
    c_all = list(attributed)
    diag.b_rows = len(b_rows)
    diag.c_records = len(c_all)
    c_mm = [r for r in c_all if r.is_market_maker]
    diag.c_mm_records = len(c_mm)

    interval = interval or _detect_interval(c_mm)
    diag.interval = interval.value
    if interval in (Interval.SESSION, Interval.UNKNOWN):
        diag.intraday_identification_testable = False
        diag.notes.append(
            "attributed feed is a session summary: the comparison is end-of-day only and "
            "says nothing about intraday identification"
        )
    if any("complexity" in r.attrs or "trade_type" in r.attrs for r in c_mm):
        diag.complexity_available = True

    # Fold both sides onto the grid.
    b_cells = aggregate_to_interval(b_rows, interval)
    c_cells: dict[tuple[datetime, SeriesKey], list[float]] = defaultdict(lambda: [0.0, 0.0])
    c_complexity: dict[tuple[datetime, SeriesKey], str] = {}
    for r in c_mm:
        cell = c_cells[(r.timestamp, r.key)]
        if r.side is Side.BUY:
            cell[0] += r.contracts
        else:
            cell[1] += r.contracts
        tag = r.attrs.get("complexity") or r.attrs.get("trade_type")
        if tag:
            c_complexity[(r.timestamp, r.key)] = str(tag)
    diag.b_cells = len(b_cells)
    diag.c_cells = len(c_cells)

    # Coverage: which (series, session) each side saw at all.
    b_series_days = {(k, cell.trading_date) for (_end, k), cell in b_cells.items()}
    c_series_days = {(r.key, r.trading_date) for r in c_mm}
    b_sessions = {d for _k, d in b_series_days}
    c_sessions = {d for _k, d in c_series_days}
    covered = b_series_days & c_series_days
    diag.sessions_matched = len(b_sessions & c_sessions)
    diag.sessions_b_only = len(b_sessions - c_sessions)
    diag.sessions_c_only = len(c_sessions - b_sessions)
    diag.series_days_matched = len(covered)
    diag.series_days_b_only = len(b_series_days - c_series_days)
    diag.series_days_c_only = len(c_series_days - b_series_days)

    c_trading_date: dict[tuple[datetime, SeriesKey], date] = {}
    for r in c_mm:
        c_trading_date[(r.timestamp, r.key)] = r.trading_date

    cells: dict[tuple[datetime, SeriesKey], MatchedCell] = {}

    def _cell(end: datetime, key: SeriesKey, trading_date: date, option_symbol: str) -> MatchedCell:
        cell = cells.get((end, key))
        if cell is None:
            cell = MatchedCell(
                bucket_end=end, trading_date=trading_date, key=key, option_symbol=option_symbol
            )
            cells[(end, key)] = cell
        return cell

    for (end, key), agg in b_cells.items():
        if (key, agg.trading_date) not in covered:
            diag.b_activity_excluded_no_c_coverage += agg.buyer_initiated + agg.seller_initiated
            continue
        cell = _cell(end, key, agg.trading_date, agg.option_symbol)
        cell.b_buyer = agg.buyer_initiated
        cell.b_seller = agg.seller_initiated
        cell.b_unclassified = agg.unclassified
        cell.has_b = True
        cell.b_extrapolated = agg.extrapolated
        if agg.gamma is not None:
            cell.gamma = agg.gamma

    for (end, key), (buys, sells) in c_cells.items():
        trading_date = c_trading_date[(end, key)]
        if (key, trading_date) not in covered:
            diag.c_activity_excluded_no_b_coverage += buys + sells
            continue
        cell = _cell(end, key, trading_date, "")
        cell.c_buys = buys
        cell.c_sells = sells
        cell.has_c = True
        tag = c_complexity.get((end, key))
        if tag:
            cell.complexity = tag

    out = sorted(cells.values(), key=lambda c: (c.bucket_end, c.key))
    for cell in out:
        if cell.gamma is None and gamma_provider is not None:
            cell.gamma = gamma_provider(cell.bucket_end, cell.key)
        if spot_provider is not None:
            cell.spot = spot_provider(cell.bucket_end)
        diag.cells_with_gamma += int(cell.gamma is not None)
        diag.cells_with_spot += int(cell.spot is not None)
        diag.cells_both += int(cell.has_b and cell.has_c)
        diag.cells_b_only += int(cell.has_b and not cell.has_c)
        diag.cells_c_only += int(cell.has_c and not cell.has_b)
    diag.matched_cells = len(out)
    return out, diag


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def _weighted_pearson(x: np.ndarray, y: np.ndarray, w: np.ndarray) -> Optional[float]:
    mask = np.isfinite(x) & np.isfinite(y) & np.isfinite(w) & (w > 0)
    if mask.sum() < 3:
        return None
    x, y, w = x[mask], y[mask], w[mask]
    wsum = w.sum()
    mx, my = (w * x).sum() / wsum, (w * y).sum() / wsum
    cov = (w * (x - mx) * (y - my)).sum()
    vx = (w * (x - mx) ** 2).sum()
    vy = (w * (y - my) ** 2).sum()
    if vx <= 0 or vy <= 0:
        return None
    return float(cov / math.sqrt(vx * vy))


def _rate(numerator: int, denominator: int) -> Optional[float]:
    return (numerator / denominator) if denominator else None


def attribution_metrics(
    cells: Sequence[MatchedCell],
    *,
    active_threshold: float,
) -> dict[str, Any]:
    """Agreement and error metrics over ``cells``, all-cells and active-cells."""
    if not cells:
        return {"n": 0, "n_active": 0}
    b = np.array([c.b_signed for c in cells], dtype=float)
    c_ = np.array([c.c_signed for c in cells], dtype=float)
    gross = np.array([c.c_gross for c in cells], dtype=float)
    gamma_w = np.array(
        [
            c.dollar_gamma_per_contract if c.dollar_gamma_per_contract is not None else np.nan
            for c in cells
        ],
        dtype=float,
    )
    active = gross >= active_threshold

    def _block(mask: np.ndarray) -> dict[str, Any]:
        n = int(mask.sum())
        if n == 0:
            return {"n": 0}
        bb, cc, gg, ww = b[mask], c_[mask], gross[mask], gamma_w[mask]
        both_nonzero = (bb != 0) & (cc != 0)
        agree = (np.sign(bb) == np.sign(cc)) & both_nonzero
        agree_with_zero = np.sign(bb) == np.sign(cc)
        c_nonzero = cc != 0
        agree_c_nonzero = (np.sign(bb) == np.sign(cc)) & c_nonzero
        abs_err = np.abs(bb - cc)
        corr = st.correlation(bb, cc)
        weights_sign = np.abs(cc) * np.where(np.isfinite(ww), ww, np.nan)
        ws_mask = np.isfinite(weights_sign) & c_nonzero & (bb != 0)
        weighted_sign_agreement = (
            float((weights_sign[ws_mask] * agree[ws_mask]).sum() / weights_sign[ws_mask].sum())
            if ws_mask.any() and weights_sign[ws_mask].sum() > 0
            else None
        )
        gross_mean = float(gg.mean()) if n else 0.0
        return {
            "n": n,
            "n_both_nonzero": int(both_nonzero.sum()),
            "n_c_zero": int((cc == 0).sum()),
            "n_b_zero": int((bb == 0).sum()),
            "sign_agreement_both_nonzero": _rate(int(agree.sum()), int(both_nonzero.sum())),
            "sign_agreement_excluding_attributed_zero": _rate(
                int(agree_c_nonzero.sum()), int(c_nonzero.sum())
            ),
            "sign_agreement_zero_as_sign": float(agree_with_zero.mean()),
            "weighted_sign_agreement_by_abs_c_gamma": weighted_sign_agreement,
            "pearson": corr.get("pearson"),
            "pearson_p": corr.get("pearson_p"),
            "spearman": corr.get("spearman"),
            "spearman_p": corr.get("spearman_p"),
            "gamma_weighted_pearson": _weighted_pearson(bb, cc, ww),
            "mae": float(abs_err.mean()),
            "rmse": float(math.sqrt(float((abs_err**2).mean()))),
            "normalized_mae": (float(abs_err.mean() / gross_mean) if gross_mean > 0 else None),
            "bias_mean_b_minus_c": float((bb - cc).mean()),
            "mean_abs_b": float(np.abs(bb).mean()),
            "mean_abs_c": float(np.abs(cc).mean()),
            "gamma_weight_coverage": float(np.isfinite(ww).mean()),
        }

    return {
        "active_threshold": active_threshold,
        "all_cells": _block(np.ones(len(cells), dtype=bool)),
        "active_cells": _block(active),
    }


def _session_bootstrap(
    cells: Sequence[MatchedCell],
    statistic: Callable[[Sequence[MatchedCell]], Optional[float]],
    *,
    n_boot: int,
    seed: int,
) -> dict[str, Any]:
    """Resample whole sessions — cells inside a session are not independent."""
    by_session: dict[date, list[MatchedCell]] = defaultdict(list)
    for c in cells:
        by_session[c.trading_date].append(c)
    sessions = sorted(by_session)
    point = statistic(cells)
    if len(sessions) < 2 or point is None:
        return {"point": point, "ci_low": None, "ci_high": None, "n_sessions": len(sessions)}
    rng = np.random.default_rng(seed)
    draws: list[float] = []
    for _ in range(n_boot):
        pick = rng.integers(0, len(sessions), size=len(sessions))
        sample: list[MatchedCell] = []
        for i in pick:
            sample.extend(by_session[sessions[i]])
        value = statistic(sample)
        if value is not None and math.isfinite(value):
            draws.append(value)
    if not draws:
        return {"point": point, "ci_low": None, "ci_high": None, "n_sessions": len(sessions)}
    return {
        "point": point,
        "ci_low": float(np.percentile(draws, 2.5)),
        "ci_high": float(np.percentile(draws, 97.5)),
        "n_sessions": len(sessions),
        "n_boot": len(draws),
    }


def _sign_agreement_stat(cells: Sequence[MatchedCell]) -> Optional[float]:
    verdicts = [c.sign_agreement for c in cells if c.sign_agreement is not None]
    return (sum(verdicts) / len(verdicts)) if verdicts else None


def _pearson_stat(cells: Sequence[MatchedCell]) -> Optional[float]:
    if len(cells) < 3:
        return None
    return st.correlation([c.b_signed for c in cells], [c.c_signed for c in cells]).get("pearson")


# ---------------------------------------------------------------------------
# Stratification
# ---------------------------------------------------------------------------


def _strata(
    cells: Sequence[MatchedCell], config: AttributionConfig
) -> dict[str, dict[str, list[MatchedCell]]]:
    families: dict[str, dict[str, list[MatchedCell]]] = {
        "option_type": defaultdict(list),
        "dte": defaultdict(list),
        "moneyness": defaultdict(list),
        "time_of_day": defaultdict(list),
        "activity_size": defaultdict(list),
        "complexity": defaultdict(list),
    }
    for c in cells:
        families["option_type"]["call" if c.option_type == "C" else "put"].append(c)
        dte = c.dte()
        for label, lo, hi in config.dte_bins:
            if lo <= dte <= hi:
                families["dte"][label].append(c)
                break
        m = c.moneyness(config.atm_band)
        if m is not None:
            families["moneyness"][m].append(c)
        minute = c.session_minute()
        for label, lo, hi in config.time_of_day_bins:
            if lo <= minute < hi:
                families["time_of_day"][label].append(c)
                break
        for label, lo, hi in config.volume_bins:
            if lo <= c.c_gross < hi:
                families["activity_size"][label].append(c)
                break
        if c.complexity:
            families["complexity"][c.complexity].append(c)
    return {k: dict(v) for k, v in families.items()}


@dataclass
class AttributionResult:
    config: AttributionConfig
    diagnostics: MatchDiagnostics
    headline: dict[str, Any]
    sensitivity: dict[str, Any]
    strata: dict[str, Any]
    confidence: dict[str, Any]
    n_cells: int
    n_sessions: int
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def as_dict(self) -> dict[str, Any]:
        return {
            "config": {
                "min_attributed_activity": self.config.min_attributed_activity,
                "activity_sensitivity": list(self.config.activity_sensitivity),
                "volume_bins": [list(b) for b in self.config.volume_bins],
                "atm_band": self.config.atm_band,
                "dte_bins": [list(b) for b in self.config.dte_bins],
                "time_of_day_bins": [list(b) for b in self.config.time_of_day_bins],
                "min_stratum_n": self.config.min_stratum_n,
            },
            "diagnostics": self.diagnostics.as_dict(),
            "headline": self.headline,
            "sensitivity": self.sensitivity,
            "strata": self.strata,
            "confidence": self.confidence,
            "n_cells": self.n_cells,
            "n_sessions": self.n_sessions,
            "generated_at": self.generated_at,
            "terminology": (
                "Model B is Aggressor-Inferred MM activity (participant identity assumed). "
                "Model C is Exchange-Classified MM activity (participant identity tagged by "
                "the exchange). Neither is an observed dealer book."
            ),
        }


def compare_attribution(
    aggressor: Iterable[AggressorBucket],
    attributed: Iterable[ParticipantActivity],
    *,
    config: AttributionConfig = AttributionConfig(),
    spot_provider: Optional[Callable[[datetime], Optional[float]]] = None,
    gamma_provider: Optional[Callable[[datetime, SeriesKey], Optional[float]]] = None,
    interval: Optional[Interval] = None,
) -> tuple[AttributionResult, list[MatchedCell]]:
    """Run the whole comparison.  Returns the result and the matched cells."""
    cells, diag = match_cells(
        aggressor,
        attributed,
        spot_provider=spot_provider,
        gamma_provider=gamma_provider,
        interval=interval,
    )
    headline = attribution_metrics(cells, active_threshold=config.min_attributed_activity)
    sensitivity = {
        f"threshold_{t:g}": attribution_metrics(cells, active_threshold=t)["active_cells"]
        for t in config.activity_sensitivity
    }
    strata: dict[str, Any] = {}
    for family, groups in _strata(cells, config).items():
        if family == "complexity" and not diag.complexity_available:
            strata[family] = {"note": "not available in the supplied data"}
            continue
        strata[family] = {}
        for label, members in sorted(groups.items()):
            block = attribution_metrics(members, active_threshold=config.min_attributed_activity)
            block["insufficient"] = len(members) < config.min_stratum_n
            strata[family][label] = block
    active = [c for c in cells if c.c_gross >= config.min_attributed_activity]
    confidence = {
        "sign_agreement_active": _session_bootstrap(
            active, _sign_agreement_stat, n_boot=config.n_boot, seed=config.seed
        ),
        "pearson_active": _session_bootstrap(
            active, _pearson_stat, n_boot=config.n_boot, seed=config.seed
        ),
        "sign_agreement_all": _session_bootstrap(
            cells, _sign_agreement_stat, n_boot=config.n_boot, seed=config.seed
        ),
    }
    result = AttributionResult(
        config=config,
        diagnostics=diag,
        headline=headline,
        sensitivity=sensitivity,
        strata=strata,
        confidence=confidence,
        n_cells=len(cells),
        n_sessions=len({c.trading_date for c in cells}),
    )
    return result, cells


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "—"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:,}"
    return f"{float(value):,.{digits}f}"


def _pct(value: Any) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "—"
    return f"{float(value):.1%}"


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    if not rows:
        return "_(no rows)_\n"
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out) + "\n"


def _metric_rows(block: Mapping[str, Any]) -> list[list[str]]:
    return [
        ["Cells", _fmt(block.get("n"))],
        ["Cells with both sides non-zero", _fmt(block.get("n_both_nonzero"))],
        ["Cells with attributed MM activity = 0", _fmt(block.get("n_c_zero"))],
        ["Sign agreement (both non-zero)", _pct(block.get("sign_agreement_both_nonzero"))],
        [
            "Sign agreement excl. attributed-zero cells",
            _pct(block.get("sign_agreement_excluding_attributed_zero")),
        ],
        ["Sign agreement, zero as its own sign", _pct(block.get("sign_agreement_zero_as_sign"))],
        [
            "Weighted sign agreement (|C| × |$γ| weights)",
            _pct(block.get("weighted_sign_agreement_by_abs_c_gamma")),
        ],
        ["Pearson r (signed contracts)", _fmt(block.get("pearson"))],
        ["Spearman ρ", _fmt(block.get("spearman"))],
        ["Gamma-weighted Pearson r", _fmt(block.get("gamma_weighted_pearson"))],
        ["MAE (contracts)", _fmt(block.get("mae"), 2)],
        ["RMSE (contracts)", _fmt(block.get("rmse"), 2)],
        ["MAE / mean gross attributed activity", _fmt(block.get("normalized_mae"))],
        ["Bias, mean(B − C) (contracts)", _fmt(block.get("bias_mean_b_minus_c"), 2)],
        ["Gamma-weight coverage", _pct(block.get("gamma_weight_coverage"))],
    ]


def render_attribution_markdown(result: AttributionResult, *, synthetic: bool = False) -> str:
    """Plain-English report answering the attribution question, with the caveats attached."""
    d = result.diagnostics
    lines: list[str] = []
    lines.append("# Aggressor Assumption vs Exchange-Classified MM Activity — Attribution Report")
    lines.append("")
    lines.append(
        f"_Generated {result.generated_at[:19]}Z_ · research-only; "
        "no production metric was changed."
    )
    lines.append("")
    if synthetic:
        lines.append(
            "> **SYNTHETIC INPUTS.** This report was rendered from invented data to prove the "
            "plumbing. Nothing in it is evidence about the aggressor assumption."
        )
        lines.append("")
    lines.append("## The question")
    lines.append("")
    lines.append(
        "When ZeroGEX's tape classification says a print was **buyer-initiated**, the "
        "aggressor assumption says a customer bought and a market maker sold. This report "
        "measures how often that assumption reproduces the sign and size of "
        "**exchange-classified Market Maker activity** in the same option series over the "
        "same interval. It is a test of participant identification, not of trading value."
    )
    lines.append("")
    lines.append(
        "> Terminology: **Model B — Aggressor-Inferred MM activity** (participant identity "
        "*assumed* from the aggressor side). **Model C — Exchange-Classified MM activity** "
        "(participant identity *tagged by the exchange*, still a reconstruction of the "
        "market-maker population, not a dealer book). Neither is observed dealer inventory."
    )
    lines.append("")
    lines.append("## Data and coverage")
    lines.append("")
    lines.append(
        _table(
            ["Metric", "Value"],
            [
                ["Exchange interval", d.interval],
                ["Intraday identification testable", str(d.intraday_identification_testable)],
                ["Aggressor minute rows (B)", _fmt(d.b_rows)],
                ["Exchange MM records (C)", _fmt(d.c_mm_records)],
                ["Matched cells", _fmt(d.matched_cells)],
                ["  both sides present", _fmt(d.cells_both)],
                ["  B only (attributed zero)", _fmt(d.cells_b_only)],
                ["  C only (no tape volume)", _fmt(d.cells_c_only)],
                [
                    "Sessions matched / B-only / C-only",
                    f"{d.sessions_matched} / {d.sessions_b_only} / {d.sessions_c_only}",
                ],
                [
                    "Series-days matched / B-only / C-only",
                    f"{d.series_days_matched} / {d.series_days_b_only} / {d.series_days_c_only}",
                ],
                [
                    "C activity excluded (no B coverage)",
                    _fmt(d.c_activity_excluded_no_b_coverage, 0),
                ],
                [
                    "B activity excluded (no C coverage)",
                    _fmt(d.b_activity_excluded_no_c_coverage, 0),
                ],
                ["Cells with a gamma weight", _fmt(d.cells_with_gamma)],
                ["Cells with spot", _fmt(d.cells_with_spot)],
                ["Simple vs complex available", str(d.complexity_available)],
            ],
        )
    )
    for note in d.notes:
        lines.append(f"- ⚠ {note}")
    lines.append("")

    head = result.headline
    lines.append("## Headline agreement")
    lines.append("")
    lines.append(
        f"Active cells are those with gross attributed MM activity ≥ "
        f"**{result.config.min_attributed_activity:g}** contracts (predeclared; sensitivity below)."
    )
    lines.append("")
    lines.append("### All matched cells")
    lines.append("")
    lines.append(_table(["Metric", "Value"], _metric_rows(head.get("all_cells", {}))))
    lines.append("### Active cells")
    lines.append("")
    lines.append(_table(["Metric", "Value"], _metric_rows(head.get("active_cells", {}))))

    conf = result.confidence
    rows = []
    for label, key in (
        ("Sign agreement, active cells", "sign_agreement_active"),
        ("Pearson r, active cells", "pearson_active"),
        ("Sign agreement, all cells", "sign_agreement_all"),
    ):
        block = conf.get(key) or {}
        rows.append(
            [
                label,
                _fmt(block.get("point")),
                f"[{_fmt(block.get('ci_low'))}, {_fmt(block.get('ci_high'))}]",
                str(block.get("n_sessions", "—")),
            ]
        )
    lines.append("Session-block bootstrap intervals (sessions resampled, not cells):")
    lines.append("")
    lines.append(_table(["Statistic", "Point", "95% CI", "Sessions"], rows))

    lines.append("## Sensitivity to the activity floor")
    lines.append("")
    rows = [
        [
            label.replace("threshold_", ""),
            _fmt(block.get("n")),
            _pct(block.get("sign_agreement_both_nonzero")),
            _pct(block.get("weighted_sign_agreement_by_abs_c_gamma")),
            _fmt(block.get("pearson")),
            _fmt(block.get("normalized_mae")),
        ]
        for label, block in result.sensitivity.items()
    ]
    lines.append(
        _table(
            [
                "Floor (contracts)",
                "Cells",
                "Sign agreement",
                "Weighted sign agreement",
                "Pearson",
                "MAE / gross",
            ],
            rows,
        )
    )

    lines.append("## Stratified agreement (active cells)")
    lines.append("")
    for family, groups in result.strata.items():
        lines.append(f"### {family.replace('_', ' ')}")
        lines.append("")
        if "note" in groups:
            lines.append(f"_{groups['note']}_")
            lines.append("")
            continue
        rows = []
        for label, block in groups.items():
            act = block.get("active_cells", {})
            rows.append(
                [
                    label,
                    _fmt(act.get("n")),
                    _pct(act.get("sign_agreement_both_nonzero")),
                    _pct(act.get("weighted_sign_agreement_by_abs_c_gamma")),
                    _fmt(act.get("pearson")),
                    _fmt(act.get("bias_mean_b_minus_c"), 2),
                    "yes" if block.get("insufficient") else "",
                ]
            )
        lines.append(
            _table(
                [
                    "Stratum",
                    "Active cells",
                    "Sign agreement",
                    "Weighted",
                    "Pearson",
                    "Bias",
                    "Insufficient",
                ],
                rows,
            )
        )

    lines.append("## Reading this report")
    lines.append("")
    lines.append(
        "- **Sign agreement (both non-zero)** is the direct answer: the share of cells where "
        "the aggressor assumption and the exchange classification put the market-maker "
        "population on the same side.\n"
        "- **Weighted sign agreement** asks the same question where it matters for gamma: "
        "cells are weighted by attributed activity times per-contract dollar gamma.\n"
        "- **Bias** is signed: a persistently negative value means the assumption calls "
        "market makers sellers more often than the exchange does.\n"
        "- Cells with zero attributed activity are reported separately so that a tape full "
        "of quiet buckets cannot masquerade as accuracy.\n"
        "- A session-summary feed can only test end-of-day agreement; it says nothing about "
        "intraday identification, and the coverage table says which case this is.\n"
        "- The comparison identifies the C1-tagged market-maker population; a market maker "
        "trading through another firm's booking is not in C, and a print ZeroGEX did not "
        "quote is not in B. Both are coverage limits, not evidence either way."
    )
    lines.append("")
    return "\n".join(lines)


def write_matched_cells_csv(cells: Sequence[MatchedCell], path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fields = list(cells[0].as_dict().keys()) if cells else ["bucket_end"]
    with p.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for c in cells:
            writer.writerow(c.as_dict())
    return p


def write_attribution_report(
    result: AttributionResult,
    cells: Sequence[MatchedCell],
    path: str | Path,
    *,
    synthetic: bool = False,
) -> Path:
    """Markdown + JSON + matched-cell CSV next to each other."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(render_attribution_markdown(result, synthetic=synthetic), encoding="utf-8")
    payload = result.as_dict()
    payload["synthetic"] = synthetic
    p.with_suffix(".json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    write_matched_cells_csv(cells, p.with_name(p.stem + "_cells.csv"))
    return p
