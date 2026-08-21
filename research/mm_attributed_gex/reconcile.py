"""Independent cross-checks on the reconstructed inventory.

An inventory reconstruction can be internally consistent and still wrong.  The
checks here are *external*: they test the reconstruction against facts ZeroGEX
already stores, so a broken column mapping or a misread position-effect flag
shows up as a number rather than as a plausible-looking research result.

Three checks, strongest first.

**Open-interest identity.**  Every contract opened by one participant is opened
against a counterparty who is also opening or closing.  Across *all*
participants, in one session::

    ΔOI  =  Σ_participants (opening_buys + opening_sells)/2
          − Σ_participants (closing_buys + closing_sells)/2

The halving is the point: one contract opened creates one unit of open
interest but appears twice in the feed — once as somebody's buy, once as
somebody's sell.  ZeroGEX stores end-of-session open interest per contract in
``option_chains``, so this is directly testable and it validates the *whole*
file: the column mapping, the participant coverage, the open/close semantics
and the units all have to be right for it to hold.

**Zero-sum check.**  Options are in zero net supply.  Summing net position
across every participant category must give zero for each series.  A non-zero
residual measures how much of the market the feed's categories fail to cover
(or a sign error in one of them).

**Volume tie-out.**  Total classified contracts should reconcile against the
contract volume ZeroGEX already ingests.  Weaker than the other two — the feeds
have different session boundaries and inclusion rules — so it is reported as a
ratio to eyeball, not as a pass/fail.

Everything here is diagnostic.  Nothing corrects the inventory; a
reconstruction that fails these checks should be reported as failing, not
silently repaired.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable, Mapping, Optional, Sequence

from research.mm_attributed_gex.schema import (
    ParticipantActivity,
    ParticipantType,
    PositionEffect,
    SeriesKey,
    Side,
)

__all__ = [
    "SeriesOiCheck",
    "ReconciliationReport",
    "reconcile_open_interest",
    "zero_sum_residual",
    "summarize",
]


@dataclass
class SeriesOiCheck:
    """Open-interest identity for one series on one session."""

    key: SeriesKey
    session: date
    implied_delta_oi: float
    observed_delta_oi: Optional[float]
    opening_contracts: float
    closing_contracts: float

    @property
    def error(self) -> Optional[float]:
        if self.observed_delta_oi is None:
            return None
        return self.implied_delta_oi - self.observed_delta_oi

    @property
    def relative_error(self) -> Optional[float]:
        err = self.error
        if err is None:
            return None
        scale = max(abs(self.observed_delta_oi or 0.0), self.opening_contracts, 1.0)
        return err / scale


@dataclass
class ReconciliationReport:
    """Aggregate verdict over a reconciliation pass."""

    checks: list[SeriesOiCheck] = field(default_factory=list)
    n_checked: int = 0
    n_matched: int = 0
    n_missing_oi: int = 0
    median_abs_relative_error: Optional[float] = None
    mean_abs_relative_error: Optional[float] = None
    p90_abs_relative_error: Optional[float] = None
    zero_sum_residual_contracts: float = 0.0
    zero_sum_residual_share: float = 0.0
    participants_covered: tuple[str, ...] = ()
    verdict: str = "not_run"
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "n_checked": self.n_checked,
            "n_matched": self.n_matched,
            "n_missing_oi": self.n_missing_oi,
            "match_rate": (self.n_matched / self.n_checked) if self.n_checked else None,
            "median_abs_relative_error": self.median_abs_relative_error,
            "mean_abs_relative_error": self.mean_abs_relative_error,
            "p90_abs_relative_error": self.p90_abs_relative_error,
            "zero_sum_residual_contracts": self.zero_sum_residual_contracts,
            "zero_sum_residual_share": self.zero_sum_residual_share,
            "participants_covered": list(self.participants_covered),
            "verdict": self.verdict,
            "notes": self.notes,
        }


def _percentile(sorted_values: Sequence[float], q: float) -> Optional[float]:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = q * (len(sorted_values) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def reconcile_open_interest(
    records: Iterable[ParticipantActivity],
    observed_oi: Mapping[tuple[SeriesKey, date], int],
    *,
    tolerance: float = 0.05,
    min_activity: int = 10,
) -> ReconciliationReport:
    """Test the open-interest identity per (series, session).

    :param records: **all** participant records for the window, not just MM —
        the identity only closes when every category is included.
    :param observed_oi: end-of-session OI per (series, session), from
        :func:`research.mm_attributed_gex.sources.fetch_open_interest_series`.
    :param tolerance: relative error under which a series counts as matched.
    :param min_activity: series/sessions with less opening volume than this are
        skipped — a one-contract session's relative error is dominated by
        rounding and would drown the signal.
    """
    report = ReconciliationReport()

    # (series, session) -> [opening_contracts, closing_contracts]
    flow: dict[tuple[SeriesKey, date], list[float]] = defaultdict(lambda: [0.0, 0.0])
    participants: set[str] = set()
    for rec in records:
        participants.add(rec.participant.value)
        if rec.position_effect is PositionEffect.OPEN:
            flow[(rec.key, rec.trading_date)][0] += rec.contracts
        elif rec.position_effect is PositionEffect.CLOSE:
            flow[(rec.key, rec.trading_date)][1] += rec.contracts
    report.participants_covered = tuple(sorted(participants))

    if not flow:
        report.verdict = "no_open_close_records"
        report.notes.append(
            "no records carried an OPEN or CLOSE position effect; the "
            "open-interest identity cannot be evaluated"
        )
        return report

    # Prior-session OI per series, so ΔOI can be formed.
    by_series: dict[SeriesKey, list[tuple[date, int]]] = defaultdict(list)
    for (key, session), oi in observed_oi.items():
        by_series[key].append((session, oi))
    for values in by_series.values():
        values.sort()

    rel_errors: list[float] = []
    for (key, session), (opening, closing) in sorted(
        flow.items(), key=lambda kv: (kv[0][1], kv[0][0])
    ):
        if opening < min_activity:
            continue
        report.n_checked += 1
        implied = (opening - closing) / 2.0

        history = by_series.get(key, ())
        observed: Optional[float] = None
        prev_oi: Optional[int] = None
        this_oi: Optional[int] = None
        for d, oi in history:
            if d < session:
                prev_oi = oi
            elif d == session:
                this_oi = oi
                break
        if this_oi is not None and prev_oi is not None:
            observed = float(this_oi - prev_oi)

        check = SeriesOiCheck(
            key=key,
            session=session,
            implied_delta_oi=implied,
            observed_delta_oi=observed,
            opening_contracts=opening,
            closing_contracts=closing,
        )
        report.checks.append(check)
        rel = check.relative_error
        if rel is None:
            report.n_missing_oi += 1
            continue
        rel_errors.append(abs(rel))
        if abs(rel) <= tolerance:
            report.n_matched += 1

    if rel_errors:
        rel_errors.sort()
        report.median_abs_relative_error = _percentile(rel_errors, 0.5)
        report.mean_abs_relative_error = sum(rel_errors) / len(rel_errors)
        report.p90_abs_relative_error = _percentile(rel_errors, 0.9)
        match_rate = report.n_matched / max(1, report.n_checked - report.n_missing_oi)
        if match_rate >= 0.8:
            report.verdict = "consistent"
        elif match_rate >= 0.5:
            report.verdict = "partially_consistent"
        else:
            report.verdict = "inconsistent"
            report.notes.append(
                f"only {match_rate:.0%} of series/sessions reconcile to observed ΔOI "
                f"within {tolerance:.0%} — suspect the column mapping, the "
                "participant coverage, or the open/close semantics before "
                "interpreting any MM-attributed result"
            )
    else:
        report.verdict = "no_overlapping_oi"
        report.notes.append(
            "no (series, session) had both classified flow and two consecutive "
            "days of stored open interest; widen the chain history or the "
            "Open-Close window so they overlap"
        )
    return report


def zero_sum_residual(
    records: Iterable[ParticipantActivity],
) -> tuple[dict[SeriesKey, float], float, float]:
    """Net position summed across all participants, which should be zero.

    Returns ``(residual_by_series, total_abs_residual, residual_share)``.
    ``residual_share`` normalizes by total classified volume, so it reads as
    "this fraction of the market is unaccounted for by the reported
    categories".
    """
    residual: dict[SeriesKey, float] = defaultdict(float)
    total_volume = 0.0
    for rec in records:
        total_volume += rec.contracts
        signed = rec.contracts if rec.side is Side.BUY else -rec.contracts
        residual[rec.key] += signed
    total_abs = sum(abs(v) for v in residual.values())
    share = (total_abs / total_volume) if total_volume > 0 else 0.0
    return dict(residual), total_abs, share


def summarize(
    records: Sequence[ParticipantActivity],
    observed_oi: Optional[Mapping[tuple[SeriesKey, date], int]] = None,
    *,
    tolerance: float = 0.05,
) -> ReconciliationReport:
    """Run every available check and return one report."""
    report = (
        reconcile_open_interest(records, observed_oi, tolerance=tolerance)
        if observed_oi
        else ReconciliationReport(verdict="no_open_interest_supplied")
    )
    _, total_abs, share = zero_sum_residual(records)
    report.zero_sum_residual_contracts = total_abs
    report.zero_sum_residual_share = share
    if share > 0.02:
        report.notes.append(
            f"participant net positions do not sum to zero: residual is "
            f"{share:.1%} of classified volume. Either a participant category "
            "is missing from the file, or buy/sell is mis-mapped for one of them"
        )
    if ParticipantType.MARKET_MAKER.value not in report.participants_covered and records:
        report.notes.append("no MARKET_MAKER records present — check the column mapping")
    return report
