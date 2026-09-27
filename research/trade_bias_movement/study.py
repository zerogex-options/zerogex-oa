"""Compare how far price travels after each state with comparable minutes.

For each state, a least-squares fit of the log forward range on what a
trader can already see -- the symbol, the time of day (5-minute slots through
the first half hour, half hours after), how big the one-minute bars have been
over the last 5, 15 and 60 minutes, and how far price swung over the last 30
minutes and the session so far -- plus one indicator for "the panel shows
this state". The
indicator's coefficient, exponentiated, is the state's **ratio**: how far
price typically travelled after it, as a multiple of what other minutes with
the same clock and the same recent movement travelled. 1.00 means the state
adds nothing to the clock and the tape.

The rows, the outcome windows and the date-block bootstrap are those of
``research.short_gamma_trend``. Each date is reduced to its own ``X'X`` and
``X'y``, so a resample re-fits the whole model with one weighted sum and one
small solve, and the adjustment's own uncertainty is in every interval.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Iterable, Optional, Sequence

import numpy as np

from research.msi_regime_excursion.excursion import ET
from research.short_gamma_trend.outcomes import RTH_OPEN, Horizon, SessionBars
from research.short_gamma_trend.rule import votes
from research.short_gamma_trend.sources import Reading
from research.short_gamma_trend.study import (
    ALL_HORIZONS,
    BuildCounts,
    Estimate,
    Row,
    build_rows,
)
from research.trade_bias_inputs.study import holm

# Pre-registered in README.md. Changing any of these after reading a result
# makes the verdicts meaningless.
PRIMARY_HORIZON = 30
CONFIRM_HORIZON = 60
MIN_SESSIONS = 15
MIN_MINUTES = 300
MATERIAL = 0.10
ALPHA = 0.05
ITERATIONS = 2000
SEED = 20260928

#: Recent movement, in bars ending at entry: the average one-minute bar over
#: these windows, and the high-to-low swing over the last ``SWING_WINDOW``
#: bars and the session so far. The average bar is the steadier gauge of how
#: busy the market is; the swing is what a chart shows at a glance.
BAR_WINDOWS = (5, 15, 60)
SWING_WINDOW = 30
N_RECENT = len(BAR_WINDOWS) + 2
#: Index of the 30-minute swing in ``MoveRow.recent``.
SWING_INDEX = len(BAR_WINDOWS)
#: Ranges are floored here before their log is taken.
FLOOR_BPS = 0.1
#: Time of day: 5-minute slots through the first half hour, where movement
#: decays fastest, then half hours to the close.
OPEN_SLOT_MIN = 5
OPEN_SLOTS = 6
HALF_HOUR_MIN = 30
N_SLOTS = OPEN_SLOTS + 12
N_FIFTHS = 5

MOVES_MORE = "MOVES MORE"
MOVES_LESS = "MOVES LESS"
SLIGHTLY_MORE = "SLIGHTLY MORE"
SLIGHTLY_LESS = "SLIGHTLY LESS"
NOTHING = "NOTHING DETECTABLE"
TOO_RARE = "TOO RARE"

MORE = "more"
LESS = "less"

#: What a state is compared against: other minutes of the same symbol, of
#: the same time of day too, or of the same time of day after the same recent
#: movement (the verdict's basis).
BY_SYMBOL = "symbol"
BY_TIME_OF_DAY = "time_of_day"
COMPARABLE = "comparable"


@dataclass(frozen=True)
class Group:
    key: str
    title: str
    states: tuple[str, ...]
    #: What the panel's copy claims about movement, or ``None``.
    claim: Optional[str]


GROUPS: tuple[Group, ...] = (
    Group("trend", "Trend Up / Down", ("TREND_UP", "TREND_DOWN"), None),
    Group("trap", "Trap Squeeze / Reversal", ("TRAP_SQUEEZE", "TRAP_REVERSAL"), MORE),
    Group("chop", "Chop (Range-Bound)", ("CHOP",), LESS),
    Group("awaiting", "Awaiting confluence", ("UNKNOWN",), LESS),
)
GROUPS_BY_KEY = {g.key: g for g in GROUPS}

STATE_TITLES = {
    "TREND_UP": "Trend Up",
    "TREND_DOWN": "Trend Down",
    "TRAP_SQUEEZE": "Trap Squeeze",
    "TRAP_REVERSAL": "Trap Reversal",
    "CHOP": "Chop",
    "UNKNOWN": "Awaiting confluence",
}


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


@dataclass
class MoveRow:
    row: Row
    #: In bps of the entry price, each window ending at the entry bar and
    #: never reaching back past the session's open: the average one-minute
    #: bar (high minus low) over the last 5, 15 and 60 bars, then the high
    #: minus the low of the last 30 bars and of the session so far.
    recent: tuple[float, ...]


class RecentMovement:
    """How far price had moved up to each bar, within its own session."""

    def __init__(self, bars: SessionBars) -> None:
        self.bars = bars.series.bars
        n = len(self.bars)
        self.session_start = [0] * n
        self.high_so_far = [0.0] * n
        self.low_so_far = [0.0] * n
        #: Running sum of one-minute bar ranges within the session.
        self.bar_sum = [0.0] * n
        for i, bar in enumerate(self.bars):
            same = i > 0 and (
                self.bars[i - 1].ts.astimezone(ET).date() == bar.ts.astimezone(ET).date()
            )
            if same:
                self.session_start[i] = self.session_start[i - 1]
                self.high_so_far[i] = max(self.high_so_far[i - 1], bar.high)
                self.low_so_far[i] = min(self.low_so_far[i - 1], bar.low)
                self.bar_sum[i] = self.bar_sum[i - 1] + (bar.high - bar.low)
            else:
                self.session_start[i] = i
                self.high_so_far[i] = bar.high
                self.low_so_far[i] = bar.low
                self.bar_sum[i] = bar.high - bar.low

    def at(self, idx: int) -> tuple[float, ...]:
        entry = self.bars[idx]
        start = self.session_start[idx]
        scale = 10_000.0 / entry.close

        def average_bar(n_bars: int) -> float:
            first = max(start, idx - n_bars + 1)
            before = self.bar_sum[first - 1] if first > start else 0.0
            return scale * (self.bar_sum[idx] - before) / (idx - first + 1)

        window = self.bars[max(start, idx - SWING_WINDOW + 1) : idx + 1]
        swing = scale * (max(b.high for b in window) - min(b.low for b in window))
        session = scale * (self.high_so_far[idx] - self.low_so_far[idx])
        return (*(average_bar(n) for n in BAR_WINDOWS), swing, session)


def build(
    symbol: str, readings: Iterable[Reading], bars: SessionBars
) -> tuple[list[MoveRow], BuildCounts]:
    """``research.short_gamma_trend.study.build_rows``, plus each row's recent movement."""
    rows, counts = build_rows(symbol, readings, bars)
    recent = RecentMovement(bars)
    out = []
    for r in rows:
        # build_rows only keeps rows with an entry bar, found the same way.
        idx = bars.series.index_at_or_before(r.out.entry_ts)
        assert idx is not None
        out.append(MoveRow(r, recent.at(idx)))
    return out, counts


def time_slot(ts: datetime) -> int:
    """0-5 for 09:30-09:34 ... 09:55-09:59 ET, then 6 for 10:00-10:29 ... 17
    for 15:30-15:59."""
    local = ts.astimezone(ET)
    minutes = local.hour * 60 + local.minute - (RTH_OPEN.hour * 60 + RTH_OPEN.minute)
    if minutes < OPEN_SLOTS * OPEN_SLOT_MIN:
        return max(0, minutes // OPEN_SLOT_MIN)
    return min(N_SLOTS - 1, OPEN_SLOTS + (minutes - OPEN_SLOTS * OPEN_SLOT_MIN) // HALF_HOUR_MIN)


# ---------------------------------------------------------------------------
# The frame and the fit
# ---------------------------------------------------------------------------

#: Bootstrap resamples fitted at once.
_BATCH = 250


class MoveFrame:
    """The scored rows as arrays, and one fixed set of bootstrap draws."""

    def __init__(
        self,
        rows: Sequence[MoveRow],
        symbols: Sequence[str],
        *,
        iterations: int = ITERATIONS,
        seed: int = SEED,
    ) -> None:
        self.rows = sorted(rows, key=lambda m: (m.row.symbol, m.row.ts))
        self.symbols = list(symbols)
        self.dates = sorted({m.row.session for m in self.rows})
        self.n_dates = len(self.dates)
        self.n_syms = len(self.symbols)
        d_index = {d: i for i, d in enumerate(self.dates)}
        s_index = {s: i for i, s in enumerate(self.symbols)}
        self.date_idx = np.array([d_index[m.row.session] for m in self.rows], dtype=np.int64)
        self.sym_idx = np.array([s_index[m.row.symbol] for m in self.rows], dtype=np.int64)
        self.slot = np.array([time_slot(m.row.out.entry_ts) for m in self.rows], dtype=np.int64)
        recent = np.array([m.recent for m in self.rows], dtype=float).reshape(-1, N_RECENT)
        self.log_recent = np.log(np.maximum(recent, FLOOR_BPS))
        self.fifth = self._fifths(recent[:, SWING_INDEX])
        self.states = np.array([m.row.states["current"] for m in self.rows], dtype=object)
        self.range: dict[Horizon, np.ndarray] = {
            h: np.array(
                [np.nan if (v := m.row.out.range.get(h)) is None else float(v) for m in self.rows],
                dtype=float,
            )
            for h in ALL_HORIZONS
        }
        if self.n_dates:
            gen = np.random.default_rng(seed)
            self.draws = gen.multinomial(
                self.n_dates, np.full(self.n_dates, 1.0 / self.n_dates), size=iterations
            ).astype(float)
        else:
            self.draws = np.zeros((0, 0))
        self._designs: dict[tuple[Horizon, str, str], Design] = {}

    def _fifths(self, last_30: np.ndarray) -> np.ndarray:
        """0-4: the fifth of the last 30 minutes' range within its (symbol,
        half hour); -1 in the first half hour, where the window is short."""
        out = np.full(len(self.rows), -1, dtype=np.int64)
        for sym in range(self.n_syms):
            for b in range(OPEN_SLOTS, N_SLOTS):
                cell = (self.sym_idx == sym) & (self.slot == b)
                if cell.any():
                    cuts = np.quantile(last_30[cell], [0.2, 0.4, 0.6, 0.8])
                    out[cell] = np.searchsorted(cuts, last_30[cell], side="right")
        return out

    def design(self, horizon: Horizon, basis: str, scope: str = "pooled") -> "Design":
        key = (horizon, basis, scope)
        if key not in self._designs:
            self._designs[key] = Design(self, horizon, basis, scope)
        return self._designs[key]

    def state_mask(self, states: Iterable[str]) -> np.ndarray:
        wanted = set(states)
        return np.array([s in wanted for s in self.states], dtype=bool)


class Design:
    """log(forward range) on the basis columns, for one horizon and scope,
    reduced to per-date ``X'X`` and ``X'y``."""

    def __init__(self, frame: MoveFrame, horizon: Horizon, basis: str, scope: str) -> None:
        y = frame.range[horizon]
        keep = np.isfinite(y)
        if scope != "pooled":
            keep &= frame.sym_idx == frame.symbols.index(scope)
        self.keep = keep
        idx = np.flatnonzero(keep)
        level = frame.sym_idx[idx]
        if basis != BY_SYMBOL:
            level = level * N_SLOTS + frame.slot[idx]
        columns = [(level[:, None] == np.unique(level)[None, :]).astype(float)]
        if basis == COMPARABLE:
            columns.append(frame.log_recent[idx])
        self.x = np.hstack(columns) if idx.size else np.zeros((0, 1))
        self.ly = np.log(np.maximum(y[idx], FLOOR_BPS))
        self.date = frame.date_idx[idx]
        n_dates, p = frame.n_dates, self.x.shape[1]
        self.gram = np.zeros((n_dates, p, p))
        self.xty = np.zeros((n_dates, p))
        for d in range(n_dates):
            sel = self.date == d
            xd = self.x[sel]
            self.gram[d] = xd.T @ xd
            self.xty[d] = xd.T @ self.ly[sel]
        self.rows_per_date = np.bincount(self.date, minlength=n_dates).astype(float)
        self.draws = frame.draws

    def fit(self, mask: np.ndarray) -> tuple[Any, np.ndarray, np.ndarray]:
        """``(point, bootstrap, per-date count)`` of the state coefficient."""
        m = mask[self.keep]
        n_dates, p = self.gram.shape[0], self.gram.shape[1]
        cross = np.zeros((n_dates, p))
        np.add.at(cross, self.date[m], self.x[m])
        count = np.bincount(self.date[m], minlength=n_dates).astype(float)
        total = np.bincount(self.date[m], weights=self.ly[m], minlength=n_dates)
        point = _coefficient(
            self.gram.sum(axis=0),
            cross.sum(axis=0),
            count.sum(),
            self.xty.sum(axis=0),
            total.sum(),
            float(self.keep.sum()),
        )
        boot = np.concatenate(
            [np.zeros(0)]
            + [
                _coefficient(
                    (w @ self.gram.reshape(n_dates, p * p)).reshape(-1, p, p),
                    w @ cross,
                    w @ count,
                    w @ self.xty,
                    w @ total,
                    w @ self.rows_per_date,
                )
                # A few hundred resamples at a time keeps memory flat.
                for w in np.array_split(self.draws, -(-len(self.draws) // _BATCH))
                if w.size
            ]
        )
        return point, boot, count


def _coefficient(
    gram: np.ndarray,
    cross: np.ndarray,
    count: Any,
    xty: np.ndarray,
    total: Any,
    n_all: Any,
) -> Any:
    """The state indicator's least-squares coefficient, by partialling out.

    With ``X`` the adjustment columns and ``d`` the indicator, the coefficient
    is ``d'My / d'Md`` for ``M`` the residual maker of ``X``: what is left of
    the state after the clock and the tape, set against what is left of the
    range. It is ``nan`` where nothing is left of the state -- no minutes, all
    minutes, or minutes that fill whole time slots and appear nowhere else --
    because then the data cannot say what the state adds.
    """
    gram, cross, xty = np.asarray(gram), np.asarray(cross), np.asarray(xty)
    count = np.asarray(count, float)
    total = np.asarray(total, float)
    n_all = np.asarray(n_all, float)
    # A time slot a resample happened to miss is an all-zero column; a 1 on
    # its diagonal makes the system solvable and changes nothing else.
    empty = np.diagonal(gram, axis1=-2, axis2=-1) == 0
    gram = gram + empty[..., None] * np.eye(gram.shape[-1])
    solved = np.linalg.solve(gram, np.stack([cross, xty], axis=-1))
    left_of_state = count - np.einsum("...p,...p->...", cross, solved[..., 0])
    left_of_range = total - np.einsum("...p,...p->...", cross, solved[..., 1])
    ok = (count > 0) & (count < n_all) & (left_of_state > 1e-8 * np.maximum(count, 1.0))
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(ok, left_of_range / np.where(ok, left_of_state, 1.0), np.nan)


def _finite(x: Any) -> Optional[float]:
    return float(x) if x is not None and math.isfinite(float(x)) else None


def _interval(coef: Any, boot: np.ndarray) -> Estimate:
    """The ratio exp(coefficient), its percentile interval, and a two-sided
    bootstrap p for a ratio of 1."""
    value = _finite(coef)
    boot = boot[np.isfinite(boot)]
    if value is None:
        return Estimate(None)
    if boot.size < 20:
        return Estimate(math.exp(value))
    lo, hi = np.percentile(boot, [2.5, 97.5])
    tail = np.count_nonzero(boot <= 0.0) if value >= 0.0 else np.count_nonzero(boot >= 0.0)
    p = max(min(1.0, 2.0 * tail / boot.size), 1.0 / boot.size)
    return Estimate(math.exp(value), math.exp(lo), math.exp(hi), p)


@dataclass
class RatioCell:
    basis: str
    horizon: Horizon
    scope: str
    #: The state's minutes scored at this horizon, and the sessions they span.
    minutes: int
    sessions: int
    ratio: Estimate


def score(
    frame: MoveFrame,
    mask: np.ndarray,
    horizon: Horizon = PRIMARY_HORIZON,
    basis: str = COMPARABLE,
    scope: str = "pooled",
) -> RatioCell:
    """The ratio for the minutes in ``mask`` against every other minute."""
    point, boot, count = frame.design(horizon, basis, scope).fit(mask)
    return RatioCell(
        basis=basis,
        horizon=horizon,
        scope=scope,
        minutes=int(count.sum()),
        sessions=int(np.count_nonzero(count > 0)),
        ratio=_interval(point, boot),
    )


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------


@dataclass
class Verdict:
    decision: str
    reasons: list[str]


CellKey = tuple[str, Horizon, str]


@dataclass
class GroupResult:
    key: str
    title: str
    claim: Optional[str]
    #: Share of scored minutes in the state, per symbol.
    share: dict[str, float]
    #: ``(basis, horizon, scope)`` -> cell.
    cells: dict[CellKey, RatioCell]
    p_primary: Optional[float] = None
    significant: bool = False
    verdict: Optional[Verdict] = None

    def primary(self) -> RatioCell:
        return self.cells[(COMPARABLE, PRIMARY_HORIZON, "pooled")]

    def too_rare(self) -> bool:
        c = self.primary()
        return c.sessions < MIN_SESSIONS or c.minutes < MIN_MINUTES or c.ratio.value is None


def decide(result: GroupResult, symbols: Sequence[str]) -> Verdict:
    """Apply the pre-registered rule in README.md. Nothing else moves it."""
    primary = result.primary()
    if result.too_rare():
        if primary.ratio.value is None and primary.minutes >= MIN_MINUTES:
            why = (
                f"{primary.minutes:,} minutes, but they cannot be told apart from the time of "
                "day and recent movement, so there is no ratio to judge"
            )
        else:
            why = (
                f"{primary.minutes:,} minutes over {primary.sessions} sessions scored at "
                f"{PRIMARY_HORIZON} minutes; the rule needs {MIN_MINUTES} and {MIN_SESSIONS}"
            )
        return Verdict(TOO_RARE, [why])
    r = primary.ratio
    assert r.value is not None
    per_symbol = {s: result.cells[(COMPARABLE, PRIMARY_HORIZON, s)].ratio.value for s in symbols}
    confirm = result.cells[(COMPARABLE, CONFIRM_HORIZON, "pooled")].ratio.value
    interval = f", 95% [{r.lo:.2f}, {r.hi:.2f}]" if r.lo is not None and r.hi is not None else ""
    p_text = f"p {result.p_primary:.4f}" if result.p_primary is not None else "p n/a"
    reasons = [
        f"pooled {PRIMARY_HORIZON}m range ratio {r.value:.2f}{interval}; {p_text}, "
        + ("significant" if result.significant else "not significant")
        + f" after Holm's correction at {ALPHA:.0%} across the four",
        "per symbol: "
        + ", ".join(f"{s} {v:.2f}" if v is not None else f"{s} n/a" for s, v in per_symbol.items()),
        f"pooled {CONFIRM_HORIZON}m ratio " + (f"{confirm:.2f}" if confirm is not None else "n/a"),
    ]
    values = list(per_symbol.values())
    if result.significant and confirm is not None:
        if r.value > 1 and confirm > 1 and all(v is not None and v > 1 for v in values):
            return Verdict(MOVES_MORE if r.value >= 1 + MATERIAL else SLIGHTLY_MORE, reasons)
        if r.value < 1 and confirm < 1 and all(v is not None and v < 1 for v in values):
            return Verdict(MOVES_LESS if r.value <= 1 - MATERIAL else SLIGHTLY_LESS, reasons)
    return Verdict(NOTHING, reasons)


def apply_verdicts(results: Sequence[GroupResult], symbols: Sequence[str]) -> None:
    """Holm's correction across all of ``results`` (a rare one as p = 1), then
    each verdict."""
    p_values: list[float] = []
    for r in results:
        p = None if r.too_rare() else r.primary().ratio.p
        r.p_primary = p
        p_values.append(1.0 if p is None else p)
    for r, flag in zip(results, holm(p_values, ALPHA)):
        r.significant = bool(flag) and r.p_primary is not None
        r.verdict = decide(r, symbols)


def matches_copy(claim: Optional[str], decision: str) -> str:
    """How a verdict sits against what the panel tells customers."""
    if claim is None:
        return "no claim"
    if decision == TOO_RARE:
        return "can't tell"
    agrees = {MORE: (MOVES_MORE, SLIGHTLY_MORE), LESS: (MOVES_LESS, SLIGHTLY_LESS)}[claim]
    opposite = {MORE: (MOVES_LESS, SLIGHTLY_LESS), LESS: (MOVES_MORE, SLIGHTLY_MORE)}[claim]
    if decision == agrees[0]:
        return "yes"
    if decision == agrees[1]:
        return "only slightly"
    if decision in opposite:
        return "opposite"
    return "no"


# ---------------------------------------------------------------------------
# The whole study
# ---------------------------------------------------------------------------


@dataclass
class ContextRow:
    title: str
    cells: dict[str, RatioCell]


@dataclass
class StudyResult:
    symbols: list[str]
    n_dates: int
    n_rows: int
    groups: list[GroupResult]
    #: Each state on its own, 30 minutes, "symbol" and "comparable" bases.
    states: list[ContextRow]
    #: Production's gamma regime, 30 minutes, "symbol" and "comparable" bases.
    regimes: list[ContextRow]
    #: The first half hour against the rest of the day, and each fifth of the
    #: last 30 minutes' range against the other fifths at the same half hour.
    recent: list[ContextRow]

    def verdicts(self) -> dict[str, str]:
        return {g.key: g.verdict.decision if g.verdict else "" for g in self.groups}

    def as_dict(self) -> dict:
        def cells(d: dict) -> dict:
            return {
                "|".join(str(p) for p in k) if isinstance(k, tuple) else k: asdict(v)
                for k, v in d.items()
            }

        def context(rows: list[ContextRow]) -> list[dict]:
            return [{"title": r.title, "cells": cells(r.cells)} for r in rows]

        return {
            "symbols": self.symbols,
            "n_dates": self.n_dates,
            "n_rows": self.n_rows,
            "groups": [
                {
                    "key": g.key,
                    "title": g.title,
                    "claim": g.claim,
                    "verdict": asdict(g.verdict) if g.verdict else None,
                    "matches_copy": (
                        matches_copy(g.claim, g.verdict.decision) if g.verdict else None
                    ),
                    "p_primary": g.p_primary,
                    "significant": g.significant,
                    "share": g.share,
                    "cells": cells(g.cells),
                }
                for g in self.groups
            ],
            "states": context(self.states),
            "regimes": context(self.regimes),
            "recent": context(self.recent),
        }


def _shares(frame: MoveFrame, mask: np.ndarray) -> dict[str, float]:
    out = {}
    for i, sym in enumerate(frame.symbols):
        in_sym = frame.sym_idx == i
        n = int(np.count_nonzero(in_sym))
        out[sym] = float(np.count_nonzero(mask & in_sym)) / n if n else 0.0
    return out


def score_group(frame: MoveFrame, group: Group) -> GroupResult:
    mask = frame.state_mask(group.states)
    cells: dict[CellKey, RatioCell] = {}
    for h in ALL_HORIZONS:
        cells[(COMPARABLE, h, "pooled")] = score(frame, mask, h)
    for sym in frame.symbols:
        cells[(COMPARABLE, PRIMARY_HORIZON, sym)] = score(frame, mask, scope=sym)
    for basis in (BY_SYMBOL, BY_TIME_OF_DAY):
        cells[(basis, PRIMARY_HORIZON, "pooled")] = score(frame, mask, basis=basis)
    return GroupResult(group.key, group.title, group.claim, _shares(frame, mask), cells)


def _context(frame: MoveFrame, title: str, mask: np.ndarray, bases: Sequence[str]) -> ContextRow:
    return ContextRow(title, {basis: score(frame, mask, basis=basis) for basis in bases})


def regime_masks(frame: MoveFrame) -> dict[str, np.ndarray]:
    """Production's short gamma, long gamma, and neither."""
    short = np.zeros(len(frame.rows), dtype=bool)
    long_ = np.zeros(len(frame.rows), dtype=bool)
    for i, m in enumerate(frame.rows):
        if m.row.inputs is None:
            continue
        v = votes(m.row.inputs)
        short[i], long_[i] = v.is_short_gamma, v.is_long_gamma
    return {"short gamma": short, "long gamma": long_, "neither": ~(short | long_)}


def run_study(
    rows: Sequence[MoveRow],
    symbols: Sequence[str],
    *,
    iterations: int = ITERATIONS,
    seed: int = SEED,
) -> StudyResult:
    frame = MoveFrame(rows, symbols, iterations=iterations, seed=seed)
    groups = [score_group(frame, g) for g in GROUPS]
    apply_verdicts(groups, frame.symbols)
    both = (BY_SYMBOL, COMPARABLE)
    states = [
        _context(frame, title, frame.state_mask((state,)), both)
        for state, title in STATE_TITLES.items()
    ]
    regimes = [_context(frame, name, m, both) for name, m in regime_masks(frame).items()]
    recent = [_context(frame, "first half hour", frame.slot < OPEN_SLOTS, (BY_SYMBOL,))]
    for k, label in enumerate(("quietest", "2nd", "middle", "4th", "busiest")):
        recent.append(_context(frame, f"{label} fifth", frame.fifth == k, (BY_TIME_OF_DAY,)))
    return StudyResult(
        symbols=list(frame.symbols),
        n_dates=frame.n_dates,
        n_rows=len(frame.rows),
        groups=groups,
        states=states,
        regimes=regimes,
        recent=recent,
    )
