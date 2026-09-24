"""Replay the readings, score every call against what price did, and decide.

Two things about this data drive the statistics (both measured in
``docs/design/msi-regime-excursion.md`` §4):

* **Minutes are not independent.** A 30-minute outcome at 10:00 shares 29 of
  its 30 minutes with the one at 10:01, and a state persists for stretches.
* **SPY and SPX on the same day are one market.** Pooling them as if they
  were independent would double-count every day.

So every interval is a **date-level block bootstrap**: whole ET dates are
resampled, with every symbol's minutes for that date inside the block. Each
date is first reduced to a handful of sums per symbol, so a resample is one
matrix product and 2,000 of them cost nothing.

The baseline for a directional call is the **unconditional drift**: the mean
forward return over every scored minute of the same symbol and horizon, the
call's own minutes included. A bearish call only earns credit for falling
more than the market fell anyway -- which is the comparison
``content/methodology.md`` §5 names. The drift is re-estimated inside every
resample rather than fixed, so its own uncertainty is in the interval.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Optional, Sequence

import numpy as np

from research.msi_regime_excursion.excursion import ET
from research.short_gamma_trend.outcomes import (
    HORIZONS,
    REST,
    Horizon,
    Outcome,
    SessionBars,
    measure,
)
from research.short_gamma_trend.rule import (
    STATE_DIRECTION,
    VARIANTS,
    candidate_state,
    current_state,
    legacy_state,
)
from research.short_gamma_trend.sources import Reading

ALL_HORIZONS: tuple[Horizon, ...] = (*HORIZONS, REST)

# Pre-registered in README.md. Changing any of these after reading a result
# makes the verdict meaningless.
PRIMARY_LABEL = "flow"
PRIMARY_HORIZON = 30
CONFIRM_HORIZON = 60
MIN_SESSIONS = 15
MIN_ROWS = 300
ITERATIONS = 2000
SEED = 20260924

#: The first session scored by the post-fix rule. Stored states before it
#: came from the pre-fix rule (see ``rule.legacy_state``).
FIX_SESSION = date(2026, 9, 24)

#: Two readings further apart than this are not "consecutive".
_CONSECUTIVE = timedelta(minutes=2)

CANDIDATE_STATES = ("SG_TREND_UP", "SG_TREND_DOWN")


@dataclass(frozen=True)
class Label:
    key: str
    title: str
    state_key: str
    states: tuple[str, ...]
    entries_only: bool = False


LABELS: tuple[Label, ...] = (
    Label("flow", "short-gamma trend: flow votes (primary)", "flow", CANDIDATE_STATES),
    Label("aligned", "short-gamma trend: flow + structure", "aligned", CANDIDATE_STATES),
    Label("momentum", "short-gamma trend: price momentum only", "momentum", CANDIDATE_STATES),
    Label(
        "flow_entry",
        "flow, first minute of each episode",
        "flow",
        CANDIDATE_STATES,
        entries_only=True,
    ),
    Label(
        "prod_trend", "production Trend Up/Down (long gamma)", "current", ("TREND_UP", "TREND_DOWN")
    ),
    Label(
        "prod_trap",
        "production Trap Squeeze/Reversal",
        "current",
        ("TRAP_SQUEEZE", "TRAP_REVERSAL"),
    ),
)
LABELS_BY_KEY = {lab.key: lab for lab in LABELS}


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


@dataclass
class Row:
    symbol: str
    ts: datetime
    session: date
    #: ``current``, ``legacy`` and one entry per candidate variant.
    states: dict[str, str]
    stored: Optional[str]
    out: Outcome


@dataclass
class BuildCounts:
    readings: int = 0
    no_inputs: int = 0
    no_entry_bar: int = 0
    rows: int = 0
    stored_compared: int = 0
    stored_agree: int = 0
    #: ``"stored -> replayed"`` for every disagreement.
    mismatches: dict[str, int] = field(default_factory=dict)

    def merge(self, other: "BuildCounts") -> None:
        self.readings += other.readings
        self.no_inputs += other.no_inputs
        self.no_entry_bar += other.no_entry_bar
        self.rows += other.rows
        self.stored_compared += other.stored_compared
        self.stored_agree += other.stored_agree
        for k, v in other.mismatches.items():
            self.mismatches[k] = self.mismatches.get(k, 0) + v


def build_rows(
    symbol: str, readings: Iterable[Reading], bars: SessionBars
) -> tuple[list[Row], BuildCounts]:
    """Replay each reading through every rule and attach what price did next."""
    counts = BuildCounts()
    rows: list[Row] = []
    for reading in readings:
        counts.readings += 1
        if reading.inputs is None:
            counts.no_inputs += 1
            continue
        out = measure(bars, reading.timestamp)
        if out is None:
            counts.no_entry_bar += 1
            continue
        session = reading.timestamp.astimezone(ET).date()
        current = current_state(reading.inputs)
        states = {"current": current, "legacy": legacy_state(reading.inputs, current)}
        for variant in VARIANTS:
            states[variant] = candidate_state(
                reading.inputs, variant, current=current, prior_move=out.prior_move
            )
        if reading.stored_state:
            expected = current if session >= FIX_SESSION else states["legacy"]
            counts.stored_compared += 1
            if reading.stored_state == expected:
                counts.stored_agree += 1
            else:
                key = f"{reading.stored_state} -> {expected}"
                counts.mismatches[key] = counts.mismatches.get(key, 0) + 1
        rows.append(Row(symbol, reading.timestamp, session, states, reading.stored_state, out))
    counts.rows = len(rows)
    return rows, counts


def directions(rows: Sequence[Row], label: Label) -> np.ndarray:
    """+1 / -1 for a row the label calls, 0 otherwise. ``rows`` must be
    sorted by (symbol, ts)."""
    out = np.zeros(len(rows), dtype=np.int8)
    prev: Optional[Row] = None
    for i, row in enumerate(rows):
        state = row.states[label.state_key]
        d = STATE_DIRECTION.get(state, 0) if state in label.states else 0
        if d and label.entries_only and prev is not None:
            continuing = (
                prev.symbol == row.symbol
                and prev.session == row.session
                and row.ts - prev.ts <= _CONSECUTIVE
                and prev.states[label.state_key] == state
            )
            if continuing:
                d = 0
        out[i] = d
        prev = row
    return out


# ---------------------------------------------------------------------------
# Date-block bootstrap
# ---------------------------------------------------------------------------

# Per (date, symbol) sufficient statistics.
_S_DR, _S_D, _N_CALL, _S_R, _N_ALL, _HITS, _S_DQ, _S_Q, _S_RNG_CALL, _S_RNG = range(10)
_WIDTH = 10


@dataclass
class Estimate:
    value: Optional[float]
    lo: Optional[float] = None
    hi: Optional[float] = None
    p: Optional[float] = None


@dataclass
class Cell:
    label: str
    horizon: Horizon
    scope: str
    n_calls: int
    n_sessions: int
    excess_bps: Estimate
    quality_bps: Estimate
    hit_rate: Estimate
    range_ratio: Estimate
    #: Mean return in the call's direction with no drift adjustment.
    raw_bps: Optional[float]


class Frame:
    """The scored rows as arrays, plus one fixed set of bootstrap draws.

    Every estimate uses the same draws, so a difference between two labels
    is computed resample by resample and its interval is honest.
    """

    def __init__(
        self,
        rows: Sequence[Row],
        symbols: Sequence[str],
        *,
        iterations: int = ITERATIONS,
        seed: int = SEED,
    ) -> None:
        self.rows = sorted(rows, key=lambda r: (r.symbol, r.ts))
        self.symbols = list(symbols)
        self.dates = sorted({r.session for r in self.rows})
        d_index = {d: i for i, d in enumerate(self.dates)}
        s_index = {s: i for i, s in enumerate(self.symbols)}
        self.n_dates = len(self.dates)
        self.n_syms = len(self.symbols)
        self.cell_index = np.array(
            [d_index[r.session] * self.n_syms + s_index[r.symbol] for r in self.rows],
            dtype=np.int64,
        )
        self.ret: dict[Horizon, np.ndarray] = {}
        self.qual: dict[Horizon, np.ndarray] = {}
        self.rng: dict[Horizon, np.ndarray] = {}
        for h in ALL_HORIZONS:
            self.ret[h] = _column(self.rows, lambda o, h=h: o.ret.get(h))
            self.qual[h] = _column(
                self.rows,
                lambda o, h=h: (
                    None if o.up.get(h) is None or o.down.get(h) is None else o.up[h] - o.down[h]
                ),
            )
            self.rng[h] = _column(self.rows, lambda o, h=h: o.range.get(h))
        self._dirs: dict[str, np.ndarray] = {}
        self._blocks: dict[tuple[str, Horizon], np.ndarray] = {}
        if self.n_dates:
            gen = np.random.default_rng(seed)
            self.draws = gen.multinomial(
                self.n_dates, np.full(self.n_dates, 1.0 / self.n_dates), size=iterations
            ).astype(float)
        else:
            self.draws = np.zeros((0, 0))

    def dirs(self, label: Label) -> np.ndarray:
        if label.key not in self._dirs:
            self._dirs[label.key] = directions(self.rows, label)
        return self._dirs[label.key]

    def blocks(self, label: Label, horizon: Horizon) -> np.ndarray:
        """``(dates, symbols, 10)`` sufficient statistics."""
        key = (label.key, horizon)
        if key not in self._blocks:
            self._blocks[key] = self._build_blocks(label, horizon)
        return self._blocks[key]

    def _build_blocks(self, label: Label, horizon: Horizon) -> np.ndarray:
        size = self.n_dates * self.n_syms
        r, q, g = self.ret[horizon], self.qual[horizon], self.rng[horizon]
        valid = ~(np.isnan(r) | np.isnan(q) | np.isnan(g))
        d = self.dirs(label).astype(float)
        call = valid & (d != 0)
        idx = self.cell_index

        def acc(mask: np.ndarray, weights: Optional[np.ndarray] = None) -> np.ndarray:
            if weights is None:
                return np.bincount(idx[mask], minlength=size).astype(float)
            return np.bincount(idx[mask], weights=weights[mask], minlength=size)

        cols: list[np.ndarray] = [np.zeros(size)] * _WIDTH
        cols[_S_DR] = acc(call, d * r)
        cols[_S_D] = acc(call, d)
        cols[_N_CALL] = acc(call)
        cols[_S_R] = acc(valid, r)
        cols[_N_ALL] = acc(valid)
        cols[_HITS] = acc(call & (d * np.where(valid, r, 0.0) > 0))
        cols[_S_DQ] = acc(call, d * q)
        cols[_S_Q] = acc(valid, q)
        cols[_S_RNG_CALL] = acc(call, g)
        cols[_S_RNG] = acc(valid, g)
        return np.stack(cols, axis=-1).reshape(self.n_dates, self.n_syms, _WIDTH)

    def sessions_with_calls(self, label: Label, horizon: Horizon, sym: Optional[int]) -> int:
        b = self.blocks(label, horizon)
        n_call = b[:, :, _N_CALL] if sym is None else b[:, sym : sym + 1, _N_CALL]
        return int(np.count_nonzero(n_call.sum(axis=1) > 0))


def _column(rows: Sequence[Row], get) -> np.ndarray:
    return np.array([np.nan if (v := get(r.out)) is None else float(v) for r in rows], dtype=float)


def _stats(sums: np.ndarray) -> dict[str, np.ndarray]:
    """Statistics from summed sufficient statistics ``(..., symbols, 10)``."""
    with np.errstate(invalid="ignore", divide="ignore"):
        n_all = sums[..., _N_ALL]
        drift = np.where(n_all > 0, sums[..., _S_R] / n_all, 0.0)
        qdrift = np.where(n_all > 0, sums[..., _S_Q] / n_all, 0.0)
        n_call = sums[..., _N_CALL].sum(axis=-1)
        excess = (sums[..., _S_DR] - drift * sums[..., _S_D]).sum(axis=-1) / n_call
        quality = (sums[..., _S_DQ] - qdrift * sums[..., _S_D]).sum(axis=-1) / n_call
        hit = sums[..., _HITS].sum(axis=-1) / n_call
        rng_all = sums[..., _S_RNG].sum(axis=-1) / n_all.sum(axis=-1)
        range_ratio = (sums[..., _S_RNG_CALL].sum(axis=-1) / n_call) / rng_all
        raw = sums[..., _S_DR].sum(axis=-1) / n_call
    return {
        "excess": excess,
        "quality": quality,
        "hit": hit,
        "range_ratio": range_ratio,
        "raw": raw,
        "n_call": n_call,
    }


def _finite(x: Any) -> Optional[float]:
    return float(x) if x is not None and math.isfinite(float(x)) else None


def _estimate(point: Any, boot: np.ndarray, *, with_p: bool) -> Estimate:
    point_f = _finite(point)
    boot = boot[np.isfinite(boot)]
    if point_f is None or boot.size < 20:
        return Estimate(point_f)
    lo, hi = np.percentile(boot, [2.5, 97.5])
    p = None
    if with_p:
        tail = np.count_nonzero(boot <= 0.0) if point_f >= 0 else np.count_nonzero(boot >= 0.0)
        p = max(min(1.0, 2.0 * tail / boot.size), 1.0 / boot.size)
    return Estimate(point_f, float(lo), float(hi), p)


def score(frame: Frame, label: Label, horizon: Horizon, scope: str = "pooled") -> Cell:
    """One label x horizon x scope ("pooled" or a symbol)."""
    blocks = frame.blocks(label, horizon)
    sym = None if scope == "pooled" else frame.symbols.index(scope)
    if sym is not None:
        blocks = blocks[:, sym : sym + 1, :]
    point = _stats(blocks.sum(axis=0))
    if frame.draws.size:
        boot = _stats(np.einsum("id,dkv->ikv", frame.draws, blocks))
    else:
        boot = {k: np.array([]) for k in point}
    return Cell(
        label=label.key,
        horizon=horizon,
        scope=scope,
        n_calls=int(point["n_call"]),
        n_sessions=frame.sessions_with_calls(label, horizon, sym),
        excess_bps=_estimate(point["excess"], boot["excess"], with_p=True),
        quality_bps=_estimate(point["quality"], boot["quality"], with_p=True),
        hit_rate=_estimate(point["hit"], boot["hit"], with_p=False),
        range_ratio=_estimate(point["range_ratio"], boot["range_ratio"], with_p=False),
        raw_bps=_finite(point["raw"]),
    )


def paired_difference(frame: Frame, a: Label, b: Label, horizon: Horizon) -> Estimate:
    """Pooled directional excess of ``a`` minus ``b``, resample by resample."""
    ba, bb = frame.blocks(a, horizon), frame.blocks(b, horizon)
    point = _stats(ba.sum(axis=0))["excess"] - _stats(bb.sum(axis=0))["excess"]
    if not frame.draws.size:
        return Estimate(_finite(point))
    boot = (
        _stats(np.einsum("id,dkv->ikv", frame.draws, ba))["excess"]
        - _stats(np.einsum("id,dkv->ikv", frame.draws, bb))["excess"]
    )
    return _estimate(point, boot, with_p=True)


# ---------------------------------------------------------------------------
# Frequency and flicker (descriptive)
# ---------------------------------------------------------------------------


@dataclass
class Frequency:
    label: str
    share_of_minutes: dict[str, float]
    sessions: int
    episodes: int
    median_episode_min: Optional[float]


def frequency(frame: Frame, label: Label) -> Frequency:
    dirs = frame.dirs(label)
    rows = frame.rows
    share: dict[str, float] = {}
    for sym in frame.symbols:
        mask = [r.symbol == sym for r in rows]
        n = sum(mask)
        share[sym] = (sum(1 for m, d in zip(mask, dirs) if m and d) / n) if n else 0.0
    lengths: list[int] = []
    sessions: set[tuple[str, date]] = set()
    run = 0
    prev: Optional[Row] = None
    prev_d = 0
    for row, d in zip(rows, dirs):
        continuing = (
            prev is not None
            and d != 0
            and d == prev_d
            and prev.symbol == row.symbol
            and prev.session == row.session
            and row.ts - prev.ts <= _CONSECUTIVE
        )
        if d and continuing:
            run += 1
        else:
            if run:
                lengths.append(run)
            run = 1 if d else 0
        if d:
            sessions.add((row.symbol, row.session))
        prev, prev_d = row, d
    if run:
        lengths.append(run)
    lengths.sort()
    median = None
    if lengths:
        mid = len(lengths) // 2
        median = (
            float(lengths[mid]) if len(lengths) % 2 else (lengths[mid - 1] + lengths[mid]) / 2.0
        )
    return Frequency(label.key, share, len({s for _, s in sessions}), len(lengths), median)


def label_changes_per_session(frame: Frame, state_key: str) -> Optional[float]:
    """Average number of times the panel's state changes in a session."""
    changes: dict[tuple[str, date], int] = {}
    prev: Optional[Row] = None
    for row in frame.rows:
        key = (row.symbol, row.session)
        changes.setdefault(key, 0)
        if (
            prev is not None
            and (prev.symbol, prev.session) == key
            and prev.states[state_key] != row.states[state_key]
        ):
            changes[key] += 1
        prev = row
    return (sum(changes.values()) / len(changes)) if changes else None


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------


@dataclass
class Verdict:
    decision: str
    reasons: list[str]


def decide(cells: dict[tuple[str, Horizon, str], Cell], symbols: Sequence[str]) -> Verdict:
    """Apply the pre-registered rule in README.md. Nothing else moves it."""
    primary = cells[(PRIMARY_LABEL, PRIMARY_HORIZON, "pooled")]
    e = primary.excess_bps
    if primary.n_sessions < MIN_SESSIONS or primary.n_calls < MIN_ROWS or e.lo is None:
        return Verdict(
            "INSUFFICIENT",
            [
                f"{primary.n_calls:,} minutes over {primary.n_sessions} sessions in the "
                f"candidate state; the rule needs {MIN_ROWS} and {MIN_SESSIONS}"
            ],
        )
    assert e.value is not None and e.hi is not None
    per_symbol = {s: cells[(PRIMARY_LABEL, PRIMARY_HORIZON, s)].excess_bps.value for s in symbols}
    confirm = cells[(PRIMARY_LABEL, CONFIRM_HORIZON, "pooled")].excess_bps.value
    reasons = [
        f"pooled {PRIMARY_HORIZON}m excess {e.value:+.2f} bps, 95% [{e.lo:+.2f}, {e.hi:+.2f}]",
        "per symbol: "
        + ", ".join(
            f"{s} {v:+.2f}" if v is not None else f"{s} n/a" for s, v in per_symbol.items()
        ),
        f"pooled {CONFIRM_HORIZON}m excess "
        + (f"{confirm:+.2f} bps" if confirm is not None else "n/a"),
    ]
    if (
        e.value > 0
        and e.lo > 0
        and all(v is not None and v > 0 for v in per_symbol.values())
        and confirm is not None
        and confirm > 0
    ):
        return Verdict("SHIP", reasons)
    if e.value < 0 and e.hi < 0:
        return Verdict("DON'T SHIP", reasons)
    return Verdict("NO EVIDENCE", reasons)


# ---------------------------------------------------------------------------
# The whole study
# ---------------------------------------------------------------------------


@dataclass
class StudyResult:
    symbols: list[str]
    n_dates: int
    cells: dict[tuple[str, Horizon, str], Cell]
    frequencies: list[Frequency]
    changes_current: Optional[float]
    changes_candidate: Optional[float]
    flow_minus_momentum: dict[Horizon, Estimate]
    verdict: Verdict

    def as_dict(self) -> dict:
        return {
            "symbols": self.symbols,
            "n_dates": self.n_dates,
            "verdict": asdict(self.verdict),
            "cells": [asdict(c) for c in self.cells.values()],
            "frequencies": [asdict(f) for f in self.frequencies],
            "label_changes_per_session": {
                "current": self.changes_current,
                "candidate_flow": self.changes_candidate,
            },
            "flow_minus_momentum": {str(h): asdict(v) for h, v in self.flow_minus_momentum.items()},
        }


def run_study(
    rows: Sequence[Row],
    symbols: Sequence[str],
    *,
    iterations: int = ITERATIONS,
    seed: int = SEED,
) -> StudyResult:
    frame = Frame(rows, symbols, iterations=iterations, seed=seed)
    cells: dict[tuple[str, Horizon, str], Cell] = {}
    for label in LABELS:
        for h in ALL_HORIZONS:
            for scope in ("pooled", *frame.symbols):
                cells[(label.key, h, scope)] = score(frame, label, h, scope)
    flow, momentum = LABELS_BY_KEY["flow"], LABELS_BY_KEY["momentum"]
    return StudyResult(
        symbols=list(frame.symbols),
        n_dates=frame.n_dates,
        cells=cells,
        frequencies=[frequency(frame, lab) for lab in LABELS if not lab.entries_only],
        changes_current=label_changes_per_session(frame, "current"),
        changes_candidate=label_changes_per_session(frame, "flow"),
        flow_minus_momentum={h: paired_difference(frame, flow, momentum, h) for h in ALL_HORIZONS},
        verdict=decide(cells, frame.symbols),
    )
