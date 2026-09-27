"""Score each input's calls against what price did, and give each a verdict.

The machinery is ``research.short_gamma_trend.study``'s: the same rows (one
per persisted minute, with its outcomes), the same date-block bootstrap over
per-(date, symbol) sufficient statistics, the same drift-adjusted
directional excess. Only the calls differ: each one comes from a single
input's reading instead of the panel's market state.

Every estimate uses the frame's one fixed set of resamples, so an input
minus momentum is computed resample by resample and its interval is honest.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Optional, Sequence

import numpy as np

from research.short_gamma_trend.outcomes import Horizon
from research.short_gamma_trend.rule import votes
from research.short_gamma_trend.study import (
    ALL_HORIZONS,
    Cell,
    Estimate,
    Frame,
    Row,
    paired_difference_blocks,
    score_blocks,
)
from research.trade_bias_inputs.calls import GAMMA_SIGN, INPUTS, InputRule

# Pre-registered in README.md. Changing any of these after reading a result
# makes the verdicts meaningless.
PRIMARY_HORIZON = 30
CONFIRM_HORIZON = 60
MIN_SESSIONS = 15
MIN_CALLS = 300
ALPHA = 0.05
ITERATIONS = 2000
SEED = 20260927

PREDICTS = "PREDICTS"
WRONG_WAY = "WRONG WAY"
NOTHING = "NOTHING DETECTABLE"
TOO_RARE = "TOO RARE"

MOMENTUM_KEY = "momentum"
REGIMES = ("short gamma", "long gamma")
PRIOR_MOVE_SPLITS = ("with the move", "against it", "no prior move")


# ---------------------------------------------------------------------------
# Calls
# ---------------------------------------------------------------------------


def input_dirs(rows: Sequence[Row], rule: InputRule) -> np.ndarray:
    """+1 / -1 / 0 per row: the call ``rule``'s input made at that minute."""
    return np.array([rule.call(rule.value(r.inputs)) for r in rows], dtype=np.int8)


def momentum_dirs(rows: Sequence[Row]) -> np.ndarray:
    """The reference call: the sign of the prior 30-minute move (0 below 5 bps)."""
    return np.array([r.out.prior_move for r in rows], dtype=np.int8)


def regime_pools(rows: Sequence[Row]) -> dict[str, np.ndarray]:
    """Production's short- and long-gamma minutes (``votes`` mirrors it)."""
    short = np.zeros(len(rows), dtype=bool)
    long_ = np.zeros(len(rows), dtype=bool)
    for i, row in enumerate(rows):
        if row.inputs is None:
            continue
        v = votes(row.inputs)
        short[i], long_[i] = v.is_short_gamma, v.is_long_gamma
    return {"short gamma": short, "long gamma": long_}


def prior_move_splits(dirs: np.ndarray, prior: np.ndarray) -> dict[str, np.ndarray]:
    """The calls split by whether they agree with the prior 30-minute move."""
    return {
        "with the move": np.where((prior != 0) & (dirs == prior), dirs, 0).astype(np.int8),
        "against it": np.where((prior != 0) & (dirs == -prior), dirs, 0).astype(np.int8),
        "no prior move": np.where(prior == 0, dirs, 0).astype(np.int8),
    }


# ---------------------------------------------------------------------------
# Per-input results
# ---------------------------------------------------------------------------


@dataclass
class InputVerdict:
    decision: str
    reasons: list[str]


@dataclass
class Band:
    label: str
    #: ``excess_bps`` here is the mean 30-minute return minus drift over the
    #: band's minutes (every minute counts as an "up" call), not a call's edge.
    cell: Cell


@dataclass
class InputResult:
    key: str
    title: str
    rule: str
    #: Share of scored minutes with a reading, and with a call, per symbol.
    available: dict[str, float]
    calling: dict[str, float]
    #: ``(horizon, scope)`` -> cell; scope is "pooled" or a symbol.
    cells: dict[tuple[Horizon, str], Cell]
    #: Pooled 30-minute excess minus price momentum's, resample by resample.
    minus_momentum: Estimate
    bands: list[Band]
    #: Pooled 30-minute excess over each regime's own drift; empty for the
    #: gamma sign, which is the regime.
    regimes: dict[str, Cell]
    #: Pooled 30-minute excess over drift, split by the prior move.
    prior_move: dict[str, Cell]
    p_primary: Optional[float] = None
    significant: bool = False
    verdict: Optional[InputVerdict] = None

    def primary(self) -> Cell:
        return self.cells[(PRIMARY_HORIZON, "pooled")]

    def too_rare(self) -> bool:
        c = self.primary()
        return c.n_sessions < MIN_SESSIONS or c.n_calls < MIN_CALLS or c.excess_bps.value is None


def decide_input(result: InputResult, symbols: Sequence[str]) -> InputVerdict:
    """Apply the pre-registered rule in README.md. Nothing else moves it."""
    primary = result.primary()
    if result.too_rare():
        return InputVerdict(
            TOO_RARE,
            [
                f"{primary.n_calls:,} calls over {primary.n_sessions} sessions scored at "
                f"{PRIMARY_HORIZON} minutes; the rule needs {MIN_CALLS} and {MIN_SESSIONS}"
            ],
        )
    e = primary.excess_bps
    assert e.value is not None
    per_symbol = {s: result.cells[(PRIMARY_HORIZON, s)].excess_bps.value for s in symbols}
    confirm = result.cells[(CONFIRM_HORIZON, "pooled")].excess_bps.value
    interval = f", 95% [{e.lo:+.2f}, {e.hi:+.2f}]" if e.lo is not None and e.hi is not None else ""
    p_text = f"p {result.p_primary:.4f}" if result.p_primary is not None else "p n/a"
    reasons = [
        f"pooled {PRIMARY_HORIZON}m excess {e.value:+.2f} bps{interval}; {p_text}, "
        + ("significant" if result.significant else "not significant")
        + f" after Holm's correction at {ALPHA:.0%} across the nine",
        "per symbol: "
        + ", ".join(
            f"{s} {v:+.2f}" if v is not None else f"{s} n/a" for s, v in per_symbol.items()
        ),
        f"pooled {CONFIRM_HORIZON}m excess "
        + (f"{confirm:+.2f} bps" if confirm is not None else "n/a"),
    ]
    if result.significant and confirm is not None:
        values = list(per_symbol.values())
        if e.value > 0 and confirm > 0 and all(v is not None and v > 0 for v in values):
            return InputVerdict(PREDICTS, reasons)
        if e.value < 0 and confirm < 0 and all(v is not None and v < 0 for v in values):
            return InputVerdict(WRONG_WAY, reasons)
    return InputVerdict(NOTHING, reasons)


def holm(p_values: Sequence[float], alpha: float = ALPHA) -> list[bool]:
    """Holm's step-down correction: a flag per test, with the chance that any
    flag is false at most ``alpha`` whatever the dependence between the tests.

    The smallest p must clear ``alpha / m``, the next ``alpha / (m - 1)``, and
    so on; the first one that misses stops the procedure.
    """
    m = len(p_values)
    out = [False] * m
    for rank, i in enumerate(sorted(range(m), key=lambda j: p_values[j])):
        if p_values[i] > alpha / (m - rank):
            break
        out[i] = True
    return out


def apply_verdicts(results: Sequence[InputResult], symbols: Sequence[str]) -> None:
    """Holm's correction across all of ``results``, then each verdict.

    An input too rare to judge enters as p = 1, so the family is always the
    nine and the bar does not depend on what the data turned out to hold.
    """
    p_values: list[float] = []
    for r in results:
        p = None if r.too_rare() else r.primary().excess_bps.p
        r.p_primary = p
        p_values.append(1.0 if p is None else p)
    flags = holm(p_values, ALPHA)
    for r, flag in zip(results, flags):
        r.significant = bool(flag) and r.p_primary is not None
        r.verdict = decide_input(r, symbols)


# ---------------------------------------------------------------------------
# The whole study
# ---------------------------------------------------------------------------


def symbol_masks(frame: Frame) -> dict[str, np.ndarray]:
    names = np.array([r.symbol for r in frame.rows], dtype=object)
    return {s: names == s for s in frame.symbols}


def _shares(mask: np.ndarray, by_symbol: dict[str, np.ndarray]) -> dict[str, float]:
    out = {}
    for sym, in_sym in by_symbol.items():
        n = int(np.count_nonzero(in_sym))
        out[sym] = float(np.count_nonzero(mask & in_sym)) / n if n else 0.0
    return out


def score_input(
    frame: Frame,
    rule: InputRule,
    *,
    momentum: np.ndarray,
    pools: dict[str, np.ndarray],
    by_symbol: Optional[dict[str, np.ndarray]] = None,
) -> InputResult:
    rows = frame.rows
    by_symbol = by_symbol if by_symbol is not None else symbol_masks(frame)
    key = f"input:{rule.key}"
    values = [rule.value(r.inputs) for r in rows]
    dirs = np.array([rule.call(v) for v in values], dtype=np.int8)
    has_value = np.array([v is not None for v in values], dtype=bool)

    cells: dict[tuple[Horizon, str], Cell] = {}
    for h in ALL_HORIZONS:
        blocks = frame.blocks_for(key, dirs, h)
        for scope in ("pooled", *frame.symbols):
            cells[(h, scope)] = score_blocks(frame, key, blocks, h, scope)

    h = PRIMARY_HORIZON
    minus_momentum = paired_difference_blocks(
        frame,
        frame.blocks_for(key, dirs, h),
        frame.blocks_for(MOMENTUM_KEY, momentum, h),
    )

    band_index = np.array(
        [-1 if (b := rule.band(v)) is None else b for v in values], dtype=np.int64
    )
    bands = []
    for i, label in enumerate(rule.band_labels):
        in_band = (band_index == i).astype(np.int8)
        bkey = f"{key}:band:{i}"
        bands.append(Band(label, score_blocks(frame, bkey, frame.blocks_for(bkey, in_band, h), h)))

    regimes: dict[str, Cell] = {}
    if rule.kind != GAMMA_SIGN:
        for name in REGIMES:
            rkey = f"{key}:regime:{name}"
            blocks = frame.blocks_for(rkey, dirs, h, pool=pools[name])
            regimes[name] = score_blocks(frame, rkey, blocks, h)

    prior: dict[str, Cell] = {}
    for name, split in prior_move_splits(dirs, momentum).items():
        pkey = f"{key}:prior:{name}"
        prior[name] = score_blocks(frame, pkey, frame.blocks_for(pkey, split, h), h)

    return InputResult(
        key=rule.key,
        title=rule.title,
        rule=rule.rule_text,
        available=_shares(has_value, by_symbol),
        calling=_shares(dirs != 0, by_symbol),
        cells=cells,
        minus_momentum=minus_momentum,
        bands=bands,
        regimes=regimes,
        prior_move=prior,
    )


def _cell_key(k: tuple[Horizon, str]) -> str:
    return f"{k[0]}|{k[1]}"


@dataclass
class StudyResult:
    symbols: list[str]
    n_dates: int
    n_rows: int
    inputs: list[InputResult]
    #: The price-momentum reference, ``(horizon, scope)`` -> cell.
    momentum: dict[tuple[Horizon, str], Cell]
    #: Pooled 30-minute excess of momentum in each regime, over its own drift.
    momentum_regimes: dict[str, Cell]

    def verdicts(self) -> dict[str, str]:
        return {r.key: r.verdict.decision if r.verdict else "" for r in self.inputs}

    def as_dict(self) -> dict:
        def input_dict(r: InputResult) -> dict:
            d = {
                "key": r.key,
                "title": r.title,
                "rule": r.rule,
                "verdict": asdict(r.verdict) if r.verdict else None,
                "p_primary": r.p_primary,
                "significant": r.significant,
                "available": r.available,
                "calling": r.calling,
                "cells": {_cell_key(k): asdict(c) for k, c in r.cells.items()},
                "minus_momentum": asdict(r.minus_momentum),
                "bands": [{"label": b.label, "cell": asdict(b.cell)} for b in r.bands],
                "regimes": {k: asdict(c) for k, c in r.regimes.items()},
                "prior_move": {k: asdict(c) for k, c in r.prior_move.items()},
            }
            return d

        return {
            "symbols": self.symbols,
            "n_dates": self.n_dates,
            "n_rows": self.n_rows,
            "inputs": [input_dict(r) for r in self.inputs],
            "momentum": {_cell_key(k): asdict(c) for k, c in self.momentum.items()},
            "momentum_regimes": {k: asdict(c) for k, c in self.momentum_regimes.items()},
        }


def run_study(
    rows: Sequence[Row],
    symbols: Sequence[str],
    *,
    iterations: int = ITERATIONS,
    seed: int = SEED,
) -> StudyResult:
    frame = Frame(rows, symbols, iterations=iterations, seed=seed)
    momentum = momentum_dirs(frame.rows)
    pools = regime_pools(frame.rows)

    momentum_cells: dict[tuple[Horizon, str], Cell] = {}
    for h in ALL_HORIZONS:
        blocks = frame.blocks_for(MOMENTUM_KEY, momentum, h)
        for scope in ("pooled", *frame.symbols):
            momentum_cells[(h, scope)] = score_blocks(frame, MOMENTUM_KEY, blocks, h, scope)
    momentum_regimes = {}
    for name in REGIMES:
        key = f"{MOMENTUM_KEY}:regime:{name}"
        blocks = frame.blocks_for(key, momentum, PRIMARY_HORIZON, pool=pools[name])
        momentum_regimes[name] = score_blocks(frame, key, blocks, PRIMARY_HORIZON)

    by_symbol = symbol_masks(frame)
    results = [
        score_input(frame, rule, momentum=momentum, pools=pools, by_symbol=by_symbol)
        for rule in INPUTS
    ]
    apply_verdicts(results, frame.symbols)
    return StudyResult(
        symbols=list(frame.symbols),
        n_dates=frame.n_dates,
        n_rows=len(frame.rows),
        inputs=results,
        momentum=momentum_cells,
        momentum_regimes=momentum_regimes,
    )
