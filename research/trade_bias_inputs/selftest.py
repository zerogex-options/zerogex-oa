"""End-to-end check against worlds whose answer is known.

**This is not evidence about the market.** Every number here is invented. It
establishes only that the machinery reports what is in the data: it finds an
input that leads price and one that misleads, calls a rare one too rare, and
does not flag inputs that are pure noise -- without being fooled by minutes a
minute apart being nearly the same observation, or by SPY and SPX being one
market.

Each world is 40 sessions of minute bars for SPY and SPX, which share one
market: one price shock per minute plus a little noise of their own, and one
drift per session. Every input has one latent state (-1, 0, +1) shared by
both symbols that holds for a stretch and then switches; its reading on each
symbol is that state times a size drawn per stretch, plus noise. Net GEX
takes three gamma-regime stretches a day, as in ``research/short_gamma_trend``.

``mixed``  Tape Flow's state pushes price its way        -> Tape Flow PREDICTS
           Trap Detection's state pushes price against it -> Trap Detection WRONG WAY
           Gamma/VWAP reads on only 10 of the sessions    -> Gamma/VWAP TOO RARE
           the other six are noise                        -> NOTHING DETECTABLE
``null``   all nine are noise                             -> all NOTHING DETECTABLE
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from research.msi_regime_excursion.excursion import ET, Bar
from research.short_gamma_trend.outcomes import SessionBars
from research.short_gamma_trend.sources import Reading
from research.short_gamma_trend.study import BuildCounts, Row, build_rows
from research.trade_bias_inputs.calls import INPUTS
from research.trade_bias_inputs.study import (
    NOTHING,
    PREDICTS,
    PRIMARY_HORIZON,
    TOO_RARE,
    WRONG_WAY,
    StudyResult,
    run_study,
)
from src.signals.trade_bias.bias import BiasInput

SYMBOLS = ("SPY", "SPX")
MODES = ("mixed", "null")
EXPECTED: dict[str, dict[str, str]] = {
    "mixed": {"tape_flow": PREDICTS, "trap_detection": WRONG_WAY, "gamma_vwap": TOO_RARE},
    "null": {},
}

#: Price drift per minute, in bps, per unit of a leading input's state.
_EFFECT = 0.3
#: Chance per minute that a latent state switches (mean stretch ~33 minutes).
_SWITCH = 0.03
_RARE_SESSIONS = 10


def expected(mode: str, key: str) -> str:
    return EXPECTED[mode].get(key, NOTHING)


def generate_world(
    mode: str,
    *,
    sessions: int = 40,
    minutes: int = 390,
    seed: int = 4242,
) -> tuple[dict[str, list[Reading]], dict[str, list[Bar]]]:
    """Raw readings and bars per symbol, before any replay."""
    if mode not in EXPECTED:
        raise ValueError(f"unknown mode {mode!r}")
    rng = random.Random(seed)
    readings: dict[str, list[Reading]] = {s: [] for s in SYMBOLS}
    bars: dict[str, list[Bar]] = {s: [] for s in SYMBOLS}
    price = {"SPY": 700.0, "SPX": 7000.0}
    day = datetime(2026, 4, 1, 13, 30, tzinfo=timezone.utc)  # 09:30 ET
    rare_days = set(rng.sample(range(sessions), _RARE_SESSIONS))

    latent = {rule.key: 0 for rule in INPUTS}
    size = {rule.key: 60.0 for rule in INPUTS}

    def reading(key: str) -> float:
        return latent[key] * size[key] + rng.gauss(0.0, 6.0)

    made = 0
    offset = 0
    while made < sessions:
        start = day + timedelta(days=offset)
        offset += 1
        if start.astimezone(ET).weekday() >= 5:
            continue
        session_idx = made
        made += 1
        session_drift = rng.gauss(0.0, 0.15)
        cuts = sorted(rng.sample(range(30, minutes - 30), 2))
        regimes = [rng.random() < 0.5 for _ in range(3)]
        gamma_vwap_reads = mode != "mixed" or session_idx in rare_days
        for m in range(minutes):
            ts = start + timedelta(minutes=m)
            for key in latent:
                if rng.random() < _SWITCH:
                    latent[key] = rng.choice((-1, 0, 1))
                    size[key] = rng.uniform(30.0, 90.0)
            short = regimes[0] if m < cuts[0] else regimes[1] if m < cuts[1] else regimes[2]
            push = session_drift
            if mode == "mixed":
                push += _EFFECT * (latent["tape_flow"] - latent["trap_detection"])
            shock = rng.gauss(0.0, 2.5)
            for sym in SYMBOLS:
                msi = 50.0 + 0.35 * latent["msi"] * size["msi"] + rng.gauss(0.0, 4.0)
                inputs = BiasInput(
                    netGEX=-50.0 if short else 50.0,
                    gexGradient=reading("gex_gradient"),
                    tapeFlow=reading("tape_flow"),
                    vannaCharm=reading("vanna_charm"),
                    odtePositioning=reading("odte_positioning"),
                    positioningTrap=reading("positioning_trap"),
                    trapDetection=reading("trap_detection"),
                    # An abstaining signal reads exactly 0.
                    gammaVWAP=reading("gamma_vwap") if gamma_vwap_reads else 0.0,
                    msi=min(99.5, max(0.5, msi)),
                )
                readings[sym].append(Reading(timestamp=ts, inputs=inputs, stored_state=None))
                p0 = price[sym]
                p1 = p0 * (1.0 + (push + shock + rng.gauss(0.0, 0.4)) / 10_000.0)
                wick = abs(rng.gauss(0.0, 0.5)) / 10_000.0
                bars[sym].append(
                    Bar(
                        ts=ts,
                        open=p0,
                        high=max(p0, p1) * (1.0 + wick),
                        low=min(p0, p1) * (1.0 - wick),
                        close=p1,
                    )
                )
                price[sym] = p1
    return readings, bars


def build_world(mode: str, **kwargs) -> tuple[list[Row], BuildCounts]:
    readings, bars = generate_world(mode, **kwargs)
    rows: list[Row] = []
    counts = BuildCounts()
    for sym in SYMBOLS:
        r, c = build_rows(sym, readings[sym], SessionBars(bars[sym]))
        rows.extend(r)
        counts.merge(c)
    return rows, counts


def check(mode: str, result: StudyResult) -> list[tuple[str, str, str, bool]]:
    """``(key, verdict, expected, passed)`` per input."""
    out = []
    for r in result.inputs:
        want = expected(mode, r.key)
        got = r.verdict.decision if r.verdict else ""
        out.append((r.key, got, want, got == want))
    return out


def run_selftest(
    *, iterations: int = 2000, seed: int = 4242
) -> list[tuple[str, StudyResult, bool]]:
    out = []
    for mode in MODES:
        rows, _ = build_world(mode, seed=seed)
        result = run_study(rows, SYMBOLS, iterations=iterations)
        out.append((mode, result, all(passed for *_, passed in check(mode, result))))
    return out


def main() -> int:
    ok = True
    for mode, result, passed in run_selftest():
        print(f"  {mode} world -> [{'PASS' if passed else 'FAIL'}]")
        by_key = {r.key: r for r in result.inputs}
        for key, got, want, fine in check(mode, result):
            e = by_key[key].primary().excess_bps
            value = f"{e.value:+6.2f}" if e.value is not None else "   n/a"
            interval = (
                f"[{e.lo:+.2f}, {e.hi:+.2f}]" if e.lo is not None and e.hi is not None else "[n/a]"
            )
            print(
                f"    {by_key[key].title:<22} {PRIMARY_HORIZON}m excess {value} bps "
                f"{interval:<17} -> {got:<18} " + ("ok" if fine else f"EXPECTED {want}")
            )
        ok = ok and passed
    return 0 if ok else 1
