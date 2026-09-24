"""End-to-end check against worlds whose answer is known.

**This is not evidence about the market.** Every number here is invented. It
establishes only that the machinery reports what is in the data: it finds
continuation when continuation was built in, reports no evidence when there
was none -- without being fooled by minutes a minute apart being nearly the
same observation -- and says DON'T SHIP when price reverts.

Each world is 40 sessions of minute bars for two symbols that share one
latent flow state and one gamma regime per stretch of the day (as SPY and SPX
share one market), with independent noise. The flow state drives the Trade
Bias inputs; in short gamma it also drives the price drift, with a sign set
by the world:

``continuation``  drift with the flow  -> must return SHIP
``null``          no drift at all      -> must NOT return SHIP or DON'T SHIP
``reversion``     drift against it     -> must return DON'T SHIP
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from research.msi_regime_excursion.excursion import ET, Bar
from research.short_gamma_trend.outcomes import SessionBars
from research.short_gamma_trend.sources import Reading
from research.short_gamma_trend.study import (
    PRIMARY_HORIZON,
    BuildCounts,
    Row,
    StudyResult,
    build_rows,
    run_study,
)
from src.signals.trade_bias.bias import BiasInput

SYMBOLS = ("SPY", "SPX")
_BETA = {"continuation": 0.6, "null": 0.0, "reversion": -0.6}
EXPECTED = {
    "continuation": {"SHIP"},
    "null": {"NO EVIDENCE"},
    "reversion": {"DON'T SHIP"},
}


def generate_world(
    mode: str,
    *,
    sessions: int = 40,
    minutes: int = 390,
    seed: int = 4242,
) -> tuple[dict[str, list[Reading]], dict[str, list[Bar]]]:
    """Raw readings and bars per symbol, before any replay."""
    if mode not in _BETA:
        raise ValueError(f"unknown mode {mode!r}")
    beta = _BETA[mode]
    rng = random.Random(seed)
    readings: dict[str, list[Reading]] = {s: [] for s in SYMBOLS}
    bars: dict[str, list[Bar]] = {s: [] for s in SYMBOLS}
    price = {"SPY": 700.0, "SPX": 7000.0}
    day = datetime(2026, 4, 1, 13, 30, tzinfo=timezone.utc)  # 09:30 ET

    made = 0
    offset = 0
    while made < sessions:
        start = day + timedelta(days=offset)
        offset += 1
        if start.astimezone(ET).weekday() >= 5:
            continue
        made += 1
        flow = 0
        # Three stretches per session, each short or long gamma.
        cuts = sorted(rng.sample(range(30, minutes - 30), 2))
        regimes = [rng.random() < 0.5 for _ in range(3)]
        for m in range(minutes):
            ts = start + timedelta(minutes=m)
            short = regimes[0] if m < cuts[0] else regimes[1] if m < cuts[1] else regimes[2]
            if rng.random() < 0.03:
                flow = rng.choice((-1, 0, 1))
            drift = beta * flow if short else 0.0
            for sym in SYMBOLS:
                inputs = BiasInput(
                    netGEX=-50.0 if short else 50.0,
                    gexGradient=0.0,
                    tapeFlow=50.0 * flow + rng.gauss(0.0, 5.0),
                    vannaCharm=30.0 * flow + rng.gauss(0.0, 4.0),
                    odtePositioning=30.0 * flow + rng.gauss(0.0, 4.0),
                    positioningTrap=rng.gauss(0.0, 4.0),
                    trapDetection=rng.gauss(0.0, 4.0),
                    gammaVWAP=rng.gauss(0.0, 4.0),
                    msi=50.0,
                )
                readings[sym].append(Reading(timestamp=ts, inputs=inputs, stored_state=None))
                p0 = price[sym]
                p1 = p0 * (1.0 + (drift + rng.gauss(0.0, 2.5)) / 10_000.0)
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
            # A reading at minute m is scored from bar m's close onward, so
            # only flow that persists past m can move its outcome.
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


def run_selftest(*, iterations: int = 2000) -> list[tuple[str, StudyResult, bool]]:
    out = []
    for mode in ("continuation", "null", "reversion"):
        rows, _ = build_world(mode)
        result = run_study(rows, SYMBOLS, iterations=iterations)
        out.append((mode, result, result.verdict.decision in EXPECTED[mode]))
    return out


def main() -> int:
    ok = True
    for mode, result, passed in run_selftest():
        cell = result.cells[("flow", PRIMARY_HORIZON, "pooled")]
        e = cell.excess_bps
        interval = (
            f"[{e.lo:+.2f}, {e.hi:+.2f}]" if e.lo is not None and e.hi is not None else "[n/a]"
        )
        value = f"{e.value:+.2f}" if e.value is not None else "n/a"
        print(
            f"  {mode:<13} flow {PRIMARY_HORIZON}m excess {value} bps {interval:<18} "
            f"-> {result.verdict.decision:<12} [{'PASS' if passed else 'FAIL'}]"
        )
        ok = ok and passed
    return 0 if ok else 1
