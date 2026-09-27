"""End-to-end check against worlds whose answer is known.

**This is not evidence about the market.** Every number here is invented. It
establishes only that the machinery reports what is in the data: it finds
states that know something about the next half hour's movement, it is not
fooled by states that only restate the clock and the tape, and it reports
nothing for states that know nothing.

Each world is 40 sessions of minute bars for SPY and SPX, which share one
market. How far price moves each minute is the product of the session's own
level, a time-of-day rhythm (busy open, quiet lunch, livelier close) and a
slowly wandering clustering factor, so recent movement predicts future
movement and the adjustment has real work to do. The states differ by world:

``informative``  a hidden regime (quiet, normal, wild) multiplies movement,
                 and the panel sees each regime 10 minutes before price
                 does: it shows a Trap state ahead of the wild regime and
                 Chop otherwise, so both know the next half hour beyond the
                 clock and the tape.    -> Trap MOVES MORE, Chop MOVES LESS
``confounded``   no hidden regime: the panel shows a Trap state when the last
                 20 minutes were busy for the time of day, and Chop otherwise.
                 Raw ratios look informative; after matching they must not
                 reach the 10% bar.     -> no MOVES MORE / MOVES LESS
``null``         the states are drawn at random.
                                        -> NOTHING DETECTABLE for all

In every world Trend Up / Down is switched on and off at random, so it knows
nothing (NOTHING DETECTABLE), and Awaiting confluence appears only at the open
of 8 sessions (TOO RARE).
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence, TypeVar

from research.msi_regime_excursion.excursion import ET, Bar
from research.short_gamma_trend.outcomes import SessionBars
from research.short_gamma_trend.sources import Reading
from research.short_gamma_trend.study import BuildCounts
from research.trade_bias_movement.study import (
    BY_SYMBOL,
    GROUPS,
    MOVES_LESS,
    MOVES_MORE,
    NOTHING,
    PRIMARY_HORIZON,
    SLIGHTLY_LESS,
    SLIGHTLY_MORE,
    TOO_RARE,
    MoveRow,
    StudyResult,
    build,
    time_slot,
    run_study,
)
from src.signals.trade_bias.bias import BiasInput

SYMBOLS = ("SPY", "SPX")
MODES = ("informative", "confounded", "null")
_NOT_MATERIAL = {NOTHING, SLIGHTLY_MORE, SLIGHTLY_LESS}
EXPECTED: dict[str, dict[str, set[str]]] = {
    "informative": {
        "trend": {NOTHING},
        "trap": {MOVES_MORE},
        "chop": {MOVES_LESS},
        "awaiting": {TOO_RARE},
    },
    "confounded": {
        "trend": {NOTHING},
        "trap": _NOT_MATERIAL,
        "chop": _NOT_MATERIAL,
        "awaiting": {TOO_RARE},
    },
    "null": {"trend": {NOTHING}, "trap": {NOTHING}, "chop": {NOTHING}, "awaiting": {TOO_RARE}},
}

#: Movement per minute, in bps, before any multiplier.
_BASE_BPS = 1.6
#: The hidden regime's multipliers and shares, and its chance of switching.
_REGIMES = ((0.5, 0.4), (1.0, 0.4), (2.0, 0.2))
_REGIME_SWITCH = 1.0 / 20.0
#: How many minutes before price the informative panel sees a regime.
_REGIME_LEAD = 10
#: Trend's on/off switching, and its share of minutes when redrawn.
_TREND_SWITCH = 1.0 / 15.0
_TREND_SHARE = 0.25
#: The null world's states and their shares.
_NULL_STATES = (("TRAP", 0.15), ("CHOP", 0.60), ("TREND", 0.25))
_AWAITING_SESSIONS = 8
_AWAITING_MINUTES = 10
#: The confounded world's look-back and the share of minutes it calls busy.
_BUSY_LOOKBACK = 20
_BUSY_SHARE = 0.25


def _time_of_day(m: int, minutes: int) -> float:
    return 0.75 + 0.9 * math.exp(-m / 25.0) + 0.35 * math.exp(-(minutes - 1 - m) / 30.0)


def _inputs(state: str, short: bool, rng: random.Random) -> BiasInput:
    """Inputs that make production's rule return ``state``."""

    def n() -> float:
        return rng.gauss(0.0, 2.0)

    if state == "UNKNOWN":
        return BiasInput(netGEX=50.0, tapeFlow=n(), msi=50.0)
    flow = {"TREND_UP": 1, "TREND_DOWN": -1, "TRAP_REVERSAL": 1, "TRAP_SQUEEZE": -1}.get(state, 0)
    structure = {"TRAP_REVERSAL": -1, "TRAP_SQUEEZE": 1}.get(state, 0)
    if state.startswith("TRAP"):
        gamma = -50.0
    elif state.startswith("TREND"):
        gamma = 50.0
    else:
        gamma = -50.0 if short else 50.0
    return BiasInput(
        netGEX=gamma,
        gexGradient=n(),
        tapeFlow=50.0 * flow + n(),
        vannaCharm=30.0 * flow + n(),
        odtePositioning=30.0 * flow + n(),
        positioningTrap=30.0 * structure + n(),
        trapDetection=n(),
        gammaVWAP=30.0 * structure + n(),
        msi=50.0 + n(),
    )


_T = TypeVar("_T")


def _pick(rng: random.Random, weighted: Sequence[tuple[_T, float]]) -> _T:
    x = rng.random()
    for value, weight in weighted:
        x -= weight
        if x < 0:
            return value
    return weighted[-1][0]


def _prior_busy(bars: list[Bar], i: int, day_start: int) -> Optional[float]:
    """High minus low over the ``_BUSY_LOOKBACK`` bars ending at ``i``, in bps."""
    if i - day_start < _BUSY_LOOKBACK - 1:
        return None
    window = bars[i - _BUSY_LOOKBACK + 1 : i + 1]
    return 10_000.0 * (max(b.high for b in window) - min(b.low for b in window)) / bars[i].close


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
    bars: dict[str, list[Bar]] = {s: [] for s in SYMBOLS}
    price = {"SPY": 700.0, "SPX": 7000.0}
    day = datetime(2026, 4, 1, 13, 30, tzinfo=timezone.utc)  # 09:30 ET
    awaiting_days = set(rng.sample(range(sessions), _AWAITING_SESSIONS))
    stamps: list[datetime] = []
    #: Per minute: the regime-driven state (or None), whether Trend overrides,
    #: its direction, the Trap direction, the gamma sign for Chop, awaiting.
    plan: list[dict] = []

    clustering = 0.0
    phi = math.exp(-1.0 / 45.0)
    shock_sd = 0.35 * math.sqrt(1.0 - phi * phi)
    regime = 1
    trend_on, trend_dir = False, 1
    null_state = "CHOP"
    made = 0
    offset = 0
    while made < sessions:
        start = day + timedelta(days=offset)
        offset += 1
        if start.astimezone(ET).weekday() >= 5:
            continue
        session_idx = made
        made += 1
        level = math.exp(rng.gauss(0.0, 0.25))
        trap_dir = rng.choice((1, -1))
        short_for_chop = rng.random() < 0.5
        path = []
        for _ in range(minutes):
            if rng.random() < _REGIME_SWITCH:
                regime = _pick(rng, [(k, share) for k, (_, share) in enumerate(_REGIMES)])
            path.append(regime)
        for m in range(minutes):
            ts = start + timedelta(minutes=m)
            stamps.append(ts)
            clustering = phi * clustering + rng.gauss(0.0, shock_sd)
            if rng.random() < _TREND_SWITCH:
                trend_on = rng.random() < _TREND_SHARE
                trend_dir = rng.choice((1, -1))
            if rng.random() < _TREND_SWITCH:
                null_state = _pick(rng, _NULL_STATES)
            multiplier = _REGIMES[path[m]][0] if mode == "informative" else 1.0
            vol = _BASE_BPS * level * _time_of_day(m, minutes) * math.exp(clustering) * multiplier
            plan.append(
                {
                    "regime": path[min(m + _REGIME_LEAD, minutes - 1)],
                    "trend": trend_on,
                    "trend_dir": trend_dir,
                    "trap_dir": trap_dir,
                    "short": short_for_chop,
                    "awaiting": session_idx in awaiting_days and m < _AWAITING_MINUTES,
                    "null": null_state,
                    "day_start": session_idx * minutes,
                }
            )
            common = rng.gauss(0.0, vol)
            for sym in SYMBOLS:
                p0 = price[sym]
                p1 = p0 * (1.0 + (common + rng.gauss(0.0, 0.15 * vol)) / 10_000.0)
                up = abs(rng.gauss(0.0, 0.3 * vol)) / 10_000.0
                down = abs(rng.gauss(0.0, 0.3 * vol)) / 10_000.0
                bars[sym].append(
                    Bar(
                        ts=ts,
                        open=p0,
                        high=max(p0, p1) * (1.0 + up),
                        low=min(p0, p1) * (1.0 - down),
                        close=p1,
                    )
                )
                price[sym] = p1

    readings: dict[str, list[Reading]] = {s: [] for s in SYMBOLS}
    for sym in SYMBOLS:
        busy_cut = _busy_cuts(bars[sym], plan) if mode == "confounded" else {}
        for i, (ts, p) in enumerate(zip(stamps, plan)):
            state = _state(mode, p, bars[sym], i, busy_cut)
            readings[sym].append(
                Reading(timestamp=ts, inputs=_inputs(state, p["short"], rng), stored_state=None)
            )
    return readings, bars


def _busy_cuts(bars: list[Bar], plan: list[dict]) -> dict[int, float]:
    """Per half hour, the prior range above which ``_BUSY_SHARE`` of minutes sit."""
    by_bucket: dict[int, list[float]] = {}
    for i, p in enumerate(plan):
        v = _prior_busy(bars, i, p["day_start"])
        if v is not None:
            by_bucket.setdefault(time_slot(bars[i].ts), []).append(v)
    cuts = {}
    for b, values in by_bucket.items():
        values.sort()
        cuts[b] = values[int((1.0 - _BUSY_SHARE) * (len(values) - 1))]
    return cuts


def _state(mode: str, p: dict, bars: list[Bar], i: int, busy_cut: dict[int, float]) -> str:
    if p["awaiting"]:
        return "UNKNOWN"
    if p["trend"] and mode != "null":
        return "TREND_UP" if p["trend_dir"] > 0 else "TREND_DOWN"
    trap = "TRAP_REVERSAL" if p["trap_dir"] > 0 else "TRAP_SQUEEZE"
    if mode == "informative":
        return trap if p["regime"] == 2 else "CHOP"
    if mode == "confounded":
        v = _prior_busy(bars, i, p["day_start"])
        cut = busy_cut.get(time_slot(bars[i].ts))
        return trap if v is not None and cut is not None and v > cut else "CHOP"
    if p["null"] == "TREND":
        return "TREND_UP" if p["trend_dir"] > 0 else "TREND_DOWN"
    return trap if p["null"] == "TRAP" else "CHOP"


def build_world(mode: str, **kwargs) -> tuple[list[MoveRow], BuildCounts]:
    readings, bars = generate_world(mode, **kwargs)
    rows: list[MoveRow] = []
    counts = BuildCounts()
    for sym in SYMBOLS:
        r, c = build(sym, readings[sym], SessionBars(bars[sym]))
        rows.extend(r)
        counts.merge(c)
    return rows, counts


def check(mode: str, result: StudyResult) -> list[tuple[str, str, set[str], bool]]:
    """``(group, verdict, expected, passed)`` per state group."""
    out = []
    for g in result.groups:
        want = EXPECTED[mode][g.key]
        got = g.verdict.decision if g.verdict else ""
        out.append((g.key, got, want, got in want))
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
    titles = {g.key: g.title for g in GROUPS}
    for mode, result, passed in run_selftest():
        print(f"  {mode} world -> [{'PASS' if passed else 'FAIL'}]")
        by_key = {g.key: g for g in result.groups}
        for key, got, want, fine in check(mode, result):
            g = by_key[key]
            raw = g.cells[(BY_SYMBOL, PRIMARY_HORIZON, "pooled")].ratio.value
            e = g.primary().ratio
            matched = (
                f"{e.value:.2f} [{e.lo:.2f}, {e.hi:.2f}]"
                if e.value is not None and e.lo is not None and e.hi is not None
                else "n/a"
            )
            raw_text = "n/a" if raw is None else f"{raw:.2f}"
            print(
                f"    {titles[key]:<24} raw {raw_text}  vs comparable {matched:<18} "
                f"-> {got:<18} " + ("ok" if fine else "EXPECTED " + " or ".join(sorted(want)))
            )
        ok = ok and passed
    return 0 if ok else 1
