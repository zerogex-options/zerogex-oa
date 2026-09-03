"""Synthetic end-to-end check — runs with no database and no market data.

This is plumbing verification, never a result.  It builds sessions with a
KNOWN latent driver, pushes them through the real labelling, feature and model
code, and asserts four things:

1. The labeller separates confirmed breaks from pierces.  A path that poked
   through the wall and fell straight back must come out ``held``.
2. A sustained move IS labelled a break.
3. The features contain no lookahead.  The same event, featurised against a
   series whose post-test values have been replaced with absurd numbers, must
   produce an identical vector.
4. The model recovers a driver that is genuinely there, and reports no skill
   when the driver is noise.

Numbers printed by this command are invented.  Nothing here says anything
about how walls actually behave.
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta
from typing import Any, Optional

from research.wall_break_odds.events import (
    ET,
    EventConfig,
    PriceBar,
    WallFrame,
    extract_wall_tests,
)
from research.wall_break_odds.features import SummarySeries, build_features
from research.wall_break_odds.model import Row, base_rate, evaluate, univariate_screen

__all__ = ["synthetic_session", "run_selftest"]


def _open(session: date) -> datetime:
    return datetime.combine(session, time(9, 30), tzinfo=ET)


def _flat_path(session: date, wall: float, closes) -> tuple[list[WallFrame], list[PriceBar]]:
    """Frames and bars for a hand-specified close path."""
    start = _open(session)
    bars = [
        PriceBar(
            ts=start + timedelta(minutes=m),
            high=max(c, wall * 0.9999),
            low=min(c, wall * 0.9999),
            close=c,
        )
        for m, c in enumerate(closes)
    ]
    frames = [
        WallFrame(ts=start + timedelta(minutes=m), call_wall=wall, put_wall=wall * 0.98)
        for m in range(len(closes))
    ]
    return frames, bars


def synthetic_session(
    session: date,
    rng: random.Random,
    *,
    wall: float = 500.0,
    pressure: Optional[float] = None,
    informative: bool = True,
    noise_rng: Optional[random.Random] = None,
) -> tuple[list[WallFrame], list[PriceBar], list[dict[str, Any]], float]:
    """One invented session: frames, bars, summary rows, and the latent driver.

    ``pressure`` in [0, 1] is the hidden tendency of the tape to push through
    the wall.  When ``informative`` is True the published wall strength is made
    to carry it (strong wall -> low pressure), which is the relationship the
    model should recover; when False the strength is pure noise and the model
    should report no skill.
    """
    pressure = rng.random() if pressure is None else pressure
    start = _open(session)
    drift = (pressure - 0.5) * 0.00010
    vol = 0.0005

    price = wall * 0.9985
    bars: list[PriceBar] = []
    frames: list[WallFrame] = []
    rows: list[dict[str, Any]] = []
    # In the null arm the strength must be drawn from a stream that shares no
    # state with the one driving the tape. Taking the next value off the same
    # generator is NOT independent enough in practice: the number of draws a
    # session consumes varies with its own path, which leaks a weak
    # correlation between "strength" and "pressure" and makes the null arm
    # flag features it should not. A separate generator removes the coupling
    # by construction rather than by hoping.
    if informative:
        strength = (2.0 - pressure) * 1.0e9
    else:
        strength = (noise_rng or random.Random(0)).uniform(0.5, 2.5) * 1.0e9

    for minute in range(390):
        ts = start + timedelta(minutes=minute)
        price *= 1.0 + drift + rng.gauss(0.0, vol)
        high = price * (1.0 + abs(rng.gauss(0.0, vol / 2)))
        low = price * (1.0 - abs(rng.gauss(0.0, vol / 2)))
        bars.append(PriceBar(ts=ts, high=high, low=low, close=price))
        frames.append(WallFrame(ts=ts, call_wall=wall, put_wall=wall * 0.98))
        rows.append(
            {
                "timestamp": ts,
                "call_wall": wall,
                "put_wall": wall * 0.98,
                "call_wall_strength": strength * (1.0 + rng.gauss(0.0, 0.01)),
                "put_wall_strength": strength * 0.8,
                "total_net_gex": 2.0e9,
                "gamma_flip_point": wall * 0.995,
                "flip_distance": (price - wall * 0.995) / price,
                "local_gex": 5.0e8,
                "convexity_risk": 1.0e11,
            }
        )
    return frames, bars, rows, pressure


def no_lookahead(rows: list[dict[str, Any]], bars: list[PriceBar], event: Any) -> bool:
    """Featurise twice — once honestly, once with the future poisoned."""
    clean = build_features(event, SummarySeries.from_rows(rows), bars)
    poisoned_rows = [
        (
            r
            if r["timestamp"] <= event.tested_at
            else {**r, "call_wall_strength": 9.9e18, "total_net_gex": -9.9e18, "call_wall": 1.0}
        )
        for r in rows
    ]
    poisoned_bars = [
        b if b.ts <= event.tested_at else PriceBar(ts=b.ts, high=1e9, low=0.0, close=1e9)
        for b in bars
    ]
    dirty = build_features(event, SummarySeries.from_rows(poisoned_rows), poisoned_bars)
    for key in clean:
        a, b = clean[key], dirty[key]
        if a is None and b is None:
            continue
        if a is None or b is None or abs(float(a) - float(b)) > 1e-9:
            print(f"    LOOKAHEAD in {key}: clean={a} poisoned={b}")
            return False
    return True


def _collect(n_sessions: int, rng: random.Random, informative: bool) -> list[Row]:
    rows_out: list[Row] = []
    noise_rng = random.Random(98765)
    day = date(2026, 1, 5)
    made = 0
    while made < n_sessions:
        day += timedelta(days=1)
        if day.weekday() >= 5:
            continue
        made += 1
        frames, bars, rows, _ = synthetic_session(
            day, rng, informative=informative, noise_rng=noise_rng
        )
        summary = SummarySeries.from_rows(rows)
        for event in extract_wall_tests("TEST", day, frames, bars, EventConfig()):
            if event.outcome == "censored" or event.side != "call":
                continue
            rows_out.append(
                Row(
                    session=day,
                    side=event.side,
                    broke=1 if event.outcome == "broke" else 0,
                    features=build_features(event, summary, bars),
                )
            )
    return rows_out


def run_selftest(n_sessions: int = 200, seed: int = 7) -> int:
    """Returns a process exit code: 0 all good, 1 something is wrong."""
    rng = random.Random(seed)
    print("Synthetic pipeline check — every number below is invented.\n")
    ok = True
    session = date(2026, 6, 1)
    wall = 500.0

    # --- 1. a pierce is not a break -------------------------------------
    closes = [wall * 1.002 if 30 <= m < 33 else wall * 0.998 for m in range(150)]
    events = extract_wall_tests("TEST", session, *_flat_path(session, wall, closes), EventConfig())
    calls = [e for e in events if e.side == "call"]
    if not calls:
        print("  FAIL a path that reached the wall produced no test event")
        ok = False
    elif any(e.outcome == "broke" for e in calls):
        print("  FAIL a 3-minute pierce was labelled a break")
        ok = False
    else:
        print(f"  ok   3-minute pierce labelled '{calls[0].outcome}', not 'broke'")

    # --- 2. a sustained move IS a break ---------------------------------
    closes = [wall * 1.002 if m >= 30 else wall * 0.998 for m in range(150)]
    events = extract_wall_tests("TEST", session, *_flat_path(session, wall, closes), EventConfig())
    calls = [e for e in events if e.side == "call"]
    if not calls or calls[0].outcome != "broke":
        got = calls[0].outcome if calls else "no event"
        print(f"  FAIL a sustained move was labelled '{got}'")
        ok = False
    else:
        print(f"  ok   sustained move labelled 'broke' after {calls[0].minutes_to_resolve:.0f} min")

    # --- 3. no lookahead in the feature vector --------------------------
    frames, bars, rows, _ = synthetic_session(date(2026, 6, 2), rng)
    evs = extract_wall_tests("TEST", date(2026, 6, 2), frames, bars, EventConfig())
    if not evs:
        print("  skip no event generated for the lookahead check")
    elif no_lookahead(rows, bars, evs[0]):
        print("  ok   feature vector is invariant to poisoned future data")
    else:
        print("  FAIL feature vector moved when the future changed")
        ok = False

    # --- 4. recover a real driver; report none when there isn't one -----
    #
    # The discriminating column is wall strength: the informative arm wires it
    # to the latent driver, the null arm draws it from an independent stream.
    # Everything else in the vector (time of day, minutes to close, test
    # ordinal, flip distance) is genuinely associated with the outcome in BOTH
    # arms — a test late in the session really does have less room to resolve —
    # so those are expected hits, not false ones. Asserting "no hits at all" in
    # the null arm would be asserting something untrue about the generator.
    flagged: dict[bool, list[str]] = {}
    for informative, label in ((True, "embedded driver"), (False, "pure noise")):
        model_rows = _collect(n_sessions, random.Random(seed + int(informative)), informative)
        rates = base_rate(model_rows)
        ev = evaluate(model_rows, n_folds=4)
        overall = rates["overall"]
        skill = (ev.get("oos") or {}).get("skill")
        rate = overall["rate"]
        print(
            f"\n  [{label}] events={overall['n']} "
            f"break rate={'n/a' if rate is None else format(rate, '.2f')}"
        )
        print(f"    evaluate status: {ev.get('status')}  skill={skill}")
        if ev.get("status") == "ok" and informative and (skill is None or skill <= 0):
            print("    FAIL an embedded driver produced no out-of-sample skill")
            ok = False
        screen = univariate_screen(model_rows)
        hits = [s["feature"] for s in screen if s.get("significant_fdr_05")]
        flagged[informative] = hits
        print(f"    FDR-significant: {hits or 'none'}")

    if "wall_strength_log" not in flagged.get(True, []):
        print("\n  FAIL wall strength was wired to the driver and the screen missed it")
        ok = False
    elif "wall_strength_log" in flagged.get(False, []):
        print("\n  FAIL wall strength was independent noise and the screen flagged it")
        ok = False
    else:
        print("\n  ok   the screen separates a wired driver from an independent one")

    print("\nSelf-test", "PASSED" if ok else "FAILED")
    return 0 if ok else 1
