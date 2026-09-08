"""Synthetic end-to-end check — no database, no market data.

This is plumbing verification, never a result.  **Every number this command
prints is invented.**  It says nothing about how opening-range extensions or
gamma levels actually behave.

It builds sessions with a KNOWN latent driver, pushes them through the real
range, event, level, feature and cohort code, and asserts five things:

1. **The opening range freezes.**  Extending a session with bars that would
   have widened the range does not change ``ORH``/``ORL``/``R``.
2. **Features contain no look-ahead.**  The same event, featurised against a
   series whose post-touch bars have been replaced with absurd numbers,
   produces an identical vector.
3. **Lead time is honoured.**  No selected frame is ever newer than
   ``touch - gamma_min_lead_seconds``, at every lead in the sweep.
4. **A planted effect is recovered.**  Sessions generated so that a
   pre-existing wall genuinely raises the reversion rate come out with the
   confluence cohort above the no-confluence cohort.
5. **No effect is invented.**  The same generator with the effect switched off
   produces cohorts that are not separated.

The generator places walls BEFORE walking the price path, and conditions the
path on them — not the other way round.  Planting walls where price happened to
revert would create an association that a look-ahead bug could also "find",
which would make this test agree with a broken harness.
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta
from typing import Any

from research.msi_regime_excursion.excursion import ET, Bar
from research.or_gamma_confluence.cohorts import build_cohorts, summarize
from research.or_gamma_confluence.config import CLOCK_VISIBLE, ResearchConfig
from research.or_gamma_confluence.events import extract_touch_events
from research.or_gamma_confluence.features import SessionContext, build_features
from research.or_gamma_confluence.levels import GammaTimeline, build_snapshot, confluence_at
from research.or_gamma_confluence.outcomes import measure_outcome
from research.or_gamma_confluence.ranges import build_ladder, build_opening_range

__all__ = ["synthetic_session", "run_selftest"]

_BASE = 29500.0
_SESSION_BARS = 390


def _open(session: date) -> datetime:
    return datetime.combine(session, time(9, 30), tzinfo=ET)


def synthetic_session(
    session: date,
    rng: random.Random,
    cfg: ResearchConfig,
    *,
    wall_prob: float = 0.45,
    effect: float = 0.55,
    step: float = 6.0,
) -> tuple[list[Bar], list[dict[str, Any]], set[int]]:
    """One invented session: bars, gamma frames, and which rungs got a wall.

    Walls are chosen up front from ``rng`` alone — never from the path — and
    the path is then walked with a reversion impulse that fires only near a
    rung that already has one.  That ordering is what makes the planted effect
    a real conditional probability rather than an artefact.
    """
    start = _open(session)

    # ── Opening range: 5 bars of chop, so R is stable across seeds. ──
    bars: list[Bar] = []
    price = _BASE
    for i in range(cfg.opening_range_minutes):
        hi = price + rng.uniform(4.0, 9.0)
        lo = price - rng.uniform(4.0, 9.0)
        close = rng.uniform(lo, hi)
        bars.append(Bar(start + timedelta(minutes=i), price, hi, lo, close))
        price = close

    orange, why = build_opening_range(bars, session, "NQ", cfg)
    if orange is None:  # pragma: no cover - the generator keeps R healthy
        raise RuntimeError(f"synthetic OR rejected: {why}")
    ladder = build_ladder(orange, cfg)

    # ── Walls, decided before a single post-OR bar exists. ──
    walled = {r.index for r in ladder.rungs if rng.random() < wall_prob}
    wall_prices = {
        r.index: r.price + rng.uniform(-1.5, 1.5) for r in ladder.rungs if r.index in walled
    }

    # ── Walk the path, reacting to walls that are already there. ──
    drift = rng.choice((-1.0, 1.0)) * rng.uniform(0.05, 0.35)
    for i in range(cfg.opening_range_minutes, _SESSION_BARS):
        impulse = 0.0
        for rung in ladder.rungs:
            if abs(price - rung.price) <= step and rung.index in walled:
                if rng.random() < effect:
                    # Push back toward the anchor — the planted reversion.
                    impulse = -step * 1.2 if rung.index > 0 else step * 1.2
                break
        move = rng.gauss(drift, step) + impulse
        close = price + move
        hi = max(price, close) + abs(rng.gauss(0, step * 0.25))
        lo = min(price, close) - abs(rng.gauss(0, step * 0.25))
        bars.append(Bar(start + timedelta(minutes=i), price, hi, lo, close))
        price = close

    # ── Gamma frames: one a minute, walls fixed for the session. ──
    frames: list[dict[str, Any]] = []
    prices = sorted(wall_prices.values())
    for i in range(_SESSION_BARS):
        ts = start + timedelta(minutes=i)
        above = [p for p in prices if p >= _BASE]
        below = [p for p in prices if p < _BASE]
        frames.append(
            {
                "timestamp": ts,
                "created_at": ts + timedelta(seconds=8),
                "call_wall": above[0] if above else None,
                "put_wall": below[-1] if below else None,
                "call_wall_strength": 1.0e9,
                "put_wall_strength": 9.0e8,
                "gamma_flip_point": _BASE,
                "gamma_flip_raw": None,
                "flip_distance": 0.0,
                "max_pain": above[0] if above else _BASE,
                "pin_strike": below[-1] if below else _BASE,
                "pin_score": 0.5,
                "pin_confidence": 0.5,
                "max_gamma_strike": None,
                "total_net_gex": 1.0e9,
                "net_gex_at_spot": 5.0e8,
                "local_gex": 2.0e8,
                "convexity_risk": 1.0,
                # Every wall, so confluence can see more than the two headline
                # levels.  Priced as a synthetic ranked ladder.
                "_all_walls": list(prices),
            }
        )
    return bars, frames, walled


def _timeline(frames: list[dict[str, Any]], cfg: ResearchConfig) -> GammaTimeline:
    return GammaTimeline(build_snapshot(f, cfg, spot=_BASE) for f in frames)


def _rows_for(
    session: date, rng: random.Random, cfg: ResearchConfig, *, effect: float
) -> list[dict[str, Any]]:
    bars, frames, _ = synthetic_session(session, rng, cfg, effect=effect)
    orange, _ = build_opening_range(bars, session, "NQ", cfg)
    ladder = build_ladder(orange, cfg)
    events = extract_touch_events("NQ", orange, ladder, bars, cfg, tick=0.25)
    tl = _timeline(frames, cfg)
    ctx = SessionContext.build("NQ", bars, cfg)
    rows = []
    for i, ev in enumerate(events):
        snap = tl.as_of(ev.touched_at, cfg.gamma_min_lead_seconds)
        conf = confluence_at(snap, ev.level_price, max_distance=120.0) if snap else None
        rows.append(
            {
                **ev.to_dict(),
                **build_features(
                    ev, ctx, orange, ladder, cfg, snapshot=snap, confluence=conf, prior=events[:i]
                ),
                **measure_outcome(ev, ctx.series, cfg),
            }
        )
    return rows


def run_selftest(sessions: int = 120, seed: int = 7, verbose: bool = True) -> int:
    """Run every plumbing assertion.  Returns a process exit code."""
    cfg = ResearchConfig(availability_clock=CLOCK_VISIBLE, gamma_min_lead_seconds=120)
    failures: list[str] = []

    def check(ok: bool, label: str) -> None:
        if verbose:
            print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
        if not ok:
            failures.append(label)

    print("Synthetic plumbing check — every number below is invented.\n")

    # ── 1. The opening range freezes ──────────────────────────────────
    print("1. Opening range is fixed once its window closes")
    rng = random.Random(seed)
    s0 = date(2026, 6, 1)
    bars, frames, _ = synthetic_session(s0, rng, cfg)
    orange_a, _ = build_opening_range(bars, s0, "NQ", cfg)
    # Re-run over a session extended with a bar that WOULD have widened it.
    wider = list(bars) + [
        Bar(bars[-1].ts + timedelta(minutes=1), _BASE, _BASE + 5_000, _BASE - 5_000, _BASE)
    ]
    orange_b, _ = build_opening_range(wider, s0, "NQ", cfg)
    check(
        orange_a.high == orange_b.high and orange_a.low == orange_b.low,
        f"later bars do not move ORH/ORL (R={orange_a.width:.2f})",
    )

    # ── 2. No look-ahead in the feature vector ────────────────────────
    print("\n2. Features do not move when the future is poisoned")
    ladder = build_ladder(orange_a, cfg)
    events = extract_touch_events("NQ", orange_a, ladder, bars, cfg, tick=0.25)
    tl = _timeline(frames, cfg)
    if not events:  # pragma: no cover
        check(False, "generator produced no events")
    else:
        ev = events[len(events) // 2]
        poisoned = [b if b.ts <= ev.touched_at else Bar(b.ts, 1e6, 1e6, 1e6, 1e6) for b in bars]
        ctx_clean = SessionContext.build("NQ", bars, cfg)
        ctx_poison = SessionContext.build("NQ", poisoned, cfg)
        snap = tl.as_of(ev.touched_at, cfg.gamma_min_lead_seconds)
        conf = confluence_at(snap, ev.level_price, max_distance=120.0) if snap else None
        idx = events.index(ev)
        f_clean = build_features(
            ev, ctx_clean, orange_a, ladder, cfg, snapshot=snap, confluence=conf, prior=events[:idx]
        )
        f_poison = build_features(
            ev,
            ctx_poison,
            orange_a,
            ladder,
            cfg,
            snapshot=snap,
            confluence=conf,
            prior=events[:idx],
        )
        diffs = [k for k in f_clean if f_clean[k] != f_poison.get(k)]
        check(not diffs, f"feature vector identical under poisoned future ({diffs[:4]})")

    # ── 3. Lead time is honoured at every sweep value ─────────────────
    print("\n3. No frame newer than (touch - lead) is ever selected")
    ok = True
    for lead in (0, 30, 60, 120, 180):
        for ev in events:
            snap = tl.as_of(ev.touched_at, lead)
            if snap is None:
                continue
            age = (ev.touched_at - snap.available_at).total_seconds()
            if age < lead - 1e-9:
                ok = False
                break
    check(ok, "lead honoured at 0/30/60/120/180 s")

    # ── 4 & 5. A planted effect is found; an absent one is not ────────
    print("\n4/5. Planted effect recovered; absent effect not invented")
    for effect, expect_gap in ((0.55, True), (0.0, False)):
        rng = random.Random(seed + int(effect * 100))
        rows: list[dict[str, Any]] = []
        for i in range(sessions):
            rows.extend(_rows_for(date(2026, 6, 1) + timedelta(days=i), rng, cfg, effect=effect))
        cs = {
            c.key: c
            for c in build_cohorts(confluence_distance=10.0, min_extension=2.0, min_broken=2)
        }
        with_c = summarize(cs["confluence"].select(rows))
        without = summarize(cs["no_confluence"].select(rows))
        a, b = with_c["reversal_rate"], without["reversal_rate"]
        gap = (a - b) if (a is not None and b is not None) else None
        label = "effect ON " if expect_gap else "effect OFF"
        detail = (
            f"{label}: confluence {a:.3f} (n={with_c['n_resolved']}) vs "
            f"no-confluence {b:.3f} (n={without['n_resolved']}), gap {gap:+.3f}"
            if gap is not None
            else f"{label}: insufficient resolved events"
        )
        if gap is None:
            check(False, detail)
        elif expect_gap:
            check(gap > 0.02, detail)
        else:
            check(abs(gap) < 0.05, detail)

    print()
    if failures:
        print(f"FAILED {len(failures)} check(s):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("All plumbing checks passed. (Numbers above are synthetic.)")
    return 0
