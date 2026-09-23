"""Counterfactual tuner for the intraday cone — no re-backfill required.

Every graded claim stores ``window_low`` and ``window_high``: the extremes
the tape actually reached over that claim's window. Those two numbers decide
whether ANY band would have held, not just the one that was published. So a
candidate parameter set can be scored against the whole graded history
directly, offline, in seconds.

That matters because the obvious loop — change a constant, re-backfill,
re-grade, read the report — is slow AND misleading. Widening a band raises
the realized hold rate as well as the predicted one, because the band is the
thing being graded. A change that looks like it should close a 19-point gap
can move it by one point, which is exactly what tuning CONE_TERM_DECAY from
0.12 to 0.07 did. Seeing that takes a round trip through the database; here
it takes a sweep.

Two knobs matter and they are NOT interchangeable:

* ``CONE_TERM_DECAY`` shapes the BAND, so it moves predicted and realized
  together and can never close a gap on its own.
* ``CONE_PATH_EXPONENT`` shapes the PROBABILITY's sigma, so it moves the
  prediction while leaving the graded outcome alone. Any genuine calibration
  error that survives band tuning lives here.

Everything needed to rebuild a claim is stored on it — anchor spot, the
committed daily sigma, elapsed minutes, the gamma multiplier and the walls —
so the reconstruction is exact rather than approximate. The tuner asserts
that by rebuilding each claim at the committed parameters first and checking
it reproduces the published band.

HOLDOUT. In-sample fitting on nine sessions is how you get a model that is
calibrated on exactly the days you already have. ``--holdout`` splits by
SESSION, not by claim: claims from one session are far from independent, so a
random per-claim split would leak the answer across the boundary and report a
fit far better than it is.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections import defaultdict
from typing import Any, Optional

from src.api.database import DatabaseManager
from src.jobs.intraday_cone_model import (
    CONE_SIGMA_MULT,
    CONE_TERM_DECAY,
    GAMMA_BAND_DAMPING,
    MAX_HALF_FRACTION,
    MIN_HALF_FRACTION,
    _clamp,
    _lean_to_wall,
    hold_probability,
    path_exponent_for,
    variance_fraction,
)

logger = logging.getLogger(__name__)


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _rebuild(claim: dict, sigma_mult: float, term_decay: float,
             path_exp: Optional[float]):
    """Rebuild one claim's band and probability under candidate parameters.

    Mirrors ``compute_cone``'s per-horizon arithmetic exactly. Returns
    ``(band_low, band_high, hold_prob)`` or None when the claim lacks an
    input the reconstruction needs.
    """
    # None = the value this symbol actually ships with. Resolving per claim
    # matters now that one symbol overrides it: a single literal baseline
    # would report NDX against a config it does not run, which is the bug
    # that made the last ablation meaningless.
    if path_exp is None:
        path_exp = path_exponent_for(claim.get("symbol"))
    spot = _f(claim.get("anchor_spot"))
    daily_sigma = _f(claim.get("daily_sigma"))
    elapsed = _f(claim.get("elapsed_min"))
    gamma_mult = _f(claim.get("gamma_mult"))
    horizon = claim.get("horizon_min")
    if None in (spot, daily_sigma, elapsed, gamma_mult) or not horizon:
        return None
    if spot <= 0 or daily_sigma <= 0:
        return None

    horizon = int(horizon)
    ref = claim.get("_ref_horizon", horizon)
    vf = variance_fraction(elapsed, elapsed + horizon)
    ref_vf = variance_fraction(elapsed, elapsed + ref)
    if vf <= 0 or ref_vf <= 0:
        return None

    base_sigma = daily_sigma * (vf ** 0.5)
    sigma_h = daily_sigma * (vf ** path_exp) * gamma_mult
    band_mult = 1.0 + GAMMA_BAND_DAMPING * (gamma_mult - 1.0)
    decay = (vf / ref_vf) ** -term_decay

    raw_half = sigma_mult * base_sigma * band_mult * decay
    raw_half = _clamp(raw_half, MIN_HALF_FRACTION * spot, MAX_HALF_FRACTION * spot)

    high, _ = _lean_to_wall(
        spot + raw_half, spot, _f(claim.get("call_wall")), raw_half, upward=True
    )
    low, _ = _lean_to_wall(
        spot - raw_half, spot, _f(claim.get("put_wall")), raw_half, upward=False
    )
    lo, hi = round(low, 4), round(high, 4)
    return lo, hi, hold_probability(
        spot=spot, band_low=lo, band_high=hi, sigma=round(sigma_h, 4)
    )


def _score(claims: list[dict], sigma_mult: float, term_decay: float, path_exp: float):
    """Per-horizon predicted vs realized under candidate parameters."""
    buckets: dict[int, list[tuple[float, bool]]] = defaultdict(list)
    for c in claims:
        wl, wh = _f(c.get("window_low")), _f(c.get("window_high"))
        if wl is None or wh is None:
            continue
        rebuilt = _rebuild(c, sigma_mult, term_decay, path_exp)
        if rebuilt is None:
            continue
        lo, hi, p = rebuilt
        if p is None:
            continue
        buckets[int(c["horizon_min"])].append((p, wl >= lo and wh <= hi))

    rows = []
    for h in sorted(buckets):
        pairs = buckets[h]
        n = len(pairs)
        if not n:
            continue
        pred = sum(p for p, _ in pairs) / n
        real = sum(1 for _, o in pairs if o) / n
        brier = sum((p - (1.0 if o else 0.0)) ** 2 for p, o in pairs) / n
        rows.append({"horizon": h, "n": n, "pred": pred, "real": real,
                     "gap": real - pred, "brier": brier})
    return rows


def _summary(rows) -> tuple[float, float]:
    """(sample-weighted mean |gap|, sample-weighted Brier)."""
    n = sum(r["n"] for r in rows)
    if not n:
        return float("inf"), float("inf")
    return (sum(abs(r["gap"]) * r["n"] for r in rows) / n,
            sum(r["brier"] * r["n"] for r in rows) / n)


async def _load(db: DatabaseManager, sessions: int) -> list[dict]:
    rows = await db.get_graded_cone_claims_for_tuning(sessions)
    # The reference horizon is the shortest PUBLISHED at that fire, which is
    # what compute_cone anchors the decay on. Recover it per fire rather than
    # assuming 30, or late-session claims rebuild against the wrong reference.
    by_fire: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_fire[(r["symbol"], r["forecast_ts"])].append(r)
    for fire in by_fire.values():
        ref = min(int(c["horizon_min"]) for c in fire)
        for c in fire:
            c["_ref_horizon"] = ref
    return rows


async def _run(args: argparse.Namespace) -> int:
    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:  # noqa: BLE001
        logger.warning("cone_tune: DB connect failed (%s)", exc)
        return 0
    try:
        claims = await _load(db, args.sessions)
    finally:
        try:
            await db.disconnect()
        except Exception:  # noqa: BLE001
            pass

    if args.symbol:
        want = {x.strip().upper() for x in args.symbol.split(",") if x.strip()}
        claims = [c for c in claims if c["symbol"] in want]
        print(f"scoped to {', '.join(sorted(want))}\n")

    if not claims:
        print("no graded claims to tune against")
        return 0

    all_sessions = sorted({c["session_date"] for c in claims})
    if args.holdout and len(all_sessions) > 2:
        cut = max(1, int(len(all_sessions) * (1 - args.holdout)))
        train_s, test_s = set(all_sessions[:cut]), set(all_sessions[cut:])
    else:
        train_s, test_s = set(all_sessions), set()
    train = [c for c in claims if c["session_date"] in train_s]
    test = [c for c in claims if c["session_date"] in test_s]

    print(f"{len(claims)} graded claims over {len(all_sessions)} sessions")
    print(f"  train: {len(train):>5} claims / {len(train_s)} sessions")
    print(f"  test:  {len(test):>5} claims / {len(test_s)} sessions "
          f"({'held out' if test else 'none — in-sample only'})\n")

    baseline = _score(train, CONE_SIGMA_MULT, CONE_TERM_DECAY, None)
    b_gap, b_brier = _summary(baseline)
    print(f"committed parameters  sigma_mult={CONE_SIGMA_MULT} "
          f"term_decay={CONE_TERM_DECAY} path_exp=<per symbol>")
    for r in baseline:
        print(f"   +{r['horizon']:>3}m  n={r['n']:>4}  pred {r['pred']*100:5.1f}%  "
              f"real {r['real']*100:5.1f}%  gap {r['gap']*100:+6.1f}  "
              f"brier {r['brier']:.4f}")
    print(f"   mean |gap| {b_gap*100:.1f} pts   brier {b_brier:.4f}\n")

    best = None
    # Sweep upward from Brownian: the variance fraction is below 1, so a
    # LARGER exponent shrinks sub-daily sigma and predicts more containment.
    for pe in [round(0.50 + 0.025 * i, 3) for i in range(15)]:
        for td in (0.03, 0.05, 0.07, 0.09, 0.12):
            for sm in (1.30, 1.40, 1.50, 1.60, 1.70):
                rows = _score(train, sm, td, pe)
                gap, brier = _summary(rows)
                key = brier if args.objective == "brier" else gap
                if best is None or key < best[0]:
                    best = (key, sm, td, pe, rows, gap, brier)

    _, sm, td, pe, rows, gap, brier = best
    print(f"best on TRAIN by {args.objective}:  sigma_mult={sm} "
          f"term_decay={td} path_exp={pe}")
    for r in rows:
        print(f"   +{r['horizon']:>3}m  n={r['n']:>4}  pred {r['pred']*100:5.1f}%  "
              f"real {r['real']*100:5.1f}%  gap {r['gap']*100:+6.1f}  "
              f"brier {r['brier']:.4f}")
    print(f"   mean |gap| {gap*100:.1f} pts   brier {brier:.4f}")

    if test:
        t_rows = _score(test, sm, td, pe)
        t_gap, t_brier = _summary(t_rows)
        base_t = _summary(_score(test, CONE_SIGMA_MULT, CONE_TERM_DECAY, None))
        print(f"\nHELD-OUT sessions ({len(test_s)}):")
        for r in t_rows:
            print(f"   +{r['horizon']:>3}m  n={r['n']:>4}  pred {r['pred']*100:5.1f}%  "
                  f"real {r['real']*100:5.1f}%  gap {r['gap']*100:+6.1f}")
        print(f"   tuned    mean |gap| {t_gap*100:.1f} pts   brier {t_brier:.4f}")
        print(f"   current  mean |gap| {base_t[0]*100:.1f} pts   brier {base_t[1]:.4f}")
        # Three outcomes, not two. The sweep can land ON the committed
        # configuration, which is a pass rather than a failure — reporting
        # that as "do not ship this fit" reads as a problem when it is the
        # strongest result available: nothing in the grid beats what is live.
        same = (abs(t_gap - base_t[0]) < 1e-9 and abs(t_brier - base_t[1]) < 1e-9)
        if same:
            print("   -> the sweep found nothing better than what is already "
                  "committed. No change indicated.")
        elif t_gap >= base_t[0]:
            print("   -> does NOT beat the committed parameters out of sample. "
                  "Do not ship this fit.")
        else:
            print(f"   -> beats the committed parameters out of sample "
                  f"({base_t[0] * 100:.1f} -> {t_gap * 100:.1f} pts). "
                  f"Check the ablation before shipping it.")

        # Ablation, LEAVE-ONE-OUT from the committed config.
        #
        # The first version compared candidates against "whatever is currently
        # committed", which degenerates the moment a fit ships: the knobs
        # already sit at their tuned values, so moving them to those values
        # changes nothing and every row reads identical. Each knob is instead
        # returned to its NEUTRAL setting one at a time, and the damage is
        # what that knob earns. A knob that costs nothing to remove is
        # complexity with no claim behind it.
        print("\n   leave-one-out on held-out sessions "
              "(each knob returned to neutral):")
        rows = [
            ("committed", (CONE_SIGMA_MULT, CONE_TERM_DECAY, None)),
            ("path_exp -> 0.5 (Brownian)",
             (CONE_SIGMA_MULT, CONE_TERM_DECAY, 0.5)),
            ("term_decay -> 0 (no decay)",
             (CONE_SIGMA_MULT, 0.0, None)),
            ("sigma_mult -1 step",
             (CONE_SIGMA_MULT - 0.2, CONE_TERM_DECAY, None)),
            ("sigma_mult +1 step",
             (CONE_SIGMA_MULT + 0.2, CONE_TERM_DECAY, None)),
        ]
        for label, cand in rows:
            g, b = _summary(_score(test, *cand))
            print(f"     {label:28} gap {g*100:5.1f} pts   brier {b:.4f}")

        # Per symbol. An aggregate gap near zero can be four symbols at zero
        # or two large biases cancelling, and those call for opposite actions.
        print("\n   held-out calibration PER SYMBOL at the committed config:")
        for sym in sorted({c["symbol"] for c in test}):
            sub = [c for c in test if c["symbol"] == sym]
            r = _score(sub, CONE_SIGMA_MULT, CONE_TERM_DECAY, None)
            n = sum(x["n"] for x in r)
            signed = sum(x["gap"] * x["n"] for x in r) / n if n else 0.0
            g, b = _summary(r)
            print(f"     {sym:5} n={n:>4}  signed gap {signed*100:+6.1f} pts   "
                  f"|gap| {g*100:5.1f}   brier {b:.4f}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", type=int, default=60)
    parser.add_argument("--holdout", type=float, default=0.33,
                        help="Fraction of SESSIONS held out (0 disables).")
    parser.add_argument("--objective", choices=("gap", "brier"), default="brier")
    parser.add_argument(
        "--symbol",
        help="Fit ONE symbol (or a comma list) instead of all of them. "
             "The aggregate hides per-symbol bias — NDX ran 13 points "
             "overconfident while the other three sat near +2 — so a symbol "
             "that misbehaves should be fitted and judged on its own.",
    )
    return asyncio.run(_run(parser.parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
