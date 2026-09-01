"""What each candidate z-score denominator would do to the Gamma Regime read.

The QUIET cut is the one number on the Gamma Shift card a reader acts on by
NOT looking further, so the denominator that feeds it has to be chosen from
this symbol's own measured distribution rather than from a plausible-sounding
argument about estimators.  Two arguments have already turned out to be wrong
in exactly that way:

* the population standard deviation, which let a chain's few violent sessions
  set the scale for its ordinary ones (SPX read 48% QUIET against SPY's 17%
  on the same index); and
* the fix for it, whose measured effect was cancelled out by a second change
  bundled into the same patch.

So this tool measures instead.  For every stored symbol it reports the shape
of the shift distribution on BOTH axes and, for each candidate scale, the
QUIET rate and state mix that scale would actually have produced over the
stored history.

READ-ONLY.  It runs SELECTs and writes nothing.

Reading the output
------------------
``sd/mean|x|`` is ``sqrt(pi/2)`` = 1.2533 for anything Gaussian.  Above that,
the tail is heavier than normal and a squared-moment scale is being set by it.

``mean|x|/med|x|`` is 1.0 for a symmetric magnitude distribution and rises
with right-skew.  This is the one that decides between the two robust
candidates: where it is large, the MEDIAN session is much smaller than the
mean one, so a mean-based scale still reports the median session as nothing
happening — which is the failure the card shows as "always QUIET".

``quiet%`` should land near 25%: the fraction of a bivariate normal inside
the ``QUIET_Z`` = 0.75 ring.  SPY vs SPX is the cross-check that settles a
disagreement, because they track the same index.

Usage:
    python -m src.tools.regime_scale_report
    python -m src.tools.regime_scale_report --symbols SPY SPX
    python -m src.tools.regime_scale_report --json /tmp/regime_scales.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import sys
from typing import Any, Callable, Dict, List, Optional, Sequence

from src.analytics import regime_shift as rs
from src.api.database import DatabaseManager
from src.tools.regime_session_refresh import default_symbols

logger = logging.getLogger(__name__)

#: Rescales a median absolute value onto the standard deviation's scale.
#: 1/0.6745 — for a normal, the median |x| is 0.6745 sigma.
_MEDIAN_TO_SIGMA = 1.4826


def median_abs_scale(values: Sequence[float]) -> Optional[float]:
    """Median absolute value, rescaled to the standard deviation's scale.

    The fully robust candidate: unlike a mean, a median is unmoved by how
    large the largest sessions are, so a right-skewed magnitude distribution
    cannot pull the yardstick above its own typical day.  Costs statistical
    efficiency (~37% against the standard deviation's 100%), which on a
    60-session window is a denominator that wobbles as the window rolls.
    """
    finite = [abs(v) for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
    if len(finite) < 2:
        return None
    finite.sort()
    mid = len(finite) // 2
    med = finite[mid] if len(finite) % 2 else (finite[mid - 1] + finite[mid]) / 2
    scale = med * _MEDIAN_TO_SIGMA
    return scale if scale > 0 else None


#: The candidates, in the order they were tried.  ``stdev`` is what shipped
#: originally, ``mean_abs`` is what ships now, ``median_abs`` is the option
#: rejected on efficiency grounds before any of this was measured.
CANDIDATES: Dict[str, Callable[[Sequence[float]], Optional[float]]] = {
    "stdev": rs.stdev,
    "mean_abs": rs.robust_scale,
    "median_abs": median_abs_scale,
}


def _f(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def describe(values: Sequence[float]) -> Dict[str, Any]:
    """Shape of one axis's distribution — the numbers that pick a scale."""
    finite = [v for v in values if math.isfinite(v)]
    if len(finite) < 2:
        return {}
    n = len(finite)
    mean = sum(finite) / n
    sd = rs.stdev(finite) or 0.0
    mean_abs = sum(abs(v) for v in finite) / n
    med_abs = (median_abs_scale(finite) or 0.0) / _MEDIAN_TO_SIGMA
    return {
        "n": n,
        "mean": mean,
        "stdev": sd,
        "mean_abs": mean_abs,
        "median_abs": med_abs,
        # Tail weight: sqrt(pi/2) for a normal; above that the squared moment
        # is being set by the tail.
        "tail_weight": sd / mean_abs if mean_abs else None,
        # Magnitude skew: 1.0 when the typical session IS the average one.
        "skew": mean_abs / med_abs if med_abs else None,
        # How much of the scale is a persistent drift rather than variation —
        # this is what zero-centring charges for and mean-centring hides.
        "drift_share": abs(mean) / mean_abs if mean_abs else None,
    }


def grade(
    leans: Sequence[float],
    stabs: Sequence[float],
    scale: Callable[[Sequence[float]], Optional[float]],
) -> Dict[str, Any]:
    """The state mix this scale would have produced over the stored history.

    One shared window for every session, matching ``/api/gex/regime-history``
    — the strip recomputes against the current full window so its bars are
    comparable to each other, and it is the strip a reader actually sees.
    """
    lean_sigma = scale(leans)
    stab_sigma = scale(stabs)
    if not lean_sigma and not stab_sigma:
        return {"quiet_rate": None, "states": {}}
    lean_sigma = lean_sigma or stab_sigma
    stab_sigma = stab_sigma or lean_sigma

    states: Dict[str, int] = {}
    for lean, stab in zip(leans, stabs):
        read = rs.classify(rs.zscore(lean, lean_sigma), rs.zscore(stab, stab_sigma))
        states[read.state] = states.get(read.state, 0) + 1
    total = sum(states.values()) or 1
    return {
        "lean_sigma": lean_sigma,
        "stability_sigma": stab_sigma,
        "quiet_rate": states.get("QUIET", 0) / total,
        "states": states,
    }


async def report(db: Any, symbols: Sequence[str], limit: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for symbol in symbols:
        rows = await db.get_regime_sessions(symbol, limit=limit)
        pairs = [
            (lean, stab)
            for lean, stab in (
                (_f(r.get("lean_raw")), _f(r.get("stability_raw"))) for r in rows
            )
            if lean is not None and stab is not None
        ]
        if len(pairs) < rs.MIN_SESSIONS_FOR_SIGMA:
            out[symbol] = {"sessions": len(pairs), "note": "not enough stored sessions"}
            continue
        leans = [p[0] for p in pairs]
        stabs = [p[1] for p in pairs]
        out[symbol] = {
            "sessions": len(pairs),
            "lean": describe(leans),
            "stability": describe(stabs),
            "candidates": {
                name: grade(leans, stabs, fn) for name, fn in CANDIDATES.items()
            },
        }
    return out


def render(data: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("")
    lines.append("Shift distribution shape")
    lines.append("  sd/mean|x| = 1.2533 for a normal; above it the tail sets a squared scale")
    lines.append("  skew       = mean|x| / median|x|; above 1 a mean-based scale is too big")
    lines.append("  drift      = |mean| / mean|x|; how much of the axis is a persistent shift")
    lines.append("")
    lines.append(
        f"{'symbol':<8}{'axis':<11}{'n':>5}{'sd/mean|x|':>12}{'skew':>8}{'drift':>8}"
    )
    lines.append("-" * 52)
    for symbol, d in data.items():
        if "lean" not in d:
            lines.append(f"{symbol:<8}{d.get('note', 'no data')}")
            continue
        for axis in ("lean", "stability"):
            s = d[axis]
            lines.append(
                f"{symbol:<8}{axis:<11}{s['n']:>5}"
                f"{(s['tail_weight'] or 0):>12.3f}"
                f"{(s['skew'] or 0):>8.2f}"
                f"{(s['drift_share'] or 0):>8.2f}"
            )
    lines.append("")
    lines.append("QUIET rate by candidate denominator  (target ~25%)")
    lines.append("")
    header = f"{'symbol':<8}" + "".join(f"{name:>12}" for name in CANDIDATES)
    lines.append(header)
    lines.append("-" * len(header))
    for symbol, d in data.items():
        if "candidates" not in d:
            continue
        row = f"{symbol:<8}"
        for name in CANDIDATES:
            rate = d["candidates"][name]["quiet_rate"]
            row += f"{'--':>12}" if rate is None else f"{rate * 100:>11.0f}%"
        lines.append(row)
    lines.append("")
    lines.append("State mix by candidate")
    for symbol, d in data.items():
        if "candidates" not in d:
            continue
        lines.append(f"  {symbol}")
        for name, g in d["candidates"].items():
            mix = ", ".join(
                f"{k.lower()}={v}" for k, v in sorted(g["states"].items(), key=lambda kv: -kv[1])
            )
            lines.append(f"    {name:<12}{mix}")
    lines.append("")
    return "\n".join(lines)


async def _run(symbols: Sequence[str], limit: int, json_path: Optional[str]) -> int:
    db = DatabaseManager()
    try:
        await db.connect()
    except Exception:
        logger.exception("regime scale report: database connect failed")
        return 1
    try:
        data = await report(db, symbols, limit)
    finally:
        await db.disconnect()

    print(render(data))
    if json_path:
        with open(json_path, "w") as fh:
            json.dump(data, fh, indent=2, default=str)
        print(f"wrote {json_path}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument(
        "--limit",
        type=int,
        default=rs.MIN_SESSIONS_FOR_SIGMA * 6,
        help="Stored sessions to read per symbol (default: the sigma window).",
    )
    parser.add_argument("--json", default=None, help="Also write the raw numbers here.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    symbols = [s.upper() for s in args.symbols] if args.symbols else default_symbols()
    return asyncio.run(_run(symbols, args.limit, args.json))


if __name__ == "__main__":
    sys.exit(main())
