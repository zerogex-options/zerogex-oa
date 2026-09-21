"""How much of the gamma flip is the market, and how much is INGEST_EXPIRATIONS?

``INGEST_EXPIRATIONS`` defaults to 3. On underlyings that expire every
trading day -- which is now all four of production's -- that buys DTE 0, 1
and 2, so under the horizon-occupancy ramp ``min(1, DTE/5)`` no contract in
the chain is weighted above 0.4. ``config.py`` lists the knob as the first
thing to trim under stream pressure and warns that "dropping them shifts
the gamma flip output (worth measuring against a baseline session before
going further)". This is that measurement.

Two live runs three seconds apart on 2026-09-16 put the SPX flip 29.91
points higher at six expirations than at three -- 0.39% of spot, against
gate bands 0.6% and 0.8% wide. That says the setting matters. It does not
say which depth is right, and one pair of runs cannot: the question is
whether the flip CONVERGES as the chain deepens (then three is simply
truncation) or keeps marching (then the flip is a function of how much
chain you bought, which is a far more serious thing to publish).

Why this is not four invocations of feed_compare
------------------------------------------------

Because the market moves between them. The 2026-09-16 result was only
readable because the two runs happened to land three seconds apart; a
four-point sweep run serially spends minutes, and SPX moves further in
that time than the effect being measured.

So the chain is fetched ONCE at the deepest requested depth and the
shallower depths are taken as prefixes of it -- the nearest N expirations
are a subset of the nearest M for any N < M. Greeks are per-contract and
depend on nothing outside their own row, so one enrichment pass serves
every depth. Every depth therefore reads the SAME quotes at the SAME
instant, and the only variable left is the one under test.

READ-ONLY: no persistence, no config mutation, no writes of any kind.

Usage:
    python -m src.tools.chain_depth_sweep --underlying '$SPXW.X'
    python -m src.tools.chain_depth_sweep --underlying QQQ --depths 3,6,9,12 --rounds 10
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from src.ingestion.providers import get_provider
from src.market_calendar import calculate_time_to_expiration, settlement_close_time_for_contract
from src.tools.feed_compare import (
    _compute_analytics,
    _detect_mangled_index_symbol,
    _enrich,
    _quotes_to_option_rows,
    sample_provider,
)
from src.utils import get_logger

logger = get_logger(__name__)

_DEFAULT_DEPTHS = (3, 6, 9, 12)


def _rows_for_depth(rows: List[Dict[str, Any]], depth: int) -> List[Dict[str, Any]]:
    """The nearest ``depth`` expirations out of an already-fetched chain."""
    expirations = sorted({r["expiration"] for r in rows})[:depth]
    keep = set(expirations)
    return [r for r in rows if r["expiration"] in keep]


def sweep_once(
    provider: Any,
    underlying: str,
    *,
    depths: Sequence[int],
    strike_count_max: int,
    strike_pct_range: float,
) -> Dict[str, Any]:
    """One fetch at the deepest depth, analysed at every depth."""
    deepest = max(depths)
    sample = sample_provider(
        provider,
        underlying,
        num_expirations=deepest,
        strike_count_max=strike_count_max,
        strike_pct_range=strike_pct_range,
    )
    if sample.error or not sample.spot:
        return {"error": sample.error or "no spot price available", "depths": {}}

    now = datetime.now(timezone.utc)
    rows = _quotes_to_option_rows(sample.quotes, sample.metadata, now)
    enriched = _enrich(rows, sample.spot, underlying)

    # Run this after the close and the front expiration has already settled:
    # its contracts price at T=0, so the IV solver cannot touch them and the
    # profile drops every one. That reads in the diagnostics as a chain
    # missing a quarter of itself, which looks like a data fault and is
    # merely the clock. Two six-minute runs were spent on it before the tool
    # said so.
    expirations = sorted({r["expiration"] for r in enriched})
    front_settled = False
    if expirations:
        close_t = settlement_close_time_for_contract(underlying, None, expirations[0])
        front_settled = calculate_time_to_expiration(now, expirations[0], close_t) <= 0

    out: Dict[str, Any] = {
        "captured_at": sample.captured_at,
        "spot": sample.spot,
        "error": None,
        "front_expiration_settled": front_settled,
        "depths": {},
    }
    for depth in depths:
        subset = _rows_for_depth(enriched, depth)
        metrics = _compute_analytics(subset, sample.spot, underlying, now, label=f"depth={depth}")
        flip = metrics.get("gamma_flip")
        out["depths"][depth] = {
            "contracts": len(subset),
            "expirations": len({r["expiration"] for r in subset}),
            "gamma_flip": flip,
            "flip_distance": None if flip is None else (sample.spot - flip) / sample.spot,
            "net_gex": metrics.get("net_gex"),
            "call_wall": metrics.get("call_wall"),
            "put_wall": metrics.get("put_wall"),
        }
    return out


def summarise(rounds: Sequence[Dict[str, Any]], depths: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    """Mean flip and resolution rate per depth, plus the step from the previous depth."""
    per_depth: Dict[int, Dict[str, Any]] = {}
    previous_flip: Optional[float] = None
    for depth in depths:
        flips = [
            r["depths"][depth]["gamma_flip"]
            for r in rounds
            if r.get("depths", {}).get(depth, {}).get("gamma_flip") is not None
        ]
        dists = [
            r["depths"][depth]["flip_distance"]
            for r in rounds
            if r.get("depths", {}).get(depth, {}).get("flip_distance") is not None
        ]
        gex = [
            r["depths"][depth]["net_gex"]
            for r in rounds
            if r.get("depths", {}).get(depth, {}).get("net_gex") is not None
        ]
        attempted = [r for r in rounds if depth in r.get("depths", {})]
        contracts = [r["depths"][depth]["contracts"] for r in attempted]
        mean_flip = sum(flips) / len(flips) if flips else None
        per_depth[depth] = {
            "resolved": len(flips),
            "attempted": len(attempted),
            "contracts": max(contracts) if contracts else 0,
            "mean_flip": mean_flip,
            "mean_flip_distance": (sum(dists) / len(dists)) if dists else None,
            "mean_net_gex": (sum(gex) / len(gex)) if gex else None,
            # The convergence signal: how far this depth moved the flip from
            # the previous one. Shrinking steps mean the shallow chain was
            # truncation; steady steps mean the flip tracks chain depth.
            "step_from_previous": (
                None if mean_flip is None or previous_flip is None else mean_flip - previous_flip
            ),
        }
        if mean_flip is not None:
            previous_flip = mean_flip
    return per_depth


def _print_summary(
    summary: Dict[int, Dict[str, Any]],
    underlying: str,
    rounds: int,
    *,
    front_expiration_settled: bool = False,
) -> None:
    print(f"\n  === {underlying}: chain depth sweep, {rounds} round(s) ===\n")
    if front_expiration_settled:
        print(
            "  AFTER THE CLOSE: the front expiration has already settled. Its\n"
            "  contracts price at T=0, so the IV solver cannot reach them and the\n"
            "  profile drops all of them -- the chain will look a quarter empty\n"
            "  for reasons that are the clock, not the feed. Re-run during RTH.\n"
        )
    print(
        f"  {'depth':<7}{'contracts':<11}{'resolved':<11}{'mean flip':>12}"
        f"{'vs spot':>10}{'step':>10}{'net_gex':>14}"
    )
    print("  " + "-" * 75)
    for depth, row in summary.items():
        flip = "-" if row["mean_flip"] is None else f"{row['mean_flip']:,.2f}"
        dist = (
            "-"
            if row["mean_flip_distance"] is None
            else f"{-100 * row['mean_flip_distance']:+.2f}%"
        )
        step = "-" if row["step_from_previous"] is None else f"{row['step_from_previous']:+.2f}"
        gex = "-" if row["mean_net_gex"] is None else f"{row['mean_net_gex'] / 1e6:,.0f}M"
        print(
            f"  {depth:<7}{row['contracts']:<11}"
            f"{str(row['resolved']) + '/' + str(row['attempted']):<11}"
            f"{flip:>12}{dist:>10}{step:>10}{gex:>14}"
        )

    steps = [abs(r["step_from_previous"]) for r in summary.values() if r["step_from_previous"]]
    if len(steps) >= 2:
        print()
        if steps[-1] < steps[0] / 2:
            print(
                "  CONVERGING: each added block of expirations moves the flip less\n"
                "  than the one before. The shallow chain is truncation, and there\n"
                "  is a depth beyond which more chain stops changing the answer."
            )
        else:
            print(
                "  NOT CONVERGING: the flip is still moving as much at the deepest\n"
                "  rung as at the shallowest. It tracks how much chain was bought,\n"
                "  which is a property of the configuration and not of the market."
            )
    elif not steps:
        print(
            "\n  NO VERDICT: too few depths resolved a flip to compare. A chain whose\n"
            "  crossing sits outside the actionable band is unresolved at EVERY\n"
            "  depth -- see the per-depth diagnostics above for which gate rejected it."
        )
    print()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--underlying", default="SPY")
    parser.add_argument("--provider", default=None, help="defaults to MARKET_DATA_PROVIDER")
    parser.add_argument(
        "--depths", default=",".join(str(d) for d in _DEFAULT_DEPTHS), help="e.g. 3,6,9,12"
    )
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    parser.add_argument("--strike-count-max", type=int, default=40)
    parser.add_argument("--strike-pct-range", type=float, default=3.0)
    args = parser.parse_args(argv)

    try:
        depths = sorted({int(d) for d in args.depths.split(",") if d.strip()})
    except ValueError:
        parser.error(f"--depths must be a comma-separated list of integers; got {args.depths!r}")
    if not depths:
        parser.error("--depths resolved to an empty list")

    # `make sweep UNDERLYING='$SPXW.X'` delivers 'PXW.X': make expands $S as
    # a variable of its own BEFORE exporting. Six rounds of a symbol that
    # cannot exist is six wasted minutes, so reject it in the first second.
    mangled = _detect_mangled_index_symbol(args.underlying)
    if mangled:
        parser.error(mangled)

    provider = get_provider(args.provider)
    collected: List[Dict[str, Any]] = []
    try:
        for i in range(args.rounds):
            result = sweep_once(
                provider,
                args.underlying,
                depths=depths,
                strike_count_max=args.strike_count_max,
                strike_pct_range=args.strike_pct_range,
            )
            if result.get("error"):
                logger.warning("round %d failed: %s", i + 1, result["error"])
            else:
                collected.append(result)
                row = result["depths"][depths[0]]
                print(
                    f"  round {i + 1}/{args.rounds}  spot={result['spot']:,.2f}  "
                    f"resolved at "
                    f"{sum(1 for d in depths if result['depths'][d]['gamma_flip'] is not None)}"
                    f"/{len(depths)} depths"
                    + (
                        ""
                        if row["gamma_flip"] is None
                        else f"  (depth {depths[0]}: " f"{row['gamma_flip']:,.2f})"
                    )
                )
            if i + 1 < args.rounds:
                time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
    finally:
        provider.close()

    if not collected:
        print("\n  no usable rounds\n")
        return 1
    _print_summary(
        summarise(collected, depths),
        args.underlying,
        len(collected),
        front_expiration_settled=any(r.get("front_expiration_settled") for r in collected),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
