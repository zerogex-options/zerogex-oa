"""Attribute a CHANGE in profile GEX at one hypothetical spot to individual contracts.

The spot-shift gamma profile is a sum over the whole chain, so when its value at
some grid point moves, no aggregate query localizes the cause: a contract 400
points away still carries most of its peak gamma at a 30-90 DTE tenor. This tool
does the only thing that settles it -- recompute the per-contract terms at the
grid point for TWO snapshots and diff them, contract by contract.

It reuses the engine's own primitives (``_calculate_bs_gamma``,
``_dte_profile_weight``, the settlement-aware TTE) rather than reimplementing the
arithmetic, so a term here is the same number ``_gamma_exposure_profile`` would
have accumulated:

    term = sign * dte_weight(T) * gamma(S_eval, K, T, r, sigma, q) * OI * 100
           * S_eval^2 * 0.01          # model A: +1 calls, -1 puts

The rollup separates the two explanations that matter and that no
``GROUP BY`` over ``option_chains`` can tell apart:

  * contracts that ENTERED or LEFT the curve between the snapshots -- the
    profile's entry test is ``sigma > 0 and oi > 0 and K > 0``, so a contract
    failing it is silently absent (see ``excluded_no_iv`` in the engine);
  * contracts PRESENT IN BOTH whose term moved, because OI, IV or TTE moved.

Usage:
    # the two snapshots bracketing the 3 Aug 2026 step, evaluated at -5%
    python -m src.tools.gex_point_attribution \\
        --underlying SPX --at "2026-08-03 11:44" --vs "2026-08-03 11:59" --band -0.05

    python -m src.tools.gex_point_attribution --underlying SPX \\
        --at "2026-08-03 11:44" --vs "2026-08-03 11:59" --band -0.05 --top 40 --csv out.csv

Read-only: issues SELECTs only, writes nothing back to the database.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.analytics.main_engine import ET, AnalyticsEngine
from src.database import db_connection
from src.market_calendar import (
    calculate_time_to_expiration,
    settlement_close_time_for_contract,
)

logger = logging.getLogger(__name__)

# Same lookback the forced-flow backfill uses to assemble an as-of book: far
# enough to catch every active contract's latest quote, short enough not to drag
# in a prior-session one.
_CONTRACT_LOOKBACK = timedelta(hours=2)

# Deliberately NOT filtered on implied_volatility / open_interest: a contract
# that fails the profile's entry test is exactly what we are hunting, so it has
# to come back from the query and be classified here rather than disappear.
_ASOF_SQL = """
    SELECT DISTINCT ON (option_symbol)
           option_symbol, strike, expiration, option_type,
           open_interest, implied_volatility
    FROM option_chains
    WHERE underlying = %s
      AND timestamp <= %s
      AND timestamp > %s
      AND expiration >= %s
    ORDER BY option_symbol, timestamp DESC
"""

_SPOT_SQL = """
    SELECT close
    FROM underlying_quotes
    WHERE symbol = %s AND timestamp <= %s
    ORDER BY timestamp DESC
    LIMIT 1
"""


def _parse_et(text: str) -> datetime:
    """'2026-08-03 11:44' (ET, naive) -> tz-aware ET datetime."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return ET.localize(datetime.strptime(text, fmt))
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"not a 'YYYY-MM-DD HH:MM' ET timestamp: {text!r}")


def _load_snapshot(cursor, db_symbol: str, ts: datetime) -> Tuple[List[Dict[str, Any]], float]:
    """As-of chain and spot for one instant. Expired contracts dropped by ET date."""
    et_date = ts.astimezone(ET).date()
    cursor.execute(_ASOF_SQL, (db_symbol, ts, ts - _CONTRACT_LOOKBACK, et_date))
    cols = [d[0] for d in cursor.description]
    rows = [dict(zip(cols, r)) for r in cursor.fetchall()]

    cursor.execute(_SPOT_SQL, (db_symbol, ts))
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise SystemExit(f"no underlying quote at or before {ts} for {db_symbol}")
    return rows, float(row[0])


def _terms(
    engine: AnalyticsEngine,
    options: List[Dict[str, Any]],
    s_eval: float,
    ts: datetime,
    apply_dte_weight: bool,
) -> Dict[str, Dict[str, Any]]:
    """Per-contract signed dollar-gamma term at ``s_eval``, keyed by option_symbol.

    Mirrors ``_gamma_exposure_profile``'s loop exactly, including its entry test
    and the model-A sign convention. A contract failing the entry test is kept
    with ``term = 0.0`` and ``entered = False`` so the diff can attribute it.
    """
    out: Dict[str, Dict[str, Any]] = {}
    tte_cache: Dict[Any, float] = {}
    r = engine.risk_free_rate
    q = engine.dividend_yield

    for opt in options:
        sym = opt["option_symbol"]
        sigma = float(opt.get("implied_volatility") or 0.0)
        oi = int(opt.get("open_interest") or 0)
        K = float(opt.get("strike") or 0.0)
        otype = opt["option_type"]
        expiration = opt["expiration"]

        rec: Dict[str, Any] = {
            "strike": K,
            "type": otype,
            "expiration": expiration,
            "oi": oi,
            "iv": sigma,
            "term": 0.0,
            "entered": False,
            "reason": "",
        }

        # The profile's own entry test -- a contract failing it never reaches
        # the curve, which is the silent exclusion we are trying to detect.
        if sigma <= 0 or oi <= 0 or K <= 0:
            rec["reason"] = "no_oi" if oi <= 0 else "no_iv" if sigma <= 0 else "no_strike"
            out[sym] = rec
            continue

        close_t = settlement_close_time_for_contract(engine.db_symbol, sym, expiration)
        cache_key = (expiration, close_t)
        T = tte_cache.get(cache_key)
        if T is None:
            T = calculate_time_to_expiration(ts, expiration, market_close_time=close_t)
            tte_cache[cache_key] = T
        if T <= 0:
            rec["reason"] = "expired"
            out[sym] = rec
            continue

        gamma = float(engine._calculate_bs_gamma(s_eval, K, T, r, sigma, q))
        dollar_gamma = gamma * oi * 100.0 * s_eval * s_eval * 0.01
        sign = 1.0 if otype == "C" else -1.0
        dte_w = engine._dte_profile_weight(T) if apply_dte_weight else 1.0

        rec.update(
            {
                "term": sign * dte_w * dollar_gamma,
                "entered": True,
                "tte_days": T * 365.0,
                "dte_w": dte_w,
            }
        )
        out[sym] = rec
    return out


def _bn(x: float) -> str:
    return f"{x / 1e9:+.3f}bn"


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Attribute a change in profile GEX at one grid point to contracts."
    )
    ap.add_argument("--underlying", default="SPX")
    ap.add_argument("--at", required=True, type=_parse_et, help="baseline snapshot, ET")
    ap.add_argument("--vs", required=True, type=_parse_et, help="comparison snapshot, ET")
    ap.add_argument(
        "--band",
        type=float,
        default=-0.05,
        help="grid point as a FRACTION of spot (-0.05 = the -5%% point). Default -0.05.",
    )
    ap.add_argument("--top", type=int, default=25, help="contracts to print (default 25)")
    ap.add_argument("--no-dte-weight", action="store_true", help="disable the horizon ramp")
    ap.add_argument("--csv", help="also write the full per-contract diff here")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    engine = AnalyticsEngine(underlying=args.underlying)
    apply_w = not args.no_dte_weight

    with db_connection() as conn:
        cur = conn.cursor()
        opts_a, spot_a = _load_snapshot(cur, engine.db_symbol, args.at)
        opts_b, spot_b = _load_snapshot(cur, engine.db_symbol, args.vs)

    # Both snapshots are evaluated at the SAME price, so the diff is the book
    # changing and not the grid point sliding with spot. The baseline's spot
    # anchors it; the drift between the two spots is reported below.
    s_eval = spot_a * (1.0 + args.band)

    ta = _terms(engine, opts_a, s_eval, args.at, apply_w)
    tb = _terms(engine, opts_b, s_eval, args.vs, apply_w)

    gex_a = sum(v["term"] for v in ta.values())
    gex_b = sum(v["term"] for v in tb.values())

    print(
        f"\n{engine.db_symbol}  GEX attribution at S = {s_eval:,.2f} "
        f"({args.band:+.1%} of the baseline spot)"
    )
    print(f"  dte_ref_days = {engine.dte_ref_days}   dte_weighting = {'on' if apply_w else 'OFF'}")
    print(
        f"  A  {args.at:%Y-%m-%d %H:%M %Z}  spot {spot_a:,.2f}  "
        f"rows {len(opts_a):>5}  entering {sum(1 for v in ta.values() if v['entered']):>5}  "
        f"GEX {_bn(gex_a)}"
    )
    print(
        f"  B  {args.vs:%Y-%m-%d %H:%M %Z}  spot {spot_b:,.2f}  "
        f"rows {len(opts_b):>5}  entering {sum(1 for v in tb.values() if v['entered']):>5}  "
        f"GEX {_bn(gex_b)}"
    )
    print(
        f"  Δ GEX = {_bn(gex_b - gex_a)}   (spot drift {spot_b - spot_a:+.2f} "
        f"= {100 * (spot_b / spot_a - 1):+.3f}%)\n"
    )

    rows = []
    for sym in set(ta) | set(tb):
        a = ta.get(sym)
        b = tb.get(sym)
        term_a = a["term"] if a else 0.0
        term_b = b["term"] if b else 0.0
        ref = b or a
        if a and b:
            bucket = (
                "both"
                if a["entered"] == b["entered"]
                else "entered curve" if b["entered"] else "left curve"
            )
        else:
            bucket = "new row" if b else "row vanished"
        rows.append(
            {
                "option_symbol": sym,
                "bucket": bucket,
                "type": ref["type"],
                "strike": ref["strike"],
                "expiration": ref["expiration"],
                "oi_a": a["oi"] if a else 0,
                "oi_b": b["oi"] if b else 0,
                "iv_a": round(a["iv"], 4) if a else 0.0,
                "iv_b": round(b["iv"], 4) if b else 0.0,
                "reason_a": a["reason"] if a else "absent",
                "reason_b": b["reason"] if b else "absent",
                "term_a": term_a,
                "term_b": term_b,
                "delta": term_b - term_a,
            }
        )
    rows.sort(key=lambda r: -abs(r["delta"]))

    if not rows:
        print("  no contracts in either snapshot -- check the timestamps and --underlying\n")
        return 1

    # Rollup first: it answers "did contracts move in/out, or did their values
    # move?" before any single contract is read.
    print("  Δ by bucket")
    agg: Dict[str, Tuple[int, float]] = {}
    for r in rows:
        n, tot = agg.get(r["bucket"], (0, 0.0))
        agg[r["bucket"]] = (n + 1, tot + r["delta"])
    for bucket, (n, tot) in sorted(agg.items(), key=lambda kv: -abs(kv[1][1])):
        print(f"    {bucket:<15} {n:>5} contracts   {_bn(tot):>12}")

    print(f"\n  top {args.top} contracts by |Δ|")
    print(
        f"    {'contract':<22}{'bucket':<15}{'K':>8} {'exp':<11}"
        f"{'OI a→b':>15}{'IV a→b':>16}{'Δ':>12}"
    )
    for r in rows[: args.top]:
        oi = f"{r['oi_a']}→{r['oi_b']}"
        iv = f"{r['iv_a']:.4f}→{r['iv_b']:.4f}"
        print(
            f"    {r['option_symbol']:<22}{r['bucket']:<15}{r['strike']:>8.0f} "
            f"{str(r['expiration']):<11}{oi:>15}{iv:>16}{_bn(r['delta']):>12}"
        )

    if args.csv:
        with open(args.csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\n  full diff -> {args.csv} ({len(rows)} contracts)")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
