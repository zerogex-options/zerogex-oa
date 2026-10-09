"""Diff the PUBLISHED flow figure between two databases, bucket by bucket.

Why this exists. The method question (F10/F11) has only ever been measured on
230-contract snapshots: switching from the quote test to the tick test
relabels 47.6% of contracts and flips the net imbalance sign 16 of 27 times.
A snapshot is not what a subscriber sees. The site serves one-minute buckets
aggregated across the whole chain, and aggregation is exactly where noise of
that shape can wash out -- or not. Nothing could answer that from snapshots,
so this reads the two databases a shadow run produces.

What it compares. ``production`` runs FLOW_CLASSIFIER=quote and the rehearsal
DB runs FLOW_CLASSIFIER=tick over the SAME feed, so the only variable is the
method. Point it at a ThetaData rehearsal instead and it answers the whole
post-cutover question.

The arithmetic is PRODUCTION'S OWN, not a restatement of it. The per-bucket
deltas come from ``src.api.database``'s LAG-CASE fragment, and the buy/sell
split reproduces the ratio-scaling in the ``flow_contract_facts`` insert:
ask/bid deltas are rescaled by the total volume delta so the unclassified
mid portion is attributed proportionally. Comparing raw ask-minus-bid instead
would measure something the product does not publish.

What it does NOT do. ``shadow-run`` runs ingestion only, so this compares the
INPUT to order_flow_imbalance and tape_flow_bias, not the rendered signal.
The transform downstream is deterministic, so matching inputs mean matching
outputs; if these differ materially, point analytics at the rehearsal DB and
compare the signals themselves.
"""

from __future__ import annotations

import argparse
import os
import statistics as st
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.utils import get_logger

logger = get_logger(__name__)

#: One row per (underlying, minute): the net imbalance that reaches a signal.
BUCKET_FLOW_SQL = """
WITH source_rows AS (
    SELECT underlying, option_symbol, timestamp, volume, ask_volume, bid_volume
    FROM option_chains
    WHERE timestamp >= %(session_start)s
      AND timestamp < %(session_end)s
      AND (%(underlying)s IS NULL OR underlying = %(underlying)s)
),
deltas AS (
    SELECT
        s.underlying,
        DATE_TRUNC('minute', s.timestamp) AS bucket,
        CASE
            WHEN LAG(s.volume) OVER w IS NULL THEN COALESCE(s.volume, 0)
            WHEN {same_session}
                THEN GREATEST(COALESCE(s.volume, 0) - COALESCE(LAG(s.volume) OVER w, 0), 0)
            ELSE COALESCE(s.volume, 0)
        END::bigint AS volume_delta,
        CASE
            WHEN LAG(s.ask_volume) OVER w IS NULL THEN COALESCE(s.ask_volume, 0)
            WHEN {same_session}
                THEN GREATEST(COALESCE(s.ask_volume, 0) - COALESCE(LAG(s.ask_volume) OVER w, 0), 0)
            ELSE COALESCE(s.ask_volume, 0)
        END::bigint AS ask_vol_delta,
        CASE
            WHEN LAG(s.bid_volume) OVER w IS NULL THEN COALESCE(s.bid_volume, 0)
            WHEN {same_session}
                THEN GREATEST(COALESCE(s.bid_volume, 0) - COALESCE(LAG(s.bid_volume) OVER w, 0), 0)
            ELSE COALESCE(s.bid_volume, 0)
        END::bigint AS bid_vol_delta
    FROM source_rows s
    WINDOW w AS (PARTITION BY s.option_symbol ORDER BY s.timestamp)
)
SELECT
    underlying,
    bucket,
    -- The ratio-scaling from the flow_contract_facts insert. The classifier
    -- leaves a mid portion it will not attribute; production splits it in the
    -- same proportion as the part it did classify, and THAT is the number
    -- that reaches order_flow_imbalance.
    SUM(
        CASE WHEN (ask_vol_delta + bid_vol_delta) > 0
             THEN ((ask_vol_delta::numeric - bid_vol_delta)
                   / (ask_vol_delta + bid_vol_delta) * volume_delta)
             ELSE 0 END
    )::bigint AS net_imbalance,
    SUM(volume_delta)::bigint AS volume
FROM deltas
GROUP BY underlying, bucket
HAVING SUM(volume_delta) > 0
ORDER BY underlying, bucket
"""


def bucket_flow_sql() -> str:
    """The query, with production's own same-session clause spliced in.

    Imported lazily: ``src.api.database`` pulls in the FastAPI application,
    which is a second or two of startup and a screenful of unrelated warnings
    on a tool whose whole job is to print a short table.
    """
    from src.api.database import _flow_lag_same_session_clause

    return BUCKET_FLOW_SQL.format(same_session=_flow_lag_same_session_clause(use_cash_keying=True))


def compare_buckets(
    production: Sequence[Tuple[Any, Any, int, int]],
    shadow: Sequence[Tuple[Any, Any, int, int]],
) -> Dict[str, Any]:
    """Join two runs on (underlying, bucket) and characterise the difference.

    Pure, so the thing the decision rests on is testable without a database.

    Buckets present in only one run are COUNTED AND REPORTED rather than
    dropped. A silent inner join would let a rehearsal that covered half the
    session read as perfect agreement on the half it managed -- which is the
    shape of the 2026-09-28 failure, where a broken join printed as a quiet
    market.
    """
    prod = {(u, b): (net, vol) for u, b, net, vol in production}
    shad = {(u, b): (net, vol) for u, b, net, vol in shadow}

    common = sorted(set(prod) & set(shad), key=lambda k: (str(k[0]), k[1]))
    rows: List[Dict[str, Any]] = []
    for key in common:
        p_net, p_vol = prod[key]
        s_net, s_vol = shad[key]
        rows.append(
            {
                "underlying": key[0],
                "bucket": key[1],
                "production": p_net,
                "shadow": s_net,
                "difference": s_net - p_net,
                "sign_agrees": (p_net > 0) == (s_net > 0) or (p_net == 0 and s_net == 0),
                "volume": max(p_vol, s_vol),
            }
        )

    scale = [abs(r["production"]) for r in rows]
    diffs = [r["difference"] for r in rows]
    agree = sum(1 for r in rows if r["sign_agrees"])

    # A flip is not one thing. On a bucket reading near zero the sign was
    # never meaningful -- the site showed "roughly balanced" and would still
    # show roughly balanced, with the arrow pointing the other way. On a
    # bucket carrying real imbalance it is a different claim about the
    # session. Counting both as one "flip" is what made 2026-10-09's 71-77%
    # unreadable: the rate alone cannot distinguish them.
    #
    # Banded against this group's OWN median, not a fixed threshold, because
    # SPY's typical bucket and NDX's are orders of magnitude apart and a
    # shared cutoff would call every NDX bucket near-zero.
    flips = [r for r in rows if not r["sign_agrees"]]
    median_abs = st.median(scale) if scale else 0.0
    bands = {"near_zero": 0, "typical": 0, "large": 0}
    for r in flips:
        mag = abs(r["production"])
        if median_abs <= 0 or mag < 0.5 * median_abs:
            bands["near_zero"] += 1
        elif mag <= 1.5 * median_abs:
            bands["typical"] += 1
        else:
            bands["large"] += 1

    # The number that actually answers "would a subscriber notice": what
    # share of the session's total absolute imbalance sits in buckets whose
    # sign changed. A 25% flip RATE concentrated in near-zero buckets can be
    # a low single-digit share of the flow, and that is a different product
    # decision from the same rate spread across the day's big prints.
    total_abs = sum(scale)
    flipped_abs = sum(abs(r["production"]) for r in flips)

    return {
        "median_abs_production": median_abs,
        "flips_near_zero": bands["near_zero"],
        "flips_typical": bands["typical"],
        "flips_large": bands["large"],
        "flipped_share_of_flow_pct": ((100.0 * flipped_abs / total_abs) if total_abs else None),
        "buckets_compared": len(rows),
        "production_only": len(set(prod) - set(shad)),
        "shadow_only": len(set(shad) - set(prod)),
        "sign_agrees": agree,
        "sign_disagrees": len(rows) - agree,
        "sign_agreement_pct": (100.0 * agree / len(rows)) if rows else None,
        "mean_abs_difference": st.mean([abs(d) for d in diffs]) if diffs else None,
        "mean_signed_difference": st.mean(diffs) if diffs else None,
        "mean_abs_production": st.mean(scale) if scale else None,
        "shadow_higher": sum(1 for d in diffs if d > 0),
        "rows": rows,
    }


def compare_by_underlying(
    production: Sequence[Tuple[Any, Any, int, int]],
    shadow: Sequence[Tuple[Any, Any, int, int]],
) -> Dict[Any, Dict[str, Any]]:
    """The same summary, per symbol.

    2026-10-09 gave 76.7% agreement on SPY and 71.2% across everything, which
    means the rest agreed around 69% -- inferred from two aggregates rather
    than measured. The tick test reads a SEQUENCE of trade prices, so a
    contract that prints rarely leaves more zero ticks inheriting a carried
    direction; it should degrade on thinner symbols, and SPX/NDX/QQQ are
    thinner than SPY. That is a number worth having rather than deducing.
    """
    symbols = sorted({u for u, _, _, _ in production} | {u for u, _, _, _ in shadow}, key=str)
    return {
        sym: compare_buckets(
            [r for r in production if r[0] == sym],
            [r for r in shadow if r[0] == sym],
        )
        for sym in symbols
    }


def _connect(db_name: str) -> Any:
    import psycopg2

    # No password: libpq reads ~/.pgpass, which is how every other tool here
    # reaches both databases. Never pass one on the command line, and never
    # print one.
    params: Dict[str, Any] = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": db_name,
        "user": os.getenv("DB_USER", "postgres"),
        "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "20")),
    }
    # Same optional-and-only-if-set handling as src/database/connection.py.
    # The deployment is RDS and the Makefile's psql line sets sslmode=require,
    # so omitting this against a server that demands TLS fails the connect.
    sslmode = os.getenv("DB_SSLMODE", "").strip()
    if sslmode:
        params["sslmode"] = sslmode
    return psycopg2.connect(**params)


def fetch(db_name: str, params: Dict[str, Any]) -> List[Tuple[Any, Any, int, int]]:
    conn = _connect(db_name)
    try:
        with conn.cursor() as cur:
            cur.execute(bucket_flow_sql(), params)
            return [tuple(r) for r in cur.fetchall()]  # type: ignore[misc]
    finally:
        conn.close()


def _print_report(result: Dict[str, Any], prod_db: str, shadow_db: str, verbose: bool) -> None:
    print("\n=== PUBLISHED FLOW, per one-minute bucket ===")
    print(f"  production: {prod_db}   rehearsal: {shadow_db}")

    n = result["buckets_compared"]
    if not n:
        print("\n  NOTHING TO COMPARE -- no bucket appears in both databases.")
        print(
            f"  production-only buckets {result['production_only']}, "
            f"rehearsal-only {result['shadow_only']}"
        )
        print("  Either the rehearsal did not run over this window, or the")
        print("  session bounds are wrong. This is NOT agreement.")
        return

    print(f"\n  buckets in both           {n}")
    if result["production_only"] or result["shadow_only"]:
        print(
            f"  production-only           {result['production_only']}   "
            f"<- in production, missing from the rehearsal"
        )
        print(f"  rehearsal-only            {result['shadow_only']}")
    print(
        f"  net imbalance SIGN agrees {result['sign_agrees']} of {n} "
        f"({result['sign_agreement_pct']:.1f}%)"
    )
    print(
        f"  rehearsal net HIGHER in   {result['shadow_higher']} of {n}   "
        f"<- near n/2 is noise, near n is bias"
    )
    print(f"  mean |difference|         {result['mean_abs_difference']:,.0f}")
    print(f"  mean signed difference    {result['mean_signed_difference']:+,.0f}")
    print(f"  mean |production net|     {result['mean_abs_production']:,.0f}   <- for scale")

    # A flip RATE alone cannot tell a cosmetic disagreement from a real one.
    print(
        f"\n  OF THE {result['sign_disagrees']} FLIPS, by the size of the bucket " f"they landed on"
    )
    print(f"    (this group's median |net| is {result['median_abs_production']:,.0f})")
    print(
        f"    near zero  (< 0.5x median)  {result['flips_near_zero']:>4}   "
        f"<- sign was never meaningful; the site read 'balanced' either way"
    )
    print(f"    typical    (0.5-1.5x)       {result['flips_typical']:>4}")
    print(
        f"    large      (> 1.5x median)  {result['flips_large']:>4}   "
        f"<- a different claim about the session"
    )
    if result["flipped_share_of_flow_pct"] is not None:
        print(
            f"\n  SHARE OF THE SESSION'S TOTAL |IMBALANCE| IN FLIPPED BUCKETS: "
            f"{result['flipped_share_of_flow_pct']:.1f}%"
        )
        print("    This is the number that answers 'would a subscriber notice'.")
        print("    A high flip RATE concentrated near zero can still be a low")
        print("    single-digit share of the flow, and that is a different")
        print("    product decision from the same rate on the day's big prints.")

    print("\n  Snapshot measurement for comparison (F11, 2026-10-08):")
    print("    47.6% of contracts relabelled, net sign flipped 16 of 27 (59%).")
    print("    If the sign agreement above is far better than 41%, bucketing")
    print("    absorbs most of the method change and subscribers see little.")

    if verbose:
        print("\n  bucket                    underlying   production      rehearsal   sign")
        for r in result["rows"]:
            print(
                f"  {str(r['bucket']):<25} {str(r['underlying']):<11} "
                f"{r['production']:>12,} {r['shadow']:>14,}   "
                f"{'ok' if r['sign_agrees'] else 'FLIP'}"
            )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--production-db", default=os.getenv("DB_NAME", "zerogex"))
    parser.add_argument("--shadow-db", default=os.getenv("SHADOW_DB", "zerogex_shadow"))
    parser.add_argument("--underlying", default=None, help="one symbol, or all")
    parser.add_argument("--session-start", required=True, help="UTC, e.g. 2026-10-09T13:30:00Z")
    parser.add_argument("--session-end", required=True)
    parser.add_argument("--verbose", action="store_true", help="print every bucket")
    args = parser.parse_args(argv)

    params = {
        "session_start": args.session_start,
        "session_end": args.session_end,
        "underlying": args.underlying,
    }
    try:
        production = fetch(args.production_db, params)
        shadow = fetch(args.shadow_db, params)
    except Exception as e:
        print(f"query failed: {e}", file=sys.stderr)
        return 2

    _print_report(
        compare_buckets(production, shadow), args.production_db, args.shadow_db, args.verbose
    )

    per_symbol = compare_by_underlying(production, shadow)
    if len(per_symbol) > 1:
        print("\n=== PER SYMBOL ===")
        print("  The tick test reads a SEQUENCE of trade prices, so a contract")
        print("  that prints rarely leaves more zero ticks inheriting a carried")
        print("  direction. Expect thinner symbols to agree less than SPY.")
        print(
            f"\n  {'symbol':<10} {'buckets':>8} {'sign agrees':>13} "
            f"{'flips near0/typ/large':>22} {'flipped % of flow':>18}"
        )
        for sym, r in per_symbol.items():
            if not r["buckets_compared"]:
                print(f"  {str(sym):<10} (no overlapping buckets)")
                continue
            pct = f"{r['sign_agreement_pct']:.1f}%"
            bands = f"{r['flips_near_zero']}/{r['flips_typical']}/{r['flips_large']}"
            share = (
                f"{r['flipped_share_of_flow_pct']:.1f}%"
                if r["flipped_share_of_flow_pct"] is not None
                else "n/a"
            )
            print(
                f"  {str(sym):<10} {r['buckets_compared']:>8} {pct:>13} " f"{bands:>22} {share:>18}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
