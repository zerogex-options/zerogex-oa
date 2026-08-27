"""Find the batch size at which TradeStation's quote endpoint stops answering.

The REST seed asks for ``OPTION_BATCH_SIZE`` option symbols in ONE URL — 100 by
default, which is a ~4KB request path. When TradeStation's quote service is
under strain that request is the first thing to time out, and the seed logs::

    REST seed batch failed: 504 Server Error: Gateway Timeout
    {"Error":"GatewayTimeout","Message":"Get Quotes timed out."}

A 504 is their gateway giving up on their own quote service — not auth (401),
not entitlement (403), and nothing a credential change can cause or cure. The
only lever on this side is asking for less per request.

This probe walks a ladder of batch sizes against the LIVE endpoint and reports
status and latency for each, so ``OPTION_BATCH_SIZE`` can be set from a
measurement rather than a guess. Symbols come from ``option_chains``, so they
are real contracts the ingester actually requests.

Usage::

    python -m src.tools.probe_option_quote_batches
    python -m src.tools.probe_option_quote_batches --underlying SPX --sizes 1,10,25,50,100
    python -m src.tools.probe_option_quote_batches --repeat 3   # is it intermittent?

Reading the output: the largest size that is reliably OK across repeats is the
value to set. If even size 1 times out, the endpoint is down and no batch size
helps — wait it out, and check https://status.tradestation.com.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import List, Optional

from dotenv import load_dotenv

from src.utils import get_logger

logger = get_logger(__name__)

_DEFAULT_SIZES = "1,5,10,25,50,75,100"


def fetch_symbols(underlying: str, needed: int) -> List[str]:
    """Real contract symbols the ingester would actually request."""
    from src.database import db_connection

    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT option_symbol
            FROM option_chains
            WHERE underlying = %s
              AND timestamp > NOW() - INTERVAL '3 days'
            ORDER BY option_symbol
            LIMIT %s
            """,
            (underlying.upper(), needed),
        )
        return [r[0] for r in cur.fetchall()]


def probe(client, symbols: List[str], size: int) -> dict:
    """One request at one batch size. Never raises."""
    batch = symbols[:size]
    started = time.monotonic()
    try:
        result = client.get_option_quotes(batch)
        elapsed = time.monotonic() - started
        quotes = len(result.get("Quotes", []) or []) if isinstance(result, dict) else 0
        errors = result.get("Errors", []) or [] if isinstance(result, dict) else []
        return {
            "size": size,
            "ok": not errors,
            "seconds": elapsed,
            "quotes": quotes,
            "detail": str(errors[0]) if errors else "",
        }
    except Exception as e:
        return {
            "size": size,
            "ok": False,
            "seconds": time.monotonic() - started,
            "quotes": 0,
            "detail": f"{type(e).__name__}: {e}"[:120],
        }


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Probe TradeStation option-quote latency by batch size."
    )
    parser.add_argument("--underlying", default="SPY")
    parser.add_argument("--sizes", default=_DEFAULT_SIZES)
    parser.add_argument("--repeat", type=int, default=1, help="Rounds, to spot flakiness")
    parser.add_argument(
        "--credential",
        choices=("main", "futures"),
        default="main",
        help="Which TradeStation identity to probe with. Running BOTH separates "
        "'their infrastructure is degraded' from 'something changed on one of my "
        "accounts' — the two look identical from a single account.",
    )
    args = parser.parse_args(argv)

    try:
        sizes = sorted({int(s) for s in args.sizes.split(",") if s.strip()})
    except ValueError:
        print(f"--sizes must be comma-separated integers, got {args.sizes!r}", file=sys.stderr)
        return 2

    symbols = fetch_symbols(args.underlying, max(sizes))
    if len(symbols) < max(sizes):
        print(
            f"Only {len(symbols)} {args.underlying} contracts in option_chains; "
            f"probing up to that size.",
            file=sys.stderr,
        )
        sizes = [s for s in sizes if s <= len(symbols)] or [len(symbols)]
    if not symbols:
        print(f"No recent {args.underlying} contracts in option_chains.", file=sys.stderr)
        return 1

    from src.ingestion.tradestation_client import TradeStationClient
    from src.config import futures_tradestation_credentials
    import os

    if args.credential == "futures":
        creds = futures_tradestation_credentials()
    else:
        creds = (
            os.getenv("TRADESTATION_CLIENT_ID", ""),
            os.getenv("TRADESTATION_CLIENT_SECRET", ""),
            os.getenv("TRADESTATION_REFRESH_TOKEN", ""),
        )
    client = TradeStationClient(*creds)

    # Name the account being probed. A 504 rate that differs between two
    # identities on the SAME application, same endpoint, same minute is an
    # account-level fact; an identical rate is an infrastructure one.
    try:
        from src.tools.tradestation_whoami import decode_jwt_payload, username_of

        who = username_of(decode_jwt_payload(client.auth.get_access_token()))
    except Exception:
        who = None

    print("=" * 68)
    print(f"Option quote probe — {args.underlying}, {args.repeat} round(s)")
    print(f"credential: {args.credential}   username: {who or '(unknown)'}")
    print("=" * 68)

    # Every size, every round. An earlier version stopped climbing at the first
    # failure and reported that size as a threshold — but these failures are
    # INTERMITTENT, so one unlucky request read as a cliff and produced a
    # confident, wrong recommendation. A rate needs more than one sample.
    results: dict = {size: [] for size in sizes}
    for round_no in range(1, max(1, args.repeat) + 1):
        for size in sizes:
            r = probe(client, symbols, size)
            results[size].append(r)
            status = "OK" if r["ok"] else "FAIL"
            note = "" if r["ok"] else f"  {r['detail'][:90]}"
            print(f"  round {round_no}  batch {size:>4}  {status:<5} {r['seconds']:>7.2f}s{note}")
            time.sleep(0.3)

    print()
    print(f"{'batch':>6} {'ok':>8} {'median s':>10} {'slowest s':>10}")
    for size in sizes:
        runs = results[size]
        oks = [r for r in runs if r["ok"]]
        times = sorted(r["seconds"] for r in runs)
        median = times[len(times) // 2] if times else 0.0
        print(f"{size:>6} {len(oks)}/{len(runs):>6} {median:>10.2f} {max(times, default=0):>10.2f}")

    # Recommend from the DISTRIBUTION, not from where the ladder first tripped.
    # A single failure at one size says nothing; a size that answered every
    # time across every round is a number you can set.
    reliable = [size for size in sizes if all(r["ok"] for r in results[size])]
    every = [r for runs in results.values() for r in runs]
    ok_rate = sum(1 for r in every if r["ok"]) / max(1, len(every))
    slow = [r["seconds"] for r in every if r["seconds"] >= 2]
    # HOW a request fails is the diagnosis, not THAT it failed. A request the
    # server accepts and then abandons on its deadline is a struggling backend;
    # one refused in a tenth of a second was never processed at all — wrong
    # entitlement, wrong symbol format, bad credential. Reporting both as
    # "failed" once led this tool to call an unentitled account an outage.
    refused = [r for r in every if not r["ok"] and r["seconds"] < 2]
    timed_out = [r for r in every if not r["ok"] and r["seconds"] >= 2]

    print("=" * 68)
    if refused and not timed_out:
        print(f"Every request was REFUSED in under two seconds ({len(refused)} of them).")
        print("That is a rejection, not a timeout — the server never tried. The")
        print("usual cause is this credential lacking the market-data entitlement")
        print("for these symbols (equity options need OPRA; a futures-only")
        print("username will refuse every one of them). Batch size is irrelevant.")
        print("Confirm which username this ran as with: make ts-whoami")
        detail = next((r["detail"] for r in refused if r["detail"]), "")
        if detail:
            print(f"First error: {detail[:200]}")
    elif len(reliable) == len(sizes) and not slow:
        print("Every size answered promptly — the endpoint is healthy.")
        print("Leave OPTION_BATCH_SIZE alone.")
    elif not reliable:
        print(f"No size answered reliably ({ok_rate:.0%} overall).")
        print(f"{len(timed_out)} request(s) were accepted and then timed out —")
        print("their backend is struggling rather than refusing you. No batch")
        print("size helps at this rate. Check https://status.tradestation.com;")
        print("the client's retry/backoff picks it up on recovery.")
    else:
        best = max(reliable)
        print(f"Largest size that answered EVERY time: {best}")
        print(f"Overall success across all sizes: {ok_rate:.0%}")
        if timed_out:
            print(f"{len(timed_out)} request(s) accepted then abandoned on a server")
            print("deadline — a struggling backend, not a rejection.")
        print()
        print(f"    OPTION_BATCH_SIZE={best}       # in .env, then make services-restart")
        print()
        print("This is a WORKAROUND for a degraded upstream, not a permanent")
        print("setting: smaller batches mean proportionally more requests. Re-run")
        print("this probe once the endpoint recovers and restore the larger size.")
    print("=" * 68)
    return 0


if __name__ == "__main__":
    sys.exit(main())
