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

    # The MAIN credential — the one option ingestion uses. Deliberately not the
    # futures credential: this is about the option quote endpoint.
    from src.ingestion.tradestation_client import TradeStationClient
    import os

    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID", ""),
        os.getenv("TRADESTATION_CLIENT_SECRET", ""),
        os.getenv("TRADESTATION_REFRESH_TOKEN", ""),
    )

    print("=" * 68)
    print(f"Option quote probe — {args.underlying}, {args.repeat} round(s)")
    print("=" * 68)
    print(f"{'batch':>6}  {'result':<8} {'seconds':>8} {'quotes':>7}  detail")

    worst_ok = 0
    for _ in range(max(1, args.repeat)):
        for size in sizes:
            r = probe(client, symbols, size)
            status = "OK" if r["ok"] else "FAILED"
            print(
                f"{r['size']:>6}  {status:<8} {r['seconds']:>8.2f} {r['quotes']:>7}  {r['detail']}"
            )
            if r["ok"]:
                worst_ok = max(worst_ok, size)
            else:
                break  # bigger sizes will not do better
            time.sleep(0.3)

    print("=" * 68)
    if worst_ok == 0:
        print("Every size failed — the quote endpoint is down, not overloaded.")
        print("No OPTION_BATCH_SIZE helps. Check https://status.tradestation.com")
        print("and re-run. The ingester's retry/backoff will pick it up on recovery.")
    else:
        print(f"Largest size that answered: {worst_ok}")
        print(f"Set in .env, then restart:   OPTION_BATCH_SIZE={worst_ok}")
        print("Smaller batches mean more requests; raise DELAY_BETWEEN_BATCHES")
        print("(default 0.5s) if that starts drawing rate limits.")
    print("=" * 68)
    return 0


if __name__ == "__main__":
    sys.exit(main())
