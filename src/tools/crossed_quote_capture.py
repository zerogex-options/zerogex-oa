"""Capture crossed Market Value option quotes, for the ThetaData bug report.

ThetaData confirmed on 2026-09-29 that ``option_snapshot_market_value``
returns quotes with the bid ABOVE the ask on penny-wide spreads, that the
crossing comes from the Market Value calculation rather than the underlying
OPRA quote, and that it is being raised with their team. They asked for
per-contract examples with timestamps.

This produces them. One CSV row per crossed contract per sample, carrying
the vendor's own timestamp so their team can line each row up against their
raw NBBO for the same instant.

Deliberately narrow:

* It reads ONLY ``option_snapshot_market_value``, through the same provider
  call the ingestion engine uses. It does not touch the realtime quote
  endpoint -- whether we may read raw NBBO at all is an open licensing
  question (see docs/compliance/market-data-feed-comparison-findings-2026-09.md,
  F4), and a diagnostic is no place to prejudge it.
* It writes no database rows. The CSV is the only output.

Usage:

    make crossed-capture MINUTES=30
    make crossed-capture UNDERLYING=SPY MINUTES=10 OUT=~/crossed.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import time
from typing import Any, Dict, List

from src.ingestion.providers import get_provider
from src.tools.feed_compare import FeedSample, sample_provider
from src.utils import get_logger

logger = get_logger(__name__)

#: The columns ThetaData asked for, plus the ones that make a row
#: self-explanatory without the covering email.
CSV_COLUMNS = (
    "captured_at_utc",
    "quote_timestamp",
    "symbol",
    "expiration",
    "strike",
    "right",
    "market_bid",
    "market_ask",
    "cross_cents",
    "underlying_spot",
)


def crossed_rows(sample: FeedSample) -> List[Dict[str, Any]]:
    """Every contract in ``sample`` whose bid sits above its ask.

    Strictly above. A LOCKED quote (bid == ask) is a legitimate state for a
    tight at-the-money contract and is not what we are reporting; only a
    genuine crossing is.
    """
    rows: List[Dict[str, Any]] = []
    captured = sample.captured_at.isoformat() if sample.captured_at else ""
    for symbol, quote in sample.quotes.items():
        bid, ask = quote.bid, quote.ask
        if bid is None or ask is None or bid <= ask:
            continue
        meta = sample.metadata.get(symbol) or {}
        rows.append(
            {
                "captured_at_utc": captured,
                "quote_timestamp": (
                    quote.timestamp.isoformat() if quote.timestamp is not None else ""
                ),
                "symbol": symbol,
                "expiration": meta.get("expiration", ""),
                "strike": meta.get("strike", ""),
                "right": meta.get("option_type", ""),
                "market_bid": bid,
                "market_ask": ask,
                # In cents, because "crossed by exactly one cent" is the
                # claim their team will want to check, and a float
                # subtraction prints it as 0.010000000000000675.
                "cross_cents": int(round((bid - ask) * 100)),
                "underlying_spot": sample.spot if sample.spot is not None else "",
            }
        )
    return rows


def _summarise(rows: List[Dict[str, Any]], samples: int, quoted: int) -> str:
    if not rows:
        return (
            f"{samples} samples, {quoted} contracts quoted, NO crossed quotes seen.\n"
            "Nothing to send -- either it is fixed or this window did not "
            "reproduce it."
        )
    widths: Dict[int, int] = {}
    for row in rows:
        widths[row["cross_cents"]] = widths.get(row["cross_cents"], 0) + 1
    spread = ", ".join(f"{c}c x{n}" for c, n in sorted(widths.items()))
    contracts = len({r["symbol"] for r in rows})
    return (
        f"{samples} samples, {quoted} contracts quoted, {len(rows)} crossed rows "
        f"across {contracts} distinct contracts.\n"
        f"cross width: {spread}"
    )


def main(argv: Any = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlying", default="SPY")
    parser.add_argument(
        "--provider",
        default="thetadata_mv",
        help="the Market Value provider; the point is to sample ITS quotes",
    )
    parser.add_argument("--duration-minutes", type=float, default=30.0)
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    parser.add_argument("--expirations", type=int, default=3)
    parser.add_argument("--strike-count-max", type=int, default=40)
    parser.add_argument("--strike-pct-range", type=float, default=3.0)
    parser.add_argument("--out", default="crossed-quotes.csv")
    args = parser.parse_args(argv)

    provider = get_provider(args.provider)
    deadline = time.monotonic() + args.duration_minutes * 60
    all_rows: List[Dict[str, Any]] = []
    samples = 0
    quoted = 0

    # Opened before the loop and flushed per sample, so a Ctrl-C partway
    # through still leaves a usable file on disk.
    handle = io.open(args.out, "w", encoding="utf-8", newline="")
    writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    try:
        while True:
            sample = sample_provider(
                provider,
                args.underlying,
                num_expirations=args.expirations,
                strike_count_max=args.strike_count_max,
                strike_pct_range=args.strike_pct_range,
            )
            if sample.error:
                print(f"  sample failed: {sample.error}", file=sys.stderr)
            else:
                samples += 1
                quoted += len(sample.quotes)
                rows = crossed_rows(sample)
                all_rows.extend(rows)
                for row in rows:
                    writer.writerow(row)
                handle.flush()
                print(
                    f"  {sample.captured_at:%H:%M:%S} UTC  "
                    f"{len(rows):>3} crossed of {len(sample.quotes)}"
                )
            if time.monotonic() >= deadline:
                break
            time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
    finally:
        handle.close()
        provider.close()

    print()
    print(_summarise(all_rows, samples, quoted))
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
