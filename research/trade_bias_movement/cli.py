"""Command line: ``selftest`` (no database) and ``run`` (read-only).

python -m research.trade_bias_movement.cli selftest
python -m research.trade_bias_movement.cli run
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from research.trade_bias_movement.study import ITERATIONS


def _parse_day(text: str) -> datetime:
    try:
        return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise SystemExit(f"cannot parse date {text!r}; use YYYY-MM-DD")


def _connect():
    try:
        from src.database import db_connection
    except Exception as exc:  # pragma: no cover
        raise SystemExit(
            f"cannot import the database layer ({exc}).\n"
            "Run from the repository root with the service's environment loaded."
        )
    return db_connection()


def cmd_selftest(args: argparse.Namespace) -> int:
    from research.trade_bias_movement.selftest import main as selftest_main

    print("Synthetic worlds (invented data; checks the machinery, not the market):")
    return selftest_main()


def cmd_run(args: argparse.Namespace) -> int:
    from research.short_gamma_trend.outcomes import SessionBars
    from research.short_gamma_trend.sources import archive_span, load_bars, load_readings
    from research.short_gamma_trend.study import BuildCounts
    from research.trade_bias_movement.report import render
    from research.trade_bias_movement.study import build, run_study

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    rows = []
    counts = BuildCounts()
    with _connect() as conn:
        spans = {s: archive_span(conn, s) for s in symbols}
        firsts = [first for first, _, _ in spans.values() if first is not None]
        if not firsts:
            print("No Trade Bias history for " + ", ".join(symbols) + ".")
            return 1
        start = _parse_day(args.since) if args.since else min(firsts)
        end = (
            _parse_day(args.until) + timedelta(days=1)
            if args.until
            else datetime.now(timezone.utc) + timedelta(minutes=1)
        )
        for sym in symbols:
            readings = load_readings(conn, sym, start, end)
            # The bars start a day early: a reading's prior window never
            # reaches back past its own session, but the loader's bounds are
            # UTC dates, and this keeps the first session's open covered.
            bars = SessionBars(
                load_bars(conn, sym, start - timedelta(days=1), end + timedelta(days=1))
            )
            sym_rows, sym_counts = build(sym, readings, bars)
            rows.extend(sym_rows)
            counts.merge(sym_counts)

    result = run_study(rows, symbols, iterations=args.iterations)
    print(render(result, counts, spans, iterations=args.iterations))
    if args.json:
        payload = {"counts": counts.__dict__, **result.as_dict()}
        with open(args.json, "w") as fh:
            json.dump(payload, fh, indent=2, default=str)
        print(f"\nwrote {args.json}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m research.trade_bias_movement.cli",
        description="Test whether the Trade Bias panel's states say how far price will move.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("selftest", help="synthetic worlds; validates the machinery")
    p.set_defaults(func=cmd_selftest)

    p = sub.add_parser("run", help="replay the archive (read-only) and print the report")
    p.add_argument("--symbols", default="SPY,SPX", help="comma-separated (default SPY,SPX)")
    p.add_argument("--since", help="first session, YYYY-MM-DD (default: start of the archive)")
    p.add_argument("--until", help="last session, YYYY-MM-DD (default: today)")
    p.add_argument("--iterations", type=int, default=ITERATIONS, help="bootstrap resamples")
    p.add_argument("--json", help="also write every number to this file")
    p.set_defaults(func=cmd_run)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
