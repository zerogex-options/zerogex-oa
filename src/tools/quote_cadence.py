"""How often each symbol's live price actually changes, measured at the source.

The Gamma Terminal's price and its forming candle must move every second. The
website can only move them as often as ingestion publishes a new price: every
stored price change is announced on the ``zgx_quote_updates`` channel (see
``_publish_quote_notify`` in ``src/ingestion/main_engine.py``), and the API's
WebSocket pushes each announcement to the browser as it arrives. This listens
to the same channel, read-only, and reports per symbol how many announcements
came in, the typical and worst gap between them, and how many gaps ran past a
second and a half.

A healthy liquid symbol in the regular session shows close to 60 a minute and
a median gap near 1.0 s. Long gaps mean the feed itself stalled: with
ThetaData, a bar poll that fails backs off for 2, 4, 8 and then 16 polls'
worth of time, and the ingestion journal says why (``thetadata bar poll for
SPY failed``). A quiet symbol can also skip a second simply because nothing
changed, since an unchanged bar is not stored or announced.

Usage::

    python -m src.tools.quote_cadence                 # listen for 60 s
    python -m src.tools.quote_cadence --seconds 300
"""

from __future__ import annotations

import argparse
import json
import select
import statistics
import time
from typing import Dict, List, NamedTuple

CHANNEL = "zgx_quote_updates"
SLOW_GAP_SECONDS = 1.5


class Cadence(NamedTuple):
    symbol: str
    updates: int
    per_minute: float
    median_gap: float
    p95_gap: float
    max_gap: float
    slow_gaps: int


def summarize(arrivals: Dict[str, List[float]], seconds: float) -> List[Cadence]:
    """Per-symbol cadence from arrival times (monotonic seconds), busiest first."""
    rows = []
    for symbol, times in arrivals.items():
        times = sorted(times)
        gaps = [b - a for a, b in zip(times, times[1:])]
        if gaps:
            ordered = sorted(gaps)
            p95 = ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))]
            median, worst = statistics.median(gaps), ordered[-1]
        else:
            median = p95 = worst = float("nan")
        rows.append(
            Cadence(
                symbol=symbol,
                updates=len(times),
                per_minute=len(times) * 60.0 / seconds if seconds > 0 else float("nan"),
                median_gap=median,
                p95_gap=p95,
                max_gap=worst,
                slow_gaps=sum(1 for g in gaps if g > SLOW_GAP_SECONDS),
            )
        )
    return sorted(rows, key=lambda r: (-r.updates, r.symbol))


def listen(seconds: float) -> Dict[str, List[float]]:
    """Collect announcement arrival times per symbol for ``seconds``."""
    from src.database.connection import db_connection

    arrivals: Dict[str, List[float]] = {}
    with db_connection() as conn:
        # LISTEN only delivers outside a transaction. The pool resets this
        # when the connection goes back.
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"LISTEN {CHANNEL}")
        try:
            end = time.monotonic() + seconds
            while (left := end - time.monotonic()) > 0:
                if select.select([conn], [], [], min(left, 1.0)) == ([], [], []):
                    continue
                conn.poll()
                now = time.monotonic()
                while conn.notifies:
                    note = conn.notifies.pop(0)
                    try:
                        symbol = json.loads(note.payload).get("symbol")
                    except (ValueError, AttributeError):
                        continue
                    if isinstance(symbol, str) and symbol:
                        arrivals.setdefault(symbol, []).append(now)
        finally:
            cursor.execute(f"UNLISTEN {CHANNEL}")
    return arrivals


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--seconds", type=float, default=60.0, help="how long to listen (default 60)"
    )
    args = parser.parse_args()

    print(f"Listening on {CHANNEL} for {args.seconds:.0f} s ...", flush=True)
    rows = summarize(listen(args.seconds), args.seconds)
    if not rows:
        print("No price announcements arrived. Outside market hours that is normal;")
        print("during them, ingestion is not publishing (check zerogex-oa-ingestion).")
        return
    print(
        f"{'symbol':<8}{'updates':>8}{'per min':>9}{'median gap':>12}{'95% gap':>9}"
        f"{'worst gap':>11}{'gaps >' + format(SLOW_GAP_SECONDS, 'g') + ' s':>12}"
    )
    for r in rows:
        print(
            f"{r.symbol:<8}{r.updates:>8}{r.per_minute:>9.1f}{r.median_gap:>11.2f}s"
            f"{r.p95_gap:>8.2f}s{r.max_gap:>10.2f}s{r.slow_gaps:>12}"
        )


if __name__ == "__main__":
    main()
