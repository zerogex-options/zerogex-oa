"""Probe how early TradeStation will serve 1-minute bars for a symbol/date.

Read-only diagnostic. When a backfill comes back short (see
``src/tools/underlying_backfill.py``'s coverage check), this separates the
three things that can cause it, which the backfill itself cannot tell apart:

* **the request** — session template too narrow, or ``firstdate`` mis-set;
* **the query mode** — ``firstdate``/``lastdate`` vs ``barsback``, which are
  different paths on TradeStation's side;
* **the vendor** — bars genuinely absent from their store for that window.

It asks the same day several ways and prints the earliest bar each one
yields. If every probe reports the same start, the request is not the
constraint and the gap is upstream. Comparing a second symbol (``--symbols
QQQ,SPY``) and a known-good date localizes it further: one symbol short on
one date is a vendor gap for that symbol; every symbol short on the same
date is a feed-wide outage.

Usage::

    python -m src.tools.probe_bar_availability --symbols QQQ,SPY --date 2026-08-18
    python -m src.tools.probe_bar_availability --symbols QQQ --date 2026-08-17
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, time, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from src.symbols import resolve_symbol

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
# Bars back far enough to reach 04:00 ET from a late-morning run (~7h of
# minutes), exercising the barsback path rather than firstdate/lastdate.
_BARSBACK = 600


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _et_day_bounds(day: date) -> Tuple[str, str]:
    """The full ET calendar day, as the UTC instants TradeStation expects."""
    first = datetime.combine(day, time(0, 0, 0), tzinfo=_ET)
    last = datetime.combine(day, time(23, 59, 59), tzinfo=_ET)
    return _iso_z(first), _iso_z(last)


def _earliest_et(bars: List[Dict[str, Any]], day: Optional[date] = None) -> Optional[datetime]:
    """Earliest bar timestamp in ET, optionally restricted to ``day``."""
    stamps = []
    for bar in bars:
        raw = bar.get("TimeStamp")
        if not raw:
            continue
        try:
            ts = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        ts_et = ts.astimezone(_ET)
        if day is not None and ts_et.date() != day:
            continue
        stamps.append(ts_et)
    return min(stamps) if stamps else None


def _probes(day: date) -> List[Tuple[str, str, Dict[str, Any]]]:
    """The (label, session template, get_bars kwargs) matrix.

    Each row varies ONE axis against the same day, so a difference in the
    result points at that axis and nothing else.
    """
    first, last = _et_day_bounds(day)
    pre_first = _iso_z(datetime.combine(day, time(4, 0), tzinfo=_ET))
    pre_last = _iso_z(datetime.combine(day, time(9, 30), tzinfo=_ET))
    day_range = dict(firstdate=first, lastdate=last)
    return [
        # Session template, holding the date range fixed at the full ET day.
        ("full ET day", "Default", day_range),
        ("full ET day", "USEQ24Hour", day_range),
        ("full ET day", "USEQPre", day_range),
        # Narrowed to the pre-market window only — a different range, same day.
        ("04:00-09:30", "USEQPre", dict(firstdate=pre_first, lastdate=pre_last)),
        # barsback instead of firstdate/lastdate: a different query path.
        (f"barsback={_BARSBACK}", "USEQ24Hour", dict(barsback=_BARSBACK)),
    ]


def probe(client, symbol: str, day: date) -> List[Tuple[str, Optional[datetime], int]]:
    """Run every probe for ``symbol``; returns (label, earliest_et, bar_count).

    A bar count of -1 means the request itself failed — a rejected template
    or parameter is an answer too, so it is reported rather than raised.
    """
    out: List[Tuple[str, Optional[datetime], int]] = []
    for window, template, kwargs in _probes(day):
        label = f"{template:<11}/ {window}"
        try:
            payload = client.get_bars(
                symbol,
                interval=1,
                unit="Minute",
                sessiontemplate=template,
                warn_if_closed=False,
                **kwargs,
            )
            bars = (payload or {}).get("Bars") or []
        except Exception as exc:  # a rejected template/param is itself an answer
            logger.warning("%s [%s] failed: %s", symbol, label, exc)
            out.append((label, None, -1))
            continue
        out.append((label, _earliest_et(bars, day), len(bars)))
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Probe TradeStation bar availability.")
    parser.add_argument("--symbols", required=True, help="Comma-separated, e.g. QQQ,SPY")
    parser.add_argument("--date", required=True, help="ET trading date, YYYY-MM-DD")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from dotenv import load_dotenv

    load_dotenv()

    from src.ingestion.tradestation_client import TradeStationClient

    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID"),
        os.getenv("TRADESTATION_CLIENT_SECRET"),
        os.getenv("TRADESTATION_REFRESH_TOKEN"),
        sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true",
    )

    day = date.fromisoformat(args.date)
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    print(f"\nEarliest 1-min bar available for {day} (ET)\n" + "=" * 62)
    for symbol in symbols:
        ts_symbol = resolve_symbol(symbol)
        print(f"\n{symbol} (fetched as {ts_symbol}):")
        for label, earliest, count in probe(client, ts_symbol, day):
            if count < 0:
                print(f"  {label:<34} request failed")
            elif earliest is None:
                print(f"  {label:<34} no bars on {day}")
            else:
                print(f"  {label:<34} {earliest:%H:%M} ET   ({count} bars)")
    print(
        "\nSame start across every row => the request is not the constraint;\n"
        "the bars are absent upstream. Compare a known-good date to confirm.\n"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
