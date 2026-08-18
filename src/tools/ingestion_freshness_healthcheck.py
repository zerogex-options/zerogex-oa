"""Alert when a configured underlying stops writing bars during its session.

This is the detector the 2026-08-17 QQQ outage did not have. One ingestion
worker (a child process of ``zerogex-oa-ingestion``, one per symbol) died at
the Monday session close and was never restarted, so QQQ wrote nothing from
Monday 20:00 ET until an operator noticed by eye and restarted the unit at
Tuesday 10:03 ET -- roughly six hours into a session it should have been
streaming. Nothing caught it: the parent process was still alive, so systemd
reported the unit ``active``, ``Restart=always`` never triggered, ``OnFailure``
never dispatched, and the per-minute liveness watchdog -- which only runs
``systemctl is-active`` -- stayed green the whole time.

Every one of those checks asks "is the process up". None asks "is the data
arriving". This one does, per symbol, which is the only question whose answer
would have caught a single dead worker among healthy siblings.

Staleness is anchored at ``max(last_bar, session_open)`` so the first run after
an open measures from the open rather than from the previous session's last
bar -- otherwise every session would start with a spurious overnight-sized gap.
Symbols outside their delivery window are reported ``not_expected`` and never
alert: cash indices (SPX, NDX) print an underlying level only 09:30-16:00 ET
even under a 24-hour template, which ``feed_session_window`` already encodes.

Usage:
    python -m src.tools.ingestion_freshness_healthcheck
    python -m src.tools.ingestion_freshness_healthcheck --json
    python -m src.tools.ingestion_freshness_healthcheck --max-stale-minutes 10

Exit codes:
    0 -- every expected symbol is fresh (or none is in its session window).
    1 -- at least one expected symbol is stale.
    2 -- database connection or query error.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import List, Optional

import pytz

from src.market_calendar import feed_session_window, underlying_feed_expected
from src.symbols import get_canonical_symbol, parse_underlyings

logger = logging.getLogger(__name__)

ET = pytz.timezone("US/Eastern")

# A 1-minute bar feed is legitimately silent up to ~60s between bars, and
# pre-market prints are sparse (a thin tape can skip several minutes without
# anything being wrong). 15 minutes is comfortably above that noise floor and
# still an order of magnitude tighter than the ~6 hours the QQQ outage ran.
DEFAULT_MAX_STALE_MINUTES = 15


@dataclass(frozen=True)
class SymbolFreshness:
    symbol: str
    status: str  # "fresh" | "stale" | "not_expected"
    last_bar: Optional[str]
    stale_minutes: Optional[float]


def configured_symbols() -> List[str]:
    """Canonical DB symbols for the underlyings the ingestion unit runs.

    Mirrors ``main_engine.main``: ``INGEST_UNDERLYINGS`` (falling back to
    ``INGEST_UNDERLYING``) resolved through ``SYMBOL_ALIASES``, then reversed
    to the canonical symbol the engine actually writes rows under -- the
    engine queries TradeStation for ``$SPXW.X`` but stores ``SPX``.
    """
    raw = os.getenv("INGEST_UNDERLYINGS") or os.getenv("INGEST_UNDERLYING") or "SPY"
    return [get_canonical_symbol(symbol) for symbol in parse_underlyings(raw)]


def evaluate(
    symbol: str,
    last_bar: Optional[datetime],
    now_et: datetime,
    session_template: str,
    max_stale: timedelta,
) -> SymbolFreshness:
    """Classify one symbol's feed. Pure -- no DB, no clock."""
    if not underlying_feed_expected(now_et, session_template, symbol):
        return SymbolFreshness(symbol, "not_expected", _iso(last_bar), None)

    # Anchor at the session open so the first check after an open does not
    # measure against the previous session's final bar.
    open_t, _close_t = feed_session_window(session_template, symbol)
    session_open = ET.localize(datetime.combine(now_et.date(), open_t))
    anchor = session_open
    if last_bar is not None:
        last_bar_et = last_bar.astimezone(ET)
        if last_bar_et > anchor:
            anchor = last_bar_et

    stale = now_et - anchor
    stale_minutes = round(stale.total_seconds() / 60.0, 1)
    status = "stale" if stale > max_stale else "fresh"
    return SymbolFreshness(symbol, status, _iso(last_bar), stale_minutes)


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _fetch_last_bars(symbols: List[str]) -> dict:
    """Return ``{symbol: last_timestamp_or_None}`` for the given symbols."""
    from src.database.connection import db_connection

    out = {symbol: None for symbol in symbols}
    if not symbols:
        return out
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT symbol, MAX(timestamp)
                FROM underlying_quotes
                WHERE symbol = ANY(%s)
                  AND timestamp > NOW() - INTERVAL '7 days'
                GROUP BY symbol
                """,
                (symbols,),
            )
            for symbol, last_ts in cursor.fetchall():
                out[symbol] = last_ts
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Alert when a configured underlying stops writing bars mid-session."
    )
    parser.add_argument(
        "--max-stale-minutes",
        type=float,
        default=float(os.getenv("INGEST_FRESHNESS_MAX_STALE_MINUTES", DEFAULT_MAX_STALE_MINUTES)),
        help=f"Staleness threshold in minutes (default: {DEFAULT_MAX_STALE_MINUTES})",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated canonical symbols to check (default: INGEST_UNDERLYINGS)",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of log lines")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from dotenv import load_dotenv

    load_dotenv()

    if args.symbols:
        symbols = [
            get_canonical_symbol(s.strip().upper()) for s in args.symbols.split(",") if s.strip()
        ]
    else:
        symbols = configured_symbols()

    if not symbols:
        logger.error("No underlyings configured (INGEST_UNDERLYINGS / INGEST_UNDERLYING)")
        return 2

    session_template = os.getenv("SESSION_TEMPLATE", "Default")
    max_stale = timedelta(minutes=args.max_stale_minutes)
    now_et = datetime.now(ET)

    try:
        last_bars = _fetch_last_bars(symbols)
    except Exception as exc:  # pragma: no cover - needs a live DB to exercise
        logger.error("Freshness check could not query underlying_quotes: %s", exc, exc_info=True)
        return 2

    results = [
        evaluate(symbol, last_bars.get(symbol), now_et, session_template, max_stale)
        for symbol in symbols
    ]
    stale = [r for r in results if r.status == "stale"]

    if args.json:
        print(
            json.dumps(
                {
                    "checked_at": now_et.isoformat(),
                    "session_template": session_template,
                    "max_stale_minutes": args.max_stale_minutes,
                    "symbols": [asdict(r) for r in results],
                },
                indent=2,
            )
        )
    else:
        for r in results:
            if r.status == "not_expected":
                logger.info("%s: outside its delivery window — not checked", r.symbol)
            elif r.status == "fresh":
                logger.info("%s: fresh (%.1f min since last bar)", r.symbol, r.stale_minutes or 0.0)
            else:
                logger.error(
                    "%s: STALE — no bar for %.1f min (threshold %.1f). Last bar: %s. "
                    "The per-symbol ingestion worker for %s is likely dead or stalled; "
                    "check `systemctl status zerogex-oa-ingestion` and the child PIDs.",
                    r.symbol,
                    r.stale_minutes or 0.0,
                    args.max_stale_minutes,
                    r.last_bar or "none in the last 7 days",
                    r.symbol,
                )

    if stale:
        logger.error(
            "Ingestion freshness check FAILED for %d of %d symbol(s): %s",
            len(stale),
            len(results),
            ", ".join(r.symbol for r in stale),
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
