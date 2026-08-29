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

from src.market_calendar import (
    current_futures_session_start,
    feed_session_window,
    is_futures_session_open,
    option_chain_feed_expected,
    option_chain_feed_window,
    underlying_feed_expected,
)
from src.symbols import get_canonical_symbol, parse_underlyings

logger = logging.getLogger(__name__)

ET = pytz.timezone("US/Eastern")

# A 1-minute bar feed is legitimately silent up to ~60s between bars, and
# pre-market prints are sparse (a thin tape can skip several minutes without
# anything being wrong). 15 minutes is comfortably above that noise floor and
# still an order of magnitude tighter than the ~6 hours the QQQ outage ran.
DEFAULT_MAX_STALE_MINUTES = 15


# Every TradeStation stream this deployment holds, by what it writes. The
# check used to cover underlying bars alone, which is one of four families:
# a dead option-chain worker, a dead VIX stream or a dead futures stream all
# left the same silence the QQQ outage did, with nothing watching.
FEED_UNDERLYING = "underlying bars"
FEED_CHAINS = "option chains"
FEED_VOLATILITY = "volatility index"
FEED_FUTURES = "ES/NQ futures"


@dataclass(frozen=True)
class FeedFreshness:
    """One stream's freshness. ``symbol`` is that feed's key, not always a
    ticker: the futures feeds are keyed by their BACKING INDEX, because that
    is what futures_quotes stores (ES rows live under SPX)."""

    feed: str
    symbol: str
    status: str  # "fresh" | "stale" | "not_expected"
    last_bar: Optional[str]
    stale_minutes: Optional[float]


def evaluate_feed(
    feed: str,
    symbol: str,
    last_bar: Optional[datetime],
    now_et: datetime,
    *,
    expected: bool,
    session_anchor: Optional[datetime],
    max_stale: timedelta,
) -> FeedFreshness:
    """Classify any feed. Pure -- no DB, no clock.

    ``session_anchor`` is where staleness is measured from when the feed has
    not written since the session opened; without it the first check after an
    open reports the whole overnight gap as an outage.
    """
    if not expected:
        return FeedFreshness(feed, symbol, "not_expected", _iso(last_bar), None)

    anchor = session_anchor or now_et
    if last_bar is not None:
        last_bar_et = last_bar.astimezone(ET)
        if last_bar_et > anchor:
            anchor = last_bar_et

    stale = now_et - anchor
    # Clamp at zero: a bar can legitimately be stamped in the FUTURE. vix_bars
    # and vxn_bars store TradeStation's raw TimeStamp, which is the bar's
    # CLOSE, on a 5-minute interval -- so the 09:30-09:35 bar is stamped 09:35
    # and reads as -3.1 minutes old at 09:31. Reporting a negative staleness is
    # nonsense on its face and undermines every other line in the report.
    #
    # The cost is that this feed's staleness is understated by up to one bar
    # interval, so a dead VIX stream is caught within ~20 minutes rather than
    # the nominal 15. Acceptable; over-reporting freshness by one interval is
    # far better than a report an operator learns to distrust.
    stale_minutes = max(0.0, round(stale.total_seconds() / 60.0, 1))
    return FeedFreshness(
        feed, symbol, "stale" if stale > max_stale else "fresh", _iso(last_bar), stale_minutes
    )


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


def _fetch_last_chain_writes(symbols: List[str]) -> dict:
    """Last option_chains write per underlying."""
    from src.database.connection import db_connection

    out = {symbol: None for symbol in symbols}
    if not symbols:
        return out
    # One bounded lookup per symbol, NOT `GROUP BY underlying` with a MAX over
    # a date range. The grouped form blew the 90s statement timeout on its
    # first production run -- fired seconds after a services-restart, so four
    # workers were re-seeding into the same table.
    #
    # Its cost scales with the range: option_chains holds every contract of
    # every underlying for every minute inside DATA_RETENTION_DAYS, and a
    # 7-day window over that is a lot of index to walk before aggregating.
    # (A 400k-row reproduction did NOT reproduce the timeout -- Postgres used
    # the index and finished in under a millisecond -- so the exact trigger is
    # production scale, write contention, or both.)
    #
    # This form sidesteps the question rather than tuning it: ORDER BY
    # timestamp DESC LIMIT 1 against idx_option_chains_underlying_timestamp
    # touches one index entry and stops, whatever the table holds.
    with db_connection() as conn:
        with conn.cursor() as cursor:
            for symbol in symbols:
                cursor.execute(
                    """
                    SELECT timestamp
                    FROM option_chains
                    WHERE underlying = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (symbol,),
                )
                row = cursor.fetchone()
                out[symbol] = row[0] if row else None
    return out


def _fetch_last_volatility_bars(tickers: List[str]) -> dict:
    """Last bar per volatility index. One table each, no symbol column."""
    from src.database.connection import db_connection

    tables = {"VIX": "vix_bars", "VXN": "vxn_bars"}
    out = {t: None for t in tickers}
    with db_connection() as conn:
        with conn.cursor() as cursor:
            for ticker in tickers:
                table = tables.get(ticker)
                if not table:
                    continue
                # Table names come from the literal map above, never from input.
                cursor.execute(f"SELECT MAX(timestamp) FROM {table}")  # nosec B608
                row = cursor.fetchone()
                out[ticker] = row[0] if row else None
    return out


def _fetch_last_futures_bars(index_symbols: List[str]) -> dict:
    """Last futures_quotes bar per BACKING INDEX (ES rows live under SPX)."""
    from src.database.connection import db_connection

    out = {symbol: None for symbol in index_symbols}
    if not index_symbols:
        return out
    # Same shape as the chains lookup, and for the same reason: futures_quotes
    # now carries every minute of every CME session inside DATA_RETENTION_DAYS,
    # where it used to hold a rolling overnight window.
    with db_connection() as conn:
        with conn.cursor() as cursor:
            for symbol in index_symbols:
                cursor.execute(
                    """
                    SELECT timestamp
                    FROM futures_quotes
                    WHERE index_symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (symbol,),
                )
                row = cursor.fetchone()
                out[symbol] = row[0] if row else None
    return out


def _session_anchor(session_template: str, symbol: str, now_et: datetime) -> datetime:
    """Today's delivery-window open, in ET."""
    open_t, _close_t = feed_session_window(session_template, symbol)
    return ET.localize(datetime.combine(now_et.date(), open_t))


def _chain_anchor(symbol: str, now_et: datetime) -> datetime:
    """Today's OPTION-chain open (09:30 ET), in ET."""
    open_t, _close_t = option_chain_feed_window(symbol, now_et.date())
    return ET.localize(datetime.combine(now_et.date(), open_t))


def collect_feeds(
    symbols: List[str],
    session_template: str,
    now_et: datetime,
    max_stale: timedelta,
) -> List[FeedFreshness]:
    """Freshness for every stream this deployment is configured to hold."""
    results: List[FeedFreshness] = []

    # --- underlying bars + option chains -------------------------------
    # The two do NOT share a window. Bars follow the session template
    # (04:00-20:00 under USEQ24Hour); chains follow the OPTIONS session,
    # 09:30-16:15, because a chain row is only written when an option quote
    # ticks. Grading chains against the bar window alerted every pre-market
    # and every evening -- real 16-45 minute quote gaps in tails where the
    # options market is shut -- which is the "cries wolf" failure this check
    # cannot afford. Cash indexes stop at 16:00 either way: Greeks are refused
    # once the index stops printing, so no rows are written.
    bars = _fetch_last_bars(symbols)
    chains = _fetch_last_chain_writes(symbols)
    for symbol in symbols:
        graded = (
            (
                FEED_UNDERLYING,
                bars,
                underlying_feed_expected(now_et, session_template, symbol),
                _session_anchor(session_template, symbol, now_et),
            ),
            (
                FEED_CHAINS,
                chains,
                option_chain_feed_expected(now_et, symbol),
                _chain_anchor(symbol, now_et),
            ),
        )
        for feed, source, expected, anchor in graded:
            results.append(
                evaluate_feed(
                    feed,
                    symbol,
                    source.get(symbol),
                    now_et,
                    expected=expected,
                    session_anchor=anchor,
                    max_stale=max_stale,
                )
            )

    # --- VIX / VXN ------------------------------------------------------
    # is_cash_index() does not recognise them, so borrow SPX's window: the
    # regular cash session. Conservative -- VIX publishes before 09:30 and
    # this will not check that tail -- but it never false-alarms, and a
    # stream dead through the session is the failure worth catching.
    vol = [t for t, on in (("VIX", _vix_enabled()), ("VXN", _vxn_enabled())) if on]
    if vol:
        vol_bars = _fetch_last_volatility_bars(vol)
        vol_expected = underlying_feed_expected(now_et, session_template, "SPX")
        vol_anchor = _session_anchor(session_template, "SPX", now_et)
        for ticker in vol:
            results.append(
                evaluate_feed(
                    FEED_VOLATILITY,
                    ticker,
                    vol_bars.get(ticker),
                    now_et,
                    expected=vol_expected,
                    session_anchor=vol_anchor,
                    max_stale=max_stale,
                )
            )

    # --- ES / NQ futures -------------------------------------------------
    # Their own calendar: Sun 18:00 -> Fri 17:00 ET minus the daily
    # maintenance break, which is nothing like an equity session.
    futures_indexes = _futures_indexes()
    if futures_indexes:
        futures_bars = _fetch_last_futures_bars(futures_indexes)
        futures_expected = is_futures_session_open(now_et)
        futures_anchor = current_futures_session_start(now_et) if futures_expected else None
        for symbol in futures_indexes:
            results.append(
                evaluate_feed(
                    FEED_FUTURES,
                    symbol,
                    futures_bars.get(symbol),
                    now_et,
                    expected=futures_expected,
                    session_anchor=futures_anchor,
                    max_stale=max_stale,
                )
            )

    return results


def _vix_enabled() -> bool:
    from src.config import _getenv_bool

    return _getenv_bool("INGEST_VIX_ENABLED", True)


def _vxn_enabled() -> bool:
    from src.config import _getenv_bool

    return _getenv_bool("INGEST_VXN_ENABLED", True)


def _futures_indexes() -> List[str]:
    from src.config import _getenv_bool

    if not _getenv_bool("INGEST_FUTURES_ENABLED", False):
        return []
    raw = os.getenv("INGEST_FUTURES_INDEXES", "SPX")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


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
        feeds = collect_feeds(symbols, session_template, now_et, max_stale)
        last_bars = _fetch_last_bars(symbols)
    except Exception as exc:  # pragma: no cover - needs a live DB to exercise
        logger.error("Freshness check could not query the ingestion tables: %s", exc, exc_info=True)
        return 2

    # Underlying bars keep their own list so the JSON's long-standing
    # "symbols" key stays exactly as any existing scraper expects; "feeds"
    # carries every stream.
    results = [
        evaluate(symbol, last_bars.get(symbol), now_et, session_template, max_stale)
        for symbol in symbols
    ]
    stale = [f for f in feeds if f.status == "stale"]

    if args.json:
        print(
            json.dumps(
                {
                    "checked_at": now_et.isoformat(),
                    "session_template": session_template,
                    "max_stale_minutes": args.max_stale_minutes,
                    "symbols": [asdict(r) for r in results],
                    "feeds": [asdict(f) for f in feeds],
                },
                indent=2,
            )
        )
    else:
        for f in feeds:
            tag = f"{f.feed} {f.symbol}"
            if f.status == "not_expected":
                logger.info("%s: outside its delivery window — not checked", tag)
            elif f.status == "fresh":
                logger.info("%s: fresh (%.1f min since last write)", tag, f.stale_minutes or 0.0)
            else:
                logger.error(
                    "%s: STALE — nothing written for %.1f min (threshold %.1f). Last: %s. "
                    "That stream is dead or stalled; check `systemctl status "
                    "zerogex-oa-ingestion` and its child PIDs.",
                    tag,
                    f.stale_minutes or 0.0,
                    args.max_stale_minutes,
                    f.last_bar or "never",
                )
        if not stale:
            checked = [f for f in feeds if f.status != "not_expected"]
            logger.info(
                "%d of %d streams checked, all fresh (%d outside their window).",
                len(checked),
                len(feeds),
                len(feeds) - len(checked),
            )
    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
