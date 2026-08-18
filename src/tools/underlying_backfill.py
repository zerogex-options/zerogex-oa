"""Backfill historical 1-minute underlying bars into ``underlying_quotes``.

The backtester's benchmark, its daily-return risk metrics (Sharpe / Sortino /
CAGR), and any price-based custom-strategy testing are all bounded by how much
underlying history is present. Underlying OHLC is the *cheap* half of deep
history — TradeStation serves years of 1-minute bars for SPY / SPX / QQQ — so
this tool pulls a date range and upserts it, extending what those price-based
paths can reach without any data purchase (the expensive half, historical
option chains, is the separate vendor decision in
``docs/design/historical-options-data-vendors.md``).

It reuses the same bar shape, validation, and ``underlying_quotes`` upsert as
the live ingester (``src/ingestion/main_engine.py``), so a backfilled bar is
indistinguishable from a streamed one. Idempotent on ``(symbol, timestamp)``.

``--symbols`` takes the canonical/DB symbols (``SPY``, ``SPX``, ``QQQ``,
``NDX``). Index symbols are resolved through ``SYMBOL_ALIASES`` for the
TradeStation fetch (``SPX`` -> ``$SPXW.X``, ``NDX`` -> ``$NDXP.X``) exactly as
the live ingester resolves them, while the rows are still written under the
canonical symbol — so a backfilled index bar lands in the same
``underlying_quotes.symbol`` the live feed and the charts use. Equities
without an alias (``SPY`` / ``QQQ``) fetch and write under the same string,
unchanged.

**Sessions.** A backfill exists to repair what the live feed missed, so it
requests the same window the live feed runs in: 04:00-20:00 ET
(``USEQ24Hour``), matching the ingester's ``SESSION_TEMPLATE``, rather than
TradeStation's ``Default``. The template bounds how much of the day the
endpoint will serve, so it has to be at least as wide as the window the
charts draw. It is a necessary condition, not a sufficient one — the vendor
can still be missing bars inside the requested window (see the coverage check
below). Cash indices (SPX / NDX) print only 09:30-16:00 ET whatever the
template, which is why that check is symbol-aware.

**Dates.** ``--start`` / ``--end`` are inclusive *ET trading dates*, and the
request windows are anchored on ET midnights. A UTC-anchored day would start
at 20:00 ET the evening before and stop at 19:59 ET, clipping the 20:00 ET
close of the last day requested.

After each symbol the tool reports the ET span it actually received and warns
per trading day whose first bar lands after the session open — the silent
failure this tool used to have was returning a plausible bar count that began
mid-morning.

Usage::

    python -m src.tools.underlying_backfill --symbols SPY,SPX,QQQ,NDX \
        --start 2026-05-01 --end 2026-07-23

    # Repair today's pre-market after an overnight ingester outage:
    python -m src.tools.underlying_backfill --symbols QQQ \
        --start 2026-08-18 --end 2026-08-18

Verify against a live TradeStation session + database — the pure range/parse
and alias-resolution logic is unit-tested (``tests/test_underlying_backfill.py``),
but the HTTP and DB paths need real credentials to exercise.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from src.market_calendar import feed_session_window, load_nyse_holidays
from src.symbols import resolve_symbol
from src.validation import safe_datetime, safe_float

logger = logging.getLogger(__name__)

# TradeStation's barchart endpoints cap a single response (~57.6k bars). Chunk
# the request window so even a 24h-session day (~1,440 1-min bars) stays well
# under the cap: 25 days × 1,440 ≈ 36k. Tunable via --days-per-chunk.
_DEFAULT_DAYS_PER_CHUNK = 25
# Politeness pause between chunk requests so a long backfill doesn't hammer the
# API or trip rate limits.
_INTER_REQUEST_SECONDS = 0.3

_ET = ZoneInfo("America/New_York")

# TradeStation session template for the fetch. The template bounds the day
# the endpoint will serve, so it has to be at least as wide as the window the
# charts draw. "USEQ24Hour" is the 04:00-20:00 ET window the ingester streams
# in production (SESSION_TEMPLATE), so a backfilled day covers exactly what
# the streamed day would have — which is the whole point of a backfill.
# Overridable via --session-template for a narrower pull ("Default",
# "USEQPre", ...).
#
# Widening the template is necessary but not sufficient: on 2026-08-18 QQQ
# returned bars from 07:35 ET under BOTH "Default" and "USEQ24Hour", for two
# different firstdates 43 minutes apart — an absolute boundary in the
# vendor's data, not in the request. When a run comes back short, the
# coverage check below is what says so; do not assume the template explains
# it.
_DEFAULT_SESSION_TEMPLATE = "USEQ24Hour"

# How late a trading day's first bar may land before the coverage check calls
# it a gap. Pre-market can open quiet, so a few missing minutes at 04:00 are
# not a fault; an hour is.
_COVERAGE_GRACE_MINUTES = 15


def _safe_bigint(value: Any) -> int:
    """Non-negative integer, defaulting to 0 (mirrors the live ingester)."""
    try:
        out = int(value)
    except (TypeError, ValueError):
        return 0
    return out if out >= 0 else 0


def _chunk_ranges(
    start: date, end: date, days_per_chunk: int = _DEFAULT_DAYS_PER_CHUNK
) -> List[Tuple[str, str]]:
    """Split ``[start, end]`` into ``(firstdate, lastdate)`` ISO-8601 chunks.

    ``start`` / ``end`` are inclusive **ET trading dates**, and each chunk
    spans ET midnight-to-midnight rendered as the UTC instants TradeStation
    expects (``YYYY-MM-DDTHH:MM:SSZ``): 00:00:00 ET of the chunk's first day
    through 23:59:59 ET of its last. Chunks never overlap and cover the whole
    window; a reversed range yields nothing.

    Anchoring on ET rather than UTC midnight matters at both edges of an
    extended-hours day (04:00-20:00 ET). A UTC-anchored day for 2026-08-18
    reads as 2026-08-17 20:00 ET .. 2026-08-18 19:59 ET: it drags in the
    previous evening's post-market and clips the requested day's own 20:00 ET
    close. The ET-anchored window (2026-08-18T04:00:00Z .. 2026-08-19T
    03:59:59Z here) contains the full 04:00-20:00 ET session and nothing else.
    Built with a real ET zone rather than a fixed offset, so a range spanning
    a DST boundary stays aligned on both sides of it.
    """
    if end < start or days_per_chunk < 1:
        return []
    out: List[Tuple[str, str]] = []
    cur = start
    step = timedelta(days=days_per_chunk)
    one_day = timedelta(days=1)
    while cur <= end:
        chunk_end = min(cur + step - one_day, end)
        first = datetime(cur.year, cur.month, cur.day, 0, 0, 0, tzinfo=_ET)
        last = datetime(chunk_end.year, chunk_end.month, chunk_end.day, 23, 59, 59, tzinfo=_ET)
        out.append((_iso_z(first), _iso_z(last)))
        cur = chunk_end + one_day
    return out


def _iso_z(dt: datetime) -> str:
    """Render a UTC datetime as ``YYYY-MM-DDTHH:MM:SSZ``."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _bar_to_row(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a raw TradeStation bar into an ``underlying_quotes`` row, or None.

    Mirrors the live ingester's validation: a full, positive OHLC set with
    ``high >= low``. Partial/degenerate bars (e.g. the current forming bar, or a
    gap) are skipped rather than allowed to violate the table's CHECK
    constraints on insert. ``UpVolume`` / ``DownVolume`` are streaming-only
    fields absent from the historical barcharts endpoint this tool uses, so
    they yield 0/0 on a backfilled bar (OHLC is unaffected).
    """
    ts = safe_datetime(raw.get("TimeStamp"), field_name="TimeStamp")  # type: ignore[arg-type]
    if ts is None:
        return None
    o = safe_float(raw.get("Open"), field_name="Open", default=None)
    h = safe_float(raw.get("High"), field_name="High", default=None)
    low = safe_float(raw.get("Low"), field_name="Low", default=None)
    c = safe_float(raw.get("Close"), field_name="Close", default=None)
    if None in (o, h, low, c):
        return None
    if o <= 0 or h <= 0 or low <= 0 or c <= 0 or h < low:
        return None
    return {
        "timestamp": ts,
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        "up_volume": _safe_bigint(raw.get("UpVolume")),
        "down_volume": _safe_bigint(raw.get("DownVolume")),
    }


def _et_span(rows: List[Dict[str, Any]]) -> str:
    """Human-readable ET span of ``rows`` for the progress log.

    A bare bar count hides the failure this tool is most prone to — a
    plausible-looking number of bars that starts mid-session — so every chunk
    logs the window it actually came back with, in the timezone the operator
    reasons about.
    """
    stamps = [r["timestamp"] for r in rows if r.get("timestamp") is not None]
    if not stamps:
        return "no bars"
    first = min(stamps).astimezone(_ET)
    last = max(stamps).astimezone(_ET)
    if first.date() == last.date():
        return f"{first:%Y-%m-%d %H:%M}-{last:%H:%M} ET"
    return f"{first:%Y-%m-%d %H:%M} ET..{last:%Y-%m-%d %H:%M} ET"


def _coverage_gaps(
    rows: List[Dict[str, Any]],
    start: date,
    end: date,
    *,
    symbol: Optional[str] = None,
    grace_minutes: int = _COVERAGE_GRACE_MINUTES,
    now_et: Optional[datetime] = None,
) -> List[Tuple[date, datetime, Optional[datetime]]]:
    """Trading days whose first fetched bar lands after the session opened.

    Returns ``(day, expected_open_et, first_bar_et_or_None)`` per offending
    day. The bar to measure against is the one the *charts* want — the first
    print of the symbol's widest real session (04:00 ET for equities and
    ETFs; 09:30 for cash indices, which have no pre-market print) — and
    deliberately NOT the open of whatever ``--session-template`` the fetch
    happened to use. Scoring a run against its own template would let the
    exact mistake this check exists to catch mark itself complete: a
    core-session fetch returns core-session bars, declares success, and
    leaves the pre-market hole on the chart.

    Weekends and NYSE holidays are skipped, as is any day whose open has not
    happened yet (backfilling through "today" before 04:00 ET is not a gap).
    ``now_et`` is injectable for tests.
    """
    holidays = load_nyse_holidays()
    now = now_et or datetime.now(tz=_ET)
    open_t, _close_t = feed_session_window("USEQ24Hour", symbol)

    first_by_day: Dict[date, datetime] = {}
    for row in rows:
        ts = row.get("timestamp")
        if ts is None:
            continue
        ts_et = ts.astimezone(_ET)
        seen = first_by_day.get(ts_et.date())
        if seen is None or ts_et < seen:
            first_by_day[ts_et.date()] = ts_et

    gaps: List[Tuple[date, datetime, Optional[datetime]]] = []
    day = start
    one_day = timedelta(days=1)
    grace = timedelta(minutes=max(0, grace_minutes))
    while day <= end:
        if day.weekday() < 5 and day not in holidays:
            expected = datetime.combine(day, open_t, tzinfo=_ET)
            if expected <= now:
                got = first_by_day.get(day)
                if got is None or got - expected > grace:
                    gaps.append((day, expected, got))
        day += one_day
    return gaps


def _log_coverage(
    symbol: str,
    rows: List[Dict[str, Any]],
    start: date,
    end: date,
    *,
    session_template: str,
) -> int:
    """Report the ET span fetched, warn per short trading day, return the count.

    The count is what makes an incomplete backfill visible: the caller turns a
    non-zero total into a non-zero exit status, so "ran fine, still missing
    half the morning" can't pass for success.
    """
    logger.info("%s: fetched %d bars (%s)", symbol, len(rows), _et_span(rows))
    gaps = _coverage_gaps(rows, start, end, symbol=symbol)
    for day, expected, got in gaps:
        if got is None:
            logger.warning(
                "%s %s: no bars fetched for a trading day (opens %s ET, template %s)",
                symbol,
                day,
                expected.strftime("%H:%M"),
                session_template,
            )
        else:
            logger.warning(
                "%s %s: first bar %s ET, %d min after the %s ET session open "
                "(template %s) — that window will stay empty on the charts",
                symbol,
                day,
                got.strftime("%H:%M"),
                int((got - expected).total_seconds() // 60),
                expected.strftime("%H:%M"),
                session_template,
            )
    return len(gaps)


_UPSERT_SQL = """
    INSERT INTO underlying_quotes
    (symbol, timestamp, open, high, low, close, up_volume, down_volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, timestamp) DO UPDATE SET
        open = COALESCE(underlying_quotes.open, EXCLUDED.open),
        high = GREATEST(underlying_quotes.high, EXCLUDED.high),
        low = LEAST(underlying_quotes.low, EXCLUDED.low),
        close = EXCLUDED.close,
        up_volume = EXCLUDED.up_volume,
        down_volume = EXCLUDED.down_volume,
        updated_at = NOW()
"""


def upsert_bars(conn, symbol: str, rows: List[Dict[str, Any]]) -> int:
    """Upsert parsed bars for ``symbol``; returns the number written."""
    if not rows:
        return 0
    cur = conn.cursor()
    cur.executemany(
        _UPSERT_SQL,
        [
            (
                symbol,
                r["timestamp"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["up_volume"],
                r["down_volume"],
            )
            for r in rows
        ],
    )
    return len(rows)


def fetch_symbol(
    client,
    symbol: str,
    start: date,
    end: date,
    *,
    days_per_chunk: int = _DEFAULT_DAYS_PER_CHUNK,
    session_template: str = _DEFAULT_SESSION_TEMPLATE,
    sleep_seconds: float = _INTER_REQUEST_SECONDS,
) -> List[Dict[str, Any]]:
    """Fetch + parse all 1-minute bars for ``symbol`` across the window.

    Uses the historical barcharts endpoint (``marketdata/barcharts``, via
    ``get_bars``) chunked over the range, in the extended-hours session
    ``session_template`` names (04:00-20:00 ET by default — see
    ``_DEFAULT_SESSION_TEMPLATE``; the endpoint returns nothing outside the
    template's own window, so the template, not the date range, is what
    decides whether pre-market bars come back at all). This is deliberately
    NOT the streaming endpoint (``get_stream_bars`` / ``marketdata/stream/barcharts``):
    that one is a real-time snapshot that ignores ``firstdate``/``lastdate``
    and returns only the latest bar, so it cannot backfill a range. The trade
    is that the historical endpoint carries no Up/Down volume split, so
    backfilled bars land with ``up_volume=0`` / ``down_volume=0`` (OHLC — what
    the candlestick charts draw — is exact). Deduplicates on timestamp (chunk
    boundaries never overlap, but a defensive dedup guards against the API
    returning an edge bar twice).
    """
    seen: set = set()
    rows: List[Dict[str, Any]] = []
    for first, last in _chunk_ranges(start, end, days_per_chunk):
        chunk_rows: List[Dict[str, Any]] = []
        payload = client.get_bars(
            symbol,
            interval=1,
            unit="Minute",
            firstdate=first,
            lastdate=last,
            sessiontemplate=session_template,
            warn_if_closed=False,
        )
        bars = (payload or {}).get("Bars") or []
        for raw in bars:
            row = _bar_to_row(raw)
            if row is None:
                continue
            key = row["timestamp"]
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
            chunk_rows.append(row)
        logger.info(
            "%s %s..%s [%s] → %d bars, %s (running %d)",
            symbol,
            first,
            last,
            session_template,
            len(bars),
            _et_span(chunk_rows),
            len(rows),
        )
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return rows


def backfill(
    symbols: List[str],
    start: date,
    end: date,
    *,
    days_per_chunk: int = _DEFAULT_DAYS_PER_CHUNK,
    session_template: str = _DEFAULT_SESSION_TEMPLATE,
    dry_run: bool = False,
    gaps: Optional[Dict[str, int]] = None,
) -> Dict[str, int]:
    """Backfill each symbol; returns ``{symbol: rows_written}``.

    ``gaps``, when passed, is filled in as ``{symbol: short_trading_days}``
    from the coverage check — an out-parameter rather than a second return
    value so the ``{symbol: rows_written}`` contract callers already depend on
    is unchanged.
    """
    from src.database import db_connection
    from src.ingestion.tradestation_client import TradeStationClient

    # Construct from env credentials, same as the live entrypoints
    # (main_engine / futures ingester). The no-arg TradeStationClient() the
    # tool used before is invalid — the constructor requires the three creds.
    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID"),
        os.getenv("TRADESTATION_CLIENT_SECRET"),
        os.getenv("TRADESTATION_REFRESH_TOKEN"),
        sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true",
    )
    written: Dict[str, int] = {}
    for symbol in symbols:
        # Resolve the canonical/DB symbol to its TradeStation fetch symbol via
        # SYMBOL_ALIASES (e.g. SPX -> $SPXW.X, NDX -> $NDXP.X), exactly as the
        # live ingester does — but keep WRITING rows under the canonical symbol
        # so a backfilled index bar lands in the same underlying_quotes.symbol
        # the charts read. Aliasless equities (SPY/QQQ) resolve to themselves.
        ts_symbol = resolve_symbol(symbol)
        if ts_symbol != symbol:
            logger.info(
                "Resolved %s -> %s via SYMBOL_ALIASES for fetch; "
                "writing rows under canonical '%s'",
                symbol,
                ts_symbol,
                symbol,
            )
        rows = fetch_symbol(
            client,
            ts_symbol,
            start,
            end,
            days_per_chunk=days_per_chunk,
            session_template=session_template,
        )
        # Report against the CANONICAL symbol: the coverage check is
        # symbol-aware (a cash index legitimately has no pre-market bar) and
        # is_cash_index keys off the canonical name, not the TS chain symbol.
        short_days = _log_coverage(symbol, rows, start, end, session_template=session_template)
        if gaps is not None:
            gaps[symbol] = short_days
        if dry_run:
            logger.info(
                "[dry-run] %s (%s): %d bars parsed, not written",
                symbol,
                ts_symbol,
                len(rows),
            )
            written[symbol] = 0
            continue
        with db_connection() as conn:
            written[symbol] = upsert_bars(conn, symbol, rows)
        logger.info("%s: wrote %d bars to underlying_quotes", symbol, written[symbol])
    return written


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill historical 1-min underlying bars.")
    parser.add_argument("--symbols", required=True, help="Comma-separated, e.g. SPY,SPX,QQQ")
    parser.add_argument(
        "--start", required=True, help="Inclusive start ET trading date, YYYY-MM-DD"
    )
    parser.add_argument("--end", required=True, help="Inclusive end ET trading date, YYYY-MM-DD")
    parser.add_argument("--days-per-chunk", type=int, default=_DEFAULT_DAYS_PER_CHUNK)
    parser.add_argument(
        "--session-template",
        default=_DEFAULT_SESSION_TEMPLATE,
        help=(
            "TradeStation session template. Default %(default)s (04:00-20:00 ET), "
            "matching the live ingester, so pre-market and post-market bars are "
            "backfilled. 'Default' restricts the fetch to the core session, does "
            "not reach the 04:00 ET open, and will NOT recover a pre-market gap."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch + parse but do not write")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Load .env so SYMBOL_ALIASES (for index-symbol resolution) and the
    # TradeStation credentials are available when run directly as
    # `python -m src.tools.underlying_backfill`. load_dotenv does not override
    # already-exported vars, so an explicit shell env still wins.
    from dotenv import load_dotenv

    load_dotenv()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if end < start:
        parser.error("--end must be on or after --start")

    gaps: Dict[str, int] = {}
    result = backfill(
        symbols,
        start,
        end,
        days_per_chunk=args.days_per_chunk,
        session_template=args.session_template,
        dry_run=args.dry_run,
        gaps=gaps,
    )
    total = sum(result.values())
    logger.info("Backfill complete: %s (total %d bars)", result, total)

    short = {sym: n for sym, n in gaps.items() if n}
    if short:
        # Exit non-zero so an incomplete repair can't read as a successful
        # one. A backfill that returns a plausible bar count but starts
        # mid-session leaves exactly the hole it was run to fill, and the
        # only place that shows up otherwise is the chart, hours later.
        logger.error(
            "Incomplete coverage: %d trading day(s) start after the session open %s. "
            "Those windows stay empty on the charts. The template caps how much of "
            "the day is served (%s), but a gap INSIDE the requested window is the "
            "vendor's, not this tool's — re-run the affected dates later, and compare "
            "another symbol and another date to tell the two apart.",
            sum(short.values()),
            short,
            args.session_template,
        )
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
