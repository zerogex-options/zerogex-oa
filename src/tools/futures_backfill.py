"""Backfill historical 1-minute futures bars into ``futures_quotes``.

The live futures ingester only ever holds a rolling window — it streams the
current session and prunes past ``FUTURES_BARS_RETENTION_DAYS`` (default 7).
That is enough for the basis measurement and the intraday chart, and not
enough for anything else ES/NQ want:

* the **daily and hourly** candlestick timeframes, which request up to 576
  buckets and today render as a near-empty strip for a future;
* a **basis history** long enough to see how the index->future ratio behaved
  through a previous quarterly roll;
* any **backtest or replay** over ES/NQ price action.

This tool pulls a date range from TradeStation's historical barcharts endpoint
and upserts it under the CASH INDEX key, exactly as the live ingester writes,
so a backfilled bar is indistinguishable from a streamed one. Idempotent on
``(index_symbol, timestamp)``.

**Retention is the thing to get right first.** The ingester prunes
``futures_quotes`` on its own schedule, and it does not know which rows came
from a backfill — so anything you load outside the retention window is deleted
on the next prune. Raise ``FUTURES_BARS_RETENTION_DAYS`` to cover the range you
intend to keep BEFORE running this, or the work is thrown away. The tool warns
when the requested range exceeds the configured retention rather than letting
that happen quietly.

**Stamping.** Bars are stamped on their own minute —
``bucket_timestamp(ts - 1s)`` — matching the live futures ingester and
``underlying_quotes``. TradeStation stamps a bar at its CLOSE, so writing the
raw timestamp would put backfilled bars one minute ahead of live ones and
mis-pair the index/future basis join. (Note ``underlying_backfill.py`` does
NOT apply this normalisation, so its rows sit a minute ahead of the live
underlying feed; that is a pre-existing inconsistency in that tool, not
something this one inherits.)

**Symbols.** ``--symbols`` takes the CASH INDEX (``SPX``, ``NDX``) — the same
key the rows are written under and the same key the API reads. The continuous
future actually fetched is resolved through ``INDEX_FUTURES_MAP``
(``SPX`` -> ``@ES``), exactly as the live ingester resolves it.

**Sessions.** Requests use ``FUTURES_SESSION_TEMPLATE`` (default ``Default``),
the same template the live stream uses, so the backfilled window matches what
the ingester would have written.

Usage::

    # Keep 90 days first, or the backfill is pruned away.
    #   FUTURES_BARS_RETENTION_DAYS=90   in .env, then restart ingestion.

    python -m src.tools.futures_backfill --symbols SPX,NDX \\
        --start 2026-06-01 --end 2026-08-21

    make futures-backfill SYMBOLS=SPX,NDX START=2026-06-01 END=2026-08-21
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from src.config import _getenv_int, _getenv_str
from src.database import db_connection, close_connection_pool
from src.symbols import resolve_index_future
from src.tools.underlying_backfill import _chunk_ranges, _et_span, _safe_bigint
from src.utils import get_logger
from src.validation import bucket_timestamp, safe_datetime, safe_float

logger = get_logger(__name__)

_DEFAULT_DAYS_PER_CHUNK = 25
_INTER_REQUEST_SECONDS = 0.3

_UPSERT_SQL = """
INSERT INTO futures_quotes
    (index_symbol, future_symbol, timestamp, open, high, low, close,
     up_volume, down_volume)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (index_symbol, timestamp) DO UPDATE SET
    future_symbol = EXCLUDED.future_symbol,
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    up_volume = EXCLUDED.up_volume,
    down_volume = EXCLUDED.down_volume,
    updated_at = NOW()
"""


def _bar_to_row(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a raw TradeStation bar into a ``futures_quotes`` row, or None.

    Mirrors the live futures ingester exactly, including the bar-start
    stamping — see the module docstring. Up/Down volume are streaming-only
    fields the historical endpoint does not carry, so they land 0/0; OHLC,
    which is what the charts and the basis read, is exact.
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
        "timestamp": bucket_timestamp(ts - timedelta(seconds=1), 60),
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        "up_volume": _safe_bigint(raw.get("UpVolume")),
        "down_volume": _safe_bigint(raw.get("DownVolume")),
    }


def upsert_bars(conn, index_symbol: str, future_symbol: str, rows: List[Dict[str, Any]]) -> int:
    """Upsert ``rows`` under the cash-index key. Idempotent."""
    if not rows:
        return 0
    cursor = conn.cursor()
    cursor.executemany(
        _UPSERT_SQL,
        [
            (
                index_symbol,
                future_symbol,
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
    conn.commit()
    return len(rows)


def fetch_future(
    client,
    future_symbol: str,
    start: date,
    end: date,
    *,
    days_per_chunk: int = _DEFAULT_DAYS_PER_CHUNK,
    session_template: str = "Default",
    sleep_seconds: float = _INTER_REQUEST_SECONDS,
) -> List[Dict[str, Any]]:
    """Fetch + parse every 1-minute bar for ``future_symbol`` across the window.

    Uses the HISTORICAL barcharts endpoint (``get_bars``), chunked over the
    range. Deliberately not the streaming endpoint, which ignores
    ``firstdate`` / ``lastdate`` and returns only the latest bar.
    """
    seen: set = set()
    rows: List[Dict[str, Any]] = []
    for first, last in _chunk_ranges(start, end, days_per_chunk):
        chunk_rows: List[Dict[str, Any]] = []
        payload = client.get_bars(
            future_symbol,
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
            if row["timestamp"] in seen:
                continue
            seen.add(row["timestamp"])
            rows.append(row)
            chunk_rows.append(row)
        logger.info(
            "%s %s..%s [%s] -> %d bars, %s (running %d)",
            future_symbol,
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


def _warn_if_retention_will_delete(start: date) -> None:
    """Loudly refuse to pretend a backfill outside retention will survive."""
    retention_days = _getenv_int("FUTURES_BARS_RETENTION_DAYS", 7)
    cutoff = date.today() - timedelta(days=retention_days)
    if start < cutoff:
        logger.warning(
            "=" * 78,
        )
        logger.warning(
            "FUTURES_BARS_RETENTION_DAYS=%d, so the ingester prunes anything before "
            "%s. Bars requested from %s WILL BE DELETED on the next prune.",
            retention_days,
            cutoff.isoformat(),
            start.isoformat(),
        )
        logger.warning(
            "Raise FUTURES_BARS_RETENTION_DAYS to at least %d and restart ingestion "
            "before relying on this backfill.",
            (date.today() - start).days + 1,
        )
        logger.warning("=" * 78)


def backfill(
    index_symbols: List[str],
    start: date,
    end: date,
    *,
    days_per_chunk: int = _DEFAULT_DAYS_PER_CHUNK,
    session_template: Optional[str] = None,
    dry_run: bool = False,
) -> int:
    """Backfill each index's mapped future. Returns rows written."""
    from src.ingestion.tradestation_client import TradeStationClient

    session_template = session_template or _getenv_str("FUTURES_SESSION_TEMPLATE", "Default")
    _warn_if_retention_will_delete(start)

    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID", ""),
        os.getenv("TRADESTATION_CLIENT_SECRET", ""),
        os.getenv("TRADESTATION_REFRESH_TOKEN", ""),
    )

    written = 0
    for index_symbol in index_symbols:
        index_symbol = index_symbol.strip().upper()
        future_symbol = resolve_index_future(index_symbol)
        if not future_symbol:
            logger.error(
                "No INDEX_FUTURES_MAP entry for %s — skipping. Configured map: %s",
                index_symbol,
                os.getenv("INDEX_FUTURES_MAP", "(defaults)"),
            )
            continue

        logger.info("Backfilling %s (%s) %s..%s", index_symbol, future_symbol, start, end)
        rows = fetch_future(
            client,
            future_symbol,
            start,
            end,
            days_per_chunk=days_per_chunk,
            session_template=session_template,
        )
        if not rows:
            logger.warning(
                "%s: no bars returned. Check the CME market-data entitlement on this "
                "TradeStation account — an unentitled future returns an empty set, "
                "not an error.",
                future_symbol,
            )
            continue

        if dry_run:
            logger.info("%s: DRY RUN, %d bars parsed, %s", index_symbol, len(rows), _et_span(rows))
            continue

        with db_connection() as conn:
            count = upsert_bars(conn, index_symbol, future_symbol, rows)
        written += count
        logger.info("%s: wrote %d bars, %s", index_symbol, count, _et_span(rows))

    return written


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Backfill historical 1-min futures bars into futures_quotes."
    )
    parser.add_argument(
        "--symbols", required=True, help="Comma-separated CASH INDEX symbols, e.g. SPX,NDX"
    )
    parser.add_argument("--start", required=True, help="Inclusive start ET date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="Inclusive end ET date, YYYY-MM-DD")
    parser.add_argument("--days-per-chunk", type=int, default=_DEFAULT_DAYS_PER_CHUNK)
    parser.add_argument("--session-template", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Fetch + parse but do not write")
    args = parser.parse_args(argv)

    logging.getLogger().setLevel(logging.INFO)
    try:
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
        end = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError as e:
        logger.error("Bad date: %s", e)
        return 2
    if end < start:
        logger.error("--end (%s) is before --start (%s)", end, start)
        return 2

    try:
        written = backfill(
            [s for s in args.symbols.split(",") if s.strip()],
            start,
            end,
            days_per_chunk=args.days_per_chunk,
            session_template=args.session_template,
            dry_run=args.dry_run,
        )
        logger.info("Done. %d bars written.", written)
        return 0
    finally:
        close_connection_pool()


if __name__ == "__main__":
    sys.exit(main())
