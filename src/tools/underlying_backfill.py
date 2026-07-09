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

Usage::

    python -m src.tools.underlying_backfill --symbols SPY,QQQ \
        --start 2022-01-01 --end 2022-12-31

Verify against a live TradeStation session + database — the pure range/parse
logic is unit-tested (``tests/test_underlying_backfill.py``), but the HTTP and
DB paths need real credentials to exercise.
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.validation import safe_datetime, safe_float

logger = logging.getLogger(__name__)

# TradeStation's barchart endpoints cap a single response (~57.6k bars). Chunk
# the request window so even a 24h-session day (~1,440 1-min bars) stays well
# under the cap: 25 days × 1,440 ≈ 36k. Tunable via --days-per-chunk.
_DEFAULT_DAYS_PER_CHUNK = 25
# Politeness pause between chunk requests so a long backfill doesn't hammer the
# API or trip rate limits.
_INTER_REQUEST_SECONDS = 0.3


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

    Each chunk is a half-open-friendly inclusive day span rendered as the UTC
    instants TradeStation expects (``YYYY-MM-DDTHH:MM:SSZ``): the first at
    00:00:00, the last at 23:59:59. Chunks never overlap and cover the whole
    window; a reversed range yields nothing.
    """
    if end < start or days_per_chunk < 1:
        return []
    out: List[Tuple[str, str]] = []
    cur = start
    step = timedelta(days=days_per_chunk)
    one_day = timedelta(days=1)
    while cur <= end:
        chunk_end = min(cur + step - one_day, end)
        first = datetime(cur.year, cur.month, cur.day, 0, 0, 0, tzinfo=timezone.utc)
        last = datetime(
            chunk_end.year, chunk_end.month, chunk_end.day, 23, 59, 59, tzinfo=timezone.utc
        )
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
    constraints on insert. ``UpVolume`` / ``DownVolume`` come from the
    stream-barcharts endpoint; a plain barchart without them yields 0/0.
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
    session_template: str = "Default",
    sleep_seconds: float = _INTER_REQUEST_SECONDS,
) -> List[Dict[str, Any]]:
    """Fetch + parse all 1-minute bars for ``symbol`` across the window.

    Uses the stream-barcharts endpoint (Up/Down volume) chunked over the range.
    Deduplicates on timestamp (chunk boundaries never overlap, but a defensive
    dedup guards against the API returning an edge bar twice).
    """
    seen: set = set()
    rows: List[Dict[str, Any]] = []
    for first, last in _chunk_ranges(start, end, days_per_chunk):
        payload = client.get_stream_bars(
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
        logger.info("%s %s..%s → %d bars (running %d)", symbol, first, last, len(bars), len(rows))
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return rows


def backfill(
    symbols: List[str],
    start: date,
    end: date,
    *,
    days_per_chunk: int = _DEFAULT_DAYS_PER_CHUNK,
    session_template: str = "Default",
    dry_run: bool = False,
) -> Dict[str, int]:
    """Backfill each symbol; returns ``{symbol: rows_written}``."""
    from src.database import db_connection
    from src.ingestion.tradestation_client import TradeStationClient

    client = TradeStationClient()
    written: Dict[str, int] = {}
    for symbol in symbols:
        rows = fetch_symbol(
            client,
            symbol,
            start,
            end,
            days_per_chunk=days_per_chunk,
            session_template=session_template,
        )
        if dry_run:
            logger.info("[dry-run] %s: %d bars parsed, not written", symbol, len(rows))
            written[symbol] = 0
            continue
        with db_connection() as conn:
            written[symbol] = upsert_bars(conn, symbol, rows)
        logger.info("%s: wrote %d bars to underlying_quotes", symbol, written[symbol])
    return written


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill historical 1-min underlying bars.")
    parser.add_argument("--symbols", required=True, help="Comma-separated, e.g. SPY,SPX,QQQ")
    parser.add_argument("--start", required=True, help="Inclusive start date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="Inclusive end date, YYYY-MM-DD")
    parser.add_argument("--days-per-chunk", type=int, default=_DEFAULT_DAYS_PER_CHUNK)
    parser.add_argument("--session-template", default="Default")
    parser.add_argument("--dry-run", action="store_true", help="Fetch + parse but do not write")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if end < start:
        parser.error("--end must be on or after --start")

    result = backfill(
        symbols,
        start,
        end,
        days_per_chunk=args.days_per_chunk,
        session_template=args.session_template,
        dry_run=args.dry_run,
    )
    total = sum(result.values())
    logger.info("Backfill complete: %s (total %d bars)", result, total)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
