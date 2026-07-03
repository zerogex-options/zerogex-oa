"""Forecast receipt writer — fires at 16:05 ET on weekdays.

Looks up the day's session OHLC and computes the verdict columns
(range_respected / pin_hit / regime_correct / setup_outcome) against the
immutable morning commitment. Writes once; the immutability trigger
guarantees a receipt cannot be silently rewritten later.

Like the morning writer, this job never raises. Missing morning row,
missing OHLC, DB failure — all log + exit 0.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger("zerogex.forecast_receipt")
ET = ZoneInfo("America/New_York")


def _today_et() -> date:
    return datetime.now(tz=ET).date()


def _is_trading_day(day: date) -> bool:
    if day.weekday() >= 5:
        return False
    if day in NYSE_HOLIDAYS:
        return False
    return True


async def _fetch_session_ohlc(
    db: DatabaseManager, symbol: str, forecast_date: date
) -> Optional[tuple[float, float, float]]:
    """Compute (low, high, close) for the day's cash session.

    Uses ``get_session_closes`` for the canonical asset-aware close, then
    reads the day's 1-min bars to derive intraday low/high. We don't try
    to be clever about extended hours — the morning commitment is about
    the cash session, so the receipt grades the cash session.
    """
    try:
        closes = await db.get_session_closes(symbol)
    except Exception as exc:
        logger.warning(
            "forecast_receipt: get_session_closes failed for %s (%s)", symbol, exc,
        )
        return None
    if not closes or closes.get("current_session_close") is None:
        return None

    current_ts = closes.get("current_session_close_ts")
    if current_ts is None:
        return None
    if isinstance(current_ts, str):
        try:
            current_ts = datetime.fromisoformat(current_ts.replace("Z", "+00:00"))
        except ValueError:
            current_ts = None
    if current_ts is None:
        return None
    current_session_date = current_ts.astimezone(ET).date()
    if current_session_date != forecast_date:
        logger.info(
            "forecast_receipt: session close is for %s, not requested %s — skipping",
            current_session_date.isoformat(), forecast_date.isoformat(),
        )
        return None

    actual_close = float(closes["current_session_close"])

    # Pull the day's 1-min underlying bars for low/high. The full session
    # query is in the database layer; we just convert it to a tuple here.
    try:
        bars = await db.get_underlying_bars_for_session(symbol, forecast_date)
    except Exception as exc:
        logger.warning(
            "forecast_receipt: get_underlying_bars_for_session failed for %s %s (%s)",
            symbol, forecast_date, exc,
        )
        bars = []

    if bars:
        lows = [float(b["low"]) for b in bars if b.get("low") is not None]
        highs = [float(b["high"]) for b in bars if b.get("high") is not None]
        if lows and highs:
            return min(lows), max(highs), actual_close

    # Fallback: if intraday bars are missing, use the close as a degenerate
    # low/high. The verdict will be that the (point) "range" is satisfied
    # iff the close fell inside the projected band — degraded but honest.
    return actual_close, actual_close, actual_close


async def _run(args: argparse.Namespace) -> int:
    day = date.fromisoformat(args.date) if args.date else _today_et()
    if not _is_trading_day(day) and not args.allow_non_trading_day:
        logger.info(
            "forecast_receipt: skipping %s — not a trading day", day.isoformat(),
        )
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:
        logger.warning("forecast_receipt: DB connect failed (%s) — exiting 0", exc)
        return 0

    try:
        symbols = [s.strip().upper() for s in args.symbol.split(",") if s.strip()]
        for sym in symbols:
            ohlc = await _fetch_session_ohlc(db, sym, day)
            if ohlc is None:
                logger.info(
                    "forecast_receipt: no session OHLC for %s %s — skipping",
                    sym, day.isoformat(),
                )
                continue
            low, high, close = ohlc
            now_et = datetime.now(tz=ET)
            if args.dry_run:
                logger.info(
                    "forecast_receipt: DRY RUN %s %s — actual L/H/C = %s / %s / %s",
                    sym, day.isoformat(), low, high, close,
                )
                continue
            row = await db.update_daily_forecast_receipt(
                symbol=sym,
                forecast_date=day,
                receipt_ts=now_et,
                actual_low=low,
                actual_high=high,
                actual_close=close,
            )
            if row is None:
                logger.info(
                    "forecast_receipt: no morning row for %s %s — nothing to grade",
                    sym, day.isoformat(),
                )
                continue
            logger.info(
                "forecast_receipt: wrote receipt for %s %s — range_respected=%s pin_hit=%s regime_correct=%s",
                sym, day.isoformat(),
                row.get("range_respected"),
                row.get("pin_hit"),
                row.get("regime_correct"),
            )
        return 0
    finally:
        try:
            await db.disconnect()
        except Exception:
            pass


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbol",
        default=os.environ.get("FORECAST_SYMBOLS", "SPY"),
        help="Comma-separated symbols to grade (default SPY).",
    )
    parser.add_argument("--date", help="Grade a specific date (YYYY-MM-DD).")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute + log the receipt but do NOT write to the DB.",
    )
    parser.add_argument(
        "--allow-non-trading-day",
        action="store_true",
        help="Override the weekend/holiday skip.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
