"""Session-levels capture — pre-market + previous-session high/low.

Captures, per non-index symbol (ETFs/equities such as SPY, QQQ):

* **Pre-market high/low** — the 04:00-09:30 ET extended-hours session of
  the current trading date.  Sources: the TradeStation bars API
  (``sessiontemplate=USEQPre``, works regardless of the live stream's
  ``SESSION_TEMPLATE``) unioned with whatever 1-min pre-market bars the
  ingestion engine already wrote to ``underlying_quotes``.
* **Previous-session high/low** — the prior trading day's regular session
  (09:30-16:00 ET; 09:30-13:00 on NYSE half-days) including the closing
  auction print (the close-time bar's ``open`` — the same asset-aware
  cash-close rule ``get_session_closes`` applies for non-INDEX symbols).

Rows land in ``session_levels`` keyed (symbol, trading_date) with
never-clobber merge semantics, so the systemd timer can fire every few
minutes through the pre-market window: each tick ratchets the pre-market
high/low outward, and a post-open tick finalizes the session.  Cash
indexes (SPX, NDX, …) are skipped — they have no pre-market print.

Like the other jobs in this package, this module never raises: missing
credentials, API failures, DB failures — all log and exit 0 so a bad run
can't wedge the timer.  The TradeStation client is optional; without
credentials the job degrades to the ``underlying_quotes`` aggregate.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.market_calendar import NYSE_HOLIDAYS
from src.symbols import get_canonical_symbol, is_cash_index, resolve_symbol

logger = logging.getLogger("zerogex.session_levels")
ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

PREMARKET_START = time(4, 0)
PREMARKET_END = time(9, 30)
RTH_START = time(9, 30)
RTH_END = time(16, 0)
HALF_DAY_END = time(13, 0)


def _today_et() -> date:
    return datetime.now(tz=ET).date()


def _is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in NYSE_HOLIDAYS


def _prev_trading_day(day: date) -> date:
    prev = day - timedelta(days=1)
    while not _is_trading_day(prev):
        prev -= timedelta(days=1)
    return prev


def _nyse_half_days() -> set[date]:
    """NYSE early-close (13:00 ET) dates from the ``NYSE_HALF_DAYS`` env var."""
    out: set[date] = set()
    for token in os.getenv("NYSE_HALF_DAYS", "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            out.add(date.fromisoformat(token))
        except ValueError:
            logger.warning("Invalid date in NYSE_HALF_DAYS: %r", token)
    return out


def _default_symbols() -> str:
    """Symbol universe: SESSION_LEVELS_SYMBOLS, else the ingestion universe."""
    explicit = os.environ.get("SESSION_LEVELS_SYMBOLS", "").strip()
    if explicit:
        return explicit
    plural = os.environ.get("INGEST_UNDERLYINGS", "").strip()
    if plural:
        return plural
    return os.environ.get("INGEST_UNDERLYING", "SPY").strip() or "SPY"


def _utc_iso(dt: datetime) -> str:
    """Format an aware datetime the way the TradeStation bars API expects."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_bar_ts(raw: Any) -> Optional[datetime]:
    """Parse a TradeStation bar TimeStamp (e.g. ``2026-07-06T08:04:00Z``)."""
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _to_float(raw: Any) -> Optional[float]:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _build_ts_client():  # pragma: no cover - thin construction wrapper
    """Build an authenticated TradeStation client, or None without creds."""
    client_id = os.getenv("TRADESTATION_CLIENT_ID", "")
    client_secret = os.getenv("TRADESTATION_CLIENT_SECRET", "")
    refresh_token = os.getenv("TRADESTATION_REFRESH_TOKEN", "")
    if not (client_id and client_secret and refresh_token):
        logger.info(
            "session_levels: TradeStation credentials not configured — "
            "capturing from underlying_quotes only"
        )
        return None
    try:
        from src.config import _getenv_bool
        from src.ingestion.tradestation_client import TradeStationClient

        client = TradeStationClient(
            client_id,
            client_secret,
            refresh_token,
            sandbox=_getenv_bool("TRADESTATION_USE_SANDBOX", False),
        )
        # Contribute this job's API usage to the shared cross-process
        # rate-limit accounting, same as the volatility-index ingesters.
        try:
            from src.ingestion.api_call_tracker import attach_db_writer

            attach_db_writer(client)
        except Exception as exc:
            logger.warning("session_levels: API-call tracker attach failed: %s", exc)
        return client
    except Exception as exc:
        logger.warning(
            "session_levels: TradeStation client init failed (%s) — "
            "capturing from underlying_quotes only",
            exc,
        )
        return None


def _bars_window_extremes(
    bars: List[Dict[str, Any]],
    day: date,
    start_t: time,
    end_t: time,
) -> Tuple[Optional[float], Optional[float]]:
    """(high, low) across bars whose ET timestamp falls in [start_t, end_t) of ``day``."""
    highs: List[float] = []
    lows: List[float] = []
    for bar in bars:
        ts = _parse_bar_ts(bar.get("TimeStamp"))
        if ts is None:
            continue
        ts_et = ts.astimezone(ET)
        if ts_et.date() != day or not (start_t <= ts_et.time() < end_t):
            continue
        high = _to_float(bar.get("High"))
        low = _to_float(bar.get("Low"))
        if high is not None:
            highs.append(high)
        if low is not None:
            lows.append(low)
    return (max(highs) if highs else None, min(lows) if lows else None)


def _fetch_premarket_from_ts(
    client: Any, ts_symbol: str, day: date, now_et: datetime
) -> Tuple[Optional[float], Optional[float]]:
    """Pre-market high/low for ``day`` from the TradeStation bars API."""
    first = datetime.combine(day, PREMARKET_START, tzinfo=ET)
    last = min(now_et, datetime.combine(day, PREMARKET_END, tzinfo=ET))
    if last <= first:
        return None, None
    try:
        resp = client.get_bars(
            ts_symbol,
            1,
            "Minute",
            firstdate=_utc_iso(first),
            lastdate=_utc_iso(last),
            sessiontemplate="USEQPre",
            warn_if_closed=False,
        )
    except Exception as exc:
        logger.warning("session_levels: pre-market bars fetch failed for %s (%s)", ts_symbol, exc)
        return None, None
    return _bars_window_extremes(resp.get("Bars") or [], day, PREMARKET_START, PREMARKET_END)


def _fetch_prev_session_from_ts(
    client: Any, ts_symbol: str, prev_day: date
) -> Tuple[Optional[float], Optional[float]]:
    """Previous-session high/low from TradeStation 1-min RTH bars.

    Minute bars are used instead of a Daily bar because their start-of-
    minute timestamps carry an unambiguous instant (Daily-bar date
    stamping conventions vary).  The high/low spans [09:30, close) plus
    the close-time bar's ``open`` — the closing-auction print — mirroring
    the DB aggregate so both sources agree on half-days and on the
    extended-hours contamination of the close-time bar.
    """
    close_t = HALF_DAY_END if prev_day in _nyse_half_days() else RTH_END
    first = datetime.combine(prev_day, RTH_START, tzinfo=ET)
    last = datetime.combine(prev_day, close_t, tzinfo=ET)
    try:
        resp = client.get_bars(
            ts_symbol,
            1,
            "Minute",
            firstdate=_utc_iso(first),
            lastdate=_utc_iso(last),
            sessiontemplate="Default",
            warn_if_closed=False,
        )
    except Exception as exc:
        logger.warning(
            "session_levels: previous-session bars fetch failed for %s (%s)",
            ts_symbol,
            exc,
        )
        return None, None

    bars = resp.get("Bars") or []
    high, low = _bars_window_extremes(bars, prev_day, RTH_START, close_t)

    # Closing-auction print: the close-time bar's open participates in the
    # high/low (see get_session_closes for the asset-aware close rationale).
    for bar in bars:
        ts = _parse_bar_ts(bar.get("TimeStamp"))
        if ts is None:
            continue
        ts_et = ts.astimezone(ET)
        if ts_et.date() == prev_day and ts_et.time() == close_t:
            auction = _to_float(bar.get("Open"))
            if auction is not None:
                high = max(high, auction) if high is not None else auction
                low = min(low, auction) if low is not None else auction
            break
    return high, low


def _union(
    a: Tuple[Optional[float], Optional[float]],
    b: Tuple[Optional[float], Optional[float]],
) -> Tuple[Optional[float], Optional[float]]:
    """Union two (high, low) pairs: max of highs, min of lows, None-safe."""
    highs = [v for v in (a[0], b[0]) if v is not None]
    lows = [v for v in (a[1], b[1]) if v is not None]
    return (max(highs) if highs else None, min(lows) if lows else None)


def _source_label(
    ts_pair: Tuple[Optional[float], Optional[float]],
    db_pair: Tuple[Optional[float], Optional[float]],
) -> Optional[str]:
    has_ts = any(v is not None for v in ts_pair)
    has_db = any(v is not None for v in db_pair)
    if has_ts and has_db:
        return "tradestation+underlying_quotes"
    if has_ts:
        return "tradestation"
    if has_db:
        return "underlying_quotes"
    return None


async def _capture_symbol(
    db: DatabaseManager,
    client: Any,
    ts_symbol: str,
    db_symbol: str,
    day: date,
    now_et: datetime,
) -> Dict[str, Any]:
    """Compute the full session-levels payload for one symbol (no writes)."""
    live = await db.compute_live_session_levels(db_symbol, day)
    db_pm = (
        _to_float(live.get("premarket_high")),
        _to_float(live.get("premarket_low")),
    )
    db_prev = (
        _to_float(live.get("prev_session_high")),
        _to_float(live.get("prev_session_low")),
    )
    db_prev_date = live.get("prev_session_date")

    ts_pm: Tuple[Optional[float], Optional[float]] = (None, None)
    ts_prev: Tuple[Optional[float], Optional[float]] = (None, None)
    calendar_prev = _prev_trading_day(day)
    if client is not None:
        ts_pm = await asyncio.to_thread(_fetch_premarket_from_ts, client, ts_symbol, day, now_et)
        ts_prev = await asyncio.to_thread(
            _fetch_prev_session_from_ts, client, ts_symbol, calendar_prev
        )

    pm_high, pm_low = _union(ts_pm, db_pm)

    # Previous session: the TradeStation fetch targets the true calendar
    # previous trading day and wins when it has data; the DB aggregate
    # (which walks back to the latest date with RTH bars) is the fallback.
    if any(v is not None for v in ts_prev):
        prev_high, prev_low = ts_prev
        prev_date: Optional[date] = calendar_prev
        prev_source: Optional[str] = "tradestation"
    else:
        prev_high, prev_low = db_prev
        prev_date = db_prev_date
        prev_source = "underlying_quotes" if any(v is not None for v in db_prev) else None

    return {
        "symbol": db_symbol,
        "trading_date": day,
        "premarket_high": pm_high,
        "premarket_low": pm_low,
        "premarket_source": _source_label(ts_pm, db_pm),
        "prev_session_date": prev_date if prev_high is not None or prev_low is not None else None,
        "prev_session_high": prev_high,
        "prev_session_low": prev_low,
        "prev_session_source": prev_source,
    }


async def _run(args: argparse.Namespace) -> int:
    day = date.fromisoformat(args.date) if args.date else _today_et()
    if not _is_trading_day(day) and not args.allow_non_trading_day:
        logger.info("session_levels: skipping %s — not a trading day", day.isoformat())
        return 0

    tokens = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not tokens:
        logger.info("session_levels: no symbols configured — nothing to do")
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:
        logger.warning("session_levels: DB connect failed (%s) — exiting 0", exc)
        return 0

    client = None if args.no_tradestation else _build_ts_client()
    now_et = datetime.now(tz=ET)

    try:
        seen: set[str] = set()
        for token in tokens:
            ts_symbol = resolve_symbol(token)
            db_symbol = get_canonical_symbol(ts_symbol)
            if db_symbol in seen:
                continue
            seen.add(db_symbol)

            # Non-indexes only: a cash index has no pre-market print and
            # the chart overlays are scoped to ETFs/equities.
            if ts_symbol.startswith("$") or is_cash_index(db_symbol):
                logger.info("session_levels: skipping %s — cash index", db_symbol)
                continue

            try:
                payload = await _capture_symbol(db, client, ts_symbol, db_symbol, day, now_et)
            except Exception as exc:
                logger.warning("session_levels: capture failed for %s (%s)", db_symbol, exc)
                continue

            has_any = any(
                payload[k] is not None
                for k in (
                    "premarket_high",
                    "premarket_low",
                    "prev_session_high",
                    "prev_session_low",
                )
            )
            if not has_any:
                logger.info(
                    "session_levels: no data for %s %s — nothing to write",
                    db_symbol,
                    day.isoformat(),
                )
                continue

            if args.dry_run:
                logger.info(
                    "session_levels: DRY RUN %s %s — PM H/L = %s / %s (%s), "
                    "prev %s H/L = %s / %s (%s)",
                    db_symbol,
                    day.isoformat(),
                    payload["premarket_high"],
                    payload["premarket_low"],
                    payload["premarket_source"],
                    payload["prev_session_date"],
                    payload["prev_session_high"],
                    payload["prev_session_low"],
                    payload["prev_session_source"],
                )
                continue

            try:
                await db.ensure_symbol_row(
                    db_symbol,
                    "ETF" if db_symbol in {"SPY", "QQQ", "IWM", "DIA"} else "EQUITY",
                )
                await db.upsert_session_levels(**payload)
            except Exception as exc:
                logger.warning("session_levels: upsert failed for %s (%s)", db_symbol, exc)
                continue

            logger.info(
                "session_levels: wrote %s %s — PM H/L = %s / %s (%s), "
                "prev %s H/L = %s / %s (%s)",
                db_symbol,
                day.isoformat(),
                payload["premarket_high"],
                payload["premarket_low"],
                payload["premarket_source"],
                payload["prev_session_date"],
                payload["prev_session_high"],
                payload["prev_session_low"],
                payload["prev_session_source"],
            )
        return 0
    finally:
        try:
            await db.disconnect()
        except Exception:
            pass
        if client is not None:
            try:
                client.close_all_streams()
            except Exception:
                pass


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbols",
        default=_default_symbols(),
        help=(
            "Comma-separated symbols to capture (default: SESSION_LEVELS_SYMBOLS "
            "env, else the INGEST_UNDERLYINGS/INGEST_UNDERLYING ingestion "
            "universe, else SPY). Cash indexes are skipped."
        ),
    )
    parser.add_argument("--date", help="Capture a specific ET trading date (YYYY-MM-DD).")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute + log the levels but do NOT write to the DB.",
    )
    parser.add_argument(
        "--allow-non-trading-day",
        action="store_true",
        help="Override the weekend/holiday skip.",
    )
    parser.add_argument(
        "--no-tradestation",
        action="store_true",
        help="Skip the TradeStation bars API and use underlying_quotes only.",
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
