"""Morning forecast writer — fires at 07:00 ET on weekdays.

Pulls live GEX + MSI + Playbook state from the in-process DatabaseManager,
computes today's projected range / pin / regime / flagship setup via the
v1 heuristic, and writes one immutable row to ``daily_forecast``.

Re-running the job for an already-committed day is a no-op: the row's
(symbol, date) primary key plus the immutability trigger guarantee that
the public commitment cannot be retroactively edited. The writer logs
"already committed" and exits 0.

This job never raises. Every failure path logs WARNING + exits 0 so a
single bad day doesn't break tomorrow's run.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
from datetime import date, datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.jobs.forecast_range_model import (
    ForecastInputs,
    ForecastResult,
    compute_forecast,
)
from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger("zerogex.forecast_writer")
ET = ZoneInfo("America/New_York")


def _today_et() -> date:
    return datetime.now(tz=ET).date()


def _is_trading_day(day: date) -> bool:
    if day.weekday() >= 5:
        return False
    if day in NYSE_HOLIDAYS:
        return False
    return True


def _is_event_day(day: date) -> bool:
    """Optional FOMC/CPI/NFP override via the EVENT_DAYS env var
    (comma-separated YYYY-MM-DD list). Empty/unset = no event days."""
    raw = os.environ.get("EVENT_DAYS", "").strip()
    if not raw:
        return False
    try:
        days = {date.fromisoformat(tok.strip()) for tok in raw.split(",") if tok.strip()}
    except ValueError:
        logger.warning("forecast_writer: malformed EVENT_DAYS=%r — ignoring", raw)
        return False
    return day in days


def _content_hash(payload: dict[str, Any]) -> str:
    """Tamper-evidence hash for the committed payload. The canonical JSON
    is sorted-keys + ISO-formatted timestamps so the hash is byte-stable
    across re-runs."""
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def _gather_inputs(db: DatabaseManager, symbol: str) -> Optional[ForecastInputs]:
    """Pull every input the model needs from the in-process DB layer.

    Falls back gracefully — missing GEX walls or MSI just narrows the
    forecast quality, not the writer's ability to commit. Spot is the
    only hard requirement; without it we can't anchor the band.
    """
    try:
        gex = await db.get_latest_gex_summary(symbol)
    except Exception as exc:
        logger.warning("forecast_writer: get_latest_gex_summary failed (%s): %s", symbol, exc)
        gex = None

    try:
        quote = await db.get_latest_quote(symbol)
    except Exception as exc:
        logger.warning("forecast_writer: get_latest_quote failed (%s): %s", symbol, exc)
        quote = None

    try:
        score = await db.get_latest_signal_score(symbol)
    except Exception as exc:
        logger.warning("forecast_writer: get_latest_signal_score failed (%s): %s", symbol, exc)
        score = None

    # Flagship setup is best-effort: the live /action endpoint computes
    # the Playbook Card on demand. From a cron context we read whatever
    # the most recent persisted card looks like (the engine writes one
    # per cycle). If nothing fresh exists today, we leave it None — the
    # OG card just skips the flagship section.
    flagship = None
    try:
        recent = await db.get_action_cards_chronological(
            underlying=symbol, limit=1, since_hours=2,
        )
        if recent:
            full = await db.get_action_card_by_id(recent[0]["id"])
            if full and str(full.get("action") or "").upper() != "STAND_DOWN":
                flagship = full
    except Exception as exc:
        logger.warning("forecast_writer: flagship setup fetch failed (%s): %s", symbol, exc)

    spot = None
    if quote and quote.get("last") is not None:
        spot = float(quote["last"])
    elif gex and gex.get("spot_price") is not None:
        spot = float(gex["spot_price"])
    if spot is None:
        logger.warning("forecast_writer: no spot for %s — cannot forecast", symbol)
        return None

    today = _today_et()

    def _f(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return ForecastInputs(
        symbol=symbol,
        forecast_date=today,
        spot=spot,
        call_wall=_f(gex.get("call_wall")) if gex else None,
        put_wall=_f(gex.get("put_wall")) if gex else None,
        gamma_flip=_f(gex.get("gamma_flip")) if gex else None,
        max_pain=_f(gex.get("max_pain")) if gex else None,
        msi_composite=_f(score.get("composite_score")) if score else None,
        msi_normalized=_f(score.get("normalized_score")) if score else None,
        flagship_setup=flagship,
        is_event_day=_is_event_day(today),
    )


def _build_payload(inputs: ForecastInputs, result: ForecastResult, open_ts: datetime) -> dict[str, Any]:
    """Translate (inputs, result) → daily_forecast row payload + hash."""
    base = {
        "symbol": inputs.symbol,
        "date": inputs.forecast_date,
        "open_ts": open_ts,
        "open_spot": inputs.spot,
        "call_wall": inputs.call_wall,
        "put_wall": inputs.put_wall,
        "gamma_flip": inputs.gamma_flip,
        "open_msi": inputs.msi_normalized,
        "regime": result.regime,
        "projected_low": result.projected_low,
        "projected_high": result.projected_high,
        "projected_close": result.projected_close,
        "pin_strike": result.pin_strike,
        "flagship_setup": inputs.flagship_setup,
        "range_model": result.range_model,
    }
    # The content hash deliberately excludes open_ts (which is recorded
    # at the moment of write and would otherwise make every dry-run
    # produce a different hash) — only the committed claims are hashed.
    hashable = {k: v for k, v in base.items() if k not in ("open_ts",)}
    base["content_hash"] = _content_hash(hashable)
    return base


async def _run(args: argparse.Namespace) -> int:
    day = date.fromisoformat(args.date) if args.date else _today_et()
    if not _is_trading_day(day) and not args.allow_non_trading_day:
        logger.info("forecast_writer: skipping %s — not a trading day", day.isoformat())
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:
        logger.warning("forecast_writer: DB connect failed (%s) — exiting 0", exc)
        return 0

    try:
        symbols = [s.strip().upper() for s in args.symbol.split(",") if s.strip()]
        for sym in symbols:
            inputs = await _gather_inputs(db, sym)
            if inputs is None:
                continue
            # Override forecast_date when --date was supplied (backfill).
            if args.date:
                inputs.forecast_date = day
            result = compute_forecast(inputs)
            open_ts = datetime.now(tz=ET)
            payload = _build_payload(inputs, result, open_ts)
            if args.dry_run:
                logger.info(
                    "forecast_writer: DRY RUN %s %s — projected [%s, %s] pin=%s regime=%s hash=%s rationale=%s",
                    sym, day.isoformat(),
                    f"${payload['projected_low']:.2f}",
                    f"${payload['projected_high']:.2f}",
                    f"${payload['pin_strike']:.2f}" if payload['pin_strike'] is not None else "—",
                    payload["regime"],
                    payload["content_hash"][:12],
                    " · ".join(result.rationale),
                )
                continue
            try:
                row = await db.insert_daily_forecast_morning(payload)
            except Exception as exc:
                logger.warning(
                    "forecast_writer: insert failed for %s %s (%s)",
                    sym, day.isoformat(), exc,
                )
                continue
            if row is None:
                logger.warning(
                    "forecast_writer: insert returned None for %s %s", sym, day.isoformat(),
                )
                continue
            already_committed = (
                row.get("content_hash") and row["content_hash"] != payload["content_hash"]
            )
            if already_committed:
                logger.info(
                    "forecast_writer: %s %s already committed with hash %s — leaving as-is",
                    sym, day.isoformat(), row["content_hash"][:12],
                )
            else:
                logger.info(
                    "forecast_writer: committed %s %s — projected [%s, %s] pin=%s regime=%s hash=%s",
                    sym, day.isoformat(),
                    f"${payload['projected_low']:.2f}",
                    f"${payload['projected_high']:.2f}",
                    f"${payload['pin_strike']:.2f}" if payload['pin_strike'] is not None else "—",
                    payload["regime"],
                    payload["content_hash"][:12],
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
        help="Comma-separated symbols to forecast (default SPY).",
    )
    parser.add_argument("--date", help="Backfill a specific date (YYYY-MM-DD).")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute + log the forecast but do NOT write to the DB.",
    )
    parser.add_argument(
        "--allow-non-trading-day",
        action="store_true",
        help="Override the weekend/holiday skip — useful for backfill testing.",
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
