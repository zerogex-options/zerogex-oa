"""Daily Gamma Forecast Card + 4 PM Receipt — Phase 3.

A single forecast row per (symbol, date) committed publicly at 07:00 ET
and verified at 16:05 ET by the matching receipt cron. This router only
READS — the writer/receipt cron jobs live in ``src.jobs.forecast_writer``
and ``src.jobs.forecast_receipt``. The website's /forecast/{date} page
is the canonical consumer.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from zoneinfo import ZoneInfo

from ..database import DatabaseManager
from .trade_signals import get_db

router = APIRouter(prefix="/api/forecast", tags=["Forecast"])

ET = ZoneInfo("America/New_York")


def _parse_date(raw: str | None) -> date:
    if raw is None:
        return datetime.now(tz=ET).date()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"Invalid date '{raw}'; expected YYYY-MM-DD."
        ) from exc
    today = datetime.now(tz=ET).date()
    if parsed < date(2024, 1, 1) or parsed > today + timedelta(days=1):
        raise HTTPException(
            status_code=422, detail=f"Date '{raw}' is outside the supported range."
        )
    return parsed


def _shape_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Normalize a daily_forecast row for JSON serialization.

    Decimals → floats, dates → ISO strings, and the morning/receipt
    sections are surfaced as explicit ``morning`` / ``receipt`` sub-dicts
    so the frontend can branch cleanly on which state to render.
    """
    if row is None:
        return None

    def _f(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    has_receipt = row.get("receipt_ts") is not None
    return {
        "symbol": row["symbol"],
        "date": row["date"].isoformat() if isinstance(row["date"], date) else row["date"],
        "morning": {
            "ts": row["open_ts"].isoformat() if row.get("open_ts") else None,
            "open_spot": _f(row.get("open_spot")),
            "call_wall": _f(row.get("call_wall")),
            "put_wall": _f(row.get("put_wall")),
            "gamma_flip": _f(row.get("gamma_flip")),
            "open_msi": _f(row.get("open_msi")),
            "regime": row.get("regime"),
            "projected_low": _f(row.get("projected_low")),
            "projected_high": _f(row.get("projected_high")),
            "projected_close": _f(row.get("projected_close")),
            "pin_strike": _f(row.get("pin_strike")),
            "flagship_setup": row.get("flagship_setup"),
            "range_model": row.get("range_model"),
            "content_hash": row.get("content_hash"),
        },
        "receipt": (
            {
                "ts": row["receipt_ts"].isoformat() if row.get("receipt_ts") else None,
                "actual_low": _f(row.get("actual_low")),
                "actual_high": _f(row.get("actual_high")),
                "actual_close": _f(row.get("actual_close")),
                "range_respected": row.get("range_respected"),
                "pin_hit": row.get("pin_hit"),
                "regime_correct": row.get("regime_correct"),
                "setup_outcome": row.get("setup_outcome"),
            }
            if has_receipt
            else None
        ),
    }


@router.get("/{forecast_date}")
async def get_forecast_for_date(
    forecast_date: str,
    symbol: str = Query(default="SPY", max_length=10),
    db: DatabaseManager = Depends(get_db),
):
    """One symbol/date forecast — morning snapshot + (when written) receipt.

    Returns 404 when no row exists for the date (writer hasn't run yet, or
    the date pre-dates the feature launch). Receipt fields are ``null`` when
    the morning row exists but the 16:05 ET receipt cron hasn't fired yet.
    """
    parsed = _parse_date(forecast_date)
    row = await db.get_daily_forecast(symbol.upper(), parsed)
    shaped = _shape_row(row)
    if shaped is None:
        raise HTTPException(
            status_code=404,
            detail=f"No forecast for {symbol.upper()} on {parsed.isoformat()}.",
        )
    return shaped


@router.get("")
async def get_latest_forecast(
    symbol: str = Query(default="SPY", max_length=10),
    db: DatabaseManager = Depends(get_db),
):
    """Latest committed forecast for the symbol (today, or the most recent
    prior trading day if today's writer hasn't fired yet)."""
    rows = await db.get_daily_forecast_history(symbol.upper(), limit=1)
    if not rows:
        raise HTTPException(
            status_code=404, detail=f"No forecasts persisted for {symbol.upper()} yet."
        )
    # The history query returns a thin column set — re-read the full row
    # so flagship_setup / morning sub-dict are populated.
    latest = await db.get_daily_forecast(symbol.upper(), rows[0]["date"])
    return _shape_row(latest)


@router.get("/history/recent")
async def get_recent_history(
    symbol: str = Query(default="SPY", max_length=10),
    limit: int = Query(default=30, ge=1, le=180),
    db: DatabaseManager = Depends(get_db),
):
    """Compact history feed — one row per recent forecast with just the
    verdict columns. Powers the rolling 30-day hit-rate strip."""
    rows = await db.get_daily_forecast_history(symbol.upper(), limit=limit)

    def _f(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return {
        "symbol": symbol.upper(),
        "count": len(rows),
        "rows": [
            {
                "date": r["date"].isoformat() if isinstance(r["date"], date) else r["date"],
                "regime": r["regime"],
                "open_spot": _f(r.get("open_spot")),
                "projected_low": _f(r.get("projected_low")),
                "projected_high": _f(r.get("projected_high")),
                "actual_close": _f(r.get("actual_close")),
                "pin_strike": _f(r.get("pin_strike")),
                "range_respected": r.get("range_respected"),
                "pin_hit": r.get("pin_hit"),
                "regime_correct": r.get("regime_correct"),
                "has_receipt": r.get("receipt_ts") is not None,
            }
            for r in rows
        ],
    }


@router.get("/stats/rolling")
async def get_rolling_stats(
    symbol: str = Query(default="SPY", max_length=10),
    window: int = Query(default=30, ge=5, le=180),
    db: DatabaseManager = Depends(get_db),
):
    """Rolling N-day hit rates by claim type — feeds the OG card footer
    strip and the page's transparency panel."""
    rows = await db.get_daily_forecast_history(symbol.upper(), limit=window)
    scored = [r for r in rows if r.get("receipt_ts") is not None]
    n = len(scored)

    def _rate(predicate) -> float | None:
        if not scored:
            return None
        eligible = [r for r in scored if predicate(r) is not None]
        if not eligible:
            return None
        wins = sum(1 for r in eligible if predicate(r))
        return round(wins / len(eligible), 4)

    return {
        "symbol": symbol.upper(),
        "window": window,
        "n_scored": n,
        "range_respected_rate": _rate(lambda r: r.get("range_respected")),
        "pin_hit_rate": _rate(lambda r: r.get("pin_hit")),
        "regime_correct_rate": _rate(lambda r: r.get("regime_correct")),
    }
