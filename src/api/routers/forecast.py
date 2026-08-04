"""Daily Gamma Forecast Card + 4 PM Receipt — Phase 3.

A single forecast row per (symbol, date) committed publicly at 07:00 ET
and verified at 16:05 ET by the matching receipt cron. This router only
READS — the writer/receipt cron jobs live in ``src.jobs.forecast_writer``
and ``src.jobs.forecast_receipt``. The website's /forecast/{date} page
is the canonical consumer.
"""

from __future__ import annotations

import math
import os
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from zoneinfo import ZoneInfo

from src.jobs.forecast_range_model import _classify_vol_state

from ..database import DatabaseManager
from .trade_signals import get_db

router = APIRouter(prefix="/api/forecast", tags=["Forecast"])

ET = ZoneInfo("America/New_York")

# The dealer-gamma regime fix went live on this date.  Before it, a units bug
# pinned every committed ``regime`` to ``long_gamma`` (see
# ``forecast_range_model._classify_regime``), so grading ``regime_correct``
# against those labels measured only the base rate of chop days.  The rolling
# ``regime_correct_rate`` below excludes pre-fix rows so it isn't misleading;
# since the window is rolling, this cutoff becomes a no-op once ~30 days of
# corrected receipts accrue.  Override with ``FORECAST_REGIME_FIX_DATE``.
_DEFAULT_REGIME_FIX_DATE = date(2026, 7, 9)


def _regime_fix_date() -> date:
    raw = os.environ.get("FORECAST_REGIME_FIX_DATE", "").strip()
    if not raw:
        return _DEFAULT_REGIME_FIX_DATE
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return _DEFAULT_REGIME_FIX_DATE


# The vol-grade scale fix went live on this date.  Before it, realized_vol_ratio
# was (high-low)/implied graded on buckets centered at 1.0, so an ordinary day
# (range ≈ 1.6× the 1-σ implied move) was mislabeled "expansion" and
# vol_state_correct measured the scale bug, not the call.  The rolling vol
# accuracy + its baseline exclude pre-fix rows for the same reason the regime
# rate excludes the units-bug era; the cutoff self-expires once the window is
# all corrected rows.  Override with FORECAST_VOL_SCALE_FIX_DATE.
_DEFAULT_VOL_SCALE_FIX_DATE = date(2026, 8, 4)

# What fraction of days a well-calibrated containment band should cover — the
# reference line the range track record is judged against (the writer's
# calibration loop targets ~0.90; 0.80 is the floor of the published band goal).
_RANGE_COVERAGE_BASELINE = 0.80


def _vol_scale_fix_date() -> date:
    raw = os.environ.get("FORECAST_VOL_SCALE_FIX_DATE", "").strip()
    if not raw:
        return _DEFAULT_VOL_SCALE_FIX_DATE
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return _DEFAULT_VOL_SCALE_FIX_DATE


def _wilson_ci(wins: int, n: int, z: float = 1.96) -> list[float] | None:
    """95% Wilson score interval for a binomial proportion.

    Small-n honest: with n=6 a 5/6 hit rate reads 0.42–0.99, not "83% ± nothing".
    Returns ``[low, high]`` clamped to [0, 1], or None when there's no data."""
    if n <= 0:
        return None
    p = wins / n
    denom = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / denom
    return [round(max(0.0, center - margin), 4), round(min(1.0, center + margin), 4)]


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


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
            "pin_tolerance": _f(row.get("pin_tolerance")),
            "regime_move_threshold": _f(row.get("regime_move_threshold")),
            "flagship_setup": row.get("flagship_setup"),
            "range_model": row.get("range_model"),
            "content_hash": row.get("content_hash"),
            # v1.4 gradeable claims — the tiles that replace pin/regime.
            "expected_vol_state": row.get("expected_vol_state"),
            "expected_vol_ratio": _f(row.get("expected_vol_ratio")),
            "implied_move": _f(row.get("implied_move")),
            "flip_cross_prob": _f(row.get("flip_cross_prob")),
            "level_touch_probs": row.get("level_touch_probs"),
            "gravity_center": _f(row.get("gravity_center")),
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
                # v1.4 verdicts.
                "realized_vol_ratio": _f(row.get("realized_vol_ratio")),
                "vol_state_correct": row.get("vol_state_correct"),
                "flip_crossed": row.get("flip_crossed"),
                "level_touch_outcomes": row.get("level_touch_outcomes"),
                "levels_brier": _f(row.get("levels_brier")),
            }
            if has_receipt
            else None
        ),
    }


@router.get("/available-dates")
async def get_available_dates(
    symbol: str = Query(default="SPY", max_length=10),
    limit: int = Query(default=60, ge=1, le=365),
    db: DatabaseManager = Depends(get_db),
):
    """Distinct forecast dates for the /forecast landing page's date picker.

    Mirrors the /api/replay/sessions contract: newest-first list with per-date
    metadata (regime, verdict pills) so cards can render without a second
    fetch. Returns ``{symbol, count, dates: []}`` — empty ``dates`` when the
    writer has never fired for that symbol.
    """
    rows = await db.get_forecast_available_dates(symbol.upper(), limit=limit)
    return {
        "symbol": symbol.upper(),
        "count": len(rows),
        "dates": [
            {
                "date": r["date"].isoformat() if isinstance(r["date"], date) else r["date"],
                "regime": r.get("regime"),
                "has_receipt": bool(r.get("has_receipt")),
                "range_respected": r.get("range_respected"),
                "pin_hit": r.get("pin_hit"),
                "regime_correct": r.get("regime_correct"),
                "expected_vol_state": r.get("expected_vol_state"),
                "vol_state_correct": r.get("vol_state_correct"),
            }
            for r in rows
        ],
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
                "expected_vol_state": r.get("expected_vol_state"),
                "expected_vol_ratio": _f(r.get("expected_vol_ratio")),
                "realized_vol_ratio": _f(r.get("realized_vol_ratio")),
                "vol_state_correct": r.get("vol_state_correct"),
                "levels_brier": _f(r.get("levels_brier")),
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

    def _rate(row_set, key) -> float | None:
        eligible = [r for r in row_set if r.get(key) is not None]
        if not eligible:
            return None
        wins = sum(1 for r in eligible if r.get(key))
        return round(wins / len(eligible), 4)

    def _rate_ci(row_set, key):
        """(rate, wilson_ci, n_eligible) for a boolean verdict column."""
        eligible = [r for r in row_set if r.get(key) is not None]
        m = len(eligible)
        if m == 0:
            return None, None, 0
        wins = sum(1 for r in eligible if r.get(key))
        return round(wins / m, 4), _wilson_ci(wins, m), m

    # Regime accuracy only counts forecasts made under the corrected
    # dealer-gamma logic; pre-fix rows are the all-``long_gamma`` units bug and
    # would just measure the chop-day base rate (see _regime_fix_date).
    fix_date = _regime_fix_date()
    regime_scored = [
        r for r in scored
        if (d := _as_date(r.get("date"))) is not None and d >= fix_date
    ]
    regime_n = sum(1 for r in regime_scored if r.get("regime_correct") is not None)

    # Range coverage — a scale-free price-vs-price claim, so every scored row
    # counts.  Judged against the published coverage target, with a Wilson CI so
    # a short window can't masquerade as precision.
    range_rate, range_ci, _range_n = _rate_ci(scored, "range_respected")

    # Vol call — gated to the corrected-scale era (see _vol_scale_fix_date) so a
    # pre-fix mislabeled receipt can't drag the number either way.  The baseline
    # is the honest strawman: "always guess the most common realized bucket."  A
    # skilled call has to beat that majority-class base rate, not just 50%.
    vol_fix_date = _vol_scale_fix_date()
    vol_scored = [
        r for r in scored
        if r.get("vol_state_correct") is not None
        and (d := _as_date(r.get("date"))) is not None and d >= vol_fix_date
    ]
    vol_rate, vol_ci, vol_n = _rate_ci(vol_scored, "vol_state_correct")
    realized_buckets = [
        _classify_vol_state(float(r["realized_vol_ratio"]))
        for r in vol_scored
        if r.get("realized_vol_ratio") is not None
    ]
    vol_baseline: float | None = None
    vol_baseline_label: str | None = None
    if realized_buckets:
        label, cnt = Counter(realized_buckets).most_common(1)[0]
        vol_baseline = round(cnt / len(realized_buckets), 4)
        vol_baseline_label = label

    # levels_brier is the mean Brier score across touch/flip probabilities
    # (lower is better, 0 = perfect, 0.25 = a coin flip).
    brier_vals = [
        float(r["levels_brier"]) for r in scored if r.get("levels_brier") is not None
    ]
    levels_brier_avg = round(sum(brier_vals) / len(brier_vals), 4) if brier_vals else None

    return {
        "symbol": symbol.upper(),
        "window": window,
        "n_scored": n,
        # Range coverage track record.
        "range_respected_rate": range_rate,
        "range_respected_ci": range_ci,
        "range_baseline": _RANGE_COVERAGE_BASELINE,
        "pin_hit_rate": _rate(scored, "pin_hit"),
        "regime_correct_rate": _rate(regime_scored, "regime_correct"),
        # Transparency: regime accuracy is scoped to the corrected-logic era.
        "regime_stats_from": fix_date.isoformat(),
        "regime_n_scored": regime_n,
        # Vol-call track record — independent of range; corrected-scale only.
        "vol_state_correct_rate": vol_rate,
        "vol_state_correct_ci": vol_ci,
        "vol_baseline": vol_baseline,
        "vol_baseline_label": vol_baseline_label,
        "vol_stats_from": vol_fix_date.isoformat(),
        "vol_n_scored": vol_n,
        # Levels calibration.
        "levels_brier_avg": levels_brier_avg,
        "levels_n_scored": len(brier_vals),
    }
