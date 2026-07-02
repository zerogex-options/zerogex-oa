"""Layer-2 online correction — nightly recalibration of the v1.2 heuristic.

Reads the trailing 20 receipts per symbol from ``daily_forecast`` and
shifts four scalars (band width multiplier, pin tolerance multiplier,
upside lean, downside lean) toward whatever the trailing error signal
suggests would have been more accurate.  Writes them to
``forecast_calibration_state`` where the writer picks them up next
morning.

Update rule per scalar:
  * Learning rate is intentionally small (LR=0.05).  We're not trying to
    chase individual days — we want a slow, explainable drift toward the
    signal the data is pointing at.
  * Every scalar is clamped to a sensible range so a bad week can't send
    the model somewhere unreasonable.
  * Requires at least MIN_RECEIPTS labels before applying any correction
    (cold-start guard).

Grades against the RAW pre-correction band + pin so the correction
loop isn't chasing its own tail: if v1.2 raw predicted [742, 750] and
Layer 2 corrected to [740, 752] and the actual range was [741, 748],
raw was "too tight downside" so we widen ``downside_lean``.  The
corrected prediction being wider doesn't tell us anything about the
raw heuristic's error — we grade the *heuristic* to learn.

Never raises.  DB failures / no-data / cold-start all log + exit 0.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager

logger = logging.getLogger("zerogex.forecast_calibrate")
ET = ZoneInfo("America/New_York")

# Configuration constants — worth revisiting once v1.3 has been live
# for a month.  Every number here should be defendable in the readme.
LOOKBACK_RECEIPTS = 20
MIN_RECEIPTS = 15            # cold-start guard; corrections stay neutral below this
LEARNING_RATE = 0.05
TARGET_COVERAGE = 0.90       # what fraction of days should the band contain

# Bounds — never let a scalar drift past these regardless of how many
# consecutive misses accumulate.
BOUNDS = {
    "band_width_mult": (0.70, 1.50),
    "pin_tolerance_mult": (0.50, 2.00),
    "upside_lean": (-0.20, 0.20),
    "downside_lean": (-0.20, 0.20),
}


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _bounded(name: str, v: float) -> float:
    lo, hi = BOUNDS[name]
    return _clamp(v, lo, hi)


def _compute_updates(
    receipts: list[dict[str, Any]], current: dict[str, Any]
) -> dict[str, Any]:
    """Compute the four new calibration scalars from the receipts.

    Grades against ``raw_*`` fields (pre-correction v1.2 heuristic).
    Returns a dict shaped like a ``forecast_calibration_state`` row
    plus a summary blob describing what happened.
    """
    # Only receipts that came from v1.2 (raw fields populated) can inform
    # calibration — legacy rows are silently skipped.
    graded = [r for r in receipts if r.get("raw_projected_low") is not None]
    n = len(graded)

    band = float(current.get("band_width_mult", 1.0))
    pin_mult = float(current.get("pin_tolerance_mult", 1.0))
    up_lean = float(current.get("upside_lean", 0.0))
    down_lean = float(current.get("downside_lean", 0.0))

    if n < MIN_RECEIPTS:
        summary = {
            "n_receipts_used": n,
            "action": "cold_start_hold",
            "message": f"only {n} v1.2 receipts available; need >= {MIN_RECEIPTS}",
        }
        return {
            "band_width_mult": band,
            "pin_tolerance_mult": pin_mult,
            "upside_lean": up_lean,
            "downside_lean": down_lean,
            "n_receipts_used": n,
            "summary": summary,
        }

    # Coverage: fraction of days where the raw band contained the actual L/H.
    covered = sum(
        1 for r in graded
        if r.get("actual_low") is not None
        and r.get("actual_high") is not None
        and float(r["actual_low"]) >= float(r["raw_projected_low"])
        and float(r["actual_high"]) <= float(r["raw_projected_high"])
    )
    coverage = covered / n

    # Coverage error signals band-width correction.  Under target →
    # widen; over target → tighten.  Learning rate keeps the daily
    # step small; over months of nudges the scalar converges toward a
    # stable band width.
    coverage_error = TARGET_COVERAGE - coverage
    band = _bounded("band_width_mult", band + LEARNING_RATE * coverage_error * 5.0)

    # Upside vs downside imbalance — which side breaks the band more often?
    upside_breaks = sum(
        1 for r in graded
        if r.get("actual_high") is not None
        and float(r["actual_high"]) > float(r["raw_projected_high"])
    )
    downside_breaks = sum(
        1 for r in graded
        if r.get("actual_low") is not None
        and float(r["actual_low"]) < float(r["raw_projected_low"])
    )
    up_break_rate = upside_breaks / n
    down_break_rate = downside_breaks / n

    # If upside breaks are systematically higher than downside, widen
    # upside lean.  Same logic in reverse for the downside.  These
    # nudges are also LR-scaled and clamped.
    imbalance = up_break_rate - down_break_rate
    up_lean = _bounded("upside_lean", up_lean + LEARNING_RATE * imbalance * 3.0)
    down_lean = _bounded("downside_lean", down_lean - LEARNING_RATE * imbalance * 3.0)

    # Pin tolerance: aim for ~50% pin_hit rate (a real-world "pinning"
    # market pins about half the time on liquid indices).  Below that
    # widen; above tighten.
    pin_hits = sum(1 for r in graded if r.get("raw_pin_hit") is True)
    pin_hit_rate = pin_hits / n
    pin_target = 0.50
    pin_error = pin_target - pin_hit_rate
    pin_mult = _bounded("pin_tolerance_mult", pin_mult + LEARNING_RATE * pin_error * 4.0)

    summary = {
        "n_receipts_used": n,
        "coverage": round(coverage, 4),
        "coverage_error": round(coverage_error, 4),
        "up_break_rate": round(up_break_rate, 4),
        "down_break_rate": round(down_break_rate, 4),
        "pin_hit_rate": round(pin_hit_rate, 4),
        "action": "updated",
    }
    return {
        "band_width_mult": band,
        "pin_tolerance_mult": pin_mult,
        "upside_lean": up_lean,
        "downside_lean": down_lean,
        "n_receipts_used": n,
        "summary": summary,
    }


async def _recalibrate_one(db: DatabaseManager, symbol: str, dry_run: bool) -> dict[str, Any]:
    """Recalibrate one symbol.  Returns the resulting summary blob."""
    current = await db.get_forecast_calibration(symbol)
    receipts = await db.get_daily_forecast_history(symbol, limit=LOOKBACK_RECEIPTS)
    # Only rows that have a receipt — we can't grade a pending day.
    scored = [r for r in receipts if r.get("receipt_ts") is not None]

    updates = _compute_updates(scored, current)
    summary = updates["summary"]
    if dry_run:
        logger.info(
            "forecast_calibrate[dry-run]: %s → band=%.3f pin=%.3f up=%+.3f down=%+.3f n=%d (%s)",
            symbol,
            updates["band_width_mult"], updates["pin_tolerance_mult"],
            updates["upside_lean"], updates["downside_lean"],
            updates["n_receipts_used"], summary.get("action"),
        )
        return {"symbol": symbol, **updates}

    ok = await db.upsert_forecast_calibration(
        symbol=symbol,
        band_width_mult=updates["band_width_mult"],
        pin_tolerance_mult=updates["pin_tolerance_mult"],
        upside_lean=updates["upside_lean"],
        downside_lean=updates["downside_lean"],
        n_receipts_used=updates["n_receipts_used"],
        summary=summary,
    )
    if ok:
        logger.info(
            "forecast_calibrate: %s → band=%.3f pin=%.3f up=%+.3f down=%+.3f n=%d (%s)",
            symbol,
            updates["band_width_mult"], updates["pin_tolerance_mult"],
            updates["upside_lean"], updates["downside_lean"],
            updates["n_receipts_used"], summary.get("action"),
        )
    return {"symbol": symbol, **updates, "ok": ok}


async def _run(args: argparse.Namespace) -> int:
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        logger.warning("forecast_calibrate: no symbols — exiting 0")
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:
        logger.warning("forecast_calibrate: DB connect failed (%s) — exiting 0", exc)
        return 0

    try:
        for sym in symbols:
            try:
                await _recalibrate_one(db, sym, args.dry_run)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "forecast_calibrate: %s failed (%s) — skipping", sym, exc,
                )
        return 0
    finally:
        try:
            await db.disconnect()
        except Exception:  # noqa: BLE001
            pass


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbols",
        default=os.environ.get("FORECAST_SYMBOLS", "SPY"),
        help="Comma-separated symbols to recalibrate (default $FORECAST_SYMBOLS or SPY).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute + log the updated scalars but do NOT write to the DB.",
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
