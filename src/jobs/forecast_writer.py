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
from src.jobs.forecast_calendar import (
    days_to_next_opex,
    is_monthly_opex_friday,
    is_post_opex_monday,
    is_vix_expiration_day,
)
from src.jobs.forecast_range_model import (
    ForecastInputs,
    ForecastResult,
    compute_forecast,
)
from src.market_calendar import NYSE_HOLIDAYS

# Cash-index symbols use VXN as their vol regime proxy; everything else
# uses VIX.  QQQ tracks NASDAQ; SPY / SPX / IWM track S&P style vol.
_VXN_SYMBOLS = {"QQQ", "NDX"}


def _strike_step_for(symbol: str) -> float:
    """Ladder step for pin snapping and tolerance scaling.  SPX uses $5
    strikes at scale; everything else uses $1."""
    return 5.0 if symbol.upper() in {"SPX", "NDX", "RUT"} else 1.0

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


async def _fetch_optional(db: DatabaseManager, method_name: str, label: str, symbol: str, *args, **kwargs) -> Any:
    """Best-effort DB fetch — catches missing methods (AttributeError) and
    coroutine failures alike so a signal fetch failure never breaks the
    forecast; it just degrades the quality of that day's inputs."""
    try:
        method = getattr(db, method_name)
    except AttributeError:
        logger.warning("forecast_writer: %s missing on db layer (skipping %s)", method_name, label)
        return None
    try:
        return await method(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001
        logger.warning("forecast_writer: %s failed (%s): %s", label, symbol, exc)
        return None


async def _gather_inputs(db: DatabaseManager, symbol: str) -> Optional[ForecastInputs]:
    """Pull every input the v1.2 model needs from the in-process DB layer.

    Every optional signal degrades gracefully — a missing VIX just means
    no implied-vol blend, missing gex_by_strike means no top-gamma-nodes,
    etc.  Spot is the only hard requirement; without it we can't anchor
    the band.
    """
    gex = await _fetch_optional(db, "get_latest_gex_summary", "get_latest_gex_summary", symbol, symbol)
    quote = await _fetch_optional(db, "get_latest_quote", "get_latest_quote", symbol, symbol)
    score = await _fetch_optional(db, "get_latest_signal_score", "get_latest_signal_score", symbol, symbol)

    # Flagship setup — best-effort read of the most recent Action Card.
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

    # Vol regime — VIX for equity index-ish, VXN for NASDAQ-family.
    is_nasdaq_family = symbol.upper() in _VXN_SYMBOLS
    vol_ticker = "VXN" if is_nasdaq_family else "VIX"
    vix_bars = await _fetch_optional(
        db, "get_volatility_index_bars", f"get_volatility_index_bars[{vol_ticker}]", symbol,
        ticker=vol_ticker, cutoff=None, tz="UTC",
    )
    vix_close = None
    if vix_bars:
        last = vix_bars[-1]
        vix_close = _f(last.get("close"))

    vix_z = await _fetch_optional(db, "get_vix_z_score_20d", f"get_vix_z_score_20d[{vol_ticker}]", symbol, vol_ticker)
    iv_rank = await _fetch_optional(db, "get_iv_rank_30d", "get_iv_rank_30d", symbol, symbol)
    atr = await _fetch_optional(db, "get_atr_5d", "get_atr_5d", symbol, symbol)
    top_nodes = await _fetch_optional(db, "get_top_gamma_nodes", "get_top_gamma_nodes", symbol, symbol, k=3) or []

    # 0DTE walls — only worth fetching on OPEX-adjacent days.  We ask
    # for today's expiration; if the analytics engine wrote a row for
    # it, we get real values back, else None.
    call_wall_0dte, put_wall_0dte = None, None
    is_opex_fri = is_monthly_opex_friday(today)
    is_post_opex = is_post_opex_monday(today)
    if is_opex_fri or is_post_opex:
        walls_0dte = await _fetch_optional(
            db, "get_walls_by_expiration", "get_walls_by_expiration", symbol, symbol, today,
        )
        if walls_0dte:
            call_wall_0dte = _f(walls_0dte.get("call_wall"))
            put_wall_0dte = _f(walls_0dte.get("put_wall"))

    # Layer 2 calibration scalars (v1.3 correction layer).  On cold start
    # this returns the neutral {1.0, 1.0, 0, 0} state which is a no-op.
    calibration_row = await _fetch_optional(
        db, "get_forecast_calibration", "get_forecast_calibration", symbol, symbol,
    )
    calibration_payload = None
    if calibration_row:
        calibration_payload = {
            "band_width_mult": float(calibration_row["band_width_mult"]),
            "pin_tolerance_mult": float(calibration_row["pin_tolerance_mult"]),
            "upside_lean": float(calibration_row["upside_lean"]),
            "downside_lean": float(calibration_row["downside_lean"]),
            "n_receipts_used": int(calibration_row.get("n_receipts_used") or 0),
        }

    # MSI sub-signals for the "screaming amplifier" — put/call ratio and
    # skew_delta.  Both live in gex_summary / basic_signals respectively.
    pcr = _f(gex.get("put_call_ratio")) if gex else None
    skew = None
    skew_signal = await _fetch_optional(db, "get_basic_signal", "skew_delta", symbol, symbol, "skew_delta")
    if skew_signal:
        ctx = skew_signal.get("context_values") or {}
        put_iv = ctx.get("otm_put_iv")
        call_iv = ctx.get("otm_call_iv")
        if put_iv is not None and call_iv is not None:
            skew = float(put_iv) - float(call_iv)

    return ForecastInputs(
        symbol=symbol,
        forecast_date=today,
        spot=spot,
        call_wall=_f(gex.get("call_wall")) if gex else None,
        put_wall=_f(gex.get("put_wall")) if gex else None,
        gamma_flip=_f(gex.get("gamma_flip")) if gex else None,
        max_pain=_f(gex.get("max_pain")) if gex else None,
        call_wall_0dte=call_wall_0dte,
        put_wall_0dte=put_wall_0dte,
        top_gamma_nodes=top_nodes,
        msi_composite=_f(score.get("composite_score")) if score else None,
        msi_normalized=_f(score.get("normalized_score")) if score else None,
        put_call_ratio=pcr,
        skew_delta=skew,
        vix_close=None if is_nasdaq_family else vix_close,
        vxn_close=vix_close if is_nasdaq_family else None,
        vix_z_score_20d=vix_z,
        iv_rank_30d=iv_rank,
        atr_5d=atr,
        flagship_setup=flagship,
        is_event_day=_is_event_day(today),
        is_opex_friday=is_opex_fri,
        is_vix_expiration=is_vix_expiration_day(today),
        is_post_opex_monday=is_post_opex,
        days_to_opex=days_to_next_opex(today),
        strike_step=_strike_step_for(symbol),
        calibration=calibration_payload,
    )


def _build_payload(inputs: ForecastInputs, result: ForecastResult, open_ts: datetime) -> dict[str, Any]:
    """Translate (inputs, result) → daily_forecast row payload + hash."""
    # forecast_inputs is a JSONB audit blob — everything the model saw
    # at write time so we can later diagnose why a specific forecast
    # went a particular way.  Excludes flagship_setup (already own
    # column) and calibration (would race against the calibration state
    # table's own timeline).
    inputs_snapshot: dict[str, Any] = {
        "vix_close": inputs.vix_close,
        "vxn_close": inputs.vxn_close,
        "vix_z_score_20d": inputs.vix_z_score_20d,
        "iv_rank_30d": inputs.iv_rank_30d,
        "atr_5d": inputs.atr_5d,
        "put_call_ratio": inputs.put_call_ratio,
        "skew_delta": inputs.skew_delta,
        "call_wall_0dte": inputs.call_wall_0dte,
        "put_wall_0dte": inputs.put_wall_0dte,
        "top_gamma_nodes": inputs.top_gamma_nodes,
        "is_opex_friday": inputs.is_opex_friday,
        "is_vix_expiration": inputs.is_vix_expiration,
        "is_post_opex_monday": inputs.is_post_opex_monday,
        "days_to_opex": inputs.days_to_opex,
        "is_event_day": inputs.is_event_day,
        "calibration_applied": result.calibration_applied,
        "rationale": result.rationale,
    }

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
        "pin_tolerance": result.pin_tolerance,
        "flagship_setup": inputs.flagship_setup,
        "range_model": result.range_model,
        "raw_projected_low": result.raw_projected_low,
        "raw_projected_high": result.raw_projected_high,
        "raw_pin_strike": result.raw_pin_strike,
        "forecast_inputs": inputs_snapshot,
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
