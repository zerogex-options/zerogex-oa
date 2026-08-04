"""Morning forecast writer — fires at 08:30 ET on weekdays.

Timing note: 08:30 ET is deliberately AFTER the ~07:30 ET options reset so
the GEX surface reflects today's positioning (not yesterday's stale chain),
and the dealer-gamma regime is a live read.  Cash indexes (SPX) do not
trade overnight, so their spot anchor is projected from the mapped future
(@ES) at this time rather than the frozen prior cash close — see
``src.jobs.index_projection``.

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
from datetime import date, datetime, timedelta, timezone
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
from src.jobs.index_projection import implied_index_spot
from src.market_calendar import NYSE_HOLIDAYS

# Cash-index symbols use VXN as their vol regime proxy; everything else
# uses VIX.  QQQ tracks NASDAQ; SPY / SPX / IWM track S&P style vol.
_VXN_SYMBOLS = {"QQQ", "NDX"}

# A GEX snapshot older than this at forecast time is treated as stale — the
# regime degrades to "transition" rather than asserting dealer positioning
# off a frozen prior-session surface.  The analytics engine rewrites the
# surface every 60s in-session / 300s off-hours, so a healthy pre-market
# snapshot is minutes old; 6h cleanly separates "today, post-07:30-reset"
# (minutes old at 08:30 ET) from a frozen prior-session surface (~16h old).
GEX_MAX_STALENESS = timedelta(hours=6)


def _signed_composite(value: Any) -> Optional[float]:
    """Map the 0-100 Market State Index composite to the signed -1..+1 scale
    the range model expects (neutral 50 -> 0).

    The writer historically fed the raw 0-100 value into a model that keys
    off a signed value, which pinned every symbol's regime to ``long_gamma``
    and silently disabled the MSI band adjustments (see
    ``forecast_range_model`` docstrings)."""
    try:
        c = float(value)
    except (TypeError, ValueError):
        return None
    return (c - 50.0) / 50.0


def _signed_normalized(value: Any) -> Optional[float]:
    """Map the 0-1 normalized MSI to the signed -100..+100 scale the range
    model and the stored ``open_msi`` expect (neutral 0.5 -> 0)."""
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return (n - 0.5) * 200.0


def _gex_is_fresh(ts: Any, *, now: Optional[datetime] = None) -> bool:
    """True when a GEX snapshot is recent enough to assert a dealer-gamma
    regime off it.  A ``None``/non-datetime timestamp, or one older than
    ``GEX_MAX_STALENESS``, is stale -> the regime degrades to transition."""
    if not isinstance(ts, datetime):
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ref = now or datetime.now(tz=timezone.utc)
    return (ref - ts) <= GEX_MAX_STALENESS


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
    # get_latest_quote returns the latest 1-min bar's ``close`` (there is no
    # ``last`` key — the old ``quote["last"]`` branch was dead code and always
    # fell through to gex.spot_price).  Both resolve to the same latest cash
    # print; gex.spot_price is the fallback when no quote row exists.
    if quote and quote.get("close") is not None:
        spot = float(quote["close"])
    elif gex and gex.get("spot_price") is not None:
        spot = float(gex["spot_price"])
    if spot is None:
        logger.warning("forecast_writer: no spot for %s — cannot forecast", symbol)
        return None

    # Futures-implied anchor for cash indexes OUTSIDE the cash session.  A
    # cash index (SPX/NDX/…) has no overnight print, so the cash spot above
    # is a frozen prior 16:00 close — anchoring the morning forecast on it
    # ignores wherever the futures have moved the market overnight.  Project
    # the implied cash level from the mapped future (@ES) instead.  Returns
    # None (keep the cash spot) in-session, on weekends, or when the futures
    # feed is empty — so SPY/QQQ and in-session runs are unaffected.
    open_spot_source = "cash"
    open_spot_projection: Optional[dict[str, Any]] = None
    futures_gap_pct: Optional[float] = None
    proj = None
    try:
        proj = await implied_index_spot(db, symbol)
    except Exception as exc:  # noqa: BLE001 — never let projection break the forecast
        logger.warning("forecast_writer: index projection failed (%s): %s", symbol, exc)
    if proj is not None:
        spot = proj.implied_price
        open_spot_source = "futures_implied"
        open_spot_projection = proj.as_audit()
        futures_gap_pct = proj.gap_pct
        logger.info(
            "forecast_writer: %s spot projected from %s — implied $%.2f "
            "(cash close $%.2f, overnight %+.2f pts / %+.2f%%)",
            symbol, proj.future_symbol, proj.implied_price,
            proj.cash_ref_close, proj.gap_points, proj.gap_pct * 100.0,
        )

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
    # get_volatility_index_bars requires a real datetime cutoff and a
    # tzinfo object — passing None + "UTC" (string) silently swallowed
    # every VIX/VXN fetch through _fetch_optional's error catcher.  A
    # 3-day lookback comfortably covers the latest bar even after a
    # holiday weekend.
    vix_cutoff = datetime.now(timezone.utc) - timedelta(days=3)
    vix_bars = await _fetch_optional(
        db, "get_volatility_index_bars", f"get_volatility_index_bars[{vol_ticker}]", symbol,
        ticker=vol_ticker, cutoff=vix_cutoff, tz=timezone.utc,
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
            # Per-symbol expected-range center for the vol grade.  Carried into
            # forecast_inputs.calibration_applied by compute_forecast so the
            # receipt grades against the basis committed this morning.
            "vol_range_basis_mult": float(
                calibration_row.get("vol_range_basis_mult", 1.0) or 1.0
            ),
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

    inputs = ForecastInputs(
        symbol=symbol,
        forecast_date=today,
        spot=spot,
        call_wall=_f(gex.get("call_wall")) if gex else None,
        put_wall=_f(gex.get("put_wall")) if gex else None,
        gamma_flip=_f(gex.get("gamma_flip")) if gex else None,
        max_pain=_f(gex.get("max_pain")) if gex else None,
        net_gex=_f(gex.get("net_gex")) if gex else None,
        gex_surface_fresh=_gex_is_fresh(gex.get("timestamp")) if gex else False,
        # Vol-character inputs for the expected-volatility claim.
        local_gex=_f(gex.get("local_gex")) if gex else None,
        convexity_risk=_f(gex.get("convexity_risk")) if gex else None,
        flip_distance=_f(gex.get("flip_distance")) if gex else None,
        call_wall_0dte=call_wall_0dte,
        put_wall_0dte=put_wall_0dte,
        top_gamma_nodes=top_nodes,
        # Feed the MSI on the SIGNED scale the range model expects: the raw
        # composite is 0-100 (neutral 50) and normalized 0-1 (neutral 0.5);
        # passing them unsigned pinned the regime + disabled the band lean.
        msi_composite=_signed_composite(score.get("composite_score")) if score else None,
        msi_normalized=_signed_normalized(score.get("normalized_score")) if score else None,
        put_call_ratio=pcr,
        skew_delta=skew,
        vix_close=None if is_nasdaq_family else vix_close,
        vxn_close=vix_close if is_nasdaq_family else None,
        vix_z_score_20d=vix_z,
        iv_rank_30d=iv_rank,
        atr_5d=atr,
        futures_gap_pct=futures_gap_pct,
        open_spot_source=open_spot_source,
        open_spot_projection=open_spot_projection,
        flagship_setup=flagship,
        is_event_day=_is_event_day(today),
        is_opex_friday=is_opex_fri,
        is_vix_expiration=is_vix_expiration_day(today),
        is_post_opex_monday=is_post_opex,
        days_to_opex=days_to_next_opex(today),
        strike_step=_strike_step_for(symbol),
        calibration=calibration_payload,
    )

    # Signal-health summary — one line per symbol per fire so a daily
    # ``journalctl -u zerogex-oa-forecast-writer | grep signals`` reveals
    # exactly what's feeding the model.  Silent degradation was the reason
    # VIX quietly missing for a week — this line makes gaps visible
    # without inflating WARNING count.  Ticks (=✓) for populated fields,
    # crosses (=✗) for missing.
    def _tick(value: Any) -> str:
        return "✓" if value is not None and value != [] else "✗"

    logger.info(
        "forecast_writer: %s signals — walls=%s pcr=%s msi=%s "
        "%s=%s vix_z=%s iv_rank=%s atr=%s nodes=%d skew=%s "
        "calib=%s flagship=%s (opex_fri=%s vix_exp=%s post_opex=%s event=%s)",
        symbol,
        _tick(inputs.call_wall if inputs.call_wall is not None else inputs.put_wall),
        _tick(inputs.put_call_ratio),
        _tick(inputs.msi_composite),
        "vix" if not is_nasdaq_family else "vxn",
        _tick(inputs.vix_close if not is_nasdaq_family else inputs.vxn_close),
        _tick(inputs.vix_z_score_20d),
        _tick(inputs.iv_rank_30d),
        _tick(inputs.atr_5d),
        len(inputs.top_gamma_nodes),
        _tick(inputs.skew_delta),
        _tick(inputs.calibration) if calibration_payload else "cold",
        _tick(inputs.flagship_setup),
        inputs.is_opex_friday, inputs.is_vix_expiration,
        inputs.is_post_opex_monday, inputs.is_event_day,
    )

    return inputs


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
        "local_gex": inputs.local_gex,
        "convexity_risk": inputs.convexity_risk,
        "flip_distance": inputs.flip_distance,
        "is_opex_friday": inputs.is_opex_friday,
        "is_vix_expiration": inputs.is_vix_expiration,
        "is_post_opex_monday": inputs.is_post_opex_monday,
        "days_to_opex": inputs.days_to_opex,
        "is_event_day": inputs.is_event_day,
        "calibration_applied": result.calibration_applied,
        "rationale": result.rationale,
        # Spot-anchor provenance: "cash" or "futures_implied" (+ the
        # projection audit) so a committed SPX open_spot can be traced back
        # to the @ES level it was projected from.
        "open_spot_source": inputs.open_spot_source,
        "open_spot_projection": inputs.open_spot_projection,
        "futures_gap_pct": inputs.futures_gap_pct,
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
        "regime_move_threshold": result.regime_move_threshold,
        "flagship_setup": inputs.flagship_setup,
        "range_model": result.range_model,
        "raw_projected_low": result.raw_projected_low,
        "raw_projected_high": result.raw_projected_high,
        "raw_pin_strike": result.raw_pin_strike,
        "forecast_inputs": inputs_snapshot,
        # v1.4 gradeable claims (replace the pin/regime tiles on the card).
        "expected_vol_state": result.expected_vol_state,
        "expected_vol_ratio": result.expected_vol_ratio,
        "implied_move": result.implied_move,
        "flip_cross_prob": result.flip_cross_prob,
        "level_touch_probs": result.level_touch_probs,
        "gravity_center": result.gravity_center,
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
