"""Intraday cone writer — fires every 15 minutes from 09:45 to 15:30 ET.

Each fire re-anchors on the current bar, re-reads the current dealer surface,
and commits one immutable claim per horizon that can still complete before
the bell.  Where ``forecast_writer`` makes one commitment a day, this makes
roughly twenty, which is the point: a cone that publishes a probability earns
the right to be believed only by accumulating enough graded claims to show
whether that probability means anything, and at one claim a day that takes a
year.

The vol basis is deliberately the SAME ``implied_move`` the morning forecast
committed, not a freshly-read VIX.  Two reasons.  The morning number is on
the record and immutable, so anchoring to it keeps the intraday cone and the
daily band denominated in one basis — a disagreement between them is then a
real disagreement about structure rather than an artifact of two jobs reading
the vol surface at different minutes.  And a re-read VIX would let the vol
basis drift through the session in a way nobody committed to, which is
exactly the kind of quiet after-the-fact improvement the immutability trigger
exists to prevent.

Like every job in this family: never raises.  A bad fire logs WARNING and
exits 0, because a missing cone at 11:15 is a gap in the track record and a
crashed timer is the end of it.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
from datetime import date, datetime, time, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.jobs.intraday_cone_model import (
    CONE_HORIZONS_MIN,
    FIRST_FIRE_MIN,
    LAST_FIRE_MIN,
    MODEL_VERSION,
    ConeInputs,
    compute_cone,
)
from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
SESSION_OPEN = time(9, 30)

#: A GEX snapshot older than this is not a live read of the current surface.
#: The analytics engine rewrites it every 60s in-session, so a healthy
#: snapshot is a minute or two old; 20 minutes cleanly separates "current"
#: from "the feed has stalled".  A stale surface does not block the cone —
#: it drops the gamma conditioning and says so, which is a wider, honest band
#: rather than a confident one conditioned on positioning we cannot verify.
GEX_MAX_STALENESS = timedelta(minutes=20)


def _today_et() -> date:
    return datetime.now(tz=ET).date()


def _is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day.isoformat() not in NYSE_HOLIDAYS


def _session_open_ts(day: date) -> datetime:
    return datetime.combine(day, SESSION_OPEN, tzinfo=ET)


def _elapsed_minutes(now: datetime, day: date) -> float:
    return (now - _session_open_ts(day)).total_seconds() / 60.0


def _f(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _content_hash(payload: dict[str, Any]) -> str:
    """Stable hash over the committed claim.

    Covers the band, the probability and the surface it was conditioned on,
    so a re-run that would have produced a different claim is detectable even
    though the insert itself is a no-op.
    """
    material = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


async def _fetch_optional(db: DatabaseManager, method: str, symbol: str, *args) -> Any:
    fn = getattr(db, method, None)
    if fn is None:
        logger.warning("intraday_cone_writer: %s unavailable", method)
        return None
    try:
        return await fn(symbol, *args)
    except Exception as exc:  # noqa: BLE001 — a thin surface is not a crash
        logger.warning("intraday_cone_writer: %s(%s) failed: %s", method, symbol, exc)
        return None


def _gex_is_fresh(ts: Any, now: datetime) -> bool:
    """Whether a GEX snapshot is a live read as of ``now``.

    Note the lower bound.  A naive ``(now - ts) <= GEX_MAX_STALENESS`` is True
    for a NEGATIVE age — a surface timestamped after the anchor — which is
    exactly what a backfill sees: reading today's walls while reconstructing
    a fire from two days ago.  That is lookahead, and lookahead in the one
    system whose entire value is honest grading would quietly invalidate
    every number it publishes.  A future-dated snapshot is not fresh; it is
    impossible, and it is rejected.
    """
    if ts is None:
        return False
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=ET)
    age = now - ts
    return timedelta(0) <= age <= GEX_MAX_STALENESS


async def _build_inputs(
    db: DatabaseManager, symbol: str, day: date, now: datetime
) -> Optional[tuple[ConeInputs, dict[str, Any]]]:
    """Assemble one fire's inputs, plus the provenance dict for the hash."""
    # Point-in-time reads, keyed to the anchor rather than to "newest row".
    #
    # In live operation `now` IS the present, so these resolve to the same
    # rows get_latest_quote / get_latest_gex_summary would have returned —
    # one code path, no backfill branch to drift out of step. In a backfill
    # they are the difference between a cone that re-anchors and one that
    # does not: a Sunday run of Friday's session read Friday's CLOSING print
    # for all 25 fires, so every band was drawn around the same price and
    # then graded against a tape that was somewhere else all day.
    session_open = _session_open_ts(day)
    quote = await _fetch_optional(db, "get_quote_as_of", symbol, now, session_open)
    gex = await _fetch_optional(db, "get_gex_summary_as_of", symbol, now, session_open)

    spot = _f(quote.get("close")) if quote else None
    if spot is None or spot <= 0:
        logger.warning(
            "intraday_cone_writer: no %s bar at or before %s — skipping fire",
            symbol, now.strftime("%Y-%m-%d %H:%M"),
        )
        return None

    # The committed morning vol basis (see the module docstring on why this is
    # not a fresh VIX read).
    implied_move: Optional[float] = None
    try:
        morning = await db.get_daily_forecast(symbol, day)
        if morning:
            implied_move = _f(morning.get("implied_move"))
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "intraday_cone_writer: morning forecast lookup failed for %s: %s", symbol, exc
        )
    if implied_move is None:
        # No morning commitment (a backfill, or the 08:30 writer missed).  The
        # realized-so-far estimate alone still produces an honest cone; it is
        # just anchored on today's tape rather than on a published claim.
        logger.info(
            "intraday_cone_writer: %s has no committed implied_move — "
            "falling back to the realized-only vol basis",
            symbol,
        )

    session_extremes = None
    try:
        session_extremes = await db.get_bar_extremes_between(
            symbol, session_open, now
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "intraday_cone_writer: session extremes failed for %s: %s", symbol, exc
        )

    fresh = _gex_is_fresh(gex.get("timestamp"), now) if gex else False
    if gex and not fresh:
        logger.info(
            "intraday_cone_writer: %s GEX surface is stale — "
            "dropping gamma conditioning for this fire",
            symbol,
        )

    inputs = ConeInputs(
        symbol=symbol,
        spot=spot,
        elapsed_min=_elapsed_minutes(now, day),
        implied_move=implied_move,
        session_high=_f(session_extremes.get("window_high")) if session_extremes else None,
        session_low=_f(session_extremes.get("window_low")) if session_extremes else None,
        call_wall=_f(gex.get("call_wall")) if (gex and fresh) else None,
        put_wall=_f(gex.get("put_wall")) if (gex and fresh) else None,
        gamma_flip=_f(gex.get("gamma_flip")) if (gex and fresh) else None,
        net_gex_at_spot=_f(gex.get("net_gex_at_spot")) if (gex and fresh) else None,
        horizons=CONE_HORIZONS_MIN,
    )
    provenance = {
        "symbol": symbol,
        "session_date": day.isoformat(),
        "spot": spot,
        "implied_move": implied_move,
        "gex_fresh": fresh,
        "model_version": MODEL_VERSION,
    }
    return inputs, provenance


def _rows_for_fire(
    inputs: ConeInputs,
    result: Any,
    provenance: dict[str, Any],
    symbol: str,
    day: date,
    now: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for h in result.horizons:
        claim = {
            **provenance,
            "horizon_min": h.horizon_min,
            "band_low": h.band_low,
            "band_high": h.band_high,
            "hold_prob": h.hold_prob,
            "sigma": h.sigma,
        }
        rows.append(
            {
                "symbol": symbol,
                "session_date": day,
                "forecast_ts": now,
                "horizon_min": h.horizon_min,
                "target_ts": now + timedelta(minutes=h.horizon_min),
                "anchor_spot": inputs.spot,
                "band_low": h.band_low,
                "band_high": h.band_high,
                "hold_prob": h.hold_prob,
                "sigma": h.sigma,
                "call_wall": inputs.call_wall,
                "put_wall": inputs.put_wall,
                "gamma_flip": inputs.gamma_flip,
                "net_gex_at_spot": inputs.net_gex_at_spot,
                "daily_sigma": round(result.daily_sigma, 4),
                "gamma_mult": round(result.gamma_mult, 4),
                "elapsed_min": int(inputs.elapsed_min),
                "model_version": result.model_version,
                "content_hash": _content_hash(claim),
            }
        )
    return rows


async def _run(args: argparse.Namespace) -> int:
    day = date.fromisoformat(args.date) if args.date else _today_et()
    now = (
        datetime.fromisoformat(args.at).replace(tzinfo=ET)
        if args.at
        else datetime.now(tz=ET)
    )

    if not _is_trading_day(day) and not args.allow_non_trading_day:
        logger.info("intraday_cone_writer: skipping %s — not a trading day", day.isoformat())
        return 0

    elapsed = _elapsed_minutes(now, day)
    if not args.allow_off_window and not (FIRST_FIRE_MIN <= elapsed <= LAST_FIRE_MIN):
        logger.info(
            "intraday_cone_writer: %s is outside the fire window "
            "(%.0f min elapsed, window %d–%d) — nothing to do",
            now.strftime("%H:%M"), elapsed, FIRST_FIRE_MIN, LAST_FIRE_MIN,
        )
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:  # noqa: BLE001
        logger.warning("intraday_cone_writer: DB connect failed (%s) — exiting 0", exc)
        return 0

    try:
        symbols = [s.strip().upper() for s in args.symbol.split(",") if s.strip()]
        for sym in symbols:
            built = await _build_inputs(db, sym, day, now)
            if built is None:
                continue
            inputs, provenance = built
            result = compute_cone(inputs)
            if not result.horizons:
                logger.info(
                    "intraday_cone_writer: %s produced no horizons (%s)",
                    sym, " · ".join(result.rationale) or "no reason given",
                )
                continue

            rows = _rows_for_fire(inputs, result, provenance, sym, day, now)
            summary = " ".join(
                f"+{h.horizon_min}m[{h.band_low:.2f}–{h.band_high:.2f}]"
                f"{h.hold_prob * 100:.0f}%" if h.hold_prob is not None
                else f"+{h.horizon_min}m[{h.band_low:.2f}–{h.band_high:.2f}]—"
                for h in result.horizons
            )
            if args.dry_run:
                logger.info(
                    "intraday_cone_writer: DRY RUN %s %s spot=%.2f %s | %s",
                    sym, now.strftime("%H:%M"), inputs.spot, summary,
                    " · ".join(result.rationale),
                )
                continue

            inserted = await db.insert_intraday_cone(rows)
            if inserted == 0:
                logger.info(
                    "intraday_cone_writer: %s %s already committed — leaving as-is",
                    sym, now.strftime("%H:%M"),
                )
            else:
                logger.info(
                    "intraday_cone_writer: committed %s %s spot=%.2f (%d horizons) %s",
                    sym, now.strftime("%H:%M"), inputs.spot, inserted, summary,
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
        "--symbol",
        default=os.environ.get("FORECAST_SYMBOLS", "SPY"),
        help="Comma-separated symbols to forecast (default SPY).",
    )
    parser.add_argument("--date", help="Session date to fire for (YYYY-MM-DD).")
    parser.add_argument(
        "--at",
        help="Fire as though it were this ET time (YYYY-MM-DDTHH:MM), for backfill.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute + log the cone but do NOT write to the DB.",
    )
    parser.add_argument(
        "--allow-non-trading-day",
        action="store_true",
        help="Override the weekend/holiday skip.",
    )
    parser.add_argument(
        "--allow-off-window",
        action="store_true",
        help="Override the 09:45-15:30 ET fire window.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
