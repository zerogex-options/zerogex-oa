"""Daily scorecard aggregator backing the public /scorecard/{date} page.

This is the data layer for the "Yesterday's Scorecard" auto-tweet feature.
A single endpoint computes one trading day's aggregate of the engine's
output (Action Cards emitted + per-signal flip P&L + closing regime) so
the website can server-render a branded recap page and the cron job can
generate a tweet at 4:15 PM ET.

The window is always one calendar day in the America/New_York time zone,
converted to UTC bounds before hitting the DB. Holidays / weekends are
not specially handled here — the cron job consults market_calendar before
invoking us; if the route is hit directly for a non-trading day it simply
returns an empty scorecard.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from zoneinfo import ZoneInfo

from ..database import DatabaseManager
from .trade_signals import _BASIC_SIGNAL_NAMES, get_db

router = APIRouter(prefix="/api/scorecard", tags=["Scorecard"])

ET = ZoneInfo("America/New_York")

# Advanced + Basic signal names — matches _VALID_SIGNAL_EVENT_NAMES in
# trade_signals.py so the per-signal aggregation hits exactly the surfaces
# the website renders dedicated pages for.
ADVANCED_SIGNAL_NAMES: tuple[str, ...] = (
    "vol_expansion",
    "eod_pressure",
    "squeeze_setup",
    "trap_detection",
    "zero_dte_position_imbalance",
    "gamma_vwap_confluence",
    "range_break_imminence",
    "market_pressure",
)
ALL_SIGNAL_NAMES: tuple[str, ...] = (*ADVANCED_SIGNAL_NAMES, *_BASIC_SIGNAL_NAMES)


def _parse_date(raw: str | None) -> date:
    """Parse a ``YYYY-MM-DD`` query string; default to today in ET."""
    if raw is None:
        return datetime.now(tz=ET).date()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"Invalid date '{raw}'; expected YYYY-MM-DD."
        ) from exc
    # Guard against absurd values that would produce empty DB windows. The
    # ingestion engine started in 2024-ish, so anything before that is a
    # typo. Allow up to "tomorrow ET" so a near-midnight client can request
    # today's scorecard if their clock drifts.
    today = datetime.now(tz=ET).date()
    if parsed < date(2024, 1, 1) or parsed > today + timedelta(days=1):
        raise HTTPException(
            status_code=422, detail=f"Date '{raw}' is outside the supported range."
        )
    return parsed


def _et_day_to_utc_bounds(day: date) -> tuple[datetime, datetime]:
    """Return [start_utc, end_utc) for the given calendar day in ET.

    Uses real ET local midnight (DST-aware via ZoneInfo) so the window
    aligns to the trader's day rather than a UTC offset that drifts twice
    a year. ``end_utc`` is the next calendar day's local midnight, so the
    window covers a full 24h regardless of DST transitions.
    """
    start_et = datetime.combine(day, time(0, 0), tzinfo=ET)
    end_et = datetime.combine(day + timedelta(days=1), time(0, 0), tzinfo=ET)
    return start_et.astimezone(ZoneInfo("UTC")), end_et.astimezone(ZoneInfo("UTC"))


def _humanize_signal_name(name: str) -> str:
    return name.replace("_", " ").title()


def _label_regime(reg: dict[str, Any] | None) -> str:
    if not reg:
        return "unknown"
    direction = (reg.get("direction") or "").lower()
    composite = reg.get("composite_score")
    # The composite_score sign is the cleanest gamma-regime proxy we have
    # without re-querying gex_summary here: positive = long-gamma stabilizing,
    # negative = short-gamma destabilizing.
    if isinstance(composite, (int, float)):
        if composite > 0.15:
            return "long gamma"
        if composite < -0.15:
            return "short gamma"
        return "transition"
    if "bull" in direction:
        return "long gamma"
    if "bear" in direction:
        return "short gamma"
    return "transition"


@router.get("/daily")
async def get_daily_scorecard(
    date: str | None = Query(
        default=None,
        description="Trading day in YYYY-MM-DD; defaults to today (America/New_York).",
    ),
    symbol: str = Query(default="SPY", max_length=10),
    horizon_minutes: int = Query(default=60, ge=5, le=240),
    db: DatabaseManager = Depends(get_db),
):
    """One trading day's aggregate of the engine's output.

    Combines three reads — persisted Action Cards, per-signal flip events
    with realized return at ``horizon_minutes``, and the closing MSI regime —
    into a single payload sized for the public ``/scorecard/{date}`` page
    and the auto-tweet job.

    **Params:**
    - ``date`` — YYYY-MM-DD; defaults to today in America/New_York.
    - ``symbol`` — underlying (default ``SPY``).
    - ``horizon_minutes`` — forward window for ``realized_return`` (5–240, default 60).

    **Returns:**
    ```json
    {
      "date": "2026-06-29",
      "symbol": "SPY",
      "tz": "America/New_York",
      "horizon_minutes": 60,
      "cards": {
        "total": 12,
        "by_action": [{"action": "SELL_CALL_SPREAD", "count": 4}, ...],
        "first_card_id": 4221,
        "first_card_permalink": "/cards/4221"
      },
      "signals": {
        "events": [{"name": "...", "flips": 3, "wins": 2, "losses": 1,
                     "avg_directional_return": 0.0042}, ...],
        "best":  {"name": "squeeze_setup",     "avg_directional_return": 0.0074, "wins": 2, "losses": 1, "flips": 3},
        "worst": {"name": "vanna_charm_flow",  "avg_directional_return": -0.0031, "wins": 1, "losses": 2, "flips": 3}
      },
      "regime": {"timestamp": "...", "composite_score": -0.28, "label": "short gamma"},
      "tweet_text": "SPY · 2026-06-29 — 12 Playbook calls. Best: Squeeze Setup +0.74%. Worst: Vanna Charm Flow −0.31%. Regime: short gamma. https://zerogex.io/scorecard/2026-06-29",
      "is_empty": false
    }
    ```

    ``tweet_text`` is the canonical one-line summary the auto-tweet job
    uses verbatim; computing it server-side keeps wording consistent
    across the OG card, the page header, and the X/Discord tweet.

    Returns ``is_empty: true`` with zeroed sections when no cards were
    emitted and no flip events occurred (typical for non-trading days or
    pre-launch dates).
    """
    sym = symbol.upper()
    day = _parse_date(date)
    start_utc, end_utc = _et_day_to_utc_bounds(day)

    payload = await db.get_daily_scorecard(
        symbol=sym,
        start_utc=start_utc,
        end_utc=end_utc,
        signal_names=list(ALL_SIGNAL_NAMES),
        horizon_minutes=horizon_minutes,
    )

    cards = payload["cards"]
    cards["first_card_permalink"] = (
        f"/cards/{cards['first_card_id']}" if cards.get("first_card_id") else None
    )

    regime = payload.get("regime")
    regime_label = _label_regime(regime)
    if regime is not None:
        regime["label"] = regime_label

    is_empty = (
        cards["total"] == 0
        and not payload["signals"]["events"]
    )

    # One-line tweet copy — kept here so the page header, the OG image,
    # and the cron job all surface identical wording (no risk of the
    # tweet promising numbers the receipt page can't show).
    best = payload["signals"]["best"]
    worst = payload["signals"]["worst"]

    def _fmt_pct(value: float | None) -> str:
        if value is None:
            return "—"
        pct = value * 100
        sign = "+" if pct >= 0 else "−"
        return f"{sign}{abs(pct):.2f}%"

    parts: list[str] = [f"{sym} · {day.isoformat()}"]
    if cards["total"]:
        parts.append(f"{cards['total']} Playbook call{'s' if cards['total'] != 1 else ''}")
    if best:
        parts.append(
            f"Best: {_humanize_signal_name(best['name'])} {_fmt_pct(best['avg_directional_return'])}"
        )
    if worst and (not best or worst["name"] != best["name"]):
        parts.append(
            f"Worst: {_humanize_signal_name(worst['name'])} {_fmt_pct(worst['avg_directional_return'])}"
        )
    if regime_label != "unknown":
        parts.append(f"Regime: {regime_label}")

    if len(parts) == 1:  # Only the symbol/date header — nothing to say.
        tweet_text = (
            f"{sym} · {day.isoformat()} — quiet tape. No Playbook calls, no signal flips."
        )
    else:
        tweet_text = " — ".join([parts[0], ". ".join(parts[1:]) + "."])

    return {
        "date": day.isoformat(),
        "symbol": sym,
        "tz": "America/New_York",
        "horizon_minutes": horizon_minutes,
        "cards": cards,
        "signals": payload["signals"],
        "regime": regime,
        "tweet_text": tweet_text,
        "is_empty": is_empty,
    }
