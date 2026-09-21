"""Intraday re-anchored cone — read API.

Three surfaces, in rising order of how much they matter.

``/session`` is the chart: a session's worth of cones, each already carrying
its verdict once its horizon matured.

``/latest`` is the live read: the most recent fire, the one a trader is
actually looking at.

``/reliability`` is the receipt, and it is the reason the rest exists.  A
cone that says 73% is worth nothing until somebody can check whether it holds
73% of the time, and a Brier score alone cannot answer that — a model that
never strays from the middle of the distribution scores respectably while
telling you nothing.  A reliability table cannot hide: it shows what actually
happened inside each confidence band, with the sample size behind it.

The endpoint publishes the baseline the cone has to beat (always predict the
base rate) alongside the cone's own score, and says plainly when the cone
loses.  That is the same bar the daily expected-volatility call is held to,
with the same consequence — published, marked, and not counted as a win.

This router only READS.  The writer and grader live in
``src.jobs.intraday_cone_writer`` and ``src.jobs.intraday_cone_receipt``.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from zoneinfo import ZoneInfo

from src.jobs.intraday_cone_model import (
    CONE_HORIZONS_MIN,
    base_rate_brier,
    calibration_error,
    reliability_table,
)

from ..database import DatabaseManager
from .trade_signals import get_db

router = APIRouter(prefix="/api/cone", tags=["Intraday Cone"])

ET = ZoneInfo("America/New_York")

#: Minimum graded claims before a hit rate or a calibration number is
#: published at all.  Mirrors the daily card's MIN_SCORED_FOR_RATES: with a
#: handful of samples a bucket can only read 0% or 100%, which looks like
#: precision and is noise.  The cone accumulates ~20 fires x 4 horizons a day,
#: so this clears within a session or two rather than a quarter.
MIN_GRADED_FOR_RATES = 40

#: Reliability buckets.  Five is a deliberate choice over ten: the table is
#: meant to be read at a glance on the page, and a 10-bucket table over a
#: 30-session window spreads the sample thin enough that half the rows carry
#: no information.
RELIABILITY_BUCKETS = 5


def _parse_session_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")


def _f(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _shape_claim(row: dict[str, Any]) -> dict[str, Any]:
    """One claim, as the page renders it."""
    return {
        "forecast_ts": row["forecast_ts"].isoformat() if row.get("forecast_ts") else None,
        "target_ts": row["target_ts"].isoformat() if row.get("target_ts") else None,
        "horizon_min": int(row["horizon_min"]),
        "anchor_spot": _f(row.get("anchor_spot")),
        "band_low": _f(row.get("band_low")),
        "band_high": _f(row.get("band_high")),
        "hold_prob": _f(row.get("hold_prob")),
        "sigma": _f(row.get("sigma")),
        "call_wall": _f(row.get("call_wall")),
        "put_wall": _f(row.get("put_wall")),
        "gamma_flip": _f(row.get("gamma_flip")),
        "gamma_mult": _f(row.get("gamma_mult")),
        "elapsed_min": row.get("elapsed_min"),
        "model_version": row.get("model_version"),
        # Verdict — present only once the horizon matured AND produced bars.
        # A matured-but-abandoned claim carries graded_at with held NULL, and
        # renders as "not scored" rather than as either outcome.
        "graded": row.get("graded_at") is not None,
        "held": row.get("held"),
        "window_low": _f(row.get("window_low")),
        "window_high": _f(row.get("window_high")),
        "brier": _f(row.get("brier")),
    }


def _group_by_fire(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Regroup flat claim rows into one entry per anchor.

    The chart draws a cone, not four unrelated bands, so the anchor-level
    fields (spot, the surface it was conditioned on) belong on the fire and
    the per-horizon fields on its legs.
    """
    fires: dict[Any, dict[str, Any]] = {}
    for row in rows:
        key = row["forecast_ts"]
        claim = _shape_claim(row)
        fire = fires.get(key)
        if fire is None:
            fire = {
                "forecast_ts": claim["forecast_ts"],
                "anchor_spot": claim["anchor_spot"],
                "elapsed_min": claim["elapsed_min"],
                "call_wall": claim["call_wall"],
                "put_wall": claim["put_wall"],
                "gamma_flip": claim["gamma_flip"],
                "gamma_mult": claim["gamma_mult"],
                "model_version": claim["model_version"],
                "horizons": [],
            }
            fires[key] = fire
        fire["horizons"].append(
            {
                k: claim[k]
                for k in (
                    "horizon_min", "target_ts", "band_low", "band_high",
                    "hold_prob", "sigma", "graded", "held", "window_low",
                    "window_high", "brier",
                )
            }
        )
    return [fires[k] for k in sorted(fires)]


@router.get("/session/{session_date}")
async def get_session_cones(
    session_date: str,
    symbol: str = Query(default="SPY", max_length=10),
    db: DatabaseManager = Depends(get_db),
):
    """Every cone fired on one session, grouped by anchor."""
    day = _parse_session_date(session_date)
    rows = await db.get_intraday_cones_for_session(symbol.upper(), day)
    fires = _group_by_fire(rows)
    graded = [c for c in rows if c.get("held") is not None]
    return {
        "symbol": symbol.upper(),
        "session_date": day.isoformat(),
        "fires": fires,
        "n_fires": len(fires),
        "n_claims": len(rows),
        "n_graded": len(graded),
        "n_held": sum(1 for c in graded if c["held"]),
    }


@router.get("/latest")
async def get_latest_cone(
    symbol: str = Query(default="SPY", max_length=10),
    session_date: Optional[str] = Query(default=None),
    db: DatabaseManager = Depends(get_db),
):
    """The most recent fire — what the live page draws."""
    day = _parse_session_date(session_date) if session_date else datetime.now(tz=ET).date()
    rows = await db.get_intraday_cones_for_session(symbol.upper(), day)
    if not rows:
        return {
            "symbol": symbol.upper(),
            "session_date": day.isoformat(),
            "fire": None,
            "reason": "no cone committed for this session yet",
        }
    fires = _group_by_fire(rows)
    return {
        "symbol": symbol.upper(),
        "session_date": day.isoformat(),
        "fire": fires[-1],
    }


def _score_block(pairs: list[tuple[float, bool]]) -> dict[str, Any]:
    """Brier + baseline + calibration for one set of graded claims.

    ``beats_baseline`` is the verdict that decides whether these numbers are
    presented as a track record or as context.  A cone that cannot beat "always
    predict the base rate" has demonstrated nothing about the market, only
    about the base rate, and saying so is the entire point of publishing a
    receipt rather than a testimonial.
    """
    n = len(pairs)
    if n == 0:
        return {"n": 0, "brier": None, "baseline_brier": None,
                "calibration_error": None, "beats_baseline": None,
                "reliability": []}

    brier = round(sum((p - (1.0 if h else 0.0)) ** 2 for p, h in pairs) / n, 6)
    baseline = base_rate_brier(pairs)
    enough = n >= MIN_GRADED_FOR_RATES
    return {
        "n": n,
        "hold_rate": round(sum(1 for _, h in pairs if h) / n, 4),
        "mean_predicted": round(sum(p for p, _ in pairs) / n, 4),
        "brier": brier,
        "baseline_brier": baseline,
        # Lower Brier is better, so the cone wins by scoring BELOW the
        # baseline.  Withheld entirely until the sample can support it.
        "beats_baseline": (
            None if not enough or baseline is None else bool(brier < baseline)
        ),
        "calibration_error": calibration_error(pairs, buckets=RELIABILITY_BUCKETS),
        "reliability": reliability_table(pairs, buckets=RELIABILITY_BUCKETS),
        "sufficient_sample": enough,
        "min_sample": MIN_GRADED_FOR_RATES,
    }


@router.get("/reliability")
async def get_reliability(
    symbol: str = Query(default="SPY", max_length=10),
    window: int = Query(default=30, ge=1, le=180, description="Trailing sessions."),
    db: DatabaseManager = Depends(get_db),
):
    """The receipt: does a published hold probability mean what it says?

    Returns an overall block plus one per horizon, each carrying the Brier
    score, the base-rate baseline it must beat, the sample-weighted
    calibration error, and the reliability table behind those numbers.

    Per-horizon matters as much as overall.  The term structure is the part
    of this model with an explicitly empirical constant in it
    (``CONE_TERM_DECAY``), so a systematic miss concentrated in the +2h bucket
    is the signal that the constant is wrong — and it is visible here before
    anyone has to argue about it.
    """
    sym = symbol.upper()
    since = datetime.now(tz=ET).date() - timedelta(days=window * 2)
    rows = await db.get_graded_cone_history(sym, since)

    # The DB window is in calendar days; trim to the requested number of
    # SESSIONS so a long weekend or a holiday cannot shorten the sample.
    sessions = sorted({r["session_date"] for r in rows})[-window:]
    keep = set(sessions)
    rows = [r for r in rows if r["session_date"] in keep]

    def _pairs(subset) -> list[tuple[float, bool]]:
        return [
            (float(r["hold_prob"]), bool(r["held"]))
            for r in subset
            if r.get("hold_prob") is not None and r.get("held") is not None
        ]

    by_horizon = {}
    for h in CONE_HORIZONS_MIN:
        by_horizon[str(h)] = _score_block(
            _pairs([r for r in rows if int(r["horizon_min"]) == h])
        )

    return {
        "symbol": sym,
        "window_sessions": window,
        "sessions_covered": len(sessions),
        "first_session": sessions[0].isoformat() if sessions else None,
        "last_session": sessions[-1].isoformat() if sessions else None,
        "overall": _score_block(_pairs(rows)),
        "by_horizon": by_horizon,
        # Stated on the response rather than only in the page copy, so the
        # meaning of "held" travels with the number to any consumer.
        "definition": (
            "held = price never left the committed band at any point during "
            "the window (forecast_ts, target_ts]. A path that pierced the band "
            "and closed back inside did not hold."
        ),
    }
