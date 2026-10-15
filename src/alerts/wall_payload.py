"""What a wall alert says: the JSON every channel renders from.

The detector decides *that* something happened; this module decides what a
member is told about it, as one structured payload stored on the event row.
The website renders the in-app card, the email and (later) the SMS from this
same object, so the three can never disagree about the wall, the price or how
strong the wall was.

Pure: no database, no clock, no configuration.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.alerts.wall_detector import WallEvent

#: Bumped when a field changes meaning; renderers key their parsing off it.
PAYLOAD_VERSION = 1

# Strength tiers, by the wall's percentile against the symbol's own 30-day,
# time-of-day-bucketed distribution of wall sizes (gex_historical_stats). The
# floors are the stops of the member-facing strength slider, so "Strong" in an
# alert means exactly what "Strong" meant when the member set the filter.
STRENGTH_TIERS: Tuple[Tuple[float, str], ...] = (
    (90.0, "very_strong"),
    (75.0, "strong"),
    (50.0, "above_typical"),
    (25.0, "moderate"),
    (0.0, "light"),
)


def strength_tier(pctile: Optional[float]) -> Optional[str]:
    """The tier key for a percentile, or ``None`` when it is unknown."""
    if pctile is None:
        return None
    for floor, key in STRENGTH_TIERS:
        if pctile >= floor:
            return key
    return STRENGTH_TIERS[-1][1]


def regime_for(net_gex_at_spot: Optional[float]) -> Optional[str]:
    """Dealer gamma at spot as a sign: ``positive`` / ``negative`` / ``None``.

    Positive dealer gamma means hedging leans against moves (walls tend to
    hold); negative means it leans with them (walls give way more easily).
    Exactly zero, or unknown, says nothing and is reported as ``None``.
    """
    if net_gex_at_spot is None:
        return None
    if net_gex_at_spot > 0:
        return "positive"
    if net_gex_at_spot < 0:
        return "negative"
    return None


def _iso(value: Any) -> Optional[str]:
    return value.isoformat() if value is not None else None


def build_payload(
    event: WallEvent,
    *,
    strength_pctile: Optional[float],
    net_gex_at_spot: Optional[float],
    current_wall: Optional[float],
    odds: Sequence[Dict[str, Any]] = (),
) -> Dict[str, Any]:
    """The stored, rendered description of one wall event.

    :param strength_pctile: the wall's size percentile at the hit (``None``
        when the symbol has no history yet).
    :param net_gex_at_spot: dealer gamma at spot from the walls row in force
        when the event fired.
    :param current_wall: the side's published wall as the event fires. After
        a break it is the NEXT wall out, which is what a member wants next.
    :param odds: reportable ``wall_break_stats`` rows for the symbol and side,
        each ``{horizon_minutes, break_prob, ci_low, ci_high, n_tests}``.
    """
    distance = event.wall - event.price if event.side == "call" else event.price - event.wall
    return {
        "v": PAYLOAD_VERSION,
        "symbol": event.symbol,
        "side": event.side,
        "kind": event.kind,
        "wall": event.wall,
        "price": event.price,
        "last_price": event.last_price,
        # Signed toward the approach side: >0 still short of the wall, <0 past it.
        "distance": distance,
        "bar_ts": _iso(event.bar_ts),
        "session_date": event.session_date.isoformat(),
        "hit_ts": _iso(event.hit_ts),
        "hit_price": event.hit_price,
        "minutes_since_hit": round(event.minutes_since_hit, 1),
        "peak": event.peak,
        "ordinal": event.ordinal,
        "approach_number": event.approach_number,
        "alerted_hit": event.alerted_hit,
        "profile": event.profile,
        # The touch tolerance this alert's stream ran at, in basis points: the
        # profile's own band, or the one the member chose.
        "touch_bp": event.touch_bp,
        "touch_band": event.touch_band,
        "break_buffer": event.break_buffer,
        "hold_distance": event.hold_distance,
        "confirm_minutes": event.confirm_minutes,
        "strength": event.strength,
        "strength_pctile": None if strength_pctile is None else round(strength_pctile, 1),
        "strength_tier": strength_tier(strength_pctile),
        "regime": regime_for(net_gex_at_spot),
        "current_wall": current_wall,
        "odds": [dict(o) for o in odds],
    }


def reportable_odds(
    rows: Sequence[Dict[str, Any]], horizons: Sequence[int]
) -> List[Dict[str, Any]]:
    """Keep the ``wall_break_stats`` rows an alert may quote.

    Only rows the nightly refresh marked reportable, at the requested
    horizons, with a probability present. An unreportable figure is hidden,
    never rounded into something that looks measured.
    """
    wanted = set(int(h) for h in horizons)
    out: List[Dict[str, Any]] = []
    for row in rows:
        try:
            horizon = int(row.get("horizon_minutes"))
        except (TypeError, ValueError):
            continue
        if horizon not in wanted or not row.get("reportable"):
            continue
        prob = row.get("break_prob")
        if prob is None:
            continue
        out.append(
            {
                "horizon_minutes": horizon,
                "break_prob": float(prob),
                "ci_low": None if row.get("ci_low") is None else float(row["ci_low"]),
                "ci_high": None if row.get("ci_high") is None else float(row["ci_high"]),
                "n_tests": None if row.get("n_tests") is None else int(row["n_tests"]),
            }
        )
    out.sort(key=lambda o: o["horizon_minutes"])
    return out
