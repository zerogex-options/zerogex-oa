"""Pure calculation helpers for the cross-symbol Market Tide metric."""

from __future__ import annotations

import math
from datetime import datetime, time, timedelta, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")


def _inputs_are_fresh(
    gex_timestamp: datetime | None,
    flow_timestamp: datetime | None,
    anchor: datetime,
    freshness: timedelta,
) -> bool:
    """Validate aligned inputs while preserving the last completed session.

    During RTH every symbol must track the market-wide anchor. After the cash
    session, index chains legitimately stop while ETF chains can keep updating;
    same-session, mutually aligned snapshots remain publishable.
    """
    if gex_timestamp is None or flow_timestamp is None:
        return False
    anchor_et = anchor.astimezone(_ET)
    gex_et = gex_timestamp.astimezone(_ET)
    flow_et = flow_timestamp.astimezone(_ET)
    if gex_et.date() != anchor_et.date() or flow_et.date() != anchor_et.date():
        return False
    if abs(gex_timestamp - flow_timestamp) > freshness:
        return False
    if time(9, 30) <= anchor_et.time() <= time(16, 15):
        cutoff = anchor - freshness
        return gex_timestamp >= cutoff and flow_timestamp >= cutoff
    return True


def _bounded_ratio(value: float, scale: float) -> float:
    """Normalize an unbounded value without allowing a single outlier to dominate."""
    if scale <= 0:
        return 0.0
    return math.tanh(value / scale)


def _capped_weights(raw_weights: list[float], cap: float = 0.15) -> list[float]:
    """Return normalized weights with a hard per-name cap when feasible.

    A cap cannot be satisfied when there are fewer than ``ceil(1 / cap)``
    names. In that case equal weights are the least concentrated result.
    """
    count = len(raw_weights)
    if not count:
        return []
    if count * cap < 1.0:
        return [1.0 / count] * count

    remaining = set(range(count))
    weights = [0.0] * count
    remaining_mass = 1.0
    while remaining:
        total = sum(max(raw_weights[i], 0.0) for i in remaining)
        proposed = {
            i: (
                remaining_mass / len(remaining)
                if total <= 0
                else remaining_mass * raw_weights[i] / total
            )
            for i in remaining
        }
        capped = [i for i, weight in proposed.items() if weight > cap]
        if not capped:
            for i, weight in proposed.items():
                weights[i] = weight
            break
        for i in capped:
            weights[i] = cap
            remaining.remove(i)
            remaining_mass -= cap
    return weights


def calculate_market_tide(
    rows: Iterable[dict[str, Any]],
    *,
    anchor: datetime,
    freshness_minutes: int = 10,
    minimum_participation: float = 0.60,
) -> dict[str, Any]:
    """Build an explainable -100..100 market-wide flow/gamma reading.

    Direction comes from signed option premium. Dealer gamma is deliberately
    used only as an amplifier/dampener: positive gamma suppresses the flow
    signal and negative gamma strengthens it.
    """
    if anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=timezone.utc)
    freshness = timedelta(minutes=freshness_minutes)
    supplied = list(rows)
    components: list[dict[str, Any]] = []
    stale_symbols: list[str] = []

    for row in supplied:
        symbol = str(row["symbol"])
        gex_ts = row.get("gex_timestamp")
        flow_ts = row.get("flow_timestamp")
        if not _inputs_are_fresh(gex_ts, flow_ts, anchor, freshness):
            stale_symbols.append(symbol)
            continue

        signed_premium = float(row.get("signed_premium") or 0.0)
        flow_scale = float(row.get("flow_p95") or 0.0)
        gamma = float(row.get("net_gex_at_spot") or 0.0)
        gamma_scale = float(row.get("gamma_p95") or 0.0)
        flow_score = _bounded_ratio(signed_premium, flow_scale)
        gamma_score = _bounded_ratio(gamma, gamma_scale)
        amplifier = max(0.5, min(1.5, 1.0 - 0.5 * gamma_score))
        components.append(
            {
                "symbol": symbol,
                "flow_score": flow_score,
                "gamma_score": gamma_score,
                "amplifier": amplifier,
                "pressure": flow_score * amplifier,
                "raw_weight": math.sqrt(max(float(row.get("gross_premium") or 0.0), 0.0)),
            }
        )

    weights = _capped_weights([item["raw_weight"] for item in components])
    for item, weight in zip(components, weights):
        item["weight"] = weight
        item["contribution"] = item["pressure"] * weight

    configured = len(supplied)
    participation = len(components) / configured if configured else 0.0
    score = 100.0 * sum(item["contribution"] for item in components)
    flow_direction = sum(item["flow_score"] * item["weight"] for item in components)
    gamma_regime = sum(item["gamma_score"] * item["weight"] for item in components)
    threshold = 0.15
    eligible = len(components)
    bullish = sum(item["flow_score"] > threshold for item in components)
    bearish = sum(item["flow_score"] < -threshold for item in components)

    if participation < minimum_participation:
        label = "insufficient_data"
        published_score = None
    else:
        published_score = round(max(-100.0, min(100.0, score)), 2)
        magnitude = abs(score)
        direction = "bullish" if score > 0 else "bearish" if score < 0 else "neutral"
        label = f"strong_{direction}" if magnitude >= 60 and direction != "neutral" else direction

    gamma_label = (
        "amplifying" if gamma_regime < -0.15 else "dampening" if gamma_regime > 0.15 else "neutral"
    )
    ranked = sorted(components, key=lambda item: item["contribution"], reverse=True)
    public_fields = ("symbol", "flow_score", "gamma_score", "amplifier", "weight", "contribution")

    def public(item: dict[str, Any]) -> dict[str, Any]:
        return {
            key: round(float(item[key]), 6) if key != "symbol" else item[key]
            for key in public_fields
        }

    return {
        "timestamp": anchor,
        "score": published_score,
        "label": label,
        "flow_direction": round(flow_direction, 6),
        "gamma_regime": round(gamma_regime, 6),
        "gamma_label": gamma_label,
        "bullish_breadth_pct": round(100.0 * bullish / eligible, 2) if eligible else 0.0,
        "bearish_breadth_pct": round(100.0 * bearish / eligible, 2) if eligible else 0.0,
        "neutral_breadth_pct": (
            round(100.0 * (eligible - bullish - bearish) / eligible, 2) if eligible else 0.0
        ),
        "participation_pct": round(100.0 * participation, 2),
        "eligible_symbols": eligible,
        "configured_symbols": configured,
        "stale_symbols": sorted(stale_symbols),
        "leaders": [public(item) for item in ranked[:5] if item["contribution"] > 0],
        "laggards": [public(item) for item in reversed(ranked[-5:]) if item["contribution"] < 0],
    }
