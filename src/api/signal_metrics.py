"""Helpers for turning raw signal scores into calibrated, actionable metrics."""

from __future__ import annotations

from typing import Any, Optional


def _score_direction(score: float) -> int:
    if score > 0:
        return 1
    if score < 0:
        return -1
    return 0


def classify_regime(components: Optional[dict[str, Any]]) -> str:
    """Classify dealer gamma regime from the stored gex_regime component.

    Dealer regime depends on the sign of raw Net GEX:
      net_gex > 0  -> dealers long gamma  -> "long_gamma"
      net_gex < 0  -> dealers short gamma -> "short_gamma"

    The scoring engine persists the component output as ``{"weight", "score"}``
    where ``score = -tanh(net_gex / norm)`` (see
    ``src/signals/components/gex_regime.py``). The negation means the sign of
    the stored score is the OPPOSITE of the sign of net_gex, so regime must
    invert when classifying from ``score``.

    The legacy ``value`` key held raw net_gex directly, so it is interpreted
    with the natural sign convention.
    """
    if not isinstance(components, dict):
        return "unknown"

    gex_component = components.get("gex_regime")
    if not isinstance(gex_component, dict):
        return "unknown"

    if "score" in gex_component:
        try:
            score = float(gex_component["score"])
        except (TypeError, ValueError):
            return "unknown"
        if score < 0:
            return "long_gamma"
        if score > 0:
            return "short_gamma"
        return "neutral_gamma"

    if "value" in gex_component:
        try:
            net_gex = float(gex_component["value"])
        except (TypeError, ValueError):
            return "unknown"
        if net_gex > 0:
            return "long_gamma"
        if net_gex < 0:
            return "short_gamma"
        return "neutral_gamma"

    return "unknown"


def calibrate_signal(
    *,
    current_composite: float,
    current_normalized: float,
    current_regime: str,
    history_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Estimate directional hit-rate and expected edge from recent history."""
    direction = _score_direction(current_composite)
    if direction == 0:
        return {
            "sample_size": 0,
            "hit_rate": None,
            "expected_move_bp": None,
            "confidence": 0.0,
            "action": "wait",
            "calibration_scope": "neutral_signal",
        }

    normalized = max(0.0, min(float(current_normalized), 1.0))
    width = 0.12

    prepared: list[dict[str, float | str]] = []
    for row in history_rows:
        try:
            hist_score = float(row["composite_score"])
            fwd_ret = float(row["fwd_return"])
            hist_norm = abs(hist_score)
            hist_dir = _score_direction(hist_score)
            regime = str(row.get("regime") or "unknown")
        except (TypeError, ValueError, KeyError):
            continue
        if hist_dir == 0:
            continue
        prepared.append(
            {
                "dir": hist_dir,
                "norm": hist_norm,
                "ret": fwd_ret,
                "regime": regime,
            }
        )

    def _filter(rows: list[dict[str, float | str]], *, by_regime: bool, by_norm: bool) -> list[dict[str, float | str]]:
        out = [r for r in rows if int(r["dir"]) == direction]
        if by_regime:
            out = [r for r in out if str(r["regime"]) == current_regime]
        if by_norm:
            out = [r for r in out if abs(float(r["norm"]) - normalized) <= width]
        return out

    sample = _filter(prepared, by_regime=True, by_norm=True)
    scope = "regime+strength"
    if len(sample) < 40:
        sample = _filter(prepared, by_regime=True, by_norm=False)
        scope = "regime_only"
    if len(sample) < 40:
        sample = _filter(prepared, by_regime=False, by_norm=False)
        scope = "direction_only"

    if not sample:
        return {
            "sample_size": 0,
            "hit_rate": None,
            "expected_move_bp": None,
            "confidence": 0.0,
            "action": "wait",
            "calibration_scope": "insufficient_history",
        }

    directional_returns = [float(r["ret"]) * direction for r in sample]
    hits = [1.0 if x > 0 else 0.0 for x in directional_returns]

    hit_rate = sum(hits) / len(hits)
    expected_move_bp = (sum(directional_returns) / len(directional_returns)) * 10_000

    edge_strength = max(0.0, min(1.0, abs(hit_rate - 0.5) * 2.0))
    sample_quality = max(0.0, min(1.0, len(sample) / 150.0))
    score_quality = 0.5 + (normalized * 0.5)
    confidence = round(edge_strength * sample_quality * score_quality, 4)

    if hit_rate >= 0.57 and expected_move_bp >= 6 and normalized >= 0.58:
        action = "enter"
    elif hit_rate >= 0.53 and expected_move_bp >= 2:
        action = "watch"
    else:
        action = "wait"

    return {
        "sample_size": len(sample),
        "hit_rate": round(hit_rate, 4),
        "expected_move_bp": round(expected_move_bp, 2),
        "confidence": confidence,
        "action": action,
        "calibration_scope": scope,
    }
