"""Helpers for opportunity-quality direction inference.

The opportunity-quality component intentionally decouples directional
inference from strict ``AND`` quadrants (net_gex sign AND smart-money sign).
This module provides a soft-vote inference used by both the component and
other callers that need a market-side hint without hard gating.
"""

from __future__ import annotations


def infer_opportunity_direction(
    *,
    net_gex: float,
    smart_call: float,
    smart_put: float,
    close: float,
    gamma_flip: float | None,
) -> tuple[str, float, dict]:
    """Infer direction from a weighted mix of flow and regime inputs.

    Returns ``(direction, confidence, diagnostics)`` where:
      * direction: bullish / bearish / neutral
      * confidence: [0, 1]
      * diagnostics: input breakdown for observability
    """
    sm_ratio = (float(smart_call) + 1.0) / (float(smart_put) + 1.0)
    flow_bias = 0.0
    if sm_ratio > 1.0:
        flow_bias = min((sm_ratio - 1.0) / 0.75, 1.0)
    elif sm_ratio < 1.0:
        flow_bias = -min((1.0 - sm_ratio) / 0.75, 1.0)

    # GEX sign controls stability/amplification, not direction. Keep this as a
    # non-directional modifier that scales confidence elsewhere; directional
    # anchor comes from flow + price structure.
    gex_stability = 1.0 if float(net_gex) >= 0 else -1.0
    flip_bias = 0.0
    if gamma_flip is not None and close > 0:
        if close > gamma_flip:
            flip_bias = 0.35
        elif close < gamma_flip:
            flip_bias = -0.35

    aggregate = (0.70 * flow_bias) + (0.30 * flip_bias)
    confidence = min(abs(aggregate), 1.0)
    if aggregate >= 0.15:
        direction = "bullish"
    elif aggregate <= -0.15:
        direction = "bearish"
    else:
        direction = "neutral"

    diagnostics = {
        "sm_ratio": round(sm_ratio, 6),
        "flow_bias": round(flow_bias, 6),
        "gex_stability": gex_stability,
        "flip_bias": round(flip_bias, 6),
        "aggregate": round(aggregate, 6),
    }
    return direction, round(confidence, 6), diagnostics
