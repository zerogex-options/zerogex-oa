"""Spectrum guarantees for signal scores.

Every signal in the stack lives on the [-1, +1] axis with 0 conventionally
meaning "neutral / insufficient data".  Operationally the user wants 0 to
be a near-impossible extreme, not the abstain case — so this module
provides two helpers that the engines apply uniformly:

  * :func:`regime_tilt` — derives a small, signed score from globally
    available ``MarketContext`` fields (close vs gamma_flip, put-call
    ratio, net GEX sign).  Used as the abstain fallback so signals
    without primary data still report a regime-flavored bias.

  * :func:`ensure_non_zero` — wraps a clamped score; if it sits within
    ``_ABSTAIN_THRESHOLD`` of zero it gets replaced with the tilt.

Both helpers are pure functions of context — they do not introduce
randomness, so a deterministic input produces a deterministic output.
"""

from __future__ import annotations

import math

from src.signals.components.base import MarketContext

# Below this magnitude a score is treated as "abstaining" — we replace
# it with a regime-derived bias so the response stays on a spectrum.
_ABSTAIN_THRESHOLD = 1e-3

# Default fallback magnitude for individual signals.  Keeps the tilt
# small enough that genuine market reads dominate, large enough that
# the score visibly differs from 0.
_DEFAULT_TILT_MAGNITUDE = 0.10

# Net-GEX magnitude (industry-standard $ gamma per 1% move) at which the
# tanh tilt approaches saturation.  Same calibration as net_gex_sign so
# the fallback agrees with the dedicated component.
_NET_GEX_SCALE = 2.0e9

# Minimum non-zero magnitude returned even when no primary cues are
# available.  A fixed, sign-neutral floor (NOT derived from price) so a
# stuck regime never lands at literal zero yet also never fabricates a
# price-level-dependent direction.
_LAST_RESORT_MIN = 0.01


def regime_tilt(
    ctx: MarketContext,
    magnitude: float = _DEFAULT_TILT_MAGNITUDE,
) -> float:
    """Compute a small non-zero score from globally-available context.

    Returns a value in ``[-magnitude, +magnitude]``.  Combines three
    weak-but-orthogonal directional cues:

      * Close vs gamma_flip (above flip = bullish)
      * Put-call ratio (PCR > 1 = bearish)
      * Net GEX sign (negative GEX = bullish-vol regime)

    Falls back to a fixed, sign-neutral tiny floor if none of the cues
    are populated, so the result is never exactly zero without inventing
    a price-derived direction.
    """
    parts: list[float] = []

    flip = ctx.gamma_flip
    close = ctx.close
    if flip is not None and close and close > 0:
        try:
            rel = (close - float(flip)) / close
            parts.append(math.tanh(rel * 50.0))
        except (TypeError, ZeroDivisionError):
            pass

    pcr = ctx.put_call_ratio
    if pcr is not None:
        try:
            pcr_f = float(pcr)
            if pcr_f > 0:
                parts.append(math.tanh((1.0 - pcr_f) * 2.0))
        except (TypeError, ValueError):
            pass

    net_gex = ctx.net_gex
    if net_gex is not None:
        try:
            parts.append(math.tanh(-float(net_gex) / _NET_GEX_SCALE))
        except (TypeError, ValueError):
            pass

    if not parts:
        # No directional cues at all. The previous implementation derived
        # a *signed* tilt from ``close % 7.0`` -- that fabricates a
        # direction that swings with the absolute price level (SPX 5103.5
        # -> strongly bearish, 5104.5 -> less bearish) and feeds spurious,
        # price-quantized votes into the MSI composite. With genuinely no
        # information the only honest output is a sign-neutral, constant,
        # negligible floor that satisfies the "never exactly zero" contract
        # without encoding a fake market read.
        return min(_LAST_RESORT_MIN, magnitude)

    avg = sum(parts) / len(parts)
    tilt = avg * magnitude
    # Guarantee strictly non-zero output even when the cues happen to
    # cancel out exactly.
    if abs(tilt) < _LAST_RESORT_MIN:
        sign = 1.0 if tilt >= 0 else -1.0
        tilt = sign * _LAST_RESORT_MIN
    return max(-magnitude, min(magnitude, tilt))


def ensure_non_zero(
    score: float,
    ctx: MarketContext,
    magnitude: float = _DEFAULT_TILT_MAGNITUDE,
) -> float:
    """Replace abstain-like scores with :func:`regime_tilt`.

    Scores with ``|score| >= _ABSTAIN_THRESHOLD`` pass through unchanged.
    """
    if abs(score) >= _ABSTAIN_THRESHOLD:
        return score
    return regime_tilt(ctx, magnitude=magnitude)
