"""Generic swing bounce / rejection detector.

The only wick-based reversal detector in the codebase (``_detect_bounce`` in
gamma_flip_bounce) is hard-wired to the gamma-flip level. This generalizes the
same shape to *any* recent swing extreme, so Trade Bias gets a price-action
pillar that answers "did we just bounce off a low / reject a high?".

Score convention (ComponentBase): +1 a sharp bounce off a recent low that has
reclaimed (bullish reversal), -1 a sharp rejection of a recent high (bearish
reversal), 0 no clear reversal / insufficient data. Uses ``recent_lows`` /
``recent_highs`` for the true wick when present, falling back to closes.
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext

# Window sizes and tolerances. A "probe" is the recent window undercutting the
# prior window's extreme; a reversal also requires the latest close to have
# reclaimed toward the opposite end of the recent range.
_RECENT_BARS = int(os.getenv("TRADE_BIAS_REVERSAL_RECENT_BARS", "5"))
_PRIOR_BARS = int(os.getenv("TRADE_BIAS_REVERSAL_PRIOR_BARS", "20"))
_PROBE_TOL_PCT = float(os.getenv("TRADE_BIAS_REVERSAL_PROBE_TOL_PCT", "0.0005"))
_REJECT_BUFFER_PCT = float(os.getenv("TRADE_BIAS_REVERSAL_REJECT_BUFFER_PCT", "0.0005"))
# How far into the recent range the close must reclaim to count (0..1 from the
# tested extreme). 0.6 = closed in the top 40% for a bounce.
_RECLAIM_POS = float(os.getenv("TRADE_BIAS_REVERSAL_RECLAIM_POS", "0.6"))


def swing_reversal_score(
    closes: list[float],
    lows: list[float] | None = None,
    highs: list[float] | None = None,
) -> float:
    usable = [c for c in (closes or []) if c and c > 0]
    n = len(usable)
    if n < _RECENT_BARS + 5:
        return 0.0

    if n > _PRIOR_BARS + _RECENT_BARS:
        prior = usable[-(_PRIOR_BARS + _RECENT_BARS) : -_RECENT_BARS]
    else:
        prior = usable[:-_RECENT_BARS]
    recent = usable[-_RECENT_BARS:]
    if len(prior) < 5:
        return 0.0

    def _tail(series: list[float] | None) -> list[float] | None:
        # Only trust the wick arrays when they align 1:1 with closes.
        if not series or len(series) != len(closes):
            return None
        return series[-_RECENT_BARS:]

    recent_lows = _tail(lows)
    recent_highs = _tail(highs)
    low_source = recent_lows if recent_lows is not None else recent
    high_source = recent_highs if recent_highs is not None else recent

    recent_low = min(low_source)
    recent_high = max(high_source)
    prior_low = min(prior)
    prior_high = max(prior)
    last = recent[-1]
    rng = recent_high - recent_low
    if rng <= 0:
        return 0.0

    # Position of the latest close within the recent range: 0 at the low, 1 at
    # the high. Bounces close near the top; rejections near the bottom.
    pos = (last - recent_low) / rng

    # Bullish bounce: recent window undercut the prior low (downside probe) and
    # the close reclaimed the upper part of the range.
    bull_probe = recent_low <= prior_low * (1.0 - _PROBE_TOL_PCT)
    if bull_probe and last > recent_low * (1.0 + _REJECT_BUFFER_PCT) and pos >= _RECLAIM_POS:
        # Strength scales with how decisively it closed off the low.
        return min(1.0, (pos - 0.5) * 2.0)

    # Bearish rejection: recent window pierced the prior high (upside probe) and
    # the close failed back into the lower part of the range.
    bear_probe = recent_high >= prior_high * (1.0 + _PROBE_TOL_PCT)
    rejected = last < recent_high * (1.0 - _REJECT_BUFFER_PCT)
    if bear_probe and rejected and pos <= (1.0 - _RECLAIM_POS):
        return -min(1.0, (0.5 - pos) * 2.0)

    return 0.0


class SwingReversalComponent(ComponentBase):
    name = "swing_reversal"
    weight = 0.0  # Not an MSI component; consumed by the Trade Bias tactical read.

    def compute(self, ctx: MarketContext) -> float:
        return swing_reversal_score(ctx.recent_closes or [], ctx.recent_lows, ctx.recent_highs)

    def context_values(self, ctx: MarketContext) -> dict:
        score = self.compute(ctx)
        return {
            "score": round(score, 6),
            "direction": "bullish" if score > 0 else "bearish" if score < 0 else "none",
            "recent_bars": _RECENT_BARS,
            "prior_bars": _PRIOR_BARS,
        }
