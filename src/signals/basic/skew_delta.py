"""Skew delta scoring component.

Short-dated option skew is one of the purest real-time fear gauges: when
puts bid up relative to calls of the same moneyness, it means market
participants are paying up for downside protection — and doing it
*before* the tape confirms bearishness.

This component computes an OTM put vs OTM call implied-volatility
differential, normalizes it against a configurable baseline, and scores
so that elevated put skew is bearish.

Inputs come from ``ctx.extra['skew']`` — a dict with
``otm_put_iv`` and ``otm_call_iv`` populated by the unified signal
engine from the ``option_chains`` table. When the data isn't available
the component returns 0 (abstain).
"""

from __future__ import annotations

import os

from src.signals.components.base import ComponentBase, MarketContext

# Baseline IV spread (put_iv - call_iv) that counts as "neutral" skew.
# Equity index skew is structurally positive — OTM puts always trade
# richer than OTM calls. This baseline lets us measure *deviation from
# normal* rather than the raw spread.
_SKEW_BASELINE = float(os.getenv("SIGNAL_SKEW_BASELINE", "0.02"))

# Spread magnitude beyond (baseline + this delta) saturates the score.
_SKEW_SATURATION = float(os.getenv("SIGNAL_SKEW_SATURATION", "0.04"))

# Which strikes count as "the OTM put" and "the OTM call".
#
# By DELTA, not by percent of spot. This used to sample 2-5% OTM, a
# convention borrowed from ~30 DTE equity options, against a chain that
# ingests 0-2 DTE. The two do not survive contact:
#
#   * The ingested chain reaches +/-1.29% on SPX and +/-1.71% on NDX (the
#     strike-count cap binds long before the 3% range does), so on both
#     index underlyings the 2-5% band contained NO ingested strike and the
#     component had never once produced a non-abstain reading.
#   * Where it did reach -- SPY and QQQ -- it was sampling the wrong thing.
#     At 0.5 DTE a 2% OTM SPY put is past 15 delta by a factor of four; its
#     IV is a tail artefact and its quote is a penny wide.
#
# Delta is tenor-invariant by construction, which is the property this
# signal needs. Measured against the repo's own Black-Scholes at production
# tenors and IVs, the 25 delta strike sits 0.29%-0.93% from spot on every
# underlying -- comfortably inside the chain on all four, SPX included.
#
# 25 delta is the risk-reversal convention; the band either side of it is
# what makes the sample robust to one bad IV solve on a discrete ladder.
_SKEW_DELTA_TARGET = float(os.getenv("SIGNAL_SKEW_DELTA_TARGET", "0.25"))
_SKEW_DELTA_BAND = float(os.getenv("SIGNAL_SKEW_DELTA_BAND", "0.10"))

# NOT YET CALIBRATED AGAINST THIS SELECTION, and deliberately left that way.
#
# _SKEW_BASELINE and _SKEW_SATURATION above predate the move to delta and
# were never fitted to data under either selection. Changing which strikes
# are sampled changes the distribution of `spread`, so the honest position
# is that the level of this score is unverified -- for the same reason it
# was unverified before, not a new one. If anything 25 delta should sit
# CLOSER to a 2-vol-point baseline than the old 2-5% band did, since that
# band sampled the tail where the smile is steepest; but that is an
# argument, not a measurement, and nothing here should be read as one.
#
# The measurement is now possible without new machinery.
# context_values() publishes otm_put_iv, otm_call_iv, spread, the sampled
# contract counts and their mean |delta| on every cycle, and those land in
# signal_component_scores.context_values. One RTH session of stored rows
# gives the per-underlying distribution of `spread`; the baseline wants its
# median and the saturation something like its interquartile width.
#
# Expect them to differ per underlying -- SPX index skew is structurally
# richer than SPY's -- which a single global constant cannot express. If
# that turns out to matter, component_normalizer_cache already exists for
# exactly this (see normalizer_cache_refresh.py) and is already wired into
# ctx.extra["normalizers"]; it was not used here because a normalizer
# fitted to no data is just a constant with more moving parts.
#
# Calibrate BOTH copies together: src/signals/advanced/range_break_imminence.py
# carries its own _SKEW_BASELINE / _SKEW_SATURATION (SIGNAL_RBI_*, same
# 0.02 / 0.04 defaults) reading the same two IVs, and weights its skew
# sub-score at 30 -- so a miscalibration costs far more there than it does
# at this component's weight of 0.04.


def delta_band() -> tuple[float, float]:
    """``(low, high)`` absolute-delta bounds for the sampled strikes.

    Symmetric on purpose: a risk reversal compares like for like, and
    sampling puts at one delta against calls at another would put the
    difference between those two deltas into the spread and call it skew.
    """
    lo = max(0.01, _SKEW_DELTA_TARGET - _SKEW_DELTA_BAND)
    hi = min(0.99, _SKEW_DELTA_TARGET + _SKEW_DELTA_BAND)
    return lo, hi


class SkewDeltaComponent(ComponentBase):
    name = "skew_delta"
    weight = 0.04

    def compute(self, ctx: MarketContext) -> float:
        spread = self._spread(ctx)
        if spread is None:
            return 0.0
        deviation = spread - _SKEW_BASELINE
        # Elevated put skew (positive deviation) = bearish.
        normalized = max(-1.0, min(1.0, deviation / _SKEW_SATURATION))
        return -normalized

    def context_values(self, ctx: MarketContext) -> dict:
        spread = self._spread(ctx)
        skew_info = (ctx.extra or {}).get("skew") or {}
        lo, hi = delta_band()
        return {
            "otm_put_iv": skew_info.get("otm_put_iv"),
            "otm_call_iv": skew_info.get("otm_call_iv"),
            "spread": round(spread, 6) if spread is not None else None,
            "baseline": _SKEW_BASELINE,
            "deviation": round(spread - _SKEW_BASELINE, 6) if spread is not None else None,
            # compute() answers 0.0 for "skew is exactly at baseline" and for
            # "there was nothing to measure", because ComponentBase defines
            # 0.0 as both. The score cannot tell them apart; this can, and
            # anything reading the score is expected to consult it. Published
            # for the same reason gex_gradient publishes wing_window_reached.
            "skew_available": spread is not None,
            # What was actually sampled, so the selection can be audited from
            # stored rows rather than re-derived: how many contracts on each
            # side, their mean |delta| against the band that asked for them,
            # and how far from spot that landed.
            "delta_band": [lo, hi],
            "put_contracts": skew_info.get("put_contracts"),
            "call_contracts": skew_info.get("call_contracts"),
            "put_abs_delta": skew_info.get("put_abs_delta"),
            "call_abs_delta": skew_info.get("call_abs_delta"),
            "put_moneyness_pct": skew_info.get("put_moneyness_pct"),
            "call_moneyness_pct": skew_info.get("call_moneyness_pct"),
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _spread(ctx: MarketContext) -> float | None:
        skew_info = (ctx.extra or {}).get("skew")
        if not isinstance(skew_info, dict):
            return None
        put_iv = skew_info.get("otm_put_iv")
        call_iv = skew_info.get("otm_call_iv")
        if put_iv is None or call_iv is None:
            return None
        try:
            return float(put_iv) - float(call_iv)
        except (TypeError, ValueError):
            return None
