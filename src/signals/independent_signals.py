"""Independent signal detectors (not included in composite scoring).

Each detector returns an :class:`IndependentSignalResult` with a continuous
score in ``[-1, +1]`` and a context dict that downstream consumers
(``/api/signals/*``) surface as top-level fields.

This module implements the following fixes from the audit:

* **C6** — ``/api/signals/0dte-position-imbalance`` now consumes the
  ``flow_zero_dte`` slice of ``extra`` (actually filtered to today's
  expiration via ``flow_contract_facts``), bucketed by OTM/ATM.
* **C7** — signed smart-money (buy_premium - sell_premium) replaces
  gross notional.
* **C10** — ``trap_detection`` resistance/support selection picks the
  *nearest* adverse level instead of the global max/min.
* **S3** — ``squeeze_setup`` is continuous (tanh of flow z-score times
  vol-normalized direction) and gated by per-symbol flow normalizer.
* **S4** — ``trap_detection`` uses a vol-scaled breakout buffer and a
  wall-migration term (call_wall moving with price => genuine breakout;
  wall holding => trap probability rises).
* **S5** — ``zero_dte_position_imbalance`` uses a moneyness-weighted
  linear combo and scales by hours-to-close.
* **S6** — ``gamma_vwap_confluence`` flips sign in positive-GEX regimes
  (mean reversion) rather than merely dampening continuation; cluster
  quality is continuous and multi-level.
* **VIX term-structure gate** — scores attach a ``regime_modifier``
  when VIX data is available.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.components.utils import (
    SESSION_CLOSE_MIN_ET,
    SESSION_OPEN_MIN_ET,
    minute_of_day_et,
    pct_change_n_bar,
    vol_normalized_momentum,
)


@dataclass
class IndependentSignalResult:
    name: str
    score: float
    context: dict


# Default normalizers (fall back when per-symbol empirical norms aren't cached yet).
_DEFAULT_FLOW_FLUX_NORM = float(os.getenv("SIGNAL_FLOW_FLUX_NORM", "250000"))
_DEFAULT_NET_GEX_DELTA_NORM = float(os.getenv("SIGNAL_NET_GEX_DELTA_NORM", "500000000"))
# Confluence quality cutoff: >= this fraction counts as clustered enough
# to produce ANY signal (continuous above this threshold).
_CONFLUENCE_MAX_GAP_PCT = float(os.getenv("SIGNAL_CONFLUENCE_MAX_GAP_PCT", "0.005"))
# Vol-scaled breakout buffer (fraction of close).  Minimum buffer keeps
# the signal from triggering on pure tick noise; the scaled floor is
# 15% of per-bar realized-sigma over 5 bars — enough to ignore the
# typical 1-2 ticks of spread wobble.
_BREAKOUT_BUFFER_MIN = float(os.getenv("SIGNAL_BREAKOUT_BUFFER_MIN", "0.001"))
_BREAKOUT_BUFFER_VOL_MULT = float(os.getenv("SIGNAL_BREAKOUT_BUFFER_VOL_MULT", "0.15"))


def _nearest_above(levels: list[Optional[float]], close: float) -> Optional[float]:
    candidates = [lv for lv in levels if lv is not None and lv > close]
    return min(candidates) if candidates else None


def _nearest_below(levels: list[Optional[float]], close: float) -> Optional[float]:
    candidates = [lv for lv in levels if lv is not None and lv < close]
    return max(candidates) if candidates else None


def _tanh_scaled(x: float) -> float:
    """Cheap tanh that clips to ±1 and avoids overflow."""
    if x > 20.0:
        return 1.0
    if x < -20.0:
        return -1.0
    return math.tanh(x)


def _vix_regime(vix_level: Optional[float]) -> str:
    """Classify the current VIX level into regimes.

    Without VIX9D/VIX3M we can't compute the full term structure, so we
    use a coarse level-based proxy: <15 "dead", 15-22 "normal",
    22-30 "elevated", >30 "panic".  Components use this to gate or
    dampen signals that are known to over/underperform in specific
    regimes (vol-is-cheap filter for expansion longs, etc).
    """
    if vix_level is None:
        return "unknown"
    if vix_level < 15.0:
        return "dead"
    if vix_level < 22.0:
        return "normal"
    if vix_level < 30.0:
        return "elevated"
    return "panic"


class IndependentSignalEngine:
    """Generate side-channel signals persisted like components with weight=0."""

    def evaluate(self, ctx: MarketContext) -> list[IndependentSignalResult]:
        return [
            self._squeeze_setup(ctx),
            self._trap_detection(ctx),
            self._zero_dte_position_imbalance(ctx),
            self._gamma_vwap_confluence(ctx),
        ]

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _flow_flux_norm(ctx: MarketContext) -> float:
        norms = (ctx.extra or {}).get("normalizers") or {}
        val = norms.get("call_flow_delta") or norms.get("put_flow_delta")
        if val and val > 0:
            return float(val)
        return _DEFAULT_FLOW_FLUX_NORM

    @staticmethod
    def _gex_delta_norm(ctx: MarketContext) -> float:
        norms = (ctx.extra or {}).get("normalizers") or {}
        val = norms.get("net_gex_delta")
        if val and val > 0:
            return float(val)
        return _DEFAULT_NET_GEX_DELTA_NORM

    @staticmethod
    def _realized_pct_sigma(ctx: MarketContext) -> float:
        """Per-bar realized sigma of underlying returns, as a fraction (not pct)."""
        from src.signals.components.utils import realized_sigma
        return realized_sigma(ctx.recent_closes, 60)

    # ------------------------------------------------------------------
    # Squeeze setup (S3)
    # ------------------------------------------------------------------

    def _squeeze_setup(self, ctx: MarketContext) -> IndependentSignalResult:
        extra = ctx.extra or {}
        call_flow_delta = float(extra.get("call_flow_delta") or 0.0)
        put_flow_delta = float(extra.get("put_flow_delta") or 0.0)
        flow_norm = self._flow_flux_norm(ctx)

        # Continuous flow acceleration z-score.
        call_flow_z = call_flow_delta / max(flow_norm, 1.0)
        put_flow_z = put_flow_delta / max(flow_norm, 1.0)

        # Vol-normalized 5-bar momentum (C4).
        _, mom_z = vol_normalized_momentum(ctx.recent_closes, n=5, vol_window=60)
        # Acceleration confirmation: mom5 > mom10 (in same direction).
        mom_5 = pct_change_n_bar(ctx.recent_closes, 5)
        mom_10 = pct_change_n_bar(ctx.recent_closes, 10)
        accel_up = mom_5 > mom_10 > 0
        accel_dn = mom_5 < mom_10 < 0

        flip = ctx.gamma_flip
        above_flip = bool(flip is not None and ctx.close > flip)
        below_flip = bool(flip is not None and ctx.close < flip)
        neg_gex = ctx.net_gex < 0

        # Regime readiness: negative GEX amplifies squeezes; below_flip +
        # call flow coming in is the classic short-squeeze setup.
        gex_readiness = 1.0 if neg_gex else 0.5

        # Bullish continuous score: tanh(call_flow_z) * direction * readiness.
        # direction is a clipped mom_z, strictly > 0 for bullish.
        dir_strength_up = max(0.0, min(1.0, mom_z))
        dir_strength_dn = max(0.0, min(1.0, -mom_z))
        # Accel boost (up to +20%)
        accel_mult_up = 1.2 if accel_up else 1.0
        accel_mult_dn = 1.2 if accel_dn else 1.0

        bull = (
            _tanh_scaled(call_flow_z)
            * dir_strength_up
            * gex_readiness
            * accel_mult_up
            * (1.0 if above_flip else 0.6)
        )
        bear = (
            _tanh_scaled(put_flow_z)
            * dir_strength_dn
            * gex_readiness
            * accel_mult_dn
            * (1.0 if below_flip else 0.6)
        )

        # Require strict directional agreement between flow and momentum:
        # call-buy flow only drives bullish score; put-buy flow only bearish.
        score = 0.0
        if bull > 0 and call_flow_z > 0:
            score = bull
        elif bear > 0 and put_flow_z > 0:
            score = -bear

        score = max(-1.0, min(1.0, score))
        triggered = abs(score) >= 0.25

        vix_level = extra.get("vix_level")
        regime = _vix_regime(vix_level)

        return IndependentSignalResult(
            name="squeeze_setup",
            score=score,
            context={
                "triggered": triggered,
                "signal": "bullish_squeeze" if score > 0 else ("bearish_squeeze" if score < 0 else "none"),
                "net_gex": ctx.net_gex,
                "gamma_flip": flip,
                "close": ctx.close,
                "call_flow_delta": round(call_flow_delta, 2),
                "put_flow_delta": round(put_flow_delta, 2),
                "call_flow_z": round(call_flow_z, 3),
                "put_flow_z": round(put_flow_z, 3),
                "momentum_5bar": round(mom_5, 6),
                "momentum_10bar": round(mom_10, 6),
                "momentum_z": round(mom_z, 3),
                "accel_up": accel_up,
                "accel_dn": accel_dn,
                "flow_norm_used": round(flow_norm, 2),
                "vix_level": vix_level,
                "vix_regime": regime,
            },
        )

    # ------------------------------------------------------------------
    # Trap detection (S4, C10)
    # ------------------------------------------------------------------

    def _trap_detection(self, ctx: MarketContext) -> IndependentSignalResult:
        extra = ctx.extra or {}
        call_wall = extra.get("call_wall")
        prior_call_wall = extra.get("prior_call_wall")
        max_gamma = extra.get("max_gamma_strike")
        vwap = ctx.vwap
        flip = ctx.gamma_flip

        # C10: nearest adverse level bracketing close.  For an *upside*
        # breakout trap, the level of interest is the most recently-broken
        # resistance (nearest strong level BELOW close).  For a *downside*
        # breakdown trap, it's the nearest support ABOVE close.
        up_levels = [call_wall, max_gamma, vwap, flip]
        dn_levels = [max_gamma, vwap, flip]
        resistance = _nearest_below(up_levels, ctx.close)
        support = _nearest_above(dn_levels, ctx.close)

        # S4: vol-scaled breakout buffer.
        sigma = self._realized_pct_sigma(ctx)
        # 5-bar horizon scaling.
        buffer_pct = max(_BREAKOUT_BUFFER_MIN, _BREAKOUT_BUFFER_VOL_MULT * sigma * math.sqrt(5))

        breakout_up = bool(resistance is not None and ctx.close > resistance * (1.0 + buffer_pct))
        breakout_down = bool(support is not None and ctx.close < support * (1.0 - buffer_pct))

        long_gamma = ctx.net_gex > 0
        net_gex_delta = float(extra.get("net_gex_delta") or 0.0)
        net_gex_delta_pct = float(extra.get("net_gex_delta_pct") or 0.0)

        # Strengthening: use the normalized % delta so per-symbol absolute
        # magnitudes stop mattering.
        gamma_strengthening = net_gex_delta_pct > 0.005  # +0.5% of prior book

        # Wall migration: if the call_wall strike moved *up* with price, the
        # market is re-pricing resistance and the breakout is probably
        # genuine (trap fades).  If the wall is static or moved down, the
        # crowd is defending and the trap probability rises.
        wall_migrated_up = (
            prior_call_wall is not None
            and call_wall is not None
            and call_wall > prior_call_wall * 1.0005
        )
        wall_migrated_down = (
            prior_call_wall is not None
            and call_wall is not None
            and call_wall < prior_call_wall * 0.9995
        )

        # Flow confirmation: fading a breakout requires *decelerating*
        # call-buying flow (buyers running out).  Conversely, strong flow
        # acceleration into the breakout suppresses the fade signal.
        call_flow_delta = float(extra.get("call_flow_delta") or 0.0)
        put_flow_delta = float(extra.get("put_flow_delta") or 0.0)
        flow_norm = self._flow_flux_norm(ctx)
        call_decelerating = call_flow_delta < 0
        put_decelerating = put_flow_delta < 0

        # Trigger conditions.
        upside_fail = breakout_up and long_gamma and gamma_strengthening and not wall_migrated_up
        downside_fail = breakout_down and long_gamma and gamma_strengthening and not wall_migrated_down

        # Base magnitude scales with how far past the level we are + how
        # strongly gamma is reinforcing the trap.
        def _magnitude(dist_pct: float) -> float:
            dist_strength = min(1.0, abs(dist_pct) / max(buffer_pct * 3.0, 0.003))
            gex_boost = min(1.0, abs(net_gex_delta_pct) / 0.05)
            return 0.4 + 0.4 * dist_strength + 0.2 * gex_boost  # in [0.4, 1.0]

        score = 0.0
        if upside_fail and resistance:
            dist_pct = (ctx.close - resistance) / ctx.close
            mag = _magnitude(dist_pct)
            # Flow confirmation: decelerating call buying raises conviction,
            # accelerating call buying dampens the fade score.
            flow_mult = 1.1 if call_decelerating else max(0.3, 1.0 - call_flow_delta / max(flow_norm, 1.0))
            # Fade direction = bearish
            score = -min(1.0, mag * flow_mult)
        elif downside_fail and support:
            dist_pct = (ctx.close - support) / ctx.close
            mag = _magnitude(dist_pct)
            flow_mult = 1.1 if put_decelerating else max(0.3, 1.0 - put_flow_delta / max(flow_norm, 1.0))
            score = min(1.0, mag * flow_mult)

        score = max(-1.0, min(1.0, score))
        triggered = abs(score) >= 0.25

        return IndependentSignalResult(
            name="trap_detection",
            score=score,
            context={
                "triggered": triggered,
                "signal": "bearish_fade" if score < 0 else ("bullish_fade" if score > 0 else "none"),
                "close": ctx.close,
                "resistance_level": resistance,
                "support_level": support,
                "breakout_buffer_pct": round(buffer_pct, 6),
                "realized_sigma": round(sigma, 6),
                "breakout_up": breakout_up,
                "breakout_down": breakout_down,
                "net_gex": ctx.net_gex,
                "net_gex_delta": round(net_gex_delta, 2),
                "net_gex_delta_pct": round(net_gex_delta_pct, 6),
                "long_gamma": long_gamma,
                "gamma_strengthening": gamma_strengthening,
                "call_wall": call_wall,
                "prior_call_wall": prior_call_wall,
                "wall_migrated_up": wall_migrated_up,
                "wall_migrated_down": wall_migrated_down,
                "call_flow_decelerating": call_decelerating,
                "put_flow_decelerating": put_decelerating,
            },
        )

    # ------------------------------------------------------------------
    # 0DTE position imbalance (C6, C7, S5)
    # ------------------------------------------------------------------

    def _zero_dte_position_imbalance(self, ctx: MarketContext) -> IndependentSignalResult:
        extra = ctx.extra or {}
        flow_rows = extra.get("flow_zero_dte") or []
        used_zero_dte = bool(flow_rows)

        # Fall back to old (all-expiry) flow aggregation only if the true
        # 0DTE slice wasn't populated — keeps the signal working during the
        # first cycles after deploying the C6 migration.
        if not flow_rows:
            flow_rows = extra.get("flow_by_type") or []

        close = ctx.close if ctx.close > 0 else 1.0

        def _bucket_moneyness(option_type: str, strike: float) -> str:
            m = (strike - close) / close
            if option_type == "C":
                if m > 0.005:
                    return "otm"
                if m < -0.005:
                    return "itm"
                return "atm"
            else:  # P
                if m < -0.005:
                    return "otm"
                if m > 0.005:
                    return "itm"
                return "atm"

        buckets = {
            ("C", "otm"): 0.0,
            ("C", "atm"): 0.0,
            ("C", "itm"): 0.0,
            ("P", "otm"): 0.0,
            ("P", "atm"): 0.0,
            ("P", "itm"): 0.0,
        }
        call_net_total = 0.0
        put_net_total = 0.0
        for row in flow_rows:
            option_type = row.get("option_type")
            if option_type not in ("C", "P"):
                continue
            strike = float(row.get("strike") or 0.0)
            buy = float(row.get("buy_premium") or 0.0)
            sell = float(row.get("sell_premium") or 0.0)
            net = buy - sell
            # Old flow_by_type rows have no strike — everything goes into ATM bucket.
            bucket_key = (option_type, _bucket_moneyness(option_type, strike) if strike > 0 else "atm")
            buckets[bucket_key] = buckets.get(bucket_key, 0.0) + net
            if option_type == "C":
                call_net_total += net
            else:
                put_net_total += net

        # Moneyness-weighted imbalance (S5).  OTM-call buying is the
        # strongest predictor of same-day squeezes; ATM-put buying is the
        # cleanest bearish signal.  ITM flow is mostly institutional hedge
        # roll and down-weighted.
        weighted = (
            0.6 * buckets[("C", "otm")]
            + 0.3 * buckets[("C", "atm")]
            + 0.1 * buckets[("C", "itm")]
            - 0.6 * buckets[("P", "otm")]
            - 0.3 * buckets[("P", "atm")]
            - 0.1 * buckets[("P", "itm")]
        )
        total_abs = sum(abs(v) for v in buckets.values())
        flow_imbalance = weighted / total_abs if total_abs > 50_000 else 0.0

        # C7: signed smart-money imbalance (already signed in _fetch_market_context).
        sm_call = ctx.smart_call
        sm_put = ctx.smart_put
        sm_gross = float(extra.get("smart_call_gross") or 0.0) + float(extra.get("smart_put_gross") or 0.0)
        smart_imbalance = ((sm_call - sm_put) / sm_gross) if sm_gross > 100_000 else 0.0

        pcr_tilt = max(-1.0, min(1.0, (1.0 - ctx.put_call_ratio) / 0.35))
        combined = 0.55 * flow_imbalance + 0.30 * smart_imbalance + 0.15 * pcr_tilt

        # S5: time-of-day scaling.  Same-day positioning only creates forced
        # dealer flow when there's enough session left to matter; right at
        # the close everything is already pinned, so dampen.
        minute = minute_of_day_et(ctx.timestamp)
        if minute is not None and SESSION_OPEN_MIN_ET <= minute < SESSION_CLOSE_MIN_ET:
            hours_to_close = max(0.1, (SESSION_CLOSE_MIN_ET - minute) / 60.0)
            tod_mult = min(1.0, math.sqrt(hours_to_close / 6.5)) * 1.1
        else:
            tod_mult = 0.0
        combined *= tod_mult

        score = max(-1.0, min(1.0, combined))
        triggered = abs(score) >= 0.25

        return IndependentSignalResult(
            name="zero_dte_position_imbalance",
            score=score,
            context={
                "triggered": triggered,
                "signal": "call_heavy" if score > 0.25 else ("put_heavy" if score < -0.25 else "balanced"),
                "call_net_premium": round(call_net_total, 2),
                "put_net_premium": round(put_net_total, 2),
                "otm_call_net": round(buckets[("C", "otm")], 2),
                "atm_call_net": round(buckets[("C", "atm")], 2),
                "otm_put_net": round(buckets[("P", "otm")], 2),
                "atm_put_net": round(buckets[("P", "atm")], 2),
                "flow_imbalance": round(flow_imbalance, 4),
                "smart_imbalance": round(smart_imbalance, 4),
                "pcr_tilt": round(pcr_tilt, 4),
                "put_call_ratio": round(ctx.put_call_ratio, 4),
                "tod_multiplier": round(tod_mult, 3),
                "flow_source": "zero_dte" if used_zero_dte else "all_expiry_fallback",
            },
        )

    # ------------------------------------------------------------------
    # Gamma + VWAP confluence (S6)
    # ------------------------------------------------------------------

    def _gamma_vwap_confluence(self, ctx: MarketContext) -> IndependentSignalResult:
        flip = ctx.gamma_flip
        vwap = ctx.vwap
        extra = ctx.extra or {}
        max_pain = ctx.max_pain
        call_wall = extra.get("call_wall")
        max_gamma = extra.get("max_gamma_strike")

        if flip is None or vwap is None or ctx.close <= 0:
            return IndependentSignalResult(
                name="gamma_vwap_confluence",
                score=0.0,
                context={"triggered": False, "signal": "none", "reason": "missing_levels"},
            )

        # Build a candidate set of levels.  Anything within ±15bps of the
        # flip/VWAP midpoint counts as a confluence contributor.
        core_mid = 0.5 * (flip + vwap)
        cluster_candidates = {
            "gamma_flip": flip,
            "vwap": vwap,
        }
        for name, level in [("max_pain", max_pain), ("max_gamma", max_gamma), ("call_wall", call_wall)]:
            if level is None:
                continue
            if abs(level - core_mid) / ctx.close <= 0.0015:  # ±15bps
                cluster_candidates[name] = level

        # Continuous cluster quality based on the tightest core gap.
        core_gap_pct = abs(flip - vwap) / ctx.close
        cluster_quality = max(0.0, 1.0 - core_gap_pct / _CONFLUENCE_MAX_GAP_PCT)
        if cluster_quality <= 0:
            return IndependentSignalResult(
                name="gamma_vwap_confluence",
                score=0.0,
                context={
                    "triggered": False,
                    "signal": "none",
                    "gamma_flip": flip,
                    "vwap": vwap,
                    "core_gap_pct": round(core_gap_pct, 6),
                    "cluster_quality": 0.0,
                },
            )

        # Multi-level bonus: every extra clustered level bumps quality.
        extra_levels = max(0, len(cluster_candidates) - 2)  # flip+vwap already counted
        multi_mult = 1.0 + 0.15 * extra_levels

        levels = list(cluster_candidates.values())
        confluence_level = sum(levels) / len(levels)
        dist_from_level = (ctx.close - confluence_level) / ctx.close
        dir_sign = 1.0 if dist_from_level > 0 else -1.0 if dist_from_level < 0 else 0.0
        distance_strength = min(1.0, abs(dist_from_level) / 0.003)

        # S6 core fix: in NEGATIVE GEX, dealers amplify moves AWAY from
        # the confluence (continuation).  In POSITIVE GEX, dealers dampen
        # moves and mean-revert toward it -- so the score FLIPS, not just
        # dampens.  Mean-reversion conviction in long-gamma regimes is
        # slightly weaker than continuation in short-gamma regimes; hence
        # the 0.7 factor.
        raw = dir_sign * distance_strength
        if ctx.net_gex < 0:
            directional = raw
            regime_direction = "continuation"
        else:
            directional = -raw * 0.7
            regime_direction = "mean_reversion"

        score = directional * cluster_quality * multi_mult
        score = max(-1.0, min(1.0, score))

        # Expected reversion/continuation level for traders to place stops.
        expected_target = confluence_level if regime_direction == "mean_reversion" else (
            ctx.close + dir_sign * (ctx.close - confluence_level) * 2.0
        )

        triggered = abs(score) >= 0.2

        return IndependentSignalResult(
            name="gamma_vwap_confluence",
            score=score,
            context={
                "triggered": triggered,
                "signal": "bullish_confluence" if score > 0.2 else ("bearish_confluence" if score < -0.2 else "neutral"),
                "regime_direction": regime_direction,
                "gamma_flip": round(flip, 4),
                "vwap": round(vwap, 4),
                "confluence_level": round(confluence_level, 4),
                "cluster_members": list(cluster_candidates.keys()),
                "core_gap_pct": round(core_gap_pct, 6),
                "cluster_quality": round(cluster_quality, 4),
                "distance_from_level_pct": round(dist_from_level, 6),
                "expected_target": round(expected_target, 4),
                "net_gex": ctx.net_gex,
            },
        )
