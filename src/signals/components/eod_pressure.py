"""End-of-day pressure scoring component.

Predicts directional dealer flow in the final ~75 minutes of the US cash
session by combining three mechanical effects:

  * **Charm-at-spot**: sum of signed dealer charm exposure for strikes
    within a vol-scaled band around spot, weighted by expiry bucket so
    0DTE charm (which dominates into the close) counts most.
  * **Pin gravity**: (pin - spot) / spot, gated by the dealer gamma
    regime. In a positive-gamma regime dealers damp moves toward the
    heavy-OI strike; in a negative-gamma regime they amplify moves
    *away* from it, so the sign flips. max_pain is preferred when
    available (it's the dollar-weighted OI max-pain, more reliable than
    max_gamma for EOD pin mechanics).
  * **Calendar amplifier**: OpEx Fridays (3rd Friday of the month) and
    quad-witching days (3rd Friday of Mar/Jun/Sep/Dec) roughly double
    charm magnitude as dealers unwind expiring hedges.

The score is **gated off** before 14:30 ET (T-90min) — EOD dynamics do
not meaningfully drive tape earlier in the session.  Uses the ET-native
minute-of-day helper so the ramp is DST-correct year-round.

Sign convention (matches vanna_charm_flow):
  * Positive => bullish EOD pressure (dealer buying into close)
  * Negative => bearish EOD pressure (dealer selling into close)
"""
from __future__ import annotations

import math
import os
from datetime import datetime

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import (
    SESSION_CLOSE_MIN_ET,
    SESSION_OPEN_MIN_ET,
    minute_of_day_et,
    realized_sigma,
)

# Charm magnitude (per-session aggregate) at which the score saturates.
_CHARM_NORM = float(os.getenv("SIGNAL_EOD_CHARM_NORM", "2.0e7"))

# Baseline ATM band (used as a floor when realized vol is very low).
_ATM_BAND_FLOOR_PCT = float(os.getenv("SIGNAL_EOD_ATM_BAND_FLOOR_PCT", "0.005"))
# Vol-scaled band = max(floor, k * sigma_per_bar * sqrt(N)).
_ATM_BAND_VOL_K = float(os.getenv("SIGNAL_EOD_ATM_BAND_VOL_K", "1.5"))
_ATM_BAND_VOL_HORIZON = int(os.getenv("SIGNAL_EOD_ATM_BAND_HORIZON", "30"))  # bars

# Pin gravity saturates at this percentage distance from spot.
_PIN_SATURATION_PCT = float(os.getenv("SIGNAL_EOD_PIN_SATURATION_PCT", "0.003"))

# Score ramps from 0 to full strength linearly across the final window.
_WINDOW_START_MIN_TO_CLOSE = 90  # 14:30 ET
_WINDOW_RAMP_END_MIN_TO_CLOSE = 15  # Full strength by 15:45 ET
_WINDOW_FULL_RAMP_MIN = 75

# Weights for combining sub-scores (sum <= 1.0 so composite stays in [-1, 1]).
_W_CHARM = 0.6
_W_PIN = 0.4

# Calendar amplification multipliers.
_AMP_OPEX = 1.5
_AMP_QUAD_WITCHING = 2.0

# Expiry-bucket weights for charm-at-spot aggregation.
# Into the final 90 min, 0DTE charm dominates; weeklies contribute;
# monthlies barely move.
_BUCKET_WEIGHTS = {
    "0dte": 0.7,
    "weekly": 0.2,
    "monthly": 0.1,
    "leaps": 0.0,
}


class EODPressureComponent(ComponentBase):
    name = "eod_pressure"
    weight = 0.06

    def compute(self, ctx: MarketContext) -> float:
        ramp = self._time_ramp(ctx.timestamp)
        if ramp <= 0.0:
            return 0.0

        charm_score = self._charm_at_spot_score(ctx)
        pin_score = self._pin_gravity_score(ctx)
        amp = self._calendar_amplifier(ctx.timestamp)

        combined = (_W_CHARM * charm_score + _W_PIN * pin_score) * amp * ramp
        return max(-1.0, min(1.0, combined))

    def context_values(self, ctx: MarketContext) -> dict:
        band = self._atm_band_pct(ctx)
        return {
            "time_ramp": round(self._time_ramp(ctx.timestamp), 3),
            "charm_at_spot": round(self._charm_at_spot_raw(ctx), 2),
            "atm_band_pct": round(band, 5),
            "pin_target": self._pin_target(ctx),
            "pin_source": self._pin_source(ctx),
            "pin_distance_pct": self._pin_distance_pct(ctx),
            "gamma_regime": "positive" if ctx.net_gex >= 0 else "negative",
            "calendar_amp": round(self._calendar_amplifier(ctx.timestamp), 3),
            "calendar_flags": self._calendar_flags(ctx.timestamp),
        }

    # ------------------------------------------------------------------
    # Sub-scores
    # ------------------------------------------------------------------

    def _charm_at_spot_score(self, ctx: MarketContext) -> float:
        raw = self._charm_at_spot_raw(ctx)
        if raw == 0.0:
            return 0.0
        return max(-1.0, min(1.0, raw / _CHARM_NORM))

    def _charm_at_spot_raw(self, ctx: MarketContext) -> float:
        if ctx.close <= 0 or not ctx.extra:
            return 0.0
        band_pct = self._atm_band_pct(ctx)
        lo = ctx.close * (1 - band_pct)
        hi = ctx.close * (1 + band_pct)

        # Prefer the expiry-bucketed map if analytics populated it.
        bucketed = ctx.extra.get("gex_by_strike_bucket")
        if isinstance(bucketed, dict) and bucketed:
            total = 0.0
            for bucket, rows in bucketed.items():
                w = _BUCKET_WEIGHTS.get(bucket, 0.0)
                if w <= 0 or not rows:
                    continue
                total += w * self._sum_dealer_charm(rows, lo, hi)
            return total

        # Fallback: flat sum across the per-strike list.
        return self._sum_dealer_charm(ctx.extra.get("gex_by_strike") or [], lo, hi)

    @staticmethod
    def _sum_dealer_charm(rows: list, lo: float, hi: float) -> float:
        total = 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                strike = float(row.get("strike"))
            except (TypeError, ValueError):
                continue
            if not (lo <= strike <= hi):
                continue
            # Prefer dealer-sign column; fall back to -raw if absent.
            c = row.get("dealer_charm_exposure")
            if c is None:
                raw = row.get("charm_exposure")
                if raw is None:
                    continue
                try:
                    c = -float(raw)
                except (TypeError, ValueError):
                    continue
            try:
                total += float(c)
            except (TypeError, ValueError):
                continue
        return total

    def _atm_band_pct(self, ctx: MarketContext) -> float:
        """Vol-scaled ATM band: wider when realized vol is high."""
        sigma = realized_sigma(ctx.recent_closes or [], window=60)
        if sigma <= 0:
            return _ATM_BAND_FLOOR_PCT
        projected = sigma * math.sqrt(max(1, _ATM_BAND_VOL_HORIZON))
        return max(_ATM_BAND_FLOOR_PCT, _ATM_BAND_VOL_K * projected)

    def _pin_gravity_score(self, ctx: MarketContext) -> float:
        distance_pct = self._pin_distance_pct(ctx)
        if distance_pct is None:
            return 0.0
        normalized = max(-1.0, min(1.0, distance_pct / _PIN_SATURATION_PCT))
        sign = 1.0 if ctx.net_gex >= 0 else -1.0
        return sign * normalized

    def _pin_distance_pct(self, ctx: MarketContext) -> float | None:
        pin = self._pin_target(ctx)
        if pin is None or ctx.close <= 0:
            return None
        return (pin - ctx.close) / ctx.close

    @staticmethod
    def _pin_target(ctx: MarketContext) -> float | None:
        """Prefer max_pain (OI-weighted, reliable); fall back to heavy-gamma strike."""
        candidates = [ctx.max_pain]
        if ctx.extra:
            candidates.append(ctx.extra.get("max_gamma_strike"))
        for candidate in candidates:
            if candidate is None:
                continue
            try:
                return float(candidate)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _pin_source(ctx: MarketContext) -> str | None:
        if ctx.max_pain is not None:
            return "max_pain"
        if ctx.extra and ctx.extra.get("max_gamma_strike") is not None:
            return "max_gamma_strike"
        return None

    # ------------------------------------------------------------------
    # Time-to-close ramp (ET-native)
    # ------------------------------------------------------------------

    @staticmethod
    def _time_ramp(ts: datetime | None) -> float:
        """Linear ramp from 0 at T-90min to 1.0 at T-15min, held at 1.0 thereafter."""
        minute = minute_of_day_et(ts)
        if minute is None or minute < SESSION_OPEN_MIN_ET or minute >= SESSION_CLOSE_MIN_ET:
            return 0.0
        to_close = SESSION_CLOSE_MIN_ET - minute
        if to_close > _WINDOW_START_MIN_TO_CLOSE:
            return 0.0
        if to_close <= _WINDOW_RAMP_END_MIN_TO_CLOSE:
            return 1.0
        elapsed = _WINDOW_START_MIN_TO_CLOSE - to_close
        return elapsed / _WINDOW_FULL_RAMP_MIN

    # ------------------------------------------------------------------
    # Calendar
    # ------------------------------------------------------------------

    def _calendar_amplifier(self, ts: datetime | None) -> float:
        flags = self._calendar_flags(ts)
        if flags.get("quad_witching"):
            return _AMP_QUAD_WITCHING
        if flags.get("opex"):
            return _AMP_OPEX
        return 1.0

    @staticmethod
    def _calendar_flags(ts: datetime | None) -> dict:
        if ts is None:
            return {"opex": False, "quad_witching": False}
        is_third_friday = ts.weekday() == 4 and 15 <= ts.day <= 21
        is_quad_month = ts.month in (3, 6, 9, 12)
        return {
            "opex": is_third_friday,
            "quad_witching": is_third_friday and is_quad_month,
        }
