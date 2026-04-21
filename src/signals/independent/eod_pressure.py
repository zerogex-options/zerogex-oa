"""Independent end-of-day pressure detector."""
from __future__ import annotations

import math
import os
from datetime import datetime

from src.signals.components.base import MarketContext
from src.signals.components.utils import (
    SESSION_CLOSE_MIN_ET,
    SESSION_OPEN_MIN_ET,
    minute_of_day_et,
    realized_sigma,
)
from src.signals.independent.base import IndependentSignalResult

# Charm magnitude (per-session aggregate) at which the score saturates.
_CHARM_NORM = float(os.getenv("SIGNAL_EOD_CHARM_NORM", "2.0e7"))

# Baseline ATM band (used as a floor when realized vol is very low).
_ATM_BAND_FLOOR_PCT = float(os.getenv("SIGNAL_EOD_ATM_BAND_FLOOR_PCT", "0.005"))
# Vol-scaled band = max(floor, k * sigma_per_bar * sqrt(N)).
_ATM_BAND_VOL_K = float(os.getenv("SIGNAL_EOD_ATM_BAND_VOL_K", "1.5"))
_ATM_BAND_VOL_HORIZON = int(os.getenv("SIGNAL_EOD_ATM_BAND_HORIZON", "30"))

# Pin gravity saturates at this percentage distance from spot.
_PIN_SATURATION_PCT = float(os.getenv("SIGNAL_EOD_PIN_SATURATION_PCT", "0.003"))

# Score ramps from 0 to full strength linearly across the final window.
_WINDOW_START_MIN_TO_CLOSE = 90
_WINDOW_RAMP_END_MIN_TO_CLOSE = 15
_WINDOW_FULL_RAMP_MIN = 75

# Weights for combining sub-scores (sum <= 1.0 so score stays in [-1, 1]).
_W_CHARM = 0.6
_W_PIN = 0.4

# Calendar amplification multipliers.
_AMP_OPEX = 1.5
_AMP_QUAD_WITCHING = 2.0

# Expiry-bucket weights for charm-at-spot aggregation.
_BUCKET_WEIGHTS = {
    "0dte": 0.7,
    "weekly": 0.2,
    "monthly": 0.1,
    "leaps": 0.0,
}


class EODPressureSignal:
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

    def evaluate(self, ctx: MarketContext) -> IndependentSignalResult:
        score = self.compute(ctx)
        signal = "bullish" if score > 0 else "bearish" if score < 0 else "neutral"
        context = self.context_values(ctx)
        context.update({"triggered": abs(score) >= 0.2, "signal": signal})
        return IndependentSignalResult(name=self.name, score=score, context=context)

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

        bucketed = ctx.extra.get("gex_by_strike_bucket")
        if isinstance(bucketed, dict) and bucketed:
            total = 0.0
            for bucket, rows in bucketed.items():
                w = _BUCKET_WEIGHTS.get(bucket, 0.0)
                if w <= 0 or not rows:
                    continue
                total += w * self._sum_dealer_charm(rows, lo, hi)
            return total
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

    @staticmethod
    def _time_ramp(ts: datetime | None) -> float:
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


# Backward-compat alias.
EodPressureSignal = EODPressureSignal
EODPressureComponent = EODPressureSignal
