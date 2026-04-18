"""End-of-day pressure scoring component.

Predicts directional dealer flow in the final ~75 minutes of the US cash
session by combining three mechanical effects:

  * **Charm-at-spot**: sum of signed charm_exposure for strikes within a
    narrow band around spot. Charm (dDelta/dTime) accelerates roughly as
    1/sqrt(T) into expiry, so these strikes are where forced dealer
    re-hedging concentrates into the close.
  * **Pin gravity**: (pin - spot) / spot, gated by the dealer gamma
    regime. In a positive-gamma regime dealers damp moves toward the
    heavy-OI strike; in a negative-gamma regime they amplify moves
    *away* from it, so the sign flips.
  * **Calendar amplifier**: OpEx Fridays (3rd Friday of the month) and
    quad-witching days (3rd Friday of Mar/Jun/Sep/Dec) roughly double
    charm magnitude as dealers unwind expiring hedges.

The score is **gated off** before 14:30 ET (T-90min) — EOD dynamics do
not meaningfully drive tape earlier in the session.

Sign convention (matches vanna_charm_flow):
  * Positive => bullish EOD pressure (dealer buying into close)
  * Negative => bearish EOD pressure (dealer selling into close)
"""
from __future__ import annotations

import os
from datetime import datetime

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.utils import (
    SESSION_CLOSE_MIN_UTC,
    SESSION_OPEN_MIN_UTC,
    minute_of_day,
)

# Charm magnitude (per-session aggregate) at which the score saturates.
_CHARM_NORM = float(os.getenv("SIGNAL_EOD_CHARM_NORM", "2.0e7"))

# Strike band around spot considered "at-the-money" for charm aggregation.
_ATM_BAND_PCT = float(os.getenv("SIGNAL_EOD_ATM_BAND_PCT", "0.01"))

# Pin gravity saturates at this percentage distance from spot.
_PIN_SATURATION_PCT = float(os.getenv("SIGNAL_EOD_PIN_SATURATION_PCT", "0.003"))

# Score ramps from 0 to full strength linearly across the final window.
# Before _WINDOW_START_MIN_TO_CLOSE the component returns 0.
_WINDOW_START_MIN_TO_CLOSE = 90  # 14:30 ET
_WINDOW_RAMP_END_MIN_TO_CLOSE = 15  # Full strength by 15:45 ET
_WINDOW_FULL_RAMP_MIN = 75  # _WINDOW_START - _WINDOW_RAMP_END

# Weights for combining sub-scores (sum <= 1.0 so composite stays in [-1, 1]).
_W_CHARM = 0.6
_W_PIN = 0.4

# Calendar amplification multipliers.
_AMP_OPEX = 1.5
_AMP_QUAD_WITCHING = 2.0


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
        return {
            "time_ramp": round(self._time_ramp(ctx.timestamp), 3),
            "charm_at_spot": round(self._charm_at_spot_raw(ctx), 2),
            "pin_target": self._pin_target(ctx),
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

    @staticmethod
    def _charm_at_spot_raw(ctx: MarketContext) -> float:
        rows = ctx.extra.get("gex_by_strike") if ctx.extra else None
        if not rows or ctx.close <= 0:
            return 0.0
        lo = ctx.close * (1 - _ATM_BAND_PCT)
        hi = ctx.close * (1 + _ATM_BAND_PCT)
        total = 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                strike = float(row.get("strike"))
                charm = row.get("charm_exposure")
                if charm is None:
                    continue
                if lo <= strike <= hi:
                    total += float(charm)
            except (TypeError, ValueError):
                continue
        return total

    def _pin_gravity_score(self, ctx: MarketContext) -> float:
        distance_pct = self._pin_distance_pct(ctx)
        if distance_pct is None:
            return 0.0
        # Normalize distance into [-1, 1].
        normalized = max(-1.0, min(1.0, distance_pct / _PIN_SATURATION_PCT))
        # Positive gamma: dealers damp moves toward pin, so bias TOWARD pin.
        # Negative gamma: dealers amplify moves, so bias AWAY from pin.
        sign = 1.0 if ctx.net_gex >= 0 else -1.0
        return sign * normalized

    def _pin_distance_pct(self, ctx: MarketContext) -> float | None:
        pin = self._pin_target(ctx)
        if pin is None or ctx.close <= 0:
            return None
        return (pin - ctx.close) / ctx.close

    @staticmethod
    def _pin_target(ctx: MarketContext) -> float | None:
        """Prefer the heavy-GEX strike; fall back to max_pain."""
        candidates = []
        if ctx.extra:
            candidates.append(ctx.extra.get("max_gamma_strike"))
        candidates.append(ctx.max_pain)
        for candidate in candidates:
            if candidate is None:
                continue
            try:
                return float(candidate)
            except (TypeError, ValueError):
                continue
        return None

    # ------------------------------------------------------------------
    # Time-to-close ramp
    # ------------------------------------------------------------------

    @staticmethod
    def _time_ramp(ts: datetime | None) -> float:
        """Linear ramp from 0 at T-90min to 1.0 at T-15min, held at 1.0 thereafter."""
        minute = minute_of_day(ts)
        if minute is None or minute < SESSION_OPEN_MIN_UTC or minute >= SESSION_CLOSE_MIN_UTC:
            return 0.0
        to_close = SESSION_CLOSE_MIN_UTC - minute
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
        # OpEx: 3rd Friday of the month (weekday()==4; 15<=day<=21).
        is_third_friday = ts.weekday() == 4 and 15 <= ts.day <= 21
        is_quad_month = ts.month in (3, 6, 9, 12)
        return {
            "opex": is_third_friday,
            "quad_witching": is_third_friday and is_quad_month,
        }
