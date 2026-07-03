"""VwapReversionScalper — buy dips / sell rips to VWAP.

Classic session-VWAP mean reversion. Works best 10:00-15:00 ET in a
positive-gamma regime when VWAP is acting as a magnet. Small size, short
hold, tight stops.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class VwapReversionScalper(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Fade the stretch to VWAP. Positive-γ magnet."
    description = (
        "Small-size 0DTE scalps that fade over-extension away from session "
        "VWAP in a positive-gamma regime. Fast exits."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        if snap.vwap is None or snap.gex_regime() not in {"positive_strong", "positive_weak"}:
            return None
        m = snap.minutes_since_open
        m2c = snap.minutes_to_close
        if m is None or m2c is None or m < 60 or m2c < 45:
            return None
        stretch = (snap.spot - snap.vwap) / snap.spot
        min_stretch = float(self.params.get("min_stretch_pct", 0.0025))
        if abs(stretch) < min_stretch:
            return None
        direction = "bearish" if stretch > 0 else "bullish"
        quality = min(1.0, abs(stretch) / 0.005) * 0.8 + 0.2
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None
        expiration = snap.et_date.isoformat()
        strike = round(snap.spot)
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_atm_debit(snap.underlying, opt_type, strike, expiration, 0.0)
        stop = snap.spot * (0.997 if direction == "bullish" else 1.003)
        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=direction,
            strategy_type="BUY_CALL_DEBIT" if direction == "bullish" else "BUY_PUT_DEBIT",
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=snap.vwap,
            stop_price=stop,
            time_stop_at=_utcnow() + timedelta(minutes=int(self.params.get("max_hold_minutes", 30))),
            rationale=f"VWAP stretch {stretch*100:+.2f}% in {snap.gex_regime()} gamma",
            components_at_entry={
                "vwap": snap.vwap,
                "stretch_pct": stretch,
                "net_gex": snap.net_gex,
            },
        )
