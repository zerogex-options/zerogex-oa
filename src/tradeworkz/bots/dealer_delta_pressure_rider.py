"""DealerDeltaPressureRider — trade the direction dealer-delta is leaning.

Big absolute ``dealer_net_delta`` in a negative-gamma regime means dealers
need to *directional*-hedge — buying rallies, selling declines. Riding that
flow works when confirmed by a same-direction move in the recent tape.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class DealerDeltaPressureRider(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Ride dealer delta pressure. Momentum + short-γ regime = follow-through."
    description = (
        "Enters directional trades in a negative-gamma regime when dealer "
        "net delta is heavily one-sided and recent price agrees."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        if snap.dealer_net_delta is None or snap.gex_regime() not in {"negative_weak", "negative_strong"}:
            return None
        m = snap.minutes_since_open
        if m is None or m < 30:
            return None
        threshold = float(self.params.get("delta_threshold", 5.0e8))
        if abs(snap.dealer_net_delta) < threshold:
            return None
        if len(snap.recent_closes) < 5:
            return None
        recent_trend = snap.recent_closes[-1] - snap.recent_closes[-5]
        if snap.dealer_net_delta > 0 and recent_trend <= 0:
            return None
        if snap.dealer_net_delta < 0 and recent_trend >= 0:
            return None

        direction = "bullish" if snap.dealer_net_delta > 0 else "bearish"
        target = (snap.call_wall if direction == "bullish" else snap.put_wall) or (
            snap.spot * (1.005 if direction == "bullish" else 0.995)
        )
        stop = snap.spot * (0.996 if direction == "bullish" else 1.004)

        quality = min(1.0, abs(snap.dealer_net_delta) / (2.0 * threshold)) * 0.7 + 0.3
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None

        expiration = snap.et_date.isoformat()
        strike = round(snap.spot)
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_atm_debit(snap.underlying, opt_type, strike, expiration, 0.0)
        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=direction,
            strategy_type="BUY_CALL_DEBIT" if direction == "bullish" else "BUY_PUT_DEBIT",
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=target,
            stop_price=stop,
            time_stop_at=_utcnow() + timedelta(minutes=int(self.params.get("max_hold_minutes", 60))),
            rationale=(
                f"dealer_net_delta={snap.dealer_net_delta:.2e} + "
                f"5-bar trend agrees; {snap.gex_regime()} regime"
            ),
            components_at_entry={
                "dealer_net_delta": snap.dealer_net_delta,
                "net_gex": snap.net_gex,
                "recent_trend": recent_trend,
            },
        )
