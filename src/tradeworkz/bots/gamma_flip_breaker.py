"""GammaFlipBreaker — momentum through the flip line.

When net_gex is thin or negative and price is breaking across gamma_flip
with force, dealers accelerate the move (short-gamma hedging). The bot buys
the break. Stop is a re-entry back through the flip line.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class GammaFlipBreaker(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Ride the breakout. Dealer hedging turns momentum into a wave."
    description = (
        "Enters directional trades when price breaks through the gamma flip "
        "in a thin or negative-gamma regime. Stops on re-entry through the flip."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        if snap.gamma_flip is None:
            return None
        if snap.gex_regime() not in {"negative_weak", "negative_strong", "positive_weak"}:
            return None
        m = snap.minutes_since_open
        if m is None or m < 30:
            return None

        cross_pct = float(self.params.get("cross_min_pct", 0.0015))
        d = (snap.spot - snap.gamma_flip) / snap.spot
        if abs(d) < cross_pct:
            return None
        # Require the last-few-bars trend to agree with the cross.
        if len(snap.recent_closes) < 5:
            return None
        last = snap.recent_closes[-1]
        first = snap.recent_closes[-5]
        if d > 0 and last <= first:
            return None
        if d < 0 and last >= first:
            return None

        direction = "bullish" if d > 0 else "bearish"
        target = (snap.call_wall if direction == "bullish" else snap.put_wall) or (
            snap.spot * (1.005 if direction == "bullish" else 0.995)
        )
        break_pct = float(self.params.get("flip_reentry_pct", 0.0008))
        stop = snap.gamma_flip * (1.0 - break_pct if direction == "bullish" else 1.0 + break_pct)

        # Quality: bigger cross + more distance-to-target wall = higher quality.
        quality = min(1.0, abs(d) / 0.006) * 0.7 + 0.3
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
            rationale=f"Flip break {d*100:+.2f}%; regime {snap.gex_regime()}",
            components_at_entry={
                "gamma_flip": snap.gamma_flip,
                "cross_pct": d,
                "net_gex": snap.net_gex,
            },
        )
