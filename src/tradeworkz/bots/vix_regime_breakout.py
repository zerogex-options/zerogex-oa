"""VixRegimeBreakout — vol-expansion breakout bot.

When VIX is rising and net_gex is turning negative, expect range-break
continuation. Enters in the direction of the most recent 10-bar move
(momentum confirmation) and targets the far wall.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class VixRegimeBreakout(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Vol is expanding. Ride the trend before the wall catches it."
    description = (
        "Enters trend-continuation trades when VIX is elevated and dealer "
        "gamma has flipped to short. Targets the far wall or a 1% envelope."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        if snap.vix is None or snap.vix < float(self.params.get("min_vix", 16.0)):
            return None
        if snap.gex_regime() not in {"negative_weak", "negative_strong"}:
            return None
        if len(snap.recent_closes) < 10:
            return None
        m = snap.minutes_since_open
        if m is None or m < 45:
            return None
        move = snap.recent_closes[-1] - snap.recent_closes[-10]
        min_move = snap.spot * float(self.params.get("min_move_pct", 0.003))
        if abs(move) < min_move:
            return None
        direction = "bullish" if move > 0 else "bearish"
        target = (snap.call_wall if direction == "bullish" else snap.put_wall) or (
            snap.spot * (1.01 if direction == "bullish" else 0.99)
        )
        stop = snap.spot * (0.9955 if direction == "bullish" else 1.0045)

        quality = min(1.0, abs(move) / (snap.spot * 0.006)) * 0.7 + 0.3
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
            rationale=f"VIX {snap.vix:.1f} + {direction} momentum in short-γ",
            components_at_entry={
                "vix": snap.vix,
                "net_gex": snap.net_gex,
                "move_10bar": move,
            },
        )
