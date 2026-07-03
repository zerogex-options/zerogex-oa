"""EodPinDrifter — last-hour drift toward max-pain / VWAP.

Between 15:00 and ~15:55 ET dealer gamma pins price toward the max-pain
strike. The bot fires when spot is far enough from max_pain that a drift
back to it is a reasonable 30-minute expectation.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class EodPinDrifter(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Ride the last-hour pin. Dealers push spot toward max-pain."
    description = (
        "0DTE afternoon drift bot: enters when the underlying is off "
        "max_pain enough to expect a mean-revert pin into 16:00 ET."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        m2c = snap.minutes_to_close
        if m2c is None or m2c > 60 or m2c < 10:
            return None
        target_ref = snap.max_pain or snap.vwap
        if target_ref is None or snap.spot <= 0:
            return None
        drift = (target_ref - snap.spot) / snap.spot
        min_drift = float(self.params.get("min_drift_pct", 0.001))
        if abs(drift) < min_drift:
            return None
        direction = "bullish" if drift > 0 else "bearish"

        quality = min(1.0, abs(drift) / 0.004) * 0.8 + 0.2
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None

        expiration = snap.et_date.isoformat()
        strike = round(snap.spot)
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_atm_debit(snap.underlying, opt_type, strike, expiration, 0.0)
        stop = snap.spot * (0.996 if direction == "bullish" else 1.004)
        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=direction,
            strategy_type="BUY_CALL_DEBIT" if direction == "bullish" else "BUY_PUT_DEBIT",
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=target_ref,
            stop_price=stop,
            time_stop_at=_utcnow() + timedelta(minutes=int(max(5, m2c - 5))),
            rationale=f"EOD drift toward {target_ref:.2f} ({drift*100:+.2f}%)",
            components_at_entry={
                "max_pain": snap.max_pain,
                "vwap": snap.vwap,
                "drift_pct": drift,
                "minutes_to_close": m2c,
            },
        )
