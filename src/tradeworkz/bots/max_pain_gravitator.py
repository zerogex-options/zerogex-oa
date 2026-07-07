"""MaxPainGravitator — trade the swing back to max_pain.

When spot is displaced from max_pain by more than a threshold and the
regime is positive-gamma (a pin regime), the bot buys a two-day debit
option in the direction of max_pain. Longer hold than the 0DTE bots.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class MaxPainGravitator(BaseBot):
    tier = "1DTE"
    direction_mode = "context"
    tagline = "Distant max_pain in a pin regime. Ride the drift back."
    description = (
        "1-2 day debit trades in the direction of max_pain when the "
        "underlying is materially displaced and gamma is positive."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        if snap.max_pain is None or snap.gex_regime() != "positive_strong":
            return None
        drift = (snap.max_pain - snap.spot) / snap.spot
        min_drift = float(self.params.get("min_drift_pct", 0.004))
        if abs(drift) < min_drift:
            return None
        direction = "bullish" if drift > 0 else "bearish"

        quality = min(1.0, abs(drift) / 0.012) * 0.8 + 0.2
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None

        # 1DTE expiration — next-session date.
        exp = (snap.et_date + timedelta(days=1)).isoformat()
        strike = round(snap.spot)
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_atm_debit(snap.underlying, opt_type, strike, exp, 0.0)
        stop = snap.spot * (0.992 if direction == "bullish" else 1.008)
        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=direction,
            strategy_type="BUY_CALL_DEBIT" if direction == "bullish" else "BUY_PUT_DEBIT",
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=snap.max_pain,
            stop_price=stop,
            time_stop_at=_utcnow() + timedelta(minutes=int(self.params.get("max_hold_minutes", 24 * 60))),
            rationale=f"max_pain {snap.max_pain:.2f}, drift {drift*100:+.2f}%",
            components_at_entry={
                "max_pain": snap.max_pain,
                "drift_pct": drift,
                "net_gex": snap.net_gex,
            },
        )
