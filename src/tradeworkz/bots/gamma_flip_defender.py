"""GammaFlipDefender — mean-reversion at the gamma flip line.

Positive-gamma regime, price tags gamma_flip from above/below, dealers
mean-revert into the flip. Fires bullish (flip rejects from above) or
bearish (flip rejects from below). Cuts on wall break of the *other* wall
or a stop-out through the flip line.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class GammaFlipDefender(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Sell rallies into the flip. Buy dips into the flip. Positive-γ only."
    description = (
        "Trades mean reversion at the gamma flip line inside a positive-gamma "
        "regime. Exits on flip break or the target wall."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        if snap.gamma_flip is None or snap.gex_regime() not in {"positive_strong", "positive_weak"}:
            return None
        m = snap.minutes_since_open
        if m is None or m < 15:
            return None
        prox_pct = float(self.params.get("flip_proximity_pct", 0.0015))
        d = (snap.spot - snap.gamma_flip) / snap.spot
        if abs(d) > prox_pct:
            return None

        # Direction: mean-revert away from the flip. If spot is above flip we
        # expect rejection down (bearish), if below we expect rejection up.
        direction = "bearish" if snap.spot > snap.gamma_flip else "bullish"
        target = (
            snap.put_wall if direction == "bearish" else snap.call_wall
        ) or (snap.max_pain or snap.spot * (0.996 if direction == "bearish" else 1.004))

        quality = min(1.0, (snap.net_gex or 0.0) / 2.5e9) * 0.6 + 0.4
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None

        expiration = snap.et_date.isoformat()
        strike = snap.round_to_strike(snap.spot)
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_atm_debit(snap.underlying, opt_type, strike, expiration, 0.0)
        break_pct = float(self.params.get("flip_break_pct", 0.003))
        stop = snap.gamma_flip * (
            (1.0 - break_pct) if direction == "bullish" else (1.0 + break_pct)
        )
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
            rationale=f"Flip defense at {snap.gamma_flip:.2f}; direction {direction}",
            components_at_entry={
                "gamma_flip": snap.gamma_flip,
                "net_gex": snap.net_gex,
                "spot_flip_distance": d,
                "vix": snap.vix,
            },
        )
