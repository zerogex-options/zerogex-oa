"""OpeningRangeHunter — trade the break of the 09:30-10:00 ET range.

Fires between 10:00 and 11:30 ET when spot closes outside the opening 30-min
range and net_gex is not strongly positive (which would kill follow-through).

The range is ``snap.opening_range_high`` / ``snap.opening_range_low`` (see
src/opening_range.py). It used to be ``session_high`` / ``session_low`` -- the
trailing 24 hours, including the bar ``spot`` came from -- and spot can never
sit beyond its own bar, so the bot could never open a trade.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class OpeningRangeHunter(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Break the opening range. Trend + thin γ = clean legs."
    description = (
        "Enters trend continuation on breaks of the opening range in "
        "regimes where dealer gamma does not choke the move."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        m = snap.minutes_since_open
        if m is None or m < 30 or m > 120:
            return None
        range_high, range_low = snap.opening_range_high, snap.opening_range_low
        if range_high is None or range_low is None:
            return None
        if snap.gex_regime() == "positive_strong":
            return None  # pin regime kills breakout follow-through

        buffer_pct = float(self.params.get("break_buffer_pct", 0.0005))
        direction: Optional[str] = None
        if snap.spot > range_high * (1.0 + buffer_pct):
            direction = "bullish"
        elif snap.spot < range_low * (1.0 - buffer_pct):
            direction = "bearish"
        else:
            return None

        target = (
            snap.call_wall if direction == "bullish" else snap.put_wall
        ) or (snap.spot * (1.006 if direction == "bullish" else 0.994))
        stop = range_high if direction == "bullish" else range_low

        rng = range_high - range_low
        quality = min(1.0, rng / (snap.spot * 0.006)) * 0.7 + 0.3
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None
        expiration = snap.et_date.isoformat()
        strike = snap.round_to_strike(snap.spot)
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
            time_stop_at=_utcnow() + timedelta(minutes=int(self.params.get("max_hold_minutes", 90))),
            rationale=f"Opening-range break; regime {snap.gex_regime()}",
            components_at_entry={
                "opening_range_high": range_high,
                "opening_range_low": range_low,
                "net_gex": snap.net_gex,
            },
        )
