"""VolExpansionStraddle — buy a straddle when vol looks primed to expand.

Fires when:
  * VIX has been sitting below a compression threshold (session low),
  * SPY has traded inside a tight session range (session_high /
    session_low as % of spot),
  * gamma regime is negative (dealers reinforce moves — a break either
    way runs), and
  * we're at least an hour into the session so the compression signal
    is meaningful.

Structure: LONG ATM CALL + LONG ATM PUT (straddle). Profit if
underlying moves far enough in EITHER direction to overcome the paid
premium. Non-directional. Delta is close to zero at entry (positive
gamma / negative theta).

Complement to the trend bots — those need directional conviction;
this one only needs conviction that SOMETHING is about to move.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class VolExpansionStraddle(BaseBot):
    tier = "0DTE"
    direction_mode = "neutral"
    tagline = "Compressed vol + tight range + short γ. Buy the straddle for the break."
    description = (
        "Buys an ATM straddle when VIX is compressed, SPY has ranged "
        "tightly, and dealer gamma is negative (breakouts reinforce). "
        "Non-directional; profits from movement in EITHER direction "
        "past the paid debit."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        m = snap.minutes_since_open
        if m is None or m < 60:
            return None

        # Vol gate — VIX must be below the compression threshold. Below
        # this is where vol paying-up is cheap relative to expected
        # realized. Above it we're paying up for vol that's already
        # priced in.
        if snap.vix is None:
            return None
        max_vix = float(self.params.get("max_vix", 16.0))
        if snap.vix > max_vix:
            return None

        # Range-compression gate — session_high − session_low relative
        # to spot must be below a threshold. A tight range up to this
        # point predicts a breakout is likelier than a continuation of
        # the pin.
        if snap.session_high is None or snap.session_low is None or snap.spot <= 0:
            return None
        range_pct = (snap.session_high - snap.session_low) / snap.spot
        max_range_pct = float(self.params.get("max_range_pct", 0.006))
        if range_pct > max_range_pct:
            return None

        # Regime gate — need NEGATIVE γ so a breakout runs. In positive
        # γ, dealers would fade the break and the straddle decays.
        if snap.gex_regime() not in {"negative_weak", "negative_strong"}:
            return None

        # Quality: tighter range = higher quality, deeper negative γ =
        # higher quality, lower VIX = higher quality.
        range_score = min(1.0, max(0.0, (max_range_pct - range_pct) / max_range_pct))
        gex_score = min(1.0, max(0.0, -1.0 * (snap.net_gex or 0.0) / 2.5e9))
        vix_score = min(1.0, max(0.0, (max_vix - snap.vix) / max_vix))
        quality = 0.4 * range_score + 0.35 * gex_score + 0.25 * vix_score
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None

        dte_target = int(self.params.get("dte_target", 0))
        expiration = resolve_expiration_iso(snap.et_date, dte_target)
        strike = snap.round_to_strike(snap.spot)
        legs = self.build_straddle(snap.underlying, strike, expiration)

        # Structural target: either wall. Whichever hits first captures
        # the win. Encode as the FAR wall (the harder-to-reach one) so
        # the target logic requires a real breakout, not noise.
        # For the stop, use a modest premium-decay heuristic — 40% of
        # session range on the wrong side of spot. Because
        # `direction='neutral'`, the default exit logic uses time_stop
        # rather than target/stop; the encoded values are advisory.
        # The engine's time_stop_at is the primary exit.
        target = (
            snap.call_wall if (snap.call_wall or 0) - snap.spot > snap.spot - (snap.put_wall or 0)
            else snap.put_wall
        )
        stop = snap.spot  # nominal; time_stop is the real exit for a straddle

        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction="neutral",
            strategy_type="LONG_STRADDLE",
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=target,
            stop_price=stop,
            time_stop_at=_utcnow() + timedelta(
                minutes=int(self.params.get("max_hold_minutes", 120))
            ),
            rationale=(
                f"Compressed range {range_pct * 100:.2f}% (max {max_range_pct * 100:.2f}%). "
                f"VIX {snap.vix:.2f} ≤ {max_vix}. {snap.gex_regime()} regime. "
                f"Straddle @ {strike}."
            ),
            components_at_entry={
                "vix": snap.vix,
                "net_gex": snap.net_gex,
                "range_pct": range_pct,
                "session_high": snap.session_high,
                "session_low": snap.session_low,
                "call_wall": snap.call_wall,
                "put_wall": snap.put_wall,
            },
        )
