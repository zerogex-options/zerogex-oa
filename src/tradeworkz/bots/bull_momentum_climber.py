"""BullMomentumClimber — buy call verticals when the tape is climbing.

Symmetric counterpart to the bearish momentum riders. Fires when:
  * regime is positive-γ (dealers reinforce upside; call wall is a
    magnet rather than a hard ceiling),
  * spot is above VWAP AND above gamma_flip (structure supports a leg
    up),
  * recent 5-bar close-to-close trend is positive by at least a
    configurable threshold (real momentum, not noise), and
  * the call wall is far enough away that a legitimate leg has room
    to work.

Structure is a BULL CALL DEBIT SPREAD — long the near-ATM call, short
one strike higher — so the max loss is capped at the debit paid and
the size is much larger than a naked long call could support at the
same risk budget. This is important: the fleet has been long puts
every session because every bot that fires in negative-γ goes short
directionally; adding a bot that fires ONLY in positive-γ AND ONLY
long delta rebalances the fleet's average delta toward zero.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class BullMomentumClimber(BaseBot):
    tier = "0DTE"
    direction_mode = "bullish"
    tagline = "Positive-γ tape climbing above VWAP and flip. Buy the call debit spread."
    description = (
        "Buys a bull call vertical when spot has cleared VWAP and the "
        "gamma flip in a positive-γ regime AND the recent 5-bar trend "
        "confirms. Multi-leg debit spread caps max loss at the paid "
        "debit; target is the call wall, stop is a break back below "
        "the flip."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        # Regime gate — dealers must be reinforcing the move.
        if snap.gex_regime() not in {"positive_strong", "positive_weak"}:
            return None
        m = snap.minutes_since_open
        if m is None or m < 30:
            return None

        # Structural gates: must be above BOTH VWAP and gamma_flip.
        if snap.vwap is None or snap.gamma_flip is None:
            return None
        if snap.spot <= snap.vwap or snap.spot <= snap.gamma_flip:
            return None

        # Momentum gate: recent 5-bar close-to-close move must be positive
        # by at least ``min_trend_pct``. Filters out sideways chop.
        if len(snap.recent_closes) < 5:
            return None
        trend = snap.recent_closes[-1] - snap.recent_closes[-5]
        trend_pct = trend / snap.spot if snap.spot > 0 else 0.0
        min_trend_pct = float(self.params.get("min_trend_pct", 0.0015))
        if trend_pct < min_trend_pct:
            return None

        # Structural target: distance to call wall must be at least
        # ``min_wall_room_pct`` — no point paying up if the target is
        # already close enough to be reached by noise.
        if snap.call_wall is None:
            return None
        wall_room = (snap.call_wall - snap.spot) / snap.spot
        min_wall_room_pct = float(self.params.get("min_wall_room_pct", 0.003))
        if wall_room < min_wall_room_pct:
            return None

        # Quality score blends GEX magnitude (deeper positive γ is safer),
        # trend strength, and how far the call wall target is (a bigger
        # target = a better setup, up to a cap).
        gex = snap.net_gex or 0.0
        gex_score = min(1.0, max(0.0, gex / 2.5e9))
        trend_score = min(1.0, max(0.0, trend_pct / 0.005))
        room_score = min(1.0, wall_room / 0.008)
        quality = 0.4 * gex_score + 0.3 * trend_score + 0.3 * room_score
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None

        dte_target = int(self.params.get("dte_target", 0))
        expiration = resolve_expiration_iso(snap.et_date, dte_target)
        # Long strike sits at ATM (nearest listed strike); short one strike
        # higher for the narrowest bull call debit on this symbol's grid
        # ($1 wide on an ETF, 5 wide on SPX).
        increment = snap.effective_strike_increment()
        long_strike = snap.round_to_strike(snap.spot)
        short_strike = long_strike + increment
        legs = self.build_vertical(
            snap.underlying, "call", long_strike, short_strike, expiration
        )

        # Target = call wall (structural). Stop = gamma_flip (structural
        # invalidation — if spot breaks BACK below flip, the bull thesis
        # is done). Both spot-level; exit_criteria reads snap.spot.
        target = snap.call_wall
        stop = snap.gamma_flip

        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction="bullish",
            strategy_type="BULL_CALL_DEBIT_SPREAD",
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=target,
            stop_price=stop,
            time_stop_at=_utcnow() + timedelta(
                minutes=int(self.params.get("max_hold_minutes", 60))
            ),
            rationale=(
                f"Above VWAP {snap.vwap:.2f} and flip {snap.gamma_flip:.2f}; "
                f"5-bar trend +{trend_pct * 100:.2f}%; wall {snap.call_wall} "
                f"({wall_room * 100:.2f}% away); {snap.gex_regime()} regime"
            ),
            components_at_entry={
                "net_gex": snap.net_gex,
                "vwap": snap.vwap,
                "gamma_flip": snap.gamma_flip,
                "recent_trend_pct": trend_pct,
                "wall_room_pct": wall_room,
                "vix": snap.vix,
                "spot_flip_distance": (snap.spot - snap.gamma_flip) / snap.spot,
            },
        )
