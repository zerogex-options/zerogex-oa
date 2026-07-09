"""RangeIronCondor — sell iron condor between the put and call walls.

When SPY is trading BETWEEN the walls in a positive-γ regime with low
VIX, dealers pin price toward max_pain and the walls act as magnetic
boundaries. That's the classic setup for a short iron condor — the
short strikes sit just outside where price is expected to close, and
the long strikes cap the risk on each side.

Structure: SHORT ONE contract of the near-OTM put + call, LONG ONE
contract of a further-OTM put + call. Profits when the underlying
finishes between the short strikes; max loss = (wing_width − credit)
per side. Non-directional; the delta is close to zero at entry.

This is the fleet's first non-directional bot. In a rangebound
positive-γ session, no directional bot has a real edge, but this one
captures theta cleanly.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class RangeIronCondor(BaseBot):
    tier = "0DTE"
    direction_mode = "neutral"
    tagline = "Positive-γ pin. Low VIX. Sell the iron condor between the walls."
    description = (
        "Sells a symmetric iron condor when spot is between the put and "
        "call walls in a positive-γ regime with subdued VIX. Non-"
        "directional; profits from theta decay if spot stays inside the "
        "wall band through the target horizon."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        # Regime gate — need positive gamma so dealers PIN price rather
        # than chase it. In negative γ, dealers reinforce moves and an
        # iron condor gets steamrolled.
        if snap.gex_regime() not in {"positive_strong", "positive_weak"}:
            return None
        m = snap.minutes_since_open
        if m is None or m < 30:
            return None

        # Vol gate — a low VIX cheap-vol environment is where theta
        # capture edges the paid credit; a spiking VIX makes the wings
        # too expensive relative to the short strikes.
        if snap.vix is None:
            return None
        max_vix = float(self.params.get("max_vix", 18.0))
        if snap.vix > max_vix:
            return None

        # Structural gate — spot must be BETWEEN the walls, and the walls
        # must be at least ``min_wall_span_pct`` apart. A tight wall band
        # doesn't leave room for the short strikes to breathe.
        if snap.call_wall is None or snap.put_wall is None:
            return None
        wall_span = (snap.call_wall - snap.put_wall) / snap.spot
        min_span = float(self.params.get("min_wall_span_pct", 0.008))
        if wall_span < min_span:
            return None
        if not (snap.put_wall < snap.spot < snap.call_wall):
            return None

        # Quality: deeper positive γ + low VIX + wider wall span → higher
        # quality. Cap each component at 1.0.
        gex = snap.net_gex or 0.0
        gex_score = min(1.0, max(0.0, gex / 2.5e9))
        vix_score = min(1.0, max(0.0, (max_vix - snap.vix) / max_vix))
        span_score = min(1.0, wall_span / 0.02)
        quality = 0.4 * gex_score + 0.35 * vix_score + 0.25 * span_score
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None

        dte_target = int(self.params.get("dte_target", 0))
        expiration = resolve_expiration_iso(snap.et_date, dte_target)

        # Strike placement: short strikes ~$2 inside each wall, long
        # strikes another $2 outside for a $2-wide wing on each side.
        # The ``wing_width`` param lets an operator widen for more
        # premium at the cost of more downside per contract.
        wing_width = int(self.params.get("wing_width", 2))
        short_buffer = int(self.params.get("short_buffer", 2))
        put_short_strike = round(snap.put_wall + short_buffer)
        put_long_strike = put_short_strike - wing_width
        call_short_strike = round(snap.call_wall - short_buffer)
        call_long_strike = call_short_strike + wing_width

        # Sanity: strikes must be ordered correctly.
        if not (
            put_long_strike < put_short_strike < snap.spot
            < call_short_strike < call_long_strike
        ):
            return None

        legs = self.build_iron_condor(
            snap.underlying,
            put_long_strike=put_long_strike,
            put_short_strike=put_short_strike,
            call_short_strike=call_short_strike,
            call_long_strike=call_long_strike,
            expiration=expiration,
        )

        # Target = max_pain (structural pin point). Stops on breach of
        # either short strike — spot moving past put_short is bearish
        # invalidation; past call_short is bullish invalidation. We
        # encode ONE stop level; the wall-drift logic on exit will fire
        # on either side.
        target = snap.max_pain or snap.spot
        # Use put_short as the stop — direction='neutral' triggers a
        # symmetric exit rule below. exit_criteria's default stop
        # comparison walks spot vs stop; for neutral we roll our own
        # via _wall_stop_signal on either wall.
        stop = put_short_strike

        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction="neutral",
            strategy_type="SHORT_IRON_CONDOR",
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=target,
            stop_price=stop,
            time_stop_at=_utcnow() + timedelta(
                minutes=int(self.params.get("max_hold_minutes", 180))
            ),
            wall_ref_price=snap.spot,
            rationale=(
                f"Positive-γ pin. Spot {snap.spot:.2f} between put_wall "
                f"{snap.put_wall} and call_wall {snap.call_wall}. VIX "
                f"{snap.vix:.2f}. IC {put_long_strike}/{put_short_strike}/"
                f"{call_short_strike}/{call_long_strike}."
            ),
            components_at_entry={
                "net_gex": snap.net_gex,
                "vix": snap.vix,
                "wall_span_pct": wall_span,
                "put_wall": snap.put_wall,
                "call_wall": snap.call_wall,
                "max_pain": snap.max_pain,
                "put_short_strike": put_short_strike,
                "call_short_strike": call_short_strike,
            },
        )
