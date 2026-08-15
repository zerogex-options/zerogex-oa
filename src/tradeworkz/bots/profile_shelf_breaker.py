"""ProfileShelfBreaker — ride a hedging cascade down (or up) a gamma shelf.

The persisted ``gex_profile`` curve G(s) IS the dealers' forced order
schedule: at any spot s their required hedge flow per unit move is
proportional to G(s). When G at spot is unremarkable but the curve drops off
a cliff just below (``profile_gex_down`` deeply negative), a modest 25–50 bp
slide flips the hedging community into short gamma of size |G_down| — they
must sell every further downtick, and the forced selling GROWS with each tick
until price reaches the trough bottom where the curve flattens. Mirror case
above spot for an upside cascade (negative gamma above spot means dealers buy
into strength).

SpotGamma-style products publish a single flip scalar and walls; the local
steepness/asymmetry of the full repriced curve exists only in ZeroGEX's
``gex_profile`` JSONB, which is consumed by exactly one frontend overlay and
zero bots. The trough-bottom target is a computed, unpublished level.

The retired flip bots traded the single published flip level statically, and
``gamma_regime_shift_rider`` trades realized net-GEX sign transitions AFTER
they print. This bot never references the flip at all: it trades the
pre-transition curve GEOMETRY (depth, one-sidedness, trough location), armed
only by a live price trigger sliding INTO the shelf — the geometry alone is a
map, not a trade.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, OpenPosition, TradeSignal


class ProfileShelfBreaker(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "A gamma cliff sits just off spot and price is sliding into it."
    description = (
        "Reads the persisted gex_profile spot-shift curve for a steep, "
        "one-sided negative-gamma shelf within ~0.5% of spot and buys a "
        "vertical spanning it when the tape starts sliding that way — the "
        "dealers' own hedge schedule accelerates the move to the trough "
        "bottom, which is the target. Defined-risk vertical."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        m = snap.minutes_since_open
        mtc = snap.minutes_to_close
        if m is None or m < float(self.params.get("min_minutes_since_open", 30)):
            return self._skip("window")
        if mtc is None or mtc < float(self.params.get("min_minutes_to_close", 90)):
            return self._skip("window")

        g_dn, g_up = snap.profile_gex_down, snap.profile_gex_up
        trough_price, trough_gex = snap.profile_trough_price, snap.profile_trough_gex
        local = snap.local_gex
        if g_dn is None or g_up is None or trough_price is None or trough_gex is None:
            return self._skip("no_profile")
        if local is None or local <= 0 or snap.spot <= 0:
            return self._skip("no_local_gex")

        # -- Shoulder position: spot must sit near the curve's zero shoulder,
        # not deep in stabilizing positive territory where the cliff is
        # unreachable behind a wall of restoring gamma.
        max_spot_frac = float(self.params.get("max_spot_gex_local_frac", 0.5))
        if snap.net_gex is not None and abs(snap.net_gex) > max_spot_frac * local:
            return self._skip("not_on_shoulder")

        depth_frac = float(self.params.get("min_shelf_depth_local_frac", 0.75))
        min_asym = float(self.params.get("min_asymmetry_ratio", 2.0))
        eps = 1e-9

        def _side(shelf: float, other: float) -> bool:
            """Deep negative shelf on this side, and clearly one-sided."""
            if shelf > -depth_frac * local:
                return False
            other_mag = max(abs(other), eps) if other < 0 else eps
            return abs(shelf) >= min_asym * other_mag if other < 0 else True

        bear_shelf = _side(g_dn, g_up) and trough_price < snap.spot
        bull_shelf = _side(g_up, g_dn) and trough_price > snap.spot
        if not bear_shelf and not bull_shelf:
            return self._skip("no_shelf")
        if bear_shelf and bull_shelf:
            # Both sides deeply negative — a full negative-gamma bowl, not a
            # one-sided cliff. That regime belongs to other bots.
            return self._skip("two_sided")
        direction = "bearish" if bear_shelf else "bullish"
        shelf_gex = g_dn if bear_shelf else g_up

        # -- Trigger: the geometry is only a map — price must actually be
        # sliding toward the shelf.
        closes = [c for c in (snap.recent_closes or []) if c and c > 0]
        bars = max(2, int(self.params.get("trigger_bars", 5)))
        if len(closes) < bars + 1:
            return self._skip("no_history")
        slide = (closes[-1] - closes[-1 - bars]) / closes[-1 - bars]
        min_slide = float(self.params.get("min_trigger_pct", 0.001))
        if direction == "bearish" and slide > -min_slide:
            return self._skip("not_sliding")
        if direction == "bullish" and slide < min_slide:
            return self._skip("not_sliding")

        # -- Flow non-opposing: abstain when aggressors are paying up hard
        # against the cascade direction.
        veto_dollars = float(self.params.get("flow_veto_dollars", 1.0e6))
        frp = snap.flow_recent_premium
        if frp is not None and abs(frp) > veto_dollars:
            if (direction == "bearish" and frp > 0) or (direction == "bullish" and frp < 0):
                return self._skip("tape_opposes")

        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        other = g_up if bear_shelf else g_dn
        asym_ratio = abs(shelf_gex) / max(abs(other), eps) if other < 0 else 4.0
        quality = 0.5 * min(1.0, abs(shelf_gex) / (1.5 * local)) + 0.5 * min(1.0, asym_ratio / 4.0)
        ml_components = {
            "shelf_gex": shelf_gex,
            "asym_ratio": asym_ratio,
            "trough_gex": trough_gex,
            "local_gex": local,
            "slide": slide,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

        # -- Structure: vertical spanning the forced-flow zone. Long one
        # increment OTM in the cascade direction; short at the trough bottom
        # (capped) — where the mechanism says the flow exhausts.
        expiration = resolve_expiration_iso(snap.et_date, int(self.params.get("dte_target", 0)))
        inc = snap.effective_strike_increment()
        max_width = max(1, int(self.params.get("max_spread_width", 4)))
        sign = 1.0 if direction == "bullish" else -1.0
        long_strike = snap.round_to_strike(snap.spot) + sign * inc
        width = max(1, min(max_width, round(abs(trough_price - snap.spot) / inc)))
        short_strike = long_strike + sign * width * inc
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_vertical(snap.underlying, opt_type, long_strike, short_strike, expiration)
        strat = "BULL_CALL_DEBIT_SPREAD" if direction == "bullish" else "BEAR_PUT_DEBIT_SPREAD"

        stop_pct = float(self.params.get("stop_pct", 0.0015))
        stop = snap.spot * (1.0 - sign * stop_pct)
        hold = int(self.params.get("max_hold_minutes", 100))

        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=direction,
            strategy_type=strat,
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=trough_price,
            stop_price=stop,
            time_stop_at=_utcnow() + timedelta(minutes=hold),
            rationale=(
                f"{direction} shelf {shelf_gex:+.2e} "
                f"({abs(shelf_gex) / local:.2f}x local_gex, {asym_ratio:.1f}:1 one-sided), "
                f"trough {trough_price:.2f}; {bars}-bar slide {slide * 100:+.2f}%"
            ),
            components_at_entry={
                "shelf_gex": shelf_gex,
                "asym_ratio": asym_ratio,
                "trough_price": trough_price,
                "local_gex": local,
                "slide": slide,
                "quality": quality,
            },
        )

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
        """Default stack plus shelf-refill invalidation.

        The curve is recomputed every analytics cycle; if fresh positioning
        refilled the trough — the shelf is no longer deep or no longer
        one-sided — the forced-flow map the trade priced is gone. Close
        regardless of P&L. Missing fields on a tick never force an exit.
        """
        local = snap.local_gex
        shelf = snap.profile_gex_down if position.direction == "bearish" else snap.profile_gex_up
        other = snap.profile_gex_up if position.direction == "bearish" else snap.profile_gex_down
        if shelf is not None and local is not None and local > 0:
            refill_frac = float(self.params.get("shelf_refill_local_frac", 0.4))
            if shelf > -refill_frac * local:
                return ExitDecision(should_close=True, reason="shelf_refilled")
            if other is not None and other < 0:
                min_asym = float(self.params.get("exit_asymmetry_ratio", 1.3))
                if abs(shelf) < min_asym * abs(other):
                    return ExitDecision(should_close=True, reason="shelf_two_sided")
        return super().exit_criteria(snap, position)
