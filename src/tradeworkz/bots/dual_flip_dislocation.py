"""DualFlipDislocation — trade the disagreement band between the two flips.

ZeroGEX computes the dealer gamma flip under two conventions and persists
both. ``gamma_flip`` (structural) weights each contract by DTE horizon
occupancy — where the full structural book flips. ``gamma_flip_raw`` is the
nearest zero crossing of the UN-weighted profile — 0DTE walls pull it toward
spot, i.e. it locates where the *fast-rehedging intraday book* flips. Nobody
consumes the raw figure; every competitor publishes ONE flip under ONE
convention, so the SPREAD between conventions is a ZeroGEX-only quantity.

The amplification band is the price region below the raw flip but above the
structural flip (raw > structural): there the 0DTE book — the desks that
re-hedge continuously because their gamma per unit time is enormous — is in
NEGATIVE gamma and amplifies every move, while the structural longs who would
offset them rebalance on a daily clock and don't show up intraday. A fresh
price entry into that band therefore rides forced fast-book hedging toward the
band's far edge:

* spot crosses DOWN through ``gamma_flip_raw`` into the band => short,
  target = the structural flip (the band's lower edge);
* spot crosses UP through the structural flip into the band (from below,
  where both books were short gamma) => long, target = ``gamma_flip_raw``
  (above the raw flip the fast book turns positive and stabilizes).

When the raw flip sits BELOW the structural flip the fast book is positive
gamma inside the spread — dampening, no trade. The retired flip bots traded a
single published level statically; ``gamma_regime_shift_rider`` trades realized
net-GEX sign transitions. This bot trades neither: it trades the disagreement
GEOMETRY, and only on a fresh, momentum-confirmed crossing.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src import config as app_config
from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, OpenPosition, TradeSignal


class DualFlipDislocation(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Two flips disagree. Ride the fast book's short gamma across the band."
    description = (
        "Fires when spot freshly enters the band between the un-DTE-weighted "
        "gamma_flip_raw (where the 0DTE fast book flips) and the structural "
        "DTE-weighted gamma_flip — the state where the intraday hedgers are "
        "short gamma while published dashboards still read positive. Momentum-"
        "confirmed, targets the band's far edge, stops on a re-cross. "
        "Defined-risk vertical."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        m = snap.minutes_since_open
        mtc = snap.minutes_to_close
        if m is None or m < float(self.params.get("min_minutes_since_open", 30)):
            return self._skip("window")
        if mtc is None or mtc < float(self.params.get("min_minutes_to_close", 60)):
            return self._skip("window")

        raw = snap.gamma_flip_raw
        structural = snap.gamma_flip
        if raw is None or structural is None or snap.spot <= 0:
            return self._skip("no_flip_pair")

        # -- Structural-flip quality: a flip resolved only on an expansion
        # rung of the adaptive span ladder is marginal by the engine's own
        # definition — don't anchor a trade to it.
        span_used = snap.gamma_flip_span_used
        span_gate = float(self.params.get("max_flip_span_used", app_config.GAMMA_PROFILE_SPAN_PCT))
        if span_used is not None and span_used > span_gate:
            return self._skip("marginal_flip")

        # -- The band must be real (relative width) and correctly ordered:
        # amplification requires the raw (fast-book) flip ABOVE the
        # structural flip so the region between is fast-short / slow-long.
        band_pct = abs(raw - structural) / snap.spot
        min_band = float(self.params.get("min_band_pct", 0.002))
        if band_pct < min_band:
            return self._skip("band_thin")
        if raw <= structural:
            return self._skip("band_inverted")

        # -- Disagreement at spot: the two conventions must classify the
        # current price differently (spot inside the band).
        if not (structural < snap.spot < raw):
            return self._skip("outside_band")

        # -- Fresh crossing, with momentum: no chasing a band spot has sat in
        # for an hour. recent_closes are 1-min bars oldest->newest, but the
        # engine (and the replay) evaluate every ~5 minutes — so "fresh" is
        # judged over the last ``cross_lookback_bars`` one-minute closes, not
        # only the single previous bar (the first screen showed a strict
        # prev-bar test made valid bands nearly unenterable at 5-min cadence:
        # 2 entries in 60 days against ~1,500 in-band ticks).
        closes = [c for c in (snap.recent_closes or []) if c and c > 0]
        lookback = max(2, int(self.params.get("cross_lookback_bars", 6)))
        mom_bars = max(2, int(self.params.get("momentum_bars", 2)))
        if len(closes) < max(lookback, mom_bars) + 1:
            return self._skip("no_history")
        window = closes[-1 - lookback : -1]  # the bars before the current one
        momentum = (closes[-1] - closes[-1 - mom_bars]) / closes[-1 - mom_bars]
        min_mom = float(self.params.get("min_momentum_pct", 0.0005))

        direction: Optional[str] = None
        target: Optional[float] = None
        stop_edge: Optional[float] = None
        if max(window) >= raw and snap.spot < raw and momentum <= -min_mom:
            # Down-cross of the raw flip: fast book just went short gamma,
            # amplifying the decline toward the structural flip below.
            direction, target, stop_edge = "bearish", structural, raw
        elif min(window) <= structural and snap.spot > structural and momentum >= min_mom:
            # Up-cross of the structural flip from below: fast book still
            # short gamma inside the band, amplifying the rally toward the
            # raw flip above (where it turns positive and stabilizes).
            direction, target, stop_edge = "bullish", raw, structural
        if direction is None:
            return self._skip("no_fresh_cross")

        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        # Quality saturates just above the entry floors (band 0.2%, momentum
        # 0.05%) so a gate-passing setup can also pass conviction — the second
        # screen's wider scale rated at-floor setups ~0.37 and killed 3 of the
        # 6 fresh crosses at the conviction gate.
        quality = 0.6 * min(1.0, band_pct / 0.003) + 0.4 * min(1.0, abs(momentum) / 0.00075)
        ml_components = {
            "band_pct": band_pct,
            "gamma_flip_raw": raw,
            "gamma_flip": structural,
            "momentum": momentum,
            "net_gex": snap.net_gex,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

        # -- Structure: vertical spanning the band; short leg at the target
        # edge so max payoff is congruent with the mechanical exhaustion level.
        expiration = resolve_expiration_iso(snap.et_date, int(self.params.get("dte_target", 0)))
        inc = snap.effective_strike_increment()
        max_width = max(1, int(self.params.get("max_spread_width", 3)))
        long_strike = snap.round_to_strike(snap.spot)
        width = max(1, min(max_width, round(abs(target - snap.spot) / inc)))
        short_strike = long_strike + (width * inc if direction == "bullish" else -width * inc)
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_vertical(snap.underlying, opt_type, long_strike, short_strike, expiration)
        strat = "BULL_CALL_DEBIT_SPREAD" if direction == "bullish" else "BEAR_PUT_DEBIT_SPREAD"

        # Stop: a re-cross back over the entry edge (buffered) — the fast
        # book flipped back and the amplification thesis is dead.
        buffer_pct = float(self.params.get("stop_buffer_pct", 0.001))
        stop = (
            stop_edge * (1.0 + buffer_pct)
            if direction == "bearish"
            else stop_edge * (1.0 - buffer_pct)
        )
        hold = int(self.params.get("max_hold_minutes", 75))

        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=direction,
            strategy_type=strat,
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=target,
            stop_price=stop,
            time_stop_at=_utcnow() + timedelta(minutes=hold),
            rationale=(
                f"spot {snap.spot:.2f} freshly inside flip band "
                f"[{structural:.2f}, {raw:.2f}] ({band_pct * 100:.2f}% wide), "
                f"{mom_bars}-bar momentum {momentum * 100:+.2f}% -> {direction} "
                f"toward {target:.2f}"
            ),
            components_at_entry={
                "gamma_flip_raw": raw,
                "gamma_flip": structural,
                "band_pct": band_pct,
                "entry_edge": stop_edge,
                "momentum": momentum,
                "quality": quality,
            },
        )

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
        """Default stack plus band-geometry invalidation.

        The band can vanish under the position — new positioning collapses
        the width, the flip ordering inverts, or the structural flip stops
        resolving on the base rung. Any of those means the disagreement state
        the trade priced no longer exists: close regardless of P&L. Missing
        fields on a tick never force an exit.
        """
        raw = snap.gamma_flip_raw
        structural = snap.gamma_flip
        if raw is not None and structural is not None and snap.spot > 0:
            band_pct = abs(raw - structural) / snap.spot
            collapse = float(self.params.get("band_collapse_pct", 0.001))
            if band_pct < collapse:
                return ExitDecision(should_close=True, reason="band_collapse")
            if raw <= structural:
                return ExitDecision(should_close=True, reason="band_inverted")
        return super().exit_criteria(snap, position)
