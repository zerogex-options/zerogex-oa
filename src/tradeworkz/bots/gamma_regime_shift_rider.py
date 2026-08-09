"""GammaRegimeShiftRider — trade the regime *transition*, not a static level.

The retired ``GammaFlipBreaker`` traded a static price break of the gamma-flip
level; ``DealerDeltaPressureRider`` traded a static net-GEX sign. Both read a
level and both were dead in the backtest — the level is published on every
GEX dashboard and arbitraged.

The edge is in the *derivative*. When dealer net gamma is collapsing tick over
tick (``net_gex_change`` strongly negative) toward/through zero while spot is
right at the gamma flip and convexity risk is elevated, the market is
CHANGING regime — long-gamma pinning giving way to short-gamma trending. That
transition is when a move stops being absorbed and starts being amplified by
dealer hedging. Riding the transition (net-GEX velocity + flip proximity +
convexity), confirmed by same-direction aggressor volume, catches the leg the
static break bots missed by firing after the structure had already settled.

Structure is a defined-risk 0DTE debit vertical in the break direction. The
stop is a *reclaim of the flip* — if spot pushes back to the pinning side, the
regime shift aborted and the thesis is gone.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class GammaRegimeShiftRider(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Dealer gamma is flipping short at the flip. Ride the regime break."
    description = (
        "Enters as dealer net gamma collapses toward short (net_gex_change "
        "strongly negative) with spot at the gamma flip and convexity elevated "
        "— the long→short regime transition that turns absorption into "
        "amplification. Flow-confirmed; stop is a reclaim of the flip."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        m = snap.minutes_since_open
        if m is None or m < float(self.params.get("min_minutes_since_open", 30)):
            return None
        mtc = snap.minutes_to_close
        if mtc is None or mtc < float(self.params.get("min_minutes_to_close", 45)):
            return None

        # -- Regime-velocity gate: dealers must be SHEDDING long gamma fast.
        dgex = snap.net_gex_change()
        if dgex is None:
            return None
        min_drop = float(self.params.get("min_net_gex_drop", 5.0e8))
        if dgex > -min_drop:  # not collapsing fast enough (dgex is negative)
            return None
        # And the shed must be leaving positive-γ territory — not a dip deeper
        # into an already-strong short regime (that's a trend-continuation bot).
        if snap.net_gex is None or snap.prior_net_gex is None:
            return None
        if snap.prior_net_gex <= 0:
            return None  # was already short — this bot wants the crossing
        if snap.gex_regime() == "positive_strong":
            return None  # still firmly pinned; the transition hasn't taken

        # -- Structural gate: spot at the flip, convexity elevated.
        if snap.gamma_flip is None or snap.flip_distance is None:
            return None
        max_flip_dist = float(self.params.get("max_flip_distance_pct", 0.003))
        if abs(snap.flip_distance) > max_flip_dist:
            return None

        # -- Direction: the way the tape is breaking, confirmed by flow.
        closes = [float(c) for c in (snap.recent_closes or []) if c is not None and c > 0]
        n = int(self.params.get("trend_lookback_bars", 5))
        if len(closes) < n or closes[-n] <= 0:
            return None
        trend = (closes[-1] - closes[-n]) / closes[-n]
        if abs(trend) < float(self.params.get("min_break_trend_pct", 0.0010)):
            return None
        direction = "bullish" if trend > 0 else "bearish"

        # Aggressor volume must confirm the break direction.
        net_vol = snap.flow_net_volume
        if net_vol is not None and net_vol != 0.0:
            if (net_vol > 0) != (direction == "bullish"):
                return None

        if self._bias_veto(snap, direction):
            return None

        # Quality: speed of the gamma shed, convexity, and break strength.
        drop_norm = float(self.params.get("net_gex_drop_norm", 2.0e9))
        drop_score = min(1.0, abs(dgex) / drop_norm) if drop_norm > 0 else 0.0
        conv = snap.convexity_risk or 0.0
        conv_norm = float(self.params.get("convexity_norm", 5.0e11))
        conv_score = min(1.0, conv / max(1e-9, conv_norm))
        trend_ref = float(self.params.get("trend_ref_pct", 0.003))
        trend_score = min(1.0, abs(trend) / max(1e-9, trend_ref))
        quality = 0.45 * drop_score + 0.25 * conv_score + 0.30 * trend_score
        ml_components = {
            "net_gex_change": dgex,
            "convexity_risk": conv,
            "flip_distance": snap.flip_distance,
            "trend": trend,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return None

        dte_target = int(self.params.get("dte_target", 0))
        expiration = resolve_expiration_iso(snap.et_date, dte_target)
        inc = snap.effective_strike_increment()
        long_strike = snap.round_to_strike(snap.spot)
        width = max(1, int(self.params.get("spread_width", 2)))
        opt_type = "call" if direction == "bullish" else "put"
        short_strike = long_strike + (width * inc if direction == "bullish" else -width * inc)
        legs = self.build_vertical(snap.underlying, opt_type, long_strike, short_strike, expiration)
        strat = "BULL_CALL_DEBIT_SPREAD" if direction == "bullish" else "BEAR_PUT_DEBIT_SPREAD"
        hold = int(self.params.get("max_hold_minutes", 60))

        # Target = the far wall in the break direction (short γ lets price run to
        # it), else a fixed extension. Stop = reclaim of the flip.
        target_pct = float(self.params.get("target_pct", 0.006))
        if direction == "bullish":
            target = snap.call_wall if (snap.call_wall and snap.call_wall > snap.spot) else None
            target = target if target is not None else snap.spot * (1.0 + target_pct)
            stop = snap.gamma_flip * (1.0 - float(self.params.get("flip_reclaim_pct", 0.0008)))
        else:
            target = snap.put_wall if (snap.put_wall and snap.put_wall < snap.spot) else None
            target = target if target is not None else snap.spot * (1.0 - target_pct)
            stop = snap.gamma_flip * (1.0 + float(self.params.get("flip_reclaim_pct", 0.0008)))

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
                f"net_gex {snap.prior_net_gex:+.2e}→{snap.net_gex:+.2e} (Δ{dgex:+.2e}) "
                f"at flip {snap.gamma_flip:.2f} ({snap.flip_distance * 100:+.2f}%); "
                f"break {direction}, flow confirms"
            ),
            components_at_entry={
                "net_gex_change": dgex,
                "net_gex": snap.net_gex,
                "prior_net_gex": snap.prior_net_gex,
                "convexity_risk": conv,
                "flip_distance": snap.flip_distance,
                "trend": trend,
                "flow_net_volume": net_vol,
                "quality": quality,
            },
        )
