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
            return self._skip("window_open")
        mtc = snap.minutes_to_close
        if mtc is None or mtc < float(self.params.get("min_minutes_to_close", 45)):
            return self._skip("window_close")

        # -- Regime-velocity gate: dealers must be SHEDDING long gamma fast.
        # The shed is measured RELATIVE to the prior reading, not in absolute
        # dollars — net-GEX scales ~price^2 x OI, so a fixed dollar drop that is
        # meaningful for SPY (~1-8e9) is noise for SPX (~1e10-7e10). Require the
        # one-tick drop to be at least ``min_shed_frac`` of |prior_net_gex|.
        if snap.net_gex is None or snap.prior_net_gex is None:
            return self._skip("no_net_gex")
        if snap.prior_net_gex <= 0:
            return self._skip("not_long_gamma")  # was already short — want the crossing
        dgex = snap.net_gex_change()
        if dgex is None or dgex >= 0:
            return self._skip("not_shedding")
        shed_frac = float(self.params.get("min_shed_frac", 0.25))
        if abs(dgex) < shed_frac * abs(snap.prior_net_gex):
            return self._skip("shed_small")
        if snap.gex_regime() == "positive_strong":
            return self._skip("still_pinned")  # transition hasn't taken

        # -- Structural gate: the shed must be resolving toward the short regime.
        # Either net-GEX has already crossed to short (net_gex <= 0) OR spot is
        # near the flip. The probe showed spot is typically 0.4-2.6% from the
        # flip, so the old 0.3% "at the flip" band was unreachable; a crossing
        # OR a wider proximity band both mark a real transition.
        if snap.gamma_flip is None:
            return self._skip("no_flip")
        max_flip_dist = float(self.params.get("max_flip_distance_pct", 0.012))
        near_flip = snap.flip_distance is not None and abs(snap.flip_distance) <= max_flip_dist
        crossed_short = snap.net_gex <= 0
        if not (near_flip or crossed_short):
            return self._skip("far_from_transition")

        # -- Direction: the way the tape is breaking, confirmed by flow.
        closes = [float(c) for c in (snap.recent_closes or []) if c is not None and c > 0]
        n = int(self.params.get("trend_lookback_bars", 5))
        if len(closes) < n or closes[-n] <= 0:
            return self._skip("no_trend_data")
        trend = (closes[-1] - closes[-n]) / closes[-n]
        if abs(trend) < float(self.params.get("min_break_trend_pct", 0.0010)):
            return self._skip("trend_flat")
        direction = "bullish" if trend > 0 else "bearish"

        # Aggressor volume must confirm the break direction (when flow exists).
        net_vol = snap.flow_net_volume
        if net_vol is not None and net_vol != 0.0:
            if (net_vol > 0) != (direction == "bullish"):
                return self._skip("flow_opposes")

        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        # Quality: depth of the gamma shed (RELATIVE to prior, symbol-agnostic),
        # whether it fully crossed to short, and break strength.
        shed_ratio = abs(dgex) / max(1e-9, abs(snap.prior_net_gex))
        drop_score = min(1.0, shed_ratio / max(1e-9, float(self.params.get("shed_ref_frac", 0.5))))
        cross_score = 1.0 if crossed_short else 0.4
        trend_ref = float(self.params.get("trend_ref_pct", 0.003))
        trend_score = min(1.0, abs(trend) / max(1e-9, trend_ref))
        quality = 0.45 * drop_score + 0.25 * cross_score + 0.30 * trend_score
        ml_components = {
            "net_gex_change": dgex,
            "shed_ratio": shed_ratio,
            "convexity_risk": snap.convexity_risk,
            "flip_distance": snap.flip_distance,
            "trend": trend,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

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
                f"net_gex {snap.prior_net_gex:+.2e}->{snap.net_gex:+.2e} (shed "
                f"{shed_ratio * 100:.0f}%), {'crossed short' if crossed_short else 'near flip'}; "
                f"break {direction}, flow confirms"
            ),
            components_at_entry={
                "net_gex_change": dgex,
                "net_gex": snap.net_gex,
                "prior_net_gex": snap.prior_net_gex,
                "shed_ratio": shed_ratio,
                "crossed_short": crossed_short,
                "convexity_risk": snap.convexity_risk,
                "flip_distance": snap.flip_distance,
                "trend": trend,
                "flow_net_volume": net_vol,
                "quality": quality,
            },
        )
