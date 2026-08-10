"""FreshFlowMomentum — ride a FRESH aggressor-flow burst that leads price.

The shelved ``aggressor_flow_divergence`` keyed on the day-to-date CUMULATIVE
net option premium and required price to be flat (a strict divergence). It
screened at PF 0.31 over 404 trades — a decisive no-edge. The post-mortem: the
cumulative figure is a LAGGING aggregate (by afternoon it is mostly the
morning's flow), so it was noise, not a lead.

This is a NEW hypothesis, not a revival. It keys on the *fresh* windowed flow —
the net aggressor premium over the last ~15 minutes
(``flow_recent_premium``) — and on its ACCELERATION versus the window before it
(``flow_prior_window_premium``). A genuine burst of aggressive one-sided premium
that is BIGGER than the prior window, confirmed on volume, and that price has
only *begun* to follow, is the platform's documented ~30s lead-lag: enter in the
flow direction and ride the pulse (a momentum-continuation trade, not the old
fade). It stands down in a strong positive-γ pin, where dealers absorb the flow.

Structure is a defined-risk 0DTE debit vertical in the flow direction; target is
the first wall that way (else a fixed extension); the stop is a move against the
flow (the pulse thesis is spent).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class FreshFlowMomentum(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Fresh aggressor-flow burst leads price. Ride the pulse."
    description = (
        "Enters in the direction of a FRESH, ACCELERATING burst of aggressor-"
        "classified net option premium (last ~15 min vs the prior window), "
        "confirmed on volume, that price has only begun to follow. Stands down "
        "in a strong positive-γ pin. Fresh-flow successor to the shelved "
        "cumulative-flow divergence bot."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        # Session window: past the opening auction, before the EOD charm regime.
        m = snap.minutes_since_open
        if m is None or m < float(self.params.get("min_minutes_since_open", 30)):
            return self._skip("window_open")
        mtc = snap.minutes_to_close
        if mtc is None or mtc < float(self.params.get("min_minutes_to_close", 45)):
            return self._skip("window_close")

        # A strong positive-γ pin absorbs aggressive flow — the lead-lag breaks.
        if snap.gex_regime() == "positive_strong":
            return self._skip("regime_pin")

        rp = snap.flow_recent_premium
        rv = snap.flow_recent_volume
        if rp is None or rv is None or rp == 0.0:
            return self._skip("no_recent_flow")

        # Premium and volume must lean the SAME way.
        if (rp > 0) != (rv > 0):
            return self._skip("vol_disagrees")

        # Material fresh burst only. (Absolute floor — flow dollars are symbol-
        # scaled and we have no percentile for them yet; documented caveat.)
        min_rp = float(self.params.get("min_recent_premium", 2.0e5))
        if abs(rp) < min_rp:
            return self._skip("flow_small")

        # Acceleration: the recent window must exceed the prior window by
        # ``accel_mult`` — a building burst, not a fading one. When there is no
        # prior window yet (early session) the magnitude floor alone governs.
        pr = snap.flow_prior_window_premium
        accel_mult = float(self.params.get("accel_mult", 1.15))
        if pr is not None and abs(rp) < accel_mult * abs(pr):
            return self._skip("not_accelerating")

        direction = "bullish" if rp > 0 else "bearish"

        # Price must have only BEGUN to follow — don't chase a move that already
        # ran (the lead is spent). Uses the recent close-to-close move.
        closes = [float(c) for c in (snap.recent_closes or []) if c is not None and c > 0]
        lookback = int(self.params.get("overrun_lookback_bars", 5))
        if len(closes) >= lookback and closes[-lookback] > 0:
            recent_move = (closes[-1] - closes[-lookback]) / closes[-lookback]
        else:
            recent_move = 0.0
        max_move = float(self.params.get("max_price_move_pct", 0.004))
        if direction == "bullish" and recent_move >= max_move:
            return self._skip("price_overrun")
        if direction == "bearish" and recent_move <= -max_move:
            return self._skip("price_overrun")

        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        # Quality: fresh magnitude + acceleration.
        prem_norm = float(self.params.get("recent_premium_norm", 1.0e6))
        mag_score = min(1.0, abs(rp) / prem_norm) if prem_norm > 0 else 0.0
        if pr is not None and abs(pr) > 0:
            accel_ratio = abs(rp) / abs(pr)
            accel_score = max(0.0, min(1.0, accel_ratio - 1.0))
        else:
            accel_score = 0.5
        quality = 0.6 * mag_score + 0.4 * accel_score
        ml_components = {
            "flow_recent_premium": rp,
            "flow_recent_volume": rv,
            "flow_prior_window_premium": pr,
            "recent_move": recent_move,
            "net_gex": snap.net_gex,
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
        hold = int(self.params.get("max_hold_minutes", 45))

        target_pct = float(self.params.get("target_pct", 0.004))
        stop_pct = float(self.params.get("stop_pct", 0.003))
        if direction == "bullish":
            target = snap.call_wall if (snap.call_wall and snap.call_wall > snap.spot) else None
            target = target if target is not None else snap.spot * (1.0 + target_pct)
            stop = snap.spot * (1.0 - stop_pct)
        else:
            target = snap.put_wall if (snap.put_wall and snap.put_wall < snap.spot) else None
            target = target if target is not None else snap.spot * (1.0 - target_pct)
            stop = snap.spot * (1.0 + stop_pct)

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
                f"fresh flow {rp:+.2e} ({direction}) accelerating vs prior "
                f"{pr if pr is None else f'{pr:+.2e}'}; price {recent_move * 100:+.2f}% so far"
            ),
            components_at_entry={
                "flow_recent_premium": rp,
                "flow_recent_volume": rv,
                "flow_prior_window_premium": pr,
                "flow_net_premium_cum": snap.flow_net_premium,
                "recent_move_pct": recent_move,
                "net_gex": snap.net_gex,
                "quality": quality,
            },
        )
