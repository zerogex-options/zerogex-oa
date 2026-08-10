"""ClimaxFlowFade — FADE an aggressive 0DTE flow burst that marks a local climax.

Two follow-the-flow bots (aggressor_flow_divergence, fresh_flow_momentum) were
screened out with the SAME signature: ~33% win rate, wins smaller than losses, a
steady bleed. That asymmetry is diagnostic — chasing an aggressive 0DTE flow
burst systematically buys a LOCAL EXTREME that reverts. The signal is real; its
sign was backwards for a momentum trade.

So this bot flips both knobs the follow-bots had wrong:

* **Direction is OPPOSITE the flow.** A fresh call-led buying burst that has
  popped price is a short; a put-led selling burst that has dumped price is a
  long. We fade the climax, betting on the reversion the follow-bots kept
  eating.
* **Regime is POSITIVE gamma** (the follow-bots stood down there). Positive
  dealer gamma is the mean-reverting regime — dealers sell rallies / buy dips —
  so a flow-driven overshoot is exactly what gets pulled back.

The differentiator from the retired VWAP-reversion / wall-fade bots (which also
faded extension in positive gamma and failed) is the TRIGGER: not just "price is
stretched", but "a large, aggressive, volume-confirmed FLOW BURST just pushed it
there" — the climax/exhaustion signal those bots lacked.

Structure is a defined-risk 0DTE debit vertical in the fade direction; target is
the mean (VWAP when available, else a modest retrace); the stop is a
CONTINUATION beyond the extreme (if the flow keeps winning, the fade is wrong).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class ClimaxFlowFade(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Aggressive flow burst spiked price into a pin. Fade the climax."
    description = (
        "Fades a large, volume-confirmed FRESH aggressor-flow burst that has "
        "overshot price, in a positive-γ (mean-reverting) regime — betting on "
        "the reversion the screened-out follow-the-flow bots kept eating. "
        "Defined-risk vertical; target is the mean (VWAP), stop is a "
        "continuation of the burst."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        # Session window: past the opening auction, before the EOD charm regime.
        m = snap.minutes_since_open
        if m is None or m < float(self.params.get("min_minutes_since_open", 30)):
            return self._skip("window_open")
        mtc = snap.minutes_to_close
        if mtc is None or mtc < float(self.params.get("min_minutes_to_close", 45)):
            return self._skip("window_close")

        # Mean reversion lives in POSITIVE gamma (dealers absorb the overshoot).
        if snap.gex_regime() not in {"positive_strong", "positive_weak"}:
            return self._skip("regime")

        rp = snap.flow_recent_premium
        rv = snap.flow_recent_volume
        if rp is None or rv is None or rp == 0.0:
            return self._skip("no_recent_flow")
        # A real burst: premium and volume lean the same way.
        if (rp > 0) != (rv > 0):
            return self._skip("vol_disagrees")
        # Material burst only (absolute floor — flow dollars are symbol-scaled;
        # documented caveat, same as the follow-bots).
        min_rp = float(self.params.get("min_recent_premium", 3.0e5))
        if abs(rp) < min_rp:
            return self._skip("flow_small")

        # The overshoot to fade: price must have EXTENDED WITH the flow recently
        # (the burst pushed it to a local extreme). Flow sign = call-led/bullish
        # => we need price UP to fade it short.
        closes = [float(c) for c in (snap.recent_closes or []) if c is not None and c > 0]
        lookback = int(self.params.get("extension_lookback_bars", 5))
        if len(closes) < lookback or closes[-lookback] <= 0:
            return self._skip("no_price_data")
        recent_move = (closes[-1] - closes[-lookback]) / closes[-lookback]
        min_ext = float(self.params.get("min_extension_pct", 0.0015))
        flow_bullish = rp > 0
        # Extension must be in the FLOW direction (a same-signed overshoot).
        if flow_bullish and recent_move < min_ext:
            return self._skip("no_overshoot")
        if not flow_bullish and recent_move > -min_ext:
            return self._skip("no_overshoot")

        # Fade: trade OPPOSITE the flow / the overshoot.
        direction = "bearish" if flow_bullish else "bullish"
        # Don't fight a strongly trending tape — a fade into a runaway trend is
        # the retired fleet's short-into-an-up-day mistake.
        if self._trend_veto(snap, direction):
            return self._skip("trend_veto")
        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        # Quality: burst size, overshoot size, and depth of positive gamma
        # (stronger pin => stronger reversion).
        prem_norm = float(self.params.get("recent_premium_norm", 1.0e6))
        mag_score = min(1.0, abs(rp) / prem_norm) if prem_norm > 0 else 0.0
        ext_ref = float(self.params.get("extension_ref_pct", 0.004))
        ext_score = min(1.0, abs(recent_move) / max(1e-9, ext_ref))
        if snap.net_gex_pctile is not None:
            gamma_score = min(1.0, max(0.0, snap.net_gex_pctile / 100.0))
        else:
            gamma_score = min(1.0, max(0.0, (snap.net_gex or 0.0) / 3.0e9))
        quality = 0.4 * mag_score + 0.3 * ext_score + 0.3 * gamma_score
        ml_components = {
            "flow_recent_premium": rp,
            "flow_recent_volume": rv,
            "recent_move": recent_move,
            "net_gex": snap.net_gex,
            "vwap_deviation_pct": snap.vwap_deviation_pct,
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

        # Target = the mean (VWAP) when it sits on the profit side within a
        # reachable band, else a fixed retrace. Stop = continuation of the burst
        # beyond the extreme (the fade thesis is wrong if the flow keeps winning).
        target_pct = float(self.params.get("target_pct", 0.003))
        stop_pct = float(self.params.get("stop_pct", 0.004))
        max_target_pct = float(self.params.get("max_target_pct", 0.006))
        if direction == "bearish":
            fixed_target = snap.spot * (1.0 - target_pct)
            floor = snap.spot * (1.0 - max_target_pct)
            if snap.vwap is not None and floor <= snap.vwap < snap.spot:
                target = snap.vwap
            else:
                target = fixed_target
            stop = snap.spot * (1.0 + stop_pct)
        else:
            fixed_target = snap.spot * (1.0 + target_pct)
            ceil = snap.spot * (1.0 + max_target_pct)
            if snap.vwap is not None and snap.spot < snap.vwap <= ceil:
                target = snap.vwap
            else:
                target = fixed_target
            stop = snap.spot * (1.0 - stop_pct)

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
                f"fresh flow burst {rp:+.2e} popped price {recent_move * 100:+.2f}% "
                f"in {snap.gex_regime()} — fade {direction} toward the mean"
            ),
            components_at_entry={
                "flow_recent_premium": rp,
                "flow_recent_volume": rv,
                "recent_move_pct": recent_move,
                "net_gex": snap.net_gex,
                "vwap": snap.vwap,
                "vwap_deviation_pct": snap.vwap_deviation_pct,
                "quality": quality,
            },
        )
