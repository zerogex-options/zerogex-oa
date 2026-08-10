"""AggressorFlowDivergence — lead price with aggressor-classified option flow.

No bot in the retired fleet ever looked at option ORDER FLOW — every one of
them traded standing positioning (walls, flip, max-pain, net-GEX). ZeroGEX
classifies every option trade by aggressor (Lee-Ready) and rolls it into
``flow_series_5min``: ``net_premium_cum`` is the day-to-date signed premium
(calls bought − sold, minus puts bought − sold), and its 5-minute delta is the
freshest lean. On liquid names this aggressive net premium leads price by tens
of seconds (the platform's own ``zero_dte_imbalance_drift`` documents the lag).

The edge here is a *divergence*: real money is aggressively paying up on one
side (large, still-accelerating ``net_premium``) while price has NOT yet moved
to match. That gap is a coiled directional move — enter in the flow direction
before price closes it. It fires only when the flow is confirmed on BOTH
premium and volume, and it stands down in a strong positive-γ pin (where
dealers absorb the flow and the lead-lag breaks).

Structure is a defined-risk 0DTE debit vertical in the flow direction; the
thesis is invalidated (small stop) the moment price moves *against* the flow.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class AggressorFlowDivergence(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Aggressive option flow leans hard; price hasn't caught up. Lead it."
    description = (
        "Enters in the direction of strong, still-accelerating aggressor-"
        "classified net option premium (flow_series_5min) when price has not "
        "yet moved to match — a lead-lag divergence. Confirmed on premium AND "
        "volume; stands down in a strong positive-γ pin."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        # Session window: past the opening auction, before the EOD charm regime.
        m = snap.minutes_since_open
        if m is None or m < float(self.params.get("min_minutes_since_open", 30)):
            return self._skip("window_open")
        mtc = snap.minutes_to_close
        if mtc is None or mtc < float(self.params.get("min_minutes_to_close", 60)):
            return self._skip("window_close")

        # In a strong positive-γ pin dealers absorb aggressive flow — the
        # lead-lag that powers this trade breaks down. Trade thin / one-sided
        # regimes.
        if snap.gex_regime() == "positive_strong":
            return self._skip("regime_pin")

        net_prem = snap.flow_net_premium
        net_vol = snap.flow_net_volume
        if net_prem is None or net_vol is None or net_prem == 0.0:
            return self._skip("no_flow")

        # Strong one-sided premium, confirmed by same-sign volume.
        min_prem = float(self.params.get("min_net_premium", 5.0e5))
        if abs(net_prem) < min_prem:
            return self._skip("premium_small")
        if (net_prem > 0) != (net_vol > 0):
            return self._skip("vol_disagrees")  # premium and volume disagree

        # Freshness: the flow must be still building this bucket, same sign as
        # the cumulative lean (fading flow is not an entry). Optional — only
        # applies when a prior bucket exists.
        delta = snap.flow_premium_delta()
        if delta is not None and (delta > 0) != (net_prem > 0):
            return self._skip("flow_fading")

        # Divergence: price must NOT already have run with the flow. Measure the
        # recent close-to-close move; if it already agrees strongly, the edge
        # (leading price) is gone.
        closes = [float(c) for c in (snap.recent_closes or []) if c is not None and c > 0]
        lookback = int(self.params.get("divergence_lookback_bars", 5))
        if len(closes) >= lookback and closes[-lookback] > 0:
            recent_move = (closes[-1] - closes[-lookback]) / closes[-lookback]
        else:
            recent_move = 0.0
        max_move = float(self.params.get("max_price_move_pct", 0.0025))
        direction = "bullish" if net_prem > 0 else "bearish"
        # Price has already caught up if it moved >= max_move WITH the flow.
        if direction == "bullish" and recent_move >= max_move:
            return self._skip("price_caught_up")
        if direction == "bearish" and recent_move <= -max_move:
            return self._skip("price_caught_up")

        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        # Quality: premium size (normalized), plus a bonus for a tighter
        # divergence (less price catch-up = more coiled).
        prem_norm = float(self.params.get("net_premium_norm", 3.0e6))
        prem_score = min(1.0, abs(net_prem) / prem_norm) if prem_norm > 0 else 0.0
        coil_score = max(0.0, 1.0 - abs(recent_move) / max(1e-9, max_move))
        quality = 0.7 * prem_score + 0.3 * coil_score
        ml_components = {
            "flow_net_premium": net_prem,
            "flow_net_volume": net_vol,
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
        hold = int(self.params.get("max_hold_minutes", 60))

        # Target: nearest wall in the flow direction, else a fixed extension.
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
                f"net option premium {net_prem:+.2e} ({direction}), vol confirms, "
                f"price only {recent_move * 100:+.2f}% so far — lead the divergence"
            ),
            components_at_entry={
                "flow_net_premium": net_prem,
                "flow_net_premium_prev": snap.flow_net_premium_prev,
                "flow_net_volume": net_vol,
                "recent_move_pct": recent_move,
                "net_gex": snap.net_gex,
                "quality": quality,
            },
        )
