"""CharmCloseMagnet — ride the *quantified* end-of-day charm hedging flow.

The retired ``EodPinDrifter`` drifted toward ``max_pain`` on nothing more than
displacement + time-of-day, and ``MaxPainGravitator`` used the OI-based
max-pain magnet. Both were folklore ("it's late, dealers pin") and the backtest
found no edge.

This bot trades the *mechanism* the folklore is a shadow of. ZeroGEX models the
actual forced flow: ``forced_flow_profile.close_charm_flow`` is the dollars of
stock dealers MUST trade by the cash close if spot holds — a scheduled,
sign-known hedging flow driven by charm (the decay of dealer deltas into
expiry). The **direction is the sign of that forced flow**, not a guess.

The edge filter the folklore lacks: the bot fires only when the forced charm
flow AGREES with a real magnet (the drift has somewhere structural to go) in a
positive-γ regime, in the final window when charm accelerates.

Magnet resolution: the gamma-restoring **Pin Strike** is the ideal target, but
it is only persisted when a meaningful pin exists (rare). So the magnet is
``pin_strike`` when present, else ``max_pain`` (always available) — and the bot
requires that magnet to sit on the *same side* as the forced flow. That keeps
the strategy runnable on real data while preserving the differentiator: unlike
the retired pin-drifters, it demands the QUANTIFIED close_charm_flow point the
same way before it will trade.

Structure is a narrow debit vertical toward the magnet (defined risk — a naked
0DTE debit bleeds too fast in the final two hours). Target is the magnet; stop
is a move to the wrong side of the charm-flip level (where charm hedging
reverses), or a fixed adverse buffer when that level is unavailable.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class CharmCloseMagnet(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Ride the forced charm flow into the magnet. Quantified, not folklore."
    description = (
        "Final-window drift in the direction of the modeled close_charm_flow "
        "(the dollars dealers must trade by the close), confirmed by a magnet "
        "(Pin Strike if present, else max_pain) on the same side. Positive-γ, "
        "defined-risk vertical."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        # -- Final-window gate: charm hedging accelerates in the last ~2 hours.
        mtc = snap.minutes_to_close
        max_mtc = float(self.params.get("max_minutes_to_close", 120))
        if mtc is None or mtc <= 0 or mtc > max_mtc:
            return self._skip("window")
        if mtc < float(self.params.get("min_minutes_to_close", 10)):
            return self._skip("too_close_to_close")

        # -- Regime gate: the charm PIN (restoring drift) is a positive-γ
        # phenomenon; in negative γ the same decay AMPLIFIES trend (another
        # bot's job).
        if snap.gex_regime() not in {"positive_strong", "positive_weak"}:
            return self._skip("regime")

        # -- The signal: the modeled forced charm flow sets the direction.
        ccf = snap.close_charm_flow
        if ccf is None or ccf == 0.0:
            return self._skip("no_charm_flow")
        direction = "bullish" if ccf > 0 else "bearish"

        # -- Magnet: pin_strike if a real pin exists, else max_pain. It must sit
        # on the same side as the forced flow (the drift has somewhere to go and
        # the mechanism agrees with the structure).
        magnet = snap.pin_strike if snap.pin_strike is not None else snap.max_pain
        if magnet is None or snap.spot <= 0:
            return self._skip("no_magnet")
        drift = (magnet - snap.spot) / snap.spot
        if (direction == "bullish") != (drift > 0):
            return self._skip("magnet_wrong_side")
        min_drift = float(self.params.get("min_drift_pct", 0.001))
        max_drift = float(self.params.get("max_drift_pct", 0.010))
        if abs(drift) < min_drift or abs(drift) > max_drift:
            return self._skip("drift_band")

        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        # -- Quality: regime strength (symbol-relative when a pctile exists),
        # how much room the drift has, and a mild pin-confidence bonus when a
        # real pin is present. No hard magnitude gate on close_charm_flow (its
        # dollar scale is symbol-dependent and we have no percentile for it yet)
        # — selectivity comes from the regime + sign-agreement + drift gates.
        if snap.net_gex_pctile is not None:
            regime_score = min(1.0, max(0.0, snap.net_gex_pctile / 100.0))
        else:
            regime_score = min(1.0, max(0.0, (snap.net_gex or 0.0) / 3.0e9))
        room_score = min(1.0, abs(drift) / max(1e-9, float(self.params.get("room_ref_pct", 0.004))))
        pin_bonus = float(snap.pin_confidence) if snap.pin_confidence is not None else 0.0
        quality = 0.5 * regime_score + 0.3 * room_score + 0.2 * pin_bonus
        ml_components = {
            "close_charm_flow": ccf,
            "net_gex": snap.net_gex,
            "drift_pct": drift,
            "pin_confidence": snap.pin_confidence,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

        # -- Defined-risk vertical toward the magnet.
        dte_target = int(self.params.get("dte_target", 0))
        expiration = resolve_expiration_iso(snap.et_date, dte_target)
        inc = snap.effective_strike_increment()
        long_strike = snap.round_to_strike(snap.spot)
        max_width = int(self.params.get("max_spread_width", 3))
        width = max(1, min(max_width, round(abs(magnet - snap.spot) / inc)))
        opt_type = "call" if direction == "bullish" else "put"
        short_strike = long_strike + (width * inc if direction == "bullish" else -width * inc)
        legs = self.build_vertical(snap.underlying, opt_type, long_strike, short_strike, expiration)
        strat = "BULL_CALL_DEBIT_SPREAD" if direction == "bullish" else "BEAR_PUT_DEBIT_SPREAD"
        hold = int(self.params.get("max_hold_minutes", 90))

        target = magnet
        stop_pct = float(self.params.get("stop_pct", 0.004))
        if direction == "bullish":
            stop = snap.charm_flip if (snap.charm_flip and snap.charm_flip < snap.spot) else None
            stop = stop if stop is not None else snap.spot * (1.0 - stop_pct)
        else:
            stop = snap.charm_flip if (snap.charm_flip and snap.charm_flip > snap.spot) else None
            stop = stop if stop is not None else snap.spot * (1.0 + stop_pct)

        magnet_kind = "pin" if snap.pin_strike is not None else "max_pain"
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
                f"close_charm_flow {ccf:+.2e} ({'buy' if ccf > 0 else 'sell'}) -> "
                f"{magnet_kind} {magnet} ({drift * 100:+.2f}% away); "
                f"{snap.gex_regime()}; {mtc:.0f}m to close"
            ),
            components_at_entry={
                "close_charm_flow": ccf,
                "magnet": magnet,
                "magnet_kind": magnet_kind,
                "pin_strike": snap.pin_strike,
                "pin_confidence": snap.pin_confidence,
                "max_pain": snap.max_pain,
                "drift_pct": drift,
                "charm_flip": snap.charm_flip,
                "net_gex": snap.net_gex,
                "minutes_to_close": mtc,
                "quality": quality,
            },
        )
