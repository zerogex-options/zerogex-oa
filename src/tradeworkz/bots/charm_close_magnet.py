"""CharmCloseMagnet — ride the *quantified* end-of-day charm hedging flow.

The retired ``EodPinDrifter`` drifted toward ``max_pain`` on nothing more than
displacement + time-of-day, and ``MaxPainGravitator`` used the OI-based
max-pain magnet. Both were folklore ("it's late, dealers pin to max pain") and
the backtest found no edge.

This bot trades the *mechanism* the folklore is a shadow of. ZeroGEX models the
actual forced flow: ``forced_flow_profile.close_charm_flow`` is the dollars of
stock dealers MUST trade by the cash close if spot holds — a scheduled,
sign-known hedging flow driven by charm (the decay of dealer deltas into
expiry). And the magnet is the gamma-restoring **Pin Strike**
(``pin_strike`` / ``pin_confidence``), not OI max-pain.

The edge is the *confirmation* the folklore lacks: the bot fires only when the
forced charm flow ACTUALLY POINTS at the pin (sign agreement) in a positive-γ
regime with a confident pin, in the final window when charm accelerates. When
the charm flow and the pin disagree, it abstains — exactly the setups where the
naive pin-drift bots lost money.

Structure is a narrow debit vertical toward the pin (defined risk — a naked
0DTE debit bleeds too fast in the final two hours). Target is the pin; stop is
a move AWAY from the pin past the charm-flip level (the drift thesis is dead if
spot pushes to the wrong side of where charm hedging reverses).
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
    tagline = "Ride the forced charm flow into the pin. Quantified, not folklore."
    description = (
        "Enters toward the gamma-restoring Pin Strike in the final hours only "
        "when the modeled close_charm_flow (the dollars dealers must trade by "
        "the close) actually points at the pin. Positive-γ, high pin "
        "confidence, defined-risk vertical."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        # -- Final-window gate: charm hedging accelerates in the last ~2 hours.
        mtc = snap.minutes_to_close
        max_mtc = float(self.params.get("max_minutes_to_close", 120))
        if mtc is None or mtc <= 0 or mtc > max_mtc:
            return None
        # Don't fire in the last few minutes — no time for the drift, and the
        # fill/settlement edge collapses.
        if mtc < float(self.params.get("min_minutes_to_close", 10)):
            return None

        # -- Regime gate: the charm PIN (restoring drift) is a positive-γ
        # phenomenon. In negative γ the same charm decay AMPLIFIES trend, which
        # is a different bot's job.
        if snap.gex_regime() not in {"positive_strong", "positive_weak"}:
            return None

        # -- Pin gate: need a confident, gamma-restoring magnet with room.
        if snap.pin_strike is None or snap.pin_confidence is None:
            return None
        if snap.pin_confidence < float(self.params.get("min_pin_confidence", 0.55)):
            return None
        drift = snap.distance_to_pin_pct()
        if drift is None:
            return None
        min_drift = float(self.params.get("min_drift_pct", 0.001))
        max_drift = float(self.params.get("max_drift_pct", 0.010))
        if abs(drift) < min_drift or abs(drift) > max_drift:
            return None

        # -- The edge filter: the modeled forced charm flow must POINT at the
        # pin. close_charm_flow > 0 => dealers must BUY into the close
        # (bullish); < 0 => must SELL. Only trade the pin when the mechanism
        # agrees with the direction the pin sits.
        ccf = snap.close_charm_flow
        if ccf is None or ccf == 0.0:
            return None
        direction = "bullish" if drift > 0 else "bearish"
        flow_is_bullish = ccf > 0
        if (direction == "bullish") != flow_is_bullish:
            return None  # charm flow disagrees with the pin — abstain

        # Don't fight the fleet's fused directional read.
        if self._bias_veto(snap, direction):
            return None

        # -- Quality: pin confidence dominates, then the size of the forced
        # flow, then how much room the drift has.
        charm_norm = float(self.params.get("close_charm_flow_norm", 8.0e8))
        charm_score = min(1.0, abs(ccf) / charm_norm) if charm_norm > 0 else 0.0
        room_score = min(1.0, abs(drift) / max(1e-9, float(self.params.get("room_ref_pct", 0.004))))
        quality = 0.5 * snap.pin_confidence + 0.35 * charm_score + 0.15 * room_score
        ml_components = {
            "pin_confidence": snap.pin_confidence,
            "close_charm_flow": ccf,
            "net_gex": snap.net_gex,
            "drift_pct": drift,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return None

        # -- Defined-risk vertical toward the pin. Long ATM, short a few strikes
        # toward the pin (narrow so 0DTE theta stays bounded).
        dte_target = int(self.params.get("dte_target", 0))
        expiration = resolve_expiration_iso(snap.et_date, dte_target)
        inc = snap.effective_strike_increment()
        long_strike = snap.round_to_strike(snap.spot)
        max_width = int(self.params.get("max_spread_width", 3))
        width = max(1, min(max_width, round(abs(snap.pin_strike - snap.spot) / inc)))
        opt_type = "call" if direction == "bullish" else "put"
        short_strike = long_strike + (width * inc if direction == "bullish" else -width * inc)
        legs = self.build_vertical(snap.underlying, opt_type, long_strike, short_strike, expiration)
        strat = "BULL_CALL_DEBIT_SPREAD" if direction == "bullish" else "BEAR_PUT_DEBIT_SPREAD"
        hold = int(self.params.get("max_hold_minutes", 90))

        target = snap.pin_strike
        # Stop: a move to the wrong side of the charm-flip level (where charm
        # hedging reverses) invalidates the drift. Fall back to a fixed adverse
        # buffer when charm_flip is unavailable.
        stop_pct = float(self.params.get("stop_pct", 0.004))
        if direction == "bullish":
            stop = snap.charm_flip if (snap.charm_flip and snap.charm_flip < snap.spot) else None
            stop = stop if stop is not None else snap.spot * (1.0 - stop_pct)
        else:
            stop = snap.charm_flip if (snap.charm_flip and snap.charm_flip > snap.spot) else None
            stop = stop if stop is not None else snap.spot * (1.0 + stop_pct)

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
                f"close_charm_flow {ccf:+.2e} ({'buy' if flow_is_bullish else 'sell'}) → "
                f"pin {snap.pin_strike} ({drift * 100:+.2f}% away, conf "
                f"{snap.pin_confidence:.2f}); {snap.gex_regime()}; {mtc:.0f}m to close"
            ),
            components_at_entry={
                "close_charm_flow": ccf,
                "pin_strike": snap.pin_strike,
                "pin_confidence": snap.pin_confidence,
                "pin_score": snap.pin_score,
                "drift_pct": drift,
                "charm_flip": snap.charm_flip,
                "net_gex": snap.net_gex,
                "minutes_to_close": mtc,
                "quality": quality,
            },
        )
