"""VannaVolCrushRider — trade the vanna hedging flow a vol move forces.

The retired ``VixRegimeBreakout`` used only the VIX *level* (> 16) as a coarse
regime gate; it never looked at vanna or at the *change* in implied vol. But
the tradeable vanna edge is a product of two things the level alone cannot
express:

    forced dealer delta flow  ≈  dealer_vanna_total × ΔIV

``gex_by_strike.dealer_vanna_exposure`` (dealer sign, summed front-expiration
into ``snap.dealer_vanna_total``) is "$ of stock dealers must trade per +1 vol
point": positive => dealers BUY as IV rises (and SELL as IV falls). Multiply by
the session ΔVIX and you get the sign AND size of the mechanical hedging flow.

The classic case is the vol-crush melt-up: a book modeled short vanna
(``dealer_vanna_total < 0``) into a falling VIX (``ΔVIX < 0``) yields a
product > 0 — dealers must BUY, and price grinds up with no news. The mirror
holds for a vol spike. The direction is a two-sign product, which is exactly
why it is not the obvious "VIX high => trend" trade the retired bot made.

Structure is a defined-risk debit vertical in the forced direction. Vanna flow
is a steady grind, not an explosion, so the target is a modest move (or the
``vanna_flip`` level) and the stop is a small adverse move / vol reversal.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal


class VannaVolCrushRider(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Vol is moving. Ride the vanna hedging flow it forces on dealers."
    description = (
        "Enters in the direction of the mechanical vanna hedging flow "
        "(dealer_vanna_total × ΔVIX): short-vanna book into a vol crush buys "
        "the underlying (melt-up), and the mirror for a vol spike. "
        "Defined-risk vertical; modest grind target."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        # Avoid the opening print; vanna flow needs a settled tape and a
        # measurable vol move.
        m = snap.minutes_since_open
        if m is None or m < float(self.params.get("min_minutes_since_open", 20)):
            return None
        # Leave the final window to the charm bot (charm dominates into close).
        mtc = snap.minutes_to_close
        if mtc is not None and mtc < float(self.params.get("min_minutes_to_close", 45)):
            return None

        vanna = snap.dealer_vanna_total
        dvix = snap.vix_change()
        if vanna is None or dvix is None:
            return None

        # Real vol move only — a flat VIX produces no vanna flow.
        min_dvix = float(self.params.get("min_vix_change", 0.30))
        if abs(dvix) < min_dvix:
            return None
        # Meaningful dealer vanna only.
        min_vanna = float(self.params.get("min_dealer_vanna", 4.0e7))
        if abs(vanna) < min_vanna:
            return None

        # Forced flow = dealer_vanna_total × ΔIV. Positive => dealers buy.
        forced = vanna * dvix
        if forced == 0.0:
            return None
        direction = "bullish" if forced > 0 else "bearish"
        if self._bias_veto(snap, direction):
            return None

        # Quality scales with the magnitude of the forced flow, normalized by a
        # per-symbol vanna scale × a reference 1-point vol move.
        forced_norm = float(self.params.get("forced_flow_norm", 1.5e8)) * max(
            0.5, float(self.params.get("vix_change_ref", 1.0))
        )
        forced_score = min(1.0, abs(forced) / forced_norm) if forced_norm > 0 else 0.0
        # A larger vol move is a cleaner signal, up to a cap.
        move_score = min(1.0, abs(dvix) / max(1e-9, float(self.params.get("vix_change_ref", 1.0))))
        quality = 0.65 * forced_score + 0.35 * move_score
        ml_components = {
            "dealer_vanna_total": vanna,
            "vix_change": dvix,
            "forced_flow": forced,
            "net_gex": snap.net_gex,
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

        # Target: a modest grind (vanna flow is not explosive). Prefer the
        # vanna_flip level when it sits in the trade's favor, else a fixed move.
        target_pct = float(self.params.get("target_pct", 0.004))
        stop_pct = float(self.params.get("stop_pct", 0.003))
        if direction == "bullish":
            target = snap.spot * (1.0 + target_pct)
            if snap.vanna_flip and snap.vanna_flip > snap.spot:
                target = min(target, snap.vanna_flip)
            stop = snap.spot * (1.0 - stop_pct)
        else:
            target = snap.spot * (1.0 - target_pct)
            if snap.vanna_flip and snap.vanna_flip < snap.spot:
                target = max(target, snap.vanna_flip)
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
                f"vanna {vanna:+.2e} × ΔVIX {dvix:+.2f} = forced {forced:+.2e} "
                f"({direction}); {snap.gex_regime()} regime"
            ),
            components_at_entry={
                "dealer_vanna_total": vanna,
                "vix": snap.vix,
                "prior_vix": snap.prior_vix,
                "vix_change": dvix,
                "forced_flow": forced,
                "vanna_flip": snap.vanna_flip,
                "net_gex": snap.net_gex,
                "quality": quality,
            },
        )
