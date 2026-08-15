"""SettlementFlowSnap — trade the pure 0DTE settlement-unwind hedge leg.

As 0DTE options approach expiry every contract's delta collapses toward a step
function (0 or 1 at settlement). Dealers hedging that book must converge their
stock hedge to the settlement delta by the cash close *regardless of price* —
this is not discretionary. ZeroGEX's forced-flow engine computes it exactly,
twice: ``forced_flow_profile.close_charm_flow`` (raw) is the full-reprice
by-close flow INCLUDING same-day strikes resolving to intrinsic, while
``close_charm_flow_smooth`` is the first-order charm drift only. The residual

    D = raw − smooth

is therefore the pure settlement-unwind component, non-zero precisely when
large near-ATM 0DTE strikes are resolving into the close.

``charm_close_magnet`` trades the SMOOTH drift toward a magnet in positive
gamma. This bot trades the orthogonal component: it requires the settlement
residual to DOMINATE the smooth drift, so its firing set is nearly disjoint
from the charm bot's, and its direction comes from a computed forced-dollar
flow — no price level is involved. The magnitude gate is relative
(``|D| >= frac × local_gex``): the settlement flow must be at least as large
as the gamma-hedge flow of a ~25 bp move, so it is big enough to move price,
and the gate means the same thing for SPY and SPX.

Structure: 0DTE debit vertical in the flow direction (a naked late-day 0DTE
debit bleeds too fast; the short leg is financed by elevated late-day ATM
premium). Exits: small spot target/stop, an invalidation that re-reads the
residual every tick (sign flip or decay => the pressure resolved early), and a
time-stop that rides nearly to the bell — the flow completes INTO the close.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, OpenPosition, TradeSignal


class SettlementFlowSnap(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Ride the pure settlement-unwind hedge into the close."
    description = (
        "Trades the raw-minus-smooth close_charm_flow residual — the exact "
        "dollars of the by-close dealer hedge that come from 0DTE strikes "
        "resolving to intrinsic, not from smooth charm drift. Fires only when "
        "that settlement leg dominates the drift and is large relative to the "
        "local gamma pool. Defined-risk vertical in the forced-flow direction."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        # -- Window: the settlement leg only matters late; enter 14:30-15:30 ET
        # so the flow still has time to express before the 15:52 time-stop.
        mtc = snap.minutes_to_close
        max_mtc = float(self.params.get("max_minutes_to_close", 90))
        min_mtc = float(self.params.get("min_minutes_to_close", 30))
        if mtc is None or mtc > max_mtc or mtc < min_mtc:
            return self._skip("window")

        # -- The signal: the settlement residual D = raw − smooth.
        residual = snap.settlement_flow_residual()
        if residual is None:
            return self._skip("no_ff_pair")
        smooth = snap.close_charm_flow_smooth or 0.0

        # -- Magnitude gate, relative: the settlement flow must rival the
        # gamma-hedge flow of a ~25 bp move (local_gex is $ per 1% within ±1%
        # of spot, so frac × local_gex is scale-consistent across symbols).
        local = snap.local_gex
        if local is None or local <= 0:
            return self._skip("no_local_gex")
        min_frac = float(self.params.get("min_residual_local_gex_frac", 0.25))
        if abs(residual) < min_frac * local:
            return self._skip("residual_small")

        # -- Dominance gate: the trade is the SETTLEMENT leg, not the smooth
        # drift charm_close_magnet already trades. Require the residual to
        # dominate so the two bots' firing sets stay nearly disjoint.
        dominance = float(self.params.get("min_residual_dominance", 1.5))
        if abs(residual) < dominance * abs(smooth):
            return self._skip("drift_dominates")

        direction = "bullish" if residual > 0 else "bearish"

        # -- Abstain when the aggressor tape is actively fighting the flow.
        veto_dollars = float(self.params.get("flow_veto_dollars", 2.0e6))
        frp = snap.flow_recent_premium
        if frp is not None and abs(frp) > veto_dollars:
            if (residual > 0) != (frp > 0):
                return self._skip("tape_opposes")

        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        # -- Quality: how far past the magnitude floor the residual runs.
        quality = min(1.0, abs(residual) / max(1e-9, 3.0 * min_frac * local))
        ml_components = {
            "settlement_residual": residual,
            "close_charm_flow_smooth": smooth,
            "local_gex": local,
            "net_gex": snap.net_gex,
            "minutes_to_close": mtc,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

        # -- Structure: 0DTE debit vertical in the forced-flow direction.
        expiration = resolve_expiration_iso(snap.et_date, int(self.params.get("dte_target", 0)))
        inc = snap.effective_strike_increment()
        width = max(1, int(self.params.get("spread_width_increments", 2)))
        long_strike = snap.round_to_strike(snap.spot)
        short_strike = long_strike + (width * inc if direction == "bullish" else -width * inc)
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_vertical(snap.underlying, opt_type, long_strike, short_strike, expiration)
        strat = "BULL_CALL_DEBIT_SPREAD" if direction == "bullish" else "BEAR_PUT_DEBIT_SPREAD"

        target_pct = float(self.params.get("target_pct", 0.003))
        stop_pct = float(self.params.get("stop_pct", 0.002))
        if direction == "bullish":
            target = snap.spot * (1.0 + target_pct)
            stop = snap.spot * (1.0 - stop_pct)
        else:
            target = snap.spot * (1.0 - target_pct)
            stop = snap.spot * (1.0 + stop_pct)

        # Ride nearly to the bell (the flow completes INTO the close); the
        # reconciler's 15:55 hard cap and the settlement backstop own the tail.
        exit_buffer_min = float(self.params.get("close_exit_buffer_minutes", 8))
        hold = max(1.0, mtc - exit_buffer_min)

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
                f"settlement residual {residual:+.2e} (raw-smooth) "
                f"{abs(residual) / local:.2f}x local_gex, "
                f"{abs(residual) / max(1e-9, abs(smooth)):.1f}x drift; "
                f"{mtc:.0f}m to close"
            ),
            components_at_entry={
                "settlement_residual": residual,
                "close_charm_flow_raw": snap.close_charm_flow_raw,
                "close_charm_flow_smooth": smooth,
                "local_gex": local,
                "minutes_to_close": mtc,
                "quality": quality,
            },
        )

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
        """Default stack plus the residual-invalidation check.

        The thesis is a live forced flow; if a fresh snapshot shows the
        residual flipped sign or decayed away, the pressure resolved early
        and the position has no reason to exist — close regardless of P&L.
        Missing fields on a tick (a data hiccup) never force an exit.
        """
        entry_residual = position.components_at_entry.get("settlement_residual")
        residual = snap.settlement_flow_residual()
        if residual is not None and isinstance(entry_residual, (int, float)) and entry_residual:
            if (residual > 0) != (entry_residual > 0):
                return ExitDecision(should_close=True, reason="flow_flip")
            revoke_frac = float(self.params.get("residual_revoke_frac", 0.4))
            if abs(residual) < revoke_frac * abs(entry_residual):
                return ExitDecision(should_close=True, reason="flow_faded")
        return super().exit_criteria(snap, position)
