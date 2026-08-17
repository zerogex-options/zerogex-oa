"""PutCapitulationCreditFade — sell the panic IV into the forced dealer bid.

In a top-quartile LONG-gamma dealer book the hedging is contractually
contrarian: as spot falls, long-gamma dealers accumulate short delta and must
BUY stock to re-hedge — a mechanical, price-insensitive bid that scales with
the down-move. A put-buying climax into that book is structurally mistimed:
the capitulators pay a panic-inflated 0DTE put premium at the precise moment
the largest player in the market is forced to buy the dip.

Two forced counterparties align: dealers long gamma (forced dip-buyers) and
the put buyers themselves, whose fresh 0DTE puts bleed both theta and the IV
spike they created once spot stabilizes. Flow-only shops see the burst but
not the absorber; positioning shops see the gamma but not the trigger.
ZeroGEX persists both: the per-type Lee-Ready split (``put_panic_premium`` /
``put_panic_dominance`` / ``put_panic_baseline`` from ``flow_contract_facts``)
and the symbol-relative positioning percentile (``net_gex_pctile``).

Structure is the point: a 0DTE put CREDIT vertical sold below the dip low
captures BOTH legs of the edge — direction (the dealer dip-buy) and vol (the
burst just spiked exactly these puts' IV; stabilization crushes it) — where a
debit call vertical captures only the first. Post-burst put IV also widens
the credit collected precisely when the signal fires, so entry quality and
pay are correlated. Exits are premium-based via the base class's credit
branch (``credit_take_frac`` / ``credit_stop_mult``) — a credit spread
profits from NOT moving down; reversion toward VWAP is a bonus, not a
requirement — plus a structural floor: spot losing the put wall means the
absorber failed.

NOT a wall bot: the dead wall-fade family entered ON wall proximity as the
signal; here the wall is only an invalidation floor and the trigger is a flow
event that can fire 1% above it. NOT climax_flow_fade: that fades aggregate
flow climaxes with a debit structure; this is per-type (put-only burst with a
dominance ratio), percentile-conditioned (the absorber must be at full
strength), and the first flow-family bot to harvest the vol leg via credit.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

import math

from src.tradeworkz.bots.base import (
    BaseBot,
    _short_horizon_sigma_pct,
    _utcnow,
    resolve_expiration_iso,
)
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, OpenPosition, TradeSignal


class PutCapitulationCreditFade(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Panic put buying into a full-strength long-gamma book. Sell it the IV."
    description = (
        "When a put-only aggressor burst (3x the session baseline, put-"
        "dominant) hits a top-quartile positive-gamma dealer book with spot "
        "still above the put wall, sells a 0DTE put credit vertical below the "
        "dip low — short the exact IV the capitulators spiked, long the "
        "mechanical dealer dip-buy. Premium-based credit exits; the put wall "
        "is the invalidation floor."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        m = snap.minutes_since_open
        mtc = snap.minutes_to_close
        if m is None or m < float(self.params.get("min_minutes_since_open", 45)):
            return self._skip("window")
        if mtc is None or mtc < float(self.params.get("min_minutes_to_close", 75)):
            return self._skip("window")

        # -- The absorber must be at FULL strength: symbol-relative strong
        # positive gamma, not merely positive.
        if snap.gex_regime() != "positive_strong":
            return self._skip("regime")

        # -- There must be a dip being bought. NOTE: build_snapshot carries at
        # most 30 recent closes, and the displacement needs bars+1 of history
        # — a default above 29 can never evaluate (the first screen died at
        # no_history on every regime-passing tick with the old 30-bar default).
        closes = [c for c in (snap.recent_closes or []) if c and c > 0]
        disp_bars = max(5, int(self.params.get("displacement_bars", 25)))
        if len(closes) < disp_bars + 1:
            return self._skip("no_history")
        displacement = (closes[-1] - closes[-1 - disp_bars]) / closes[-1 - disp_bars]
        # Vol-relative dip threshold. The second screen exposed a structural
        # tension in a fixed 0.35% gate: the strong positive-gamma regime this
        # bot requires SUPPRESSES 0.35% dips (958 of the 962 regime-passing
        # ticks died at no_dip), so the trigger could ~never co-occur with the
        # absorber. "A real dip" must mean real relative to the pinned tape's
        # own vol: k sigma-scaled over the displacement window, floored so a
        # dead tape can't fire on noise.
        sigma_1m = _short_horizon_sigma_pct(snap)
        floor_pct = float(self.params.get("min_displacement_pct", 0.0020))
        if sigma_1m is not None:
            k = float(self.params.get("displacement_sigma_mult", 2.0))
            min_disp = max(floor_pct, k * sigma_1m * math.sqrt(disp_bars))
        else:
            min_disp = float(self.params.get("displacement_fallback_pct", 0.0035))
        if displacement > -min_disp:
            return self._skip("no_dip")

        # -- The capitulation burst: put-side net aggressor buying at a
        # multiple of the session's own per-window baseline, and put-DOMINANT
        # (one-way fear, not two-way repricing).
        panic = snap.put_panic_premium
        baseline = snap.put_panic_baseline
        dominance = snap.put_panic_dominance
        if panic is None or dominance is None:
            return self._skip("no_flow_split")
        if baseline is None or baseline <= 0:
            return self._skip("no_baseline")
        burst_mult = panic / baseline
        min_mult = float(self.params.get("min_burst_multiple", 3.0))
        if burst_mult < min_mult:
            return self._skip("no_burst")
        min_dom = float(self.params.get("min_put_dominance", 0.65))
        if dominance < min_dom:
            return self._skip("two_way_flow")

        # -- Room to absorb: the dealers get to buy BEFORE the structural
        # floor is even tested.
        dist_wall = snap.distance_to_put_wall_pct()
        min_room = float(self.params.get("min_wall_room_pct", 0.0025))
        if dist_wall is None or dist_wall < min_room:
            return self._skip("no_wall_room")

        # -- A real vol event is not a flow capitulation: stand down when the
        # session ΔVIX says the fear is macro, not micro.
        dvix = snap.vix_change()
        max_dvix = float(self.params.get("max_vix_change", 2.0))
        if dvix is not None and dvix > max_dvix:
            return self._skip("vol_event")

        direction = "bullish"
        if self._trend_veto(snap, direction):
            return self._skip("trend_veto")
        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        max_mult = float(self.params.get("burst_saturation_multiple", 8.0))
        mult_q = min(1.0, max(0.0, (burst_mult - min_mult) / max(1e-9, max_mult - min_mult)))
        dom_q = min(1.0, max(0.0, (dominance - min_dom) / max(1e-9, 0.85 - min_dom)))
        quality = 0.6 * mult_q + 0.4 * dom_q
        ml_components = {
            "burst_multiple": burst_mult,
            "put_dominance": dominance,
            "displacement": displacement,
            "net_gex_pctile": snap.net_gex_pctile,
            "wall_room_pct": dist_wall,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

        # -- Structure: put credit vertical below the dip low, where the
        # panic IV is fattest. Short strike ~0.35% under spot, long wing two
        # increments lower caps the risk. Engine-side MIN_CREDIT_PER_SHARE
        # naturally filters weak-vol non-events.
        expiration = resolve_expiration_iso(snap.et_date, int(self.params.get("dte_target", 0)))
        inc = snap.effective_strike_increment()
        offset_pct = float(self.params.get("short_strike_offset_pct", 0.0035))
        wing_width = max(1, int(self.params.get("wing_width_increments", 2)))
        short_strike = snap.round_to_strike(snap.spot * (1.0 - offset_pct))
        if short_strike >= snap.spot:
            short_strike -= inc
        long_strike = short_strike - wing_width * inc
        # build_vertical(long, short): long the lower wing, short the higher
        # put — a bull put credit spread.
        legs = self.build_vertical(snap.underlying, "put", long_strike, short_strike, expiration)

        # Hard finish before the settlement window — short 0DTE gamma into
        # the close is not the thesis.
        hold = float(self.params.get("max_hold_minutes", 120))
        eod_buffer = float(self.params.get("eod_exit_buffer_minutes", 15))
        hold = max(1.0, min(hold, mtc - eod_buffer))

        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=direction,
            strategy_type="BULL_PUT_CREDIT_SPREAD",
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            # Premium-based exits only (credit_take/credit_stop params); no
            # spot target — the position profits from spot simply holding.
            target_price=None,
            stop_price=None,
            time_stop_at=_utcnow() + timedelta(minutes=hold),
            rationale=(
                f"put capitulation {burst_mult:.1f}x baseline "
                f"({dominance * 100:.0f}% put-dominant) into "
                f"pctile-{snap.net_gex_pctile:.0f} long-gamma book; "
                f"dip {displacement * 100:+.2f}%, wall room {dist_wall * 100:.2f}%"
                if snap.net_gex_pctile is not None
                else f"put capitulation {burst_mult:.1f}x baseline into strong long-gamma book"
            ),
            components_at_entry={
                "burst_multiple": burst_mult,
                "put_dominance": dominance,
                "displacement": displacement,
                "put_wall": snap.put_wall,
                "short_strike": short_strike,
                "quality": quality,
            },
        )

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
        """Credit take/stop (base class) plus the structural floor.

        Spot trading below the put wall means the absorber failed — the
        forced dip-buy the credit was sold against is gone. Exit at market
        regardless of premium.
        """
        wall = snap.put_wall
        entry_wall = position.components_at_entry.get("put_wall")
        floor = (
            wall
            if wall is not None
            else (float(entry_wall) if isinstance(entry_wall, (int, float)) else None)
        )
        if floor is not None and snap.spot > 0:
            buffer_pct = float(self.params.get("wall_break_buffer_pct", 0.0005))
            if snap.spot < floor * (1.0 - buffer_pct):
                return ExitDecision(should_close=True, reason="wall_lost")
        return super().exit_criteria(snap, position)
