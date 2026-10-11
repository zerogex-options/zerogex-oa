"""HedgingDemandCloseMomentum — the day's move runs into the bell when dealers are short gamma.

The published effect
--------------------
Gao, Han, Li & Zhou (2018, JFE, "Market Intraday Momentum"): on SPY the first
half-hour return predicts the last half-hour return in the same direction.
Baltussen, Da, Lammers & Martens (2021, JFE, "Hedging Demand and Market
Intraday Momentum"): across 60+ futures the return from the prior close to the
last half hour predicts the final half hour, and the effect is concentrated
where two FORCED traders have to rebalance WITH the day's move into the bell:
options market makers who are net short gamma (they buy as price rises and
sell as it falls, and their required hedge grows into the close as 0DTE gamma
peaks), and leveraged / inverse ETFs that must rebalance at the close in the
direction of the day's return. The papers proxy the hedgers' sign; ZeroGEX
measures it directly as ``net_gex_at_spot`` and its symbol-relative percentile.

Why this is not a dead row
--------------------------
The short-gamma trend study (``research/short_gamma_trend``) asked whether ANY
30-minute one-sided-flow episode continues, at any hour, and found nothing
(leaning to reversal). The follow-the-flow bots (PF 0.31-0.33) read aggressor
OPTION flow direction. The nine Trade Bias inputs were per-minute direction
reads. None of them is the fixed clock-to-clock structure the papers document:
the cash-session return at 15:00 ET, traded only into 15:55, only when the
hedgers are short gamma. The brief records this structure as untested here.

The controls that decide whether the gamma gate earns its place are params:
``required_gamma_sign: "any"`` is the unconditional Gao et al. trade, and
``"positive"`` is the complement, which the mechanism says should NOT work.

Structure: a 0DTE ATM vertical in the day-return direction, ~0.5 sigma wide
on the remaining move, held to the bell. Debit by default (direction plus the
last hour's convexity); ``structure: "credit_vertical"`` sells the ATM option on
the other side instead and is paid the steepest hour of 0DTE theta. Exits: the
time stop at the bell (the published object IS the last-half-hour return), a
cross back through the cash open (the day's return changed sign — the
rebalancing demand reversed), and the premium stop.
"""

from __future__ import annotations

import math
from datetime import timedelta
from typing import Any, Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, OpenPosition, TradeSignal

_TRADING_DAYS = 252.0


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


class HedgingDemandCloseMomentum(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Short-gamma hedgers must chase the day's move into the bell. Ride it."
    description = (
        "Market intraday momentum, conditioned the way Baltussen et al. say it "
        "works: at 15:00 ET, when dealers are short gamma and the cash session "
        "has moved at least half an implied daily sigma from its open, buys a "
        "0DTE ATM vertical in the direction of the day's return and holds it "
        "to 15:55. Closes early only if spot crosses back through the cash open."
    )

    def _p(self, key: str, default: Any) -> Any:
        v = self.params.get(key)
        return default if v is None else v

    # ------------------------------------------------------------------
    # The implied daily move, as a fraction of spot
    # ------------------------------------------------------------------

    def _implied_daily_frac(self, snap: MarketSnapshot) -> Optional[float]:
        """Forecast implied move / spot when the morning forecast exists, else
        the VIX-implied 1-day move. The ATM front straddle is deliberately NOT
        used here: at 15:00 ET a 0DTE straddle prices the last hour, not the
        day, and the papers' conditioning variable is a daily sigma."""
        if snap.spot <= 0:
            return None
        im = snap.forecast_implied_move
        if im is not None and im > 0:
            return float(im) / snap.spot
        if snap.vix is not None and snap.vix > 0:
            return (float(snap.vix) / 100.0) / math.sqrt(_TRADING_DAYS)
        return None

    def _remaining_sigma_dollars(self, snap: MarketSnapshot, daily_frac: float) -> Optional[float]:
        rem = snap.expected_remaining_move()
        if rem is not None and rem > 0:
            return rem
        mtc = snap.minutes_to_close
        if mtc is None or mtc <= 0:
            return None
        return daily_frac * snap.spot * math.sqrt(mtc / 390.0)

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        if snap.spot <= 0:
            return self._skip("no_close")

        # -- Window: 15:00-15:20 ET by default. One trade per symbol-session
        # (the hold runs to the bell).
        mtc = snap.minutes_to_close
        if mtc is None:
            return self._skip("window")
        if not (
            float(self._p("min_minutes_to_close", 40))
            <= mtc
            <= float(self._p("max_minutes_to_close", 60))
        ):
            return self._skip("window")

        # -- The hedgers' sign, measured. Negative gamma is the paper's
        # condition; "positive" is the complement control, "any" the
        # unconditional Gao et al. trade.
        want = str(self._p("required_gamma_sign", "negative")).lower()
        if want != "any":
            if snap.net_gex is None:
                return self._skip("no_net_gex")
            if want == "negative" and snap.net_gex >= 0:
                return self._skip("regime")
            if want == "positive" and snap.net_gex <= 0:
                return self._skip("regime")
            max_pct = self._p("max_net_gex_pctile", None)
            if (
                want == "negative"
                and max_pct is not None
                and snap.net_gex_pctile is not None
                and snap.net_gex_pctile > float(max_pct)
            ):
                return self._skip("regime")

        # -- The day's return against its implied daily sigma.
        r_day = snap.day_return()
        if r_day is None:
            return self._skip("no_cash_open")
        daily_frac = self._implied_daily_frac(snap)
        if daily_frac is None or daily_frac <= 0:
            return self._skip("no_implied")
        move_sigmas = abs(r_day) / daily_frac
        if move_sigmas < float(self._p("min_day_move_sigma", 0.5)):
            return self._skip("day_flat")
        if move_sigmas > float(self._p("max_day_move_sigma", 3.0)):
            return self._skip("day_exhausted")
        direction = "bullish" if r_day > 0 else "bearish"
        sign = 1.0 if r_day > 0 else -1.0

        # -- Gao's own predictor, optionally required to agree.
        fhh = snap.first_half_hour_return()
        fhh_agrees = fhh is not None and (fhh > 0) == (r_day > 0) and fhh != 0
        if bool(self._p("require_first_half_hour_agreement", False)):
            if fhh is None:
                return self._skip("no_first_half_hour")
            if not fhh_agrees:
                return self._skip("fhh_disagrees")

        sigma_rem = self._remaining_sigma_dollars(snap, daily_frac)
        if sigma_rem is None or sigma_rem <= 0:
            return self._skip("no_sigma")

        # -- Optional obstacle check (off by default — a wall is never the
        # signal here, and the gate stays clean without it).
        room_mult = float(self._p("wall_room_sigma_mult", 0.0) or 0.0)
        if room_mult > 0:
            wall = snap.call_wall if direction == "bullish" else snap.put_wall
            if wall is not None and abs(float(wall) - snap.spot) < room_mult * sigma_rem:
                return self._skip("wall_in_the_way")

        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        # -- Conviction. Calibrated to the catalog floor: a 1-sigma day in
        # bottom-quartile gamma clears it with or without Gao agreement (the
        # agreement is optional, so it must not be load-bearing here); a
        # half-sigma day at a middling percentile does not.
        min_move = float(self._p("min_day_move_sigma", 0.5))
        q_move = _clamp01((move_sigmas - min_move) / 0.75)
        pct = snap.net_gex_pctile
        q_regime = _clamp01((50.0 - float(pct)) / 50.0) if pct is not None else 0.5
        if want == "any":
            q_regime = 0.6
        q_fhh = 1.0 if fhh_agrees else 0.0
        quality = 0.6 * q_move + 0.3 * q_regime + 0.1 * q_fhh
        ml_components = {
            "day_return_sigmas": move_sigmas,
            "net_gex_pctile": pct,
            "fhh_agrees": q_fhh,
            "net_gex": snap.net_gex,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

        # -- Structure: ATM vertical in the day-return direction, ~half a
        # remaining-sigma wide, floored at one increment.
        inc = snap.effective_strike_increment()
        width_incs = max(
            int(self._p("min_width_increments", 1)),
            int(round(float(self._p("vertical_width_sigma_mult", 0.5)) * sigma_rem / inc)),
        )
        width = width_incs * inc
        atm = snap.round_to_strike(snap.spot)
        structure = str(self._p("structure", "debit_vertical"))
        expiration = resolve_expiration_iso(snap.et_date, int(self._p("dte_target", 0)))
        if structure == "credit_vertical":
            # Sell the ATM option on the side the move is leaving behind.
            if direction == "bullish":
                short = atm if atm < snap.spot else atm - inc
                legs = self.build_vertical(snap.underlying, "put", short - width, short, expiration)
                strategy_type = "BULL_PUT_CREDIT_SPREAD"
            else:
                short = atm if atm > snap.spot else atm + inc
                legs = self.build_vertical(
                    snap.underlying, "call", short + width, short, expiration
                )
                strategy_type = "BEAR_CALL_CREDIT_SPREAD"
        else:
            if direction == "bullish":
                legs = self.build_vertical(snap.underlying, "call", atm, atm + width, expiration)
                strategy_type = "BULL_CALL_DEBIT_SPREAD"
            else:
                legs = self.build_vertical(snap.underlying, "put", atm, atm - width, expiration)
                strategy_type = "BEAR_PUT_DEBIT_SPREAD"

        stop_mult = float(self._p("stop_sigma_mult", 0.0) or 0.0)
        stop_price = snap.spot - sign * stop_mult * sigma_rem if stop_mult > 0 else None
        hold = max(1.0, mtc - float(self._p("eod_exit_buffer_minutes", 5)))

        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=direction,
            strategy_type=strategy_type,
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            # No spot target: the object is the last-half-hour return, which
            # the time stop at the bell collects.
            target_price=None,
            stop_price=stop_price,
            time_stop_at=_utcnow() + timedelta(minutes=hold),
            rationale=(
                f"day {r_day * 100:+.2f}% = {move_sigmas:.2f} implied sigmas, "
                f"net_gex {snap.net_gex / 1e9:+.2f}B"
                + (f" (pctile {pct:.0f})" if pct is not None else "")
                + (", first half hour agrees" if fhh_agrees else "")
                + f"; {strategy_type} {width_incs} inc wide"
            ),
            components_at_entry={
                "cash_open": snap.cash_open,
                "day_return": r_day,
                "day_return_sigmas": move_sigmas,
                "implied_daily_frac": daily_frac,
                "sigma_rem": sigma_rem,
                "first_half_hour_return": fhh,
                "net_gex": snap.net_gex,
                "net_gex_pctile": pct,
                "structure": structure,
                "width_increments": width_incs,
                "entry_spot": snap.spot,
                "quality": quality,
            },
        )

    # ------------------------------------------------------------------
    # Exit
    # ------------------------------------------------------------------

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
        """Base stack plus the day's return changing sign.

        A cross back through the cash open means the leveraged-ETF rebalance
        and the short-gamma hedge now point the other way; the thesis is gone
        whatever the premium says.
        """
        if bool(self._p("exit_on_momentum_loss", True)):
            cash_open = position.components_at_entry.get("cash_open")
            if isinstance(cash_open, (int, float)) and cash_open > 0 and snap.spot > 0:
                if position.direction == "bullish" and snap.spot < float(cash_open):
                    return ExitDecision(should_close=True, reason="momentum_lost")
                if position.direction == "bearish" and snap.spot > float(cash_open):
                    return ExitDecision(should_close=True, reason="momentum_lost")
        return super().exit_criteria(snap, position)
