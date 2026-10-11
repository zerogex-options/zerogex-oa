"""Shared engine for the v6 short-premium bots: sell the 0DTE move, gated on magnitude.

Why this engine exists
----------------------
Every bot in the fleet before v6 was a DIRECTIONAL bet, and every one of them
died or stalled, while five independent studies on this platform's own data
found the same thing: dealer-gamma structure predicts how FAR price travels,
not which way (``docs/design/msi-regime-excursion.md``,
``research/trade_bias_movement``, ``research/short_gamma_trend``). The only
structure that is paid for "less travel than the market priced" is a short
option structure. This module is that structure, built once, with the gates
every v6 candidate shares, so each strategy is a thin subclass that names its
own entry conditions and nothing else.

The trade
---------
A defined-risk short 0DTE iron butterfly (shorts at a center strike, long
wings a sigma-multiple out) or iron condor (shorts ``short_offset_sigma``
either side of the center). Max loss is wing width minus credit, which the
live sizer reads from ``structure_max_loss_per_share``; the harness fills all
four legs long-at-ask / short-at-bid plus slippage, so the credit the screen
reports is the credit a seller actually keeps.

Every distance is a multiple of ONE sigma: the market's own 1-sigma remaining
move, read from the ATM front straddle (``snap.straddle_implied_sigma()``),
falling back to the morning forecast's diurnally-scaled implied move and then
to realized vol extrapolated over the remaining session. Nothing is a wall, a
dollar or a VIX level, so SPY and SPX mean the same thing.

The gates every subclass inherits (each a named ``_skip`` reason)
----------------------------------------------------------------
* ``window`` — minutes since the open / to the close, and enough hold left.
* ``no_straddle`` / ``no_sigma`` — the implied side must be priced.
* ``regime`` / ``no_pctile`` — a symbol-relative dealer-gamma regime; fails
  CLOSED when the percentile history is absent (never the absolute fallback).
* ``tail_realized`` / ``no_history`` — the trailing realized move, projected
  over the rest of the day on the cone model's diurnal profile, must sit
  under a cap of the straddle-implied remaining move
  (``snap.realized_over_straddle``). Dollars against dollars: the earlier
  annualized-vol form compared RTH-annualized realized with a
  calendar-annualized IV and read ~0.43 for a fair straddle, so it never
  tripped — the adversarial review caught it.
* ``magnitude_band`` / ``no_magnitude`` — the magnitude-only MSI band. OFF
  by default: its evidence is unadjusted for the clock and the tape, and
  its ``net_gex_sign`` component is near-collinear with the regime gate.
  A sweep axis that has to earn its place.
* ``range_spent`` / ``forecast_expansion`` — today's cash range against the
  morning forecast, and the forecast's own expansion call.
* ``flip_near`` — the gamma flip (either convention) inside a sigma of the
  center: a flip cross turns the absorber into an amplifier.
* ``vol_shock`` / ``event_day`` — the tail stand-downs. ``iv_elevated`` is
  available but OFF by default: ``daily_atm_iv`` is an all-expiration EOD
  figure and ``atm_iv_front`` a same-day midday one, so the percentile is
  not of the same thing.
* ``conviction`` — the base-class gate, with a quality scale calibrated so a
  typical gate-passing setup clears it (the v5 lesson).

Param convention: a gate is DISABLED by a falsy value (``0``, ``""``, ``[]``);
``None`` means "use the engine default". This is what lets a sweep twin drop
one gate at a time.

Exits: the base class's credit take / stop (``credit_take_frac`` /
``credit_stop_mult`` — note the WING is the real stop on a fly: max loss is
roughly 0.8-0.9x the credit at a 1.25-sigma wing, so a stop multiple above
that never fires) plus invalidations that close regardless of P&L —
``regime_lost``, ``flip_entered``, ``short_touched``, ``vol_shock`` — and a
time stop that by default never holds the short straddle into the settlement
window (``eod_exit_buffer_minutes``; a sweep arm holds to settlement).

Controls are params, not new bots: ``required_regimes: "any"`` is the
tape-only twin, ``max_realized_over_straddle: 0`` on top of it is the
clock-only twin, and ``tw_execution_sweep`` runs the grid in one pass.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, List, Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, OpenPosition, TradeSignal

#: Sigma sources, tried in the order ``params["sigma_sources"]`` lists them.
SIGMA_STRADDLE = "straddle"
SIGMA_FORECAST = "forecast"
SIGMA_REALIZED = "realized"

#: Shortest hold worth opening a four-leg structure for.
_MIN_HOLD_MINUTES = 15.0


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


class ShortPremiumBot(BaseBot):
    """Base for the v6 short-premium bots. Subclasses set the gate params and
    may override :meth:`extra_gates`, :meth:`center_strike` and
    :meth:`quality`."""

    tier = "0DTE"
    direction_mode = "neutral"

    # ------------------------------------------------------------------
    # Hooks for subclasses
    # ------------------------------------------------------------------

    def extra_gates(self, snap: MarketSnapshot, sigma: float) -> Optional[str]:
        """Strategy-specific gates. Return a ``_skip`` reason, or ``None`` to pass."""
        return None

    def center_strike(self, snap: MarketSnapshot, sigma: float) -> float:
        """Where the short strikes sit. Default: the strike nearest spot."""
        return snap.round_to_strike(snap.spot)

    def quality(self, snap: MarketSnapshot, sigma: float) -> float:
        """0..1 read of how clean the setup is, calibrated against the
        conviction floor (see ``confidence_threshold_default`` in the catalog):
        half regime strength, half how far realized sits under the straddle."""
        if self._regime_gate_enabled():
            pct = snap.net_gex_pctile
            q_regime = _clamp01((float(pct) - 50.0) / 50.0) if pct is not None else 0.0
        else:
            q_regime = 0.6  # tape-only twin: neutral, so conviction cannot bias it
        cap = self._p("max_realized_over_straddle", 0.85)
        ratio = snap.realized_over_straddle(int(self._p("realized_window_bars", 30)))
        q_tail = _clamp01(1.0 - ratio / cap) if (ratio is not None and cap) else 0.5
        return 0.5 * q_regime + 0.5 * q_tail

    # ------------------------------------------------------------------
    # Params
    # ------------------------------------------------------------------

    def _p(self, key: str, default: Any) -> Any:
        """Param with an engine default. ``None`` => default; a falsy value
        (0, "", []) is a deliberate OFF and is returned as-is."""
        v = self.params.get(key)
        return default if v is None else v

    def _regime_gate_enabled(self) -> bool:
        req = self._p("required_regimes", ["positive_strong"])
        return not (isinstance(req, str) and req.lower() == "any")

    # ------------------------------------------------------------------
    # Sigma
    # ------------------------------------------------------------------

    def remaining_sigma(self, snap: MarketSnapshot) -> tuple[Optional[float], Optional[str]]:
        """The 1-sigma $ move for the rest of the front expiration's life,
        from the first available source in ``sigma_sources``."""
        sources = self._p("sigma_sources", [SIGMA_STRADDLE, SIGMA_FORECAST])
        for src in sources:
            if src == SIGMA_STRADDLE:
                s = snap.straddle_implied_sigma()
            elif src == SIGMA_FORECAST:
                s = snap.expected_remaining_move()
            elif src == SIGMA_REALIZED:
                s = snap.realized_remaining_move(int(self._p("realized_window_bars", 30)))
            else:
                s = None
            if s is not None and s > 0:
                return float(s), src
        return None, None

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        if snap.spot <= 0:
            return self._skip("no_close")

        # -- Window. Both ends: skip the open's variance hump and leave a hold
        # worth the four-leg friction.
        m = snap.minutes_since_open
        mtc = snap.minutes_to_close
        if m is None or mtc is None:
            return self._skip("window")
        if m < float(self._p("min_minutes_since_open", 60)):
            return self._skip("window")
        if mtc < float(self._p("min_minutes_to_close", 150)):
            return self._skip("window")
        eod_buffer = float(self._p("eod_exit_buffer_minutes", 20))
        hold = min(float(self._p("max_hold_minutes", 150)), mtc - eod_buffer)
        if hold < _MIN_HOLD_MINUTES:
            return self._skip("window")

        # -- The implied side must be priced.
        if snap.atm_iv_front is None or snap.atm_straddle_mid_front is None:
            return self._skip("no_straddle")
        sigma, sigma_src = self.remaining_sigma(snap)
        if sigma is None:
            return self._skip("no_sigma")

        # -- Regime: symbol-relative, fail closed.
        if self._regime_gate_enabled():
            if snap.net_gex is None:
                return self._skip("no_net_gex")
            if snap.net_gex_pctile is None and bool(self._p("regime_fail_closed", True)):
                return self._skip("no_pctile")
            required = self._p("required_regimes", ["positive_strong"])
            if snap.gex_regime() not in set(required):
                return self._skip("regime")

        # -- Magnitude-only MSI band (off unless the catalog lists bands).
        bands = self._p("allowed_magnitude_bands", None)
        if bands:
            band = snap.magnitude_band()
            if band is None:
                if bool(self._p("require_magnitude", True)):
                    return self._skip("no_magnitude")
            elif band not in set(bands):
                return self._skip("magnitude_band")

        # -- Tail: the tape must be realizing under what the straddle charges.
        bars = int(self._p("realized_window_bars", 30))
        cap = self._p("max_realized_over_straddle", 0.85)
        ratio_now = snap.realized_over_straddle(bars)
        if cap:
            if ratio_now is None:
                return self._skip("no_history")
            if ratio_now > float(cap):
                return self._skip("tail_realized")

        # -- The morning forecast, when it exists for this symbol.
        forbid = self._p("forbid_forecast_vol_state", "expansion")
        if forbid and snap.forecast_expected_vol_state == forbid:
            return self._skip("forecast_expansion")
        range_cap = self._p("max_cash_range_over_implied", None)
        if range_cap:
            r = snap.cash_range_over_implied()
            if r is not None and r > float(range_cap):
                return self._skip("range_spent")

        # -- Flip clearance: neither flip convention inside a sigma of center.
        center = self.center_strike(snap, sigma)
        clear = float(self._p("flip_clear_sigma_mult", 1.0))
        if clear > 0:
            for flip in (snap.gamma_flip, snap.gamma_flip_raw):
                if flip is not None and abs(float(flip) - center) < clear * sigma:
                    return self._skip("flip_near")

        # -- Tail stand-downs.
        max_dvix = self._p("max_vix_change_frac", 0.05)
        dvix = snap.vix_change()
        if max_dvix and dvix is not None and snap.vix and dvix / snap.vix > float(max_dvix):
            return self._skip("vol_shock")
        max_pctile = self._p("max_atm_iv_pctile", 0)
        if max_pctile:
            if snap.atm_iv_pctile_60d is None:
                if bool(self._p("require_iv_pctile", False)):
                    return self._skip("no_iv_pctile")
            elif snap.atm_iv_pctile_60d > float(max_pctile):
                return self._skip("iv_elevated")
        if snap.forecast_is_event_day:
            return self._skip("event_day")
        if snap.et_date.isoformat() in set(self._p("event_dates", []) or []):
            return self._skip("event_day")

        # -- Strategy-specific gates.
        reason = self.extra_gates(snap, sigma)
        if reason:
            return self._skip(reason)

        # -- Structure.
        inc = snap.effective_strike_increment()
        k_short = float(self._p("short_offset_sigma", 0.0))
        if k_short > 0:
            put_short = snap.round_to_strike(center - k_short * sigma)
            call_short = snap.round_to_strike(center + k_short * sigma)
            if put_short >= center:
                put_short = center - inc
            if call_short <= center:
                call_short = center + inc
            strategy_type = "SHORT_IRON_CONDOR"
        else:
            put_short = call_short = center
            strategy_type = "SHORT_IRON_BUTTERFLY"
        wing_mult = float(self._p("wing_sigma_mult", 1.25))
        min_wing = max(1, int(self._p("min_wing_increments", 1)))
        wing_incs = max(min_wing, int(round(wing_mult * sigma / inc)))
        put_long = put_short - wing_incs * inc
        call_long = call_short + wing_incs * inc
        if not (put_long < put_short <= call_short < call_long):
            return self._skip("bad_structure")

        # -- Conviction.
        q = _clamp01(self.quality(snap, sigma))
        ml_components = {
            "net_gex_pctile": snap.net_gex_pctile,
            "realized_over_straddle": ratio_now,
            "magnitude_score": snap.magnitude_score(),
            "sigma": sigma,
            "straddle_mid": snap.atm_straddle_mid_front,
        }
        conviction = self.compute_conviction(snap, q, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

        expiration = resolve_expiration_iso(snap.et_date, int(self._p("dte_target", 0)))
        legs = self.build_iron_condor(
            snap.underlying,
            put_long_strike=put_long,
            put_short_strike=put_short,
            call_short_strike=call_short,
            call_long_strike=call_long,
            expiration=expiration,
        )
        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction="neutral",
            strategy_type=strategy_type,
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            # Premium exits only (credit_take / credit_stop); a neutral
            # position has no spot target and the base exit ignores them.
            target_price=None,
            stop_price=None,
            time_stop_at=_utcnow() + timedelta(minutes=hold),
            rationale=self.rationale(
                snap, sigma, sigma_src, ratio_now, put_long, put_short, call_short, call_long
            ),
            components_at_entry={
                "center": center,
                "put_long": put_long,
                "put_short": put_short,
                "call_short": call_short,
                "call_long": call_long,
                "sigma": sigma,
                "sigma_source": sigma_src,
                "wing_increments": wing_incs,
                "net_gex": snap.net_gex,
                "net_gex_pctile": snap.net_gex_pctile,
                "gamma_flip": snap.gamma_flip,
                "realized_over_straddle": ratio_now,
                "realized_remaining_move": snap.realized_remaining_move(bars),
                # Diagnostic only — mixed conventions, see the snapshot docstring.
                "realized_over_implied_annualized": snap.realized_over_implied(bars),
                "magnitude_score": snap.magnitude_score(),
                "magnitude_band": snap.magnitude_band(),
                "straddle_mid": snap.atm_straddle_mid_front,
                "straddle_bid": snap.atm_straddle_bid_front,
                "atm_iv": snap.atm_iv_front,
                "atm_iv_pctile_60d": snap.atm_iv_pctile_60d,
                "forecast_expected_vol_state": snap.forecast_expected_vol_state,
                "entry_spot": snap.spot,
                "minutes_to_close": mtc,
                "quality": q,
            },
        )

    def rationale(
        self,
        snap: MarketSnapshot,
        sigma: float,
        sigma_src: Optional[str],
        ratio: Optional[float],
        put_long: float,
        put_short: float,
        call_short: float,
        call_long: float,
    ) -> str:
        pct = snap.net_gex_pctile
        mag = snap.magnitude_score()
        return (
            f"{snap.gex_regime()}"
            + (f" (pctile {pct:.0f})" if pct is not None else "")
            + (f", realized/straddle {ratio:.2f}" if ratio is not None else "")
            + (f", magnitude {mag:.0f}" if mag is not None else "")
            + f"; sigma ${sigma:.2f} ({sigma_src}); "
            + f"{put_long:g}/{put_short:g}/{call_short:g}/{call_long:g}"
        )

    # ------------------------------------------------------------------
    # Exit
    # ------------------------------------------------------------------

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
        """Invalidations that close regardless of P&L, then the base stack.

        The base class owns the credit take / stop and the time stop. These
        run first because each says the mechanism the premium was sold
        against is gone, and waiting for the credit stop on a running move
        is how a defined-risk short structure reaches its wing.
        """
        comps = position.components_at_entry or {}
        entry_pctile = comps.get("net_gex_pctile")
        if bool(self._p("exit_on_regime_loss", True)) and self._regime_gate_enabled():
            if (
                snap.net_gex is not None
                and snap.net_gex < 0
                and (entry_pctile is None or float(entry_pctile) >= 50.0)
            ):
                return ExitDecision(should_close=True, reason="regime_lost")

        sigma = comps.get("sigma")
        center = comps.get("center")
        if bool(self._p("exit_on_flip_entry", True)) and isinstance(sigma, (int, float)):
            if isinstance(center, (int, float)):
                clear = float(self._p("flip_exit_sigma_mult", 0.5))
                for flip in (snap.gamma_flip, snap.gamma_flip_raw):
                    if flip is not None and abs(float(flip) - float(center)) < clear * float(sigma):
                        return ExitDecision(should_close=True, reason="flip_entered")

        buffer_mult = self._p("short_touch_buffer_sigma", 0.0)
        if buffer_mult is not None and isinstance(sigma, (int, float)):
            ps, cs = comps.get("put_short"), comps.get("call_short")
            buf = float(buffer_mult) * float(sigma)
            if isinstance(ps, (int, float)) and snap.spot < float(ps) - buf:
                return ExitDecision(should_close=True, reason="short_touched")
            if isinstance(cs, (int, float)) and snap.spot > float(cs) + buf:
                return ExitDecision(should_close=True, reason="short_touched")

        max_dvix = self._p("max_vix_change_frac", 0.05)
        dvix = snap.vix_change()
        if (
            bool(self._p("exit_on_vol_shock", True))
            and max_dvix
            and dvix is not None
            and snap.vix
            and dvix / snap.vix > float(max_dvix)
        ):
            return ExitDecision(should_close=True, reason="vol_shock")

        return super().exit_criteria(snap, position)


__all__: List[str] = [
    "ShortPremiumBot",
    "SIGMA_STRADDLE",
    "SIGMA_FORECAST",
    "SIGMA_REALIZED",
]
