"""GexGradientDrift — the bot for ``gex_gradient_trend`` (Asymmetric Gamma Drift).

Asymmetric dealer gamma above vs below spot creates a multi-day drift toward
the lower-gamma direction, because there is less hedging resistance there to
absorb the move.

Why this bot exists
-------------------
``gex_gradient_trend`` is the one strategy in the catalog with a conclusive
realized-P&L edge — profit factor 2.80 on 33 QQQ trades over 60 days, net of
fills, slippage and commission — and until now it had no bot, so nothing could
trade it. It was the highest-value gap the catalog audit reported.

Fidelity is the whole job here
------------------------------
The measured edge belongs to the *pattern's* exact gates and geometry, so this
bot reproduces them rather than reinterpreting the thesis:

* It reads the **same persisted signal scores** the pattern gates on
  (``signal_component_scores`` / ``signal_scores``, via the snapshot's
  ``component_score`` / ``msi_component_score``), on the same x100 scale, so
  the pattern's calibrated thresholds are reused verbatim instead of being
  re-derived from raw gamma — a re-derivation would drift from the thing that
  was actually validated.
* It replicates the pattern's **own ATR estimator** — population sigma over
  the last 30 one-minute closes, scaled by sqrt(390) to a daily figure —
  rather than the base class's ``_short_horizon_sigma_pct``, which uses the
  last 15 returns. They give different numbers, and strike offset and target
  are both multiples of this one, so using the base helper would quietly move
  the trade off the measured geometry.
* Gate order, thresholds and miss labels mirror ``_check_triggers``, so a
  ``make tradeworkz-backtest`` funnel for this bot is directly comparable
  with the pattern's ``explain_miss`` output.

Three deliberate divergences, each because the live engine needs something
the Action Card did not:

1. **Strikes snap to the underlying's real grid** (``snap.round_to_strike``)
   where the pattern rounded to the nearest dollar. A $1-rounded SPX strike
   does not exist, so a faithfully-rounded leg would never fill. On QQQ — the
   symbol the edge was measured on — the grid IS $1, so the measured
   behaviour is unchanged.
2. **A conviction floor applies that the pattern does not have.** The engine
   refuses to open below ``confidence_threshold``; the pattern cards every
   setup that clears its triggers and lets confidence ride along. So this bot
   is strictly more selective than the pattern near that floor — notably, a
   ``vol_expansion`` warning docks conviction enough to decline a
   middling-gradient setup the pattern would have carded. The quality scale
   is calibrated against the floor so the passing set is not empty (see the
   conviction comment in ``open_criteria``), and the bot screen measures the
   bot as configured.
3. **The stop is the premium stop plus a gradient-decay exit**, because the
   pattern's stop is ``kind="signal_event"`` with no spot level to hand the
   reconciler: ``gradient_decay_below_20_or_-50pct_premium``. Both halves are
   implemented — ``max_premium_loss_pct`` 0.50 and an ``exit_criteria``
   override that closes when the gradient decays under 20, plus a per-bot
   ``premium_stop_grace_seconds`` so that stop measures MOVES and not the
   entry bid/ask gap (the fleet's 45s default is shorter than one replay
   step).
4. **Expiry walks WEEKDAYS, not calendar days** (``resolve_expiration_iso``).
   The pattern adds 5 *calendar* days, which lands on a Saturday for a Monday
   entry and a Sunday for a Tuesday entry. The backtester matches expiry
   EXACTLY (``AND expiration = %s`` in ``_fetch_leg_quote_from``; only strike
   is fuzzy-matched), so those cards find no quote and are dropped —
   meaning the pattern's validated sample is filtered by day of week, and
   this bot will fire on Mondays and Tuesdays where the pattern effectively
   could not. Expect the bot screen to differ from PF 2.80 for that reason
   alone; it is not evidence that the bot is wrong. The pattern's arithmetic
   is a latent bug, but fixing it changes live signal output and invalidates
   the existing calibration record, so it is a separate decision.

First-screen corrections (2026-09-13, 1 trade / PF 0.00 / 5-minute hold)
------------------------------------------------------------------------
Two defects in this bot's own configuration, both found by that screen and
both fixed here:

* An earlier revision restricted entry to the last 30 minutes of the session,
  on the reading that the pattern's ``Entry(trigger="at_close")`` meant
  "enter at the closing print". It does not — ``at_close`` is a member of
  ``playbook.backtest._IMMEDIATE_TRIGGERS``, so it means "fill at this bar, at
  market". The pattern emits on every scoring cycle all session and the
  backtester fills immediately, the per-pattern cooldown collapsing the
  stream. The window cut the opportunity set by ~92% and is gone.
* The only trade closed after one replay step on ``premium_stop``. The fleet
  grace is 45s while the harness replays at 5-minute steps, so the stop was
  measured bid-vs-ask on a fresh position — the exact failure the grace
  exists to prevent. Hence ``premium_stop_grace_seconds`` above.

Nothing here is funded on the pattern's evidence. ``is_provisionable``
requires an EDGE screen from a *bot* harness, so writing this bot makes the
strategy screenable (``make tradeworkz-backtest --bots gex_gradient_trend``)
and not live. See ``docs/design/strategy-catalog.md``.
"""

from __future__ import annotations

import math
from datetime import timedelta
from typing import List, Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, OpenPosition, TradeSignal

#: RTH minutes in a session — scales a 1-minute sigma to a daily one. Matches
#: the pattern's ``_DAILY_SIGMA_SCALAR``.
_DAILY_SIGMA_SCALAR = math.sqrt(390.0)

#: Closes the pattern's realized-sigma estimator looks back over.
_SIGMA_LOOKBACK_CLOSES = 30
#: Minimum usable closes before the estimator will produce a figure.
_SIGMA_MIN_CLOSES = 5


def realized_sigma_1min(closes: List[float]) -> float:
    """Population sigma of 1-minute simple returns over the last 30 closes.

    A verbatim port of ``gex_gradient_trend._realized_sigma_1min``. Kept as a
    module-level function (not a method) so a test can assert the two
    implementations agree on the same input — that equality is what makes the
    bot's strike and target land where the measured pattern put them.
    """
    usable = [c for c in (closes or []) if c and c > 0][-_SIGMA_LOOKBACK_CLOSES:]
    if len(usable) < _SIGMA_MIN_CLOSES:
        return 0.0
    rets = [
        (usable[i] - usable[i - 1]) / usable[i - 1]
        for i in range(1, len(usable))
        if usable[i - 1] > 0
    ]
    if not rets:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / len(rets)
    return math.sqrt(max(var, 0.0))


class GexGradientDrift(BaseBot):
    tier = "swing"
    direction_mode = "context"
    tagline = "Gamma is lopsided. Drift toward the thin side."
    description = (
        "Asymmetric dealer gamma above versus below spot creates a multi-day "
        "drift toward the lower-gamma direction, where there is less hedging "
        "resistance to absorb the move. Buys a 5-DTE OTM debit in the drift "
        "direction on a confirming close, targets 1.5x daily ATR, and cuts "
        "when the gradient decays."
    )

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        if snap.spot <= 0:
            return self._skip("no_close")

        # -- Gate 1: the gradient itself must be decisively one-sided.
        grad = snap.component_score("gex_gradient")
        if grad is None:
            return self._skip("no_gradient")
        min_score = float(self.params.get("min_gradient_score", 40.0))
        if abs(grad) < min_score:
            return self._skip("gradient_weak")
        drift = "bullish" if grad > 0 else "bearish"

        # -- Gate 2: net_gex sign must agree with the gradient's sign. The
        # component already inverts and damps its own reading under long
        # gamma; a disagreement here means the two are describing different
        # regimes, which is not a drift setup.
        if snap.net_gex is None:
            return self._skip("no_net_gex")
        if (grad * snap.net_gex) < 0:
            return self._skip("net_gex_disagrees")

        # -- Gate 3: not in breakout mode. This is drift, not break — a
        # breakout would overrun a slow multi-day trade.
        rbi_label = snap.component_context("range_break_imminence").get("label")
        if rbi_label == "Breakout Mode":
            return self._skip("breakout_mode")

        # -- Gate 4: some volatility must be available to power the drift.
        vol_regime = snap.msi_component_score("volatility_regime")
        if vol_regime is None:
            return self._skip("no_vol_regime")
        if vol_regime < float(self.params.get("min_volatility_regime", -0.5)):
            return self._skip("vol_regime_dead")

        # -- Gate 5: a confirming bar in the drift direction. The geometry is
        # a map; the tape has to agree before it is a trade.
        closes = [c for c in (snap.recent_closes or []) if c and c > 0]
        if len(closes) < 2:
            return self._skip("no_history")
        drift_sign = 1.0 if drift == "bullish" else -1.0
        if (closes[-1] - closes[-2]) * drift_sign <= 0:
            return self._skip("no_confirming_bar")

        if self._bias_veto(snap, drift):
            return self._skip("bias_veto")

        # -- Geometry: strike offset and target are both multiples of the
        # pattern's own daily ATR estimate (see the module docstring on why
        # this is not the base class's sigma helper).
        sigma_1min = realized_sigma_1min(closes)
        if sigma_1min <= 0.0:
            return self._skip("no_sigma")
        atr_daily = sigma_1min * _DAILY_SIGMA_SCALAR
        atr_dollars = atr_daily * snap.spot
        inc = snap.effective_strike_increment()
        otm_offset = max(inc, float(self.params.get("otm_atr_mult", 0.5)) * atr_dollars)
        target_mult = float(self.params.get("target_atr_mult", 1.5))

        strike = snap.round_to_strike(snap.spot + drift_sign * otm_offset)
        # A tight ATR can round the OTM strike back to the money; nudge it one
        # increment out so the structure stays the OTM debit that was measured.
        if drift == "bullish" and strike <= snap.spot:
            strike += inc
        elif drift == "bearish" and strike >= snap.spot:
            strike -= inc
        target = snap.spot + drift_sign * target_mult * atr_dollars

        # -- Conviction. Note this gate has NO pattern analogue: the pattern
        # emits a card for every setup that clears its triggers and lets
        # confidence ride along, whereas the live engine refuses to open
        # below ``confidence_threshold``. So the bot trades a subset of what
        # was measured, and the quality scale below has to be calibrated
        # against that floor or the subset is empty.
        #
        # That failure mode is not hypothetical — it is exactly what the
        # catalog's research log records for ``weekly_charm_grind``: three
        # ticks in 60 days cleared every hard gate and all three then died at
        # conviction, producing a zero-trade screen that said nothing about
        # the thesis. Saturations are therefore set at readings that actually
        # occur (a gradient of 100 needs perfect one-sidedness with no wing
        # concentration and full magnitude confidence, which essentially
        # never prints), not at the theoretical maximum.
        #
        # The vol_expansion penalty is the pattern's, applied to quality
        # rather than to the final number so the calibrated base and the ML
        # overlay still compose normally. It is a penalty, never a veto — the
        # Breakout Mode gate above is the hard version of that check.
        grad_sat = float(self.params.get("quality_gradient_saturation", 70.0))
        gradient_strength = min(1.0, abs(grad) / max(min_score, grad_sat))
        vol_floor = float(self.params.get("min_volatility_regime", -0.5))
        vol_sat = float(self.params.get("quality_vol_saturation", 0.5))
        vol_span = max(1e-6, vol_sat - vol_floor)
        vol_strength = min(1.0, max(0.0, (vol_regime - vol_floor) / vol_span))
        quality = 0.7 * gradient_strength + 0.3 * vol_strength
        if snap.component_triggered("vol_expansion"):
            quality = max(0.0, quality - float(self.params.get("vol_expansion_penalty", 0.10)))

        ml_components = {
            "gex_gradient_score": grad,
            "net_gex": snap.net_gex,
            "volatility_regime": vol_regime,
            "atr_daily": atr_daily,
            "msi_score": snap.msi_score,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

        expiration = resolve_expiration_iso(snap.et_date, int(self.params.get("dte_target", 5)))
        opt_type = "call" if drift == "bullish" else "put"
        legs = self.build_atm_debit(snap.underlying, opt_type, strike, expiration, entry_price=0.0)
        hold = int(self.params.get("max_hold_minutes", 3 * 24 * 60))

        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=drift,
            strategy_type="LONG_CALL_DEBIT" if drift == "bullish" else "LONG_PUT_DEBIT",
            legs=legs,
            entry_price=0.0,
            conviction=conviction,
            target_price=round(target, 4),
            # No spot-level stop by design: the pattern's stop is a signal
            # event. The premium stop and the gradient-decay exit below are
            # the two halves of it.
            stop_price=None,
            time_stop_at=_utcnow() + timedelta(minutes=hold),
            rationale=(
                f"gex_gradient {grad:+.0f} ({drift}) agrees with net_gex "
                f"{snap.net_gex / 1e9:+.2f}B, vol_regime {vol_regime:+.2f}, "
                f"confirming close; {int(self.params.get('dte_target', 5))}-DTE OTM "
                f"{opt_type} at {strike:.2f}, target {target:.2f} "
                f"({target_mult:.1f}x daily ATR ${atr_dollars:.2f})"
            ),
            components_at_entry={
                "gex_gradient_score": grad,
                "net_gex": snap.net_gex,
                "volatility_regime": vol_regime,
                "atr_daily": atr_daily,
                "atr_dollars": atr_dollars,
                "otm_offset": otm_offset,
                "strike": strike,
                "entry_spot": snap.spot,
                "vol_expansion_triggered": snap.component_triggered("vol_expansion"),
                "rbi_label": rbi_label,
                "quality": quality,
            },
        )

    # ------------------------------------------------------------------
    # Sizing
    # ------------------------------------------------------------------

    def size_multiplier(self) -> float:
        """Half size, matching the pattern's own ``size_multiplier=0.5``.

        A multi-day drift carried through overnight gaps deserves less
        capital than a 0DTE scalp, and the validated pattern says so on every
        card it emits. Applied as a factor ON TOP of the ML-learned knob, so
        learned sizing still moves the number.
        """
        static = float(self.params.get("static_size_multiplier", 0.5))
        return max(0.25, min(1.5, super().size_multiplier() * static))

    # ------------------------------------------------------------------
    # Exit
    # ------------------------------------------------------------------

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
        """Default stack plus the pattern's gradient-decay invalidation.

        The pattern's stop is ``gradient_decay_below_20_or_-50pct_premium``.
        The premium half is the base class's ``max_premium_loss_pct`` (0.50 in
        this bot's params); this is the gradient half: the asymmetry that was
        the entire reason for the trade has flattened, so the drift has no
        engine left regardless of where P&L sits.

        Also closes on a sign flip — the gradient now points the other way,
        which is a stronger invalidation than mere decay. A tick that cannot
        supply the component never forces an exit: a signals-engine gap is
        not a thesis failure, and the time stop still bounds the trade.
        """
        grad = snap.component_score("gex_gradient")
        if grad is None:
            return super().exit_criteria(snap, position)

        decay_floor = float(self.params.get("gradient_exit_score", 20.0))
        if abs(grad) < decay_floor:
            return ExitDecision(should_close=True, reason="gradient_decayed")

        entry_grad = position.components_at_entry.get("gex_gradient_score")
        if isinstance(entry_grad, (int, float)) and entry_grad:
            if (grad > 0) != (entry_grad > 0):
                return ExitDecision(should_close=True, reason="gradient_flipped")

        return super().exit_criteria(snap, position)
