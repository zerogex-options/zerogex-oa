"""HedgeImpulseQuietTape — front-run the pending dealer hedge, never chase it.

When a customer aggressively buys options, the dealer on the other side
inherits the opposite delta and must neutralize it in stock within minutes —
not discretionary. The obligation in SHARES is exactly

    Σ delta_i · (buy_volume_i − sell_volume_i) · 100

over the recent tape (``flow_contract_facts`` persists per-contract delta and
the Lee-Ready buy/sell split), surfaced as ``snap.hedge_impulse_shares`` and
normalized by same-window underlying volume (``hedge_impulse_ratio``).

Both retired flow-followers died the same death (PF 0.31 / 0.33, 33% win
rate, wins < losses): raw premium bursts COINCIDE with local price extremes,
so following them buys the top. The fix here is structural, not a tuned
threshold:

1. **Delta-weighting** — a $500k sweep of 0.03-delta lottos creates almost no
   hedge demand; it is exactly the noise that polluted raw premium.
2. **The FLAT-TAPE gate** — if a large hedge obligation has accrued while
   spot moved < ~0.15% in 15 minutes, the hedge has NOT been executed yet
   (blocks worked quietly, hedges being staged). We are ahead of the forced
   stock flow instead of behind it. The dead bots entered exactly when this
   bot refuses to.
3. **Negative-gamma conditioning** — in a short-gamma book the ensuing hedge
   is amplified by other dealers' own gamma hedging rather than absorbed.
   (``climax_flow_fade`` owns the positive-gamma mirror — fading the burst.)
4. **Persistence** — the impulse sign must have held across two consecutive
   evaluations; one-tick spikes are prints, not campaigns.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Dict, Optional, Tuple

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, OpenPosition, TradeSignal


class HedgeImpulseQuietTape(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "A hedge obligation is staged and the tape hasn't moved. Front-run it."
    description = (
        "Computes the literal shares dealers must trade to hedge the last "
        "~15 minutes of delta-weighted aggressor option flow and enters in "
        "the hedge direction ONLY while the tape is still flat, in negative "
        "gamma — ahead of the forced stock flow, never chasing it. "
        "Defined-risk vertical."
    )

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Last impulse observation per underlying: (sign, timestamp). Bot
        # instances persist across engine/backtest ticks, so this implements
        # the two-evaluation persistence gate without any DB state.
        self._last_impulse: Dict[str, Tuple[int, object]] = {}

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        m = snap.minutes_since_open
        mtc = snap.minutes_to_close
        if m is None or m < float(self.params.get("min_minutes_since_open", 30)):
            return self._skip("window")
        if mtc is None or mtc < float(self.params.get("min_minutes_to_close", 60)):
            return self._skip("window")

        ratio = snap.hedge_impulse_ratio()
        if ratio is None:
            return self._skip("no_impulse")

        # -- Persistence: the sign must have held across two consecutive
        # evaluations within the staleness window. Record-and-return on the
        # first sighting.
        sign = 1 if ratio > 0 else -1
        prev = self._last_impulse.get(snap.underlying)
        self._last_impulse[snap.underlying] = (sign, snap.timestamp)
        persist_min = float(self.params.get("persistence_max_minutes", 12))
        persisted = False
        if prev is not None and prev[0] == sign:
            try:
                age = (snap.timestamp - prev[1]).total_seconds() / 60.0
                persisted = 0 < age <= persist_min
            except TypeError:
                persisted = False

        min_ratio = float(self.params.get("min_impulse_ratio", 0.08))
        if abs(ratio) < min_ratio:
            return self._skip("impulse_small")
        if not persisted:
            return self._skip("not_persistent")

        # -- The defining gate: the tape must NOT have moved yet. Entering
        # after the move is exactly how the two dead flow bots bought tops.
        closes = [c for c in (snap.recent_closes or []) if c and c > 0]
        flat_bars = max(2, int(self.params.get("flat_lookback_bars", 15)))
        if len(closes) < flat_bars + 1:
            return self._skip("no_history")
        tape_move = (closes[-1] - closes[-1 - flat_bars]) / closes[-1 - flat_bars]
        max_flat = float(self.params.get("max_flat_move_pct", 0.0015))
        if abs(tape_move) > max_flat:
            return self._skip("tape_already_moved")

        # -- Regime: negative gamma amplifies the coming hedge; in positive
        # gamma dealers absorb it (that mirror belongs to climax_flow_fade).
        if snap.gex_regime() not in {"negative_weak", "negative_strong"}:
            return self._skip("regime")

        direction = "bullish" if ratio > 0 else "bearish"
        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        sat_ratio = float(self.params.get("quality_saturation_ratio", 0.20))
        quality = min(1.0, abs(ratio) / max(1e-9, sat_ratio))
        ml_components = {
            "hedge_impulse_ratio": ratio,
            "hedge_impulse_shares": snap.hedge_impulse_shares,
            "tape_move": tape_move,
            "net_gex": snap.net_gex,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

        expiration = resolve_expiration_iso(snap.et_date, int(self.params.get("dte_target", 0)))
        inc = snap.effective_strike_increment()
        width = max(1, int(self.params.get("spread_width_increments", 2)))
        long_strike = snap.round_to_strike(snap.spot)
        short_strike = long_strike + (width * inc if direction == "bullish" else -width * inc)
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_vertical(snap.underlying, opt_type, long_strike, short_strike, expiration)
        strat = "BULL_CALL_DEBIT_SPREAD" if direction == "bullish" else "BEAR_PUT_DEBIT_SPREAD"

        target_pct = float(self.params.get("target_pct", 0.004))
        stop_pct = float(self.params.get("stop_pct", 0.003))
        if direction == "bullish":
            target = snap.spot * (1.0 + target_pct)
            stop = snap.spot * (1.0 - stop_pct)
        else:
            target = snap.spot * (1.0 - target_pct)
            stop = snap.spot * (1.0 + stop_pct)
        hold = int(self.params.get("max_hold_minutes", 75))

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
                f"pending hedge {snap.hedge_impulse_shares:+.2e} sh "
                f"({ratio * 100:+.1f}% of tape), tape flat "
                f"({tape_move * 100:+.2f}% / {flat_bars}m), {snap.gex_regime()}"
            ),
            components_at_entry={
                "hedge_impulse_ratio": ratio,
                "hedge_impulse_shares": snap.hedge_impulse_shares,
                "tape_move": tape_move,
                "entry_spot": snap.spot,
                "quality": quality,
            },
        )

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
        """Default stack plus impulse-reversal and thesis-expiry checks.

        * ``impulse_flip`` — the 15-min impulse flipped sign with meaningful
          size: the inventory reversed, the forced flow now points against
          the position.
        * ``impulse_expired`` — the tape stayed flat well past entry: the
          hedge got internalized or crossed off-tape; the thesis had a clock
          and it ran out.
        """
        ratio = snap.hedge_impulse_ratio()
        entry_ratio = position.components_at_entry.get("hedge_impulse_ratio")
        if ratio is not None and isinstance(entry_ratio, (int, float)) and entry_ratio:
            flip_ratio = float(self.params.get("flip_exit_ratio", 0.04))
            if (ratio > 0) != (entry_ratio > 0) and abs(ratio) >= flip_ratio:
                return ExitDecision(should_close=True, reason="impulse_flip")
        entry_spot = position.components_at_entry.get("entry_spot")
        expire_min = float(self.params.get("no_move_expiry_minutes", 45))
        if isinstance(entry_spot, (int, float)) and entry_spot and position.opened_at is not None:
            try:
                age_min = (_utcnow() - position.opened_at).total_seconds() / 60.0
            except TypeError:
                age_min = 0.0
            still_flat = abs(snap.spot - float(entry_spot)) / float(entry_spot) < float(
                self.params.get("no_move_expiry_pct", 0.001)
            )
            if age_min >= expire_min and still_flat:
                return ExitDecision(should_close=True, reason="impulse_expired")
        return super().exit_criteria(snap, position)
