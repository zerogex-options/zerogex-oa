"""WeeklyCharmGrind — ride the weekly book's deterministic midday delta decay.

Charm is the one forced flow driven by a variable that advances with
certainty: clock time. The deltas of the 1–7 DTE weekly book — concentrated
in OTM put hedges and call overwrites — decay intraday whether or not price
moves, and delta-neutral dealer desks must rebalance to the decayed deltas on
their normal intraday schedule. During the 11:00–14:00 lull, when a positive-
gamma book damps noise and gamma-hedge flow is near zero, that charm
rebalance becomes the DOMINANT forced flow and expresses as a slow one-way
grind.

ZeroGEX persists exactly the required input and nobody consumes it: the
per-expiration-bucket dealer charm ladder (``gex_by_strike.dealer_charm_exposure``
grouped by ``expiration_bucket``), surfaced as ``dealer_charm_0dte`` /
``dealer_charm_weekly``. The bucket-dominance gate (weekly ≫ 0dte) keeps this
bot strictly out of ``charm_close_magnet``'s territory: different field
family, different clock (hard exit at 15:00 ET, an hour before the other
bot's window ends), different logic (directional drift from weekly-book
decay, not magnet reversion).

Structure: a 1DTE debit vertical — the edge is a 25–35 bp grind over 2–3
hours, and 0DTE ATM theta over that window eats exactly that much; 1DTE
roughly halves the theta at similar net delta, and the position always closes
same-day so no overnight-marking gap applies.

Known caveat, embraced: the ``dealer_charm_exposure`` column's sign
convention is flagged in ``forced_flow.py`` as inconsistent with the
forced-flow model-A convention. The same field family already drives
``vanna_vol_crush_rider``; the screen is the arbiter of sign — a systematic
inversion in the backtest is a data bug to report, not a tuning axis.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, OpenPosition, TradeSignal


class WeeklyCharmGrind(BaseBot):
    tier = "1DTE"
    direction_mode = "context"
    tagline = "Quiet positive-gamma midday: the weekly book's time decay is the only flow."
    description = (
        "Rides the deterministic delta-decay rebalance of the 1-7 DTE weekly "
        "dealer book through the 11:00-14:00 lull — only when the weekly "
        "charm bucket dominates the 0DTE bucket, the session is compressed, "
        "and the book is positive-gamma quiet enough for a small persistent "
        "flow to express as drift. 1DTE debit vertical, hard exit 15:00 ET."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        m = snap.minutes_since_open
        window_start = float(self.params.get("window_start_minutes", 90))
        window_end = float(self.params.get("window_end_minutes", 270))
        if m is None or m < window_start or m > window_end:
            return self._skip("window")

        # -- Damped regime: positive gamma suppresses noise so a small
        # persistent flow can express as drift.
        if snap.gex_regime() not in {"positive_strong", "positive_weak"}:
            return self._skip("regime")

        # -- Compressed session so far: an established trend is already
        # consuming (or swamping) the charm flow.
        if snap.session_high is None or snap.session_low is None or snap.spot <= 0:
            return self._skip("no_session_range")
        session_range = (snap.session_high - snap.session_low) / snap.spot
        max_range = float(self.params.get("max_session_range_pct", 0.006))
        if session_range > max_range:
            return self._skip("range_wide")

        # -- The signal: the weekly bucket's charm, and it must DOMINATE the
        # 0DTE bucket (same-day settlement mechanics belong to other bots).
        charm_w = snap.dealer_charm_weekly
        charm_0 = snap.dealer_charm_0dte
        if charm_w is None or charm_w == 0.0:
            return self._skip("no_bucket_charm")
        dominance = float(self.params.get("min_bucket_dominance", 2.0))
        if charm_0 is not None and abs(charm_w) < dominance * abs(charm_0):
            return self._skip("odte_dominates")

        # -- Magnitude, relative: the charm flow still to run inside the
        # window must rival the gamma-hedge flow of a ~10 bp move.
        # dealer_charm_exposure is $/day; the window fraction of the trading
        # day converts it to remaining-window dollars.
        local = snap.local_gex
        if local is None or local <= 0:
            return self._skip("no_local_gex")
        window_frac = max(0.0, (window_end - m)) / 390.0
        remaining_flow = abs(charm_w) * window_frac
        min_frac = float(self.params.get("min_flow_local_gex_frac", 0.10))
        if remaining_flow < min_frac * local:
            return self._skip("flow_small")

        # Dealer sign convention on the source column: positive charm =>
        # dealers BUY as time passes.
        direction = "bullish" if charm_w > 0 else "bearish"
        if self._bias_veto(snap, direction):
            return self._skip("bias_veto")

        flow_ratio = remaining_flow / local
        dom_ratio = abs(charm_w) / max(1e-9, abs(charm_0)) if charm_0 else 4.0
        # Quality saturates just above the entry floor: a setup that cleared
        # every hard gate must also be able to clear the conviction gate (the
        # first screen's 3 sole gate-survivors all died at conviction because
        # the old /0.25 scale rated them ~0.4).
        flow_sat = float(self.params.get("quality_flow_saturation", 0.15))
        quality = min(1.0, flow_ratio / max(1e-9, flow_sat)) * min(1.0, max(0.5, dom_ratio / 4.0))
        ml_components = {
            "dealer_charm_weekly": charm_w,
            "dealer_charm_0dte": charm_0,
            "flow_ratio": flow_ratio,
            "session_range": session_range,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return self._skip("conviction")

        # -- Structure: 1DTE debit vertical (≈half the theta of 0DTE for the
        # same net delta over a multi-hour hold; always exits same-day).
        expiration = resolve_expiration_iso(snap.et_date, int(self.params.get("dte_target", 1)))
        inc = snap.effective_strike_increment()
        width = max(1, int(self.params.get("spread_width_increments", 2)))
        long_strike = snap.round_to_strike(snap.spot)
        short_strike = long_strike + (width * inc if direction == "bullish" else -width * inc)
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_vertical(snap.underlying, opt_type, long_strike, short_strike, expiration)
        strat = "BULL_CALL_DEBIT_SPREAD" if direction == "bullish" else "BEAR_PUT_DEBIT_SPREAD"

        target_pct = float(self.params.get("target_pct", 0.0035))
        stop_pct = float(self.params.get("stop_pct", 0.0025))
        if direction == "bullish":
            target = snap.spot * (1.0 + target_pct)
            stop = snap.spot * (1.0 - stop_pct)
        else:
            target = snap.spot * (1.0 - target_pct)
            stop = snap.spot * (1.0 + stop_pct)

        # Hard exit at ~15:00 ET — always out before the EOD window where the
        # 0DTE settlement/close-charm flows take over (charm_close_magnet's
        # regime) and never holding the 1DTE structure overnight.
        mtc = snap.minutes_to_close
        eod_exit_min = float(self.params.get("eod_exit_minutes_before_close", 60))
        hold = max(1.0, (mtc - eod_exit_min) if mtc is not None else 1.0)

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
                f"weekly charm {charm_w:+.2e}/day ({dom_ratio:.1f}x the 0DTE bucket), "
                f"remaining-window flow {flow_ratio:.2f}x local_gex; "
                f"session range {session_range * 100:.2f}%, {snap.gex_regime()}"
            ),
            components_at_entry={
                "dealer_charm_weekly": charm_w,
                "dealer_charm_0dte": charm_0,
                "flow_ratio": flow_ratio,
                "dominance_ratio": dom_ratio,
                "session_range": session_range,
                "quality": quality,
            },
        )

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
        """Default stack plus positioning-turnover invalidation.

        A grind thesis dies the moment the book that generates it turns
        over: the weekly charm flips sign (drift now points the other way)
        or the 0DTE bucket catches up to the weekly bucket (the ladder is no
        longer weekly-dominated — e.g. a big same-day print landed). Missing
        fields on a tick never force an exit.
        """
        charm_w = snap.dealer_charm_weekly
        charm_0 = snap.dealer_charm_0dte
        entry_charm = position.components_at_entry.get("dealer_charm_weekly")
        if charm_w is not None and isinstance(entry_charm, (int, float)) and entry_charm:
            if (charm_w > 0) != (entry_charm > 0):
                return ExitDecision(should_close=True, reason="charm_flip")
            if charm_0 is not None and charm_0 != 0:
                exit_dom = float(self.params.get("exit_bucket_dominance", 1.2))
                if abs(charm_w) < exit_dom * abs(charm_0):
                    return ExitDecision(should_close=True, reason="bucket_mixed")
        return super().exit_criteria(snap, position)
