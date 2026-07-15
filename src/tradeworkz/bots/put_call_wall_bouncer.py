"""PutCallWallBouncer — the flagship wall-fade bot.

Fires when SPY is pressed against a wall in a positive-gamma regime. The
edge is dealer hedging: at the call wall dealers are net long gamma above
spot and mechanically sell rallies; at the put wall they buy dips. The bot
sizes proportional to wall strength × conviction and cuts on:

* underlying blowing past the wall by ``wall_break_pct`` (default 0.3%),
* the wall itself migrating in the direction that disfavors the trade,
* configured target hit (max_pain / gamma_flip),
* configured stop or time-stop.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal

# Floor for the session-recency term in :meth:`PutCallWallBouncer._quality`.
# The session term is a mild "earlier is better" tilt, not a veto — see the
# regression note in ``_quality``. Flooring it here (rather than letting it
# decay to 0) keeps a strong-gamma wall setup tradeable in the final hour of
# the session, prime 0DTE wall-defense time.
_SESSION_SCORE_FLOOR = 0.5


class PutCallWallBouncer(BaseBot):
    tier = "0DTE"
    direction_mode = "context"
    tagline = "Fade the wall. Ride the mean reversion. Cut if the wall breaks."
    description = (
        "Enters mean-reversion trades against the nearest call or put wall in "
        "a positive-gamma regime. Size scales with the wall's dollar-gamma "
        "strength; risk is capped by an intraday wall-break stop."
    )

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        if snap.gex_regime() not in {"positive_strong", "positive_weak"}:
            return None
        m = snap.minutes_since_open
        if m is None or m < 30:  # avoid opening-print noise
            return None
        prox_pct = float(self.params.get("wall_proximity_pct", 0.002))
        call_d = snap.distance_to_call_wall_pct()
        put_d = snap.distance_to_put_wall_pct()

        direction: Optional[str] = None
        wall_side: Optional[str] = None
        wall_price: Optional[float] = None
        target: Optional[float] = None

        if call_d is not None and abs(call_d) <= prox_pct and snap.spot <= (snap.call_wall or 0):
            direction = "bearish"
            wall_side = "call"
            wall_price = snap.call_wall
            target = snap.max_pain or snap.gamma_flip or snap.spot * 0.997
        elif put_d is not None and abs(put_d) <= prox_pct and snap.spot >= (snap.put_wall or 0):
            direction = "bullish"
            wall_side = "put"
            wall_price = snap.put_wall
            target = snap.max_pain or snap.gamma_flip or snap.spot * 1.003
        else:
            return None

        quality = self._quality(snap, wall_side)
        conviction = self.compute_conviction(snap, quality)
        if conviction < self.confidence_threshold():
            return None

        # Wall-size-relative sizing input. The engine reads
        # components_at_entry["wall_strength"] (0..1) and passes it to
        # sizing.compute_contracts, where wall_scale = 0.5 + strength maps
        # it to a 0.5..1.5x size knob. We feed the wall's dollar-gamma
        # PERCENTILE (how large this wall is vs the symbol's own 30d
        # history at this time of day), normalized to 0..1. None when
        # history is absent -> the engine leaves wall_strength unset and
        # sizing stays wall-agnostic (scale 1.0), exactly as before.
        wall_strength_pctile = (
            snap.call_wall_strength_pctile if wall_side == "call" else snap.put_wall_strength_pctile
        )
        wall_strength_dollar = (
            snap.call_wall_strength if wall_side == "call" else snap.put_wall_strength
        )
        wall_strength_norm = (
            max(0.0, min(1.0, wall_strength_pctile / 100.0))
            if wall_strength_pctile is not None
            else None
        )

        expiration = _today_expiration(snap)
        strike = snap.round_to_strike(snap.spot)
        opt_type = "call" if direction == "bullish" else "put"
        legs = self.build_atm_debit(snap.underlying, opt_type, strike, expiration, 0.0)
        stop = (wall_price or snap.spot) * (
            1.0 + float(self.params.get("wall_break_pct", 0.003))
            if direction == "bearish"
            else 1.0 - float(self.params.get("wall_break_pct", 0.003))
        )

        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction=direction,
            strategy_type="BUY_CALL_DEBIT" if direction == "bullish" else "BUY_PUT_DEBIT",
            legs=legs,
            entry_price=0.0,  # filled by engine
            conviction=conviction,
            target_price=target,
            stop_price=stop,
            time_stop_at=_utcnow()
            + timedelta(minutes=int(self.params.get("max_hold_minutes", 90))),
            wall_ref_price=wall_price,
            wall_ref_side=wall_side,
            rationale=(
                f"{wall_side}-wall rejection at {wall_price} in {snap.gex_regime()} gamma; "
                f"conviction {conviction:.2f}"
            ),
            components_at_entry={
                "wall_side": wall_side,
                "wall_price": wall_price,
                "net_gex": snap.net_gex,
                "distance_pct": call_d if wall_side == "call" else put_d,
                "vix": snap.vix,
                "quality": quality,
                # 0..1 sizing input (None -> wall-agnostic); the raw dollar
                # magnitude and its percentile are kept for the audit trail
                # and for the wall-aware stop (commit 2 reads them at exit).
                "wall_strength": wall_strength_norm,
                "wall_strength_pctile": wall_strength_pctile,
                "wall_strength_dollar": wall_strength_dollar,
            },
        )

    def _quality(self, snap: MarketSnapshot, wall_side: str) -> float:
        """Bot-authored 0..1 read of setup quality.

        Rewards deeper positive-gamma and — as a mild tilt — an earlier
        session (more time for mean reversion to play out). The session
        term is a *preference*, not a veto: a strong-gamma wall rejection
        is a valid fade all session long, including the final hour, which
        is prime 0DTE wall-defense time as dealers pin toward expiry.

        The gamma component is symbol-relative when a historical
        distribution exists — ``net_gex_pctile`` (0..100) says how strong
        today's dealer gamma is for THIS symbol — and falls back to the old
        absolute ``net_gex / 3e9`` scale otherwise, so a SPY-tuned constant
        no longer has to stand in for SPX-scale (or any other) dollar-gamma.

        Regression guard: the session term used to decay linearly to 0 at
        ~15:00 ET (``mins - 30 >= 300``). At the 0.5 weight below, that
        single term subtracted enough that ``compute_conviction`` could no
        longer reach ``confidence_threshold`` regardless of how strong the
        gamma / how clean the wall touch — a dead-zone in which the Bouncer
        was *mathematically unable* to open in the last hour of the
        session. Flooring the session term at ``_SESSION_SCORE_FLOOR``
        preserves the earlier-is-better tilt while letting a high-gamma
        setup still clear the bar late in the day. Weak-gamma setups are
        unaffected — they still fall short of the threshold as intended.
        """
        if snap.net_gex_pctile is not None:
            gex_score = min(1.0, max(0.0, snap.net_gex_pctile / 100.0))
        else:
            gex = snap.net_gex or 0.0
            gex_score = min(1.0, max(0.0, gex / 3.0e9))
        mins = snap.minutes_since_open or 0.0
        session_decay = 1.0 - min(1.0, max(0.0, (mins - 30) / 300.0))
        session_score = max(_SESSION_SCORE_FLOOR, session_decay)
        return 0.5 * gex_score + 0.5 * session_score


def _today_expiration(snap: MarketSnapshot) -> str:
    """Same-session expiration as ISO date — TradeWorkz assumes 0DTE cash-settled."""
    return snap.et_date.isoformat()
