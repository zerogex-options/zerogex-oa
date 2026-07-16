"""PutWallMagnetReversal — fade a *massive* put wall in negative gamma.

The sibling of PutCallWallBouncer, built for the regime the Bouncer
refuses to trade. In NEGATIVE gamma (spot below the gamma flip) dealer
hedging amplifies moves rather than defending walls, so the Bouncer's
"walls hold" edge is gone. But a *historically large* put wall can still
bounce price even here — not via dealer-gamma support, but as a
max-pain / charm magnet and a huge-OI liquidity node into which a fast
sell-off exhausts. That is a weaker, probabilistic edge with a FAT LEFT
TAIL: when a put wall breaks in negative gamma the move cascades. So the
whole risk posture is inverted from the Bouncer —

* **Selectivity, not room.** Only fires when the put wall is in the top
  ``min_put_wall_pctile`` of its own history (``put_wall_strength_pctile``,
  the commit-1 symbol-relative measure) — the "massive enough" gate. No
  history yet -> no trade.
* **Defined risk.** A bull call debit spread caps the loss at the debit if
  the magnet fails and price cascades.
* **Fast, tight stops.** Tight ``wall_break_*`` buffer, single-bar
  confirmation, ``wall_break_strength_widen = 0`` (a big wall raises size
  and conviction but must NOT loosen the stop — the cascade does not care
  how big the wall was), and a tight premium damage-control cap. This is
  the deliberate opposite of the Bouncer's "let it play out".

Target is the gamma flip — the regime boundary the bounce is reverting
toward. Only trades ``negative_weak`` (not deep ``negative_strong``): a
steamroller in strongly-negative gamma is too large to stand in front of.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec, TradeSignal


class PutWallMagnetReversal(BaseBot):
    tier = "0DTE"
    direction_mode = "bullish"
    tagline = "Negative-γ knife into a massive put wall. Fade the magnet, cut fast on a break."
    description = (
        "Fades a historically large put wall in a negative-gamma regime — a "
        "max-pain / liquidity-node magnet, not a dealer-defended wall. Bull "
        "call debit spread caps risk; a tight, single-bar-confirmed wall-break "
        "stop cuts fast because a break in negative gamma cascades. Target is "
        "the gamma flip. Sizes up into the biggest walls, never loosens the stop."
    )

    # The bot's inverted risk profile travels with the class so it is
    # self-contained even without operator param overrides. Every value is
    # still overridable via the tw_bots.params column. Contrast the fade
    # Bouncer's looser defaults (min 0.3% / max 1.2% / 2-bar confirm / widen
    # 0.5 / 40% premium cap).
    _DEFAULTS: Dict[str, Any] = {
        "min_put_wall_pctile": 90.0,  # "massive enough" gate
        "wall_proximity_pct": 0.002,  # spot must be within 0.2% above the wall
        "wall_collapse_pct": 0.003,  # skip if the wall itself is sliding down
        "min_minutes_since_open": 30,
        "max_spread_width_pct": 0.004,  # cap the debit-spread width
        "max_hold_minutes": 45,  # if it hasn't bounced fast, it's breaking
        # Tight, fast wall-break stop (read by BaseBot._wall_break_buffer_pct
        # / _beyond_confirmed / _wall_stop_signal).
        "wall_break_min_pct": 0.0015,
        "wall_break_max_pct": 0.005,
        "wall_break_confirm_bars": 1,
        "wall_break_strength_widen": 0.0,
        "wall_shift_pct": 0.002,
        # Hard dollar cap — tighter than the fade bot's 0.40.
        "max_premium_loss_pct": 0.25,
    }

    def __init__(self, spec: BotSpec, ml_state: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(spec, ml_state=ml_state)
        for key, value in self._DEFAULTS.items():
            self.params.setdefault(key, value)

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        # Regime gate — the INVERSE of the Bouncer. Must be negative gamma,
        # but only the shallow band: a deep negative_strong cascade is too
        # big to fade.
        if snap.gex_regime() != "negative_weak":
            return None

        m = snap.minutes_since_open
        if m is None or m < float(self.params.get("min_minutes_since_open", 30)):
            return None

        if snap.put_wall is None or snap.spot <= 0:
            return None

        # "Massive enough" gate — the heart of the thesis. Requires the
        # symbol-relative percentile, so the bot is inert until the nightly
        # refresh has accumulated a wall-strength distribution (correct: we
        # do not trade "massive" until we can measure it).
        pctile = snap.put_wall_strength_pctile
        if pctile is None or pctile < float(self.params.get("min_put_wall_pctile", 90.0)):
            return None

        # Proximity: spot pressed just above the put wall (approaching from
        # above). distance_to_put_wall_pct = (spot - put_wall) / spot.
        put_d = snap.distance_to_put_wall_pct()
        prox = float(self.params.get("wall_proximity_pct", 0.002))
        if put_d is None or put_d < 0.0 or put_d > prox:
            return None
        if snap.spot < snap.put_wall:
            return None

        # Wall-stability gate: a put wall sliding DOWN into the drop is
        # support evaporating, not a magnet — skip. (prior_put_wall is the
        # immediately-preceding gex_summary wall.)
        collapse = float(self.params.get("wall_collapse_pct", 0.003))
        if snap.prior_put_wall is not None and snap.put_wall < snap.prior_put_wall * (
            1.0 - collapse
        ):
            return None

        # Target = the gamma flip (regime boundary the bounce reverts
        # toward). Must be above spot to be a valid upside target; fall back
        # to max_pain, then a small fixed reversion.
        target = None
        if snap.gamma_flip is not None and snap.gamma_flip > snap.spot:
            target = snap.gamma_flip
        elif snap.max_pain is not None and snap.max_pain > snap.spot:
            target = snap.max_pain
        else:
            target = snap.spot * 1.003

        wall_strength_norm = max(0.0, min(1.0, pctile / 100.0))
        # Quality: weight the wall's historical size heavily (the thesis),
        # plus a LATE-session tilt — the magnet strengthens into the close as
        # 0DTE charm / max-pain pull grows (the inverse of the Bouncer's
        # earlier-is-better session term).
        session_score = max(0.0, min(1.0, m / 360.0))
        quality = 0.6 * wall_strength_norm + 0.4 * session_score

        ml_components = {
            "net_gex": snap.net_gex,
            "vix": snap.vix,
            "distance_pct": put_d,
        }
        conviction = self.compute_conviction(snap, quality, components=ml_components)
        if conviction < self.confidence_threshold():
            return None

        dte_target = int(self.params.get("dte_target", 0))
        expiration = resolve_expiration_iso(snap.et_date, dte_target)
        increment = snap.effective_strike_increment()
        long_strike = snap.round_to_strike(snap.spot)
        # Short leg reaches toward the target, capped to a max width so the
        # debit stays small; never narrower than one strike.
        max_width = float(self.params.get("max_spread_width_pct", 0.004)) * snap.spot
        raw_short = min(target, snap.spot + max_width)
        short_strike = snap.round_to_strike(raw_short)
        if short_strike <= long_strike:
            short_strike = long_strike + increment
        legs = self.build_vertical(snap.underlying, "call", long_strike, short_strike, expiration)

        return TradeSignal(
            bot_id=self.spec.id,
            underlying=snap.underlying,
            direction="bullish",
            strategy_type="BULL_CALL_DEBIT_SPREAD",
            legs=legs,
            entry_price=0.0,  # filled by engine
            conviction=conviction,
            target_price=target,
            # No static stop: the tight, single-bar-confirmed wall-break stop
            # is owned by BaseBot._wall_stop_signal, keyed off wall_ref_side.
            stop_price=None,
            time_stop_at=_utcnow()
            + timedelta(minutes=int(self.params.get("max_hold_minutes", 45))),
            wall_ref_price=snap.put_wall,
            wall_ref_side="put",
            rationale=(
                f"Negative-γ put-wall magnet at {snap.put_wall} "
                f"(strength {pctile:.0f}th pct); target flip {target:.2f}; "
                f"conviction {conviction:.2f}; tight confirmed break stop"
            ),
            components_at_entry={
                "wall_side": "put",
                "wall_price": snap.put_wall,
                "net_gex": snap.net_gex,
                "distance_pct": put_d,
                "vix": snap.vix,
                "quality": quality,
                "gamma_flip": snap.gamma_flip,
                "session_score": session_score,
                # 0..1 sizing input — sizes UP into a bigger wall (capped at
                # 1.5x by sizing.wall_scale), but the stop stays tight
                # (wall_break_strength_widen=0).
                "wall_strength": wall_strength_norm,
                "wall_strength_pctile": pctile,
                "wall_strength_dollar": snap.put_wall_strength,
            },
        )
