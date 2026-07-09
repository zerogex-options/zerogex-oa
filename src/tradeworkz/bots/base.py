"""Base class every TradeWorkz bot inherits from.

A bot is stateless across ticks — it looks at the :class:`MarketSnapshot`
and either returns a :class:`TradeSignal` (open / add) or ``None``. The
engine handles capital, fills, DB writes, and notifications.

The base class also owns the ML-driven confidence / sizing knobs. Bots
subclass and override:

* :meth:`open_criteria` — return a :class:`TradeSignal` or ``None``.
* :meth:`exit_criteria` — decide close/add/cut for an open position.

The class methods :meth:`build_legs`, :meth:`_atm_debit`, and the confidence
helpers are shared boilerplate.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec, ExitDecision, Leg, OpenPosition, TradeSignal

logger = logging.getLogger(__name__)


def resolve_expiration_iso(from_et_date: date, dte_target: int) -> str:
    """ISO expiration date ``dte_target`` trading days after ``from_et_date``.

    ``dte_target == 0`` returns ``from_et_date`` (same-day 0DTE). Positive
    values walk forward counting only weekdays — Sat / Sun are skipped so
    a "1DTE" entered on Friday resolves to the following Monday, matching
    what an options chain actually offers. Holidays aren't accounted for
    at this level; the ingest layer either has the expiration or it
    doesn't, and the spread_price lookup fails closed if not.
    """
    if dte_target <= 0:
        return from_et_date.isoformat()
    d = from_et_date
    counted = 0
    while counted < dte_target:
        d = d + timedelta(days=1)
        if d.weekday() < 5:  # Mon..Fri
            counted += 1
    return d.isoformat()


class BaseBot:
    """Common surface implemented by every bot."""

    #: Registry id — must be unique across the fleet.
    bot_id: str
    #: Human-readable name for the leaderboard.
    display_name: str
    #: '0DTE' | '1DTE' | 'swing'.
    tier: str = "0DTE"
    #: 'bullish' | 'bearish' | 'context' | 'non_directional'.
    direction_mode: str = "context"
    tagline: str = ""
    description: str = ""

    def __init__(self, spec: BotSpec, ml_state: Optional[Dict[str, Any]] = None) -> None:
        self.spec = spec
        self.params: Dict[str, Any] = dict(spec.params or {})
        self.ml_state: Dict[str, Any] = dict(ml_state or {})

    # -- ML knobs --------------------------------------------------------

    def confidence_base(self) -> float:
        """Calibrated pattern base hit-rate, clamped to [0.4, 0.85]."""
        v = self.ml_state.get("confidence_base")
        if v is None:
            return float(self.params.get("confidence_base_default", 0.55))
        return float(v)

    def size_multiplier(self) -> float:
        """ML-tuned sizing knob: 0.5..1.25 depending on rolling profit factor."""
        v = self.ml_state.get("size_multiplier")
        if v is None:
            return 1.0
        return max(0.25, min(1.5, float(v)))

    def confidence_threshold(self) -> float:
        v = self.ml_state.get("confidence_threshold")
        if v is None:
            return float(self.params.get("confidence_threshold_default", 0.55))
        return float(v)

    def _max_premium_loss_pct(self) -> float:
        """Resolve the premium damage-control threshold for this bot.

        Per-bot ``params['max_premium_loss_pct']`` (if set) takes
        precedence over the fleet-wide ``tw_config.MAX_PREMIUM_LOSS_PCT``
        so an operator can loosen the stop for high-variance strategies
        like a straddle without touching the env var, and tighten it for
        theta trades where the wing risk is different. A value of 0
        disables the check for this bot.
        """
        from src.tradeworkz import config as tw_config

        override = self.params.get("max_premium_loss_pct")
        if override is not None:
            try:
                return max(0.0, min(1.0, float(override)))
            except (TypeError, ValueError):
                pass
        return float(tw_config.MAX_PREMIUM_LOSS_PCT)

    # -- Subclass overrides ---------------------------------------------

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        """Return a signal if the bot wants to open (or add). Default: no-op."""
        return None

    def exit_criteria(
        self, snap: MarketSnapshot, position: OpenPosition
    ) -> ExitDecision:
        """Default exit policy — target / stop / time-stop. Subclasses can extend.

        Every bot in the fleet sets ``position.target_price`` and
        ``position.stop_price`` at UNDERLYING-SPOT levels (e.g., "close
        the bearish put when SPY drops to 740"), not at option-premium
        levels. The comparison here must therefore be against
        ``snap.spot`` — the current spot price of the underlying — NOT
        against ``position.current_price``, which is the mark-to-market
        option premium (e.g., $0.68).

        The prior version compared the spot-level target against the
        option premium, which was numerically absurd: for a bearish put
        with target=740, ``option_premium (0.68) <= 740`` was trivially
        True and the trade closed as "target" on the first tick after
        the min-hold window — regardless of whether SPY had actually
        moved to the target. That's why the audit showed "target" as
        the close reason on trades that lost money and why every trade
        held for about 90 seconds and then exited.

        min_hold_until still gates the exit — a bot's price level can
        legitimately be hit inside the first 90 seconds, but we require
        a minimum patience window so a wick doesn't unwind the whole
        thesis. time_stop_at forces a close after the bot's configured
        max_hold_minutes even if neither target nor stop hit.
        """
        # min_hold_until: we may only take price-level exits AFTER this
        # elapses. Time-stop is still evaluated below.
        now = _utcnow()
        in_min_hold = position.min_hold_until is not None and now < position.min_hold_until

        # Premium damage-control stop. Fires REGARDLESS of min_hold —
        # damage-control trumps patience. Spot-based structural stops
        # invalidate the thesis, but on 0DTE ATM debits a 0.3-1% adverse
        # spot move can eat 60-80% of premium before spot reaches the
        # structural stop. Anything worse than a configurable premium
        # loss threshold exits here first.
        #
        # Skipped when entry_price is zero or negative (defensive: never
        # divide) and when current_price is None. Also skipped for a
        # credit structure where entry_price could be negative — those
        # need a wider profit-loss check the reconciler can grow into
        # later. For now only long-debit structures (entry_price > 0)
        # get the check, which covers every current bot.
        pct = self._max_premium_loss_pct()
        if (
            pct > 0
            and position.entry_price and position.entry_price > 0
            and position.current_price is not None
        ):
            loss_pct = 1.0 - (position.current_price / position.entry_price)
            if loss_pct >= pct:
                return ExitDecision(should_close=True, reason="premium_stop")

        spot = snap.spot
        if not in_min_hold and position.target_price is not None:
            if position.direction == "bullish" and spot >= position.target_price:
                return ExitDecision(should_close=True, reason="target")
            if position.direction == "bearish" and spot <= position.target_price:
                return ExitDecision(should_close=True, reason="target")
        if not in_min_hold and position.stop_price is not None:
            if position.direction == "bullish" and spot <= position.stop_price:
                return ExitDecision(should_close=True, reason="stop")
            if position.direction == "bearish" and spot >= position.stop_price:
                return ExitDecision(should_close=True, reason="stop")

        if in_min_hold:
            return ExitDecision(should_close=False)
        if position.time_stop_at and now >= position.time_stop_at:
            return ExitDecision(should_close=True, reason="time_stop")

        # Wall-drift and wall-shift risk management: if the bot referenced a
        # wall at entry, cut when the underlying blows through it or the wall
        # itself migrates against the trade. This is the common risk logic —
        # subclasses turn it on by setting position.wall_ref_price at entry.
        drift = self._wall_stop_signal(snap, position)
        if drift is not None:
            return drift
        return ExitDecision(should_close=False)

    # -- Shared helpers used by concrete bots ---------------------------

    def _wall_stop_signal(
        self, snap: MarketSnapshot, position: OpenPosition
    ) -> Optional[ExitDecision]:
        """Cut if the trade blows through the referenced wall or the wall
        migrates the wrong way. Return ``None`` when no wall exit fires.
        """
        ref = position.wall_ref_price
        side = (position.wall_ref_side or "").lower()
        if ref is None or side not in {"call", "put"}:
            return None
        wall_drift_pct = float(self.params.get("wall_drift_pct", 0.003))
        spot = snap.spot
        if side == "call":
            # Bullish trades fade at the call wall; bearish momentum breaks
            # through it. Symmetry: whichever direction, blowing past the
            # wall by wall_drift_pct is a stop event.
            if position.direction == "bullish" and spot > ref * (1.0 + wall_drift_pct):
                return ExitDecision(should_close=True, reason="wall_break")
            if position.direction == "bearish" and spot < ref * (1.0 - wall_drift_pct):
                return ExitDecision(should_close=True, reason="wall_break")
            if snap.call_wall is not None and position.direction == "bullish":
                if snap.call_wall > ref * (1.0 + wall_drift_pct):
                    return ExitDecision(should_close=True, reason="wall_shift")
        else:  # put
            if position.direction == "bearish" and spot < ref * (1.0 - wall_drift_pct):
                return ExitDecision(should_close=True, reason="wall_break")
            if position.direction == "bullish" and spot > ref * (1.0 + wall_drift_pct):
                return ExitDecision(should_close=True, reason="wall_break")
            if snap.put_wall is not None and position.direction == "bearish":
                if snap.put_wall < ref * (1.0 - wall_drift_pct):
                    return ExitDecision(should_close=True, reason="wall_shift")
        return None

    def build_atm_debit(
        self,
        underlying: str,
        option_type: str,
        strike: float,
        expiration: str,
        entry_price: float,
    ) -> List[Leg]:
        """Convenience: single-leg long debit structure."""
        return [
            Leg(
                option_symbol=_synthetic_option_symbol(underlying, option_type, strike, expiration),
                side="long",
                option_type=option_type,
                strike=strike,
                expiration=expiration,
            )
        ]

    def build_vertical(
        self,
        underlying: str,
        option_type: str,
        long_strike: float,
        short_strike: float,
        expiration: str,
    ) -> List[Leg]:
        """Convenience: five-point-ish debit or credit vertical."""
        return [
            Leg(
                option_symbol=_synthetic_option_symbol(
                    underlying, option_type, long_strike, expiration
                ),
                side="long",
                option_type=option_type,
                strike=long_strike,
                expiration=expiration,
            ),
            Leg(
                option_symbol=_synthetic_option_symbol(
                    underlying, option_type, short_strike, expiration
                ),
                side="short",
                option_type=option_type,
                strike=short_strike,
                expiration=expiration,
            ),
        ]

    def build_straddle(
        self,
        underlying: str,
        strike: float,
        expiration: str,
    ) -> List[Leg]:
        """Long straddle — buy an ATM call and put at the same strike.

        Non-directional volatility bet: profits when the underlying moves
        far enough in EITHER direction to overcome the combined premium.
        Sizing sees `entry_price` as the total debit paid for both legs.
        """
        return [
            Leg(
                option_symbol=_synthetic_option_symbol(underlying, "call", strike, expiration),
                side="long",
                option_type="call",
                strike=strike,
                expiration=expiration,
            ),
            Leg(
                option_symbol=_synthetic_option_symbol(underlying, "put", strike, expiration),
                side="long",
                option_type="put",
                strike=strike,
                expiration=expiration,
            ),
        ]

    def build_strangle(
        self,
        underlying: str,
        call_strike: float,
        put_strike: float,
        expiration: str,
    ) -> List[Leg]:
        """Long strangle — buy an OTM call above and OTM put below spot.

        Similar payoff to a straddle but cheaper because both legs start
        OTM. ``call_strike`` should be > spot, ``put_strike`` < spot.
        """
        return [
            Leg(
                option_symbol=_synthetic_option_symbol(underlying, "call", call_strike, expiration),
                side="long",
                option_type="call",
                strike=call_strike,
                expiration=expiration,
            ),
            Leg(
                option_symbol=_synthetic_option_symbol(underlying, "put", put_strike, expiration),
                side="long",
                option_type="put",
                strike=put_strike,
                expiration=expiration,
            ),
        ]

    def build_iron_condor(
        self,
        underlying: str,
        put_long_strike: float,
        put_short_strike: float,
        call_short_strike: float,
        call_long_strike: float,
        expiration: str,
    ) -> List[Leg]:
        """Short iron condor — sell one call and one put, buy wings for protection.

        Strikes must satisfy
        ``put_long < put_short < spot < call_short < call_long``.
        Non-directional theta play: collects premium if the underlying
        expires between the short strikes; maximum loss is capped at
        (wing_width − credit) per side.
        """
        return [
            Leg(
                option_symbol=_synthetic_option_symbol(
                    underlying, "put", put_long_strike, expiration
                ),
                side="long",
                option_type="put",
                strike=put_long_strike,
                expiration=expiration,
            ),
            Leg(
                option_symbol=_synthetic_option_symbol(
                    underlying, "put", put_short_strike, expiration
                ),
                side="short",
                option_type="put",
                strike=put_short_strike,
                expiration=expiration,
            ),
            Leg(
                option_symbol=_synthetic_option_symbol(
                    underlying, "call", call_short_strike, expiration
                ),
                side="short",
                option_type="call",
                strike=call_short_strike,
                expiration=expiration,
            ),
            Leg(
                option_symbol=_synthetic_option_symbol(
                    underlying, "call", call_long_strike, expiration
                ),
                side="long",
                option_type="call",
                strike=call_long_strike,
                expiration=expiration,
            ),
        ]

    def compute_conviction(
        self, snap: MarketSnapshot, quality_score: float
    ) -> float:
        """Blend the calibrated confidence base with the per-tick quality.

        ``quality_score`` is a bot-authored 0..1 read of how clean the setup
        is right now. We linearly interpolate between the calibrated base
        and the quality reading so a bot with a strong historical hit rate
        can still fire on a marginal setup, and vice versa.
        """
        base = self.confidence_base()
        q = max(0.0, min(1.0, quality_score))
        blended = 0.4 * base + 0.6 * q
        return max(0.0, min(1.0, blended))


def _synthetic_option_symbol(underlying: str, opt: str, strike: float, exp: str) -> str:
    """Build the option_chains ``option_symbol`` for this ATM leg.

    Must match the exact format the ingest layer writes into
    ``option_chains.option_symbol`` — otherwise pricing.spread_price
    returns None every tick and no trade ever opens. That format is
    TradeStation's, not OSI/OCC:

        {option_root} {YYMMDD}{C|P}{strike}

    where ``strike`` is the integer strike as-is when whole
    (``746``), or ``{strike:.2f}`` when fractional (``746.50``).
    ``option_root`` is resolved via ``resolve_option_root`` so
    index chains that trade under a different root (SPX → SPXW)
    line up correctly.
    """
    from src.symbols import resolve_option_root

    root = resolve_option_root(underlying)
    exp_compact = exp.replace("-", "")[-6:]  # YYMMDD
    right = "C" if opt.lower().startswith("c") else "P"
    if float(strike).is_integer():
        strike_str = str(int(strike))
    else:
        strike_str = f"{float(strike):.2f}"
    return f"{root} {exp_compact}{right}{strike_str}"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)
