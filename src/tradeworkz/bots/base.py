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
import math
import statistics
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import BotSpec, ExitDecision, Leg, OpenPosition, TradeSignal

logger = logging.getLogger(__name__)

# Trading minutes in a US equity RTH year — 252 sessions × 390 min. Used to
# convert an annualized VIX into a per-1-minute sigma when realized recent
# closes aren't available to estimate short-horizon vol directly.
_RTH_MINUTES_PER_YEAR = 252.0 * 390.0


def _short_horizon_sigma_pct(snap: MarketSnapshot) -> Optional[float]:
    """Estimate the underlying's ~1-minute return sigma as a fraction.

    Prefers realized vol — the population stdev of the most recent up-to-15
    one-minute simple returns from ``snap.recent_closes`` (the snapshot
    stores 1-minute closes oldest→newest). Falls back to a VIX-implied
    per-minute sigma (annualized ``vix/100`` de-annualized over RTH
    minutes) when there aren't enough closes, and to ``None`` when neither
    source is usable — callers then apply their configured floor.

    Deliberately estimates the *1-minute* sigma, not the hold-horizon move:
    the wall-break buffer is a small multiple of it, and confirmation over
    N one-minute closes supplies the time dimension.
    """
    closes = [float(c) for c in (snap.recent_closes or []) if c is not None and c > 0]
    if len(closes) >= 6:
        rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))]
        rets = rets[-15:]
        if len(rets) >= 2:
            sigma = statistics.pstdev(rets)
            if sigma > 0.0:
                return sigma
    vix = snap.vix
    if vix is not None and vix > 0.0:
        return (float(vix) / 100.0) / math.sqrt(_RTH_MINUTES_PER_YEAR)
    return None


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


def compute_ladder_levels(
    entry_spot: Optional[float],
    target_price: Optional[float],
    direction: str,
    t1_frac: float,
    s2_frac: float,
    t2_frac: float,
    max_move_pct: float = 0.0,
) -> Optional[Dict[str, float]]:
    """Freeze the scale-out ladder's spot levels at entry, or ``None``.

    ``R = effective_target - entry_spot`` is the distance the ladder harvests
    over. The levels are fixed fractions of R:

        T1 (take 50%)      = entry_spot + t1_frac * R   (default 0.90·R)
        S2 (runner floor)  = entry_spot + s2_frac * R   (default 0.75·R)
        T2 (take 25% more) = entry_spot + t2_frac * R    (default 1.5·R)

    ``effective_target`` is the bot's structural ``target_price`` **capped to a
    reachable envelope** ``entry_spot × (1 ± max_move_pct)`` when
    ``max_move_pct > 0``. Some bots (e.g. VixRegimeBreakout) target a far gamma
    wall that a 0DTE will not reach intraday; anchoring T1 to it (T1 ≈ 0.90·R of
    an 18-point move) leaves the ladder inert and a big premium winner
    unprotected. Capping the target to, say, 0.8% keeps T1/S2/T2 reachable so
    the ladder actually harvests. The cap only ever pulls the target *toward*
    entry, so a realistic target already inside the envelope is unchanged, and
    the profitable sign is preserved. ``max_move_pct <= 0`` disables the cap.

    Returns ``None`` — "don't arm the ladder, use the single-target exit" —
    unless the position is directional, has both an entry spot and a target, and
    the RAW target has the *profitable* sign for the direction (bullish target
    above entry, bearish below). The sign guard runs before the cap: a
    misconfigured signal whose target sits on the wrong side of entry falls back
    to the plain exit instead.
    """
    if entry_spot is None or target_price is None:
        return None
    if direction not in ("bullish", "bearish"):
        return None
    r = target_price - entry_spot
    if direction == "bullish" and r <= 0:
        return None
    if direction == "bearish" and r >= 0:
        return None
    if max_move_pct and max_move_pct > 0 and entry_spot > 0:
        if direction == "bullish":
            effective_target = min(target_price, entry_spot * (1.0 + max_move_pct))
        else:
            effective_target = max(target_price, entry_spot * (1.0 - max_move_pct))
        r = effective_target - entry_spot
    return {
        "t1_trigger_price": entry_spot + t1_frac * r,
        "s2_stop_price": entry_spot + s2_frac * r,
        "target2_price": entry_spot + t2_frac * r,
    }


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

    # -- Scale-out ladder knobs -----------------------------------------
    # Each resolves a per-bot ``params[...]`` override over the fleet-wide
    # config default, mirroring :meth:`_max_premium_loss_pct`. See
    # docs/design/tradeworkz-exit-strategy.md.

    def _resolve_ladder_frac(
        self, param_key: str, cfg_default: float, lo: float, hi: float
    ) -> float:
        val = self.params.get(param_key)
        if val is None:
            val = cfg_default
        try:
            return max(lo, min(hi, float(val)))
        except (TypeError, ValueError):
            return max(lo, min(hi, float(cfg_default)))

    def _scale_out_enabled(self) -> bool:
        from src.tradeworkz import config as tw_config

        override = self.params.get("scale_out_enabled")
        if override is not None:
            return bool(override)
        return bool(tw_config.SCALE_OUT_ENABLED)

    def _min_scale_contracts(self) -> int:
        from src.tradeworkz import config as tw_config

        override = self.params.get("min_scale_contracts")
        try:
            v = int(override) if override is not None else int(tw_config.MIN_SCALE_CONTRACTS)
        except (TypeError, ValueError):
            v = int(tw_config.MIN_SCALE_CONTRACTS)
        return max(1, v)

    def _t1_trigger_fraction(self) -> float:
        from src.tradeworkz import config as tw_config

        return self._resolve_ladder_frac("t1_trigger_frac", tw_config.T1_TRIGGER_FRACTION, 0.0, 1.0)

    def _s2_stop_fraction(self) -> float:
        from src.tradeworkz import config as tw_config

        return self._resolve_ladder_frac("s2_stop_frac", tw_config.S2_STOP_FRACTION, 0.0, 1.0)

    def _t2_target_fraction(self) -> float:
        from src.tradeworkz import config as tw_config

        return self._resolve_ladder_frac("t2_target_frac", tw_config.T2_TARGET_FRACTION, 0.0, 10.0)

    def _t1_take_fraction(self) -> float:
        from src.tradeworkz import config as tw_config

        return self._resolve_ladder_frac("t1_take_frac", tw_config.T1_TAKE_FRACTION, 0.0, 1.0)

    def _t2_take_fraction(self) -> float:
        from src.tradeworkz import config as tw_config

        return self._resolve_ladder_frac("t2_take_frac", tw_config.T2_TAKE_FRACTION, 0.0, 1.0)

    def _runner_trail_giveback_pct(self) -> float:
        from src.tradeworkz import config as tw_config

        return self._resolve_ladder_frac(
            "runner_trail_giveback_pct", tw_config.RUNNER_TRAIL_GIVEBACK_PCT, 0.0, 1.0
        )

    def _ladder_max_move_pct(self) -> float:
        from src.tradeworkz import config as tw_config

        return self._resolve_ladder_frac(
            "ladder_max_move_pct", tw_config.LADDER_MAX_MOVE_PCT, 0.0, 1.0
        )

    def scale_config(self) -> Dict[str, Any]:
        """Resolved scale-out knobs, for ``open_position`` to freeze the ladder.

        The take fractions and the give-back are NOT frozen here — they are read
        live from the bot at exit time — so only the geometry inputs (enable
        flag, min-contract floor, the T1/S2/T2 fractions, and the target cap)
        are returned.
        """
        return {
            "enabled": self._scale_out_enabled(),
            "min_contracts": self._min_scale_contracts(),
            "t1_frac": self._t1_trigger_fraction(),
            "s2_frac": self._s2_stop_fraction(),
            "t2_frac": self._t2_target_fraction(),
            "max_move_pct": self._ladder_max_move_pct(),
        }

    def _trend_veto(self, snap: MarketSnapshot, direction: str) -> bool:
        """True when a strong recent trend runs AGAINST a mean-reversion entry.

        Reversion bots fade extension; on a strongly trending tape that means
        fighting the trend (the live incident: the fleet shorting an up-day and
        the puts getting run over). A bearish fade is vetoed when the last
        ``trend_veto_lookback_bars`` one-minute closes are up by at least
        ``trend_veto_pct``; a bullish fade when they are down that much.
        Momentum / breakout bots do NOT call this — they want the trend. Per-bot
        overridable; a ``trend_veto_pct`` of 0 disables it. See
        :data:`TREND_VETO_PCT`.
        """
        from src.tradeworkz import config as tw_config

        pct = self._resolve_ladder_frac("trend_veto_pct", tw_config.TREND_VETO_PCT, 0.0, 1.0)
        if pct <= 0.0 or direction not in ("bullish", "bearish"):
            return False
        try:
            n = int(self.params.get("trend_veto_lookback_bars", tw_config.TREND_VETO_LOOKBACK_BARS))
        except (TypeError, ValueError):
            n = int(tw_config.TREND_VETO_LOOKBACK_BARS)
        n = max(2, n)
        closes = [float(c) for c in (snap.recent_closes or []) if c is not None and c > 0]
        if len(closes) < n:
            return False
        window = closes[-n:]
        if window[0] <= 0:
            return False
        move = (window[-1] - window[0]) / window[0]
        if direction == "bearish" and move >= pct:
            return True  # strong up-move — don't fade it short
        if direction == "bullish" and move <= -pct:
            return True  # strong down-move — don't fade it long
        return False

    # -- Subclass overrides ---------------------------------------------

    def open_criteria(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        """Return a signal if the bot wants to open (or add). Default: no-op."""
        return None

    def exit_criteria(self, snap: MarketSnapshot, position: OpenPosition) -> ExitDecision:
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
        from src.tradeworkz import config as tw_config

        pct = self._max_premium_loss_pct()
        # Grace window: a freshly opened position is first marked at the close
        # side (bid − slippage), so the entry-tick bid/ask + slippage gap can
        # look like a large loss that is not a real adverse move. Suppress the
        # premium stop until the grace elapses so it fires on moves, not the
        # spread. See PREMIUM_STOP_GRACE_SECONDS.
        grace = int(tw_config.PREMIUM_STOP_GRACE_SECONDS)
        in_premium_grace = (
            grace > 0
            and position.opened_at is not None
            and (now - position.opened_at).total_seconds() < grace
        )
        if (
            pct > 0
            and not in_premium_grace
            and position.entry_price
            and position.entry_price > 0
            and position.current_price is not None
        ):
            loss_pct = 1.0 - (position.current_price / position.entry_price)
            if loss_pct >= pct:
                return ExitDecision(should_close=True, reason="premium_stop")

        # Scale-out ladder. When armed (geometry frozen at entry), this OWNS the
        # exit — profit-taking cuts at T1/T2 plus the runner stops — so the
        # single-target branch below runs only for unarmed positions. The
        # premium stop above still fires first for either, as the hard floor.
        ladder = self._scale_exit_decision(snap, position, in_min_hold, now)
        if ladder is not None:
            return ladder

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

    def _scale_exit_decision(
        self,
        snap: MarketSnapshot,
        position: OpenPosition,
        in_min_hold: bool,
        now: datetime,
    ) -> Optional[ExitDecision]:
        """Scale-out ladder decision, or ``None`` to fall back to single-target.

        Returns ``None`` when the ladder is not armed — an unarmed position then
        uses the default target / stop / time / wall logic in
        :meth:`exit_criteria`. When armed (all three geometry levels were frozen
        at open), this method OWNS the exit: T1 / T2 profit-taking cuts, the
        runner stop (S2 spot floor OR the premium give-back trail), the Stage-0
        structural stop, the time-stop, and wall drift. The premium
        damage-control stop is evaluated by the caller BEFORE this, so it remains
        the hard floor in every stage.

        Stages: 0 = full position (target is T1), 1 = post-T1 runner (target is
        T2, protected by S2 + trail), 2 = post-T2 final runner (rides under the
        same S2 + trail to the time-stop). See
        docs/design/tradeworkz-exit-strategy.md.
        """
        if not self._scale_out_enabled():
            return None
        # Armed iff the geometry was frozen at entry (open_position writes all
        # three only when the ladder was armable). NULLs => not armed.
        if (
            position.t1_trigger_price is None
            or position.s2_stop_price is None
            or position.target2_price is None
        ):
            return None
        if position.direction not in ("bullish", "bearish"):
            return None

        bull = position.direction == "bullish"
        spot = snap.spot
        stage = int(position.scale_stage or 0)

        # -- Runner stops (Stage >= 1): whichever of the premium give-back trail
        # and the S2 spot floor trips first closes the remaining runner. Only
        # reachable after T1, which is itself min_hold-gated, so the runner is
        # always past the patience window here.
        if stage >= 1:
            giveback = self._runner_trail_giveback_pct()
            hwm = position.high_water_mark
            if (
                giveback > 0
                and hwm is not None
                and hwm > 0
                and position.current_price is not None
                and position.current_price <= (1.0 - giveback) * hwm
            ):
                return ExitDecision(should_close=True, reason="runner_trail_stop")
            s2 = position.s2_stop_price
            if (bull and spot <= s2) or (not bull and spot >= s2):
                return ExitDecision(should_close=True, reason="runner_s2_stop")

        # -- Profit-taking cuts (price-level; min_hold-gated like today's
        # target). T1 in Stage 0, T2 in Stage 1. cut_fraction is applied to the
        # right base in reconciler.partial_close_position.
        if not in_min_hold:
            if stage == 0:
                t1 = position.t1_trigger_price
                if (bull and spot >= t1) or (not bull and spot <= t1):
                    return ExitDecision(
                        should_close=False,
                        should_cut=True,
                        cut_fraction=self._t1_take_fraction(),
                        reason="scale_t1",
                    )
            elif stage == 1:
                t2 = position.target2_price
                if (bull and spot >= t2) or (not bull and spot <= t2):
                    return ExitDecision(
                        should_close=False,
                        should_cut=True,
                        cut_fraction=self._t2_take_fraction(),
                        reason="scale_t2",
                    )

        # -- Stage-0 structural stop (S1 spot stop), min_hold-gated. Superseded
        # by S2 + trail once the ladder passes T1, so Stage 0 only.
        if not in_min_hold and stage == 0 and position.stop_price is not None:
            if (bull and spot <= position.stop_price) or (
                not bull and spot >= position.stop_price
            ):
                return ExitDecision(should_close=True, reason="stop")

        # Mirror today's patience window: nothing else fires during min_hold.
        if in_min_hold:
            return ExitDecision(should_close=False)

        # -- Time-stop (all stages) and wall drift (all stages). --
        if position.time_stop_at and now >= position.time_stop_at:
            return ExitDecision(should_close=True, reason="time_stop")
        drift = self._wall_stop_signal(snap, position)
        if drift is not None:
            return drift
        return ExitDecision(should_close=False)

    # -- Shared helpers used by concrete bots ---------------------------

    def _wall_break_buffer_pct(self, snap: MarketSnapshot, wall_strength: Optional[float]) -> float:
        """Volatility-scaled distance past a wall that counts as a break.

        A wall fade wants a stop that ``allows it to play out`` — walls get
        pierced by noise and revert — without letting a genuine break run.
        A flat percentage can't do both: 0.3% is a wick in a 30-VIX tape
        and a real break in a 12-VIX one. So the buffer scales with a
        short-horizon sigma estimate (:func:`_short_horizon_sigma_pct`):

            buffer = clamp(vol_mult × sigma_1min, min_pct, max_pct)

        and is widened for a historically large wall (``wall_strength`` in
        0..1, from commit 1's sizing input) so a massive wall — which the
        Bouncer also sizes UP into — gets correspondingly more room:

            buffer ×= 1 + strength_widen × wall_strength   (pre-clamp)

        Defaults are chosen so a baseline ~16-VIX tape lands near the old
        flat 0.3% (``min_pct`` floor), so calm-market behavior is unchanged
        and the widening only ever *loosens* into volatility / large walls,
        never tighter than before. ``max_pct`` caps the loosening. Every
        term is a per-bot param so this is tunable / backtestable without a
        code change; the option-premium damage-control stop
        (:data:`MAX_PREMIUM_LOSS_PCT`) remains the independent hard dollar
        cap for a fast blow-through regardless of how wide this gets.
        """
        min_pct = max(0.0, float(self.params.get("wall_break_min_pct", 0.003)))
        max_pct = max(min_pct, float(self.params.get("wall_break_max_pct", 0.012)))
        vol_mult = max(0.0, float(self.params.get("wall_break_vol_mult", 6.0)))
        strength_widen = max(0.0, float(self.params.get("wall_break_strength_widen", 0.5)))

        sigma = _short_horizon_sigma_pct(snap)
        buf = min_pct if sigma is None else vol_mult * sigma
        if wall_strength is not None:
            buf *= 1.0 + strength_widen * max(0.0, min(1.0, wall_strength))
        return max(min_pct, min(max_pct, buf))

    def _beyond_confirmed(self, snap: MarketSnapshot, level: float, above: bool) -> bool:
        """True when spot has broken past ``level`` and the break is confirmed.

        Confirmation filters a single-print wick through the level: the
        current spot must be beyond it AND the most recent
        ``wall_break_confirm_bars`` one-minute closes must all be beyond it
        too. With ``confirm_bars <= 1`` (or too little history to confirm)
        this degrades to the prior single-tick check, so it can only ever
        make the stop *more* patient, never less.
        """
        current_beyond = snap.spot > level if above else snap.spot < level
        if not current_beyond:
            return False
        n = int(self.params.get("wall_break_confirm_bars", 2))
        closes = [float(c) for c in (snap.recent_closes or []) if c is not None]
        if n <= 1 or len(closes) < n:
            return True
        recent = closes[-n:]
        return all((c > level) if above else (c < level) for c in recent)

    def _wall_stop_signal(
        self, snap: MarketSnapshot, position: OpenPosition
    ) -> Optional[ExitDecision]:
        """Cut a wall fade when the wall it references genuinely fails.

        Two failure modes, both keyed off the *side* of the wall (the
        barrier), which is what physically defines a fade — the option
        ``direction`` is redundant with it and, worse, the prior version
        keyed off direction and inverted the test: for the Bouncer's
        call-wall fade (bearish) and put-wall bounce (bullish) it fired
        ``wall_break`` when spot moved TOWARD the target, silently capping
        winners at wall∓drift and mislabeling them, while the migration
        branch (gated on the opposite direction) never fired at all. Only
        the Bouncer sets ``wall_ref_side`` (call→fade-from-below,
        put→fade-from-above), so keying off side is correct for every
        current producer. The iron condor sets ``wall_ref_price`` but no
        side, so it still no-ops here (its own short-strike stop governs).

        * **wall_break** — spot blows THROUGH the wall away from the fade's
          target (call wall = resistance → a push ABOVE it; put wall =
          support → a break BELOW it), by the volatility-scaled
          :meth:`_wall_break_buffer_pct`, and the break is
          :meth:`_beyond_confirmed`.
        * **wall_shift** — the referenced wall itself migrates adversely
          (call wall rolling UP, put wall sliding DOWN) beyond
          ``wall_shift_pct``: the structure you faded has moved, so the
          thesis is stale. Structural, so no wick-confirmation.

        Returns ``None`` when neither fires.
        """
        ref = position.wall_ref_price
        side = (position.wall_ref_side or "").lower()
        if ref is None or side not in {"call", "put"}:
            return None

        ws = (position.components_at_entry or {}).get("wall_strength")
        wall_strength = float(ws) if isinstance(ws, (int, float)) else None
        buf = self._wall_break_buffer_pct(snap, wall_strength)
        shift_pct = float(self.params.get("wall_shift_pct", 0.003))

        if side == "call":
            if self._beyond_confirmed(snap, ref * (1.0 + buf), above=True):
                return ExitDecision(should_close=True, reason="wall_break")
            if snap.call_wall is not None and snap.call_wall > ref * (1.0 + shift_pct):
                return ExitDecision(should_close=True, reason="wall_shift")
        else:  # put
            if self._beyond_confirmed(snap, ref * (1.0 - buf), above=False):
                return ExitDecision(should_close=True, reason="wall_break")
            if snap.put_wall is not None and snap.put_wall < ref * (1.0 - shift_pct):
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
        self,
        snap: MarketSnapshot,
        quality_score: float,
        components: Optional[Dict[str, Any]] = None,
    ) -> float:
        """Blend the calibrated confidence base with the per-tick quality,
        then nudge toward the online classifier's P(win) when it is trained.

        ``quality_score`` is a bot-authored 0..1 read of how clean the setup
        is right now. We linearly interpolate between the calibrated base
        and the quality reading so a bot with a strong historical hit rate
        can still fire on a marginal setup, and vice versa.

        ``components`` — when a bot passes the same feature blob it will log
        at entry — activates the ML overlay: the per-bot online
        logistic-regression classifier (``tw_ml_state.weights``, trained one
        SGD step per closed trade) scores this setup's P(win), and we pull
        the blended conviction a bounded fraction toward it. This is the
        step that makes the learned model actually *drive* trades rather
        than only being trained and shelved. It is deliberately conservative:

        * a no-op unless the bot passes ``components`` AND the model has seen
          at least ``ml_min_samples`` trades AND has non-zero weights — so a
          fresh or lightly-traded bot behaves exactly as before;
        * a bounded convex nudge (``ml_conviction_weight``, default 0.25) —
          the classifier can lean the gate, never dominate it, and the
          result stays in [0, 1];
        * the provisional blend is fed in as the ``conviction`` feature (the
          real entry conviction is circular here); the pull is small enough
          that the one-shot approximation is stable.
        """
        base = self.confidence_base()
        q = max(0.0, min(1.0, quality_score))
        blended = 0.4 * base + 0.6 * q
        p_win = self._ml_win_prob(components, provisional=blended)
        if p_win is not None:
            w = max(0.0, min(1.0, float(self.params.get("ml_conviction_weight", 0.25))))
            blended = (1.0 - w) * blended + w * p_win
        return max(0.0, min(1.0, blended))

    def _ml_win_prob(
        self, components: Optional[Dict[str, Any]], provisional: float
    ) -> Optional[float]:
        """Classifier P(win) for this setup, or ``None`` to skip the overlay.

        Gated: needs a feature blob, at least ``ml_min_samples`` trades of
        history, and a trained (non-zero) weight vector — otherwise the
        blend is left untouched. The ``conviction`` feature defaults to the
        provisional blend so training (which logs the final conviction) and
        inference use a consistent value.
        """
        if components is None:
            return None
        try:
            n_samples = int(self.ml_state.get("n_samples") or 0)
        except (TypeError, ValueError):
            n_samples = 0
        min_samples = int(self.params.get("ml_min_samples", 20))
        if n_samples < min_samples:
            return None
        feats = dict(components)
        feats.setdefault("conviction", provisional)
        from src.tradeworkz import ml as ml_mod

        return ml_mod.predict_win_prob(self.ml_state.get("weights"), feats)


def _synthetic_option_symbol(underlying: str, opt: str, strike: float, exp: str) -> str:
    """Build the option_chains ``option_symbol`` for this ATM leg.

    Must match the exact format the ingest layer writes into
    ``option_chains.option_symbol`` — otherwise pricing.spread_price
    returns None every tick and no trade ever opens. That format is
    TradeStation's, not OSI/OCC:

        {option_root} {YYMMDD}{C|P}{strike}

    where ``strike`` is the integer strike as-is when whole
    (``746``), or ``{strike:.2f}`` when fractional (``746.50``).

    The root is resolved the SAME way the ingest layer resolves it:
    underlying → ``resolve_symbol`` (SYMBOL_ALIASES) → ``resolve_option_root``
    (OPTION_ROOT_ALIASES). This matters for cash indices whose tradeable
    chain lists under a different TS symbol and root — e.g. SPX's 0DTE chain
    is ``$SPXW.X`` → root ``SPXW``, so the quotable contract is
    ``SPXW 260714P7530``, not ``SPX 260714P7530``. Building the bare-root
    form would miss the ingested quote and no SPX trade would ever fill.
    ETFs (no alias) pass through unchanged, so SPY/QQQ/IWM are unaffected.
    """
    from src.symbols import resolve_option_root, resolve_symbol

    root = resolve_option_root(resolve_symbol(underlying))
    exp_compact = exp.replace("-", "")[-6:]  # YYMMDD
    right = "C" if opt.lower().startswith("c") else "P"
    if float(strike).is_integer():
        strike_str = str(int(strike))
    else:
        strike_str = f"{float(strike):.2f}"
    return f"{root} {exp_compact}{right}{strike_str}"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)
