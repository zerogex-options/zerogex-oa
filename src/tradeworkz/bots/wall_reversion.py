"""Shared engine for the two split wall-reversion bots.

The retired ``PutCallWallBouncer`` faded BOTH walls from one context bot and was
the worst loser in the fleet (PF 0.53 even frictionless — the signal was dead,
not just expensive). Its flaw was that it faded on mere PROXIMITY with a naked
0DTE ATM debit: it entered on the touch, treated every wall the same, and had no
way to tell a rejection from a break.

The two successor bots — :class:`CallWallRejector` (bearish, call wall) and
:class:`PutWallBouncer` (bullish, put wall) — are split so each side can be
tuned / enabled independently, and share this one engine so they stay exact
mirrors. The engine keeps the original's good risk feature (the vol-scaled,
wick-confirmed wall-break stop, via ``wall_ref_side``) and adds the three filters
that separate a *rejection* from a *break*:

1. **Rejection confirmation** — price must TAG the wall and ROLL BACK (a recent
   extreme reached the wall and the current close has retreated from it), not
   just sit near it. Fading a fresh push into the wall is how the original
   entered right before breaks.
2. **Wall-strength gate** — only fade a historically LARGE wall
   (``{call,put}_wall_strength_pctile``); dealers defend big walls, small ones
   break. Best-effort: skipped when the percentile history is absent.
3. **Flow no-pierce** — using the fresh windowed flow: if aggressive order flow
   is still piercing the wall (call-led buying up into the call wall / put-led
   selling down into the put wall), it is breaking — stand down.

Structure is a defined-risk debit vertical toward the mean (max_pain / flip),
which bleeds far less 0DTE theta than the original's single long option.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import TradeSignal

# Floor for the session-recency term (mirrors the retired bot): keeps a
# strong-gamma wall fade tradeable in the final hour — prime pin time — instead
# of decaying to a dead zone where conviction can't clear the threshold.
_SESSION_SCORE_FLOOR = 0.5


def wall_reversion_signal(bot: BaseBot, snap: MarketSnapshot, side: str) -> Optional[TradeSignal]:
    """Return a wall-fade signal for ``side`` ('call' -> bearish, 'put' -> bullish).

    Uses ``bot._skip(reason)`` for every abstention so the backtest miss tally
    names the gate. Shared by :class:`CallWallRejector` and
    :class:`PutWallBouncer`.
    """
    is_call = side == "call"
    wall = snap.call_wall if is_call else snap.put_wall
    if wall is None or snap.spot <= 0:
        return bot._skip("no_wall")

    # Wall defense is a positive-γ phenomenon (dealers long gamma at the wall
    # sell rallies / buy dips).
    if snap.gex_regime() not in {"positive_strong", "positive_weak"}:
        return bot._skip("regime")
    m = snap.minutes_since_open
    if m is None or m < float(bot.params.get("min_minutes_since_open", 30)):
        return bot._skip("window")

    # -- Pressed against the wall (spot just inside it, within proximity).
    prox = float(bot.params.get("wall_proximity_pct", 0.002))
    if is_call:
        dist = (wall - snap.spot) / snap.spot  # spot below the call wall
    else:
        dist = (snap.spot - wall) / snap.spot  # spot above the put wall
    if dist < 0 or dist > prox:
        return bot._skip("not_pressed")

    # -- Rejection confirmation: a recent extreme reached the wall AND the
    # current close has retreated from it (rolled over / bounced).
    closes = [float(c) for c in (snap.recent_closes or []) if c is not None and c > 0]
    lookback = int(bot.params.get("reject_lookback_bars", 5))
    if len(closes) < lookback:
        return bot._skip("no_price_data")
    window = closes[-lookback:]
    reject_margin = float(bot.params.get("wall_reject_margin_pct", 0.0007))
    if is_call:
        hi = max(window)
        tagged = hi >= wall * (1.0 - prox)
        rolled = closes[-1] <= hi * (1.0 - reject_margin)
        if not (tagged and rolled):
            return bot._skip("no_rejection")
    else:
        lo = min(window)
        tagged = lo <= wall * (1.0 + prox)
        bounced = closes[-1] >= lo * (1.0 + reject_margin)
        if not (tagged and bounced):
            return bot._skip("no_rejection")

    # -- Wall-strength gate (best-effort): only fade a historically large wall.
    pctile = snap.call_wall_strength_pctile if is_call else snap.put_wall_strength_pctile
    min_pctile = float(bot.params.get("min_wall_strength_pctile", 50.0))
    if pctile is not None and pctile < min_pctile:
        return bot._skip("weak_wall")

    # -- Flow no-pierce: if aggressive fresh flow is still driving THROUGH the
    # wall, it is breaking, not rejecting. (call wall breaks up on call-led
    # buying; put wall breaks down on put-led selling.)
    rp = snap.flow_recent_premium
    pierce = float(bot.params.get("pierce_premium", 6.0e5))
    if rp is not None:
        if is_call and rp > pierce:
            return bot._skip("flow_piercing")
        if not is_call and rp < -pierce:
            return bot._skip("flow_piercing")

    direction = "bearish" if is_call else "bullish"
    # Don't fade a strongly trending tape (the retired short-into-an-up-day loss).
    if bot._trend_veto(snap, direction):
        return bot._skip("trend_veto")
    if bot._bias_veto(snap, direction):
        return bot._skip("bias_veto")

    # -- Quality: depth of positive gamma, wall size, and a session-recency floor.
    if snap.net_gex_pctile is not None:
        gamma_score = min(1.0, max(0.0, snap.net_gex_pctile / 100.0))
    else:
        gamma_score = min(1.0, max(0.0, (snap.net_gex or 0.0) / 3.0e9))
    wall_score = min(1.0, max(0.0, pctile / 100.0)) if pctile is not None else 0.5
    mins = snap.minutes_since_open or 0.0
    session_score = max(_SESSION_SCORE_FLOOR, 1.0 - min(1.0, max(0.0, (mins - 30) / 300.0)))
    quality = 0.5 * gamma_score + 0.3 * wall_score + 0.2 * session_score
    ml_components = {
        "net_gex": snap.net_gex,
        "wall_side": side,
        "distance_pct": dist,
        "wall_strength_pctile": pctile,
        "flow_recent_premium": rp,
    }
    conviction = bot.compute_conviction(snap, quality, components=ml_components)
    if conviction < bot.confidence_threshold():
        return bot._skip("conviction")

    # -- Defined-risk vertical toward the mean (max_pain / flip), on the correct
    # side. Long ATM, short toward the target.
    dte_target = int(bot.params.get("dte_target", 0))
    expiration = resolve_expiration_iso(snap.et_date, dte_target)
    inc = snap.effective_strike_increment()
    long_strike = snap.round_to_strike(snap.spot)
    target_pct = float(bot.params.get("target_pct", 0.003))
    max_width = int(bot.params.get("max_spread_width", 5))
    if is_call:  # bearish: target below spot
        target = _first_below(snap.spot, [snap.max_pain, snap.gamma_flip])
        target = target if target is not None else snap.spot * (1.0 - target_pct)
        opt_type = "put"
        width = max(1, min(max_width, round(abs(snap.spot - target) / inc)))
        short_strike = long_strike - width * inc
    else:  # bullish: target above spot
        target = _first_above(snap.spot, [snap.max_pain, snap.gamma_flip])
        target = target if target is not None else snap.spot * (1.0 + target_pct)
        opt_type = "call"
        width = max(1, min(max_width, round(abs(target - snap.spot) / inc)))
        short_strike = long_strike + width * inc
    legs = bot.build_vertical(snap.underlying, opt_type, long_strike, short_strike, expiration)
    strat = "BEAR_PUT_DEBIT_SPREAD" if is_call else "BULL_CALL_DEBIT_SPREAD"
    hold = int(bot.params.get("max_hold_minutes", 90))

    # Wall-size-relative sizing input (0..1) for the engine's compute_contracts,
    # and the vol-scaled wall-break stop reads wall_ref_side each tick.
    wall_strength_norm = (pctile / 100.0) if pctile is not None else None

    return TradeSignal(
        bot_id=bot.spec.id,
        underlying=snap.underlying,
        direction=direction,
        strategy_type=strat,
        legs=legs,
        entry_price=0.0,
        conviction=conviction,
        target_price=target,
        stop_price=None,  # dynamic vol-scaled wall-break stop (BaseBot)
        time_stop_at=_utcnow() + timedelta(minutes=hold),
        wall_ref_price=wall,
        wall_ref_side=side,
        rationale=(
            f"{side}-wall {wall} rejection confirmed in {snap.gex_regime()} "
            f"({dist * 100:.2f}% away); fade {direction} toward {target:.2f}"
        ),
        components_at_entry={
            "wall_side": side,
            "wall_price": wall,
            "net_gex": snap.net_gex,
            "distance_pct": dist,
            "wall_strength": wall_strength_norm,
            "wall_strength_pctile": pctile,
            "flow_recent_premium": rp,
            "quality": quality,
        },
    )


def _first_below(spot: float, candidates) -> Optional[float]:
    for c in candidates:
        if c is not None and c < spot:
            return float(c)
    return None


def _first_above(spot: float, candidates) -> Optional[float]:
    for c in candidates:
        if c is not None and c > spot:
            return float(c)
    return None
