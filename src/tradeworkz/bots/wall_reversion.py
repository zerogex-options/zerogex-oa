"""Shared, execution-PARAMETERIZED engine for the wall-reversion bots.

The retired ``PutCallWallBouncer`` faded both walls from one context bot and was
the worst loser in the fleet (PF 0.53 frictionless). The first split successors
(:class:`CallWallRejector` / :class:`PutWallBouncer`) added rejection /
wall-strength / flow-no-pierce filters but still lost as a 0DTE DEBIT vertical —
because a debit needs price to *move* to a target, while "the wall holds" is a
*boundary*, not a target.

Rather than one-shot another structure, the engine is now parameterized so the
SAME signal can be run through many EXECUTIONS and swept for a robust
configuration (see ``src/tools/tw_execution_sweep.py``). The signal
(regime + rejection + wall-strength + flow-no-pierce) is fixed; ``params``
select the structure and its exits:

* ``structure``:
    - ``debit_vertical`` (default) — long ATM, short toward the target.
    - ``single_debit``   — a single long option (the retired bot's structure).
    - ``credit_spread``  — SELL a spread just beyond the wall (bear-call beyond
      the call wall / bull-put beyond the put wall). Profits if the wall simply
      isn't broken — the boundary-matching structure. Uses a credit-aware exit
      (decay profit-take + wall-break stop) via :func:`wall_credit_exit`.
* ``target_mode`` ∈ {max_pain, flip, vwap, fixed} — the debit target.
* ``spread_width`` / ``wing_width`` / ``dte_target`` / ``max_hold_minutes`` /
  ``stop_pct`` / ``break_buffer_pct`` / ``profit_take_frac``.

Every abstention calls ``bot._skip(reason)`` so the backtest miss tally names
the gate.
"""

from __future__ import annotations

from datetime import timedelta
from typing import List, Optional, Tuple

from src.tradeworkz.bots.base import BaseBot, _utcnow, resolve_expiration_iso
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import ExitDecision, Leg, OpenPosition, TradeSignal

_SESSION_SCORE_FLOOR = 0.5
_CREDIT_STRATS = {"BEAR_CALL_CREDIT_SPREAD", "BULL_PUT_CREDIT_SPREAD"}


def wall_reversion_signal(bot: BaseBot, snap: MarketSnapshot, side: str) -> Optional[TradeSignal]:
    """Return a wall-fade signal for ``side`` ('call' -> bearish, 'put' -> bullish).

    The signal is fixed; the STRUCTURE is chosen by ``bot.params['structure']``.
    """
    is_call = side == "call"
    wall = snap.call_wall if is_call else snap.put_wall
    if wall is None or snap.spot <= 0:
        return bot._skip("no_wall")

    if snap.gex_regime() not in {"positive_strong", "positive_weak"}:
        return bot._skip("regime")
    m = snap.minutes_since_open
    if m is None or m < float(bot.params.get("min_minutes_since_open", 30)):
        return bot._skip("window")

    prox = float(bot.params.get("wall_proximity_pct", 0.002))
    dist = (wall - snap.spot) / snap.spot if is_call else (snap.spot - wall) / snap.spot
    if dist < 0 or dist > prox:
        return bot._skip("not_pressed")

    # Rejection confirmation: a recent extreme reached the wall AND the current
    # close has retreated from it.
    closes = [float(c) for c in (snap.recent_closes or []) if c is not None and c > 0]
    lookback = int(bot.params.get("reject_lookback_bars", 5))
    if len(closes) < lookback:
        return bot._skip("no_price_data")
    window = closes[-lookback:]
    reject_margin = float(bot.params.get("wall_reject_margin_pct", 0.0007))
    if is_call:
        hi = max(window)
        if not (hi >= wall * (1.0 - prox) and closes[-1] <= hi * (1.0 - reject_margin)):
            return bot._skip("no_rejection")
    else:
        lo = min(window)
        if not (lo <= wall * (1.0 + prox) and closes[-1] >= lo * (1.0 + reject_margin)):
            return bot._skip("no_rejection")

    pctile = snap.call_wall_strength_pctile if is_call else snap.put_wall_strength_pctile
    min_pctile = float(bot.params.get("min_wall_strength_pctile", 50.0))
    if pctile is not None and pctile < min_pctile:
        return bot._skip("weak_wall")

    rp = snap.flow_recent_premium
    pierce = float(bot.params.get("pierce_premium", 6.0e5))
    if rp is not None:
        if is_call and rp > pierce:
            return bot._skip("flow_piercing")
        if not is_call and rp < -pierce:
            return bot._skip("flow_piercing")

    direction = "bearish" if is_call else "bullish"
    if bot._trend_veto(snap, direction):
        return bot._skip("trend_veto")
    if bot._bias_veto(snap, direction):
        return bot._skip("bias_veto")

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

    legs, strat, target, stop = _build_wall_structure(bot, snap, is_call, wall)
    if legs is None:
        return bot._skip("bad_structure")

    hold = int(bot.params.get("max_hold_minutes", 90))
    is_credit = strat in _CREDIT_STRATS
    # The dynamic vol-scaled wall-break stop is only meaningful for the debit
    # fades; the credit structure carries its own spot-level break stop.
    wall_ref_side = None if is_credit else side
    wall_ref_price = None if is_credit else wall
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
        stop_price=stop,
        time_stop_at=_utcnow() + timedelta(minutes=hold),
        wall_ref_price=wall_ref_price,
        wall_ref_side=wall_ref_side,
        rationale=(
            f"{side}-wall {wall} rejection in {snap.gex_regime()} "
            f"({dist * 100:.2f}% away); {strat} fade {direction}"
        ),
        components_at_entry={
            "wall_side": side,
            "wall_price": wall,
            "structure": strat,
            "net_gex": snap.net_gex,
            "distance_pct": dist,
            "wall_strength": wall_strength_norm,
            "wall_strength_pctile": pctile,
            "flow_recent_premium": rp,
            "quality": quality,
        },
    )


def _build_wall_structure(
    bot: BaseBot, snap: MarketSnapshot, is_call: bool, wall: float
) -> Tuple[Optional[List[Leg]], str, Optional[float], Optional[float]]:
    """Build (legs, strategy_type, target_price, stop_price) for the chosen structure."""
    structure = str(bot.params.get("structure", "debit_vertical"))
    dte = int(bot.params.get("dte_target", 0))
    exp = resolve_expiration_iso(snap.et_date, dte)
    inc = snap.effective_strike_increment()
    long_atm = snap.round_to_strike(snap.spot)
    u = snap.underlying

    if structure in ("debit_vertical", "single_debit"):
        opt_type = "put" if is_call else "call"
        target = _resolve_target(bot, snap, is_call)
        if structure == "single_debit":
            legs = bot.build_atm_debit(u, opt_type, long_atm, exp, 0.0)
            strat = "BUY_PUT_DEBIT" if is_call else "BUY_CALL_DEBIT"
        else:
            max_width = int(bot.params.get("max_spread_width", 5))
            width = max(1, min(max_width, round(abs(snap.spot - target) / inc)))
            short = long_atm - width * inc if is_call else long_atm + width * inc
            legs = bot.build_vertical(u, opt_type, long_atm, short, exp)
            strat = "BEAR_PUT_DEBIT_SPREAD" if is_call else "BULL_CALL_DEBIT_SPREAD"
        # Debit fades stop via the base vol-scaled wall-break (wall_ref_side); no
        # static spot stop here.
        return legs, strat, target, None

    if structure == "credit_spread":
        wing = max(1, int(bot.params.get("wing_width", 3)))
        buf = float(bot.params.get("break_buffer_pct", 0.001))
        if is_call:  # bear call credit spread: short near the wall, long wing above
            short_k = snap.round_to_strike(wall)
            if short_k <= long_atm:
                short_k = long_atm + inc
            long_k = short_k + wing * inc
            legs = bot.build_vertical(u, "call", long_k, short_k, exp)  # long far, short near
            strat = "BEAR_CALL_CREDIT_SPREAD"
            stop = short_k * (1.0 + buf)  # spot >= this => wall broke up (base bearish stop)
        else:  # bull put credit spread: short near the wall, long wing below
            short_k = snap.round_to_strike(wall)
            if short_k >= long_atm:
                short_k = long_atm - inc
            long_k = short_k - wing * inc
            legs = bot.build_vertical(u, "put", long_k, short_k, exp)  # long far, short near
            strat = "BULL_PUT_CREDIT_SPREAD"
            stop = short_k * (1.0 - buf)  # spot <= this => wall broke down (base bullish stop)
        return legs, strat, None, stop

    return None, "UNKNOWN", None, None


def _resolve_target(bot: BaseBot, snap: MarketSnapshot, is_call: bool) -> float:
    """Debit target on the profit side (below spot for a call fade, above for put)."""
    mode = str(bot.params.get("target_mode", "max_pain"))
    target_pct = float(bot.params.get("target_pct", 0.003))
    by_mode = {"max_pain": snap.max_pain, "flip": snap.gamma_flip, "vwap": snap.vwap}
    ordered = [mode, "max_pain", "flip"]  # primary then fallbacks
    for name in ordered:
        v = by_mode.get(name)
        if v is None:
            continue
        if is_call and v < snap.spot:
            return float(v)
        if not is_call and v > snap.spot:
            return float(v)
    return snap.spot * (1.0 - target_pct) if is_call else snap.spot * (1.0 + target_pct)


def wall_credit_exit(
    bot: BaseBot, snap: MarketSnapshot, position: OpenPosition
) -> Optional[ExitDecision]:
    """Credit-structure exit: take profit on decay, else fall through to base.

    For a credit spread both the entry and the mark are NEGATIVE (the entry is
    ``-credit`` received; the mark is ``-(cost to close)``), and the per-share
    profit is ``mark - entry`` — it rises from ~0 at entry toward ``|credit|`` as
    the spread decays to zero. We book profit once that captured profit reaches
    ``profit_take_frac`` of the credit. The wall-break stop and the time-stop are
    the base's spot-level ``stop_price`` / ``time_stop_at`` set at entry, so this
    only adds the decay take. Returns ``None`` for a non-credit position (the bot
    then uses the base exit).
    """
    if str(position.strategy_type) not in _CREDIT_STRATS:
        return None
    entry = position.entry_price
    mark = position.current_price
    if entry is None or entry >= 0 or mark is None:
        return None
    now = _utcnow()
    in_min_hold = position.min_hold_until is not None and now < position.min_hold_until
    if in_min_hold:
        return None
    take_frac = float(bot.params.get("profit_take_frac", 0.5))
    # captured profit (mark - entry) >= take_frac * credit_received => book it.
    if (mark - entry) >= take_frac * abs(entry):
        return ExitDecision(should_close=True, reason="credit_take")
    return None
