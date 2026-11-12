"""Express any bot's trade in any capped-risk option structure.

A bot decides WHEN and WHICH WAY: ``open_criteria`` returns a signal with a
direction, an expiration, spot levels and a conviction. HOW to hold that view
is a separate choice — a long call, a debit spread, a credit spread, a
butterfly — and the same entry can lose in one and win in another: a 0DTE long
option pays theta and the bid/ask on every trade, while a credit spread on the
same signal is paid to be right about direction OR time.
``params['structure_override']`` re-expresses any bot's signal without
touching the bot, so the search for an edge can include the search for the
right structure. Set it in the bot's catalog ``bot_params`` (provision
re-syncs params on every API start). It is not ``params['structure']``: two
bots (wall_reversion, hedging_demand_close_momentum) already read that key to
pick between their own native structures, with their own strike logic.

Families — a structure must fit the signal it re-expresses:

* **Directional** (bullish / bearish signals): ``long_single``,
  ``debit_vertical``, ``credit_vertical``, ``butterfly`` (centered on the
  signal's target).
* **Short volatility** (neutral signals whose native trade sells premium):
  ``iron_condor``, ``iron_fly``, ``long_butterfly``.
* **Long volatility** (neutral signals whose native trade buys premium):
  ``long_straddle``, ``long_strangle``.

A structure from the wrong family (a bullish signal as an iron condor) is
refused, not guessed at: the signal is dropped and the miss is counted.

Geometry, in strike increments of the snapshot's listed grid, around the
at-the-money strike: ``structure_offset_increments`` moves the near strike
away from the money (OTM in the trade's direction for debits, against it for
credits; default 0, except a condor's short strikes and a strangle's legs,
which default to one width out), and ``structure_width_increments`` is the
spread / wing width (default 2). The expiration, spot target and stop, time
stop and conviction are the bot's own.

Every structure here has a capped worst case — each short option is covered by
a long one of the same right (:func:`src.tradeworkz.sizing.is_capped_risk`).
Naked shorts and short straddles / strangles are absent on purpose: sizing
divides the heat budget by the structure's max loss, and theirs is unbounded.
A credit structure exits on the bot's spot levels and time stop; its premium
take / stop are the opt-in ``credit_take_frac`` / ``credit_stop_mult`` params
(see ``BaseBot._credit_exit_decision``).
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

from src.tradeworkz.models import Leg, TradeSignal

if TYPE_CHECKING:  # pragma: no cover
    from src.tradeworkz.bots.base import BaseBot
    from src.tradeworkz.context import MarketSnapshot

logger = logging.getLogger(__name__)

DIRECTIONAL = ("long_single", "debit_vertical", "credit_vertical", "butterfly")
SHORT_VOL = ("iron_condor", "iron_fly", "long_butterfly")
LONG_VOL = ("long_straddle", "long_strangle")
STRUCTURES = DIRECTIONAL + SHORT_VOL + LONG_VOL

#: Values that mean "trade the bot's own structure".
NATIVE = ("", "native", "none")

_DEFAULT_WIDTH_INCREMENTS = 2

# (bot_id, structure) pairs already warned about, so a misconfigured bot logs
# once instead of every 5-second tick.
_warned: set = set()


def _leg(snap: "MarketSnapshot", right: str, side: str, strike: float, expiration: str) -> Leg:
    from src.tradeworkz.bots.base import _synthetic_option_symbol

    return Leg(
        option_symbol=_synthetic_option_symbol(snap.underlying, right, strike, expiration),
        side=side,
        option_type=right,
        strike=strike,
        expiration=expiration,
    )


def _increments(params: Dict, key: str, default: int) -> int:
    try:
        return max(0, int(params.get(key, default)))
    except (TypeError, ValueError):
        return default


def family_of(signal: TradeSignal) -> str:
    """``directional`` / ``short_vol`` / ``long_vol`` for a native signal."""
    if signal.direction in ("bullish", "bearish"):
        return "directional"
    if any(str(leg.side).lower() == "short" for leg in signal.legs):
        return "short_vol"
    return "long_vol"


def _family_members(family: str) -> Tuple[str, ...]:
    return {"directional": DIRECTIONAL, "short_vol": SHORT_VOL, "long_vol": LONG_VOL}[family]


def _earliest_expiration(signal: TradeSignal) -> Optional[str]:
    exps = sorted(str(leg.expiration)[:10] for leg in signal.legs if leg.expiration)
    return exps[0] if exps else None


Builder = Callable[["MarketSnapshot", TradeSignal, str, Dict], Tuple[List[Leg], str]]


def _directional(kind: str) -> Builder:
    def build(snap, signal, exp, params):
        inc = snap.effective_strike_increment()
        atm = snap.round_to_strike(snap.spot)
        w = max(1, _increments(params, "structure_width_increments", _DEFAULT_WIDTH_INCREMENTS))
        off = _increments(params, "structure_offset_increments", 0)
        bull = signal.direction == "bullish"
        sgn = 1.0 if bull else -1.0
        right = "call" if bull else "put"
        if kind == "long_single":
            k = atm + sgn * off * inc
            return [_leg(snap, right, "long", k, exp)], ("LONG_CALL" if bull else "LONG_PUT")
        if kind == "debit_vertical":
            k = atm + sgn * off * inc
            legs = [
                _leg(snap, right, "long", k, exp),
                _leg(snap, right, "short", k + sgn * w * inc, exp),
            ]
            return legs, ("BULL_CALL_DEBIT_SPREAD" if bull else "BEAR_PUT_DEBIT_SPREAD")
        if kind == "credit_vertical":
            # Sell the side the view says expires worthless: puts below for a
            # bullish view, calls above for a bearish one.
            right = "put" if bull else "call"
            short = atm - sgn * off * inc
            legs = [
                _leg(snap, right, "short", short, exp),
                _leg(snap, right, "long", short - sgn * w * inc, exp),
            ]
            return legs, ("BULL_PUT_CREDIT_SPREAD" if bull else "BEAR_CALL_CREDIT_SPREAD")
        # butterfly: body on the target when it sits on the profitable side,
        # else one width out in the trade's direction.
        center = atm + sgn * w * inc
        tgt = signal.target_price
        if tgt is not None and (tgt - snap.spot) * sgn > 0:
            center = snap.round_to_strike(tgt)
            if (center - atm) * sgn <= 0:
                center = atm + sgn * inc
        legs = [
            _leg(snap, right, "long", center - w * inc, exp),
            _leg(snap, right, "short", center, exp),
            _leg(snap, right, "short", center, exp),
            _leg(snap, right, "long", center + w * inc, exp),
        ]
        return legs, ("LONG_CALL_BUTTERFLY" if bull else "LONG_PUT_BUTTERFLY")

    return build


def _iron_condor(snap, signal, exp, params):
    inc = snap.effective_strike_increment()
    atm = snap.round_to_strike(snap.spot)
    w = max(1, _increments(params, "structure_width_increments", _DEFAULT_WIDTH_INCREMENTS))
    off = max(1, _increments(params, "structure_offset_increments", w))
    legs = [
        _leg(snap, "put", "long", atm - (off + w) * inc, exp),
        _leg(snap, "put", "short", atm - off * inc, exp),
        _leg(snap, "call", "short", atm + off * inc, exp),
        _leg(snap, "call", "long", atm + (off + w) * inc, exp),
    ]
    return legs, "SHORT_IRON_CONDOR"


def _iron_fly(snap, signal, exp, params):
    inc = snap.effective_strike_increment()
    atm = snap.round_to_strike(snap.spot)
    w = max(1, _increments(params, "structure_width_increments", _DEFAULT_WIDTH_INCREMENTS))
    legs = [
        _leg(snap, "put", "long", atm - w * inc, exp),
        _leg(snap, "put", "short", atm, exp),
        _leg(snap, "call", "short", atm, exp),
        _leg(snap, "call", "long", atm + w * inc, exp),
    ]
    return legs, "SHORT_IRON_BUTTERFLY"


def _long_butterfly(snap, signal, exp, params):
    inc = snap.effective_strike_increment()
    atm = snap.round_to_strike(snap.spot)
    w = max(1, _increments(params, "structure_width_increments", _DEFAULT_WIDTH_INCREMENTS))
    legs = [
        _leg(snap, "call", "long", atm - w * inc, exp),
        _leg(snap, "call", "short", atm, exp),
        _leg(snap, "call", "short", atm, exp),
        _leg(snap, "call", "long", atm + w * inc, exp),
    ]
    return legs, "LONG_CALL_BUTTERFLY"


def _long_straddle(snap, signal, exp, params):
    atm = snap.round_to_strike(snap.spot)
    legs = [_leg(snap, "call", "long", atm, exp), _leg(snap, "put", "long", atm, exp)]
    return legs, "LONG_STRADDLE"


def _long_strangle(snap, signal, exp, params):
    inc = snap.effective_strike_increment()
    atm = snap.round_to_strike(snap.spot)
    w = max(1, _increments(params, "structure_width_increments", _DEFAULT_WIDTH_INCREMENTS))
    off = max(1, _increments(params, "structure_offset_increments", w))
    legs = [
        _leg(snap, "call", "long", atm + off * inc, exp),
        _leg(snap, "put", "long", atm - off * inc, exp),
    ]
    return legs, "LONG_STRANGLE"


_BUILDERS: Dict[str, Builder] = {
    **{k: _directional(k) for k in DIRECTIONAL},
    "iron_condor": _iron_condor,
    "iron_fly": _iron_fly,
    "long_butterfly": _long_butterfly,
    "long_straddle": _long_straddle,
    "long_strangle": _long_strangle,
}


def _warn_once(bot_id: str, kind: str, why: str) -> None:
    if (bot_id, kind) in _warned:
        return
    _warned.add((bot_id, kind))
    logger.warning("TradeWorkz %s: structure %r not used — %s", bot_id, kind, why)


def reexpress(
    bot: "BaseBot", snap: "MarketSnapshot", signal: TradeSignal, kind: str
) -> Optional[TradeSignal]:
    """``signal`` rebuilt as structure ``kind``, or None when it can't be.

    None means the configured structure doesn't fit this signal (unknown name,
    wrong family, no expiration) — the caller drops the signal rather than
    trading a structure nobody asked for.
    """
    from src.tradeworkz.sizing import is_capped_risk

    kind = kind.strip().lower()
    bot_id = getattr(bot.spec, "id", "?")
    builder = _BUILDERS.get(kind)
    if builder is None:
        _warn_once(bot_id, kind, f"unknown; one of {', '.join(STRUCTURES)}")
        return None
    family = family_of(signal)
    if kind not in _family_members(family):
        _warn_once(bot_id, kind, f"this bot's signals are {family}")
        return None
    exp = _earliest_expiration(signal)
    if exp is None or snap.spot is None or snap.spot <= 0:
        return None
    legs, strategy_type = builder(snap, signal, exp, bot.params)
    legs_dicts = [
        {"side": lg.side, "option_type": lg.option_type, "strike": lg.strike} for lg in legs
    ]
    if not is_capped_risk(legs_dicts):  # pragma: no cover - guarded by construction
        _warn_once(bot_id, kind, "built an uncapped structure")
        return None
    components = dict(signal.components_at_entry or {})
    components["structure"] = kind
    components["native_strategy_type"] = signal.strategy_type
    rationale = f"{signal.rationale} | expressed as {kind}" if signal.rationale else kind
    return replace(
        signal,
        legs=legs,
        strategy_type=strategy_type,
        components_at_entry=components,
        rationale=rationale,
    )
