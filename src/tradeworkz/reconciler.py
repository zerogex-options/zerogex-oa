"""Position lifecycle — open, mark, close.

The reconciler wraps the DB writes so bots stay pure. Every write happens
inside a single transaction with a per-bot advisory lock to prevent double
opens if two engine ticks race.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from src.tradeworkz import config as tw_config
from src.tradeworkz import ml as ml_mod
from src.tradeworkz import notifications
from src.tradeworkz.models import BotCapital, ExitDecision, OpenPosition, TradeSignal
from src.tradeworkz.pricing import spread_price
from src.tradeworkz.sizing import compute_contracts, structure_max_loss_per_share

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")

# 15:55 ET — 5 minutes before the 4 pm regular-session equity close, and
# the last moment we can reliably get a filling option quote on the day
# a 0DTE expires. Every open-position time_stop_at is hard-capped at
# this instant on the earliest leg expiration so a 0DTE can never sit
# open past the point where the options market closes and the quote
# cache goes stale — see _cap_time_stop_at_expiration.
_EXPIRATION_CLOSE_HHMM = time(15, 55)


def _earliest_leg_expiration(legs: List[Dict[str, Any]]) -> Optional[date]:
    """Earliest leg expiration date across ``legs``, or ``None``.

    Structures currently trade single-leg debits, so the earliest is
    also the only expiration; the loop handles multi-leg spreads for
    completeness. Returns ``None`` if no leg carries a parseable ISO
    date so callers can no-op instead of guessing.
    """
    earliest: Optional[date] = None
    for leg in legs:
        exp_str = leg.get("expiration") or ""
        if not isinstance(exp_str, str) or len(exp_str) < 10:
            continue
        try:
            exp = date.fromisoformat(exp_str[:10])
        except ValueError:
            continue
        if earliest is None or exp < earliest:
            earliest = exp
    return earliest


def _expiration_close_cap_utc(expiration: date) -> datetime:
    """Return ``expiration`` at 15:55 ET converted to UTC.

    ``zoneinfo`` handles DST automatically so 15:55 ET is always the
    right wall-clock time in Eastern regardless of whether we're
    inside EDT or EST.
    """
    et_dt = datetime.combine(expiration, _EXPIRATION_CLOSE_HHMM, tzinfo=_ET)
    return et_dt.astimezone(timezone.utc)


def _cap_time_stop_at_expiration(
    time_stop_at: Optional[datetime], legs: List[Dict[str, Any]]
) -> Optional[datetime]:
    """Hard-cap ``time_stop_at`` at 15:55 ET on the earliest leg expiration.

    The bots each set their own max_hold_minutes (60-90 default), which
    is fine for a swing bot but nonsense for a 0DTE: an entry at 15:20 ET
    with max_hold_minutes=60 would put time_stop_at at 16:20 ET, 20
    minutes AFTER the options market closes for that expiration. The
    reconciler would then try to close the position on an option that
    no longer trades, spread_price would return None (quote too stale),
    and the position would sit "open" for hours with a stale mark. This
    cap makes sure time_stop_at cannot fire after the last moment we
    can plausibly fill.

    A ``None`` earliest-expiration (no parseable leg dates) is a no-op:
    we can't cap what we can't see, and refusing the signal would be
    worse than trusting the bot's own time_stop_at.
    """
    exp = _earliest_leg_expiration(legs)
    if exp is None:
        return time_stop_at
    cap = _expiration_close_cap_utc(exp)
    if time_stop_at is None or time_stop_at > cap:
        return cap
    return time_stop_at


def _bot_lock_id(bot_id: str) -> int:
    """Stable 32-bit advisory-lock id for a bot."""
    return int(hashlib.blake2b(bot_id.encode(), digest_size=4).hexdigest(), 16)


def _acquire_lock(conn: Any, bot_id: str) -> bool:
    if not tw_config.RECONCILE_LOCK_ENABLED:
        return True
    cur = conn.cursor()
    cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (_bot_lock_id(bot_id),))
    row = cur.fetchone()
    return bool(row[0]) if row else False


def load_capital(conn: Any, bot_id: str) -> Optional[BotCapital]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT bot_id, starting_capital, current_capital, peak_capital,
               max_heat_pct, kelly_fraction, daily_kill_pct
        FROM tw_bot_capital WHERE bot_id = %s
        """,
        (bot_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return BotCapital(
        bot_id=row[0],
        starting_capital=float(row[1]),
        current_capital=float(row[2]),
        peak_capital=float(row[3]),
        max_heat_pct=float(row[4]),
        kelly_fraction=float(row[5]),
        daily_kill_pct=float(row[6]),
    )


def load_open_positions(conn: Any, bot_id: str) -> List[OpenPosition]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, bot_id, underlying, opened_at, direction, strategy_type,
               legs, entry_price, current_price, quantity_open,
               unrealized_pnl, stop_price, target_price, time_stop_at,
               min_hold_until, wall_ref_price, wall_ref_side,
               entry_conviction, components_at_entry,
               entry_spot, quantity_initial, t1_trigger_price, s2_stop_price,
               target2_price, scale_stage, high_water_mark,
               realized_pnl_booked, exit_tranches
        FROM tw_positions WHERE bot_id = %s
        """,
        (bot_id,),
    )
    result: List[OpenPosition] = []
    for r in cur.fetchall():
        legs = r[6] or []
        if isinstance(legs, str):
            legs = json.loads(legs)
        comp = r[18] or {}
        if isinstance(comp, str):
            comp = json.loads(comp)
        tranches = r[27] or []
        if isinstance(tranches, str):
            tranches = json.loads(tranches)
        result.append(
            OpenPosition(
                id=r[0],
                bot_id=r[1],
                underlying=r[2],
                opened_at=r[3],
                direction=r[4],
                strategy_type=r[5],
                legs=list(legs),
                entry_price=float(r[7]),
                current_price=float(r[8] or 0.0),
                quantity_open=int(r[9]),
                unrealized_pnl=float(r[10] or 0.0),
                stop_price=float(r[11]) if r[11] is not None else None,
                target_price=float(r[12]) if r[12] is not None else None,
                time_stop_at=r[13],
                min_hold_until=r[14],
                wall_ref_price=float(r[15]) if r[15] is not None else None,
                wall_ref_side=r[16],
                entry_conviction=float(r[17]) if r[17] is not None else None,
                components_at_entry=dict(comp),
                entry_spot=float(r[19]) if r[19] is not None else None,
                quantity_initial=int(r[20]) if r[20] is not None else None,
                t1_trigger_price=float(r[21]) if r[21] is not None else None,
                s2_stop_price=float(r[22]) if r[22] is not None else None,
                target2_price=float(r[23]) if r[23] is not None else None,
                scale_stage=int(r[24] or 0),
                high_water_mark=float(r[25]) if r[25] is not None else None,
                realized_pnl_booked=float(r[26] or 0.0),
                exit_tranches=list(tranches),
            )
        )
    return result


def open_position(
    conn: Any,
    signal: TradeSignal,
    capital: BotCapital,
    size_multiplier: float,
    wall_strength: Optional[float] = None,
    daily_realized_pnl: float = 0.0,
    entry_spot: Optional[float] = None,
    scale_cfg: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    """Insert one row into ``tw_positions``. Returns the new position id.

    Skips if there is already an open position for the same bot / underlying
    within :data:`tw_config.ENTRY_DEDUPE_WINDOW_SECONDS`.

    ``entry_spot`` (the underlying spot at fill) and ``scale_cfg`` (the bot's
    resolved scale-out knobs) arm the scale-out ladder — see
    docs/design/tradeworkz-exit-strategy.md. When omitted, or when the ladder
    can't be armed, the position falls back to the single-target exit.
    """
    if not _acquire_lock(conn, signal.bot_id):
        logger.debug("skipping open: could not acquire lock for %s", signal.bot_id)
        return None

    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(1) FROM tw_positions
        WHERE bot_id = %s AND underlying = %s
          AND opened_at >= NOW() - (%s || ' seconds')::interval
        """,
        (signal.bot_id, signal.underlying, str(tw_config.ENTRY_DEDUPE_WINDOW_SECONDS)),
    )
    dupe = cur.fetchone()
    if dupe and dupe[0]:
        return None
    cur.execute(
        "SELECT COUNT(1) FROM tw_positions WHERE bot_id = %s AND underlying = %s",
        (signal.bot_id, signal.underlying),
    )
    open_count = cur.fetchone()
    if open_count and open_count[0]:
        # Only one open position per bot / underlying pair at a time.
        return None

    legs_dicts = [asdict(leg) for leg in signal.legs]
    entry_price = spread_price(conn, legs_dicts, action="open")
    if entry_price is None:
        logger.debug("no fillable quote for bot=%s legs=%s", signal.bot_id, legs_dicts)
        return None
    # spread_price returns the SIGNED net: positive = debit paid, negative =
    # credit received. Long-debit structures need a positive debit and a
    # premium above the floor; defined-risk credit structures (any short leg)
    # need a real net credit (a slippage-eroded ~$0 net can't profit).
    has_short = any(str(leg.get("side", "")).lower() == "short" for leg in legs_dicts)
    if not has_short:
        if entry_price <= 0:
            logger.debug("no fillable quote for bot=%s legs=%s", signal.bot_id, legs_dicts)
            return None
        min_prem = float(tw_config.MIN_ENTRY_PREMIUM)
        if min_prem > 0 and entry_price < min_prem:
            logger.debug(
                "entry premium %.4f below floor %.4f for bot=%s",
                entry_price, min_prem, signal.bot_id,
            )
            return None
    else:
        credit = -entry_price  # credit received per share (negative entry = credit)
        if credit < float(tw_config.MIN_CREDIT_PER_SHARE):
            logger.debug(
                "net credit %.4f below floor %.4f for bot=%s",
                credit, tw_config.MIN_CREDIT_PER_SHARE, signal.bot_id,
            )
            return None

    # Size on the structure's TRUE per-contract max loss, not the net premium.
    # Identical to entry_price for long-debit structures; for a defined-risk
    # credit structure (iron condor) it's the wing width, which prevents the
    # ~34x over-sizing that a near-zero net debit would otherwise produce.
    max_loss = structure_max_loss_per_share(legs_dicts, entry_price)
    contracts = compute_contracts(
        capital=capital,
        entry_price=entry_price,
        conviction=signal.conviction,
        size_multiplier=size_multiplier,
        wall_strength=wall_strength,
        daily_realized_pnl=daily_realized_pnl,
        max_loss_per_share=max_loss,
    )
    if contracts <= 0:
        return None

    now = datetime.now(timezone.utc)
    min_hold_until = now + timedelta(seconds=tw_config.MIN_HOLD_SECONDS)
    # Cap the bot's requested time_stop_at at 15:55 ET on the earliest
    # leg expiration so a 0DTE can never time-stop after the options
    # market shuts. See _cap_time_stop_at_expiration for rationale.
    capped_time_stop_at = _cap_time_stop_at_expiration(signal.time_stop_at, legs_dicts)
    payload = dict(signal.components_at_entry)
    payload.update(
        {
            "conviction": signal.conviction,
            "rationale": signal.rationale,
            "size_multiplier": size_multiplier,
            # Explicit provenance marker so the audit surface can
            # distinguish real engine-opened positions from seeder
            # rows without inferring from field absence. simulate.py
            # stamps "simulate" on its rows. Any row that predates this
            # commit will show as legacy-live (absent field → treated
            # as live) in the audit filter.
            "origin": "live",
        }
    )
    # Arm the scale-out ladder (docs/design/tradeworkz-exit-strategy.md).
    # entry_spot / quantity_initial / high_water_mark are always recorded; the
    # three frozen price levels are populated only when the ladder is armable
    # (enabled, directional, has a target, size >= the min-contract floor, and R
    # has the profitable sign). NULL price levels => the exit path uses today's
    # single-target behavior. scale_stage / realized_pnl_booked / exit_tranches
    # take their column defaults (0 / 0 / []).
    from src.tradeworkz.bots.base import compute_ladder_levels

    cfg = scale_cfg or {}
    levels: Optional[Dict[str, float]] = None
    if cfg.get("enabled") and contracts >= int(cfg.get("min_contracts", 4)):
        levels = compute_ladder_levels(
            entry_spot,
            signal.target_price,
            signal.direction,
            float(cfg.get("t1_frac", 0.90)),
            float(cfg.get("s2_frac", 0.75)),
            float(cfg.get("t2_frac", 1.5)),
            float(cfg.get("max_move_pct", 0.0)),
        )
    t1_price = levels["t1_trigger_price"] if levels else None
    s2_price = levels["s2_stop_price"] if levels else None
    t2_price = levels["target2_price"] if levels else None

    cur.execute(
        """
        INSERT INTO tw_positions (
            bot_id, underlying, opened_at, updated_at, direction,
            strategy_type, legs, entry_price, current_price, quantity_open,
            unrealized_pnl, stop_price, target_price, time_stop_at,
            min_hold_until, wall_ref_price, wall_ref_side, entry_conviction,
            components_at_entry,
            entry_spot, quantity_initial, t1_trigger_price, s2_stop_price,
            target2_price, high_water_mark
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, 0,
            %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
            %s, %s, %s, %s, %s, %s
        ) RETURNING id
        """,
        (
            signal.bot_id,
            signal.underlying,
            now,
            now,
            signal.direction,
            signal.strategy_type,
            json.dumps(legs_dicts),
            entry_price,
            entry_price,
            contracts,
            signal.stop_price,
            signal.target_price,
            capped_time_stop_at,
            min_hold_until,
            signal.wall_ref_price,
            signal.wall_ref_side,
            signal.conviction,
            json.dumps(payload),
            entry_spot,
            contracts,
            t1_price,
            s2_price,
            t2_price,
            entry_price,
        ),
    )
    row = cur.fetchone()
    position_id = int(row[0]) if row else None
    if position_id is not None:
        notifications.fanout_event(
            conn,
            bot_id=signal.bot_id,
            event_type="entry",
            payload={
                "underlying": signal.underlying,
                "direction": signal.direction,
                "strategy_type": signal.strategy_type,
                "conviction": signal.conviction,
                "entry_price": entry_price,
                "contracts": contracts,
                "target_price": signal.target_price,
                "stop_price": signal.stop_price,
                "rationale": signal.rationale,
            },
            position_id=position_id,
        )
    return position_id


def mark_position(conn: Any, pos: OpenPosition) -> Optional[float]:
    """Mark-to-market ``pos``. Returns updated per-share liquidation value.

    P&L is ``(mark − entry) × contracts × 100``. This is correct for BOTH
    long-debit structures and defined-risk credit structures (the iron condor),
    because :func:`spread_price` returns the SIGNED net of the whole structure —
    a debit is positive, a credit negative, at BOTH open and close — so
    ``entry_price`` and ``mark`` already carry the structure's sign. A short
    condor collected for a 0.50 credit (``entry_price = −0.50``) that decays to a
    0.00 mark books ``(0 − (−0.50)) × qty × 100`` = a WIN; no per-leg-``side``
    keying is needed. It is also independent of the bot's directional thesis: a
    put bought at 1.80 and sold at 2.20 gained $0.40/share either way. (The
    earlier code negated the P&L for ``direction == 'bearish'``, flipping every
    winner and loser — the bug that ran bearish sleeves from $111k to $4.4M in a
    bull market. The negation is gone.)
    """
    mark = spread_price(conn, pos.legs, action="close")
    if mark is None:
        return None
    upnl = (mark - pos.entry_price) * pos.quantity_open * 100.0
    # Track the mark's high-water mark for the runner's premium give-back trail
    # (docs/design/tradeworkz-exit-strategy.md). Tracked from entry; the trail
    # only consults it once the ladder is in Stage >= 1. GREATEST keeps it
    # monotone even if a NULL slipped in on a pre-ladder row.
    new_hwm = mark if pos.high_water_mark is None else max(pos.high_water_mark, mark)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE tw_positions
        SET current_price = %s, unrealized_pnl = %s,
            high_water_mark = GREATEST(COALESCE(high_water_mark, %s), %s),
            updated_at = NOW()
        WHERE id = %s
        """,
        (mark, upnl, mark, mark, pos.id),
    )
    pos.current_price = mark
    pos.unrealized_pnl = upnl
    pos.high_water_mark = new_hwm
    return mark


def partial_close_position(
    conn: Any, pos: OpenPosition, decision: ExitDecision
) -> Optional[int]:
    """Book a scale-out tranche onto ``pos`` WITHOUT writing a trade row.

    Sells ``cut_qty`` contracts at the current close mark, accumulates the
    realized P&L onto ``realized_pnl_booked`` and the tranche detail onto
    ``exit_tranches``, shrinks ``quantity_open``, advances ``scale_stage``,
    re-marks the remaining runner, and emits a ``scale`` audit event. It
    deliberately does NOT touch ``tw_bot_capital`` or ``tw_trades`` — the final
    :func:`close_position` folds every tranche into one size-weighted
    consolidated trade, so Invariants A/B hold at every instant
    (docs/design/tradeworkz-exit-strategy.md §7).

    ``decision.cut_fraction`` is applied to the ORIGINAL size at T1
    (scale_stage 0 → 1) and to the CURRENT remainder at T2 (1 → 2); at least one
    runner contract is always left. Returns the position id, or ``None`` if the
    tranche can't be priced or sized.
    """
    frac = max(0.0, min(1.0, float(decision.cut_fraction or 0.0)))
    if frac <= 0.0 or pos.quantity_open <= 1:
        return None
    fill = spread_price(conn, pos.legs, action="close")
    if fill is None:
        return None

    to_stage = int(pos.scale_stage or 0) + 1
    # T1 takes a fraction of the ORIGINAL size; T2 a fraction of the remainder.
    base_qty = (pos.quantity_initial or pos.quantity_open) if to_stage == 1 else pos.quantity_open
    cut_qty = int(frac * base_qty)  # floor for positive values
    cut_qty = max(1, min(cut_qty, pos.quantity_open - 1))  # always leave a runner

    tranche_realized = (fill - pos.entry_price) * cut_qty * 100.0
    remaining = pos.quantity_open - cut_qty
    booked = float(pos.realized_pnl_booked or 0.0) + tranche_realized
    upnl = (fill - pos.entry_price) * remaining * 100.0
    tranche = {
        "stage": to_stage,
        "reason": decision.reason or f"scale_t{to_stage}",
        "qty": cut_qty,
        "price": round(float(fill), 6),
        "realized": round(float(tranche_realized), 4),
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    tranches = list(pos.exit_tranches or [])
    tranches.append(tranche)

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE tw_positions
        SET quantity_open = %s,
            scale_stage = %s,
            realized_pnl_booked = %s,
            exit_tranches = %s::jsonb,
            current_price = %s,
            unrealized_pnl = %s,
            updated_at = NOW()
        WHERE id = %s
        """,
        (remaining, to_stage, booked, json.dumps(tranches), fill, upnl, pos.id),
    )
    # Keep the in-memory position consistent for any later read this tick.
    pos.quantity_open = remaining
    pos.scale_stage = to_stage
    pos.realized_pnl_booked = booked
    pos.exit_tranches = tranches
    pos.current_price = fill
    pos.unrealized_pnl = upnl

    notifications.fanout_event(
        conn,
        bot_id=pos.bot_id,
        event_type="scale",
        payload={
            "underlying": pos.underlying,
            "direction": pos.direction,
            "reason": tranche["reason"],
            "stage": to_stage,
            "contracts": cut_qty,
            "remaining": remaining,
            "entry_price": pos.entry_price,
            "exit_price": float(fill),
            "realized_pnl": float(tranche_realized),
            "booked_realized_pnl": float(booked),
        },
        position_id=pos.id,
    )
    return pos.id


def close_position(
    conn: Any, pos: OpenPosition, reason: str, fallback_fill: Optional[float] = None
) -> Optional[int]:
    """Close ``pos``, write an immutable tw_trades row, update capital,
    fan out notifications, and update the bot's ML state.

    Returns the ``tw_trades`` row id.

    ``fallback_fill`` is a last-resort per-share settlement price used ONLY
    when the structure cannot be priced live (``spread_price`` returns
    ``None`` — e.g. an expired 0DTE whose option quotes have rolled off and
    whose intrinsic settlement is unavailable). The engine passes the
    position's last observed mark here when force-settling a position that is
    past its hard time_stop, so a 0DTE can never strand open indefinitely. A
    normal close passes no fallback and still bails on an unpriceable
    structure, exactly as before.
    """
    final_fill = spread_price(conn, pos.legs, action="close")
    if final_fill is None:
        final_fill = fallback_fill
    if final_fill is None:
        return None
    # Consolidated close. Fold every scale-out tranche already booked on this
    # position (realized_pnl_booked / exit_tranches) together with the final
    # tranche into ONE immutable trade row. `quantity` = the ORIGINAL size;
    # `exit_price` = the size-weighted average fill, derived from the total
    # realized so that Invariant A — realized_pnl == (exit − entry) × qty × 100
    # — holds by construction (docs/design/tradeworkz-exit-strategy.md §7). For
    # an unarmed / never-scaled position (quantity_initial NULL or ==
    # quantity_open, no booked realized) this reduces EXACTLY to the pre-ladder
    # single close: booked=0, total_qty=quantity_open, exit_price=final_fill.
    #
    # Long debit position — per-contract P&L is (exit − entry) × 100 regardless
    # of directional thesis (see mark_position for why the bearish negation was
    # wrong); it is signed by direction only through where the spot levels sit.
    remaining_qty = pos.quantity_open
    final_realized = (final_fill - pos.entry_price) * remaining_qty * 100.0
    booked = float(pos.realized_pnl_booked or 0.0)
    realized = booked + final_realized
    total_qty = int(pos.quantity_initial) if pos.quantity_initial else remaining_qty
    if total_qty <= 0:
        total_qty = remaining_qty
    if total_qty > 0:
        # Size-weighted exit that reproduces `realized` exactly for this qty.
        # Holds for debit AND credit — entry_price carries the sign, so
        # Invariant A (realized == (exit − entry) × qty × 100) is preserved.
        exit_price = pos.entry_price + realized / (total_qty * 100.0)
    else:
        exit_price = final_fill
    # pnl_percent as return on the signed net entry: (exit − entry) / |entry|.
    # Identical to exit/entry − 1 for a debit; sensible and correctly-signed for
    # a credit structure (negative entry), where a decaying condor is a WIN.
    pnl_pct = (exit_price - pos.entry_price) / abs(pos.entry_price) if pos.entry_price else 0.0

    if realized > 0:
        outcome = "win"
    elif realized < 0:
        outcome = "loss"
    else:
        outcome = "scratch"
    now = datetime.now(timezone.utc)

    # Full tranche ledger for the audit trail — every partial take plus this
    # final tranche, behind the one consolidated number.
    final_tranche = {
        "stage": int(pos.scale_stage or 0),
        "reason": reason,
        "qty": remaining_qty,
        "price": round(float(final_fill), 6),
        "realized": round(float(final_realized), 4),
        "final": True,
    }
    all_tranches = list(pos.exit_tranches or []) + [final_tranche]
    components_at_exit = {
        "exit_reason": reason,
        "weighted_exit": round(float(exit_price), 6),
        "final_fill": round(float(final_fill), 6),
        "scale_stage": int(pos.scale_stage or 0),
        "booked_realized_pnl": round(booked, 4),
        "tranches": all_tranches,
    }

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tw_trades (
            bot_id, underlying, opened_at, closed_at, direction,
            strategy_type, legs, entry_price, exit_price, quantity,
            realized_pnl, pnl_percent, outcome, close_reason,
            entry_conviction, components_at_entry, components_at_exit
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s,
            %s, %s::jsonb, %s::jsonb
        ) RETURNING id
        """,
        (
            pos.bot_id,
            pos.underlying,
            pos.opened_at,
            now,
            pos.direction,
            pos.strategy_type,
            json.dumps(pos.legs),
            pos.entry_price,
            exit_price,
            total_qty,
            realized,
            pnl_pct,
            outcome,
            reason,
            pos.entry_conviction,
            json.dumps(pos.components_at_entry),
            json.dumps(components_at_exit),
        ),
    )
    row = cur.fetchone()
    trade_id = int(row[0]) if row else None

    cur.execute("DELETE FROM tw_positions WHERE id = %s", (pos.id,))
    cur.execute(
        """
        UPDATE tw_bot_capital
        SET current_capital = current_capital + %s,
            peak_capital = GREATEST(peak_capital, current_capital + %s),
            updated_at = NOW()
        WHERE bot_id = %s
        """,
        (realized, realized, pos.bot_id),
    )

    state = ml_mod.load_state(conn, pos.bot_id)
    components = dict(pos.components_at_entry)
    components["conviction"] = pos.entry_conviction or components.get("conviction")
    state = ml_mod.online_update(state, components, won=(outcome == "win"))
    ml_mod.save_state(conn, state)
    # Defensive recompute of the rolling aggregates (hit rate -> confidence,
    # profit factor -> size multiplier, adaptive threshold) + the auto-disable
    # circuit breaker, right after each close. This is what keeps the ML
    # surface live between sweeps — without it recalibrate_bot never runs and
    # size_multiplier / profit_factor sit at their defaults forever.
    if tw_config.CALIBRATION_ENABLED:
        # Isolate the recompute in a SAVEPOINT: a calibration SQL failure
        # aborts the transaction, and without this the close's own commit
        # (and the fanout below) would roll back with it — losing the trade
        # record. The savepoint confines a failure to the recompute alone.
        gcur = conn.cursor()
        try:
            gcur.execute("SAVEPOINT tw_recalibrate")
            ml_mod.recalibrate_bot(conn, pos.bot_id)
            gcur.execute("RELEASE SAVEPOINT tw_recalibrate")
        except Exception:  # pragma: no cover - calibration must not break a close
            try:
                gcur.execute("ROLLBACK TO SAVEPOINT tw_recalibrate")
            except Exception:
                pass
            logger.warning("recalibrate_bot failed for %s after close", pos.bot_id, exc_info=True)

    notifications.fanout_event(
        conn,
        bot_id=pos.bot_id,
        event_type="exit",
        payload={
            "underlying": pos.underlying,
            "direction": pos.direction,
            "outcome": outcome,
            "realized_pnl": realized,
            "pnl_percent": pnl_pct,
            "reason": reason,
            # Consolidated original size, not the final runner's remaining qty.
            "contracts": total_qty,
            "entry_price": pos.entry_price,
            "exit_price": exit_price,
            "scaled": bool(pos.exit_tranches),
        },
        trade_id=trade_id,
        position_id=pos.id,
    )
    return trade_id


def daily_realized_pnl(conn: Any, bot_id: str) -> float:
    # Sum realized P&L over the ET trading day (the daily-kill-switch basis),
    # NOT the UTC calendar day: a UTC boundary would reset the kill switch at
    # 20:00 ET mid-session and mis-attribute an after-hours close to the next
    # day. Bucket the close and "now" both in America/New_York so they agree.
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(realized_pnl), 0)
        FROM tw_trades
        WHERE bot_id = %s
          AND (closed_at AT TIME ZONE 'America/New_York')::date
              = (NOW() AT TIME ZONE 'America/New_York')::date
        """,
        (bot_id,),
    )
    row = cur.fetchone()
    return float(row[0]) if row else 0.0
