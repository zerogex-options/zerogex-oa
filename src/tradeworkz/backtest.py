"""TradeWorkz backtest harness — does any bot actually have entry edge?

This is a **research tool**, not part of the live engine. It replays each bot's
real ``open_criteria`` / ``exit_criteria`` against the market as it actually
looked at past instants, prices the fills from the historical option chain, and
reports per-bot expectancy / profit factor / win rate. Nothing here writes to
any ``tw_*`` table — it reads history and simulates in memory.

How it stays faithful to live behavior
--------------------------------------
* **Snapshots** are reconstructed with :func:`context.build_snapshot(as_of=t)`,
  which bounds every read to ``timestamp <= t`` — the bot sees exactly the rows
  that existed at ``t`` (Phase 1).
* **The clock** is injected via :func:`bots.base.set_backtest_clock`, so the
  bot's own ``time_stop_at`` stamp, the min-hold gate, the premium-stop grace,
  and the time-stop all run on replay time — not real time.
* **Fills** come from :func:`historical_spread_price`, the time-bounded,
  archive-aware analog of :func:`pricing.spread_price`: long pays ask, short
  collects bid, plus the same slippage; expired legs settle at intrinsic.
* **Entry gates** (min-premium / min-credit), the **bias veto**, and the
  **RTH opens window** are replicated from the engine, so a signal that the live
  engine would have refused is refused here too.

What is deliberately turned OFF (to isolate raw entry/exit edge)
---------------------------------------------------------------
* **Scaling / the ladder** — positions are never armed (ladder geometry left
  NULL), so every trade is a single entry → single exit round-trip. This
  measures the signal, not the exit-management overlay.
* **ML calibration** — bots run with empty ML state (cold start): confidence and
  sizing fall back to their param defaults, no learned win-prob adjustment. We
  are testing the strategy logic, not the calibrator.
* **Capital / Kelly sizing** — every trade is measured at **one contract**.
  P&L is reported per-contract dollars and as return on the signed net entry, so
  the ranking reflects edge-per-trade, independent of how big the sleeve sizes.

Known limitations (disclosed, not hidden)
-----------------------------------------
* **Regime percentile bands** (``gex_historical_stats``) are a nightly 30-day
  aggregate with no per-instant history, so the symbol-relative "strong/weak"
  split uses today's bands, not the bands as they stood at ``t``. This shifts
  slowly and never changes the *sign* of the regime; it can nudge a
  strong-vs-weak boundary entry. Everything else is strictly as-of.
* **Coverage is bounded by option-chain history.** The report prints the actual
  quotable window and how many timesteps priced, so a thin history reads as thin
  coverage — never as "no edge".

Run it: ``make tradeworkz-backtest`` (or
``python -m src.tradeworkz.backtest --days 5 --interval-min 5``).
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.market_calendar import ET as _ET
from src.market_calendar import is_market_hours
from src.signals.execution import leg_fill_price
from src.tradeworkz import config as tw_config
from src.tradeworkz.bots import base as bot_base
from src.tradeworkz.context import build_snapshot
from src.tradeworkz.models import BotSpec, OpenPosition
from src.tradeworkz.pricing import _leg_intrinsic, _underlying_from_option_symbol
from src.tradeworkz.reconciler import (
    _cap_time_stop_at_expiration,
    _earliest_leg_expiration,
)
from src.tradeworkz.registry import get_bot_class, known_specs
from src.tradeworkz.sizing import structure_max_loss_per_share

logger = logging.getLogger(__name__)

_RTH_OPEN = time(9, 30)
_RTH_CLOSE = time(16, 0)

_DEFAULT_DAYS = 5
_DEFAULT_INTERVAL_MIN = 5
# How far back (minutes) a quote may be and still count as a fill at ``at``.
# Mirrors the spirit of OPTION_QUOTE_MAX_AGE_SECONDS: a stale quote is not a
# fill. Bounded to the PAST only — never forward — so there is no look-ahead.
_DEFAULT_TOLERANCE_MIN = 10
# Below this trade count a per-bot verdict is "insufficient" rather than a
# confident edge/no-edge call — a handful of round-trips proves nothing.
_MIN_TRADES_FOR_VERDICT = 20
# Soft floor per half for the train/test robustness flag (matches the
# execution sweep's guard): fewer trades in either half means the split
# cannot certify robustness, whatever the numbers say.
_MIN_TRADES_PER_HALF = 6


# ===========================================================================
# Phase 2 — historical leg pricer
# ===========================================================================

_LEG_QUOTE_TABLES = ("option_chains", "option_chains_archive")


def _archive_available(conn: Any) -> bool:
    """Whether ``option_chains_archive`` exists, memoized on the connection.

    Probed once via ``to_regclass`` rather than catching a failing SELECT per
    call — a failed statement would poison the transaction and break every
    later read in the run.
    """
    cached = getattr(conn, "_tw_bt_archive", None)
    if cached is not None:
        return bool(cached)
    available = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.option_chains_archive') IS NOT NULL")
        row = cur.fetchone()
        available = bool(row and row[0])
    except Exception:  # pragma: no cover - default to live-only on probe error
        available = False
    try:
        conn._tw_bt_archive = available
    except Exception:  # pragma: no cover - some fakes disallow attribute set
        pass
    return available


def _quote_from_table(
    conn: Any,
    table: str,
    *,
    leg: Dict[str, Any],
    at: datetime,
    tolerance_min: int,
) -> Optional[Tuple[float, float, float]]:
    """Nearest ``(bid, ask, last)`` for ``leg`` at-or-before ``at`` from ``table``.

    Bounded to ``[at - tolerance_min, at]`` so a fill can never see a future
    quote and a rolled-off contract can't fill on a stale print. Matches the
    exact ``option_symbol`` first; if that contract has no row in the window,
    falls back to the nearest listed strike of the same right/expiration (a
    synthetic ATM strike may not land exactly on a historically listed strike).
    """
    cur = conn.cursor()
    lo = at - timedelta(minutes=tolerance_min)
    sym = leg.get("option_symbol")
    if sym:
        cur.execute(
            f"""
            SELECT bid, ask, last
            FROM {table}
            WHERE option_symbol = %s
              AND timestamp <= %s AND timestamp >= %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (sym, at, lo),
        )
        row = cur.fetchone()
        if row is not None:
            return (float(row[0] or 0.0), float(row[1] or 0.0), float(row[2] or 0.0))

    # Structured fallback: nearest strike of the same right + expiration.
    underlying = _underlying_from_option_symbol(sym) if sym else None
    try:
        strike = float(leg.get("strike"))
    except (TypeError, ValueError):
        strike = None
    exp = leg.get("expiration")
    otype = str(leg.get("option_type") or "").lower()
    right = "C" if otype.startswith("c") else "P" if otype.startswith("p") else None
    if underlying is None or strike is None or not exp or right is None:
        return None
    cur.execute(
        f"""
        SELECT bid, ask, last
        FROM {table}
        WHERE underlying = %s AND option_type = %s AND expiration = %s
          AND timestamp <= %s AND timestamp >= %s
        ORDER BY ABS(strike - %s), timestamp DESC
        LIMIT 1
        """,
        (underlying, right, exp[:10], at, lo, strike),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return (float(row[0] or 0.0), float(row[1] or 0.0), float(row[2] or 0.0))


def _nearest_leg_quote(
    conn: Any, leg: Dict[str, Any], at: datetime, tolerance_min: int
) -> Optional[Tuple[float, float, float]]:
    """Nearest leg quote at-or-before ``at``, live chain first then the archive."""
    for table in _LEG_QUOTE_TABLES:
        if table == "option_chains_archive" and not _archive_available(conn):
            continue
        quote = _quote_from_table(conn, table, leg=leg, at=at, tolerance_min=tolerance_min)
        if quote is not None:
            return quote
    return None


def historical_spread_price(
    conn: Any,
    legs: List[Dict[str, Any]],
    *,
    at: datetime,
    action: str,
    slippage_pct: Optional[float] = None,
    tolerance_min: int = _DEFAULT_TOLERANCE_MIN,
) -> Optional[float]:
    """Signed per-share debit/credit for ``legs`` priced as of a past instant.

    The time-bounded analog of :func:`pricing.spread_price`. ``action`` is
    ``'open'`` (long pays ask, short collects bid) or ``'close'`` (sides invert),
    plus the same configurable slippage. Positive = debit paid, negative =
    credit received. Returns ``None`` when any leg has no usable quote in the
    lookback window and can't be intrinsic-settled — i.e. no fill.

    Expired-leg fallback mirrors live: on ``action == 'close'`` a leg whose
    quotes have rolled off but whose expiration is on/before ``at`` settles at
    intrinsic against the underlying's regular-session close (via
    :func:`pricing._leg_intrinsic`), so an expired 0DTE books its cash P&L
    instead of failing to price.
    """
    slip = tw_config.EXECUTION_SLIPPAGE_PCT if slippage_pct is None else slippage_pct
    settle_day = at.astimezone(_ET).date()
    long_sum = 0.0
    short_sum = 0.0
    for leg in legs:
        side = (leg.get("side") or "").lower()
        if side not in {"long", "short"}:
            return None
        quote = _nearest_leg_quote(conn, leg, at, tolerance_min)
        if quote is None:
            if action == "close":
                intrinsic = _leg_intrinsic(conn, leg, settle_day)
                if intrinsic is not None:
                    # _leg_intrinsic already carries the short-side sign, so it
                    # is the leg's per-share P&L contribution; feed it back
                    # through the same long_sum − short_sum convention.
                    if side == "long":
                        long_sum += intrinsic
                    else:
                        short_sum += -intrinsic
                    continue
            return None
        bid, ask, last = quote
        price = leg_fill_price(
            bid=bid, ask=ask, last=last, side=side, action=action, slippage_pct=slip
        )
        if side == "long":
            long_sum += price
        else:
            short_sum += price
    return long_sum - short_sum


# ===========================================================================
# Phase 3 — replay engine
# ===========================================================================

# Module-level clock cell the injected closure reads. The hook installed on
# bots.base captures this dict and returns its current value, so advancing the
# replay is a single assignment (``_CLOCK["now"] = t``) — no re-binding the hook
# each step. Reset to the epoch sentinel when a run finishes.
_CLOCK_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_CLOCK: Dict[str, datetime] = {"now": _CLOCK_EPOCH}


def _is_trading_day(d: date) -> bool:
    """Weekday that is not a configured NYSE holiday."""
    return is_market_hours(_ET.localize(datetime.combine(d, time(12, 0))))


def rth_timesteps(start_utc: datetime, end_utc: datetime, interval_min: int) -> List[datetime]:
    """RTH (09:30–16:00 ET) wall-clocks in ``[start_utc, end_utc]`` as UTC.

    Steps every ``interval_min`` minutes on each trading day, skipping weekends
    and NYSE holidays. Endpoints inclusive of the 16:00 ET close so an expiring
    0DTE gets a final settlement step.
    """
    if interval_min <= 0:
        raise ValueError("interval_min must be positive")
    start_et = start_utc.astimezone(_ET)
    end_et = end_utc.astimezone(_ET)
    steps: List[datetime] = []
    day = start_et.date()
    last_day = end_et.date()
    while day <= last_day:
        if _is_trading_day(day):
            cur_et = _ET.localize(datetime.combine(day, _RTH_OPEN))
            close_et = _ET.localize(datetime.combine(day, _RTH_CLOSE))
            while cur_et <= close_et:
                cur_utc = cur_et.astimezone(timezone.utc)
                if start_utc <= cur_utc <= end_utc:
                    steps.append(cur_utc)
                cur_et = cur_et + timedelta(minutes=interval_min)
        day = day + timedelta(days=1)
    return steps


def _opens_allowed(now_utc: datetime, no_new_opens_after: time) -> bool:
    """Replica of engine._opens_allowed for the replay clock.

    Blocks opens outside the ET cash session and at/after the 0DTE no-new-opens
    cap, so a position can never be opened already past its own hard time_stop.
    """
    if not tw_config.RTH_ONLY_OPENS:
        return True
    if not is_market_hours(now_utc):
        return False
    return now_utc.astimezone(_ET).time() < no_new_opens_after


def _force_settle_due(pos: OpenPosition, now_utc: datetime) -> bool:
    """Replica of engine._force_settle_due: unpriceable AND past time_stop/expired."""
    tsa = pos.time_stop_at
    if tsa is not None and now_utc >= tsa:
        return True
    exp = _earliest_leg_expiration(pos.legs)
    return exp is not None and exp < now_utc.astimezone(_ET).date()


@dataclass
class SimTrade:
    """One simulated round-trip, measured at a single contract."""

    bot_id: str
    underlying: str
    direction: str
    strategy_type: str
    opened_at: datetime
    closed_at: datetime
    hold_minutes: float
    entry_net: float
    exit_net: float
    max_loss_per_share: float
    pnl_dollars: float  # (exit − entry) × 100, downside-clamped to defined risk
    pnl_pct: float  # (exit − entry) / |entry| — matches live tw_trades
    outcome: str  # 'win' | 'loss' | 'scratch'
    reason: str
    conviction: float


def _resolve_universe(universe: str, fleet: Tuple[str, ...]) -> List[str]:
    """Resolve a bot's ``universe`` field to concrete tickers (engine parity)."""
    if not universe or universe.strip() == "*":
        return list(fleet)
    return [sym.strip().upper() for sym in universe.split(",") if sym.strip()]


class _BotRunner:
    """Holds one bot's replay state (open book + realized trades) across steps."""

    def __init__(self, spec: BotSpec, *, slippage_pct: Optional[float], tolerance_min: int):
        self.spec = spec
        # Neutral / frozen ML: empty state → confidence & sizing use param
        # defaults, no learned win-prob adjustment (cold start).
        self.bot = get_bot_class(spec.strategy_class)(spec, ml_state=None)
        self.slippage_pct = slippage_pct
        self.tolerance_min = tolerance_min
        self.open_by_underlying: Dict[str, OpenPosition] = {}
        self.trades: List[SimTrade] = []
        self.vetoed = 0
        self.no_quote_opens = 0
        self.priced_marks = 0
        # Signals that passed open_criteria, and why any of them failed to
        # become a position at the entry-viability gate (see _maybe_open).
        self.signals = 0
        self.entry_rejects: Dict[str, int] = {}

    def step(self, conn: Any, u: str, snap: Any, now_utc: datetime, opens_allowed: bool) -> None:
        """Advance this bot one timestep for underlying ``u``.

        Exits are evaluated first (a held position is marked, then run through
        ``exit_criteria``); only if no position is held do we look for a new
        entry — the same ordering and one-position-per-bot/underlying rule the
        live engine enforces.
        """
        pos = self.open_by_underlying.get(u)
        if pos is not None:
            self._manage(conn, u, pos, snap, now_utc)
            return  # never open in an underlying we still hold (or just closed)
        if not opens_allowed:
            return
        self._maybe_open(conn, u, snap, now_utc)

    def _manage(self, conn: Any, u: str, pos: OpenPosition, snap: Any, now_utc: datetime) -> None:
        mark = historical_spread_price(
            conn,
            pos.legs,
            at=now_utc,
            action="close",
            slippage_pct=self.slippage_pct,
            tolerance_min=self.tolerance_min,
        )
        if mark is None:
            # Unpriceable. If past its hard time_stop (or expired), force-settle
            # at the last observed mark so a 0DTE can't strand open; else skip
            # and retry next step when a quote may return.
            if _force_settle_due(pos, now_utc) and pos.current_price is not None:
                self._close(
                    u, pos, exit_net=pos.current_price, reason="time_stop_settle", at=now_utc
                )
            return
        self.priced_marks += 1
        pos.current_price = mark
        pos.unrealized_pnl = (mark - pos.entry_price) * pos.quantity_open * 100.0
        if pos.high_water_mark is None or mark > pos.high_water_mark:
            pos.high_water_mark = mark
        decision = self.bot.exit_criteria(snap, pos)
        if decision.should_close:
            self._close(u, pos, exit_net=mark, reason=decision.reason or "signal", at=now_utc)
        # Scaling is OFF (ladder never armed), so should_cut is ignored by design.

    def _maybe_open(self, conn: Any, u: str, snap: Any, now_utc: datetime) -> None:
        signal = self.bot.open_criteria(snap)
        if signal is None:
            return
        self.signals += 1
        if self.bot._bias_veto(snap, signal.direction):
            self.vetoed += 1
            return
        legs = [asdict(leg) for leg in signal.legs]
        entry_net = historical_spread_price(
            conn,
            legs,
            at=now_utc,
            action="open",
            slippage_pct=self.slippage_pct,
            tolerance_min=self.tolerance_min,
        )
        if entry_net is None:
            self.no_quote_opens += 1
            return
        # Entry-viability gates (mirror reconciler.open_position): classify by
        # the SIGN of the net, not by the presence of a short leg — a defined-
        # risk DEBIT spread (net > 0) is a debit, not a credit structure. Each
        # rejection is tallied so a 0-trade run shows signals dying at entry.
        if entry_net > 0:
            if tw_config.MIN_ENTRY_PREMIUM > 0 and entry_net < tw_config.MIN_ENTRY_PREMIUM:
                self._entry_reject("below_min_premium")
                return
        elif entry_net < 0:
            if -entry_net < float(tw_config.MIN_CREDIT_PER_SHARE):
                self._entry_reject("below_min_credit")
                return
        else:
            self._entry_reject("zero_net")
            return
        self.open_by_underlying[u] = self._make_position(signal, legs, entry_net, snap, now_utc)

    def _entry_reject(self, reason: str) -> None:
        self.entry_rejects[reason] = self.entry_rejects.get(reason, 0) + 1

    def _make_position(
        self, signal: Any, legs: List[Dict[str, Any]], entry_net: float, snap: Any, at: datetime
    ) -> OpenPosition:
        """Build the in-memory position. Ladder fields stay NULL → scaling OFF."""
        min_hold_until = at + timedelta(seconds=tw_config.MIN_HOLD_SECONDS)
        capped_ts = _cap_time_stop_at_expiration(signal.time_stop_at, legs)
        return OpenPosition(
            id=-1,
            bot_id=self.spec.id,
            underlying=signal.underlying,
            opened_at=at,
            direction=signal.direction,
            strategy_type=signal.strategy_type,
            legs=legs,
            entry_price=entry_net,
            current_price=entry_net,
            quantity_open=1,
            unrealized_pnl=0.0,
            stop_price=signal.stop_price,
            target_price=signal.target_price,
            time_stop_at=capped_ts,
            min_hold_until=min_hold_until,
            wall_ref_price=signal.wall_ref_price,
            wall_ref_side=signal.wall_ref_side,
            entry_conviction=signal.conviction,
            components_at_entry=dict(signal.components_at_entry or {}),
            entry_spot=snap.spot,
            quantity_initial=1,
            high_water_mark=entry_net,
        )

    def _close(
        self, u: str, pos: OpenPosition, *, exit_net: float, reason: str, at: datetime
    ) -> None:
        max_loss = structure_max_loss_per_share(pos.legs, pos.entry_price)
        pnl_share = exit_net - pos.entry_price
        # Defined-risk floor: a trade can never lose more than its structural
        # max loss, no matter how illiquid the close print (matches the live
        # risk-bounded accounting). No upside clamp — long structures are
        # unbounded and the rare condor's capped win is left slightly generous.
        if max_loss > 0 and pnl_share < -max_loss:
            pnl_share = -max_loss
        pnl_dollars = pnl_share * 100.0
        pnl_pct = pnl_share / abs(pos.entry_price) if pos.entry_price else 0.0
        outcome = "win" if pnl_dollars > 0 else "loss" if pnl_dollars < 0 else "scratch"
        hold_min = max(0.0, (at - pos.opened_at).total_seconds() / 60.0)
        self.trades.append(
            SimTrade(
                bot_id=pos.bot_id,
                underlying=pos.underlying,
                direction=pos.direction,
                strategy_type=pos.strategy_type,
                opened_at=pos.opened_at,
                closed_at=at,
                hold_minutes=round(hold_min, 1),
                entry_net=round(pos.entry_price, 4),
                exit_net=round(exit_net, 4),
                max_loss_per_share=round(max_loss, 4),
                pnl_dollars=round(pnl_dollars, 2),
                pnl_pct=round(pnl_pct, 4),
                outcome=outcome,
                reason=reason,
                conviction=round(float(pos.entry_conviction or 0.0), 3),
            )
        )
        self.open_by_underlying.pop(u, None)

    def finalize(self, conn: Any, at: datetime) -> None:
        """Force-close any still-open positions at the last priceable mark."""
        for u, pos in list(self.open_by_underlying.items()):
            mark = historical_spread_price(
                conn,
                pos.legs,
                at=at,
                action="close",
                slippage_pct=self.slippage_pct,
                tolerance_min=self.tolerance_min,
            )
            if mark is None:
                mark = pos.current_price
            if mark is None:
                self.open_by_underlying.pop(u, None)
                continue
            self._close(u, pos, exit_net=mark, reason="window_end", at=at)


def _load_backtest_bots(conn: Any, bot_ids: Optional[List[str]]) -> List[BotSpec]:
    """Load bot specs for the screen.

    Loads ALL bots (or the given subset) regardless of the live ``enabled``
    flag — the point of the screen is to measure edge, including for bots that
    are currently disabled. The live on/off switch is not a market gate, so it
    is not replicated; the report notes each bot's current enabled state for
    context.

    Primary source is ``tw_bots`` (the provisioned fleet). When explicit
    ``bot_ids`` are requested, any id NOT present in ``tw_bots`` falls back to
    the registry catalog (``registry.known_specs`` — candidates + shelved + the
    standalone magnet). This is what lets a ``CANDIDATE_SPECS`` bot be screened
    before it is ever provisioned: the promotion discipline keeps candidates out
    of the DB, but the whole point of a candidate is that it must clear the
    backtest first, so the harness has to be able to find it by id.
    """
    cur = conn.cursor()
    if bot_ids:
        cur.execute(
            """
            SELECT id, display_name, strategy_class, tier, direction_mode,
                   universe, tagline, description, is_public, enabled, params
            FROM tw_bots WHERE id = ANY(%s) ORDER BY id
            """,
            (list(bot_ids),),
        )
    else:
        cur.execute("""
            SELECT id, display_name, strategy_class, tier, direction_mode,
                   universe, tagline, description, is_public, enabled, params
            FROM tw_bots ORDER BY id
            """)
    specs: List[BotSpec] = []
    for r in cur.fetchall():
        params = r[10]
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except Exception:
                params = {}
        specs.append(
            BotSpec(
                id=r[0],
                display_name=r[1],
                strategy_class=r[2],
                tier=r[3],
                direction_mode=r[4],
                universe=r[5] or "*",
                tagline=r[6] or "",
                description=r[7] or "",
                params=dict(params or {}),
                is_public=bool(r[8]),
                enabled=bool(r[9]),
            )
        )

    # Registry fallback for explicitly-requested ids not in tw_bots — screens
    # un-provisioned candidates / shelved bots (see docstring).
    if bot_ids:
        found = {s.id for s in specs}
        missing = [b for b in bot_ids if b not in found]
        if missing:
            catalog = known_specs()
            for bid in missing:
                spec = catalog.get(bid)
                if spec is not None:
                    specs.append(spec)
    return specs


def _chain_window(conn: Any) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Min / max option_chains timestamp — the quotable coverage window."""
    cur = conn.cursor()
    cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM option_chains")
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def run_fleet_backtest(
    conn: Any,
    *,
    days: int = _DEFAULT_DAYS,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    interval_min: int = _DEFAULT_INTERVAL_MIN,
    bot_ids: Optional[List[str]] = None,
    specs: Optional[List[BotSpec]] = None,
    slippage_pct: Optional[float] = None,
    tolerance_min: int = _DEFAULT_TOLERANCE_MIN,
) -> Dict[str, Any]:
    """Replay the fleet over a historical window and return per-bot results.

    The window defaults to the last ``days`` calendar days ending now, clamped
    to the option-chain coverage window so we never step over instants that
    cannot be priced. Snapshots are built once per (timestep, underlying) and
    shared across bots — so running many bots (e.g. an execution sweep of one
    signal across param configs) costs ~the same wall-clock as one.

    ``specs`` injects a ready list of :class:`BotSpec` directly (bypassing the
    ``tw_bots`` / registry load), which the execution-sweep tool uses to screen
    param-varied synthetic configs of the same strategy. When given, ``bot_ids``
    is ignored.
    """
    now = datetime.now(timezone.utc)
    end = end or now
    start = start or (end - timedelta(days=days))

    chain_lo, chain_hi = _chain_window(conn)
    if chain_lo is not None:
        start = max(start, chain_lo)
    if chain_hi is not None:
        end = min(end, chain_hi)
    if start >= end:
        return {
            "window": {"start": None, "end": None},
            "coverage": {"chain_min": _iso(chain_lo), "chain_max": _iso(chain_hi)},
            "bots": [],
            "error": "no priceable window (option_chains history does not cover the request)",
        }

    if specs is None:
        specs = _load_backtest_bots(conn, bot_ids)
    if not specs:
        return {
            "window": {"start": _iso(start), "end": _iso(end)},
            "bots": [],
            "error": "no bots found",
        }

    fleet = tw_config.fleet_universes()
    runners = [_BotRunner(s, slippage_pct=slippage_pct, tolerance_min=tolerance_min) for s in specs]
    bot_universes = {s.id: _resolve_universe(s.universe, fleet) for s in specs}
    needed = sorted({u for us in bot_universes.values() for u in us})

    no_new_opens_after = _parse_hhmm(tw_config.RTH_NO_NEW_OPENS_AFTER_ET, time(15, 55))
    steps = rth_timesteps(start, end, interval_min)
    logger.info(
        "tradeworkz backtest: %d steps × %d underlyings × %d bots (%s → %s)",
        len(steps),
        len(needed),
        len(runners),
        _iso(start),
        _iso(end),
    )

    priced_steps = 0
    bot_base.set_backtest_clock(lambda: _CLOCK["now"])
    try:
        for i, t in enumerate(steps):
            _CLOCK["now"] = t
            snaps = {u: build_snapshot(conn, u, as_of=t) for u in needed}
            if any(v is not None for v in snaps.values()):
                priced_steps += 1
            opens_ok = _opens_allowed(t, no_new_opens_after)
            for runner in runners:
                for u in bot_universes[runner.spec.id]:
                    snap = snaps.get(u)
                    if snap is not None:
                        runner.step(conn, u, snap, t, opens_ok)
            if steps and (i + 1) % 200 == 0:
                logger.info("  ... %d/%d steps", i + 1, len(steps))
        # Settle any positions still open at the window's end.
        if steps:
            for runner in runners:
                runner.finalize(conn, steps[-1])
    finally:
        bot_base.set_backtest_clock(None)
        _CLOCK["now"] = _CLOCK_EPOCH

    split_at = start + (end - start) / 2
    bot_reports = [summarize_bot(r, split_at=split_at) for r in runners]
    bot_reports.sort(key=lambda b: (b["n_trades"] > 0, b["expectancy"]), reverse=True)
    return {
        "window": {"start": _iso(start), "end": _iso(end)},
        "params": {
            "interval_min": interval_min,
            "tolerance_min": tolerance_min,
            "slippage_pct": (
                slippage_pct if slippage_pct is not None else tw_config.EXECUTION_SLIPPAGE_PCT
            ),
            "scaling": "off",
            "ml": "neutral",
            "contracts": 1,
        },
        "coverage": {
            "chain_min": _iso(chain_lo),
            "chain_max": _iso(chain_hi),
            "steps": len(steps),
            "steps_with_data": priced_steps,
        },
        "fleet": _fleet_totals(bot_reports),
        "bots": bot_reports,
    }


# ===========================================================================
# Phase 4 — reporting
# ===========================================================================


def _half_metrics(trades: List[SimTrade]) -> Dict[str, Any]:
    """Compact metrics for one half of a train/test split."""
    n = len(trades)
    gross_win = sum(t.pnl_dollars for t in trades if t.pnl_dollars > 0)
    gross_loss = -sum(t.pnl_dollars for t in trades if t.pnl_dollars < 0)
    total = sum(t.pnl_dollars for t in trades)
    pf = (gross_win / gross_loss) if gross_loss > 0 else None
    return {
        "n_trades": n,
        "total_pnl": round(total, 2),
        "expectancy": round(total / n, 2) if n else 0.0,
        "profit_factor": round(pf, 3) if pf is not None else None,
    }


def _split_report(
    trades: List[SimTrade],
    split_at: Optional[datetime],
    full_verdict: str,
) -> Optional[Dict[str, Any]]:
    """Window-midpoint TRAIN/TEST split — the promotion overfitting guard.

    Same semantics as ``tw_execution_sweep``: trades are assigned to halves by
    entry time against the WINDOW midpoint (not by trade count, so a burst of
    late trades cannot masquerade as an even split), and ``robust`` requires
    the full-window verdict to be "edge" AND positive expectancy in BOTH
    halves AND at least ``_MIN_TRADES_PER_HALF`` trades in each. A config that
    shines in one half and dies in the other is noise, not edge — and is not
    promotable regardless of its full-window profit factor.
    """
    if split_at is None:
        return None
    train = [t for t in trades if t.opened_at < split_at]
    test = [t for t in trades if t.opened_at >= split_at]
    train_m, test_m = _half_metrics(train), _half_metrics(test)
    robust = (
        full_verdict == "edge"
        and train_m["expectancy"] > 0
        and test_m["expectancy"] > 0
        and min(train_m["n_trades"], test_m["n_trades"]) >= _MIN_TRADES_PER_HALF
    )
    return {"at": _iso(split_at), "train": train_m, "test": test_m, "robust": robust}


def summarize_bot(runner: _BotRunner, split_at: Optional[datetime] = None) -> Dict[str, Any]:
    """Per-bot expectancy / profit factor / win rate at one contract."""
    trades = runner.trades
    n = len(trades)
    wins = [t for t in trades if t.pnl_dollars > 0]
    losses = [t for t in trades if t.pnl_dollars < 0]
    gross_win = sum(t.pnl_dollars for t in wins)
    gross_loss = -sum(t.pnl_dollars for t in losses)
    total = sum(t.pnl_dollars for t in trades)
    pf = (gross_win / gross_loss) if gross_loss > 0 else None
    expectancy = (total / n) if n else 0.0
    win_rate = (len(wins) / n) if n else 0.0
    return {
        "bot_id": runner.spec.id,
        "display_name": runner.spec.display_name,
        "strategy_class": runner.spec.strategy_class,
        "currently_enabled": runner.spec.enabled,
        "n_trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 4),
        "profit_factor": round(pf, 3) if pf is not None else None,
        "expectancy": round(expectancy, 2),
        "total_pnl": round(total, 2),
        "avg_win": round(gross_win / len(wins), 2) if wins else 0.0,
        "avg_loss": round(gross_loss / len(losses), 2) if losses else 0.0,
        "avg_hold_min": round(sum(t.hold_minutes for t in trades) / n, 1) if n else 0.0,
        "bias_vetoed": runner.vetoed,
        "no_quote_opens": runner.no_quote_opens,
        # Signals that cleared open_criteria, and where any that did not become
        # positions were rejected at the entry-viability gate. A large
        # ``signals`` with 0 trades and non-empty ``entry_rejects`` means the
        # setup fires but the fills are being refused (not a signal problem).
        "signals": getattr(runner, "signals", 0),
        "entry_rejects": dict(
            sorted(getattr(runner, "entry_rejects", {}).items(), key=lambda kv: kv[1], reverse=True)
        ),
        # Per-gate rejection tally (most-frequent first). When n_trades is 0
        # this is the diagnostic: it names the gate that abstained every tick.
        "miss_reasons": dict(
            sorted(
                getattr(runner.bot, "miss_reasons", {}).items(),
                key=lambda kv: kv[1],
                reverse=True,
            )
        ),
        "verdict": _verdict(n, pf, expectancy),
        "split": _split_report(trades, split_at, _verdict(n, pf, expectancy)),
        "equity_curve": _equity_curve(trades),
    }


def _verdict(n: int, pf: Optional[float], expectancy: float) -> str:
    """Cull screen call: needs both a positive expectancy and PF ≥ 1.1 to pass."""
    if n < _MIN_TRADES_FOR_VERDICT:
        return "insufficient"
    if pf is None:  # no losing trades at all over a real sample
        return "edge" if expectancy > 0 else "no-edge"
    if pf >= 1.1 and expectancy > 0:
        return "edge"
    if pf >= 0.9:
        return "marginal"
    return "no-edge"


def _equity_curve(trades: List[SimTrade]) -> List[float]:
    """Cumulative per-contract dollar P&L, one point per closed trade."""
    curve: List[float] = []
    running = 0.0
    for t in trades:
        running += t.pnl_dollars
        curve.append(round(running, 2))
    return curve


def _fleet_totals(bot_reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = sum(b["n_trades"] for b in bot_reports)
    total = sum(b["total_pnl"] for b in bot_reports)
    with_edge = [b["bot_id"] for b in bot_reports if b["verdict"] == "edge"]
    marginal = [b["bot_id"] for b in bot_reports if b["verdict"] == "marginal"]
    return {
        "n_trades": n,
        "total_pnl": round(total, 2),
        "expectancy": round(total / n, 2) if n else 0.0,
        "bots_with_edge": with_edge,
        "bots_marginal": marginal,
    }


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if isinstance(dt, datetime) else None


def _parse_hhmm(raw: str, fallback: time) -> time:
    try:
        hh, mm = str(raw).strip().split(":")
        return time(int(hh), int(mm))
    except (ValueError, AttributeError):
        return fallback


def format_report(result: Dict[str, Any]) -> str:
    """Human-readable table for the CLI."""
    lines: List[str] = []
    w = result.get("window", {})
    cov = result.get("coverage", {})
    if result.get("error"):
        lines.append(f"backtest: {result['error']}")
        lines.append(f"  option_chains coverage: {cov.get('chain_min')} → {cov.get('chain_max')}")
        return "\n".join(lines)
    p = result.get("params", {})
    lines.append("=" * 92)
    lines.append("TradeWorkz backtest — per-bot entry edge (1 contract, scaling OFF, ML neutral)")
    lines.append("=" * 92)
    lines.append(f"window:   {w.get('start')} → {w.get('end')}")
    lines.append(
        f"replay:   every {p.get('interval_min')}m | quote tol {p.get('tolerance_min')}m | "
        f"slippage {p.get('slippage_pct')}"
    )
    lines.append(
        f"coverage: chain {cov.get('chain_min')} → {cov.get('chain_max')} | "
        f"{cov.get('steps_with_data')}/{cov.get('steps')} steps had data"
    )
    lines.append("")
    header = (
        f"{'bot_id':<26}{'trades':>7}{'win%':>7}{'PF':>7}"
        f"{'expect$':>9}{'total$':>10}{'hold_m':>7}  verdict"
    )
    lines.append(header)
    lines.append("-" * 92)
    for b in result.get("bots", []):
        pf = "  —  " if b["profit_factor"] is None else f"{b['profit_factor']:.2f}"
        flag = "" if b["currently_enabled"] else " (disabled)"
        lines.append(
            f"{b['bot_id']:<26}{b['n_trades']:>7}{b['win_rate'] * 100:>6.1f}%"
            f"{pf:>7}{b['expectancy']:>9.1f}{b['total_pnl']:>10.1f}"
            f"{b['avg_hold_min']:>7.0f}  {b['verdict']}{flag}"
        )
        split = b.get("split")
        if split and b["n_trades"]:
            tr, te = split["train"], split["test"]
            lines.append(
                f"{'  └ split':<26}{'':>7}{'':>7}{'':>7}"
                f"{'':>9}{'':>10}{'':>7}  "
                f"train {tr['n_trades']}t/{tr['expectancy']:+.0f}$ | "
                f"test {te['n_trades']}t/{te['expectancy']:+.0f}$ | "
                f"{'ROBUST' if split['robust'] else 'not robust'}"
            )
    f = result.get("fleet", {})
    lines.append("-" * 92)
    lines.append(
        f"{'FLEET':<26}{f.get('n_trades', 0):>7}{'':>7}{'':>7}"
        f"{f.get('expectancy', 0.0):>9.1f}{f.get('total_pnl', 0.0):>10.1f}"
    )
    lines.append("")
    lines.append(f"edge:     {', '.join(f.get('bots_with_edge') or []) or '(none)'}")
    lines.append(f"marginal: {', '.join(f.get('bots_marginal') or []) or '(none)'}")
    lines.append(
        "note: verdict needs PF ≥ 1.1 AND positive expectancy over "
        f"≥ {_MIN_TRADES_FOR_VERDICT} trades; fewer = 'insufficient'."
    )
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(
        prog="tradeworkz-backtest",
        description="Replay TradeWorkz bots over historical data to screen for entry edge.",
    )
    ap.add_argument(
        "--days",
        type=int,
        default=_DEFAULT_DAYS,
        help=f"lookback window in days (default {_DEFAULT_DAYS})",
    )
    ap.add_argument(
        "--interval-min",
        type=int,
        default=_DEFAULT_INTERVAL_MIN,
        help=f"replay step in minutes (default {_DEFAULT_INTERVAL_MIN})",
    )
    ap.add_argument(
        "--tolerance-min",
        type=int,
        default=_DEFAULT_TOLERANCE_MIN,
        help=f"max quote age for a fill (default {_DEFAULT_TOLERANCE_MIN})",
    )
    ap.add_argument(
        "--slippage",
        type=float,
        default=None,
        help="override execution slippage fraction (default: config)",
    )
    ap.add_argument(
        "--bots", type=str, default=None, help="comma-separated bot ids to test (default: all)"
    )
    ap.add_argument("--start", type=str, default=None, help="ISO start (overrides --days)")
    ap.add_argument("--end", type=str, default=None, help="ISO end (default: now)")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = ap.parse_args(argv)

    bot_ids = [b.strip() for b in args.bots.split(",") if b.strip()] if args.bots else None
    start = _parse_iso(args.start)
    end = _parse_iso(args.end)

    from src.database import db_connection

    with db_connection() as conn:
        result = run_fleet_backtest(
            conn,
            days=args.days,
            start=start,
            end=end,
            interval_min=args.interval_min,
            bot_ids=bot_ids,
            slippage_pct=args.slippage,
            tolerance_min=args.tolerance_min,
        )

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(format_report(result))
    return 0


def _parse_iso(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
