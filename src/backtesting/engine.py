"""Backtest simulation engine.

Replays persisted Action Cards over a historical window and prices each one as
a real option-leg round trip via a forward bar-by-bar walk
(``docs/design/backtesting-platform.md`` §2):

* **Entry** fills at its trigger bar — immediately for at-market Cards, or at
  the first bar that touches ``entry.ref_price`` for touch/break triggers — and
  the entry option is priced at that fill bar.
* **Exit** is resolved by scanning the underlying series **strictly after** the
  fill bar (a ≥1-bar minimum hold, so a target that prints inside the entry bar
  no longer forces a zero-hold same-instant round trip that booked pure spread
  loss). The first level target/stop touch resolves the exit, else it times out
  at the last bar in the hold window; the exit option is priced at that bar.
* **P&L** uses ``src.signals.execution.leg_fill_price`` — long bought at
  ask·(1+slip), sold at bid·(1−slip), plus per-contract commission both ways.

Premium/event-kind target/stop exits are handled in Phase 2; here a Card whose
target and stop are both non-level is ``unresolved``.

Position sizing allocates ``risk_per_trade_pct`` of *running realized equity*
to premium per trade, capped by ``max_concurrent`` simultaneously-open
positions. The engine is pure given a DB connection and a spec; the run
lifecycle / persistence lives in ``runner.py``.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import Callable, Optional

from src.backtesting.models import (
    BacktestSpec,
    EquityPoint,
    RunResult,
    TradeResult,
)
from src.signals.execution import leg_fill_price
from src.signals.playbook.backtest import (
    _IMMEDIATE_TRIGGERS,
    _hit_stop,
    _hit_target,
    _level_or_none,
    _signed_excursion,
    CardRow,
    fetch_action_cards,
    fetch_quotes,
)

logger = logging.getLogger(__name__)

# How far from the desired entry/exit instant we will accept an option_chains
# quote. Chains are minute bars; a few minutes of tolerance absorbs the
# occasional skipped poll without silently mispricing a fill.
_QUOTE_TOLERANCE_MIN = 6

# Default hold window when a Card omits max_hold_minutes and the spec does not
# override it. Mirrors the playbook harness' practical 0DTE horizon.
_DEFAULT_MAX_HOLD_MIN = 240


def _to_card_row(raw) -> CardRow:
    """``fetch_action_cards`` already returns CardRow; identity passthrough.

    Kept as a seam so tests can feed plain CardRow instances built by hand.
    """
    return raw


def _select_leg(card: CardRow) -> Optional[dict]:
    """Pick the single option leg to trade for a directional Card.

    Prefers the Card's own persisted ``legs`` (a BUY leg), which is the exact
    contract the live playbook would have traded. Falls back to a synthetic
    ATM contract derived from direction + entry ref price when legs are absent
    (older Cards predating leg persistence).
    """
    payload = card.payload or {}
    legs = payload.get("legs") or []
    for leg in legs:
        side = str(leg.get("side") or "").upper()
        if side in ("BUY", "LONG", ""):
            right = str(leg.get("right") or "").upper()[:1]
            if right not in ("C", "P"):
                right = "C" if card.direction == "bullish" else "P"
            return {
                "expiry": leg.get("expiry"),
                "strike": leg.get("strike"),
                "right": right,
            }
    # Synthetic ATM fallback: option type by direction, strike at entry ref.
    entry = (payload.get("entry") or {}).get("ref_price")
    if not isinstance(entry, (int, float)) or entry <= 0:
        return None
    return {
        "expiry": None,  # resolved to the nearest expiration at lookup time
        "strike": round(float(entry)),
        "right": "C" if card.direction == "bullish" else "P",
    }


def _leg_is_buy(side: str, action: str) -> bool:
    """Opening a long = buy; closing a short = buy-to-close (sign of cashflow)."""
    return (side == "long") == (action == "open")


def _select_legs(card: CardRow) -> Optional[list[dict]]:
    """All legs of the structure (1 for single-leg, 2 for a vertical, …).

    Each leg is normalized to ``{expiry, strike, right, side('long'|'short'),
    qty}``. Falls back to a single synthetic ATM long when the Card carries no
    legs.
    """
    payload = card.payload or {}
    raw_legs = payload.get("legs") or []
    legs: list[dict] = []
    for leg in raw_legs:
        side_raw = str(leg.get("side") or "BUY").upper()
        side = "long" if side_raw in ("BUY", "LONG", "") else "short"
        right = str(leg.get("right") or "").upper()[:1]
        if right not in ("C", "P"):
            right = "C" if card.direction == "bullish" else "P"
        try:
            qty = max(1, int(leg.get("qty") or 1))
        except (TypeError, ValueError):
            qty = 1
        legs.append({
            "expiry": leg.get("expiry"), "strike": leg.get("strike"),
            "right": right, "side": side, "qty": qty,
        })
    if legs:
        return legs
    single = _select_leg(card)
    if single is None:
        return None
    return [{**single, "side": "long", "qty": 1}]


def _price_legs(conn, underlying, legs, at, action, slip):
    """Net per-share cashflow to ``action`` (open/close) the position at ``at``.

    Buys are a negative cashflow (you pay the ask), sells positive (you receive
    the bid); each scaled by leg qty. Returns ``(cashflow, resolved_legs)`` —
    ``resolved_legs`` carry the concrete contract so the exit prices the same
    options — or ``None`` if any leg is unpriceable.
    """
    cashflow = 0.0
    resolved: list[dict] = []
    for leg in legs:
        q = _fetch_leg_quote(conn, underlying=underlying, leg=leg, at=at)
        if q is None:
            return None
        fill = leg_fill_price(
            bid=q["bid"], ask=q["ask"], last=q["last"],
            side=leg["side"], action=action, slippage_pct=slip,
        )
        if fill <= 0:
            return None
        sign = -1.0 if _leg_is_buy(leg["side"], action) else 1.0
        cashflow += sign * fill * leg["qty"]
        resolved.append({
            **leg, "option_symbol": q["option_symbol"], "strike": q["strike"],
            "expiry": q["expiration"], "right": q["option_type"],
        })
    return cashflow, resolved


def _close_cashflow_from_series(resolved_legs, series_by_symbol, at, slip) -> Optional[float]:
    """Net cashflow to close the position at ``at`` using pre-fetched series.

    Returns None when any leg lacks a quote at ``at`` (so the overlay simply
    skips that bar rather than mispricing the spread).
    """
    cashflow = 0.0
    for leg in resolved_legs:
        row = series_by_symbol.get(leg["option_symbol"], {}).get(at)
        if row is None:
            return None
        fill = leg_fill_price(
            bid=row["bid"], ask=row["ask"], last=row["last"],
            side=leg["side"], action="close", slippage_pct=slip,
        )
        sign = -1.0 if _leg_is_buy(leg["side"], "close") else 1.0
        cashflow += sign * fill * leg["qty"]
    return cashflow


def _structure_label(resolved_legs: list[dict]) -> str:
    if len(resolved_legs) == 1:
        return "single"
    rights = {leg["right"] for leg in resolved_legs}
    if len(resolved_legs) == 2 and len(rights) == 1:
        return "vertical"
    if len(resolved_legs) == 2:
        return "strangle" if rights == {"C", "P"} else "spread"
    if len(resolved_legs) == 4:
        return "condor"
    return "multi_leg"


def _defined_risk_clamp(pnl_per_share: float, net_debit: float, resolved_legs: list[dict]) -> float:
    """Bound a vertical's realized P&L to its no-arbitrage limits.

    A vertical's liquidation value is in [0, width], so its P&L is in
    [-net_debit, width-net_debit] (debit) or the credit-spread mirror. Illiquid
    near-expiry bid/ask on the short leg can imply a worse close than that
    bound; without the clamp a tiny-debit spread (sized into a large contract
    count) books losses far beyond its defined risk. Singles (value ≥ 0) are
    already bounded, so they pass through unchanged.
    """
    strikes = [float(leg["strike"]) for leg in resolved_legs if leg.get("strike") is not None]
    is_vertical = (
        len(resolved_legs) == 2
        and len({leg["right"] for leg in resolved_legs}) == 1
        and len(strikes) == 2
    )
    if not is_vertical:
        return pnl_per_share
    width = max(strikes) - min(strikes)
    if width <= 0:
        return pnl_per_share
    if net_debit >= 0:
        lo, hi = -net_debit, width - net_debit
    else:  # credit spread: received -net_debit, risk = width − credit
        credit = -net_debit
        lo, hi = -(width - credit), credit
    return min(max(pnl_per_share, lo), hi)


def _max_loss_per_share(open_cashflow: float, resolved_legs: list[dict]) -> Optional[float]:
    """Defined-risk capital at risk per share (×100 = per contract).

    Debit (you paid): the debit. Credit (you received): width − credit. A
    non-positive result means the structure isn't a sane defined-risk position.
    """
    net_debit = -open_cashflow
    if net_debit >= 0:
        return net_debit if net_debit > 0 else None
    strikes = [float(leg["strike"]) for leg in resolved_legs if leg.get("strike") is not None]
    width = (max(strikes) - min(strikes)) if len(strikes) >= 2 else 0.0
    max_loss = width + net_debit  # net_debit is negative ⇒ width − credit
    return max_loss if max_loss > 0 else None


# Source tables tried, in order, for a leg quote: the live hot table first
# (covers the 90-day retained window), then the durable archive (covers older
# windows once the nightly src/tools/backtest_archive.py job has copied them).
_LEG_QUOTE_TABLES = ("option_chains", "option_chains_archive")


def _archive_available(conn) -> bool:
    """Whether option_chains_archive exists, memoized on the connection.

    Checked once via ``to_regclass`` rather than catching a failing SELECT per
    call — a failed statement would poison a non-autocommit transaction and
    break every subsequent card in the run.
    """
    cached = getattr(conn, "_zg_archive_available", None)
    if cached is not None:
        return cached
    available = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.option_chains_archive') IS NOT NULL")
        row = cur.fetchone()
        available = bool(row and row[0])
    except Exception:  # pragma: no cover - default to live-only on any probe error
        available = False
    try:
        conn._zg_archive_available = available
    except Exception:  # pragma: no cover - some fakes disallow attribute set
        pass
    return available


def _fetch_leg_quote_from(
    conn,
    table: str,
    *,
    underlying: str,
    leg: dict,
    at: datetime,
) -> Optional[tuple]:
    """Nearest quote row for ``leg`` around ``at`` from a single table, or None."""
    cur = conn.cursor()
    lo = at - timedelta(minutes=_QUOTE_TOLERANCE_MIN)
    hi = at + timedelta(minutes=_QUOTE_TOLERANCE_MIN)
    params: list = [underlying, leg["right"], lo, hi]
    expiry_clause = ""
    if leg.get("expiry"):
        expiry_clause = "AND expiration = %s"
        params.append(leg["expiry"])
    if leg.get("strike") is not None:
        # Match the closest strike to the requested one (synthetic ATM may not
        # land exactly on a listed strike).
        strike_clause = "ORDER BY ABS(strike - %s), ABS(EXTRACT(EPOCH FROM (timestamp - %s)))"
        params.extend([leg["strike"], at])
    else:
        strike_clause = "ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - %s)))"
        params.append(at)
    cur.execute(
        f"""
        SELECT option_symbol, strike, expiration, option_type,
               bid, ask, last, mid, timestamp
        FROM {table}
        WHERE underlying = %s
          AND option_type = %s
          AND timestamp BETWEEN %s AND %s
          {expiry_clause}
        {strike_clause}
        LIMIT 1
        """,
        params,
    )
    return cur.fetchone()


def _fetch_leg_quote(
    conn,
    *,
    underlying: str,
    leg: dict,
    at: datetime,
) -> Optional[dict]:
    """Nearest leg quote around ``at``, trying live chains then the archive.

    Returns a dict with bid/ask/last/mid/option_symbol/strike/expiration, or
    None when no quote exists within the tolerance window in either source.
    """
    row = None
    for table in _LEG_QUOTE_TABLES:
        if table == "option_chains_archive" and not _archive_available(conn):
            continue
        row = _fetch_leg_quote_from(conn, table, underlying=underlying, leg=leg, at=at)
        if row is not None:
            break
    if row is None:
        return None
    return {
        "option_symbol": row[0],
        "strike": float(row[1]) if row[1] is not None else None,
        "expiration": row[2],
        "option_type": row[3],
        "bid": float(row[4]) if row[4] is not None else 0.0,
        "ask": float(row[5]) if row[5] is not None else 0.0,
        "last": float(row[6]) if row[6] is not None else 0.0,
        "mid": float(row[7]) if row[7] is not None else 0.0,
        "ts": row[8],
    }


def _fetch_option_series(conn, option_symbol: str, after_ts: datetime, deadline: datetime) -> dict:
    """Minute premium series for one resolved contract, keyed by timestamp.

    Spans ``(after_ts, deadline]``. Used by the Phase-2 premium exit overlay to
    detect take-profit / stop-loss touches and to price the exit at the trigger
    bar. Live ``option_chains`` takes precedence; the archive fills older bars.
    """
    series: dict[datetime, dict] = {}
    for table in _LEG_QUOTE_TABLES:
        if table == "option_chains_archive" and not _archive_available(conn):
            continue
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT timestamp, bid, ask, last, mid
            FROM {table}
            WHERE option_symbol = %s AND timestamp > %s AND timestamp <= %s
            ORDER BY timestamp
            """,
            (option_symbol, after_ts, deadline),
        )
        for r in cur.fetchall():
            ts = r[0]
            if ts in series:  # live (queried first) wins
                continue
            series[ts] = {
                "bid": float(r[1]) if r[1] is not None else 0.0,
                "ask": float(r[2]) if r[2] is not None else 0.0,
                "last": float(r[3]) if r[3] is not None else 0.0,
                "mid": float(r[4]) if r[4] is not None else 0.0,
            }
    return series


def _option_mark(row: dict) -> float:
    """Best mark for a premium-trigger check: mid, else (bid+ask)/2, else last."""
    mid = row.get("mid") or 0.0
    if mid > 0:
        return mid
    bid, ask, last = row.get("bid") or 0.0, row.get("ask") or 0.0, row.get("last") or 0.0
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return max(last, ask, bid, 0.0)


def _resolve_entry_fill(quotes, *, entry_ref: float, immediate: bool):
    """Return the (ts, idx) of the bar the trade fills on, or (None, None).

    Immediate triggers (at_market/at_close/…) fill on the first bar at/after the
    Card timestamp. Touch/break triggers fill on the first bar whose intrabar
    range reaches ``entry_ref`` — a Card whose trigger never prints is a genuine
    ``no_fill`` and must not be counted as a trade (it was the pattern firing,
    not a realized entry).
    """
    for idx, (ts, o, high, low, c) in enumerate(quotes):
        if immediate or (low <= entry_ref <= high):
            return ts, idx
    return None, None


def _build_candidate(
    conn,
    card: CardRow,
    spec: BacktestSpec,
) -> tuple[Optional[dict], str]:
    """Resolve one Card into a priced option round-trip via a forward walk.

    Unlike the original two-point model (which priced entry and exit at the same
    ``compute_outcome`` timestamp and so booked a pure spread loss whenever the
    target printed inside the entry bar), this walks the underlying series:

      1. fills the entry at its trigger bar (immediate, or first touch/break),
      2. prices the entry option at that fill bar,
      3. scans bars **strictly after** the fill bar (a ≥1-bar min hold, so there
         is never a zero-hold same-instant round trip) for the first level
         target/stop touch, else times out at the last bar,
      4. prices the exit option at that resolved bar.

    Returns ``(candidate, reason)``; on a drop ``reason`` names the funnel stage
    (``outcome:<label>`` / ``no_leg`` / ``no_entry_quote`` / ``no_exit_quote`` /
    ``bad_premium``) for the run diagnostics. Premium-kind target/stop exits are
    handled in a follow-up (Phase 2); here non-level exits are ``unresolved``.
    """
    payload = dict(card.payload or {})
    # Apply the spec's max-hold override (or supply a default) so cards that
    # never recorded one are still resolvable.
    if spec.exit.max_hold_minutes is not None:
        payload = {**payload, "max_hold_minutes": spec.exit.max_hold_minutes}
    elif not payload.get("max_hold_minutes"):
        payload = {**payload, "max_hold_minutes": _DEFAULT_MAX_HOLD_MIN}

    direction = card.direction or payload.get("direction") or ""
    if direction not in ("bullish", "bearish"):
        return None, "outcome:non_directional"

    entry_payload = payload.get("entry") or {}
    entry_ref = entry_payload.get("ref_price")
    if not isinstance(entry_ref, (int, float)) or entry_ref <= 0:
        return None, "outcome:no_entry_ref"
    entry_ref = float(entry_ref)
    trigger = str(entry_payload.get("trigger") or "").strip().lower()
    immediate = trigger in _IMMEDIATE_TRIGGERS

    target_price = _level_or_none(payload.get("target"))
    stop_price = _level_or_none(payload.get("stop"))
    profit_target_pct = spec.exit.profit_target_pct
    stop_loss_pct = spec.exit.stop_loss_pct
    has_level = target_price is not None or stop_price is not None
    has_premium = profit_target_pct is not None or stop_loss_pct is not None
    if not has_level and not has_premium:
        # Card's target/stop are premium/event-kind and no premium overlay was
        # supplied — nothing resolvable. Configure exit.profit_target_pct /
        # stop_loss_pct to backtest these on the option premium series.
        return None, "outcome:unresolved"

    legs = _select_legs(
        CardRow(
            underlying=card.underlying, timestamp=card.timestamp, pattern=card.pattern,
            action=card.action, tier=card.tier, direction=direction,
            confidence=card.confidence, payload=payload,
        )
    )
    if not legs:
        return None, "no_leg"

    max_hold = int(payload.get("max_hold_minutes") or _DEFAULT_MAX_HOLD_MIN)
    quotes = fetch_quotes(
        conn, card.underlying, card.timestamp, card.timestamp + timedelta(minutes=max_hold)
    )
    quotes = [q for q in quotes if q[0] >= card.timestamp]
    if not quotes:
        return None, "outcome:no_data"

    # 1) Entry fill bar.
    fill_ts, fill_idx = _resolve_entry_fill(quotes, entry_ref=entry_ref, immediate=immediate)
    if fill_ts is None:
        return None, "outcome:no_fill"

    # 2) Open the position at the fill bar; lock the resolved contracts.
    slip = spec.fill_model.slippage_pct
    opened = _price_legs(conn, card.underlying, legs, fill_ts, "open", slip)
    if opened is None:
        return None, "no_entry_quote"
    open_cashflow, resolved_legs = opened
    net_debit = -open_cashflow              # >0 debit paid, <0 credit received
    max_loss = _max_loss_per_share(open_cashflow, resolved_legs)
    if max_loss is None:
        return None, "bad_premium"

    # Phase-2 premium exit overlay, generalized to the structure: take-profit /
    # stop on net position P&L expressed as a fraction of capital-at-risk
    # (max_loss). For a single long this is exactly the old "mark vs entry·(1±pct)".
    deadline = card.timestamp + timedelta(minutes=max_hold)
    series_by_symbol: dict = {}
    if has_premium:
        for leg in resolved_legs:
            series_by_symbol[leg["option_symbol"]] = _fetch_option_series(
                conn, leg["option_symbol"], fill_ts, deadline
            )
    prem_target = profit_target_pct * max_loss if profit_target_pct else None
    prem_stop = stop_loss_pct * max_loss if stop_loss_pct else None

    # 3) Forward walk for the exit — bars STRICTLY AFTER the fill bar (min hold).
    mfe_pct = 0.0
    mae_pct = 0.0
    exit_ts: Optional[datetime] = None
    outcome = "time_exit"
    for (ts, o, high, low, c) in quotes[fill_idx + 1:]:
        favorable, adverse = (high, low) if direction == "bullish" else (low, high)
        mfe_pct = max(mfe_pct, _signed_excursion(direction, entry_ref, favorable))
        mae_pct = min(mae_pct, _signed_excursion(direction, entry_ref, adverse))
        hit_t = _hit_target(direction, target_price, high, low)
        hit_s = _hit_stop(direction, stop_price, high, low)
        # Premium overlay: net P&L if we closed the whole structure at this bar.
        if has_premium:
            close_cf = _close_cashflow_from_series(resolved_legs, series_by_symbol, ts, slip)
            if close_cf is not None:
                pnl = open_cashflow + close_cf
                if prem_target is not None and pnl >= prem_target:
                    hit_t = True
                if prem_stop is not None and pnl <= -prem_stop:
                    hit_s = True
        # Same-bar both-touch: intrabar order is unknown → resolve to stop.
        if hit_t and hit_s:
            exit_ts, outcome = ts, "stop_hit"
            break
        if hit_t:
            exit_ts, outcome = ts, "target_hit"
            break
        if hit_s:
            exit_ts, outcome = ts, "stop_hit"
            break
        exit_ts = ts  # trail the last seen bar for the time-exit case

    if exit_ts is None:
        # Only the fill bar existed — no bar to hold into.
        return None, "outcome:no_exit_bar"

    # 4) Close the position at the resolved bar — prefer the already-fetched
    # premium series (consistent with what the overlay saw), else a point lookup.
    close_cashflow: Optional[float] = None
    if has_premium:
        close_cashflow = _close_cashflow_from_series(resolved_legs, series_by_symbol, exit_ts, slip)
    if close_cashflow is None:
        closed = _price_legs(conn, card.underlying, resolved_legs, exit_ts, "close", slip)
        if closed is None:
            return None, "no_exit_quote"
        close_cashflow, _ = closed

    primary = resolved_legs[0]
    hold_minutes = max(0, int((exit_ts - fill_ts).total_seconds() // 60))
    pnl_per_share = _defined_risk_clamp(open_cashflow + close_cashflow, net_debit, resolved_legs)
    exit_value = net_debit + pnl_per_share  # keeps (exit − entry)·100 == pnl

    return {
        "card": card,
        "outcome": outcome,
        "entered_at": fill_ts,
        "exited_at": exit_ts,
        "structure": _structure_label(resolved_legs),
        "n_legs": len(resolved_legs),
        "legs": [
            {"option_symbol": leg["option_symbol"], "right": leg["right"],
             "side": leg["side"],
             "strike": float(leg["strike"]) if leg["strike"] is not None else None,
             "expiration": leg["expiry"].isoformat() if leg.get("expiry") else None,
             "qty": leg["qty"]}
            for leg in resolved_legs
        ],
        "option_symbol": primary["option_symbol"],
        "option_type": primary["right"],
        "strike": primary["strike"],
        "expiration": primary["expiry"],
        "entry_premium": net_debit,            # net debit (credit if negative)
        "exit_premium": exit_value,            # net value to close (defined-risk clamped)
        "max_loss_per_share": max_loss,
        "pnl_per_contract": pnl_per_share * 100.0,
        "hold_minutes": hold_minutes,
        "mfe_pct": round(mfe_pct, 6),
        "mae_pct": round(mae_pct, 6),
    }, "ok"


def _simulate(candidates: list[dict], spec: BacktestSpec) -> RunResult:
    """Chronological capital/concurrency walk over priced candidates."""
    capital = spec.sizing.capital
    risk_frac = spec.sizing.risk_per_trade_pct / 100.0
    commission = spec.fill_model.commission_per_contract
    max_concurrent = spec.sizing.max_concurrent

    # Order by entry; tie-break by exit so closes are deterministic.
    candidates = sorted(candidates, key=lambda c: (c["entered_at"], c["exited_at"]))

    realized_equity = capital
    peak_equity = capital
    open_positions: list[dict] = []  # each: {exit_at, net_pnl}
    trades: list[TradeResult] = []
    equity: list[EquityPoint] = []
    seq = 0
    concurrency_skipped = 0
    sized_out = 0

    def _close_until(when: datetime) -> None:
        nonlocal realized_equity, peak_equity
        open_positions.sort(key=lambda p: p["exit_at"])
        while open_positions and open_positions[0]["exit_at"] <= when:
            pos = open_positions.pop(0)
            realized_equity += pos["net_pnl"]
            peak_equity = max(peak_equity, realized_equity)
            dd = 0.0 if peak_equity <= 0 else (realized_equity - peak_equity) / peak_equity * 100.0
            equity.append(EquityPoint(t=pos["exit_at"], equity=realized_equity, drawdown_pct=dd))

    for cand in candidates:
        # Realize any positions that closed before this entry.
        _close_until(cand["entered_at"])

        if len(open_positions) >= max_concurrent:
            concurrency_skipped += 1
            continue  # concurrency cap reached; skip this signal

        # Capital at risk per contract = defined max loss (= net debit for a
        # single long / debit spread; width − credit for a credit spread).
        risk_per_contract = cand.get("max_loss_per_share", cand["entry_premium"]) * 100.0
        if risk_per_contract <= 0:
            sized_out += 1
            continue
        # Allocate risk_frac of *currently realized* equity to max loss.
        risk_dollars = max(realized_equity, 0.0) * risk_frac
        contracts = int(math.floor(risk_dollars / risk_per_contract))
        contracts = max(contracts, 1)
        # Never risk more than the capital on hand can cover.
        if risk_per_contract * contracts > max(realized_equity, 0.0):
            contracts = int(math.floor(max(realized_equity, 0.0) / risk_per_contract))
        if contracts < 1:
            sized_out += 1
            continue  # can't afford even one position

        gross = cand["pnl_per_contract"] * contracts
        # Commission is per leg, per side (round trip).
        comm = commission * cand.get("n_legs", 1) * contracts * 2.0
        net = gross - comm
        cost_basis = risk_per_contract * contracts
        return_pct = (net / cost_basis * 100.0) if cost_basis > 0 else None

        seq += 1
        trades.append(
            TradeResult(
                seq=seq,
                pattern=cand["card"].pattern,
                direction=cand["card"].direction,
                tier=cand["card"].tier,
                option_symbol=cand["option_symbol"],
                option_type=cand["option_type"],
                strike=cand["strike"],
                expiration=cand["expiration"],
                entered_at=cand["entered_at"],
                exited_at=cand["exited_at"],
                entry_premium=cand["entry_premium"],
                exit_premium=cand["exit_premium"],
                contracts=contracts,
                gross_pnl=gross,
                commission=comm,
                net_pnl=net,
                return_pct=return_pct,
                outcome=cand["outcome"],
                mfe_pct=cand["mfe_pct"],
                mae_pct=cand["mae_pct"],
                hold_minutes=cand["hold_minutes"],
                structure=cand.get("structure", "single"),
                legs=cand.get("legs", []),
            )
        )
        open_positions.append({"exit_at": cand["exited_at"], "net_pnl": net})

    # Close everything still open at the end of the window.
    if candidates:
        sentinel = datetime.max.replace(tzinfo=candidates[0]["exited_at"].tzinfo)
    else:
        sentinel = datetime.max
    _close_until(sentinel)

    summary = _summarize(trades, equity, capital)
    summary["diagnostics"] = {
        "concurrency_skipped": concurrency_skipped,
        "sized_out": sized_out,
    }
    return RunResult(trades=trades, equity=equity, summary=summary)


def _summarize(trades: list[TradeResult], equity: list[EquityPoint], capital: float) -> dict:
    n = len(trades)
    if n == 0:
        return {
            "n_trades": 0, "win_rate": None, "net_pnl": 0.0, "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0, "profit_factor": None, "avg_win_pct": None,
            "avg_loss_pct": None, "avg_hold_minutes": None, "by_pattern": [],
        }
    wins = [t for t in trades if t.net_pnl > 0]
    losses = [t for t in trades if t.net_pnl < 0]
    net_pnl = sum(t.net_pnl for t in trades)
    gross_win = sum(t.net_pnl for t in wins)
    gross_loss = abs(sum(t.net_pnl for t in losses))
    max_dd = min((p.drawdown_pct for p in equity), default=0.0)

    def _avg(seq):
        seq = list(seq)
        return sum(seq) / len(seq) if seq else None

    by_pattern: dict[str, dict] = {}
    for t in trades:
        b = by_pattern.setdefault(
            t.pattern, {"pattern": t.pattern, "n": 0, "wins": 0, "net_pnl": 0.0}
        )
        b["n"] += 1
        b["wins"] += 1 if t.net_pnl > 0 else 0
        b["net_pnl"] += t.net_pnl
    by_pattern_list = [
        {
            "pattern": b["pattern"],
            "n": b["n"],
            "win_rate": (b["wins"] / b["n"]) if b["n"] else None,
            "net_pnl": round(b["net_pnl"], 2),
        }
        for b in sorted(by_pattern.values(), key=lambda x: -x["net_pnl"])
    ]

    return {
        "n_trades": n,
        "win_rate": len(wins) / n,
        "net_pnl": round(net_pnl, 2),
        "total_return_pct": round(net_pnl / capital * 100.0, 2) if capital else None,
        "max_drawdown_pct": round(max_dd, 2),
        "profit_factor": round(gross_win / gross_loss, 3) if gross_loss > 0 else None,
        "avg_win_pct": round(
            _avg(t.return_pct for t in wins if t.return_pct is not None) or 0.0, 2
        ) if wins else None,
        "avg_loss_pct": round(
            _avg(t.return_pct for t in losses if t.return_pct is not None) or 0.0, 2
        ) if losses else None,
        "avg_hold_minutes": round(
            _avg(t.hold_minutes for t in trades if t.hold_minutes is not None) or 0.0, 1
        ),
        "by_pattern": by_pattern_list,
    }


def _apply_cooldown(cards: list, cooldown_minutes: int) -> list:
    """Collapse the continuous card stream to discrete per-pattern entries.

    Cards arrive ~every cycle, so without this a backtest would price (and the
    concurrency cap would then mostly discard) thousands of near-identical
    signals per day. Keeps the first card of each pattern, then suppresses any
    further card of that pattern until ``cooldown_minutes`` have elapsed.
    ``cooldown_minutes <= 0`` is a passthrough (price every card).
    """
    if cooldown_minutes <= 0:
        return list(cards)
    gap = timedelta(minutes=cooldown_minutes)
    last_kept: dict[str, datetime] = {}
    kept: list = []
    for card in sorted(cards, key=lambda c: c.timestamp):
        prev = last_kept.get(card.pattern)
        if prev is None or (card.timestamp - prev) >= gap:
            kept.append(card)
            last_kept[card.pattern] = card.timestamp
    return kept


def run_backtest(
    conn,
    spec: BacktestSpec,
    *,
    progress_cb: Optional[Callable[[float], None]] = None,
) -> RunResult:
    """Execute a backtest against ``conn`` and return the full result.

    ``progress_cb`` (if given) is invoked with a 0.0–1.0 fraction as Cards are
    priced, so the runner can persist progress for the UI poll loop.
    """
    start_dt = datetime.combine(spec.start_date, datetime.min.time())
    end_dt = datetime.combine(spec.end_date, datetime.max.time())
    if spec.strategy is not None:
        # Phase 3: custom strategy — synthesize cards from the condition rule.
        from src.backtesting.strategy import generate_strategy_cards

        max_hold = spec.exit.max_hold_minutes or _DEFAULT_MAX_HOLD_MIN
        all_cards = generate_strategy_cards(conn, spec, max_hold=max_hold)
        in_scope = all_cards
    else:
        all_cards = fetch_action_cards(conn, spec.underlying, start_dt, end_dt)
        if spec.patterns:
            wanted = set(spec.patterns)
            in_scope = [c for c in all_cards if c.pattern in wanted]
        else:
            in_scope = list(all_cards)
    cards = _apply_cooldown(in_scope, spec.cooldown_minutes)

    # Funnel diagnostics so a 0-trade run is explainable: where did cards go?
    diag = {
        "cards_total": len(all_cards),
        "cards_in_scope": len(in_scope),
        "cards_after_cooldown": len(cards),
        "drops": {},          # reason -> count (outcome:no_fill, no_entry_quote, …)
        "priced_candidates": 0,
    }

    candidates: list[dict] = []
    total = len(cards) or 1
    for i, card in enumerate(cards):
        try:
            cand, reason = _build_candidate(conn, _to_card_row(card), spec)
        except Exception:  # pragma: no cover - defensive; one bad Card must not kill the run
            logger.warning(
                "backtest: skipping card at %s due to pricing error",
                getattr(card, "timestamp", "?"),
                exc_info=True,
            )
            cand, reason = None, "error"
        if cand is not None:
            candidates.append(cand)
        else:
            diag["drops"][reason] = diag["drops"].get(reason, 0) + 1
        if progress_cb is not None and (i % 25 == 0 or i == total - 1):
            progress_cb((i + 1) / total)
    diag["priced_candidates"] = len(candidates)

    result = _simulate(candidates, spec)
    result.summary["diagnostics"] = {**diag, **result.summary.get("diagnostics", {})}
    if progress_cb is not None:
        progress_cb(1.0)
    return result
