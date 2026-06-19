"""Backtest simulation engine.

Replays persisted Action Cards over a historical window and prices each one as
a real option-leg round trip. The v1 fidelity model (see
``docs/design/backtesting-platform.md`` §2) is deliberately split:

* **Exit *timing*** is resolved on the underlying series by reusing
  ``src.signals.playbook.backtest.compute_outcome`` — the proven intrabar
  MFE/MAE + entry-trigger-fill logic.
* **P&L** is priced from the chosen option leg's bid/ask via
  ``src.signals.execution.leg_fill_price`` — long bought at ask·(1+slip) on
  entry, sold at bid·(1−slip) on exit, plus per-contract commission both ways.

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
    CardRow,
    compute_outcome,
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


def _exit_timestamp(outcome) -> Optional[datetime]:
    return outcome.target_hit_at or outcome.stop_hit_at or outcome.expired_at


def _build_candidate(
    conn,
    card: CardRow,
    spec: BacktestSpec,
) -> Optional[dict]:
    """Resolve one Card into a priced candidate trade, or None if unusable.

    Computes exit timing on the underlying, then prices the option leg at
    entry and exit. Returns a dict with everything except final sizing (which
    needs the chronological equity walk).
    """
    payload = dict(card.payload or {})
    # Apply the spec's max-hold override (or supply a default) so cards that
    # never recorded one are still resolvable.
    if spec.exit.max_hold_minutes is not None:
        payload = {**payload, "max_hold_minutes": spec.exit.max_hold_minutes}
    elif not payload.get("max_hold_minutes"):
        payload = {**payload, "max_hold_minutes": _DEFAULT_MAX_HOLD_MIN}
    card = CardRow(
        underlying=card.underlying,
        timestamp=card.timestamp,
        pattern=card.pattern,
        action=card.action,
        tier=card.tier,
        direction=card.direction,
        confidence=card.confidence,
        payload=payload,
    )

    max_hold = int(payload.get("max_hold_minutes") or _DEFAULT_MAX_HOLD_MIN)
    quotes = fetch_quotes(
        conn, card.underlying, card.timestamp, card.timestamp + timedelta(minutes=max_hold)
    )
    outcome = compute_outcome(card, quotes)
    # Only price trades that actually opened and resolved to a price exit.
    if outcome.outcome not in ("target_hit", "stop_hit", "time_exit"):
        return None
    exit_at = _exit_timestamp(outcome)
    if exit_at is None:
        return None

    leg = _select_leg(card)
    if leg is None:
        return None

    entry_q = _fetch_leg_quote(conn, underlying=card.underlying, leg=leg, at=card.timestamp)
    if entry_q is None:
        return None
    # Lock the leg to the contract we actually entered so the exit prices the
    # same option (important for the synthetic-ATM fallback).
    resolved_leg = {
        "expiry": entry_q["expiration"],
        "strike": entry_q["strike"],
        "right": entry_q["option_type"],
    }
    exit_q = _fetch_leg_quote(conn, underlying=card.underlying, leg=resolved_leg, at=exit_at)
    if exit_q is None:
        return None

    slip = spec.fill_model.slippage_pct
    entry_premium = leg_fill_price(
        bid=entry_q["bid"], ask=entry_q["ask"], last=entry_q["last"],
        side="long", action="open", slippage_pct=slip,
    )
    exit_premium = leg_fill_price(
        bid=exit_q["bid"], ask=exit_q["ask"], last=exit_q["last"],
        side="long", action="close", slippage_pct=slip,
    )
    if entry_premium <= 0:
        return None

    hold_minutes = max(0, int((exit_at - card.timestamp).total_seconds() // 60))
    return {
        "card": card,
        "outcome": outcome,
        "entered_at": card.timestamp,
        "exited_at": exit_at,
        "option_symbol": entry_q["option_symbol"],
        "option_type": entry_q["option_type"],
        "strike": entry_q["strike"],
        "expiration": entry_q["expiration"],
        "entry_premium": entry_premium,
        "exit_premium": exit_premium,
        "pnl_per_contract": (exit_premium - entry_premium) * 100.0,
        "hold_minutes": hold_minutes,
        "mfe_pct": outcome.mfe_pct,
        "mae_pct": outcome.mae_pct,
    }


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
            continue  # concurrency cap reached; skip this signal

        entry_premium = cand["entry_premium"]
        per_contract_cost = entry_premium * 100.0
        if per_contract_cost <= 0:
            continue
        # Allocate risk_frac of *currently realized* equity as premium.
        risk_dollars = max(realized_equity, 0.0) * risk_frac
        contracts = int(math.floor(risk_dollars / per_contract_cost))
        contracts = max(contracts, 1)
        # Never spend more premium than we have on hand.
        if per_contract_cost * contracts > max(realized_equity, 0.0):
            contracts = int(math.floor(max(realized_equity, 0.0) / per_contract_cost))
        if contracts < 1:
            continue  # can't afford even one contract

        gross = cand["pnl_per_contract"] * contracts
        comm = commission * contracts * 2.0  # round trip
        net = gross - comm
        cost_basis = per_contract_cost * contracts
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
                entry_premium=entry_premium,
                exit_premium=cand["exit_premium"],
                contracts=contracts,
                gross_pnl=gross,
                commission=comm,
                net_pnl=net,
                return_pct=return_pct,
                outcome=cand["outcome"].outcome,
                mfe_pct=cand["mfe_pct"],
                mae_pct=cand["mae_pct"],
                hold_minutes=cand["hold_minutes"],
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
    cards = fetch_action_cards(conn, spec.underlying, start_dt, end_dt)
    if spec.patterns:
        wanted = set(spec.patterns)
        cards = [c for c in cards if c.pattern in wanted]

    candidates: list[dict] = []
    total = len(cards) or 1
    for i, card in enumerate(cards):
        try:
            cand = _build_candidate(conn, _to_card_row(card), spec)
        except Exception:  # pragma: no cover - defensive; one bad Card must not kill the run
            logger.warning(
                "backtest: skipping card at %s due to pricing error",
                getattr(card, "timestamp", "?"),
                exc_info=True,
            )
            cand = None
        if cand is not None:
            candidates.append(cand)
        if progress_cb is not None and (i % 25 == 0 or i == total - 1):
            progress_cb((i + 1) / total)

    result = _simulate(candidates, spec)
    if progress_cb is not None:
        progress_cb(1.0)
    return result
