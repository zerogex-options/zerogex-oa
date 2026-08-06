"""Fleet performance TREND — the derivative, not the since-inception level.

The dashboard hero (``/tradeworkz/summary``) reports
``current_capital / starting_capital - 1`` — cumulative return since inception.
That number is anchored to the pre-fix drawdown (and to any large loss that
settled late, e.g. an overnight stranded-position cleanup landing in the next
session), so it stays deep red for a long time even while the fleet is
improving. It answers "how far underwater are we in total?" — never "is
performance getting better?"

This module turns the per-session trade ledger into the trend view: per-session
P&L, a cumulative line rebased to the window start (an "epoch", not inception),
and rolling-window win rate / profit factor / expectancy so the slope is
visible. It is pure (no DB, no I/O): the router fetches rows and hands them
here, which makes the rolling math unit-testable. See
``/tradeworkz/performance-trend``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence


def _win_rate(wins: int, trades: int) -> Optional[float]:
    return (wins / trades) if trades > 0 else None


def _profit_factor(gross_win: float, gross_loss: float) -> Optional[float]:
    # gross_loss is a POSITIVE magnitude of losing P&L. Undefined with no
    # losses (division by zero) — a window of only winners has no ratio, so
    # None is truthfully "n/a", not "infinitely good".
    return (gross_win / gross_loss) if gross_loss > 0 else None


def _expectancy(realized: float, trades: int) -> Optional[float]:
    # Average realized $ per trade over the window — the single number that
    # says "is each trade, on average, making or losing money?"
    return (realized / trades) if trades > 0 else None


def _rolling(sessions: Sequence[Dict[str, Any]], end_idx: int, window: int) -> Dict[str, Any]:
    """Aggregate the trailing ``window`` sessions ending at ``end_idx`` (inclusive)."""
    start = max(0, end_idx - window + 1)
    realized = gross_win = gross_loss = 0.0
    trades = wins = 0
    for s in sessions[start : end_idx + 1]:
        realized += float(s["realized_pnl"])
        trades += int(s["trades"])
        wins += int(s["wins"])
        gross_win += float(s["gross_win"])
        gross_loss += float(s["gross_loss"])
    return {
        "sessions": end_idx - start + 1,
        "realized_pnl": round(realized, 2),
        "trades": trades,
        "win_rate": _win_rate(wins, trades),
        "profit_factor": _profit_factor(gross_win, gross_loss),
        "expectancy": _expectancy(realized, trades),
    }


def build_trend(
    daily: Sequence[Dict[str, Any]],
    *,
    spy_closes: Optional[Dict[Any, float]] = None,
    windows: Sequence[int] = (5, 10, 20),
    fleet_start_capital: float = 0.0,
) -> Dict[str, Any]:
    """Build the fleet trend payload from per-session aggregates.

    ``daily`` — one dict per ET session, ASCENDING by date, each with:
    ``session_date``, ``realized_pnl``, ``trades``, ``wins``, ``losses``,
    ``gross_win`` (sum of winning P&L), ``gross_loss`` (POSITIVE sum of losing
    P&L). ``spy_closes`` maps session_date -> SPY close for the buy-hold
    benchmark. Both cumulative lines (fleet, SPY) are rebased to the FIRST
    session in ``daily`` so the chart shows performance over the window, not
    since inception.

    Returns ``{series, headline, benchmark, windows}``. ``headline`` carries the
    latest value of each rolling window for the hero row; that is what should
    replace the since-inception NAV as the primary "is it improving" number.
    """
    windows = [w for w in windows if w >= 1]
    spy_closes = spy_closes or {}
    series: List[Dict[str, Any]] = []
    cumulative = 0.0
    spy_anchor: Optional[float] = None

    for i, s in enumerate(daily):
        realized = float(s["realized_pnl"])
        cumulative += realized
        spy_close = spy_closes.get(s["session_date"])
        if spy_anchor is None and spy_close is not None:
            spy_anchor = spy_close
        spy_ret = (
            (spy_close / spy_anchor - 1.0)
            if (spy_close is not None and spy_anchor)
            else None
        )
        point: Dict[str, Any] = {
            "session_date": s["session_date"],
            "realized_pnl": round(realized, 2),
            "trades": int(s["trades"]),
            "wins": int(s["wins"]),
            "losses": int(s["losses"]),
            "cumulative_pnl": round(cumulative, 2),
            "fleet_cum_return_pct": (
                cumulative / fleet_start_capital if fleet_start_capital > 0 else None
            ),
            "spy_close": spy_close,
            "spy_cum_return_pct": spy_ret,
            "rolling": {str(w): _rolling(daily, i, w) for w in windows},
        }
        series.append(point)

    headline: Dict[str, Any] = {}
    if series:
        last = series[-1]
        headline = {
            "as_of": last["session_date"],
            "windows": {w: last["rolling"][str(w)] for w in map(str, windows)},
            "fleet_cum_return_pct": last["fleet_cum_return_pct"],
            "spy_cum_return_pct": last["spy_cum_return_pct"],
            "cumulative_pnl": last["cumulative_pnl"],
        }
    return {
        "series": series,
        "headline": headline,
        "windows": list(windows),
    }
