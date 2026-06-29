"""Realized option-P&L feed for pattern calibration (the bridge).

The playbook calibration loop (``src/signals/playbook/calibration.py``)
historically fed only from the **underlying-touch** harness — "did the
underlying reach the target/stop?", a conservative proxy that ignores premium
decay, bid/ask, and commission and therefore overstates 0DTE edge.

This module is the realized-P&L counterpart. It runs the leg-level backtest
engine over a lookback window, aggregates per-pattern **win rate measured on
actual option P&L** (net of fills + slippage + commission), and writes the
result into ``playbook_pattern_stats`` tagged ``source='option_pnl'`` so the
live calibration store can be pointed at it (or auto-prefer it) via
``SIGNALS_PATTERN_CALIBRATION_SOURCE``.

It does NOT change the confidence formula or the live consult — it only adds a
second, more honest measurement the existing store already knows how to read.

Standardized measurement spec
-----------------------------
To make the win rate a property of the *pattern* (not of a sizing/structure
choice), the calibration run uses a fixed, deliberately permissive spec:

* **single-leg ATM** entries from the persisted Action Cards (no spreads),
* a **high concurrency cap and large capital** so neither the ``max_concurrent``
  gate nor sizing drops otherwise-valid entries — we want every cooled-down
  entry priced, to maximize the sample,
* the **same per-pattern cooldown** the customer backtester uses, so the stream
  collapses to discrete entries the same way,
* exits left to each Card's own target/stop levels (no premium overlay).

A trade is a **win** when its realized ``net_pnl > 0`` after commission. The
beta smoothing matches the underlying-touch harness so the two sources are
directly comparable.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pytz

from src import config
from src.backtesting.engine import run_backtest
from src.backtesting.models import BacktestSpec
from src.signals.playbook.backtest import _PRIOR_ALPHA, _PRIOR_BETA

logger = logging.getLogger(__name__)

SOURCE = "option_pnl"
_DEFAULT_DAYS = 60


@dataclass
class PnlPatternStat:
    """Per-pattern realized-P&L stats for one (underlying, window)."""

    pattern: str
    underlying: str
    window_start: date
    window_end: date
    n_trades: int = 0
    n_wins: int = 0
    n_losses: int = 0
    # Dollar economics — gross_win_pnl is the sum of winning trades' net_pnl
    # (always ≥ 0); gross_loss_pnl is the absolute sum of losing trades'
    # net_pnl (also ≥ 0). Net P&L = gross_win − gross_loss; profit factor =
    # gross_win / gross_loss; expectancy = (gross_win − gross_loss) / n_trades.
    # Stored so the insights endpoint can serve PF/expectancy from one DB
    # read without re-running the engine.
    gross_win_pnl: float = 0.0
    gross_loss_pnl: float = 0.0

    @property
    def hit_rate(self) -> float | None:
        return (self.n_wins / self.n_trades) if self.n_trades else None

    @property
    def proposed_base(self) -> float | None:
        """Beta-smoothed realized win rate (same prior as the touch harness).

        Returns None when no trade resolved. Deliberately NOT clamped to the
        catalog band — clamping belongs at the live consumer, so a genuinely
        losing pattern is recorded honestly here.
        """
        if self.n_trades == 0:
            return None
        wins = self.n_wins + _PRIOR_ALPHA
        total = self.n_trades + _PRIOR_ALPHA + _PRIOR_BETA
        return wins / total


def calibration_spec(
    underlying: str, window_start: date, window_end: date,
    *, patterns: list[str] | None = None,
    structure: str = "single", width_pct: float = 0.01,
) -> BacktestSpec:
    """Build the standardized single-leg measurement spec (see module docstring).

    ``patterns`` restricts the run to specific pattern ids (used by the
    single-pattern explain/drill-in); the default empty list scans all patterns.

    Disciplined-trade exits model the option's own premium: a standardized
    stop-loss (``SIGNALS_PATTERN_CALIBRATION_PNL_STOP_PCT``) cuts a decaying long
    rather than riding it to expiry, and a take-profit
    (``SIGNALS_PATTERN_CALIBRATION_PNL_TARGET_PCT``) books a winner that spikes
    then gives it back. Each card's own underlying target/stop still applies;
    whichever triggers first wins. Either knob set to 0 disables that side.
    """
    stop = config.SIGNALS_PATTERN_CALIBRATION_PNL_STOP_PCT
    target = config.SIGNALS_PATTERN_CALIBRATION_PNL_TARGET_PCT
    return BacktestSpec.from_dict(
        {
            "underlying": underlying,
            "start_date": window_start.isoformat(),
            "end_date": window_end.isoformat(),
            "patterns": list(patterns) if patterns else [],
            "structure": structure,
            "width_pct": width_pct,
            "fill_model": {"slippage_pct": 0.01, "commission_per_contract": 0.65},
            # Large capital + max concurrency so neither sizing nor the
            # concurrency cap drops entries — maximize the measured sample.
            "sizing": {
                "capital": 1_000_000.0,
                "risk_per_trade_pct": 1.0,
                "max_concurrent": 20,
            },
            # Keep each Card's own target/stop levels, but overlay a standardized
            # premium take-profit / stop so winners are booked and losers cut
            # instead of riding a near-dated long to full decay.
            "exit": {
                "max_hold_minutes": None,
                "stop_loss_pct": stop if stop and stop > 0 else None,
                "profit_target_pct": target if target and target > 0 else None,
            },
        }
    )


def _window(days: int) -> tuple[date, date]:
    """The (window_start, window_end) ET dates for a ``days``-back lookback."""
    et = pytz.timezone("America/New_York")
    end_dt = datetime.now(pytz.UTC)
    start_dt = end_dt - timedelta(days=days)
    return start_dt.astimezone(et).date(), end_dt.astimezone(et).date()


def aggregate_economics(trades) -> dict:
    """Per-pattern P&L economics: win rate, profit factor, expectancy, net P&L."""
    out: dict[str, dict] = {}
    for t in trades:
        e = out.setdefault(
            t.pattern, {"n": 0, "wins": 0, "net": 0.0, "gw": 0.0, "gl": 0.0}
        )
        e["n"] += 1
        e["net"] += t.net_pnl
        if t.net_pnl > 0:
            e["wins"] += 1
            e["gw"] += t.net_pnl
        else:
            e["gl"] += abs(t.net_pnl)
    for e in out.values():
        e["win_rate"] = e["wins"] / e["n"] if e["n"] else None
        e["pf"] = (e["gw"] / e["gl"]) if e["gl"] > 0 else float("inf")
        e["expectancy"] = e["net"] / e["n"] if e["n"] else 0.0
    return out


def run_structures(conn, *, underlying: str, days: int = _DEFAULT_DAYS,
                   structures=("single", "vertical")) -> dict:
    """Run the calibration backtest under each structure; return per-pattern
    economics keyed by structure name. One full backtest per structure (all
    patterns at once), so cost is len(structures) runs, not per-pattern.
    """
    window_start, window_end = _window(days)
    out: dict[str, dict] = {}
    for s in structures:
        spec = calibration_spec(underlying, window_start, window_end, structure=s)
        result = run_backtest(conn, spec)
        out[s] = aggregate_economics(result.trades)
    return out


def explain_trades(conn, *, underlying: str, pattern: str, days: int = _DEFAULT_DAYS):
    """Run the calibration backtest for ONE pattern and return its RunResult.

    Used by the drill-in report to inspect the actual per-trade P&L behind a
    pattern's measured base (e.g. confirming a touch-vs-P&L divergence is a real
    theta trap, not a pricing artifact).
    """
    window_start, window_end = _window(days)
    spec = calibration_spec(underlying, window_start, window_end, patterns=[pattern])
    return run_backtest(conn, spec)


def aggregate_trades(trades, *, underlying: str, window_start: date,
                     window_end: date) -> list[PnlPatternStat]:
    """Group engine trades by pattern into realized win/loss counts + economics."""
    by_pattern: dict[str, PnlPatternStat] = {}
    for t in trades:
        ps = by_pattern.setdefault(
            t.pattern,
            PnlPatternStat(
                pattern=t.pattern,
                underlying=underlying,
                window_start=window_start,
                window_end=window_end,
            ),
        )
        ps.n_trades += 1
        if t.net_pnl > 0:
            ps.n_wins += 1
            ps.gross_win_pnl += float(t.net_pnl)
        else:
            ps.n_losses += 1
            ps.gross_loss_pnl += abs(float(t.net_pnl))
    return list(by_pattern.values())


def upsert_pnl_stats(conn, stats) -> None:
    """Persist realized-P&L stats with ``source='option_pnl'``.

    Unmeasured columns (avg_confidence / avg_mfe_pct / avg_mae_pct) are written
    NULL: this feed measures realized P&L, not underlying excursions. The win /
    loss counts reuse the ``n_target_hit`` / ``n_stop_hit`` columns so the row
    shape stays uniform with the touch harness.
    """
    cur = conn.cursor()
    for s in stats:
        cur.execute(
            """
            INSERT INTO playbook_pattern_stats
                (pattern, underlying, window_start, window_end,
                 n_emitted, n_resolved, n_target_hit, n_stop_hit, n_time_exit,
                 hit_rate, avg_confidence, avg_mfe_pct, avg_mae_pct,
                 proposed_base, gross_win_pnl, gross_loss_pnl,
                 source, computed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    NULL, NULL, NULL, %s, %s, %s, %s, NOW())
            ON CONFLICT (pattern, underlying, window_start, window_end, source) DO UPDATE SET
                n_emitted      = EXCLUDED.n_emitted,
                n_resolved     = EXCLUDED.n_resolved,
                n_target_hit   = EXCLUDED.n_target_hit,
                n_stop_hit     = EXCLUDED.n_stop_hit,
                n_time_exit    = EXCLUDED.n_time_exit,
                hit_rate       = EXCLUDED.hit_rate,
                proposed_base  = EXCLUDED.proposed_base,
                gross_win_pnl  = EXCLUDED.gross_win_pnl,
                gross_loss_pnl = EXCLUDED.gross_loss_pnl,
                computed_at    = NOW()
            """,
            (
                s.pattern,
                s.underlying,
                s.window_start,
                s.window_end,
                s.n_trades,       # n_emitted (every priced entry)
                s.n_trades,       # n_resolved
                s.n_wins,         # n_target_hit ⇒ realized winners
                s.n_losses,       # n_stop_hit   ⇒ realized losers
                0,                # n_time_exit (not distinguished here)
                s.hit_rate,
                s.proposed_base,
                s.gross_win_pnl,
                s.gross_loss_pnl,
                SOURCE,
            ),
        )
    conn.commit()


def run(*, underlying: str, days: int = _DEFAULT_DAYS, conn=None,
        write: bool = True) -> list[PnlPatternStat]:
    """End-to-end: run the standardized backtest, aggregate, persist.

    Opens its own connection when ``conn`` is None. ``write=False`` returns the
    stats without persisting (tests / dry runs).
    """
    window_start, window_end = _window(days)

    if conn is None:
        from src.database.connection import db_connection

        with db_connection() as local_conn:
            return _run_with_conn(local_conn, underlying, window_start, window_end, write)
    return _run_with_conn(conn, underlying, window_start, window_end, write)


def _run_with_conn(conn, underlying, window_start, window_end, write) -> list[PnlPatternStat]:
    spec = calibration_spec(underlying, window_start, window_end)
    result = run_backtest(conn, spec)
    stats = aggregate_trades(
        result.trades, underlying=underlying,
        window_start=window_start, window_end=window_end,
    )
    logger.info(
        "option_pnl calibration: %s [%s→%s] %d patterns, %d trades",
        underlying, window_start, window_end, len(stats),
        sum(s.n_trades for s in stats),
    )
    if write and stats:
        upsert_pnl_stats(conn, stats)
    return stats
