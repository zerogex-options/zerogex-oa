"""Measure the PutWallMagnetReversal thesis on historical data.

The question the bot rests on: in a negative-gamma regime, when a put wall
is *historically massive* and spot is pressed just above it, does price
bounce to the gamma flip more often than it cascades through the wall — and
does a bigger wall bounce more reliably?

There is no live-engine replay, so this reconstructs the bot's entry gates
from the historical ``gex_summary`` + ``gex_by_strike`` + ``underlying_quotes``
tables and measures the forward outcome of every qualifying minute. It is
faithful where it counts — it builds a real :class:`MarketSnapshot` per bar
and calls the SAME ``gex_regime`` / ``distance_to_put_wall_pct`` the live bot
uses — and an approximation where it must be: a per-symbol percentile stands
in for the live 30d time-of-day-bucketed one, and outcomes are level-touches
on the 1-minute close series rather than tick-confirmed option fills. That is
enough to decide whether the edge is real before the bot is enabled.

Outcome per qualifying entry, over the next ``max_hold_minutes``:
  * WIN  — a close reaches the target (gamma flip) first,
  * LOSS — a close breaks the stop (put_wall x (1 - stop_buffer)) first,
  * TIMEOUT — neither; marked to the final close.
R is expressed in units of risk (entry - stop): a win to target is
+reward/risk R, a stop is -1R.

Usage:
    python -m src.tools.put_wall_magnet_backtest --symbols SPY QQQ --days 60
    python -m src.tools.put_wall_magnet_backtest --min-pctile 95 --days 90
"""

from __future__ import annotations

import argparse
import logging
import sys
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Sequence, Tuple

import numpy as np

from src.database.connection import db_connection
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.gex_stats import percentile_rank

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gate parameters — mirror PutWallMagnetReversal._DEFAULTS.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class GateParams:
    min_put_wall_pctile: float = 90.0
    wall_proximity_pct: float = 0.002
    wall_collapse_pct: float = 0.003
    min_minutes_since_open: float = 30.0
    stop_buffer_pct: float = 0.0015  # = wall_break_min_pct
    max_hold_minutes: int = 45


@dataclass
class Bar:
    """One historical minute of the features the gates need."""

    ts: datetime
    spot: float
    net_gex: Optional[float]
    gamma_flip: Optional[float]
    max_pain: Optional[float]
    put_wall: Optional[float]
    put_gamma_at_wall: float
    prior_put_wall: Optional[float] = None
    put_wall_strength: Optional[float] = None
    put_wall_pctile: Optional[float] = None
    net_gex_pctile: Optional[float] = None


@dataclass
class TradeResult:
    ts: datetime
    entry_spot: float
    target: float
    stop: float
    outcome: str  # 'win' | 'loss' | 'timeout'
    r_multiple: float
    put_wall_pctile: float


# ---------------------------------------------------------------------------
# Pure analysis helpers (unit-tested).
# ---------------------------------------------------------------------------
def dollar_wall_strength(put_gamma_at_wall: float, spot: float) -> float:
    """Dollar-gamma at the put wall — the walls.py convention (gamma x100xS^2x.01)."""
    return abs(put_gamma_at_wall) * 100.0 * spot * spot * 0.01


def distribution_knots(
    values: Sequence[Optional[float]], min_samples: int = 20
) -> Optional[Tuple[float, float, float, float, float]]:
    """The p05/p25/p50/p75/p95 knots percentile_rank expects, or None if thin."""
    arr = np.asarray(
        [float(v) for v in values if v is not None and np.isfinite(float(v))],
        dtype=float,
    )
    if arr.size < min_samples:
        return None
    p = [float(np.percentile(arr, q)) for q in (5, 25, 50, 75, 95)]
    return (p[0], p[1], p[2], p[3], p[4])


def forward_outcome(
    entry_spot: float,
    target: float,
    stop: float,
    forward_closes: Sequence[float],
) -> Tuple[str, float]:
    """Classify a bounce trade against its forward close series.

    First touch wins: a close >= target -> win; a close <= stop -> loss;
    neither within the horizon -> timeout, marked to the final close. R is in
    units of risk = entry - stop (a stop is exactly -1R).
    """
    risk = entry_spot - stop
    if risk <= 0:
        return "invalid", 0.0
    for close in forward_closes:
        if close >= target:
            return "win", (target - entry_spot) / risk
        if close <= stop:
            return "loss", -1.0
    final = forward_closes[-1] if forward_closes else entry_spot
    return "timeout", (final - entry_spot) / risk


def qualifies(snap: MarketSnapshot, gate: GateParams) -> bool:
    """The bot's entry gates, reusing the live snapshot methods.

    Mirrors PutWallMagnetReversal.open_criteria's decision (regime, minutes,
    massive-enough, proximity/approach-from-above, wall-collapse) without the
    pricing/leg-build the backtest doesn't need.
    """
    if snap.gex_regime() != "negative_weak":
        return False
    m = snap.minutes_since_open
    if m is None or m < gate.min_minutes_since_open:
        return False
    if snap.put_wall is None or snap.spot <= 0:
        return False
    if snap.put_wall_strength_pctile is None:
        return False
    if snap.put_wall_strength_pctile < gate.min_put_wall_pctile:
        return False
    put_d = snap.distance_to_put_wall_pct()
    if put_d is None or put_d < 0.0 or put_d > gate.wall_proximity_pct:
        return False
    if snap.spot < snap.put_wall:
        return False
    if snap.prior_put_wall is not None and snap.put_wall < snap.prior_put_wall * (
        1.0 - gate.wall_collapse_pct
    ):
        return False
    return True


def target_for(snap: MarketSnapshot) -> float:
    """Bounce target — the gamma flip, falling back like the live bot."""
    if snap.gamma_flip is not None and snap.gamma_flip > snap.spot:
        return snap.gamma_flip
    if snap.max_pain is not None and snap.max_pain > snap.spot:
        return snap.max_pain
    return snap.spot * 1.003


def summarize(trades: Sequence[TradeResult]) -> dict:
    """Overall + by-percentile-bucket expectancy. The bucket view is the
    thesis test: does a *bigger* wall bounce more reliably?"""

    def _block(rows: Sequence[TradeResult]) -> dict:
        n = len(rows)
        if n == 0:
            return {"n": 0}
        wins = [r for r in rows if r.outcome == "win"]
        losses = [r for r in rows if r.outcome == "loss"]
        rs = [r.r_multiple for r in rows]
        return {
            "n": n,
            "win_rate": len(wins) / n,
            "loss_rate": len(losses) / n,
            "timeout_rate": sum(1 for r in rows if r.outcome == "timeout") / n,
            "expectancy_r": sum(rs) / n,
            "avg_win_r": (sum(r.r_multiple for r in wins) / len(wins)) if wins else 0.0,
        }

    buckets = [(90.0, 95.0), (95.0, 99.0), (99.0, 100.01)]
    by_bucket = {
        f"{lo:.0f}-{hi:.0f}": _block([t for t in trades if lo <= t.put_wall_pctile < hi])
        for lo, hi in buckets
    }
    return {"overall": _block(trades), "by_pctile": by_bucket}


# ---------------------------------------------------------------------------
# Series assembly (pure given loaded rows) + DB loading.
# ---------------------------------------------------------------------------
def enrich(bars: List[Bar]) -> List[Bar]:
    """Fill prior_put_wall (lag), put_wall_strength, and the two percentiles."""
    for i, b in enumerate(bars):
        b.prior_put_wall = bars[i - 1].put_wall if i > 0 else None
        if b.put_wall is not None and b.spot > 0:
            b.put_wall_strength = dollar_wall_strength(b.put_gamma_at_wall, b.spot)
    ws_knots = distribution_knots([b.put_wall_strength for b in bars])
    ng_knots = distribution_knots([b.net_gex for b in bars])
    for b in bars:
        if ws_knots and b.put_wall_strength is not None:
            b.put_wall_pctile = percentile_rank(b.put_wall_strength, *ws_knots)
        if ng_knots and b.net_gex is not None:
            b.net_gex_pctile = percentile_rank(b.net_gex, *ng_knots)
    return bars


def run(
    symbol: str, bars: List[Bar], closes: List[Tuple[datetime, float]], gate: GateParams
) -> List[TradeResult]:
    """Reconstruct entries over ``bars`` and measure each against ``closes``.

    ``closes`` is the (timestamp, close) series for the symbol, ascending —
    the single source of spot: each bar's spot is the nearest-preceding
    close (what the live snapshot reads), and the same series supplies the
    forward window for each entry.
    """
    close_ts = [c[0] for c in closes]
    close_px = [c[1] for c in closes]
    # Spot = nearest-preceding close. Done before enrich so the dollar-gamma
    # scale and the percentiles are computed on the real spot.
    for b in bars:
        idx = bisect_right(close_ts, b.ts) - 1
        b.spot = close_px[idx] if idx >= 0 else 0.0
    enrich(bars)
    results: List[TradeResult] = []
    for b in bars:
        snap = MarketSnapshot(
            underlying=symbol,
            timestamp=b.ts,
            spot=b.spot,
            net_gex=b.net_gex,
            net_gex_pctile=b.net_gex_pctile,
            gamma_flip=b.gamma_flip,
            max_pain=b.max_pain,
            put_wall=b.put_wall,
            prior_put_wall=b.prior_put_wall,
            put_wall_strength=b.put_wall_strength,
            put_wall_strength_pctile=b.put_wall_pctile,
        )
        if not qualifies(snap, gate):
            continue
        target = target_for(snap)
        stop = b.put_wall * (1.0 - gate.stop_buffer_pct)
        # Forward window: closes strictly after entry, up to max_hold minutes.
        lo = bisect_right(close_ts, b.ts)
        window = [
            close_px[j]
            for j in range(lo, len(close_ts))
            if (close_ts[j] - b.ts).total_seconds() <= gate.max_hold_minutes * 60
        ]
        if not window:
            continue
        outcome, r = forward_outcome(b.spot, target, stop, window)
        if outcome == "invalid":
            continue
        results.append(
            TradeResult(b.ts, b.spot, target, stop, outcome, r, b.put_wall_pctile or 0.0)
        )
    return results


def _load_bars(conn: Any, symbol: str, days: int) -> List[Bar]:
    """Load the negative-γ gex_summary bars + the put-gamma at each wall.

    Set-based (not a per-row correlated subquery): the wall's put-gamma is a
    single indexed JOIN of gex_by_strike onto the candidate timestamps, and
    only negative-net-gamma rows are considered (the only ones that can
    qualify), which is what keeps a 90-day scan under the statement timeout.
    Spot is NOT read here — run() aligns it from the closes series.
    """
    cur = conn.cursor()
    cur.execute(
        """
        WITH gs AS (
            SELECT underlying, timestamp, net_gex_at_spot, gamma_flip_point,
                   max_pain, put_wall
            FROM gex_summary
            WHERE underlying = %s
              AND timestamp >= NOW() - (%s || ' days')::interval
              AND put_wall IS NOT NULL
              AND net_gex_at_spot < 0
        ),
        wall_gamma AS (
            SELECT gbs.timestamp AS ts, SUM(gbs.put_gamma) AS pg
            FROM gex_by_strike gbs
            JOIN gs
              ON gs.underlying = gbs.underlying
             AND gs.timestamp = gbs.timestamp
             AND gbs.strike = gs.put_wall
            GROUP BY gbs.timestamp
        )
        SELECT gs.timestamp, gs.net_gex_at_spot, gs.gamma_flip_point,
               gs.max_pain, gs.put_wall, COALESCE(wg.pg, 0)
        FROM gs
        LEFT JOIN wall_gamma wg ON wg.ts = gs.timestamp
        ORDER BY gs.timestamp
        """,
        (symbol, str(days)),
    )
    bars: List[Bar] = []
    for r in cur.fetchall():
        bars.append(
            Bar(
                ts=r[0],
                spot=0.0,  # filled from the closes series in run()
                net_gex=float(r[1]) if r[1] is not None else None,
                gamma_flip=float(r[2]) if r[2] is not None else None,
                max_pain=float(r[3]) if r[3] is not None else None,
                put_wall=float(r[4]) if r[4] is not None else None,
                put_gamma_at_wall=float(r[5] or 0.0),
            )
        )
    return bars


def _load_closes(conn: Any, symbol: str, days: int) -> List[Tuple[datetime, float]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, close FROM underlying_quotes
        WHERE symbol = %s AND timestamp >= NOW() - (%s || ' days')::interval
          AND close IS NOT NULL
        ORDER BY timestamp
        """,
        (symbol, str(days)),
    )
    return [(r[0], float(r[1])) for r in cur.fetchall()]


def _print_report(symbol: str, stats: dict) -> None:
    o = stats["overall"]
    print(f"\n=== {symbol} ===")
    if o["n"] == 0:
        print("  no qualifying setups (need negative_weak γ + a top-pctile put wall)")
        return
    print(
        f"  overall: n={o['n']}  win={o['win_rate']:.1%}  loss={o['loss_rate']:.1%}"
        f"  timeout={o['timeout_rate']:.1%}  expectancy={o['expectancy_r']:+.2f}R"
        f"  avg_win={o['avg_win_r']:.2f}R"
    )
    print("  by put-wall percentile (does a bigger wall bounce better?):")
    for label, blk in stats["by_pctile"].items():
        if blk["n"] == 0:
            print(f"    {label:>7}: n=0")
        else:
            print(
                f"    {label:>7}: n={blk['n']:<4} win={blk['win_rate']:.1%}"
                f"  expectancy={blk['expectancy_r']:+.2f}R"
            )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--symbols", nargs="*", default=["SPY", "QQQ", "IWM"])
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--min-pctile", type=float, default=90.0)
    parser.add_argument("--proximity", type=float, default=0.002)
    parser.add_argument("--stop-buffer", type=float, default=0.0015)
    parser.add_argument("--max-hold", type=int, default=45)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    gate = GateParams(
        min_put_wall_pctile=args.min_pctile,
        wall_proximity_pct=args.proximity,
        stop_buffer_pct=args.stop_buffer,
        max_hold_minutes=args.max_hold,
    )
    with db_connection() as conn:
        # This is a one-shot analytical read over a wide gex_by_strike scan;
        # lift the app's 90s statement timeout for this session so a long
        # lookback isn't cut off. Best-effort — ignored if the role can't set
        # it. The process exits after, so the pooled connection's override
        # never affects request traffic.
        try:
            with conn.cursor() as c:
                c.execute("SET statement_timeout = 600000")  # 10 minutes (ms)
        except Exception:
            logger.debug("could not raise statement_timeout", exc_info=True)
        for symbol in (s.upper() for s in args.symbols):
            bars = _load_bars(conn, symbol, args.days)
            closes = _load_closes(conn, symbol, args.days)
            if not bars or not closes:
                print(f"\n=== {symbol} ===\n  no data in the last {args.days}d")
                continue
            trades = run(symbol, bars, closes, gate)
            _print_report(symbol, summarize(trades))
    print(
        "\nNote: per-symbol percentile + level-touch outcomes approximate the "
        "live bot (30d TOD-bucketed percentile, tick-confirmed fills). Treat "
        "the by-percentile trend, not the absolute win-rate, as the signal.\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
