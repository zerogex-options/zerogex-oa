"""Score the signed Trade Bias direction against forward returns.

The override sweep (:mod:`trade_bias_override_sweep`) measures only
override-state rows. This module scores the GENERAL signed ``bias_score`` — the
-100..+100 continuous read persisted every cycle — by how often
``sign(bias_score)`` matches the underlying's forward move, stratified by the
0-100 confidence. It answers the plain question a trader asks: *"when Trade Bias
leans long or short, is it right more than half the time, and by how much?"*

Like the override sweep it reads only ``trade_bias_scores`` — each row's payload
already carries ``bias_score``, ``confidence`` and the underlying ``close`` — so
no market feed or engine run is required. It reuses the override sweep's
forward-return helpers so the session-boundary handling stays in one place.

Run::

    python -m src.backtesting.trade_bias_direction_sweep \\
        --symbol SPY --tenor swing --days 30 --horizons 15,30,60

Read ``hit_rate`` (fraction of leans that went the right way; > 0.50 beats a
coin flip) and ``edge_bps`` (mean signed forward move; > 0 = right more than
wrong). A well-calibrated signal shows hit_rate and edge RISING with the
confidence bucket.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from src.backtesting.trade_bias_override_sweep import (
    _forward_returns,
    _parse_ts,
    count_with_forward,
)

# |bias_score| at or below this is treated as flat (matches the engine's
# direction-label deadband, TRADE_BIAS ``_DIR_DEADBAND`` = 1.0), so a hair of
# noise around zero isn't scored as a real lean.
_DEFAULT_DEADBAND = 1.0

# Confidence buckets (0-100). Half-open [lo, hi); the top bucket includes 100.
_BUCKETS: tuple[tuple[float, float, str], ...] = (
    (0.0, 25.0, "0-25"),
    (25.0, 50.0, "25-50"),
    (50.0, 75.0, "50-75"),
    (75.0, 100.01, "75-100"),
)
_ALL = "ALL"


@dataclass
class DirRow:
    """The fields the direction sweep needs from one ``trade_bias_scores`` payload."""

    timestamp: datetime
    close: Optional[float]
    bias_score: Optional[float]  # signed [-100, 100]
    confidence: Optional[float]  # 0-100 (headline confidence)


@dataclass
class DirectionStat:
    horizon_min: int
    bucket: str  # confidence bucket label, or "ALL"
    n_scored: int  # directional rows with a valid forward return
    hit_rate: Optional[float]  # fraction whose forward move matched the lean
    avg_edge_bps: Optional[float]  # mean(forward_return * sign(bias_score)) in bps
    avg_abs_move_bps: Optional[float]  # mean |forward_return| on scored rows, bps


def _bucket_for(conf: Optional[float]) -> Optional[str]:
    if conf is None:
        return None
    for lo, hi, label in _BUCKETS:
        if lo <= conf < hi:
            return label
    return None


def evaluate_direction(
    rows: list[DirRow],
    horizons_min: list[int],
    deadband: float = _DEFAULT_DEADBAND,
) -> list[DirectionStat]:
    """Score ``sign(bias_score)`` vs forward return, per horizon, per confidence bucket.

    Pure / DB-free so it is unit-testable. A row counts when it has a valid
    forward return AND ``|bias_score| > deadband``; the return is signed by the
    lean, and a hit is ``edge > 0``. Every scored row also lands in the ``ALL``
    bucket, so ``ALL`` is the headline hit-rate and the numbered buckets show
    whether higher confidence actually earns a higher hit-rate.
    """
    rows = sorted(rows, key=lambda r: r.timestamp)
    fwd = _forward_returns(rows, horizons_min)

    labels = [b[2] for b in _BUCKETS] + [_ALL]
    stats: list[DirectionStat] = []
    for h in horizons_min:
        returns = fwd[h]
        acc: dict[str, dict[str, float]] = {
            lbl: {"hits": 0.0, "edge": 0.0, "abs": 0.0, "n": 0.0} for lbl in labels
        }
        for i, r in enumerate(rows):
            ret = returns[i]
            if ret is None or r.bias_score is None or abs(r.bias_score) <= deadband:
                continue
            sign = 1.0 if r.bias_score > 0 else -1.0
            edge = ret * sign
            bkt = _bucket_for(r.confidence)
            for lbl in (_ALL, bkt) if bkt else (_ALL,):
                a = acc[lbl]
                a["n"] += 1
                a["edge"] += edge
                a["abs"] += abs(ret)
                if edge > 0:
                    a["hits"] += 1
        for lbl in labels:
            a = acc[lbl]
            n = int(a["n"])
            stats.append(
                DirectionStat(
                    horizon_min=h,
                    bucket=lbl,
                    n_scored=n,
                    hit_rate=(a["hits"] / n) if n else None,
                    avg_edge_bps=(a["edge"] / n * 1e4) if n else None,
                    avg_abs_move_bps=(a["abs"] / n * 1e4) if n else None,
                )
            )
    return stats


# --------------------------------------------------------------------------
# DB loading + CLI
# --------------------------------------------------------------------------

def _row_from_payload(ts: Any, payload: Any) -> Optional[DirRow]:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return None
    if not isinstance(payload, dict):
        return None
    tstamp = _parse_ts(payload.get("timestamp")) or _parse_ts(ts)
    if tstamp is None:
        return None
    return DirRow(
        timestamp=tstamp,
        close=payload.get("close"),
        bias_score=payload.get("bias_score"),
        confidence=payload.get("confidence"),
    )


def load_rows(symbol: str, tenor: str, days: int) -> list[DirRow]:
    from src.database import db_connection

    query = """
        SELECT timestamp, payload
        FROM trade_bias_scores
        WHERE underlying = %s AND tenor = %s
          AND timestamp >= NOW() - make_interval(days => %s)
        ORDER BY timestamp ASC
    """
    rows: list[DirRow] = []
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, (symbol.upper(), tenor, days))
        for ts, payload in cur.fetchall():
            row = _row_from_payload(ts, payload)
            if row is not None:
                rows.append(row)
    return rows


def _format_table(stats: list[DirectionStat]) -> str:
    header = (
        f"{'horizon':>8} {'confidence':>11} {'n':>7} {'hit_rate':>9} "
        f"{'edge_bps':>9} {'abs_bps':>9}"
    )
    lines = [header, "-" * len(header)]
    for s in stats:
        hit = f"{s.hit_rate * 100:6.1f}%" if s.hit_rate is not None else "     --"
        edge = f"{s.avg_edge_bps:9.2f}" if s.avg_edge_bps is not None else "       --"
        absm = f"{s.avg_abs_move_bps:9.2f}" if s.avg_abs_move_bps is not None else "       --"
        lines.append(
            f"{s.horizon_min:>7}m {s.bucket:>11} {s.n_scored:>7} {hit:>9} {edge} {absm}"
        )
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Score the signed Trade Bias direction against forward returns."
    )
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--tenor", default="swing", choices=["swing", "intraday"])
    parser.add_argument("--days", type=int, default=30, help="lookback window (calendar days)")
    parser.add_argument("--horizons", default="15,30,60",
                        help="comma-separated forward horizons in minutes")
    parser.add_argument("--deadband", type=float, default=_DEFAULT_DEADBAND,
                        help="|bias_score| at or below this is treated as flat (not scored)")
    parser.add_argument("--min-rows", type=int, default=2,
                        help="minimum rows-with-forward-data required before scoring")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = parser.parse_args(argv)

    horizons = [int(x) for x in args.horizons.split(",") if x.strip()]

    rows = load_rows(args.symbol, args.tenor, args.days)

    # Guard: a thin window can't be scored (each row needs a later row for its
    # forward return), so bail with a clear count instead of a table of zeros
    # that reads like a real "never right" result.
    n_forward = count_with_forward(rows, horizons)
    if n_forward < args.min_rows:
        msg = (
            f"Insufficient history: {len(rows)} row(s) for {args.symbol.upper()} "
            f"(tenor={args.tenor}) in the last {args.days} day(s), {n_forward} with "
            f"forward data — need >= {args.min_rows} with forward data.\n"
            f"Let the engine accumulate a session (rows land ~1/sec/tenor during the "
            f"24x5 run window) and re-run."
        )
        if args.json:
            print(json.dumps({
                "insufficient_history": True,
                "rows": len(rows),
                "rows_with_forward_data": n_forward,
                "min_rows": args.min_rows,
            }, indent=2))
        else:
            print(msg)
        return 1

    stats = evaluate_direction(rows, horizons, deadband=args.deadband)

    if args.json:
        print(json.dumps([dataclasses.asdict(s) for s in stats], default=str, indent=2))
    else:
        print(f"Trade Bias direction hit-rate — {args.symbol.upper()} / {args.tenor} / "
              f"{len(rows)} rows / last {args.days}d / deadband {args.deadband:g}")
        print(_format_table(stats))
        print(
            "\nhit_rate = fraction of directional reads whose forward move matched the "
            "lean (> 50% beats a coin flip).\nedge_bps = mean(forward_return x "
            "sign(bias_score)), in bps — positive = right more than wrong.\n"
            "A calibrated signal shows hit_rate / edge rising with the confidence bucket."
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
