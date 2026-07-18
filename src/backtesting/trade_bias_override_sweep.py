"""Sweep the Trade Bias override threshold against recent sessions.

The override gate (``TRADE_BIAS_OVERRIDE_D`` + ``..._MIN_ALIGNED``) is the one
real judgment call in the fusion: too low and it whipsaws in negative gamma,
too high and it never fires when the tape genuinely reverses. Because every
cycle is persisted to ``trade_bias_scores`` (with the tactical pillar values,
the structural baseline, and the underlying close), calibrating it is a
backtest rather than a guess.

For each persisted row this replays ``fuse`` across a grid of override
thresholds — reusing the exact production ``compute_tactical`` / ``fuse`` code —
and scores each threshold by how often it overrides and whether those overrides
were directionally right over a forward horizon (the underlying's move from the
row's close to the close ~H minutes later, read straight from later rows).

Run::

    python -m src.backtesting.trade_bias_override_sweep \\
        --symbol SPY --tenor swing --days 5 \\
        --horizons 15,30,60 --grid 0.30,0.40,0.50,0.60,0.70

It reads only ``trade_bias_scores`` — no engine or market feed required — so it
runs against whatever history the engine has already produced.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Optional

from src.signals.trade_bias.fusion import compute_tactical, fuse, profile_for

_PILLAR_KEYS = ("price_action", "flow", "tape", "momentum")
_DIR_SIGN = {"long": 1, "short": -1, "neutral": 0}


@dataclass
class BiasRow:
    """The fields the sweep needs from one ``trade_bias_scores`` payload."""

    timestamp: datetime
    close: Optional[float]
    pillars: dict  # {price_action, flow, tape, momentum}: float | None
    struct_trend: str  # bullish | bearish | neutral
    struct_confidence: float  # structural.confidence (0-10)
    struct_max_confidence: float  # structural.maxConfidence (usually 10)


def _reconstruct_structural(row: BiasRow) -> SimpleNamespace:
    """A minimal stand-in for the BiasResult that ``fuse`` reads.

    Only ``trend`` / ``confidence`` / ``maxConfidence`` drive the state
    decision; the label/setup/playbook fields are output-only, so placeholders
    are fine for a threshold sweep.
    """
    return SimpleNamespace(
        trend=row.struct_trend,
        confidence=row.struct_confidence,
        maxConfidence=row.struct_max_confidence or 10.0,
        regimeLabel="",
        biasLabel="",
        bias="",
        setup="",
        playbook=[],
        expectedBehavior=[],
    )


def _forward_returns(
    rows: list[BiasRow], horizons_min: list[int]
) -> dict[int, list[Optional[float]]]:
    """For each horizon, the signed forward return of every row.

    ``ret[i] = (close[j] - close[i]) / close[i]`` where ``j`` is the first row
    at or after ``t_i + horizon``, provided it lands within the same session day
    and no more than ``horizon`` again beyond the target (so a gap across the
    close doesn't borrow the next day's open). ``None`` when unavailable.
    """
    times = [r.timestamp.timestamp() for r in rows]
    out: dict[int, list[Optional[float]]] = {}
    for h in horizons_min:
        span = h * 60.0
        col: list[Optional[float]] = []
        for i, r in enumerate(rows):
            ci = r.close
            if ci is None or ci <= 0:
                col.append(None)
                continue
            target = times[i] + span
            j = bisect_left(times, target, lo=i)
            if j >= len(rows):
                col.append(None)
                continue
            rj = rows[j]
            # Reject a match that jumped across a session boundary or is too far
            # past the target (thin history) — either would poison the return.
            if rj.timestamp.date() != r.timestamp.date() or times[j] - target > span:
                col.append(None)
                continue
            cj = rj.close
            col.append((cj - ci) / ci if cj is not None and ci else None)
        out[h] = col
    return out


@dataclass
class ThresholdStat:
    override_d: float
    horizon_min: int
    n_rows: int  # rows with a valid forward return
    n_override: int
    override_rate: float
    hit_rate: Optional[float]  # fraction of overrides that went the right way
    avg_edge_bps: Optional[float]  # mean(forward_return * dir_sign) in bps
    avg_abs_move_bps: Optional[float]  # mean |forward_return| on override rows


def evaluate_sweep(
    rows: list[BiasRow],
    tenor: str,
    grid: list[float],
    horizons_min: list[int],
    min_aligned: Optional[int] = None,
) -> list[ThresholdStat]:
    """Replay ``fuse`` over the threshold grid and score each against forward returns.

    Pure / DB-free so it is unit-testable: pass reconstructed rows in, get the
    stats table out.
    """
    rows = sorted(rows, key=lambda r: r.timestamp)
    base_profile = profile_for(tenor)
    fwd = _forward_returns(rows, horizons_min)

    # compute_tactical is threshold-independent (weights + align_min only), so
    # derive each row's tactical read once and reuse across the grid.
    tacticals = [
        compute_tactical(
            r.pillars.get("price_action"),
            r.pillars.get("flow"),
            r.pillars.get("tape"),
            r.pillars.get("momentum"),
            base_profile,
        )
        for r in rows
    ]
    structurals = [_reconstruct_structural(r) for r in rows]

    stats: list[ThresholdStat] = []
    for d in grid:
        profile = dataclasses.replace(base_profile, override_d=d)
        if min_aligned is not None:
            profile = dataclasses.replace(profile, override_min_aligned=min_aligned)
        fused = [fuse(structurals[i], tacticals[i], profile) for i in range(len(rows))]
        for h in horizons_min:
            returns = fwd[h]
            n_rows = 0
            n_override = 0
            hits = 0
            edge_sum = 0.0
            abs_sum = 0.0
            scored = 0
            for i in range(len(rows)):
                ret = returns[i]
                if ret is None:
                    continue
                n_rows += 1
                f = fused[i]
                if f.state != "override":
                    continue
                n_override += 1
                sign = _DIR_SIGN.get(f.direction, 0)
                if sign == 0:
                    continue
                scored += 1
                edge = ret * sign
                edge_sum += edge
                abs_sum += abs(ret)
                if edge > 0:
                    hits += 1
            stats.append(
                ThresholdStat(
                    override_d=d,
                    horizon_min=h,
                    n_rows=n_rows,
                    n_override=n_override,
                    override_rate=(n_override / n_rows) if n_rows else 0.0,
                    hit_rate=(hits / scored) if scored else None,
                    avg_edge_bps=(edge_sum / scored * 1e4) if scored else None,
                    avg_abs_move_bps=(abs_sum / scored * 1e4) if scored else None,
                )
            )
    return stats


# --------------------------------------------------------------------------
# DB loading + CLI
# --------------------------------------------------------------------------

def _parse_ts(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _row_from_payload(ts: Any, payload: Any) -> Optional[BiasRow]:
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
    tactical = payload.get("tactical") or {}
    pillars_raw = (tactical.get("pillars") if isinstance(tactical, dict) else None) or {}
    pillars = {k: pillars_raw.get(k) for k in _PILLAR_KEYS}
    struct = payload.get("structural_bias") or {}
    trend = struct.get("trend") if isinstance(struct, dict) else None
    return BiasRow(
        timestamp=tstamp,
        close=payload.get("close"),
        pillars=pillars,
        struct_trend=trend or "neutral",
        struct_confidence=float(payload.get("confidence_raw") or 0.0),
        struct_max_confidence=float(payload.get("max_confidence_raw") or 10.0),
    )


def load_rows(symbol: str, tenor: str, days: int) -> list[BiasRow]:
    from src.database import db_connection

    query = """
        SELECT timestamp, payload
        FROM trade_bias_scores
        WHERE underlying = %s AND tenor = %s
          AND timestamp >= NOW() - make_interval(days => %s)
        ORDER BY timestamp ASC
    """
    rows: list[BiasRow] = []
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, (symbol.upper(), tenor, days))
        for ts, payload in cur.fetchall():
            row = _row_from_payload(ts, payload)
            if row is not None:
                rows.append(row)
    return rows


def _format_table(stats: list[ThresholdStat]) -> str:
    header = (
        f"{'override_d':>10} {'horizon':>8} {'rows':>7} {'overrides':>10} "
        f"{'ovr_rate':>9} {'hit_rate':>9} {'edge_bps':>9} {'abs_bps':>9}"
    )
    lines = [header, "-" * len(header)]
    for s in stats:
        hit = f"{s.hit_rate * 100:6.1f}%" if s.hit_rate is not None else "     --"
        edge = f"{s.avg_edge_bps:9.2f}" if s.avg_edge_bps is not None else "       --"
        absm = f"{s.avg_abs_move_bps:9.2f}" if s.avg_abs_move_bps is not None else "       --"
        lines.append(
            f"{s.override_d:>10.2f} {s.horizon_min:>7}m {s.n_rows:>7} {s.n_override:>10} "
            f"{s.override_rate * 100:>8.2f}% {hit:>9} {edge} {absm}"
        )
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Sweep the Trade Bias override threshold.")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--tenor", default="swing", choices=["swing", "intraday"])
    parser.add_argument("--days", type=int, default=5, help="lookback window (calendar days)")
    parser.add_argument("--grid", default="0.30,0.40,0.50,0.60,0.70",
                        help="comma-separated override_d values to test")
    parser.add_argument("--horizons", default="15,30,60",
                        help="comma-separated forward horizons in minutes")
    parser.add_argument("--min-aligned", type=int, default=None,
                        help="override the aligned-pillar count for the whole sweep")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = parser.parse_args(argv)

    grid = [float(x) for x in args.grid.split(",") if x.strip()]
    horizons = [int(x) for x in args.horizons.split(",") if x.strip()]

    rows = load_rows(args.symbol, args.tenor, args.days)
    if not rows:
        print(f"No trade_bias_scores rows for {args.symbol.upper()} "
              f"(tenor={args.tenor}) in the last {args.days} day(s).")
        return 1

    stats = evaluate_sweep(rows, args.tenor, grid, horizons, min_aligned=args.min_aligned)

    if args.json:
        print(json.dumps([dataclasses.asdict(s) for s in stats], default=str, indent=2))
    else:
        print(f"Trade Bias override sweep — {args.symbol.upper()} / {args.tenor} / "
              f"{len(rows)} rows / last {args.days}d")
        print(_format_table(stats))
        print(
            "\nedge_bps = mean(forward_return x override_direction), in bps — positive means "
            "overrides were directionally right.\nPick the lowest override_d whose hit_rate / "
            "edge stays strong without the override_rate ballooning."
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
