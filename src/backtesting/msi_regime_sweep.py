"""Validate the MSI (Composite Score) as a regime gauge against realized behavior.

The MSI is a 0-100 DIRECTIONLESS regime score: high = trend / expansion (trades
run), low = chop / high-risk reversal (mean-reversion). This sweep asks the
plain validity question: *do high-MSI periods actually trend more than low-MSI
periods?*

For each persisted reading it measures, over a forward window, whether the move
CONTINUES (trending) or REVERSES (chopping): it splits the window at its
midpoint and checks whether the second half carries the first half's direction.
A trending regime shows continuation > 50%; a mean-reverting one < 50%. If the
MSI is a valid regime gauge, the continuation rate should rise monotonically
with the MSI band -- and the trend/expansion band should show larger absolute
moves than the chop band.

Reads only trade_bias_scores: each cycle persists the 0-100 MSI (payload
``inputs.msi``) alongside the underlying close, so no extra join is needed. That
MSI is copied straight from the composite each cycle, and is identical across
tenors -- so a single tenor (default swing) is used to avoid double-counting.

Run::

    python -m src.backtesting.msi_regime_sweep --symbol SPY --days 30 --horizons 30,60

Read ``continuation_rate`` (> 50% = trending, < 50% = mean-reverting) and
``abs_move_bps`` (expansion). Validity = continuation rising with the MSI band.
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

# MSI regime bands — match core/regime.ts and scoring_engine._regime_label.
_BANDS: tuple[tuple[float, float, str], ...] = (
    (0.0, 20.0, "reversal <20"),
    (20.0, 40.0, "chop 20-40"),
    (40.0, 70.0, "controlled 40-70"),
    (70.0, 100.01, "trend >=70"),
)
_ALL = "ALL"


@dataclass
class MsiRow:
    timestamp: datetime
    close: Optional[float]
    msi: Optional[float]  # 0-100 composite


@dataclass
class MsiBandStat:
    horizon_min: int
    band: str
    n: int  # rows with a full split-window forward path in this band
    continuation_rate: Optional[float]  # P(second half carries the first half's sign)
    avg_abs_move_bps: Optional[float]  # mean |forward return| over the window, bps


def _band_for(msi: Optional[float]) -> Optional[str]:
    if msi is None:
        return None
    for lo, hi, label in _BANDS:
        if lo <= msi < hi:
            return label
    return None


def evaluate_msi_regime(
    rows: list[MsiRow],
    horizons_min: list[int],
) -> list[MsiBandStat]:
    """Score forward continuation vs reversal by MSI band. Pure / DB-free.

    For horizon ``h`` the window is split at ``h // 2``: a row 'continues' when
    the second-half move carries the same sign as the first-half move (an
    exactly-flat half doesn't count). ``ALL`` aggregates across bands.
    """
    rows = sorted(rows, key=lambda r: r.timestamp)
    labels = [b[2] for b in _BANDS] + [_ALL]
    stats: list[MsiBandStat] = []
    for h in horizons_min:
        half = max(1, h // 2)
        wanted = [half, h] if half != h else [h]
        fwd = _forward_returns(rows, wanted)
        r_half_col = fwd[half]
        r_full_col = fwd[h]
        acc = {lbl: {"cont": 0.0, "abs": 0.0, "n": 0.0} for lbl in labels}
        for i, r in enumerate(rows):
            r_half = r_half_col[i]
            r_full = r_full_col[i]
            if r_half is None or r_full is None:
                continue
            band = _band_for(r.msi)
            if band is None:
                continue
            # Second-half direction ~ sign(r_full - r_half); a row continues when
            # that carries the first half's sign.
            second = r_full - r_half
            cont = (r_half > 0 and second > 0) or (r_half < 0 and second < 0)
            for lbl in (band, _ALL):
                a = acc[lbl]
                a["n"] += 1
                a["abs"] += abs(r_full)
                if cont:
                    a["cont"] += 1
        for lbl in labels:
            a = acc[lbl]
            n = int(a["n"])
            stats.append(
                MsiBandStat(
                    horizon_min=h,
                    band=lbl,
                    n=n,
                    continuation_rate=(a["cont"] / n) if n else None,
                    avg_abs_move_bps=(a["abs"] / n * 1e4) if n else None,
                )
            )
    return stats


# --------------------------------------------------------------------------
# DB loading + CLI
# --------------------------------------------------------------------------

def _row_from_payload(ts: Any, payload: Any) -> Optional[MsiRow]:
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
    inputs = payload.get("inputs")
    msi = inputs.get("msi") if isinstance(inputs, dict) else None
    return MsiRow(timestamp=tstamp, close=payload.get("close"), msi=msi)


def load_rows(symbol: str, tenor: str, days: int) -> list[MsiRow]:
    from src.database import db_connection

    query = """
        SELECT timestamp, payload
        FROM trade_bias_scores
        WHERE underlying = %s AND tenor = %s
          AND timestamp >= NOW() - make_interval(days => %s)
        ORDER BY timestamp ASC
    """
    rows: list[MsiRow] = []
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, (symbol.upper(), tenor, days))
        for ts, payload in cur.fetchall():
            row = _row_from_payload(ts, payload)
            if row is not None:
                rows.append(row)
    return rows


def _format_table(stats: list[MsiBandStat]) -> str:
    header = f"{'horizon':>8} {'msi_band':>17} {'n':>7} {'continuation':>13} {'abs_bps':>9}"
    lines = [header, "-" * len(header)]
    for s in stats:
        cont = f"{s.continuation_rate * 100:8.1f}%" if s.continuation_rate is not None else "       --"
        absm = f"{s.avg_abs_move_bps:9.2f}" if s.avg_abs_move_bps is not None else "       --"
        lines.append(f"{s.horizon_min:>7}m {s.band:>17} {s.n:>7} {cont:>13} {absm}")
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the MSI as a regime gauge (continuation vs reversal by band)."
    )
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--tenor", default="swing", choices=["swing", "intraday"],
                        help="which tenor's rows to read (MSI is identical across tenors)")
    parser.add_argument("--days", type=int, default=30, help="lookback window (calendar days)")
    parser.add_argument("--horizons", default="30,60",
                        help="comma-separated forward horizons in minutes (split at the midpoint)")
    parser.add_argument("--min-rows", type=int, default=2,
                        help="minimum rows-with-forward-data required before scoring")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = parser.parse_args(argv)

    horizons = [int(x) for x in args.horizons.split(",") if x.strip()]

    rows = load_rows(args.symbol, args.tenor, args.days)

    n_forward = count_with_forward(rows, horizons)
    if n_forward < args.min_rows:
        msg = (
            f"Insufficient history: {len(rows)} row(s) for {args.symbol.upper()} "
            f"(tenor={args.tenor}) in the last {args.days} day(s), {n_forward} with "
            f"forward data — need >= {args.min_rows}.\n"
            f"Let the engine accumulate a session and re-run."
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

    stats = evaluate_msi_regime(rows, horizons)

    if args.json:
        print(json.dumps([dataclasses.asdict(s) for s in stats], default=str, indent=2))
    else:
        print(f"MSI regime validity — {args.symbol.upper()} / {args.tenor} / "
              f"{len(rows)} rows / last {args.days}d")
        print(_format_table(stats))
        print(
            "\ncontinuation = fraction of windows whose second half carried the first "
            "half's direction (> 50% trending, < 50% mean-reverting).\n"
            "abs_bps = mean |forward move| over the window.\n"
            "The MSI is valid as a regime gauge if continuation RISES with the band "
            "(reversal < chop < controlled < trend) and trend shows the larger moves."
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
