"""Diagnostic: why did the v4 edge candidates open 0 trades in the backtest?

The four edge bots (charm_close_magnet, vanna_vol_crush_rider,
aggressor_flow_divergence, gamma_regime_shift_rider) each hard-gate on the NEW
snapshot fields (pin/charm/vanna/flow/regime-velocity). If those fields are
``None`` over the backtest window — because the source table/column was not
being populated back then, or a fetch fails — every bot no-ops and the screen
shows 0 trades with 0 vetoes and 0 fill failures (exactly what we saw).

This probe answers, without guessing:

  1. **Source coverage** — over the window, how many rows exist in each source
     table and how many have the specific columns the bots read NON-NULL
     (gex_summary.pin_strike / flip_distance / convexity_risk,
     gex_by_strike.dealer_vanna_exposure / dealer_charm_exposure,
     forced_flow_profile.close_charm_flow(_smooth), flow_series_5min.*).
  2. **Snapshot population** — for a handful of sampled instants it rebuilds the
     exact ``MarketSnapshot`` the bots see (via ``build_snapshot(as_of=t)``) and
     prints which edge fields came back populated.

Run it (same DB the backtest uses):

    python -m src.tools.tw_edge_field_probe --days 45
    python -m src.tools.tw_edge_field_probe --days 45 --samples 12 --underlying SPY

Read the coverage table first: a column at 0 / very-low non-null is the reason
the dependent bot never fired. Nothing here writes to the DB.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple

from src.tradeworkz import config as tw_config
from src.tradeworkz.context import build_snapshot

# (label, table, [columns to count non-null]).
_COVERAGE = [
    ("gex_summary", "gex_summary", ["pin_strike", "flip_distance", "convexity_risk", "local_gex"]),
    (
        "gex_by_strike",
        "gex_by_strike",
        ["dealer_vanna_exposure", "dealer_charm_exposure", "vanna_exposure", "charm_exposure"],
    ),
    (
        "forced_flow_profile",
        "forced_flow_profile",
        ["close_charm_flow", "close_charm_flow_smooth", "charm_flip", "vanna_flip"],
    ),
    (
        "flow_series_5min",
        "flow_series_5min",
        ["net_premium_cum", "net_volume_cum"],
    ),
    ("vix_bars", "vix_bars", ["close"]),
]

# The edge fields each bot hard-gates on (for the per-sample dump).
_EDGE_FIELDS = [
    "pin_strike",
    "pin_confidence",
    "close_charm_flow",
    "charm_flip",
    "dealer_vanna_total",
    "dealer_charm_total",
    "flow_net_premium",
    "flow_net_volume",
    "prior_vix",
    "flip_distance",
    "convexity_risk",
    "prior_net_gex",
]


def _window(conn: Any, days: int) -> Tuple[datetime, datetime]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    cur = conn.cursor()
    cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM option_chains")
    row = cur.fetchone()
    if row and row[0]:
        start = max(start, row[0])
    if row and row[1]:
        end = min(end, row[1])
    return start, end


def _has_column(conn: Any, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def _table_exists(conn: Any, table: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
    row = cur.fetchone()
    return bool(row and row[0])


def _ts_column(conn: Any, table: str) -> str:
    # flow_series_5min keys its time column 'bar_start'; everything else 'timestamp'.
    return "bar_start" if table == "flow_series_5min" else "timestamp"


def print_coverage(conn: Any, start: datetime, end: datetime) -> None:
    print("\n=== SOURCE COVERAGE over window ===")
    print(f"window: {start.isoformat()}  ->  {end.isoformat()}\n")
    for label, table, cols in _COVERAGE:
        if not _table_exists(conn, table):
            print(f"{label:22s}  TABLE MISSING")
            continue
        tscol = _ts_column(conn, table)
        cur = conn.cursor()
        present = [c for c in cols if _has_column(conn, table, c)]
        missing = [c for c in cols if c not in present]
        counts = ", ".join(f"COUNT({c}) AS n_{c}" for c in present) if present else ""
        sel = "COUNT(*) AS total" + (", " + counts if counts else "")
        try:
            cur.execute(
                f"SELECT {sel} FROM {table} WHERE {tscol} BETWEEN %s AND %s",
                (start, end),
            )
            row = cur.fetchone()
        except Exception as exc:  # pragma: no cover - diagnostic only
            print(f"{label:22s}  QUERY ERROR: {exc}")
            continue
        total = row[0] if row else 0
        print(f"{label:22s}  rows={total}")
        for i, c in enumerate(present, start=1):
            nn = row[i] if row and len(row) > i else 0
            pct = (100.0 * nn / total) if total else 0.0
            print(f"    {c:26s} non-null={nn:>8}  ({pct:5.1f}%)")
        for c in missing:
            print(f"    {c:26s} COLUMN MISSING")
    print()


def _sample_instants(
    conn: Any, underlying: str, start: datetime, end: datetime, n: int
) -> List[datetime]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp FROM gex_summary
        WHERE underlying = %s AND timestamp BETWEEN %s AND %s
        ORDER BY timestamp
        """,
        (underlying, start, end),
    )
    ts = [r[0] for r in cur.fetchall() if r[0] is not None]
    if not ts:
        return []
    if len(ts) <= n:
        return ts
    step = len(ts) // n
    return [ts[i * step] for i in range(n)]


def print_samples(conn: Any, underlying: str, start: datetime, end: datetime, n: int) -> None:
    print(f"=== SNAPSHOT FIELD DUMP — {underlying} ({n} sampled instants) ===")
    instants = _sample_instants(conn, underlying, start, end, n)
    if not instants:
        print("  (no gex_summary rows for this symbol in the window)\n")
        return
    populated = {f: 0 for f in _EDGE_FIELDS}
    for t in instants:
        snap = build_snapshot(conn, underlying, as_of=t)
        if snap is None:
            print(f"  {t.isoformat()}  build_snapshot -> None")
            continue
        vals = []
        for f in _EDGE_FIELDS:
            v = getattr(snap, f, None)
            if v is not None:
                populated[f] += 1
            vals.append(f"{f}={_fmt(v)}")
        print(f"  {t.isoformat()}")
        print("      " + "  ".join(vals))
    print(f"\n  populated-count across {len(instants)} samples:")
    for f in _EDGE_FIELDS:
        print(f"      {f:22s} {populated[f]}/{len(instants)}")
    print()


def _fmt(v: Any) -> str:
    if v is None:
        return "None"
    if isinstance(v, float):
        if abs(v) >= 1e5 or (v != 0 and abs(v) < 1e-3):
            return f"{v:.2e}"
        return f"{v:.4f}"
    return str(v)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=45, help="window size (default 45)")
    ap.add_argument("--samples", type=int, default=8, help="snapshot samples per symbol")
    ap.add_argument(
        "--underlying",
        type=str,
        default=None,
        help="single symbol to dump (default: whole fleet universe)",
    )
    args = ap.parse_args(argv)

    from src.database import db_connection

    with db_connection() as conn:
        start, end = _window(conn, args.days)
        print_coverage(conn, start, end)
        symbols = [args.underlying] if args.underlying else list(tw_config.fleet_universes())
        for sym in symbols:
            print_samples(conn, sym, start, end, args.samples)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
