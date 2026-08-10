"""Execution sweep — screen ONE signal across many EXECUTIONS at once.

A single 45-day screen tests one *execution* of a strategy, not the strategy.
A signal that has no edge as a 0DTE debit vertical may have edge as a credit
spread, at a different DTE, hold, or target. Running those one at a time is a
~1-hour round-trip each; this runs a whole grid in ONE pass, because the
backtest builds each snapshot once and shares it across all bots (30 configs
≈ the wall-clock of 1).

**Overfitting guard (the point).** Picking the best PF from a grid on a single
sample finds a "winner" by chance. So every config is scored on a TRAIN half
(first 50% of the window) AND a TEST half (second 50%). A config is only
``robust`` when the FULL-window PF clears the gate AND both halves have positive
expectancy (the edge holds out-of-sample, not just in aggregate). Treat a config
that shines in one half and dies in the other as noise.

Usage (same DB the backtest uses):

    python -m src.tools.tw_execution_sweep --bot call_wall_rejector --days 45
    python -m src.tools.tw_execution_sweep --bot put_wall_bouncer --interval-min 5

It runs two passes (train, test); at interval 5 that is ~2× a normal screen.
Nothing here writes to any tw_* table.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.tradeworkz.backtest import _chain_window, run_fleet_backtest
from src.tradeworkz.models import BotSpec
from src.tradeworkz.registry import known_specs

# Minimum trades in a HALF before its sign counts (a soft floor — the filtered
# wall signal is low-frequency, so halves are small; we lean on the combined
# figure for ranking and use the halves as a sign-consistency check).
_MIN_HALF_TRADES = 6


def wall_execution_grid() -> List[Tuple[str, Dict[str, Any]]]:
    """(label, param-overrides) execution variants for a wall-reversion bot.

    Spans the structure axis (debit vertical / single debit / credit spread),
    the debit target, spread geometry, hold, and the credit take. Kept modest
    (fewer configs => less multiple-comparisons risk) but broad enough to tell a
    structure story.
    """
    grid: List[Tuple[str, Dict[str, Any]]] = []
    # Debit verticals: target mode × hold.
    for tmode in ("max_pain", "flip", "vwap"):
        for hold in (45, 90):
            grid.append(
                (
                    f"debit_vert/{tmode}/{hold}m",
                    {"structure": "debit_vertical", "target_mode": tmode, "max_hold_minutes": hold},
                )
            )
    # Single long debit (the retired bot's structure), two holds.
    for hold in (45, 90):
        grid.append(
            (
                f"single_debit/max_pain/{hold}m",
                {"structure": "single_debit", "max_hold_minutes": hold},
            )
        )
    # Credit spreads (the boundary-matching structure): wing × take × hold.
    for wing in (2, 5):
        for take in (0.5, 0.35):
            for hold in (120, 240):
                grid.append(
                    (
                        f"credit/wing{wing}/take{int(take * 100)}/{hold}m",
                        {
                            "structure": "credit_spread",
                            "wing_width": wing,
                            "profit_take_frac": take,
                            "max_hold_minutes": hold,
                        },
                    )
                )
    return grid


def build_specs(base: BotSpec, grid: List[Tuple[str, Dict[str, Any]]]) -> List[BotSpec]:
    specs: List[BotSpec] = []
    for i, (label, over) in enumerate(grid):
        params = dict(base.params or {})
        params.update(over)
        specs.append(
            BotSpec(
                id=f"{base.id}__cfg{i:02d}",
                display_name=label,
                strategy_class=base.strategy_class,
                tier=base.tier,
                direction_mode=base.direction_mode,
                universe="*",
                tagline="",
                description=label,
                params=params,
            )
        )
    return specs


def _by_id(result: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {b["bot_id"]: b for b in result.get("bots", [])}


def _combined(train: Dict[str, Any], test: Dict[str, Any]) -> Tuple[int, Optional[float], float]:
    """Combined (n, PF, expectancy) reconstructed from the two half-summaries.

    n and total P&L are additive; gross win / loss recombine from
    ``avg_win*wins`` / ``avg_loss*losses`` so the combined PF is exact.
    """
    n = int(train["n_trades"]) + int(test["n_trades"])
    gross_win = train["avg_win"] * train["wins"] + test["avg_win"] * test["wins"]
    gross_loss = train["avg_loss"] * train["losses"] + test["avg_loss"] * test["losses"]
    total = train["total_pnl"] + test["total_pnl"]
    pf = (gross_win / gross_loss) if gross_loss > 0 else None
    expectancy = (total / n) if n else 0.0
    return n, pf, expectancy


def _pf_str(pf: Optional[float]) -> str:
    return "  inf" if pf is None else f"{pf:5.2f}"


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bot", required=True, help="base bot id (e.g. call_wall_rejector)")
    ap.add_argument("--days", type=int, default=45)
    ap.add_argument("--interval-min", type=int, default=5)
    ap.add_argument("--tolerance-min", type=int, default=10)
    args = ap.parse_args(argv)

    catalog = known_specs()
    base = catalog.get(args.bot)
    if base is None:
        print(f"unknown bot id: {args.bot}")
        return 2

    grid = wall_execution_grid()
    specs = build_specs(base, grid)

    from src.database import db_connection

    with db_connection() as conn:
        chain_lo, chain_hi = _chain_window(conn)
        now = datetime.now(timezone.utc)
        end = min(now, chain_hi) if chain_hi else now
        start = end - _days_delta(args.days)
        if chain_lo:
            start = max(start, chain_lo)
        mid = start + (end - start) / 2

        common = dict(interval_min=args.interval_min, tolerance_min=args.tolerance_min, specs=specs)
        print(f"=== execution sweep: {args.bot} — {len(specs)} configs ===")
        print(f"train {start.isoformat()} -> {mid.isoformat()}")
        print(f"test  {mid.isoformat()} -> {end.isoformat()}\n")
        train = run_fleet_backtest(conn, start=start, end=mid, **common)
        test = run_fleet_backtest(conn, start=mid, end=end, **common)

    tr, te = _by_id(train), _by_id(test)
    rows = []
    for spec in specs:
        t, e = tr.get(spec.id), te.get(spec.id)
        if not t or not e:
            continue
        n, pf, exp = _combined(t, e)
        robust = (
            pf is not None
            and pf >= 1.1
            and exp > 0
            and t["expectancy"] > 0
            and e["expectancy"] > 0
            and t["n_trades"] >= _MIN_HALF_TRADES
            and e["n_trades"] >= _MIN_HALF_TRADES
        )
        rows.append((robust, pf if pf is not None else 1e9, spec.description, n, pf, exp, t, e))

    rows.sort(key=lambda r: (r[0], r[1]), reverse=True)
    hdr = (
        f"{'config':28s} {'nAll':>4} {'PFall':>6} {'expAll':>8} "
        f"{'nTr':>4} {'expTr':>8} {'nTe':>4} {'expTe':>8}  robust"
    )
    print(hdr)
    print("-" * len(hdr))
    for robust, _key, label, n, pf, exp, t, e in rows:
        print(
            f"{label:28s} {n:>4} {_pf_str(pf):>6} {exp:>8.2f} "
            f"{t['n_trades']:>4} {t['expectancy']:>8.2f} "
            f"{e['n_trades']:>4} {e['expectancy']:>8.2f}  {'YES' if robust else ''}"
        )
    print(
        "\nRead: 'robust' = full-window PF >= 1.1 AND positive expectancy in BOTH halves "
        f"(each >= {_MIN_HALF_TRADES} trades). Anything else is a candidate at best — a "
        "config strong in one half and weak in the other is noise, not edge."
    )
    return 0


def _days_delta(days: int):
    from datetime import timedelta

    return timedelta(days=days)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
