"""Forward-test scorecard: has any restarted bot found an edge yet?

Reads the forward-test bots' live paper trades (``tw_trades``, seeder rows
excluded) and scores each bot overall, then split by symbol, by structure and
by tuning (the ``params_version`` the engine stamps on every entry), against
the catalog's promotion bar — the same bar a backtest screen has to clear:
at least ``PROMOTION_MIN_TRADES`` trades, profit factor at least
``PROMOTION_MIN_PROFIT_FACTOR``, positive expectancy.

The splits are the point. "Keep tweaking until we find an edge" means a bot
can lose overall while one symbol or one structure carries it, and a tweak
only counts once its own trades clear the bar — a tuning's results never
borrow from the tuning before it.

When a bot's current tuning clears the bar, record a ``ResearchRun`` with
``harness="tradeworkz-live"`` on its catalog entry; with stage VALIDATED that
funds it on evidence instead of as a test, and its card drops the
"Forward test:" label on the next API start.

Read-only. ``make tradeworkz-forward-test`` (``ARGS="--since 2026-11-13 --json"``).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from src.strategies import PROMOTION_MIN_PROFIT_FACTOR, PROMOTION_MIN_TRADES


@dataclass(frozen=True)
class TradeRow:
    bot_id: str
    underlying: str
    strategy_type: str
    params_version: str
    pnl: float
    opened_at: Optional[datetime]
    closed_at: Optional[datetime]


@dataclass(frozen=True)
class Score:
    trades: int
    wins: int
    win_rate: Optional[float]
    profit_factor: Optional[float]  # None when nothing lost
    expectancy: float  # mean realized P&L per trade, dollars
    total_pnl: float
    avg_hold_minutes: Optional[float]
    verdict: str


def verdict(trades: int, profit_factor: Optional[float], expectancy: float) -> str:
    """Where a slice stands against the promotion bar."""
    if trades == 0:
        return "no trades yet"
    pf_ok = profit_factor is None or profit_factor >= PROMOTION_MIN_PROFIT_FACTOR
    if trades >= PROMOTION_MIN_TRADES:
        if pf_ok and expectancy > 0:
            return "EDGE"
        return "losing" if expectancy <= 0 else "no edge yet"
    return "promising" if pf_ok and expectancy > 0 else "no edge yet"


def score(rows: Sequence[TradeRow]) -> Score:
    n = len(rows)
    wins = sum(1 for r in rows if r.pnl > 0)
    gross_win = sum(r.pnl for r in rows if r.pnl > 0)
    gross_loss = -sum(r.pnl for r in rows if r.pnl < 0)
    pf = gross_win / gross_loss if gross_loss > 0 else None
    total = sum(r.pnl for r in rows)
    exp = total / n if n else 0.0
    holds = [
        (r.closed_at - r.opened_at).total_seconds() / 60.0
        for r in rows
        if r.opened_at is not None and r.closed_at is not None
    ]
    return Score(
        trades=n,
        wins=wins,
        win_rate=(wins / n) if n else None,
        profit_factor=pf,
        expectancy=exp,
        total_pnl=total,
        avg_hold_minutes=(sum(holds) / len(holds)) if holds else None,
        verdict=verdict(n, pf, exp),
    )


def _split(rows: Iterable[TradeRow], key: str) -> Dict[str, List[TradeRow]]:
    out: Dict[str, List[TradeRow]] = defaultdict(list)
    for r in rows:
        out[getattr(r, key) or "?"].append(r)
    return dict(sorted(out.items(), key=lambda kv: -len(kv[1])))


def build_report(
    rows: Sequence[TradeRow],
    bot_ids: Sequence[str],
    current_versions: Dict[str, str],
    since: Optional[date] = None,
) -> Dict[str, Any]:
    """Per-bot scores with symbol / structure / tuning splits."""
    by_bot: Dict[str, List[TradeRow]] = defaultdict(list)
    for r in rows:
        by_bot[r.bot_id].append(r)
    bots = []
    for bot_id in bot_ids:
        trades = by_bot.get(bot_id, [])
        current = current_versions.get(bot_id)
        tunings = {
            v: asdict(score(rs)) | {"current": v == current}
            for v, rs in _split(trades, "params_version").items()
        }
        current_score = tunings.get(current) if current else None
        bots.append(
            {
                "bot_id": bot_id,
                "current_params_version": current,
                "overall": asdict(score(trades)),
                "by_symbol": {k: asdict(score(v)) for k, v in _split(trades, "underlying").items()},
                "by_structure": {
                    k: asdict(score(v)) for k, v in _split(trades, "strategy_type").items()
                },
                "by_tuning": tunings,
                "current_tuning_clears_bar": bool(
                    current_score and current_score["verdict"] == "EDGE"
                ),
            }
        )
    bots.sort(key=lambda b: (b["overall"]["trades"] > 0, b["overall"]["expectancy"]), reverse=True)
    return {
        "since": since.isoformat() if since else None,
        "bar": {
            "min_trades": PROMOTION_MIN_TRADES,
            "min_profit_factor": PROMOTION_MIN_PROFIT_FACTOR,
            "expectancy": "> 0",
        },
        "bots": bots,
    }


def _fmt_row(label: str, s: Dict[str, Any]) -> str:
    wr = f"{s['win_rate'] * 100:.0f}%" if s["win_rate"] is not None else "—"
    pf = (
        f"{s['profit_factor']:.2f}"
        if s["profit_factor"] is not None
        else ("—" if not s["trades"] else "no loss")
    )
    return (
        f"{label:<40} {s['trades']:>6} {wr:>5} {pf:>8} "
        f"{s['expectancy']:>+11.2f} {s['total_pnl']:>+10.0f}   {s['verdict']}"
    )


def format_report(report: Dict[str, Any]) -> str:
    bar = report["bar"]
    lines = [
        "",
        "TradeWorkz forward tests — live paper trades"
        + (f" since {report['since']}" if report["since"] else ""),
        f"An edge: >= {bar['min_trades']} trades, profit factor >= "
        f"{bar['min_profit_factor']:.2f}, expectancy > 0 (the catalog's promotion bar)",
        "=" * 104,
        f"{'bot / slice':<40} {'trades':>6} {'win':>5} {'PF':>8} "
        f"{'$/trade':>11} {'P&L $':>10}   verdict",
        "-" * 104,
    ]
    for b in report["bots"]:
        lines.append(_fmt_row(b["bot_id"], b["overall"]))
        if not b["overall"]["trades"]:
            continue
        for k, s in b["by_symbol"].items():
            lines.append(_fmt_row(f"  symbol    {k}", s))
        for k, s in b["by_structure"].items():
            lines.append(_fmt_row(f"  structure {k}", s))
        for k, s in b["by_tuning"].items():
            lines.append(_fmt_row(f"  tuning    {k}{' (current)' if s['current'] else ''}", s))
        if b["current_tuning_clears_bar"]:
            t = b["by_tuning"][b["current_params_version"]]
            lines.append(
                f"  -> current tuning clears the bar: record ResearchRun(harness="
                f"'tradeworkz-live', trades={t['trades']}, profit_factor="
                f"{t['profit_factor'] if t['profit_factor'] is not None else 'None'}, "
                f"expectancy={t['expectancy']:.2f}, verdict=Verdict.EDGE) on its catalog "
                "entry, with stage=VALIDATED, to fund it on evidence"
            )
        lines.append("")
    return "\n".join(lines)


def load_trades(conn: Any, bot_ids: Sequence[str], since: Optional[date]) -> List[TradeRow]:
    """Closed live trades for ``bot_ids`` (seeder rows excluded)."""
    since_ts = datetime.combine(since, datetime.min.time(), tzinfo=timezone.utc) if since else None
    cur = conn.cursor()
    cur.execute(
        """
        SELECT bot_id, underlying, strategy_type, realized_pnl, opened_at, closed_at,
               components_at_entry ->> 'params_version'
        FROM tw_trades
        WHERE bot_id = ANY(%s)
          AND (components_at_entry ->> 'origin') IS DISTINCT FROM 'simulate'
          AND (%s::timestamptz IS NULL OR closed_at >= %s::timestamptz)
        ORDER BY closed_at
        """,
        (list(bot_ids), since_ts, since_ts),
    )
    out = []
    for bot_id, u, stype, pnl, opened, closed, pv in cur.fetchall():
        out.append(
            TradeRow(
                bot_id=bot_id,
                underlying=u,
                strategy_type=stype,
                params_version=pv or "untagged",
                pnl=float(pnl or 0.0),
                opened_at=opened,
                closed_at=closed,
            )
        )
    return out


def main(argv: Optional[List[str]] = None) -> int:
    from src.tradeworkz.engine import params_version
    from src.tradeworkz.registry import FORWARD_TEST_SPECS, known_specs

    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--since", help="only trades closed on/after this date (YYYY-MM-DD)")
    p.add_argument("--bots", help="comma-separated bot ids (default: every forward-test bot)")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    args = p.parse_args(argv)

    specs = known_specs()
    if args.bots:
        wanted = [b.strip() for b in args.bots.split(",") if b.strip()]
        unknown = [b for b in wanted if b not in specs]
        if unknown:
            print(f"unknown bot id(s): {', '.join(unknown)}", file=sys.stderr)
            return 2
        bot_specs = [specs[b] for b in wanted]
    else:
        bot_specs = list(FORWARD_TEST_SPECS)
    bot_ids = [s.id for s in bot_specs]
    current = {s.id: params_version(s.params) for s in bot_specs}
    since = date.fromisoformat(args.since) if args.since else None

    from src.database import db_connection

    with db_connection() as conn:
        rows = load_trades(conn, bot_ids, since)
    report = build_report(rows, bot_ids, current, since)
    print(json.dumps(report, indent=2, default=str) if args.json else format_report(report))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
