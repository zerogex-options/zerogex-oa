"""Command line for the wall-break-odds study.

    python -m research.wall_break_odds.cli selftest
    python -m research.wall_break_odds.cli build-dataset SPX \
        --start 2026-01-02 --end 2026-06-30 --out research_output/wall_events.jsonl
    python -m research.wall_break_odds.cli analyze research_output/wall_events.jsonl

``selftest`` needs no database and no market data. The other two read the
production database strictly read-only and write only to the paths you name.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime

from research.wall_break_odds.events import ET
from typing import Any, Optional, Sequence

from research.wall_break_odds.events import EventConfig
from research.wall_break_odds.model import Row, base_rate, evaluate, fit_full, univariate_screen
from research.wall_break_odds.report import render_report

# NOTE: research.wall_break_odds.dataset (and through it sources, and through
# that src.database) is imported INSIDE the commands that need a database, not
# here. `selftest` and `analyze` must run on a checkout with nothing but numpy
# installed — a plumbing check that needs the production dependency stack to
# start is not much of a plumbing check.


def _iso(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _config_from_args(args: argparse.Namespace) -> EventConfig:
    return EventConfig(
        touch_pct=args.touch / 1e4,
        break_buffer_pct=args.buffer / 1e4,
        confirm_minutes=args.confirm,
        resolution_minutes=args.horizon,
        rearm_minutes=args.rearm,
    )


def cmd_selftest(args: argparse.Namespace) -> int:
    from research.wall_break_odds.selftest import run_selftest

    return run_selftest(n_sessions=args.sessions, seed=args.seed)


def cmd_build_dataset(args: argparse.Namespace) -> int:
    from research.wall_break_odds.dataset import build_dataset, write_jsonl
    from research.wall_break_odds.sources import DatabaseUnavailable, research_connection

    cfg = _config_from_args(args)
    records: list[dict[str, Any]] = []
    seen = used = censored = 0
    skipped: dict[str, int] = {}

    flow_rows = flow_contracts = 0

    def progress(result: Any) -> None:
        nonlocal seen, used, flow_rows, flow_contracts
        seen += 1
        flow_rows += result.n_flow_rows
        flow_contracts += result.n_flow_contracts
        if result.skipped_reason:
            skipped[result.skipped_reason] = skipped.get(result.skipped_reason, 0) + 1
        elif result.events:
            used += 1
        if seen % 10 == 0:
            print(f"  {seen} sessions, {len(records)} events", file=sys.stderr)

    try:
        with research_connection() as conn:
            for result in build_dataset(
                conn,
                args.symbol,
                args.start,
                args.end,
                cfg,
                strike_step=args.strike_step,
                with_flow=not args.no_flow,
                progress=progress,
            ):
                records.extend(result.events)
    except DatabaseUnavailable as exc:
        print(f"database unavailable: {exc}", file=sys.stderr)
        return 2

    censored = sum(1 for r in records if r.get("outcome") == "censored")
    meta = {
        "symbol": args.symbol,
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "sessions_seen": seen,
        "sessions_used": used,
        "skipped": skipped,
        "events_total": len(records),
        "events_censored": censored,
        "events_resolved": len(records) - censored,
        "flow_rows_fetched": flow_rows,
        "flow_contracts_usable": flow_contracts,
        "events_with_flow": sum(
            1 for r in records if (r.get("features") or {}).get("flow_toward_break") is not None
        ),
        "config": {
            "touch_pct": cfg.touch_pct,
            "break_buffer_pct": cfg.break_buffer_pct,
            "confirm_minutes": cfg.confirm_minutes,
            "resolution_minutes": cfg.resolution_minutes,
            "rearm_minutes": cfg.rearm_minutes,
        },
    }
    n = write_jsonl(args.out, records)
    with open(args.out + ".meta.json", "w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2, default=str)
    print(f"wrote {n} events to {args.out}")
    print(f"wrote provenance to {args.out}.meta.json")
    if not n:
        print("\nNo events. That is a finding about the window, not an error.")
    return 0


def _rows_from_records(records: Sequence[dict[str, Any]], side: Optional[str]) -> list[Row]:
    rows: list[Row] = []
    for r in records:
        if r.get("outcome") not in ("broke", "held"):
            continue  # censored events are excluded, never folded into 'held'
        if side and r.get("side") != side:
            continue
        rows.append(
            Row(
                session=_iso(str(r["session"])[:10]),
                side=str(r.get("side")),
                broke=1 if r.get("outcome") == "broke" else 0,
                features=dict(r.get("features") or {}),
            )
        )
    rows.sort(key=lambda x: x.session)
    return rows


def _observations(records: Sequence[dict[str, Any]], horizon: Optional[int]) -> list[Any]:
    """Survival observations, deriving the watch time when it is absent.

    Datasets written before ``observed_minutes`` existed can still be analysed:
    a break was watched until it confirmed, a censored test until the bell, and
    a hold for the full horizon. Derived values are approximations of what the
    labeller recorded directly, so rebuilding is still preferable.
    """
    from research.wall_break_odds.survival import Observation

    out: list[Observation] = []
    for r in records:
        outcome = r.get("outcome")
        if outcome not in ("broke", "held", "censored"):
            continue
        minutes = r.get("observed_minutes")
        if minutes is None:
            if outcome == "broke":
                minutes = r.get("minutes_to_resolve")
            elif outcome == "censored":
                tested = r.get("tested_at")
                try:
                    ts = datetime.fromisoformat(str(tested)).astimezone(ET)
                    minutes = max((16 * 60) - (ts.hour * 60 + ts.minute), 0)
                except (TypeError, ValueError):
                    minutes = None
            else:
                minutes = horizon
        if minutes is None:
            continue
        out.append(Observation(minutes=float(minutes), broke=outcome == "broke"))
    return out


def cmd_analyze(args: argparse.Namespace) -> int:
    from research.wall_break_odds.dataset import read_jsonl

    records = read_jsonl(args.dataset)
    meta: dict[str, Any] = {}
    try:
        with open(args.dataset + ".meta.json", "r", encoding="utf-8") as fh:
            meta = json.load(fh)
    except OSError:
        meta = {"symbol": "?", "start": "?", "end": "?", "events_total": len(records)}

    for side in ([None] if not args.by_side else [None, "call", "put"]):
        rows = _rows_from_records(records, side)
        if side:
            print("\n\n" + "#" * 78)
            print(f"# {side.upper()} WALL ONLY")
            print("#" * 78 + "\n")
        from research.wall_break_odds.survival import kaplan_meier

        subset = [r for r in records if not side or r.get("side") == side]
        horizon = (meta.get("config") or {}).get("resolution_minutes")
        obs = _observations(subset, horizon)
        report = render_report(
            meta,
            base_rate(rows),
            univariate_screen(rows),
            evaluate(rows, n_folds=args.folds),
            fit_full(rows),
            survival=(kaplan_meier(obs), len(obs), sum(1 for o in obs if o.broke)),
        )
        print(report)
        if args.out and side is None:
            with open(args.out, "w", encoding="utf-8") as fh:
                fh.write(report)
            print(f"\nwrote {args.out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m research.wall_break_odds.cli",
        description="P(break | tested) for call and put gamma walls. Research only.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("selftest", help="synthetic end-to-end check; no database needed")
    p.add_argument("--sessions", type=int, default=400)
    p.add_argument("--seed", type=int, default=7)
    p.set_defaults(func=cmd_selftest)

    p = sub.add_parser("build-dataset", help="label wall tests over a date range (read-only)")
    p.add_argument("symbol")
    p.add_argument("--start", type=_iso, required=True, help="YYYY-MM-DD, ET session date")
    p.add_argument("--end", type=_iso, required=True)
    p.add_argument("--out", required=True, help="JSONL output path")
    p.add_argument("--strike-step", type=float, default=5.0, help="strike ladder step")
    p.add_argument("--no-flow", action="store_true", help="skip the flow_by_contract reads")
    _add_event_args(p)
    p.set_defaults(func=cmd_build_dataset)

    p = sub.add_parser("analyze", help="base rates, screen, walk-forward, from a saved dataset")
    p.add_argument("dataset")
    p.add_argument("--folds", type=int, default=5)
    p.add_argument("--by-side", action="store_true", help="also report call and put separately")
    p.add_argument("--out", help="write the pooled report to this path as well")
    p.set_defaults(func=cmd_analyze)
    return parser


def _add_event_args(p: argparse.ArgumentParser) -> None:
    """The label definition, exposed so a result can be re-run under a
    different one. A break under a 10-minute confirmation is a pierce under a
    20-minute one; anyone quoting a number from this study should be able to
    check how much it moves."""
    p.add_argument("--touch", type=float, default=5.0, help="touch band, basis points")
    p.add_argument("--buffer", type=float, default=5.0, help="break buffer, basis points")
    p.add_argument("--confirm", type=int, default=10, help="minutes of confirmation for a break")
    p.add_argument("--horizon", type=int, default=60, help="minutes a test gets to resolve")
    p.add_argument("--rearm", type=int, default=15, help="cooldown before the wall re-arms")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
