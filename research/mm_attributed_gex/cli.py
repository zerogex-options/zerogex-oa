"""Command-line entry point for the MM-Attributed GEX experiment.

Run from the repository root::

    python -m research.mm_attributed_gex.cli <command> [options]

Commands, in the order you would use them on real data::

    inspect-cboe   read a delivered Cboe file and propose a column mapping
    check-load     parse files with a profile and report what came through
    reconstruct    build MM inventory and report completeness + reconciliation
    build-dataset  produce the side-by-side existing-vs-MM research dataset
    backtest       run the experiment battery over a dataset
    report         render the research report from a backtest result
    pipeline-check plumbing self-test on synthetic data (NOT a research result)

Everything that touches the database is read-only.  No command writes to any
production table.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

logger = logging.getLogger("mm_attributed_gex")


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbol", default="SPX", help="underlying (default: SPX)")
    parser.add_argument(
        "--profile",
        default="cboe_open_close_v1",
        help="built-in profile name or path to a saved JSON profile",
    )
    parser.add_argument(
        "--allow-unconfirmed",
        action="store_true",
        help="parse with a profile that has not been confirmed against a real file "
        "(exploratory only — results must not be reported)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")


def _load_profile(name: str):
    from research.mm_attributed_gex.cboe.profiles import load_profile_file

    return load_profile_file(name)


def _parse_dt(text: str) -> datetime:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# inspect-cboe
# ---------------------------------------------------------------------------


def cmd_inspect_cboe(args: argparse.Namespace) -> int:
    from research.mm_attributed_gex.cboe.inspect import propose_profile, render_proposal
    from research.mm_attributed_gex.cboe.profiles import save_profile_file

    proposal = propose_profile(args.file, default_symbol=args.symbol)
    print(render_proposal(proposal))
    if proposal.profile and args.save:
        path = save_profile_file(proposal.profile, args.save)
        print(f"\nProposed profile written to {path}")
        print(
            "Review every mapping against the vendor's field documentation, then set "
            '"confirmed": true in that file before using it for research.'
        )
    return 0 if proposal.usable else 2


# ---------------------------------------------------------------------------
# check-load
# ---------------------------------------------------------------------------


def cmd_check_load(args: argparse.Namespace) -> int:
    from research.mm_attributed_gex.cboe.loader import LoadResult, load_paths

    profile = _load_profile(args.profile)
    result = LoadResult()
    count = 0
    sample: list[Any] = []
    for rec in load_paths(
        args.files, profile, allow_unconfirmed=args.allow_unconfirmed, result=result
    ):
        count += 1
        if len(sample) < 5:
            sample.append(rec)
    print(json.dumps(result.as_dict(), indent=2))
    print(f"\nrecords yielded: {count}")
    if sample:
        print("\nfirst records:")
        for rec in sample:
            print(
                f"  {rec.trading_date} {rec.timestamp.isoformat()} "
                f"{rec.symbol} {rec.expiration} {rec.strike:g}{rec.option_type} "
                f"{rec.participant.value}/{rec.side.value}/{rec.position_effect.value} "
                f"x{rec.contracts}"
            )
    if not result.mm_contracts_total:
        print(
            "\nNO Market Maker contracts were parsed. Either the file has no MM columns "
            "or the profile does not map them — re-run inspect-cboe.",
            file=sys.stderr,
        )
        return 2
    return 0


# ---------------------------------------------------------------------------
# reconstruct
# ---------------------------------------------------------------------------


def cmd_reconstruct(args: argparse.Namespace) -> int:
    from research.mm_attributed_gex.cboe.loader import load_paths
    from research.mm_attributed_gex.inventory import InventoryReconstructor
    from research.mm_attributed_gex import reconcile as reconcile_mod

    profile = _load_profile(args.profile)
    records = list(load_paths(args.files, profile, allow_unconfirmed=args.allow_unconfirmed))
    if not records:
        print("No records parsed; nothing to reconstruct.", file=sys.stderr)
        return 2

    listing_dates: dict = {}
    observed_oi: dict = {}
    if not args.no_db:
        try:
            from research.mm_attributed_gex.sources import (
                fetch_open_interest_series,
                fetch_series_listing_dates,
                research_connection,
            )

            expirations = sorted({r.expiration for r in records})
            sessions = sorted({r.trading_date for r in records})
            with research_connection() as conn:
                listing_dates = fetch_series_listing_dates(conn, args.symbol, expirations)
                observed_oi = fetch_open_interest_series(
                    conn, args.symbol, expirations, sessions[0], sessions[-1]
                )
            print(
                f"listing dates resolved for {len(listing_dates)} series; "
                f"{len(observed_oi)} open-interest observations loaded"
            )
        except Exception as exc:  # noqa: BLE001 - the DB is optional here
            print(
                f"proceeding without database evidence ({exc}); censoring falls back to "
                "the window heuristic and the open-interest identity cannot be checked",
                file=sys.stderr,
            )

    recon = InventoryReconstructor(symbol=args.symbol, listing_dates=listing_dates)
    recon.consume(records).finalize()
    summary = recon.summary()
    reconciliation = reconcile_mod.summarize(records, observed_oi or None)

    print(
        json.dumps(
            {"reconstruction": summary, "reconciliation": reconciliation.as_dict()},
            indent=2,
            default=str,
        )
    )
    if args.out:
        payload = {
            "reconstruction": summary,
            "reconciliation": reconciliation.as_dict(),
            "positions": [p.as_dict() for p in recon.positions()],
        }
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print(f"\nwritten to {args.out}")
    return 0


# ---------------------------------------------------------------------------
# build-dataset
# ---------------------------------------------------------------------------


def cmd_build_dataset(args: argparse.Namespace) -> int:
    from research.mm_attributed_gex.cboe.loader import load_paths
    from research.mm_attributed_gex.dataset import (
        DatasetSpec,
        build_dataset,
        write_csv,
        write_jsonl,
    )
    from research.mm_attributed_gex.sources import (
        fetch_chain_snapshot,
        fetch_composite_scores,
        fetch_production_summaries,
        fetch_series_listing_dates,
        fetch_snapshot_timestamps,
        fetch_underlying_bars,
        fetch_vix_closes,
        research_connection,
    )

    profile = _load_profile(args.profile)
    files = list(args.files)

    def records_factory():
        return load_paths(files, profile, allow_unconfirmed=args.allow_unconfirmed)

    start = _parse_dt(args.start)
    end = _parse_dt(args.end)

    with research_connection() as conn:
        stamps = fetch_snapshot_timestamps(
            conn, args.symbol, start, end, step_minutes=args.step_minutes
        )
        if not stamps:
            print(
                f"No gex_summary timestamps for {args.symbol} in "
                f"[{args.start}, {args.end}] — nothing to compare against.",
                file=sys.stderr,
            )
            return 2
        print(f"{len(stamps)} snapshot timestamps")

        summaries = fetch_production_summaries(conn, args.symbol, start, end)
        composites = fetch_composite_scores(conn, args.symbol, start, end)
        bars = fetch_underlying_bars(conn, args.symbol, start, end)
        vix = dict(fetch_vix_closes(conn, start, end))
        expirations = sorted({r.expiration for r in records_factory()})
        listing_dates = fetch_series_listing_dates(conn, args.symbol, expirations)

        spot_by_ts = {ts: close for ts, _o, _h, _l, close in bars}
        sorted_bar_ts = sorted(spot_by_ts)

        def spot_provider(ts: datetime) -> Optional[float]:
            import bisect

            i = bisect.bisect_right(sorted_bar_ts, ts) - 1
            if i < 0:
                return None
            found = sorted_bar_ts[i]
            if (ts - found) > timedelta(minutes=15):
                return None
            return spot_by_ts[found]

        chain_cache: dict[datetime, list] = {}

        def chain_provider(ts: datetime):
            cached = chain_cache.get(ts)
            if cached is None:
                cached = fetch_chain_snapshot(conn, args.symbol, ts)
                chain_cache[ts] = cached
            return cached

        def summary_provider(ts: datetime):
            return summaries.get(ts)

        def composite_provider(ts: datetime):
            return composites.get(ts)

        sorted_vix = sorted(vix)

        def vix_provider(ts: datetime) -> Optional[float]:
            import bisect

            i = bisect.bisect_right(sorted_vix, ts) - 1
            return vix[sorted_vix[i]] if i >= 0 else None

        spec = DatasetSpec(
            symbol=args.symbol,
            headline_universe=args.headline_universe,
            clean_only=not args.include_censored,
            apply_horizon_weighting=not args.raw_positioning,
            use_net_flow_estimator=args.net_flow_estimator,
        )

        def progress(done: int, total: int) -> None:
            if done % 50 == 0 or done == total:
                print(f"  {done}/{total} snapshots", flush=True)

        rows, provenance = build_dataset(
            records_factory,
            stamps,
            chain_provider,
            spot_provider,
            spec=spec,
            summary_provider=summary_provider,
            composite_provider=composite_provider,
            vix_provider=vix_provider,
            listing_dates=listing_dates,
            progress=progress,
        )

    out = Path(args.out)
    write_jsonl(rows, out)
    write_csv(rows, out.with_suffix(".csv"))
    out.with_name(out.stem + "_provenance.json").write_text(
        json.dumps(provenance, indent=2, default=str), encoding="utf-8"
    )
    print(json.dumps(provenance, indent=2, default=str))
    print(f"\n{len(rows)} rows -> {out} (+ .csv, + _provenance.json)")
    return 0 if rows else 2


# ---------------------------------------------------------------------------
# backtest
# ---------------------------------------------------------------------------


def cmd_backtest(args: argparse.Namespace) -> int:
    from research.mm_attributed_gex.backtest import ExperimentConfig, run_experiment
    from research.mm_attributed_gex.dataset import read_jsonl
    from research.mm_attributed_gex.outcomes import Bar, BarSeries
    from research.mm_attributed_gex.report import write_report
    from research.mm_attributed_gex.sources import fetch_underlying_bars, research_connection

    rows = read_jsonl(args.dataset)
    if not rows:
        print(f"{args.dataset} is empty", file=sys.stderr)
        return 2
    stamps = [datetime.fromisoformat(r["timestamp"]) for r in rows]
    start, end = min(stamps) - timedelta(days=1), max(stamps) + timedelta(days=3)

    with research_connection() as conn:
        raw_bars = fetch_underlying_bars(conn, args.symbol, start, end)
    series = BarSeries([Bar(ts, o, h, low, c) for ts, o, h, low, c in raw_bars])
    print(f"{len(series)} underlying bars loaded")

    provenance_path = Path(args.dataset).with_name(Path(args.dataset).stem + "_provenance.json")
    provenance = (
        json.loads(provenance_path.read_text(encoding="utf-8"))
        if provenance_path.exists()
        else None
    )

    config = ExperimentConfig(
        horizons=tuple(int(h) for h in args.horizons.split(",")),
        n_boot=args.n_boot,
    )
    result = run_experiment(rows, series, config=config, sampling_minutes=args.step_minutes)
    path = write_report(result, args.out, provenance=provenance)
    print(f"\nreport -> {path}\nresult  -> {path.with_suffix('.json')}")
    from research.mm_attributed_gex.report import decide

    verdict = decide(result)
    print(f"\nVERDICT: {verdict.label}")
    for reason in verdict.rationale:
        print(f"  - {reason}")
    return 0


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


def cmd_report(args: argparse.Namespace) -> int:
    from research.mm_attributed_gex.backtest import ExperimentResult
    from research.mm_attributed_gex.report import render_markdown

    payload = json.loads(Path(args.result).read_text(encoding="utf-8"))
    raw = payload.get("result", payload)
    result = ExperimentResult(
        **{k: v for k, v in raw.items() if k in ExperimentResult.__annotations__}
    )
    text = render_markdown(
        result,
        provenance=payload.get("provenance"),
        reconciliation=payload.get("reconciliation"),
    )
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"report -> {args.out}")
    else:
        print(text)
    return 0


# ---------------------------------------------------------------------------
# pipeline-check
# ---------------------------------------------------------------------------


def cmd_pipeline_check(args: argparse.Namespace) -> int:
    """End-to-end plumbing check on synthetic data.

    This proves the pipeline runs and the layers fit together.  It says
    NOTHING about whether MM-attributed GEX works — the inputs are invented.
    The output is labelled accordingly and must never be quoted as a result.
    """
    from research.mm_attributed_gex.selftest import run_pipeline_check

    payload = run_pipeline_check(symbol=args.symbol)
    print(json.dumps(payload, indent=2, default=str))
    print(
        "\nPLUMBING CHECK ONLY — inputs are synthetic. This is not evidence "
        "about the methodology's value."
    )
    return 0 if payload.get("ok") else 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m research.mm_attributed_gex.cli",
        description="Market-Maker Attributed GEX research experiment (research-only).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("inspect-cboe", help="propose a column mapping from a real file")
    p.add_argument("file")
    p.add_argument("--save", help="write the proposed profile to this JSON path")
    _add_common(p)
    p.set_defaults(func=cmd_inspect_cboe)

    p = sub.add_parser("check-load", help="parse files and report what came through")
    p.add_argument("files", nargs="+")
    _add_common(p)
    p.set_defaults(func=cmd_check_load)

    p = sub.add_parser("reconstruct", help="build MM inventory and report completeness")
    p.add_argument("files", nargs="+")
    p.add_argument("--out", help="write full positions JSON here")
    p.add_argument("--no-db", action="store_true", help="skip database evidence")
    _add_common(p)
    p.set_defaults(func=cmd_reconstruct)

    p = sub.add_parser("build-dataset", help="produce the side-by-side research dataset")
    p.add_argument("files", nargs="+", help="Cboe Open-Close files or directories")
    p.add_argument("--start", required=True, help="ISO start timestamp")
    p.add_argument("--end", required=True, help="ISO end timestamp")
    p.add_argument("--out", default="research_output/mm_attributed_dataset.jsonl")
    p.add_argument("--step-minutes", type=int, default=5)
    p.add_argument("--headline-universe", default="near_term")
    p.add_argument(
        "--include-censored",
        action="store_true",
        help="include left-censored series in the headline numbers (partial-data arm)",
    )
    p.add_argument(
        "--raw-positioning",
        action="store_true",
        help="drop ZeroGEX's horizon-occupancy weighting (raw MM positioning arm)",
    )
    p.add_argument(
        "--net-flow-estimator",
        action="store_true",
        help="use buys−sells ignoring the open/close flag",
    )
    _add_common(p)
    p.set_defaults(func=cmd_build_dataset)

    p = sub.add_parser("backtest", help="run the experiment battery over a dataset")
    p.add_argument("dataset")
    p.add_argument("--out", default="research_output/mm_attributed_report.md")
    p.add_argument("--horizons", default="5,15,30,60")
    p.add_argument("--step-minutes", type=int, default=5)
    p.add_argument("--n-boot", type=int, default=2000)
    _add_common(p)
    p.set_defaults(func=cmd_backtest)

    p = sub.add_parser("report", help="render a report from a saved result JSON")
    p.add_argument("result")
    p.add_argument("--out")
    _add_common(p)
    p.set_defaults(func=cmd_report)

    p = sub.add_parser("pipeline-check", help="synthetic end-to-end plumbing check")
    _add_common(p)
    p.set_defaults(func=cmd_pipeline_check)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if getattr(args, "verbose", False) else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    # The production analytics kernels log one INFO line per resolved gamma
    # flip. That is right for a once-a-minute service and useless here, where
    # the same kernel runs thousands of times per research run. Raising the
    # level on
    # THIS process's logger changes no production configuration.
    if not getattr(args, "verbose", False):
        logging.getLogger("src.analytics.main_engine").setLevel(logging.WARNING)
    try:
        return int(args.func(args))
    except KeyboardInterrupt:  # pragma: no cover
        return 130
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        logger.error("%s", exc, exc_info=getattr(args, "verbose", False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
