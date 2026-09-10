"""Command line for the opening-range × gamma-confluence study.

Follows the convention of the repo's other research packages
(``python -m research.<pkg>.cli <command>``), and the same discipline: every
database command is read-only, every output is a file, and ``selftest`` needs
no database at all.

    # 0. Plumbing only — no database, no market data, invented numbers.
    python -m research.or_gamma_confluence.cli selftest

    # 1. How much history is actually there, and is the publish clock usable?
    python -m research.or_gamma_confluence.cli coverage

    # 2. Label touch events over a window (read-only against production).
    python -m research.or_gamma_confluence.cli build-dataset NQ NDX \\
        --start 2026-06-01 --end 2026-09-05 \\
        --out research_output/orgc_events.jsonl

    # 3. Cohorts, sensitivity grids, chronological out-of-sample.
    python -m research.or_gamma_confluence.cli analyze \\
        research_output/orgc_events.jsonl --out research_output/orgc_report.md

    # 4. Parameter neighbourhoods, not a single winning value.
    python -m research.or_gamma_confluence.cli sweep NQ \\
        --start 2026-06-01 --end 2026-09-05
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any, Optional, Sequence

from research.or_gamma_confluence import sources
from research.or_gamma_confluence.config import (
    AVAILABILITY_CLOCKS,
    CLOCK_VISIBLE,
    EXTENSION_MODES,
    MODE_BOUNDARY,
    ResearchConfig,
)
from research.or_gamma_confluence.dataset import build_dataset, read_jsonl, read_meta, write_jsonl
from research.or_gamma_confluence.features import TREND_FILTERS
from research.or_gamma_confluence.instruments import DEFAULT_SYMBOLS, known_symbols
from research.or_gamma_confluence.report import build_summary, render_markdown, write_csv
from research.or_gamma_confluence.selftest import run_selftest

logger = logging.getLogger("or_gamma_confluence")


def _iso(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from None


def _config_from_args(args: argparse.Namespace) -> ResearchConfig:
    """Every CLI knob resolved into ONE frozen config.

    Nothing downstream reads ``args``; a run is identified by this object's
    fingerprint, so a parameter that never reaches here can never appear in a
    result's provenance.
    """
    kwargs: dict[str, Any] = {}
    for attr, field in (
        ("or_minutes", "opening_range_minutes"),
        ("extension_mode", "extension_mode"),
        ("extension_step", "extension_step"),
        ("max_extension", "max_extension"),
        ("touch_bp", "touch_tolerance_bp"),
        ("gamma_lead", "gamma_min_lead_seconds"),
        ("clock", "availability_clock"),
        ("poll_lag", "client_poll_lag_seconds"),
        ("trend_filter", "trend_filter"),
        ("rearm", "rearm_minutes"),
        ("min_frames", "min_session_frames"),
        ("label", "label"),
    ):
        value = getattr(args, attr, None)
        if value is not None:
            kwargs[field] = value
    if getattr(args, "no_gex_ranks", False):
        kwargs["use_gex_ranks"] = False
    return ResearchConfig(**kwargs)


def cmd_selftest(args: argparse.Namespace) -> int:
    return run_selftest(sessions=args.sessions, seed=args.seed)


def cmd_coverage(args: argparse.Namespace) -> int:
    """How much history exists, per symbol and per table.

    Answers the question the code cannot: ``gex_summary`` and
    ``underlying_quotes`` are retention-exempt while ``gex_by_strike`` is
    pruned, so the wall/flip arm and the ranked-GEX arm have different windows
    — and both are deployment-specific.  Also reports whether ``created_at`` is
    a usable publish clock or has been made fiction by backfilling.
    """
    cfg = _config_from_args(args)
    symbols = args.symbols or list(DEFAULT_SYMBOLS)
    with sources.research_connection() as conn:
        payload = sources.coverage(conn, symbols, cfg)

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"wrote {args.out}")

    print("\n=== History depth ===")
    print(f"{'symbol':<7}{'table':<20}{'sessions':>9}{'rows':>12}  window")
    for sym, block in payload["symbols"].items():
        for table, span in block["tables"].items():
            if not span.get("available"):
                print(
                    f"{sym:<7}{table:<20}{'—':>9}{'—':>12}  UNAVAILABLE: "
                    f"{span.get('error', '')[:50]}"
                )
                continue
            lo = (span.get("earliest") or "—")[:10]
            hi = (span.get("latest") or "—")[:10]
            print(f"{sym:<7}{table:<20}{span['sessions']:>9}{span['rows']:>12}  {lo} .. {hi}")

    print("\n=== Is created_at a usable publish clock? ===")
    print(
        f"{'book':<7}{'rows':>10}{'null%':>8}{'p50 s':>9}{'p95 s':>9}"
        f"{'max s':>10}{'neg':>6}{'backfill%':>11}  verdict"
    )
    seen: set[str] = set()
    for sym, block in payload["symbols"].items():
        book = block["spec"]["gamma_symbol"]
        if book in seen:
            continue
        seen.add(book)
        c = block["publish_clock"]
        if not c.get("available") or not c.get("rows"):
            print(f"{book:<7}{'—':>10}  no gex_summary rows in the lag window")
            continue
        bad = (c.get("created_at_null_pct") or 0) > 1 or (c.get("backfilled_pct") or 0) > 5
        verdict = (
            "USE 'data' CLOCK — publish clock unreliable" if bad else "publish/visible clocks OK"
        )
        print(
            f"{book:<7}{c['rows']:>10}{c.get('created_at_null_pct', 0):>8.2f}"
            f"{(c.get('lag_p50_s') or 0):>9.1f}{(c.get('lag_p95_s') or 0):>9.1f}"
            f"{(c.get('lag_max_s') or 0):>10.1f}{c.get('negative_lag_rows', 0):>6}"
            f"{c.get('backfilled_pct', 0):>11.2f}  {verdict}"
        )
    print("\n=== Usable window (what actually bounds the study) ===")
    print(f"{'symbol':<7}{'gamma':>7}{'bars':>7}{'usable':>8}{'ranked':>8}  bound by")
    for sym, block in payload["symbols"].items():
        tables = block["tables"]
        gamma = (tables.get("gex_summary") or {}).get("sessions") or 0
        ranked = (tables.get("gex_by_strike") or {}).get("sessions") or 0
        bar_table = (
            "futures_quotes" if block["spec"]["bar_source"] == "futures" else "underlying_quotes"
        )
        bars = (tables.get(bar_table) or {}).get("sessions") or 0
        usable = min(gamma, bars) if gamma and bars else 0
        bound = "gamma frames" if gamma <= bars else "bars"
        flag = "  <-- THIN" if usable and usable < 40 else ""
        print(f"{sym:<7}{gamma:>7}{bars:>7}{usable:>8}{ranked:>8}  {bound}{flag}")

    print("\nNotes:")
    print("  * 'usable' is the intersection of gamma frames and bars — the real")
    print("    ceiling on sessions. 'ranked' is the gex_by_strike window, which")
    print("    caps the ranked-GEX ('GEX #4') confluence arm ONLY; every wall /")
    print("    flip / max-pain / pin cohort runs on the full 'usable' window.")
    print("    Run with --no-gex-ranks to use the long arm alone.")
    print("  * SESSIONS, not events, are the independent unit: the cohort")
    print("    comparison resamples whole sessions, so a symbol under ~40")
    print("    sessions will not separate cohorts however many touches it has.")
    print("  * 'backfill%' counts rows whose created_at - timestamp exceeds")
    print(
        f"    max_publish_lag_seconds ({cfg.max_publish_lag_seconds}s ="
        f" {cfg.max_publish_lag_seconds / 3600:.0f}h). Those rows were written long"
    )
    print("    after the fact, so created_at is a backfill time rather than a")
    print("    publish time, and each such FRAME is dropped. A session is only")
    print(f"    failed closed when over {cfg.max_rejected_frame_frac:.0%} of its frames are")
    print("    unusable — an isolated slow publish costs a frame, not a day.")
    print("  * A p50 lag near 30-40s means the 'visible' clock sits roughly")
    print("    60-70s behind the 'data' clock. That gap is larger than half the")
    print("    lead-time sweep, so the clock choice is not a detail.")
    return 0


def cmd_build_dataset(args: argparse.Namespace) -> int:
    cfg = _config_from_args(args)
    symbols = args.symbols or list(DEFAULT_SYMBOLS)
    print(f"config: {cfg.describe()}")
    print(f"symbols: {', '.join(symbols)}   window: {args.start} .. {args.end}")
    with sources.research_connection() as conn:
        meta = write_jsonl(
            args.out,
            build_dataset(conn, symbols, args.start, args.end, cfg),
            cfg,
            symbols=symbols,
            start=args.start,
            end=args.end,
        )
    print(f"\nwrote {meta['rows']} events -> {args.out}")
    print(f"provenance -> {args.out}.meta.json")
    for sym, acc in meta["per_symbol"].items():
        print(
            f"  {sym:<5} sessions {acc['sessions_used']}/{acc['sessions_seen']} "
            f"events {acc['events']} (no-gamma {acc['events_without_gamma']}) "
            f"gex-rank frames {acc['gex_rank_frames']}"
        )
    if meta["skip_reasons"]:
        print(f"  skips: {json.dumps(meta['skip_reasons'])}")
    if meta["rows"] == 0:
        print(
            "\nNo events. That is a coverage result, not an absence of effect — "
            "check the skip reasons above and `cli coverage`."
        )
    return 0


def cmd_analyze(args: argparse.Namespace) -> int:
    rows: list[dict[str, Any]] = []
    meta: Optional[dict[str, Any]] = None
    for path in args.datasets:
        rows.extend(read_jsonl(path))
        meta = meta or read_meta(path)
    if not rows:
        print("no events in the supplied dataset(s)")
        return 1

    # Rebuild the config from the dataset's own provenance where possible, so
    # an analysis cannot silently describe rows built under other parameters.
    cfg = _config_from_args(args)
    if meta and meta.get("config_fingerprint"):
        stored = meta["config_fingerprint"]
        row_prints = {r.get("config_fingerprint") for r in rows if r.get("config_fingerprint")}
        if row_prints and row_prints != {stored}:
            print(f"REFUSING: dataset mixes config fingerprints {sorted(row_prints)}")
            return 2
        if not args.force and row_prints and stored not in row_prints:
            print(f"REFUSING: meta fingerprint {stored} not present in rows")
            return 2

    summary = build_summary(rows, cfg, meta, confluence_distance=args.confluence_distance)
    markdown = render_markdown(summary)
    print(markdown)

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(markdown, encoding="utf-8")
        print(f"\nwrote {args.out}")
    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        print(f"wrote {args.json_out}")
    if args.csv_out:
        n = write_csv(rows, args.csv_out)
        print(f"wrote {n} rows -> {args.csv_out}")
    return 0


def _cell_metrics(rows, cfg, confluence_distance: float) -> dict[str, Any]:
    """The three questions a sweep cell has to answer, as numbers.

    Deliberately not "which cell had the best rate": a sweep is a stability
    check, and the thing being checked is whether each EFFECT survives moving
    its parameters, not whether some cell can be found where the number is
    large.
    """
    from research.msi_regime_excursion import stats as _stats
    from research.or_gamma_confluence.cohorts import (
        build_cohorts,
        compare_to_baseline,
        has_confluence,
    )
    from research.or_gamma_confluence.events import OUTCOME_REVERSAL, RESOLVED_OUTCOMES

    resolved = [r for r in rows if r.get("outcome") in RESOLVED_OUTCOMES]
    out: dict[str, Any] = {
        "n": len(resolved),
        "sessions": len({r.get("session") for r in resolved}),
        "reversal_rate": (
            sum(1 for r in resolved if r.get("outcome") == OUTCOME_REVERSAL) / len(resolved)
            if resolved
            else None
        ),
    }
    # H1, as one number: does reversion rise with extension depth? Spearman on
    # (depth, reversal) per event. Positive = the rubber-band claim.
    depths = [abs(float(r.get("extension_k") or 0.0)) for r in resolved]
    hits = [1.0 if r.get("outcome") == OUTCOME_REVERSAL else 0.0 for r in resolved]
    out["depth_rho"] = _stats.spearman(depths, hits) if len(resolved) > 10 else None

    # H3, as one number: the confluence effect against the unconditional base.
    if resolved:
        conf = compare_to_baseline(
            resolved, lambda r: has_confluence(r, confluence_distance), iterations=600
        )
        out["confluence_diff"] = conf.get("clustered_diff")
        out["confluence_p"] = conf.get("clustered_p")
    # H2, as one number: broken ladders vs the base rate.
    cohorts = {
        c.key: c
        for c in build_cohorts(
            confluence_distance=confluence_distance,
            min_extension=cfg.min_extension_for_reversion,
            min_broken=cfg.min_failed_extensions_for_continuation,
        )
    }
    broken = cohorts.get("prior_broken")
    if broken is not None and resolved:
        b = compare_to_baseline(resolved, broken.predicate, iterations=600)
        out["broken_diff"] = b.get("clustered_diff")
        out["broken_p"] = b.get("clustered_p")
        out["broken_n"] = b.get("n")
    return out


def cmd_sweep(args: argparse.Namespace) -> int:
    """Parameter neighbourhoods, reported as a surface rather than a winner.

    One dataset is built per cell, because each of these axes changes what the
    dataset IS:

    * ``or_minutes`` and ``extension_step`` move the opening range and the
      ladder, so the touch events themselves differ;
    * ``gamma_lead`` selects a different gamma snapshot per touch
      (``dataset.py`` passes it to ``GammaTimeline.as_of``), so the stored
      confluence distances differ.

    Only ``confluence_distance`` is genuinely re-cohortable from a saved
    build, because raw distances are stored — and that axis is therefore swept
    in ``analyze`` instead, where every cell provably sees identical events.

    Each cell reports one number per hypothesis: a Spearman rho for "deeper
    reverts more", and clustered effects for confluence and for broken
    ladders.  Read down a column for stability; a value that exists in one
    cell and not its neighbours is noise.
    """
    symbols = args.symbols or list(DEFAULT_SYMBOLS)
    base = _config_from_args(args)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    grid = [
        (o, st, ld) for o in args.or_minutes_grid for st in args.step_grid for ld in args.lead_grid
    ]
    print(
        f"{len(grid)} cells x {len(symbols)} symbols. Each cell is a full "
        f"rebuild; budget several minutes per cell."
    )
    if base.use_gex_ranks:
        print(
            "Tip: --no-gex-ranks skips the gex_by_strike reads and is much "
            "faster. It drops the ranked-GEX levels only; wall / flip / "
            "max-pain / pin confluence is unaffected."
        )

    cells: list[dict[str, Any]] = []
    with sources.research_connection() as conn:
        for or_min, step, lead in grid:
            cfg = base.variant(
                opening_range_minutes=or_min,
                extension_step=step,
                gamma_min_lead_seconds=lead,
            )
            path = outdir / f"orgc_or{or_min}_s{step:g}_l{lead}_{cfg.fingerprint()}.jsonl"
            meta = write_jsonl(
                path,
                build_dataset(conn, symbols, args.start, args.end, cfg),
                cfg,
                symbols=symbols,
                start=args.start,
                end=args.end,
            )
            m = _cell_metrics(read_jsonl(path), cfg, args.confluence_distance)
            cells.append(
                {
                    "or_minutes": or_min,
                    "extension_step": step,
                    "gamma_lead": lead,
                    "fingerprint": cfg.fingerprint(),
                    "path": str(path),
                    "rows": meta["rows"],
                    **m,
                }
            )
            rate_txt = "—" if m["reversal_rate"] is None else f"{m['reversal_rate'] * 100:.1f}%"
            print(
                f"  OR={or_min:>2}m step={step:g}R lead={lead:>3}s -> "
                f"n={m['n']:>5} reversal={rate_txt}"
            )

    grid_path = outdir / "orgc_sweep.json"
    grid_path.write_text(json.dumps(cells, indent=2, default=str), encoding="utf-8")

    def _f(v, places=3, scale=1.0, suffix=""):
        return "—" if v is None else f"{v * scale:+.{places}f}{suffix}"

    print(f"\n=== Sweep surface ({len(cells)} cells) -> {grid_path} ===")
    print(
        f"{'OR':>4}{'step':>7}{'lead':>6}{'n':>7}{'days':>6}{'reversal':>10}"
        f"{'H1 rho':>9}{'H3 conf':>10}{'  p':>7}{'H2 broken':>11}{'  p':>7}"
    )

    def _p(v):
        return "—" if v is None else f"{v:.3f}"

    for c in cells:
        rate = "—" if c["reversal_rate"] is None else f"{c['reversal_rate'] * 100:.1f}%"
        print(
            f"{c['or_minutes']:>4}{c['extension_step']:>7g}{c['gamma_lead']:>6}"
            f"{c['n']:>7}{c['sessions']:>6}{rate:>10}"
            f"{_f(c.get('depth_rho')):>9}"
            f"{_f(c.get('confluence_diff'), 1, 100, 'pt'):>10}"
            f"{_p(c.get('confluence_p')):>7}"
            f"{_f(c.get('broken_diff'), 1, 100, 'pt'):>11}"
            f"{_p(c.get('broken_p')):>7}"
        )
    print("\nH1 rho  : Spearman(extension depth, reversal). Positive supports")
    print("          the rubber-band claim; near zero falsifies it.")
    print("H3 conf : confluence effect vs the unconditional base rate.")
    print("H2 broken: broken-ladder effect vs the same base. Negative means")
    print("          less reversion, i.e. continuation.")
    print("\nRead DOWN each column. An effect that appears in one cell and not")
    print("its neighbours is noise; a real one survives its own parameters.")
    print("No multiplicity correction across cells -- with this many, expect")
    print("one p<0.05 by chance.")
    return 0


def _add_config_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--or-minutes",
        dest="or_minutes",
        type=int,
        help="opening range length in minutes (default 5)",
    )
    p.add_argument(
        "--extension-mode",
        dest="extension_mode",
        choices=EXTENSION_MODES,
        help=f"ladder anchor (default {MODE_BOUNDARY})",
    )
    p.add_argument(
        "--extension-step",
        dest="extension_step",
        type=float,
        help="ladder increment in units of R (default 0.5)",
    )
    p.add_argument(
        "--max-extension",
        dest="max_extension",
        type=float,
        help="furthest rung in units of R (default 10)",
    )
    p.add_argument(
        "--touch-bp", dest="touch_bp", type=float, help="touch band in basis points (default 1.0)"
    )
    p.add_argument(
        "--gamma-lead",
        dest="gamma_lead",
        type=int,
        help="minimum gamma lead time in seconds (default 120)",
    )
    p.add_argument(
        "--clock",
        dest="clock",
        choices=AVAILABILITY_CLOCKS,
        help=f"availability clock (default {CLOCK_VISIBLE})",
    )
    p.add_argument(
        "--poll-lag",
        dest="poll_lag",
        type=int,
        help="client poll lag in seconds, visible clock only (default 30)",
    )
    p.add_argument(
        "--trend-filter",
        dest="trend_filter",
        choices=TREND_FILTERS,
        help="which trend read cohorts split on (default none)",
    )
    p.add_argument(
        "--rearm",
        dest="rearm",
        type=int,
        help="minutes before a spent rung re-arms; omit for first-touch-only",
    )
    p.add_argument(
        "--min-frames",
        dest="min_frames",
        type=int,
        help="minimum gamma frames for a session to be used (default 60)",
    )
    p.add_argument(
        "--no-gex-ranks",
        dest="no_gex_ranks",
        action="store_true",
        help="skip gex_by_strike; wall/flip/max-pain/pin cohorts only",
    )
    p.add_argument("--label", dest="label", help="free-text run label for provenance")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m research.or_gamma_confluence.cli",
        description="Opening-range extension x gamma confluence — research only.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("selftest", help="synthetic end-to-end check; no database needed")
    p.add_argument("--sessions", type=int, default=120)
    p.add_argument("--seed", type=int, default=7)
    p.set_defaults(func=cmd_selftest)

    p = sub.add_parser("coverage", help="how much history exists; is the publish clock usable")
    p.add_argument(
        "symbols",
        nargs="*",
        choices=known_symbols() + [],
        default=None,
        help=f"default: {' '.join(DEFAULT_SYMBOLS)}",
    )
    p.add_argument("--out", help="also write the full JSON here")
    _add_config_args(p)
    p.set_defaults(func=cmd_coverage)

    p = sub.add_parser("build-dataset", help="label touch events over a window (read-only)")
    p.add_argument("symbols", nargs="*", choices=known_symbols() + [], default=None)
    p.add_argument("--start", type=_iso, required=True, help="YYYY-MM-DD, ET session date")
    p.add_argument("--end", type=_iso, required=True)
    p.add_argument("--out", required=True, help="JSONL output path")
    _add_config_args(p)
    p.set_defaults(func=cmd_build_dataset)

    p = sub.add_parser("analyze", help="cohorts, sensitivity grids, out-of-sample")
    p.add_argument("datasets", nargs="+", help="one or more JSONL datasets")
    p.add_argument(
        "--confluence-distance",
        type=float,
        default=10.0,
        help="points, for the cohort split (default 10; the grid sweeps all)",
    )
    p.add_argument("--out", help="write the Markdown report here")
    p.add_argument("--json-out", help="write the JSON summary here")
    p.add_argument("--csv-out", help="write the event rows as CSV here")
    p.add_argument(
        "--force", action="store_true", help="analyse despite a config-fingerprint mismatch"
    )
    _add_config_args(p)
    p.set_defaults(func=cmd_analyze)

    p = sub.add_parser("sweep", help="parameter neighbourhoods (one build per cell)")
    p.add_argument("symbols", nargs="*", choices=known_symbols() + [], default=None)
    p.add_argument("--start", type=_iso, required=True)
    p.add_argument("--end", type=_iso, required=True)
    p.add_argument("--outdir", default="research_output/orgc_sweep")
    p.add_argument("--or-minutes-grid", type=int, nargs="+", default=(5, 15, 30))
    p.add_argument("--step-grid", type=float, nargs="+", default=(0.25, 0.5, 1.0))
    p.add_argument(
        "--lead-grid",
        type=int,
        nargs="+",
        default=(120,),
        help="gamma lead times in seconds. Each value is a REBUILD (the lead "
        "selects a different snapshot per touch), so this multiplies runtime. "
        "Sweep it on its own: --lead-grid 0 30 60 120 180 --or-minutes-grid 5 "
        "--step-grid 0.5",
    )
    p.add_argument("--confluence-distance", type=float, default=10.0)
    _add_config_args(p)
    p.set_defaults(func=cmd_sweep)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except sources.DatabaseUnavailable as exc:
        print(f"database unavailable: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
