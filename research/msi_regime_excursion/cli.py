"""Command line for the MSI excursion study.

``extract`` is the only command that touches the database, and it only reads.
It writes a JSONL dataset; ``analyze`` turns that into the report without any
database at all. The split is deliberate: the dataset is the evidence, it can
be archived, handed to someone else, or re-analysed under different settings
without re-querying production or hoping the archive has not rolled over.

    # 0. Nothing but plumbing -- synthetic worlds, not a market result.
    python -m research.msi_regime_excursion.cli selftest

    # 0b. A property of the shipped code. Needs no database.
    python -m research.msi_regime_excursion.cli structural

    # 1. What does the archive actually hold?
    python -m research.msi_regime_excursion.cli describe

    # 2. Pull the dataset (read-only).
    python -m research.msi_regime_excursion.cli extract \\
        --start 2026-06-01 --end 2026-09-01 \\
        --out research_output/msi_excursion.jsonl

    # 3. Report. No database.
    python -m research.msi_regime_excursion.cli analyze \\
        research_output/msi_excursion.jsonl \\
        --out research_output/msi_excursion_report.md
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from research.msi_regime_excursion.excursion import (
    DEFAULT_HORIZONS,
    REST_OF_SESSION,
    BarSeries,
)
from research.msi_regime_excursion.report import render_markdown
from research.msi_regime_excursion.sources import (
    DEFAULT_INSTRUMENTS,
    archive_span,
    instrument,
    load_bars,
    load_readings,
)
from research.msi_regime_excursion.study import Row, build_rows, run_study

DATASET_VERSION = 1


def _parse_day(text: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise SystemExit(f"cannot parse date {text!r}; use YYYY-MM-DD")


def _connect():
    """Open a read-only-by-convention production connection."""
    try:
        from src.database import db_connection
    except Exception as exc:  # pragma: no cover
        raise SystemExit(
            f"cannot import the database layer ({exc}).\n"
            "Run from the repository root with the service's environment loaded."
        )
    return db_connection()


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------

def cmd_describe(args: argparse.Namespace) -> int:
    keys = [k.strip().upper() for k in args.instruments.split(",") if k.strip()]
    rows: list[tuple[str, ...]] = []
    with _connect() as conn:
        for key in keys:
            inst = instrument(key)
            s_min, s_max, s_n = archive_span(
                conn, "signal_scores", "underlying", inst.score_symbol
            )
            if inst.bar_source == "futures":
                b_min, b_max, b_n = archive_span(
                    conn, "futures_quotes", "index_symbol", inst.bar_symbol
                )
            else:
                b_min, b_max, b_n = archive_span(
                    conn, "underlying_quotes", "symbol", inst.bar_symbol
                )
            rows.append((
                key,
                inst.score_symbol,
                f"{s_n:,}",
                f"{s_min:%Y-%m-%d}" if s_min else "—",
                f"{s_max:%Y-%m-%d}" if s_max else "—",
                f"{inst.bar_source}:{inst.bar_symbol}",
                f"{b_n:,}",
                f"{b_min:%Y-%m-%d}" if b_min else "—",
                f"{b_max:%Y-%m-%d}" if b_max else "—",
            ))

    if args.json:
        cols = ["instrument", "score_symbol", "score_rows", "score_first", "score_last",
                "bar_source", "bar_rows", "bar_first", "bar_last"]
        print(json.dumps([dict(zip(cols, r)) for r in rows], indent=2))
        return 0

    header = (f"{'inst':<5} {'score<-':<8} {'rows':>10} {'first':>11} {'last':>11}  "
              f"{'bars':<16} {'rows':>10} {'first':>11} {'last':>11}")
    print(header)
    print("-" * len(header))
    for r in rows:
        print(f"{r[0]:<5} {r[1]:<8} {r[2]:>10} {r[3]:>11} {r[4]:>11}  "
              f"{r[5]:<16} {r[6]:>10} {r[7]:>11} {r[8]:>11}")
    print(
        "\nES / NQ carry no MSI of their own — the score column shows whose option\n"
        "book it comes from. That is the product's behavior, not a gap in the archive:\n"
        "src/jobs/futures_projection.py projects levels, never scores."
    )
    return 0


# ---------------------------------------------------------------------------
# extract
# ---------------------------------------------------------------------------

def cmd_extract(args: argparse.Namespace) -> int:
    start = _parse_day(args.start)
    end = _parse_day(args.end)
    horizons = [int(h) for h in args.horizons.split(",") if h.strip()]
    keys = [k.strip().upper() for k in args.instruments.split(",") if k.strip()]
    # Bars must run past the last reading by the longest horizon, or the tail of
    # the window has no measurable forward path.
    bar_end = end + timedelta(minutes=max(horizons) + 60)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    written = 0
    meta = {
        "dataset_version": DATASET_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "horizons": horizons,
        "bias_lookback_min": args.bias_lookback,
        "instruments": keys,
        "per_instrument": {},
    }

    with open(args.out, "w", encoding="utf-8") as fh, _connect() as conn:
        for key in keys:
            inst = instrument(key)
            readings = load_readings(conn, inst.score_symbol, start, end)
            bars = load_bars(conn, inst, start, bar_end)
            series = BarSeries(bars)
            rows = build_rows(
                readings, series,
                horizons=horizons,
                bias_lookback_min=args.bias_lookback,
            )
            meta["per_instrument"][key] = {
                "readings": len(readings),
                "bars": len(bars),
                "rows": len(rows),
                "sessions": len({r.session for r in rows}),
            }
            for row in rows:
                fh.write(json.dumps({
                    "instrument": key,
                    "timestamp": row.timestamp.isoformat(),
                    "session": row.session.isoformat(),
                    "msi": row.msi,
                    "band": row.band,
                    "persisted_band": row.persisted_band,
                    "bias": row.bias,
                    "reconstruction_error": row.reconstruction_error,
                    "variants": row.variants,
                    "measures": {f"{m}|{h}": v for (m, h), v in row.measures.items()},
                    "points": {f"{m}|{h}": v for (m, h), v in row.points.items()},
                }, default=str) + "\n")
                written += 1
            print(
                f"{key:<5} readings={len(readings):,} bars={len(bars):,} "
                f"rows={len(rows):,} sessions={meta['per_instrument'][key]['sessions']}",
                file=sys.stderr,
            )

    meta_path = args.out + ".meta.json"
    with open(meta_path, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2)
    print(f"\nwrote {written:,} rows -> {args.out}\n      metadata -> {meta_path}",
          file=sys.stderr)
    if written == 0:
        print(
            "\nNo rows. Either the window predates the archive (signal_scores and\n"
            "underlying_quotes are pruned at DATA_RETENTION_DAYS, default 90; \n"
            "futures_quotes likewise) or the instruments had no readings in it.\n"
            "Run `describe` to see what the archive actually holds.",
            file=sys.stderr,
        )
        return 1
    return 0


# ---------------------------------------------------------------------------
# analyze
# ---------------------------------------------------------------------------

def _load_dataset(path: str) -> tuple[dict[str, list[Row]], dict]:
    from datetime import date as _date

    by_inst: dict[str, list[Row]] = {}
    horizons: set[object] = set()
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(
                    f"{path}:{lineno} is not JSON ({exc.msg}).\n"
                    "`analyze` expects the JSONL written by `extract`, one row per "
                    "line — not the markdown report and not the .meta.json sidecar."
                ) from None
            if not isinstance(d, dict) or "instrument" not in d or "measures" not in d:
                raise SystemExit(
                    f"{path}:{lineno} is JSON but not an extract row (needs at least "
                    "'instrument', 'timestamp', 'session', 'msi', 'measures').\n"
                    "Re-run `extract` to regenerate the dataset."
                )
            row = Row(
                timestamp=datetime.fromisoformat(d["timestamp"]),
                session=_date.fromisoformat(d["session"]),
                msi=float(d["msi"]),
                band=d.get("band"),
                persisted_band=d.get("persisted_band"),
                variants=d.get("variants") or {},
                reconstruction_error=d.get("reconstruction_error"),
                bias=int(d.get("bias") or 0),
            )
            for key, value in (d.get("measures") or {}).items():
                measure, _, h = key.rpartition("|")
                hz: object = h if h == REST_OF_SESSION else int(h)
                row.measures[(measure, hz)] = value
                horizons.add(hz)
            for key, value in (d.get("points") or {}).items():
                measure, _, h = key.rpartition("|")
                hz = h if h == REST_OF_SESSION else int(h)
                row.points[(measure, hz)] = value
            by_inst.setdefault(d["instrument"], []).append(row)

    meta: dict = {}
    try:
        with open(path + ".meta.json", "r", encoding="utf-8") as fh:
            meta = json.load(fh)
    except OSError:
        pass
    meta["_horizons"] = sorted(
        horizons, key=lambda h: (h == REST_OF_SESSION, h if isinstance(h, int) else 0)
    )
    return by_inst, meta


def cmd_analyze(args: argparse.Namespace) -> int:
    by_inst, meta = _load_dataset(args.dataset)
    if not by_inst:
        print(f"{args.dataset} has no rows.", file=sys.stderr)
        return 1

    horizons = meta.get("_horizons") or list(DEFAULT_HORIZONS)
    if args.horizons:
        horizons = [
            REST_OF_SESSION if h.strip() == REST_OF_SESSION else int(h)
            for h in args.horizons.split(",") if h.strip()
        ]

    if getattr(args, "clean_only", False):
        from research.msi_regime_excursion.study import RECONSTRUCTION_TOLERANCE
        kept: dict[str, list[Row]] = {}
        for key, rows in by_inst.items():
            clean = [
                r for r in rows
                if r.reconstruction_error is not None
                and r.reconstruction_error <= RECONSTRUCTION_TOLERANCE
            ]
            print(f"{key}: kept {len(clean):,} of {len(rows):,} rows that reconstruct",
                  file=sys.stderr)
            if clean:
                kept[key] = clean
        by_inst = kept
        if not by_inst:
            print("No rows reconstruct — nothing to analyse.", file=sys.stderr)
            return 1

    results = []
    for key in sorted(by_inst, key=lambda k: DEFAULT_INSTRUMENTS.index(k)
                      if k in DEFAULT_INSTRUMENTS else 99):
        rows = sorted(by_inst[key], key=lambda r: r.timestamp)
        results.append(run_study(
            instrument(key), rows, horizons=horizons,
            iterations=args.iterations, min_bucket=args.min_bucket,
        ))
        print(f"analysed {key}: {len(rows):,} rows", file=sys.stderr)

    window = ""
    if meta.get("start") and meta.get("end"):
        window = f"{meta['start'][:10]} to {meta['end'][:10]}"
    markdown = render_markdown(results, horizons=horizons, window=window)

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(markdown)
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        print(markdown)

    payload = {
        "meta": {k: v for k, v in meta.items() if not k.startswith("_")},
        "instruments": [
            {
                "instrument": r.instrument, "n_rows": r.n_rows,
                "n_sessions": r.n_sessions,
                "band_counts": r.band_counts,
                "reconstruction_ok": r.reconstruction_ok,
                "reconstruction_total": r.reconstruction_total,
                "notes": r.notes,
                "correlations": [c.as_dict() for c in r.correlations],
                "buckets": [b.as_dict() for b in r.buckets],
                "targets": [t.as_dict() for t in r.targets],
            }
            for r in results
        ],
    }
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        print(f"wrote {args.json_out}", file=sys.stderr)

    # The full report is thousands of rows. Always finish with the digest, so a
    # run ends with the part that answers the question rather than the part
    # that audits it.
    if not args.no_digest:
        from research.msi_regime_excursion.digest import render_digest
        print()
        print(render_digest(payload))
    return 0


# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m research.msi_regime_excursion.cli",
        description="Does the MSI regime gauge predict realized forward excursion?",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("describe", help="what the archive holds (read-only)")
    p.add_argument("--instruments", default=",".join(DEFAULT_INSTRUMENTS))
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_describe)

    p = sub.add_parser("extract", help="pull the dataset (read-only)")
    p.add_argument("--start", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--end", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--instruments", default=",".join(DEFAULT_INSTRUMENTS))
    p.add_argument("--horizons", default=",".join(str(h) for h in DEFAULT_HORIZONS))
    p.add_argument("--bias-lookback", type=int, default=30,
                   help="minutes used to read the prevailing bias (default 30, "
                        "matching frontend/core/impliedDirection.ts)")
    p.add_argument("--out", default="research_output/msi_excursion.jsonl")
    p.set_defaults(func=cmd_extract)

    p = sub.add_parser("analyze", help="dataset -> report (no database)")
    p.add_argument("dataset")
    p.add_argument("--horizons", default="", help="restrict to these horizons")
    p.add_argument("--iterations", type=int, default=2000,
                   help="block-bootstrap resamples (default 2000)")
    p.add_argument("--min-bucket", type=int, default=30,
                   help="skip a bucket with fewer usable rows than this")
    p.add_argument("--out", default="", help="write markdown here (default stdout)")
    p.add_argument("--json-out", default="", help="also write the raw findings as JSON")
    p.add_argument("--no-digest", action="store_true",
                   help="suppress the one-screen summary printed at the end")
    p.add_argument("--clean-only", action="store_true",
                   help="keep only rows whose composite reconstructs exactly — i.e. "
                        "rows where no component abstained. Use this to check whether "
                        "a result is being diluted by readings scored on partial data.")
    p.set_defaults(func=cmd_analyze)

    p = sub.add_parser("abstention",
                       help="where components abstain, by ET hour (no database)")
    p.add_argument("dataset")
    p.set_defaults(func=lambda a: __import__(
        "research.msi_regime_excursion.abstention", fromlist=["main"]).main(a.dataset))

    p = sub.add_parser("digest", help="one-screen summary of a findings JSON")
    p.add_argument("findings", help="the file written by `analyze --json-out`")
    p.set_defaults(func=lambda a: __import__(
        "research.msi_regime_excursion.digest", fromlist=["main"]).main(a.findings))

    p = sub.add_parser("selftest", help="synthetic worlds; validates the machinery")
    p.set_defaults(func=lambda a: __import__(
        "research.msi_regime_excursion.selftest", fromlist=["main"]).main())

    p = sub.add_parser("structural", help="flow-direction sweep against the real engine")
    p.add_argument("--structure", default="all")
    p.add_argument("--steps", type=int, default=9)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=lambda a: __import__(
        "research.msi_regime_excursion.structural", fromlist=["main"]).main(
            ["--structure", a.structure, "--steps", str(a.steps)]
            + (["--json"] if a.json else [])))

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
