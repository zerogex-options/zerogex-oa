"""Emit a psql script that EXPLAINs the replay frames read against a real DB.

Answers one question: *is this read fenced to the session, or does it scale
with how much history ``gex_by_strike`` holds?*

``get_gex_frames_for_session`` builds its per-minute strike ladder off a
timestamp coming out of ``session_summary`` -- a MATERIALIZED, i.e.
optimisation-fence, CTE.  The session bound lives behind that fence, so the
planner cannot see it.  Written as a plain join, the intended ~390 index probes
were only an intention: under the right stats PG picks a hash/merge join and
reads every row the underlying has in the table, filtering afterwards.  That is
the shape behind ``get_gex_frames_for_session(NDX, ...) failed after 31.9s`` --
a read that scales with retention rather than with the session, so it gets
slower every day and blows ``command_timeout=30`` on the biggest underlyings
first.  The ladder is a LATERAL now, which pins the probe shape.

Because the failure is PLAN-dependent, "it was fast just now" proves nothing --
the same query on the same data can flip plans as stats move.  So the script
runs the read twice: once with the planner free, and once with
``enable_nestloop = off``, which forces the fallback the fence exists to
survive.  The second run is the decisive one.

The SQL is extracted from ``src/api/database.py`` at run time rather than
copied here, so this cannot drift from what the API actually executes.

Usage (see ``make replay-frames-explain``)::

    python -m src.tools.replay_frames_explain --symbol NDX --date 2026-07-24 \\
        | psql "$CONN"

Read-only: EXPLAIN ANALYZE does execute the query, so every statement is
wrapped in a statement_timeout the operator can lower.  Nothing is written.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date as date_cls, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

_DATABASE_PY = Path(__file__).resolve().parents[2] / "src" / "api" / "database.py"

_ET = ZoneInfo("America/New_York")
_UTC = ZoneInfo("UTC")


def _frames_query() -> str:
    """The live SQL text of ``get_gex_frames_for_session``.

    Same extraction the query-shape guards in
    ``tests/test_replay_frames_read.py`` use, for the same reason: the thing
    worth EXPLAINing is what the API runs, not a copy of it that can rot.
    """
    src = _DATABASE_PY.read_text()
    start = src.index("async def get_gex_frames_for_session(")
    match = re.search(r'query = """(.*?)"""', src[start:], re.DOTALL)
    if not match:
        raise SystemExit(f"{_DATABASE_PY}: no query string found in get_gex_frames_for_session")
    return match.group(1)


def _session_bounds(session_date: date_cls) -> tuple[datetime, datetime]:
    """09:30-16:01 ET as UTC — mirrors the method's own window exactly."""
    start_et = datetime.combine(session_date, time(9, 30), tzinfo=_ET)
    end_et = datetime.combine(session_date, time(16, 1), tzinfo=_ET)
    return start_et.astimezone(_UTC), end_et.astimezone(_UTC)


def _bind(query: str, symbol: str, start_utc: datetime, end_utc: datetime, band: float) -> str:
    """Substitute the $N placeholders with literals psql can run directly."""
    return (
        query.replace("$1", f"'{symbol}'")
        .replace("$2", f"'{start_utc.isoformat()}'::timestamptz")
        .replace("$3", f"'{end_utc.isoformat()}'::timestamptz")
        .replace("$4", repr(float(band)))
    )


def build_script(symbol: str, session_date: date_cls, band: float, timeout_ms: int) -> str:
    start_utc, end_utc = _session_bounds(session_date)
    bound = _bind(_frames_query(), symbol, start_utc, end_utc, band)
    # One line: psql's \echo and statement separation both get confused by a
    # query spanning many lines with embedded comments, and the comments are
    # stripped anyway because they document the shape rather than run.
    flat = " ".join(
        line.split("--", 1)[0].strip() if "--" in line else line.strip()
        for line in bound.splitlines()
    ).strip()

    lines = [
        f"\\echo === replay frames read: {symbol} {session_date.isoformat()} ===",
        f"\\echo window {start_utc.isoformat()} .. {end_utc.isoformat()} (band {band})",
        f"SET statement_timeout = {timeout_ms};",
        "",
        "\\echo",
        "\\echo [1/4] How much history does this underlying have here?",
        "\\echo       session_rows / total_rows is the amplification an unfenced",
        "\\echo       read pays: it reads total_rows to return session_rows.",
        "SELECT",
        "    (SELECT COUNT(*) FROM gex_by_strike",
        f"      WHERE underlying = '{symbol}') AS total_rows,",
        "    (SELECT COUNT(*) FROM gex_by_strike",
        f"      WHERE underlying = '{symbol}'",
        f"        AND timestamp >= '{start_utc.isoformat()}'::timestamptz",
        f"        AND timestamp <  '{end_utc.isoformat()}'::timestamptz) AS session_rows,",
        "    pg_size_pretty(pg_total_relation_size('gex_by_strike')) AS table_size;",
        "",
        "\\echo",
        "\\echo [2/4] Planner stats freshness (a stale ANALYZE is how plans flip)",
        "SELECT relname, n_live_tup, n_dead_tup, last_autovacuum, last_autoanalyze",
        "  FROM pg_stat_user_tables WHERE relname IN ('gex_by_strike','gex_summary');",
        "",
        "\\echo",
        "\\echo [3/4] EXPLAIN ANALYZE — planner free (the plan production usually gets)",
        f"EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) {flat};",
        "",
        "\\echo",
        "\\echo [4/4] EXPLAIN ANALYZE — nested loop DISABLED (the fallback the fence exists for)",
        "\\echo       THIS IS THE DECISIVE RUN. Unfenced, it reads every row the",
        "\\echo       underlying has; fenced, it stays a per-minute probe.",
        "SET enable_nestloop = off;",
        f"EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) {flat};",
        "RESET enable_nestloop;",
        "RESET statement_timeout;",
        "",
        "\\echo",
        "\\echo --- how to read [3] and [4] ---",
        "\\echo PASS: the gex_by_strike node reads a few hundred rows per loop with",
        "\\echo       loops ~= the session's minute count (~390), under a Nested Loop",
        "\\echo       Left Join. Both runs look like this once the fence is in.",
        "\\echo FAIL: the gex_by_strike node shows loops=1 and rows in the millions",
        "\\echo       (matching total_rows from [1]) under a Hash/Merge Join. That is",
        "\\echo       the unfenced read, and it is what times out at 30s.",
        "\\echo NOTE: [4] failing while [3] passes is still a FAIL — it means the",
        "\\echo       read is one planner mood away from the timeout.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Emit a psql script that EXPLAINs the replay frames read.",
    )
    parser.add_argument("--symbol", default="NDX", help="Underlying (default: NDX)")
    parser.add_argument(
        "--date",
        required=True,
        help="Session date, YYYY-MM-DD (ET) — e.g. the date from the warning",
    )
    parser.add_argument(
        "--band",
        type=float,
        default=0.04,
        help="strike_band_pct, matching the endpoint default (0.04)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=120000,
        help=(
            "statement_timeout for the EXPLAIN runs. Above the API's own 30s so "
            "an unfenced read reports its real cost instead of being cut off, "
            "but bounded so it cannot sit on a production connection (default: "
            "120000)"
        ),
    )
    args = parser.parse_args(argv)

    try:
        session_date = date_cls.fromisoformat(args.date)
    except ValueError:
        parser.error(f"--date must be YYYY-MM-DD, got {args.date!r}")

    sys.stdout.write(build_script(args.symbol.upper(), session_date, args.band, args.timeout_ms))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
