"""Incremental, immutable archiver for the timestamped tables that the
daily ``make db-prune`` permanently deletes (option_chains, gex_by_strike,
gex_summary, underlying_quotes, flow_contract_facts, flow_by_contract).

Why this exists
---------------
Retention is a single rolling window: ``zerogex-oa-db-maintain.timer``
runs ``make db-prune`` nightly at 02:00 ET, which issues
``DELETE FROM <tbl> WHERE timestamp < NOW() - INTERVAL
'$DATA_RETENTION_DAYS days'`` for every table in ``DB_MAINTAIN_TABLES``.
Anything older than the window (60 days by default) is gone forever.  The
deleted tables are exactly the high-value, high-cardinality series you
want for backtesting, so this tool snapshots each completed ET *trading
day* to columnar Parquet **before** it can age out of the window.

Design
------
* One immutable file per (table, trading-day):
  ``<dest>/<table>/dt=YYYY-MM-DD/part.parquet``.  This Hive-style layout
  is read with partition pruning by DuckDB / Polars / pandas / Spark, so
  a backtest scans only the days/columns it needs.
* DuckDB does the extract: it attaches the Postgres source read-only and
  streams each day straight into ZSTD Parquet.  All date bucketing runs
  *inside* Postgres via ``postgres_query`` so ``AT TIME ZONE
  'America/New_York'`` keeps exact Postgres timezone semantics.
* Destination is local *or* S3 — ``ARCHIVE_DEST`` accepts a filesystem
  path or an ``s3://bucket/prefix`` URI, so you can start on disk/EBS and
  flip to S3 later with no code change.
* Idempotent: a day whose partition file already exists is skipped unless
  ``--force``.  Local writes go to a ``.tmp`` sibling and are atomically
  ``os.replace``\\d into place, so a killed run never leaves a torn file.
* Self-healing: each run re-checks the last ``ARCHIVE_LOOKBACK_DAYS``
  completed days (default 7) and backfills any that are missing, so a
  skipped night (box down, deploy) is recovered on the next run with no
  intervention — as long as the lookback stays well inside the retention
  window.  Weekend/holiday dates simply have no rows, so no empty files
  are written.

Scheduling
----------
Run by ``zerogex-oa-db-archive.timer`` at 01:30 ET — ahead of the 02:00
prune — so the just-completed session is captured before any prune could
touch it.  The lookback gives a multi-day safety margin on top of that.

Usage
-----
    python -m src.tools.db_archive                 # archive missing recent days
    python -m src.tools.db_archive --dry-run       # show what would be written
    python -m src.tools.db_archive --since 2026-05-01 --until 2026-05-31
    python -m src.tools.db_archive --tables option_chains,gex_summary --force
    ARCHIVE_DEST=s3://my-bucket/zerogex python -m src.tools.db_archive

Env knobs (CLI flags override):
    ARCHIVE_DEST            base dir or s3:// URI   (default /var/lib/zerogex/archive)
    ARCHIVE_TABLES          comma list to archive   (default: the 6 pruned tables)
    ARCHIVE_LOOKBACK_DAYS   completed days to recheck (default 7)
    ARCHIVE_COMPRESSION     parquet codec           (default ZSTD)
    DB_HOST / DB_PORT / DB_USER / DB_NAME           Postgres source (.env)
    PGPASSFILE             libpq password file      (default ~/.pgpass)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
log = logging.getLogger("db_archive")

# The timestamped tables that db-prune deletes on the rolling retention
# window — i.e. everything in the Makefile's DB_MAINTAIN_TABLES that
# actually exists in schema.sql with a `timestamp TIMESTAMPTZ` column.
# The dead entries (flow_smart_money / trade_signals /
# position_optimizer_signals — listed in DB_MAINTAIN_TABLES but absent
# from schema.sql) are intentionally excluded.
DEFAULT_TABLES = [
    "option_chains",
    "gex_by_strike",
    "gex_summary",
    "underlying_quotes",
    "flow_contract_facts",
    "flow_by_contract",
]

# Every archived table buckets on this TIMESTAMPTZ column (verified
# against schema.sql).  Override per-invocation only if you extend the
# table list to something keyed differently.
TIMESTAMP_COLUMN = "timestamp"

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(name: str) -> str:
    """Validate a SQL identifier (table/column) — these reach DuckDB/PG
    by interpolation, so reject anything that isn't a bare identifier."""
    if not _IDENT_RE.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def _pg_conninfo() -> str:
    """libpq conninfo mirroring the Makefile's PSQL (sslmode=require,
    keepalives).  Password is resolved by libpq from PGPASSFILE/.pgpass,
    exactly like every other tool in the repo — no secret on the CLI."""
    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT", "5432")
    user = os.environ.get("DB_USER")
    name = os.environ.get("DB_NAME")
    missing = [k for k, v in {"DB_HOST": host, "DB_USER": user, "DB_NAME": name}.items() if not v]
    if missing:
        raise SystemExit(f"Missing required DB env var(s): {', '.join(missing)} (set them in .env)")
    # Ensure libpq finds the deploy user's .pgpass even under systemd.
    os.environ.setdefault("PGPASSFILE", str(Path.home() / ".pgpass"))
    return (
        f"host={host} port={port} user={user} dbname={name} sslmode=require "
        f"keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3"
    )


def _is_s3(dest: str) -> bool:
    return dest.lower().startswith("s3://")


def _partition_uri(dest: str, table: str, day: date) -> str:
    """Hive-style partition path: <dest>/<table>/dt=YYYY-MM-DD/part.parquet."""
    leaf = f"{table}/dt={day.isoformat()}/part.parquet"
    if _is_s3(dest):
        return f"{dest.rstrip('/')}/{leaf}"
    return str(Path(dest).expanduser() / leaf)


def _s3_exists(uri: str) -> bool:
    import boto3
    from botocore.exceptions import ClientError

    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    try:
        boto3.client("s3").head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def _target_exists(uri: str) -> bool:
    return _s3_exists(uri) if _is_s3(uri) else Path(uri).exists()


def _sql_lit(value: str) -> str:
    """Single-quote a string literal for embedding inside the
    postgres_query SQL string (doubles embedded quotes)."""
    return value.replace("'", "''")


def _connect(dest: str, conninfo: str):
    import duckdb

    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{_sql_lit(conninfo)}' AS pg (TYPE postgres, READ_ONLY)")
    if _is_s3(dest):
        # Resolve S3 creds from the standard AWS chain (instance profile,
        # env, shared config) — same boto3 chain the rest of the platform
        # uses.  No keys ever touch this file.
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("CREATE OR REPLACE SECRET archive_s3 (TYPE s3, PROVIDER credential_chain)")
    return con


def _distinct_days(con, table: str, start: datetime, end_excl: datetime) -> List[date]:
    """ET trading days that actually carry rows in [start, end_excl).
    Bucketing runs inside Postgres so timezone semantics are exact and
    only non-empty days come back (weekends/holidays drop out)."""
    col = _ident(TIMESTAMP_COLUMN)
    tbl = _ident(table)
    inner = (
        f"SELECT DISTINCT ({col} AT TIME ZONE ''America/New_York'')::date AS dt "
        f"FROM {tbl} "
        f"WHERE {col} >= ''{start.isoformat()}'' AND {col} < ''{end_excl.isoformat()}'' "
        f"ORDER BY dt"
    )
    rows = con.execute(f"SELECT dt FROM postgres_query('pg', '{inner}')").fetchall()
    return [r[0] for r in rows]


def _copy_day(con, table: str, day: date, target_uri: str, compression: str) -> int:
    """Extract one ET trading day of `table` to a single Parquet file.
    Local targets are written to a .tmp sibling and atomically renamed."""
    col = _ident(TIMESTAMP_COLUMN)
    tbl = _ident(table)
    inner = (
        f"SELECT * FROM {tbl} "
        f"WHERE ({col} AT TIME ZONE ''America/New_York'')::date = ''{day.isoformat()}''"
    )
    select_sql = f"SELECT * FROM postgres_query('pg', '{inner}')"

    if _is_s3(target_uri):
        write_uri, finalize = target_uri, None
    else:
        out = Path(target_uri)
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(out.suffix + ".tmp")
        write_uri, finalize = str(tmp), lambda: os.replace(tmp, out)

    copy_sql = (
        f"COPY ({select_sql}) TO '{_sql_lit(write_uri)}' "
        f"(FORMAT PARQUET, COMPRESSION {compression})"
    )
    result = con.execute(copy_sql).fetchone()
    if finalize:
        finalize()
    # DuckDB COPY ... TO returns a single-row count.
    return int(result[0]) if result else 0


def _date_window(args) -> Tuple[date, date]:
    """[since, until] inclusive of completed ET days to (re)check.
    Default: the trailing ARCHIVE_LOOKBACK_DAYS ending yesterday ET."""
    today_et = datetime.now(ET).date()
    if args.until:
        until = date.fromisoformat(args.until)
    else:
        until = today_et - timedelta(days=1)  # never archive the in-progress day
    if args.since:
        since = date.fromisoformat(args.since)
    else:
        since = until - timedelta(days=max(args.lookback_days - 1, 0))
    if since > until:
        raise SystemExit(f"--since ({since}) is after --until ({until})")
    return since, until


def archive(args) -> int:
    dest = args.dest
    tables = [_ident(t.strip()) for t in args.tables if t.strip()]
    since, until = _date_window(args)
    # Half-open [start, end_excl) in ET, expressed as tz-aware bounds.
    start = datetime.combine(since, datetime.min.time(), ET)
    end_excl = datetime.combine(until + timedelta(days=1), datetime.min.time(), ET)

    log.info(
        "Archiving %s → %s  (ET days %s … %s, %d table(s)%s)",
        ",".join(tables), dest, since, until, len(tables),
        ", DRY-RUN" if args.dry_run else "",
    )

    conninfo = _pg_conninfo()
    con = _connect(dest, conninfo)

    total_files = total_rows = total_skipped = total_empty = 0
    for table in tables:
        days = _distinct_days(con, table, start, end_excl)
        if not days:
            total_empty += 1
            log.info("  %-22s no rows in window — nothing to archive", table)
            continue
        for day in days:
            uri = _partition_uri(dest, table, day)
            if not args.force and _target_exists(uri):
                total_skipped += 1
                log.debug("  %-22s %s  skip (exists)", table, day)
                continue
            if args.dry_run:
                total_files += 1
                log.info("  %-22s %s  would write → %s", table, day, uri)
                continue
            rows = _copy_day(con, table, day, uri, args.compression)
            total_files += 1
            total_rows += rows
            log.info("  %-22s %s  wrote %d rows → %s", table, day, rows, uri)

    con.close()
    log.info(
        "Done: %d file(s)%s, %d row(s), %d skipped (already archived)%s",
        total_files,
        " planned" if args.dry_run else " written",
        total_rows,
        total_skipped,
        f", {total_empty} table(s) empty in window" if total_empty else "",
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m src.tools.db_archive",
        description="Archive pruned timestamped tables to partitioned Parquet for backtesting.",
    )
    p.add_argument(
        "--dest",
        default=os.environ.get("ARCHIVE_DEST", "/var/lib/zerogex/archive"),
        help="Base destination: local dir or s3://bucket/prefix (env ARCHIVE_DEST).",
    )
    p.add_argument(
        "--tables",
        type=lambda s: s.split(","),
        default=os.environ.get("ARCHIVE_TABLES", ",".join(DEFAULT_TABLES)).split(","),
        help="Comma-separated tables to archive (env ARCHIVE_TABLES).",
    )
    p.add_argument(
        "--lookback-days",
        type=int,
        default=int(os.environ.get("ARCHIVE_LOOKBACK_DAYS", "7")),
        help="Completed ET days back to (re)check and backfill (env ARCHIVE_LOOKBACK_DAYS).",
    )
    p.add_argument("--since", help="Override start date YYYY-MM-DD (inclusive).")
    p.add_argument("--until", help="Override end date YYYY-MM-DD (inclusive; default = yesterday ET).")
    p.add_argument(
        "--compression",
        default=os.environ.get("ARCHIVE_COMPRESSION", "ZSTD"),
        help="Parquet compression codec (env ARCHIVE_COMPRESSION; default ZSTD).",
    )
    p.add_argument("--force", action="store_true", help="Re-write partitions that already exist.")
    p.add_argument("--dry-run", action="store_true", help="List partitions that would be written; write nothing.")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging (per-day skips).")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    try:
        return archive(args)
    except ImportError as exc:
        log.error("Missing dependency: %s — install with `pip install -e .[archive]`", exc)
        return 1
    except Exception as exc:  # surfaced to the systemd journal / OnFailure alert
        log.error("Archive failed: %s", exc, exc_info=args.verbose)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
