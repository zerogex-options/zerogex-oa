"""Put back the Action Card history production is missing, from a restored
copy of the database.

On 2026-09-24 a TRUNCATE emptied ``signal_action_cards`` and, by cascade,
``playbook_card_outcomes``. An RDS point-in-time restore of the database as
it stood before the wipe exists as a separate instance (``zerogex-restore``),
but its rows were never copied back: production's history starts on
2026-09-25, and every older ``/cards/<id>`` permalink, Scorecard day,
backtester session and graded idea is missing.

This copies the rows production lacks from that restored copy back into
production, under their original ids, so the old permalinks resolve again.

    make playbook-restore-cards FROM=zerogex-restore               # rehearse
    make playbook-restore-cards FROM=zerogex-restore CONFIRM=yes   # write

``FROM`` is the restored instance's identifier (its host is DB_HOST with the
first label swapped) or a full host name. The restored copy is read with the
same port, database, user and password as production; set
RESTORE_DB_PASSWORD if its password differs.

A rehearsal performs the whole copy inside one transaction on production and
rolls it back, so the database itself checks every id, foreign key and the
one-grade-per-Card index, and nothing changes. With ``--apply`` the same
transaction commits, and only when every count matches the plan.

It refuses, and changes nothing, when:

* the source is production itself;
* an id the source holds already belongs to a different row in production;
* a row it would add is not older than every row production already has, or
  sits at or above production's id counter, where a future Card would
  collide with it;
* production lacks a symbol or a required column a row needs, or a grade's
  Card;
* a row put back would change what goes out live: the entry bar
  (adaptive_gate.py) learns only from ideas issued on or after
  PLAYBOOK_ADAPTIVE_SINCE, and, when pattern calibration is on, its nightly
  backtest must no longer reach the Cards put back.

Rows production already has (same id, same row) are skipped, so a second run
adds nothing.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from src import config

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

# Rows larger than this many bytes in total spill from memory to a temp file.
_SPOOL_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class TableSpec:
    name: str
    label: str
    time_col: str
    # Columns that must agree for a row production already holds under the
    # same id to count as that row put back earlier rather than a clash.
    key_cols: tuple[str, ...]
    # Read alongside the key, for the report only.
    info_cols: tuple[str, ...] = ()


CARDS = TableSpec(
    "signal_action_cards", "Action Cards", "timestamp", ("underlying", "pattern", "timestamp")
)
GRADES = TableSpec(
    "playbook_card_outcomes",
    "Grades",
    "issued_at",
    ("card_id", "underlying", "pattern", "issued_at"),
    ("outcome",),
)
# Parents first: a grade references its Card.
TABLES = (CARDS, GRADES)


# ---------------------------------------------------------------------------
# Pure planning (unit-tested without a database)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Column:
    name: str
    nullable: bool
    has_default: bool
    generated: bool = False


def resolve_source_host(source: str, db_host: str) -> str:
    """``zerogex-restore`` -> DB_HOST with its first label swapped; a name
    with a dot is taken as the full host."""
    source = (source or "").strip().lower()
    db_host = (db_host or "").strip().lower()
    if not source:
        raise ValueError("FROM is empty: name the restored instance, e.g. FROM=zerogex-restore")
    host = source
    if "." not in source:
        if "." not in db_host:
            raise ValueError(
                f"DB_HOST={db_host!r} has no domain to put {source!r} in; "
                "pass the restored copy's full host name"
            )
        host = f"{source}.{db_host.split('.', 1)[1]}"
    if host == db_host:
        raise ValueError(f"{host} is production (DB_HOST) itself")
    return host


def pgpass_fields(line: str) -> list[str]:
    """Split one ~/.pgpass line the way libpq does: ``\\`` escapes the next
    character, and everything after the fourth separator is the password."""
    fields: list[str] = []
    cur: list[str] = []
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "\\" and i + 1 < len(line):
            cur.append(line[i + 1])
            i += 2
            continue
        if ch == ":" and len(fields) < 4:
            fields.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
        i += 1
    fields.append("".join(cur))
    return fields


def pgpass_password(
    lines: Iterable[str], *, host: str, port: str, dbname: str, user: str
) -> Optional[str]:
    """The password of the first line matching production's connection."""
    want = (host, str(port), dbname, user)
    for raw in lines:
        line = raw.rstrip("\r\n")
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        fields = pgpass_fields(line)
        if len(fields) != 5:
            continue
        if all(f == "*" or f == w for f, w in zip(fields[:4], want)):
            return fields[4]
    return None


def source_password(
    environ: Mapping[str, str],
    *,
    provider: Callable[[], Optional[str]],
    host: str,
    port: str,
    dbname: str,
    user: str,
    pgpass_lines: Callable[[], Iterable[str]],
) -> Optional[str]:
    """Password for the restored copy, which holds production's credentials
    as of the restore point.

    RESTORE_DB_PASSWORD wins; then production's own password, from its
    configured provider, DB_PASSWORD, or the ~/.pgpass line for DB_HOST.
    libpq would not use that line by itself: it matches on host name.
    """
    explicit = environ.get("RESTORE_DB_PASSWORD")
    if explicit:
        return explicit
    from_provider = provider()
    if from_provider:
        return from_provider
    if environ.get("DB_PASSWORD"):
        return environ["DB_PASSWORD"]
    try:
        lines = list(pgpass_lines())
    except OSError:
        return None
    return pgpass_password(lines, host=host, port=port, dbname=dbname, user=user)


def copyable_columns(
    target: Sequence[Column], source_names: Iterable[str]
) -> tuple[list[str], list[str], list[str]]:
    """``(copied, dropped, missing_required)`` columns, copied in
    production's order. A production column the source lacks is fine when it
    is nullable or has a default."""
    src = list(source_names)
    src_set = set(src)
    tgt_names = {c.name for c in target}
    copied = [c.name for c in target if c.name in src_set and not c.generated]
    missing_required = [
        c.name
        for c in target
        if c.name not in src_set and not c.nullable and not c.has_default and not c.generated
    ]
    dropped = [n for n in src if n not in tgt_names]
    return copied, dropped, missing_required


@dataclass
class Classified:
    to_add: list[int] = field(default_factory=list)
    already_there: list[int] = field(default_factory=list)
    clashes: list[int] = field(default_factory=list)


def classify(
    source: Mapping[int, tuple], target: Mapping[int, tuple], width: Optional[int] = None
) -> Classified:
    """Sort source ids by whether production lacks them, already holds the
    same row, or holds a different row under the same id.

    Only the first ``width`` values of each row identify it; the rest (a
    grade's outcome, which the grader fills in later) may differ."""
    out = Classified()
    for row_id in sorted(source):
        if row_id not in target:
            out.to_add.append(row_id)
        elif target[row_id][:width] == source[row_id][:width]:
            out.already_there.append(row_id)
        else:
            out.clashes.append(row_id)
    return out


def next_serial(last_value: Optional[int], is_called: Optional[bool]) -> Optional[int]:
    """The next id the sequence hands out."""
    if last_value is None:
        return None
    return int(last_value) + 1 if is_called else int(last_value)


def headroom_problem(
    label: str, to_add: Sequence[int], target_min_id: Optional[int], next_id: Optional[int]
) -> Optional[str]:
    """Rows put back must sit below everything production has, and below
    the ids its sequence will hand out, or a future insert collides."""
    if not to_add:
        return None
    top = max(to_add)
    if target_min_id is not None and top >= target_min_id:
        return (
            f"{label}: the ids to put back reach {top:,}, but production already has "
            f"rows from id {target_min_id:,}; the copy would interleave with live rows"
        )
    if next_id is not None and top >= next_id:
        return (
            f"{label}: id {top:,} is at or above production's next id {next_id:,}, "
            "so a future Card would collide with it"
        )
    return None


def et_midnight(day: date) -> datetime:
    return datetime.combine(day, time(0, 0), tzinfo=ET)


def entry_bar_reach(
    issued: Iterable[datetime], *, since: date, lookback_days: int, now: datetime
) -> int:
    """Grades put back that the live entry bar would count, now or once graded."""
    floor = max(et_midnight(since), now - timedelta(days=lookback_days))
    return sum(1 for t in issued if t >= floor)


def calibration_clear_on(
    card_times: Iterable[datetime], *, lookback_days: int, now: datetime
) -> Optional[date]:
    """None when the nightly calibration backtest no longer reaches any Card
    put back; otherwise the first ET date from which it doesn't."""
    newest = max(card_times, default=None)
    if newest is None or newest < now - timedelta(days=lookback_days):
        return None
    return (newest + timedelta(days=lookback_days)).astimezone(ET).date() + timedelta(days=1)


def calibration_lookback_days(environ: Mapping[str, str]) -> int:
    """What the nightly job uses: CALIB_DAYS when set (the Makefile passes it
    as --days), otherwise SIGNALS_PATTERN_CALIBRATION_LOOKBACK_DAYS."""
    raw = (environ.get("CALIB_DAYS") or "").split("#", 1)[0].strip()
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    return int(config.SIGNALS_PATTERN_CALIBRATION_LOOKBACK_DAYS)


# ---------------------------------------------------------------------------
# Database reads
# ---------------------------------------------------------------------------


def _ident(name: str):
    from psycopg2 import sql

    return sql.Identifier(name)


def table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (table,))
        return bool(cur.fetchone()[0])


def columns_of(conn, table: str) -> list[Column]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname, NOT a.attnotnull, a.atthasdef, a.attgenerated <> ''
            FROM pg_attribute a
            WHERE a.attrelid = to_regclass(%s)
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (table,),
        )
        return [Column(r[0], bool(r[1]), bool(r[2]), bool(r[3])) for r in cur.fetchall()]


def read_keys(conn, spec: TableSpec, ids: Optional[Sequence[int]] = None) -> dict[int, tuple]:
    """``{id: key_cols + info_cols}`` for the whole table, or just ``ids``."""
    from psycopg2 import sql

    cols = sql.SQL(", ").join(_ident(c) for c in ("id",) + spec.key_cols + spec.info_cols)
    query = sql.SQL("SELECT {cols} FROM {table}").format(cols=cols, table=_ident(spec.name))
    params: tuple = ()
    if ids is not None:
        if not ids:
            return {}
        query = query + sql.SQL(" WHERE id = ANY(%s)")
        params = (list(ids),)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return {int(r[0]): tuple(r[1:]) for r in cur.fetchall()}


def id_bounds(conn, table: str) -> tuple[Optional[int], Optional[int], int]:
    from psycopg2 import sql

    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT MIN(id), MAX(id), COUNT(*) FROM {t}").format(t=_ident(table)))
        lo, hi, n = cur.fetchone()
        return lo, hi, int(n)


def next_id(conn, table: str) -> Optional[int]:
    from psycopg2 import sql

    with conn.cursor() as cur:
        cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (table,))
        seq = cur.fetchone()[0]
        if not seq:
            return None
        # pg_get_serial_sequence returns a qualified, already-quoted name.
        cur.execute(sql.SQL("SELECT last_value, is_called FROM {}").format(sql.SQL(seq)))
        last_value, is_called = cur.fetchone()
        return next_serial(last_value, is_called)


def server_identity(conn) -> tuple:
    with conn.cursor() as cur:
        cur.execute("SELECT inet_server_addr()::text, inet_server_port(), current_database()")
        return tuple(cur.fetchone())


def describe(conn) -> str:
    try:
        params = conn.get_dsn_parameters()
    except Exception:  # noqa: BLE001 - only used for the report
        return "?"
    return f"{params.get('host') or 'local socket'}/{params.get('dbname') or '?'}"


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------


@dataclass
class TablePlan:
    spec: TableSpec
    present_in_source: bool = True
    columns: list[str] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    source_rows: int = 0
    source_keys: dict[int, tuple] = field(default_factory=dict)
    target_bounds: tuple = (None, None, 0)
    sorted_ids: Classified = field(default_factory=Classified)

    def added_keys(self) -> list[tuple]:
        return [self.source_keys[i] for i in self.sorted_ids.to_add]

    def key(self, row: tuple, col: str) -> Any:
        cols = self.spec.key_cols + self.spec.info_cols
        return row[cols.index(col)]


@dataclass
class Plan:
    tables: list[TablePlan]
    problems: list[str]
    notes: list[str]


def build_plan(src, dst, *, now: datetime, environ: Mapping[str, str]) -> Plan:
    problems: list[str] = []
    notes: list[str] = []
    plans: list[TablePlan] = []

    if server_identity(src) == server_identity(dst):
        problems.append("the source and production are the same database")

    for spec in TABLES:
        plan = TablePlan(spec)
        plans.append(plan)
        if not table_exists(dst, spec.name):
            problems.append(f"production has no {spec.name} table")
            continue
        if not table_exists(src, spec.name):
            plan.present_in_source = False
            notes.append(f"{spec.label}: the restored copy has no {spec.name}; skipped")
            continue
        copied, dropped, missing = copyable_columns(
            columns_of(dst, spec.name), [c.name for c in columns_of(src, spec.name)]
        )
        plan.columns, plan.dropped = copied, dropped
        if "id" not in copied:
            problems.append(f"{spec.name}: no id column to keep")
            continue
        if missing:
            problems.append(
                f"{spec.name}: production requires {', '.join(missing)}, "
                "which the restored copy doesn't have"
            )
        plan.source_keys = read_keys(src, spec)
        plan.source_rows = len(plan.source_keys)
        plan.target_bounds = id_bounds(dst, spec.name)
        target_keys = read_keys(dst, spec, list(plan.source_keys))
        plan.sorted_ids = classify(plan.source_keys, target_keys, len(spec.key_cols))
        if plan.sorted_ids.clashes:
            sample = ", ".join(f"{i:,}" for i in plan.sorted_ids.clashes[:5])
            problems.append(
                f"{spec.label}: {len(plan.sorted_ids.clashes):,} ids already belong to "
                f"different rows in production (e.g. {sample})"
            )
        problem = headroom_problem(
            spec.label, plan.sorted_ids.to_add, plan.target_bounds[0], next_id(dst, spec.name)
        )
        if problem:
            problems.append(problem)

    cards, grades = plans
    card_rows = cards.added_keys()
    grade_rows = grades.added_keys()

    # Every symbol a row put back names must exist in production.
    symbols = sorted(
        {cards.key(r, "underlying") for r in card_rows}
        | {grades.key(r, "underlying") for r in grade_rows}
    )
    if symbols and table_exists(dst, "symbols"):
        with dst.cursor() as cur:
            cur.execute("SELECT symbol FROM symbols WHERE symbol = ANY(%s)", (symbols,))
            have = {r[0] for r in cur.fetchall()}
        missing_symbols = [s for s in symbols if s not in have]
        if missing_symbols:
            problems.append(
                f"production's symbols table lacks {', '.join(missing_symbols)}, "
                "which rows put back refer to"
            )

    # Every grade put back needs its Card, and must be that Card's only grade.
    card_ids = sorted(
        {grades.key(r, "card_id") for r in grade_rows if grades.key(r, "card_id") is not None}
    )
    if card_ids:
        arriving = set(cards.sorted_ids.to_add)
        present = set(read_keys(dst, CARDS, [i for i in card_ids if i not in arriving]))
        orphans = [i for i in card_ids if i not in arriving and i not in present]
        if orphans:
            problems.append(
                f"{len(orphans):,} grades would point at Cards neither production nor "
                f"the restored copy has (e.g. Card {orphans[0]:,})"
            )
        with dst.cursor() as cur:
            cur.execute(
                "SELECT card_id FROM playbook_card_outcomes WHERE card_id = ANY(%s)", (card_ids,)
            )
            graded = [r[0] for r in cur.fetchall()]
        if graded:
            problems.append(
                f"{len(graded):,} Cards to put back already have a grade in production "
                f"(e.g. Card {graded[0]:,})"
            )

    # Nothing put back may change what goes out live.
    issued = [grades.key(r, "issued_at") for r in grade_rows]
    since = date.fromisoformat(str(config.PLAYBOOK_ADAPTIVE_SINCE))
    reach = entry_bar_reach(
        issued,
        since=since,
        lookback_days=int(config.PLAYBOOK_ADAPTIVE_LOOKBACK_DAYS),
        now=now,
    )
    if reach:
        problems.append(
            f"{reach:,} grades put back are from on or after {since} "
            "(PLAYBOOK_ADAPTIVE_SINCE), so the live entry bar would learn from them"
        )
    elif grade_rows:
        notes.append(
            f"Entry bar: it learns only from ideas issued on or after {since} "
            "(PLAYBOOK_ADAPTIVE_SINCE); every grade put back is older."
        )
    if card_rows:
        if config.SIGNALS_PATTERN_CALIBRATION_ENABLED:
            days = calibration_lookback_days(environ)
            clear_on = calibration_clear_on(
                [cards.key(r, "timestamp") for r in card_rows], lookback_days=days, now=now
            )
            if clear_on:
                problems.append(
                    f"pattern calibration is on and its nightly backtest looks back {days} "
                    f"days, which still reaches Cards put back; run this on or after "
                    f"{clear_on}, or they would move live Card confidence"
                )
            else:
                notes.append(
                    f"Pattern calibration: on, but its {days}-day lookback no longer "
                    "reaches any Card put back."
                )
        else:
            notes.append("Pattern calibration: off, so it reads none of this.")
    pending = [
        grades.key(r, "issued_at") for r in grade_rows if grades.key(r, "outcome") == "pending"
    ]
    if pending:
        notes.append(
            f"{len(pending):,} grades put back are still 'pending' (oldest "
            f"{min(pending).astimezone(ET).date()}); the grader finishes them on its next pass."
        )
    return Plan(plans, problems, notes)


# ---------------------------------------------------------------------------
# The copy
# ---------------------------------------------------------------------------


def copy_rows(src, dst, plan: TablePlan) -> int:
    """COPY the rows to add out of the source and INSERT them in production,
    inside production's open transaction. Returns rows inserted."""
    from psycopg2 import sql

    ids = plan.sorted_ids.to_add
    if not ids:
        return 0
    cols = sql.SQL(", ").join(_ident(c) for c in plan.columns)
    table = _ident(plan.spec.name)
    temp = _ident(f"_restore_{plan.spec.name}")
    with tempfile.SpooledTemporaryFile(max_size=_SPOOL_BYTES) as buf:
        with src.cursor() as cur:
            query = sql.SQL(
                "COPY (SELECT {cols} FROM {table} WHERE id = ANY({ids}) ORDER BY id) TO STDOUT"
            ).format(cols=cols, table=table, ids=sql.Literal(list(ids)))
            cur.copy_expert(query.as_string(src), buf)
        buf.seek(0)
        with dst.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "CREATE TEMP TABLE {temp} ON COMMIT DROP AS"
                    " SELECT {cols} FROM {table} WITH NO DATA"
                ).format(temp=temp, cols=cols, table=table)
            )
            cur.copy_expert(
                sql.SQL("COPY {temp} ({cols}) FROM STDIN")
                .format(temp=temp, cols=cols)
                .as_string(dst),
                buf,
            )
            cur.execute(
                sql.SQL(
                    "INSERT INTO {table} ({cols}) SELECT {cols} FROM {temp} ORDER BY id"
                ).format(table=table, cols=cols, temp=temp)
            )
            return cur.rowcount


def _count_present(dst, spec: TableSpec, ids: Sequence[int]) -> int:
    from psycopg2 import sql

    if not ids:
        return 0
    with dst.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {t} WHERE id = ANY(%s)").format(t=_ident(spec.name)),
            (list(ids),),
        )
        return int(cur.fetchone()[0])


def _span(plan: TablePlan) -> str:
    rows = plan.added_keys()
    if not rows:
        return ""
    times = [plan.key(r, plan.spec.time_col) for r in rows]
    ids = plan.sorted_ids.to_add
    first = min(times).astimezone(ET).date()
    last = max(times).astimezone(ET).date()
    return f" (ids {ids[0]:,}-{ids[-1]:,}, {first} to {last} ET)"


def report_plan(plan: Plan, out: Callable[[str], None]) -> None:
    for t in plan.tables:
        if not t.present_in_source:
            continue
        lo, hi, n = t.target_bounds
        out(f"{t.spec.label} ({t.spec.name})")
        out(f"  restored copy: {t.source_rows:,} rows")
        span = f", ids {lo:,}-{hi:,}" if n else ""
        out(f"  production:    {n:,} rows{span}")
        out(
            f"  to put back:   {len(t.sorted_ids.to_add):,}{_span(t)}; "
            f"already back: {len(t.sorted_ids.already_there):,}"
        )
        if t.dropped:
            out(f"  not copied (production has no such column): {', '.join(t.dropped)}")
    for note in plan.notes:
        out(note)


def restore(
    src,
    dst,
    *,
    apply: bool,
    now: Optional[datetime] = None,
    environ: Optional[Mapping[str, str]] = None,
    out: Callable[[str], None] = print,
    on_written: Optional[Callable[[], None]] = None,
    write_hint: str = "re-run with --apply",
) -> int:
    """Plan, check, copy inside one transaction on ``dst``, then commit
    (``apply``) or roll back. Returns 0 on success, 1 on a refusal.

    ``src`` should be a read-only session; ``dst`` must have no transaction
    open. ``on_written`` runs after the inserts, before the commit decision
    (tests use it to look inside the transaction).
    """
    now = now or datetime.now(timezone.utc)
    environ = os.environ if environ is None else environ
    out(f"From:  {describe(src)}")
    out(f"Into:  {describe(dst)}")
    out("")
    if dst.autocommit:
        # One transaction or nothing: a rehearsal's rollback, and the
        # all-or-nothing write, both depend on it.
        dst.autocommit = False
    try:
        with dst.cursor() as cur:
            # Don't queue behind a long lock (a schema apply) and stall live
            # writers queued behind us; give the copy itself room.
            cur.execute("SET LOCAL lock_timeout = '10s'")
            cur.execute("SET LOCAL statement_timeout = '600s'")
        plan = build_plan(src, dst, now=now, environ=environ)
        report_plan(plan, out)
        out("")
        if plan.problems:
            dst.rollback()
            out("Refusing. Nothing changed:")
            for p in plan.problems:
                out(f"  - {p}")
            return 1
        work = [t for t in plan.tables if t.present_in_source and t.sorted_ids.to_add]
        if not work:
            dst.rollback()
            out("Nothing to put back: production already has every row the restored copy holds.")
            return 0
        added: dict[str, int] = {}
        for t in work:
            inserted = copy_rows(src, dst, t)
            if inserted != len(t.sorted_ids.to_add):
                planned = len(t.sorted_ids.to_add)
                raise RuntimeError(
                    f"{t.spec.name}: inserted {inserted:,} rows, planned {planned:,}"
                )
            want = len(t.sorted_ids.to_add) + len(t.sorted_ids.already_there)
            have = _count_present(dst, t.spec, list(t.source_keys))
            if have != want:
                raise RuntimeError(
                    f"{t.spec.name}: production now holds {have:,} of the restored copy's "
                    f"rows, expected {want:,}"
                )
            added[t.spec.label] = inserted
        if on_written:
            on_written()
        summary = " and ".join(f"{n:,} {label.lower()}" for label, n in added.items())
        if apply:
            dst.commit()
            out(f"Done: put back {summary}.")
            if added.get(CARDS.label):
                out("Old /cards/<id> links, Scorecard days and the graded record are back.")
        else:
            dst.rollback()
            out(f"Rehearsal: put back {summary} inside one transaction; every check")
            out("passed, then it was rolled back. Nothing changed.")
            out(f"To write it: {write_hint}")
        return 0
    except Exception:
        try:
            dst.rollback()
        except Exception:  # noqa: BLE001
            logger.warning("rollback after a failed restore also failed", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _connect_source(host: str):
    import psycopg2

    from src.database.password_providers import get_db_password

    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME", "zerogex")
    user = os.getenv("DB_USER", "postgres")
    pgpass = Path(os.getenv("PGPASSFILE") or (Path.home() / ".pgpass"))
    password = source_password(
        os.environ,
        provider=get_db_password,
        host=os.getenv("DB_HOST", ""),
        port=port,
        dbname=dbname,
        user=user,
        pgpass_lines=lambda: pgpass.read_text().splitlines(),
    )
    params: dict[str, Any] = {
        "host": host,
        "port": int(port),
        "dbname": dbname,
        "user": user,
        "connect_timeout": 15,
        "sslmode": os.getenv("DB_SSLMODE", "").strip() or "require",
        "application_name": "restore_action_cards",
    }
    if password:
        params["password"] = password
    conn = psycopg2.connect(**params)
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    conn.set_client_encoding("UTF8")
    return conn


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Put back Action Cards and grades production lacks, from a restored copy"
    )
    parser.add_argument(
        "--source",
        required=True,
        help="restored RDS instance identifier (e.g. zerogex-restore) or full host name",
    )
    parser.add_argument("--apply", action="store_true", help="commit (default: rehearse)")
    args = parser.parse_args(argv)

    try:
        host = resolve_source_host(args.source, os.getenv("DB_HOST", ""))
    except ValueError as exc:
        print(f"Refusing: {exc}")
        return 1

    from src.database.connection import db_connection

    try:
        src = _connect_source(host)
    except Exception as exc:  # noqa: BLE001 - say what to fix, not a traceback
        print(f"Could not connect to the restored copy at {host}: {exc}".rstrip())
        print("If it timed out, its security group doesn't let this box in;")
        print("if the password was refused, set RESTORE_DB_PASSWORD.")
        return 1
    try:
        with db_connection() as dst:
            dst.set_client_encoding("UTF8")
            return restore(
                src,
                dst,
                apply=args.apply,
                write_hint=f"make playbook-restore-cards FROM={args.source} CONFIRM=yes",
            )
    finally:
        src.close()


if __name__ == "__main__":
    sys.exit(main())
