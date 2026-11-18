"""Putting back the Action Card history, against two real Postgres databases.

The unit suite (tests/test_restore_action_cards.py) covers the planning rules.
Only a real server proves the copy itself: COPY round-trips every payload
byte, ids survive, the rehearsal really rolls back, the sequences are left
alone, and the entry bar's SQL ignores what was put back.

Run it against two SCRATCH databases (they may share a server)::

    RESTORE_CARDS_TEST_SOURCE_DSN=postgresql://u@host:5432/scratch_a \\
    RESTORE_CARDS_TEST_TARGET_DSN=postgresql://u@host:5432/scratch_b \\
        pytest -m integration tests/test_restore_action_cards_db.py

The DSNs are never derived from ``.env``. Everything happens in a throwaway
schema created in each database and dropped afterwards; the tables come from
``setup/database/schema.sql`` itself.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from src import config
from src.signals.playbook import adaptive_gate
from src.tools import restore_action_cards as rac

pytestmark = pytest.mark.integration

_SRC_DSN = os.getenv("RESTORE_CARDS_TEST_SOURCE_DSN")
_DST_DSN = os.getenv("RESTORE_CARDS_TEST_TARGET_DSN")
_SKIP = "RESTORE_CARDS_TEST_{SOURCE,TARGET}_DSN not set — restore DB harness skipped."

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
NOW = datetime.now(UTC).replace(microsecond=0)
# The restored copy's Cards are 30 days old; production's started 20 days
# ago, after a one-id gap, as on 2026-09-25 (11,649 then 11,651).
OLD = NOW - timedelta(days=30)
NEW = NOW - timedelta(days=20)
FLOOR = (NOW - timedelta(days=25)).astimezone(ET).date()
N_OLD = 40
N_NEW = 10
FIRST_NEW = N_OLD + 2

# Everything COPY's text format has to escape.
AWKWARD = 'tab\there, newline\nhere, back\\slash, quote " and ü ★ \\N'

SCHEMA_SQL = Path(__file__).resolve().parents[1] / "setup" / "database" / "schema.sql"


def _ddl(table: str) -> str:
    text = SCHEMA_SQL.read_text()
    m = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \(.*?\n\);", text, re.S)
    assert m, f"{table} not found in schema.sql"
    return m.group(0)


def _connect(dsn: str, schema: str):
    import psycopg2

    return psycopg2.connect(dsn, options=f"-c search_path={schema}")


def _card(i: int, ts: datetime, symbol: str) -> tuple:
    payload = {
        "entry": {"ref_price": 500 + i / 4, "trigger": "at_market"},
        "rationale": f"card {i}: {AWKWARD}",
        "legs": [{"strike": 500 + i, "right": "C"}],
        "max_hold_minutes": 60,
    }
    direction = "bullish" if i % 2 else "bearish"
    return (i, symbol, ts, f"pattern_{i % 4}", "BUY", "0DTE", direction, 0.5, json.dumps(payload))


def _grade(i: int, ts: datetime, symbol: str, outcome: str) -> tuple:
    r = {"target_hit": 1.5, "stop_hit": -1.0, "time_exit": 0.2}.get(outcome)
    return (
        i,
        i,
        symbol,
        f"pattern_{i % 4}",
        "BUY",
        "0DTE",
        "bullish" if i % 2 else "bearish",
        0.5,
        ts,
        500.0,
        "at_market",
        505.0,
        498.0,
        60,
        outcome,
        r,
    )


def _seed(conn, cards: list, grades: list, symbols: list, next_id: int) -> None:
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO symbols (symbol) VALUES (%s)", [(s,) for s in symbols])
        cur.executemany(
            "INSERT INTO signal_action_cards (id, underlying, timestamp, pattern, action, tier,"
            " direction, confidence, payload) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            cards,
        )
        cur.executemany(
            "INSERT INTO playbook_card_outcomes (id, card_id, underlying, pattern, action, tier,"
            " direction, confidence, issued_at, entry_price, entry_trigger, target_price,"
            " stop_price, max_hold_minutes, outcome, r_multiple)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            grades,
        )
        for t in ("signal_action_cards", "playbook_card_outcomes"):
            cur.execute(
                "SELECT setval(pg_get_serial_sequence(%s, 'id'), %s, true)", (t, next_id - 1)
            )
    conn.commit()


@pytest.fixture()
def dbs(monkeypatch):
    if not (_SRC_DSN and _DST_DSN):
        pytest.skip(_SKIP)
    schema = f"restore_test_{uuid.uuid4().hex[:8]}"
    made: list[str] = []
    src = dst = None
    try:
        for dsn in (_SRC_DSN, _DST_DSN):
            conn = _connect(dsn, "public")
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA {schema}")
                cur.execute(f"SET search_path = {schema}")
                cur.execute("CREATE TABLE symbols (symbol VARCHAR(10) PRIMARY KEY)")
                cur.execute(_ddl("signal_action_cards"))
                cur.execute(_ddl("playbook_card_outcomes"))
            conn.commit()
            conn.close()
            made.append(dsn)
        src, dst = _populate(schema)
        monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_SINCE", FLOOR)
        monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_LOOKBACK_DAYS", 90)
        monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_ENABLED", False)
        yield src, dst, schema
    finally:
        if src is not None:
            src.close()
        if dst is not None:
            dst.rollback()
            dst.close()
        for dsn in made:
            conn = _connect(dsn, "public")
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA {schema} CASCADE")
            conn.commit()
            conn.close()


def _populate(schema: str):
    old_cards = [
        _card(i, OLD + timedelta(minutes=i), "SPY" if i % 3 else "QQQ") for i in range(1, N_OLD + 1)
    ]
    old_grades = [
        _grade(
            i,
            OLD + timedelta(minutes=i),
            "SPY" if i % 3 else "QQQ",
            "pending" if i > N_OLD - 3 else "time_exit",
        )
        for i in range(1, N_OLD + 1)
    ]
    new_ids = range(FIRST_NEW, FIRST_NEW + N_NEW)
    new_cards = [_card(i, NEW + timedelta(minutes=i), "SPY") for i in new_ids]
    new_grades = [_grade(i, NEW + timedelta(minutes=i), "SPY", "target_hit") for i in new_ids]

    writer = _connect(_SRC_DSN, schema)
    _seed(writer, old_cards, old_grades, ["SPY", "QQQ"], N_OLD + 1)
    writer.close()
    dst = _connect(_DST_DSN, schema)
    _seed(dst, new_cards, new_grades, ["SPY", "QQQ"], FIRST_NEW + N_NEW)
    src = _connect(_SRC_DSN, schema)
    src.set_session(readonly=True, isolation_level="REPEATABLE READ")
    return src, dst


def _run(src, dst, *, apply: bool, **kw) -> tuple[int, str]:
    lines: list[str] = []
    code = rac.restore(src, dst, apply=apply, now=NOW, environ={}, out=lines.append, **kw)
    return code, "\n".join(lines)


def _count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        n = cur.fetchone()[0]
    conn.rollback()
    return n


def _next_card_id(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO signal_action_cards (underlying, timestamp, pattern, action, tier,"
            " direction, confidence, payload) VALUES ('SPY', now(), 'p', 'BUY', '0DTE',"
            " 'bullish', 0.5, '{}') RETURNING id"
        )
        new_id = cur.fetchone()[0]
    conn.rollback()
    return new_id


def test_rehearsal_does_the_whole_copy_then_changes_nothing(dbs):
    src, dst, _ = dbs
    seen = {}

    def inside():
        with dst.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM signal_action_cards")
            seen["cards"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM playbook_card_outcomes")
            seen["grades"] = cur.fetchone()[0]

    code, text = _run(src, dst, apply=False, on_written=inside)
    assert code == 0, text
    assert seen == {"cards": N_OLD + N_NEW, "grades": N_OLD + N_NEW}
    assert "Rehearsal" in text and "Nothing changed" in text
    assert "3 grades put back are still 'pending'" in text
    assert _count(dst, "signal_action_cards") == N_NEW
    assert _count(dst, "playbook_card_outcomes") == N_NEW


def test_apply_puts_every_row_back_under_its_old_id(dbs):
    src, dst, _ = dbs
    code, text = _run(src, dst, apply=True)
    assert code == 0, text
    assert f"put back {N_OLD} action cards and {N_OLD} grades" in text

    query = (
        "SELECT id, underlying, timestamp, pattern, payload FROM signal_action_cards"
        " WHERE id <= %s ORDER BY id"
    )
    with src.cursor() as cur:
        cur.execute(query, (N_OLD,))
        before = cur.fetchall()
    src.rollback()
    with dst.cursor() as cur:
        cur.execute(query, (N_OLD,))
        after = cur.fetchall()
        cur.execute(
            "SELECT COUNT(*) FROM playbook_card_outcomes o JOIN signal_action_cards c"
            " ON c.id = o.card_id WHERE o.id <= %s AND o.card_id = o.id",
            (N_OLD,),
        )
        linked = cur.fetchone()[0]
    dst.rollback()
    assert after == before
    assert after[0][4]["rationale"] == f"card 1: {AWKWARD}"
    assert linked == N_OLD
    assert _count(dst, "signal_action_cards") == N_OLD + N_NEW
    # The next live Card takes the next id after production's own, as before.
    assert _next_card_id(dst) == FIRST_NEW + N_NEW


def test_the_entry_bar_learns_nothing_from_what_was_put_back(dbs):
    src, dst, _ = dbs
    assert _run(src, dst, apply=True)[0] == 0
    with dst.cursor() as cur:
        cur.execute(adaptive_gate.records_sql())
        rows = cur.fetchall()
    dst.rollback()
    # Only production's own ten graded ideas count; the forty older ones don't.
    assert sum(r[3] for r in rows) == N_NEW


def test_a_second_run_adds_nothing(dbs):
    src, dst, _ = dbs
    assert _run(src, dst, apply=True)[0] == 0
    with dst.cursor() as cur:  # the grader finishes a restored pending grade
        cur.execute(
            "UPDATE playbook_card_outcomes SET outcome = 'stop_hit', r_multiple = -1"
            " WHERE id = %s",
            (N_OLD,),
        )
    dst.commit()
    code, text = _run(src, dst, apply=True)
    assert code == 0, text
    assert "Nothing to put back" in text
    assert _count(dst, "signal_action_cards") == N_OLD + N_NEW


def test_an_id_production_used_for_another_card_is_refused(dbs):
    src, dst, _ = dbs
    with dst.cursor() as cur:
        cur.execute(
            "INSERT INTO signal_action_cards (id, underlying, timestamp, pattern, action, tier,"
            " direction, confidence, payload) VALUES (3, 'SPY', now(), 'other', 'BUY', '0DTE',"
            " 'bullish', 0.5, '{}')"
        )
    dst.commit()
    code, text = _run(src, dst, apply=True)
    assert code == 1
    assert "already belong to different rows" in text and "Nothing changed" in text
    assert _count(dst, "signal_action_cards") == N_NEW + 1


def test_the_same_database_is_refused(dbs):
    _, dst, schema = dbs
    other = _connect(_DST_DSN, schema)
    try:
        code, text = _run(other, dst, apply=True)
    finally:
        other.close()
    assert code == 1
    assert "same database" in text


def test_grades_the_entry_bar_would_learn_from_are_refused(dbs, monkeypatch):
    src, dst, _ = dbs
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_SINCE", (OLD - timedelta(days=1)).date())
    code, text = _run(src, dst, apply=True)
    assert code == 1
    assert "PLAYBOOK_ADAPTIVE_SINCE" in text
    assert _count(dst, "signal_action_cards") == N_NEW


def test_calibration_that_still_reaches_the_cards_is_refused_until_it_doesnt(dbs, monkeypatch):
    src, dst, _ = dbs
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_ENABLED", True)
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_LOOKBACK_DAYS", 60)
    code, text = _run(src, dst, apply=True)
    assert code == 1
    assert "pattern calibration is on" in text and "run this on or after" in text
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_LOOKBACK_DAYS", 20)
    code, text = _run(src, dst, apply=True)
    assert code == 0, text
    assert "no longer reaches" in text


def test_a_symbol_production_lacks_is_refused(dbs):
    src, dst, _ = dbs
    with dst.cursor() as cur:
        cur.execute("DELETE FROM playbook_card_outcomes WHERE underlying = 'QQQ'")
        cur.execute("DELETE FROM signal_action_cards WHERE underlying = 'QQQ'")
        cur.execute("DELETE FROM symbols WHERE symbol = 'QQQ'")
    dst.commit()
    code, text = _run(src, dst, apply=True)
    assert code == 1
    assert "symbols table lacks QQQ" in text


def test_a_rehearsal_on_an_autocommit_connection_still_changes_nothing(dbs):
    src, dst, _ = dbs
    dst.autocommit = True
    code, text = _run(src, dst, apply=False)
    assert code == 0, text
    assert dst.autocommit is False
    assert _count(dst, "signal_action_cards") == N_NEW
    assert _count(dst, "playbook_card_outcomes") == N_NEW
