"""Regression: one pooled connection stuck in an aborted transaction failed
API requests until a restart.

On 2026-11-17 the API logged "Error fetching analytics snapshot: current
transaction is aborted, commands ignored until end of transaction block" about
every 7 s, each paired with "Rolled back open transaction before returning DB
connection", for at least the half hour checked, until the 00:18 ET restart.

That pairing is the fingerprint of a connection whose SERVER-side transaction
is aborted while psycopg2 believes the connection is idle. psycopg2's
``rollback()`` sends ROLLBACK only for a transaction the driver itself opened,
so neither ``db_connection()``'s rollback nor the one in
``close_db_connection`` reached the server; the pool took the connection back
and handed it to the next caller, whose first statement failed the same way.

These tests strand a real connection in that state and check that no later
borrower inherits it. They need a scratch Postgres, so they are
``integration``-marked and skip unless ``POOL_ABORTED_TX_TEST_DSN`` is set.
Each test builds and drops its own schema::

    POOL_ABORTED_TX_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_db_pool_aborted_transaction.py -q
"""

from __future__ import annotations

import logging
import os
import uuid

import pytest

from src.database import connection as db_conn

pytestmark = pytest.mark.integration

_DSN = os.getenv("POOL_ABORTED_TX_TEST_DSN")


@pytest.fixture
def real_pool(monkeypatch):
    """The module's pool, swapped for a real one into a throwaway schema."""
    if not _DSN:
        pytest.skip("POOL_ABORTED_TX_TEST_DSN not set: needs a scratch Postgres.")
    import psycopg2
    from psycopg2 import pool as pg_pool

    schema = f"zgx_pool_abort_{uuid.uuid4().hex[:10]}"
    admin = psycopg2.connect(_DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(
            f"CREATE TABLE {schema}.option_chains "
            "(underlying VARCHAR(10) NOT NULL, timestamp TIMESTAMPTZ NOT NULL)"
        )
    # minconn=1, as in production's .env: the pool keeps one idle connection,
    # so requests arriving one at a time all get the same one.
    pool = pg_pool.ThreadedConnectionPool(1, 4, _DSN, options=f"-c search_path={schema}")
    monkeypatch.setattr(db_conn, "_connection_pool", pool)
    try:
        yield pool
    finally:
        pool.closeall()
        with admin.cursor() as cur:
            cur.execute(f"DROP SCHEMA {schema} CASCADE")
        admin.close()


def _strand_in_aborted_transaction(conn) -> None:
    """Leave ``conn`` with an aborted transaction on the server while psycopg2
    believes it is idle, the state the production connection was in.

    psycopg2 can get here on its own: its ``commit()`` marks the driver idle
    even when COMMIT raised. A transaction the driver didn't open, then a
    failing statement, reaches the same state deterministically.
    """
    import psycopg2
    from psycopg2 import extensions

    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("BEGIN")
    with pytest.raises(psycopg2.errors.DivisionByZero):
        cur.execute("SELECT 1/0")
    conn.autocommit = False
    assert conn.get_transaction_status() == extensions.TRANSACTION_STATUS_INERROR
    assert conn.status == extensions.STATUS_READY


def test_a_stranded_connection_fails_no_later_borrower(real_pool):
    conn = db_conn.get_db_connection()
    _strand_in_aborted_transaction(conn)
    db_conn.close_db_connection(conn)

    for _ in range(3):
        with db_conn.db_connection() as c:
            cur = c.cursor()
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)


def test_a_block_that_strands_its_connection_does_not_pass_it_on(real_pool):
    """Every API caller goes through ``db_connection()``; its clean exit
    commits, which psycopg2 also skips for a transaction it didn't open."""
    with db_conn.db_connection() as conn:
        _strand_in_aborted_transaction(conn)

    with db_conn.db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_snapshot_reads_after_a_stranded_connection_log_no_error(real_pool, caplog):
    """The production symptom itself: ``_get_snapshot`` logged
    "Error fetching analytics snapshot: current transaction is aborted" on
    every call. With an empty chain table it should find no data, quietly."""
    from src.analytics.main_engine import AnalyticsEngine

    conn = db_conn.get_db_connection()
    _strand_in_aborted_transaction(conn)
    db_conn.close_db_connection(conn)

    engine = AnalyticsEngine(underlying="SPY")
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            assert engine._get_snapshot() is None

    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors == []
