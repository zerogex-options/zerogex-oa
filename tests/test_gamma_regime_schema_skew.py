"""The writer must survive running one deploy ahead of its own column.

schema.sql is not re-run by a bare ``git pull``; only ``make pull`` /
``make schema-apply`` apply it, and the Makefile documents a prior incident
from exactly that skew. So new engine code can legitimately reach production
before the column it writes exists.

That happened here. The failed INSERT aborted the whole snapshot, so no bars
were written at all and the entire structure series went dark over one
optional column. These tests pin the degraded behaviour: without the column,
bars still land and the cushion is simply absent.
"""

import re
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine

UTC = timezone.utc


class _Cursor:
    """Cursor that answers the writer's probes and records its statements."""

    def __init__(self, columns):
        self._columns = set(columns)
        self.statements = []
        self._last = ""

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        self._last = sql

    def fetchone(self):
        if "percentile_cont" in self._last:
            return (20.0,)  # typical 30-minute move
        return None

    def fetchall(self):
        if "information_schema.columns" in self._last:
            return [(c,) for c in sorted(self._columns)]
        if "SELECT bar_start FROM gamma_regime_5min" in self._last:
            return []  # nothing written yet, so every bar is due
        if "gamma_flip_point" in self._last:
            # Flip observations for the session, one row per 5-minute bar that
            # had a gex_summary row -- the writer resolves the rest by carrying
            # forward (tests/test_gamma_flip_carry_forward.py owns that).
            return [(datetime(2026, 4, 24, 13, 30, tzinfo=UTC), 690.0)]
        # The chain read: one strike, enough to build a snapshot.
        return [(700.0, 700.0, datetime(2026, 4, 24).date(), 1.0e6, 1.0e6, 0.0, 10, 10)]

    @property
    def description(self):
        return [
            ("spot_price",),
            ("strike",),
            ("expiration",),
            ("net_gex",),
            ("call_gex",),
            ("put_gex",),
            ("call_oi",),
            ("put_oi",),
        ]


def _engine():
    eng = AnalyticsEngine.__new__(AnalyticsEngine)
    eng._analytics_flow_cache_refresh_enabled = True
    eng.db_symbol = "SPY"
    return eng


ALL_COLUMNS = ("gamma_flip", "typical_move_30m")


def _run(columns=ALL_COLUMNS):
    """Drive one snapshot refresh against a database with these columns."""
    AnalyticsEngine._gamma_regime_optional_cols = None  # re-probe per test
    eng = _engine()
    cursor = _Cursor(columns)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)

    with patch.object(main_engine, "db_connection", return_value=cm):
        eng._refresh_gamma_regime_snapshot(datetime(2026, 4, 24, 17, 0, tzinfo=UTC))
    return cursor


def _inserts(cursor):
    return [sql for sql, _ in cursor.statements if "INSERT INTO gamma_regime_5min" in sql]


def test_bars_are_still_written_when_every_optional_column_is_missing():
    """The regression. One absent optional column used to take down every
    bar; now it only costs that field."""
    inserts = _inserts(_run(columns=()))

    assert inserts, "no bars written at all — the column skew killed the snapshot"
    assert "gamma_flip" not in inserts[0]
    assert "typical_move_30m" not in inserts[0]


def test_optional_columns_are_written_once_they_exist():
    inserts = _inserts(_run())

    assert inserts
    for col in ALL_COLUMNS:
        assert col in inserts[0]
        assert f"{col} = EXCLUDED.{col}" in inserts[0]


def test_a_partial_migration_writes_what_it_can():
    """Columns arrive one deploy at a time, so the writer has to handle any
    subset rather than only all-or-nothing."""
    inserts = _inserts(_run(columns=("gamma_flip",)))

    assert inserts
    assert "gamma_flip" in inserts[0]
    assert "typical_move_30m" not in inserts[0]


def test_column_and_value_counts_match_in_every_shape():
    """An f-string assembling SQL is exactly where a column/value mismatch
    hides, and psycopg2 would only surface it at execution time.

    Placeholders are counted by regex rather than by splitting on ")", since
    ``%(symbol)s`` contains one. The character class must allow digits, or
    ``%(typical_move_30m)s`` goes uncounted and the test reports a mismatch
    that is its own.
    """
    for columns in ((), ("gamma_flip",), ("typical_move_30m",), ALL_COLUMNS):
        sql = _inserts(_run(columns))[0]
        col_block = sql.split("INSERT INTO gamma_regime_5min (")[1].split(") VALUES")[0]
        n_cols = len([c for c in col_block.replace("\n", " ").split(",") if c.strip()])
        n_vals = len(
            re.findall(r"%\([a-z0-9_]+\)s", sql.split("VALUES (")[1].split("ON CONFLICT")[0])
        )
        assert n_cols == n_vals, f"{columns}: {n_cols} columns vs {n_vals} values"


def test_the_probe_runs_once_and_is_cached():
    """A catalog lookup per bar would be waste; per process is the right
    granularity, since a restart is exactly when the answer can change."""
    cursor = _run()
    probes = [s for s, _ in cursor.statements if "information_schema.columns" in s]

    assert len(probes) == 1


def test_no_flip_lookup_when_the_column_is_absent():
    """Nowhere to put the answer, so do not pay for the query."""
    cursor = _run(columns=())
    lookups = [s for s, _ in cursor.statements if "gamma_flip_point" in s]

    assert lookups == []


def test_the_move_scale_is_computed_once_per_cycle_not_per_bar():
    """It is a multi-day median that barely moves intraday; recomputing it for
    each of 78 backfilled bars would be pure waste."""
    cursor = _run()
    moves = [s for s, _ in cursor.statements if "percentile_cont" in s]

    assert len(moves) == 1
