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

    def __init__(self, has_flip: bool):
        self._has_flip = has_flip
        self.statements = []
        self._last = ""

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        self._last = sql

    def fetchone(self):
        if "information_schema.columns" in self._last:
            return (1,) if self._has_flip else None
        if "gamma_flip_point" in self._last:
            return (690.0,)
        return None

    def fetchall(self):
        if "SELECT bar_start FROM gamma_regime_5min" in self._last:
            return []  # nothing written yet, so every bar is due
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


def _run(has_flip: bool):
    """Drive one snapshot refresh against a cursor with or without the column."""
    AnalyticsEngine._gamma_regime_has_flip = None  # re-probe per test
    eng = _engine()
    cursor = _Cursor(has_flip)
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


def test_bars_are_still_written_when_the_column_is_missing():
    """The regression. One absent optional column used to take down every
    bar; now it only costs the cushion."""
    inserts = _inserts(_run(has_flip=False))

    assert inserts, "no bars written at all — the column skew killed the snapshot"
    assert "gamma_flip" not in inserts[0]


def test_the_flip_is_written_once_the_column_exists():
    inserts = _inserts(_run(has_flip=True))

    assert inserts
    assert "gamma_flip" in inserts[0]
    assert "gamma_flip = EXCLUDED.gamma_flip" in inserts[0]


def test_column_and_value_counts_match_in_both_shapes():
    """An f-string assembling SQL is exactly where a column/value mismatch
    hides, and psycopg2 would only surface it at execution time.

    Placeholders are counted by regex rather than by splitting on ")", since
    ``%(symbol)s`` contains one.
    """
    for has_flip in (True, False):
        sql = _inserts(_run(has_flip))[0]
        col_block = sql.split("INSERT INTO gamma_regime_5min (")[1].split(") VALUES")[0]
        n_cols = len([c for c in col_block.replace("\n", " ").split(",") if c.strip()])
        n_vals = len(re.findall(r"%\([a-z_]+\)s", sql.split("VALUES (")[1].split("ON CONFLICT")[0]))
        assert n_cols == n_vals, f"has_flip={has_flip}: {n_cols} columns vs {n_vals} values"


def test_the_probe_runs_once_and_is_cached():
    """A catalog lookup per bar would be waste; per process is the right
    granularity, since a restart is exactly when the answer can change."""
    cursor = _run(has_flip=True)
    probes = [s for s, _ in cursor.statements if "information_schema.columns" in s]

    assert len(probes) == 1


def test_no_flip_lookup_when_the_column_is_absent():
    """Nowhere to put the answer, so do not pay for the query."""
    cursor = _run(has_flip=False)
    lookups = [s for s, _ in cursor.statements if "gamma_flip_point" in s]

    assert lookups == []
