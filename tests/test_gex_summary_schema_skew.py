"""The gex_summary writer must survive running one deploy ahead of its columns.

schema.sql is not re-run by a bare ``git pull`` -- only ``make pull`` /
``make schema-apply`` apply it -- so engine code can legitimately reach
production before a column it writes exists. 2635cb3 hardened the
gamma_regime_5min writer after exactly that happened there; these tests hold
the same line on the larger table.

The stake is higher here. ``_store_gex_summary`` shares a ``db_connection()``
transaction with the by-strike, profile and daily-rollup writes, and that
transaction rolls back on ANY exception. One missing optional column in this
one INSERT therefore discards the entire snapshot -- not a degraded reading,
no rows at all, for every symbol, every cycle, until someone runs the ALTER.
The degraded behaviour these tests pin is: the row lands, the absent field is
simply not in it, and a warning names the command that fixes it.

The statement is assembled from a single ordered column list rather than
spliced from fragments, so the four places a column has to appear -- the
column list, the VALUES list, the DO UPDATE SET and the IS DISTINCT FROM
guard -- are generated together. Several tests below check that arithmetic in
both the full and the degraded shape, because an f-string building SQL is
precisely where a mismatch hides and psycopg2 would only surface it at
execution time.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

from src.analytics.main_engine import (
    _GEX_SUMMARY_COLUMNS,
    _GEX_SUMMARY_KEY_COLUMNS,
    _GEX_SUMMARY_OPTIONAL_COLUMNS,
    AnalyticsEngine,
)

UTC = timezone.utc
TS = datetime(2026, 9, 20, 14, 0, tzinfo=UTC)


class _Cursor:
    """Answers the writer's column probe and records what it executed."""

    def __init__(self, present):
        self.present = set(present)
        self.statements = []
        self._last = ""
        self.probes = 0

    def execute(self, sql, params=None):
        self._last = sql
        if "information_schema.columns" in sql:
            self.probes += 1
        else:
            self.statements.append((sql, params))

    def fetchall(self):
        if "information_schema.columns" in self._last:
            return [(c,) for c in sorted(self.present)]
        return []

    def fetchone(self):
        return None

    @property
    def upsert(self):
        """The one INSERT the writer issued."""
        stmts = [s for s, _ in self.statements if "INSERT INTO gex_summary" in s]
        assert len(stmts) == 1, f"expected exactly one upsert, got {len(stmts)}"
        return stmts[0]

    @property
    def bound(self):
        return [p for s, p in self.statements if "INSERT INTO gex_summary" in s][0]


@pytest.fixture(autouse=True)
def _clear_process_cache():
    """The probe answer is cached for the life of the process by design."""
    AnalyticsEngine._gex_summary_optional_cols = None
    AnalyticsEngine._gex_summary_sql = None
    yield
    AnalyticsEngine._gex_summary_optional_cols = None
    AnalyticsEngine._gex_summary_sql = None


def _summary():
    return {
        "underlying": "SPX",
        "timestamp": TS,
        "max_gamma_strike": 5000,
        "max_gamma_value": 1.0,
        # Explicitly unresolved, so the writer persists NULL rather than
        # running the carry-forward SELECT (that path has its own suite).
        "gamma_flip_point": None,
        "gamma_flip_unresolved": True,
        "put_call_ratio": 1.1,
        "max_pain": 4990,
        "total_call_volume": 10,
        "total_put_volume": 20,
        "total_call_oi": 30,
        "total_put_oi": 40,
        "total_net_gex": 1234.5,
        "net_gex_at_spot": 99.0,
        "local_gex": 7.0,
        "convexity_risk": 0.5,
        "call_wall": 5010,
        "put_wall": 4980,
        "gamma_flip_raw": 4995.0,
        "pin_strike": 5000,
        "pin_score": 0.8,
        "pin_confidence": 0.9,
        "gamma_flip_reason": "BEYOND_MAX_DISTANCE",
        "data_as_of": TS,
    }


def _write(present):
    cur = _Cursor(present)
    AnalyticsEngine.__new__(AnalyticsEngine)._store_gex_summary(_summary(), cur)
    return cur


def _insert_columns(sql):
    block = re.search(r"INSERT INTO gex_summary\s*\((.*?)\)\s*VALUES", sql, re.S).group(1)
    return [c.strip() for c in block.split(",") if c.strip()]


def _value_slots(sql):
    block = re.search(r"VALUES\s*\((.*?)\)\s*ON CONFLICT", sql, re.S).group(1)
    return [v.strip() for v in block.split(",") if v.strip()]


def _set_targets(sql):
    block = re.search(r"DO UPDATE SET(.*?)\nWHERE", sql, re.S).group(1)
    return re.findall(r"(\w+)\s*=", block)


def _guard_columns(sql):
    return re.findall(r"EXCLUDED\.(\w+) IS DISTINCT FROM", sql[sql.rindex("WHERE") :])


def test_the_declared_optional_set_is_exactly_what_schema_adds_by_alter():
    """The writer is only as skew-proof as this set is current.

    A column added to gex_summary by ALTER but not listed optional is one the
    writer will still hard-code, which is the whole failure being guarded
    against -- so the schema, not a memory of it, decides the set.
    """
    schema = open("setup/database/schema.sql").read()
    altered = set(re.findall(r"ALTER TABLE gex_summary ADD COLUMN IF NOT EXISTS (\w+)", schema))
    assert altered, "parsed no ALTER ... ADD COLUMN lines — the scrape broke"
    assert altered == set(_GEX_SUMMARY_OPTIONAL_COLUMNS), (
        "schema.sql and _GEX_SUMMARY_OPTIONAL_COLUMNS disagree. "
        f"in schema only: {sorted(altered - set(_GEX_SUMMARY_OPTIONAL_COLUMNS))}; "
        f"declared only: {sorted(set(_GEX_SUMMARY_OPTIONAL_COLUMNS) - altered)}"
    )


def test_every_column_present_writes_the_whole_row():
    cur = _write(_GEX_SUMMARY_OPTIONAL_COLUMNS)
    cols = _insert_columns(cur.upsert)
    assert set(cols) == set(_GEX_SUMMARY_COLUMNS) | {"computed_at"}
    assert cur.bound["gamma_flip_reason"] == "BEYOND_MAX_DISTANCE"
    assert cur.bound["total_net_gex"] == 1234.5


def test_a_missing_optional_column_still_writes_the_row_without_it():
    """The incident shape: the row lands, minus the one absent field."""
    cur = _write(set(_GEX_SUMMARY_OPTIONAL_COLUMNS) - {"gamma_flip_reason"})
    cols = _insert_columns(cur.upsert)
    assert "gamma_flip_reason" not in cols
    assert "gamma_flip_reason" not in _set_targets(cur.upsert)
    assert "gamma_flip_reason" not in _guard_columns(cur.upsert)
    # Everything else still lands -- the point of degrading instead of failing.
    assert "total_net_gex" in cols and "pin_score" in cols


def test_a_missing_column_warns_with_the_command_that_fixes_it(caplog):
    with caplog.at_level("WARNING"):
        _write(set(_GEX_SUMMARY_OPTIONAL_COLUMNS) - {"gamma_flip_reason"})
    warning = "\n".join(r.getMessage() for r in caplog.records)
    assert "gamma_flip_reason" in warning
    assert "make schema-apply" in warning


@pytest.mark.parametrize(
    "absent",
    [
        frozenset(),
        frozenset({"gamma_flip_reason"}),
        frozenset({"computed_at"}),
        frozenset({"data_as_of", "pin_strike_reason", "convexity_risk"}),
        frozenset(_GEX_SUMMARY_OPTIONAL_COLUMNS),
    ],
    ids=["all-present", "one-missing", "no-computed-at", "several-missing", "none-present"],
)
def test_columns_and_placeholders_agree_in_every_assembled_shape(absent):
    """Counting columns against placeholders, in each shape the probe can
    produce. An f-string assembling SQL is exactly where this drifts, and
    psycopg2 would only report it at execution time."""
    cur = _write(set(_GEX_SUMMARY_OPTIONAL_COLUMNS) - absent)
    sql = cur.upsert
    cols, slots = _insert_columns(sql), _value_slots(sql)
    assert len(cols) == len(slots), f"{len(cols)} columns vs {len(slots)} values"

    # Every bound placeholder names a column in the list, in the same order.
    named = [re.fullmatch(r"%\((\w+)\)s", s).group(1) for s in slots if s != "NOW()"]
    assert named == [c for c in cols if c != "computed_at"]

    # Each key the statement binds must be one the writer supplies.
    assert set(named) <= set(cur.bound)


@pytest.mark.parametrize(
    "absent",
    [frozenset(), frozenset({"gamma_flip_reason"}), frozenset({"computed_at"})],
    ids=["all-present", "one-missing", "no-computed-at"],
)
def test_the_conflict_clause_tracks_the_column_list(absent):
    cur = _write(set(_GEX_SUMMARY_OPTIONAL_COLUMNS) - absent)
    sql = cur.upsert
    cols = set(_insert_columns(sql))
    updatable = cols - set(_GEX_SUMMARY_KEY_COLUMNS) - {"computed_at"}

    # The key is never reassigned by the update it conflicts on.
    assert not (set(_set_targets(sql)) & set(_GEX_SUMMARY_KEY_COLUMNS))
    assert set(_set_targets(sql)) - {"computed_at"} == updatable

    # computed_at is NOW() on every cycle, so it must stay OUT of the guard:
    # in it, every row differs from itself and the upsert rewrites the whole
    # table forever. tests/…_carry_sql and the convergence harness care.
    assert "computed_at" not in _guard_columns(sql)
    assert set(_guard_columns(sql)) == updatable


def test_a_missing_REQUIRED_column_is_not_quietly_tolerated():
    """Degrading is for columns bolted on later. A database missing a core
    metric is broken rather than behind, and hiding that would file a gutted
    row under a name that claims to be a summary."""
    required = set(_GEX_SUMMARY_COLUMNS) - set(_GEX_SUMMARY_OPTIONAL_COLUMNS)
    assert {"underlying", "timestamp", "total_net_gex"} <= required
    # The probe only ever removes optional columns, so a required one stays in
    # the statement and Postgres rejects the write, loudly.
    cur = _write(frozenset())
    assert required <= set(_insert_columns(cur.upsert))


def test_the_catalog_is_probed_once_per_process_not_once_per_write():
    """A per-write lookup would be a catalog query every cycle to answer a
    question only a deploy can change."""
    cur = _Cursor(_GEX_SUMMARY_OPTIONAL_COLUMNS)
    engine = AnalyticsEngine.__new__(AnalyticsEngine)
    for _ in range(3):
        engine._store_gex_summary(_summary(), cur)
    assert cur.probes == 1
    assert len([s for s, _ in cur.statements if "INSERT INTO gex_summary" in s]) == 3
