"""Regression tests for /api/signals/basic bundle query shape and behavior.

Context: signal_component_scores is written every signal-engine cycle
(~1 row/s/component).  The bundle endpoint previously used
``DISTINCT ON (component_name) ... ORDER BY component_name, timestamp
DESC`` over ``component_name IN (6)``, which forced a scan + sort of
every historical row for those components and deduped to 6 — cost grew
with the table all session (observed 0.6s -> 6s -> 12s within one
trading day) while every per-signal endpoint stayed ~10ms.

The query is now 6 LATERAL single-row lookups, each resolving to the
same indexed ``component_name=… AND underlying=… ORDER BY timestamp
DESC LIMIT 1`` the per-signal endpoints use, so it is constant-time
regardless of table size.  These tests pin that shape so a refactor
can't silently reintroduce the table-wide DISTINCT ON.
"""

import asyncio
from contextlib import asynccontextmanager

from src.api.database import DatabaseManager

_BASIC = (
    "tape_flow_bias",
    "skew_delta",
    "vanna_charm_flow",
    "dealer_delta_pressure",
    "gex_gradient",
    "positioning_trap",
)


class _RecordingConn:
    def __init__(self, fetch_rows=None):
        self._fetch_rows = fetch_rows or []
        self.queries = []

    async def fetch(self, query, *_args):
        self.queries.append(query)
        return list(self._fetch_rows)


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


def test_bundle_query_uses_per_component_lateral_not_distinct_on():
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)

    asyncio.run(db.get_latest_basic_signals_bundle("SPY"))

    assert conn.queries, "bundle query was never executed"
    sql = conn.queries[0]

    # The table-wide scan+sort+dedupe must be gone.
    assert "DISTINCT ON" not in sql
    # Replaced by a per-component VALUES list + LATERAL LIMIT 1.
    assert "CROSS JOIN LATERAL" in sql
    assert "LIMIT 1" in sql
    assert "VALUES" in sql
    # All six basic components are still enumerated.
    for name in _BASIC:
        assert f"'{name}'" in sql


def test_bundle_maps_present_rows_and_leaves_missing_as_none():
    db = DatabaseManager()
    rows = [
        {
            "component_name": "tape_flow_bias",
            "timestamp": "2026-05-15T20:00:00Z",
            "clamped_score": 0.42,
            "weighted_score": 0.0,
            "weight": 0.0,
            "context_values": {"foo": 1},
        },
        {
            "component_name": "gex_gradient",
            "timestamp": "2026-05-15T20:00:00Z",
            "clamped_score": -0.20,
            "weighted_score": 0.0,
            "weight": 0.0,
            "context_values": None,
        },
    ]
    conn = _RecordingConn(fetch_rows=rows)
    _install_conn(db, conn)

    out = asyncio.run(db.get_latest_basic_signals_bundle("SPY"))

    # Every basic key is present in the result mapping.
    assert set(out.keys()) == set(_BASIC)
    # Present rows are mapped with score = clamped_score * 100 (2dp).
    assert out["tape_flow_bias"]["score"] == 42.0
    assert out["tape_flow_bias"]["context_values"] == {"foo": 1}
    assert out["gex_gradient"]["score"] == -20.0
    assert out["gex_gradient"]["context_values"] == {}
    # Components with no row stay None (CROSS JOIN drops them) — matches
    # the old DISTINCT ON behavior.
    for name in ("skew_delta", "vanna_charm_flow", "dealer_delta_pressure", "positioning_trap"):
        assert out[name] is None


def test_bundle_passes_symbol_as_only_bind_param():
    db = DatabaseManager()
    captured = {}

    class _Conn(_RecordingConn):
        async def fetch(self, query, *args):
            captured["args"] = args
            return []

    conn = _Conn()
    _install_conn(db, conn)
    asyncio.run(db.get_latest_basic_signals_bundle("spy"))
    # symbol upper-cased by the router, but the query method passes it through;
    # only one bind param ($1 = underlying).
    assert captured["args"] == ("spy",)
