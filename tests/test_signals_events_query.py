"""get_signal_events must query the real signal_events schema.

The method SELECT'd and ORDER BY'd ``emitted_at``, a column that does not
exist — signal_events records the emit time in ``timestamp`` (see the writer
in src/signals/unified_signal_engine.py). Every call raised
UndefinedColumnError, which the method swallows and turns into ``[]``.

This test drives the method with a fake connection and asserts the emitted
SQL references ``timestamp`` (not ``emitted_at``) and that the row mapping is
returned intact. The query's execution against a real Postgres (schema
loaded) was verified separately.
"""

from __future__ import annotations

import asyncio
import contextlib

from src.api.queries.signals import SignalsQueriesMixin


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows
        self.sql = None
        self.args = None

    async def fetch(self, sql, *args):
        self.sql = sql
        self.args = args
        return self._rows


class _Harness(SignalsQueriesMixin):
    def __init__(self, conn):
        self._conn = conn

    @contextlib.asynccontextmanager
    async def _acquire_connection(self):
        yield self._conn


def _row():
    return {
        "id": 1,
        "underlying": "SPY",
        "signal_name": "vol_expansion",
        "timestamp": "2026-07-08T00:00:00+00:00",
        "direction": "bullish",
        "score": 0.8,
        "context_values": {},
        "close_at_emit": 500.0,
        "close_30m": None,
        "close_60m": None,
        "close_120m": None,
        "outcome_30m": None,
        "outcome_60m": None,
        "outcome_120m": None,
    }


def test_get_signal_events_uses_timestamp_not_emitted_at():
    conn = _FakeConn([_row()])
    h = _Harness(conn)
    out = asyncio.run(h.get_signal_events("SPY", "", 100))

    assert conn.sql is not None, "query never ran"
    # The non-existent column must not come back.
    assert "emitted_at" not in conn.sql
    # timestamp is both selected and used for ordering.
    assert "timestamp," in conn.sql
    assert "ORDER BY timestamp" in conn.sql
    # Params forwarded unchanged (symbol, signal_name sentinel, limit).
    assert conn.args == ("SPY", "", 100)
    # Row mapping preserved, keyed by the real column.
    assert out[0]["timestamp"] == "2026-07-08T00:00:00+00:00"
    assert out[0]["context_values"] == {}


def test_get_signal_events_parses_json_context_values():
    row = _row()
    row["context_values"] = '{"gex": 1.5}'  # asyncpg may hand back JSON as text
    conn = _FakeConn([row])
    h = _Harness(conn)
    out = asyncio.run(h.get_signal_events("SPY", "vol_expansion", 50))
    assert conn.args == ("SPY", "vol_expansion", 50)
    assert out[0]["context_values"] == {"gex": 1.5}
