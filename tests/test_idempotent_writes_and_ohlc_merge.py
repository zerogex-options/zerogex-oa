"""Regression tests for two data-integrity fixes:

* #2 underlying_quotes ON CONFLICT must AGGREGATE the in-progress minute
  (first-seen open, max high, min low, last close) — not blindly
  overwrite. _merge_bar only carries volume forward, so a reconnect /
  out-of-order partial bar would otherwise regress the stored H/L.

* #5 signal_events and signal_action_cards have no UNIQUE on their
  logical key. A process restart / overlapping cycle re-emitting the
  same (key, timestamp) double-inserts (double-counts hit-rate,
  double-fires downstream). The writers now use an idempotent
  INSERT … SELECT … WHERE NOT EXISTS guard (no schema change, so no
  deployment-time dedup risk).

No live Postgres in CI, so — consistent with the repo's existing SQL
tests (see test_api_database_flow_refresh.py / test_api_flow_series.py)
— these pin the generated SQL *contract* and the param/placeholder
parity through the real code paths. A revert to the buggy form fails
these.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone

import pytz

import src.ingestion.main_engine as me
from src.ingestion.main_engine import IngestionEngine

ET = pytz.timezone("US/Eastern")


# ===========================================================================
# #2 — underlying_quotes intra-minute OHLC merge
# ===========================================================================
class _RecCursor:
    def __init__(self, sink):
        self._sink = sink

    def execute(self, sql, params=None):
        self._sink.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _RecConn:
    def __init__(self, sink):
        self._sink = sink

    def cursor(self):
        return _RecCursor(self._sink)

    def commit(self):
        pass

    def rollback(self):
        pass


def _patch_db(monkeypatch, sink):
    @contextlib.contextmanager
    def _conn():
        yield _RecConn(sink)

    monkeypatch.setattr(me, "db_connection", _conn)


def _underlying_engine() -> IngestionEngine:
    e = IngestionEngine.__new__(IngestionEngine)
    e._db_backoff_until = 0.0
    e._db_consecutive_failures = 0
    e.errors_count = 0
    e.underlying_bars_stored = 0
    e.last_flush_time = None
    return e


def test_underlying_upsert_aggregates_intraminute_does_not_overwrite(monkeypatch):
    sink: list = []
    _patch_db(monkeypatch, sink)
    e = _underlying_engine()

    e._upsert_underlying_quote(
        {
            "symbol": "SPX",
            "timestamp": datetime(2026, 5, 15, 14, 31, tzinfo=timezone.utc),
            "open": 5500.0,
            "high": 5505.0,
            "low": 5498.0,
            "close": 5502.0,
            "up_volume": 1000,
            "down_volume": 400,
        }
    )

    assert len(sink) == 1
    sql, params = sink[0]
    norm = " ".join(sql.split())  # collapse whitespace for robust matching

    # The conflict clause must take the period-correct aggregate, NOT a
    # blind EXCLUDED overwrite (the pre-fix bug).
    assert "ON CONFLICT (symbol, timestamp) DO UPDATE SET" in norm
    assert "open = COALESCE(underlying_quotes.open, EXCLUDED.open)" in norm
    assert "high = GREATEST(underlying_quotes.high, EXCLUDED.high)" in norm
    assert "low = LEAST(underlying_quotes.low, EXCLUDED.low)" in norm
    # close stays last-tick-wins — that part was correct.
    assert "close = EXCLUDED.close" in norm
    # Regression guard: the buggy unconditional overwrites are gone.
    assert "high = EXCLUDED.high" not in norm
    assert "low = EXCLUDED.low" not in norm

    # The INSERT column/param contract is unchanged (8 cols, 8 params,
    # same order) — the fix is conflict-clause-only.
    assert norm.count("%s") == 8
    assert params == (
        "SPX",
        datetime(2026, 5, 15, 14, 31, tzinfo=timezone.utc),
        5500.0,
        5505.0,
        5498.0,
        5502.0,
        1000,
        400,
    )


def test_underlying_upsert_skipped_during_circuit_breaker(monkeypatch):
    """Pre-existing behavior must be preserved: no write while backing off."""
    sink: list = []
    _patch_db(monkeypatch, sink)
    e = _underlying_engine()
    e._db_backoff_until = me._time.monotonic() + 999.0

    e._upsert_underlying_quote(
        {
            "symbol": "SPX",
            "timestamp": datetime(2026, 5, 15, 14, 31, tzinfo=timezone.utc),
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "up_volume": 0,
            "down_volume": 0,
        }
    )
    assert sink == []


# ===========================================================================
# #5b — signal_action_cards idempotent insert (sync + async writers)
# ===========================================================================
def test_action_card_sync_insert_is_idempotent_guarded():
    from src.signals.playbook.cycle import insert_action_card_sync

    executions: list = []

    class _C:
        def cursor(self):
            return self

        def execute(self, sql, params):
            executions.append((sql, params))

        def commit(self):
            pass

        def rollback(self):
            pass

    ts = datetime(2026, 5, 15, 18, 30, tzinfo=timezone.utc)
    insert_action_card_sync(
        _C(),
        {
            "underlying": "SPY",
            "timestamp": ts,
            "pattern": "call_wall_fade",
            "action": "SELL_CALL_SPREAD",
            "tier": "0DTE",
            "direction": "bearish",
            "confidence": 0.65,
        },
    )

    assert len(executions) == 1
    sql, params = executions[0]
    norm = " ".join(sql.split())
    assert "INSERT INTO signal_action_cards" in norm
    assert "SELECT" in norm and "WHERE NOT EXISTS" in norm
    assert "WHERE underlying = %s AND pattern = %s AND timestamp = %s" in norm
    assert "VALUES (" not in norm  # the old unguarded form is gone
    # Param/placeholder parity (psycopg2 sequential %s): 8 insert + 3 guard.
    assert norm.count("%s") == len(params) == 11
    # Trailing three are the dedup keys.
    assert params[-3:] == ("SPY", "call_wall_fade", ts)


def test_action_card_async_insert_is_idempotent_guarded():
    from src.api.database import DatabaseManager

    captured: list = []

    class _Conn:
        async def execute(self, sql, *args):
            captured.append((sql, args))

    @contextlib.asynccontextmanager
    async def _acq(self):
        yield _Conn()

    db = DatabaseManager()
    db._acquire_connection = _acq.__get__(db, DatabaseManager)

    ts = datetime(2026, 5, 15, 18, 30, tzinfo=timezone.utc)
    asyncio.run(
        db.insert_action_card(
            {
                "underlying": "SPY",
                "timestamp": ts,
                "pattern": "gamma_flip_bounce",
                "action": "BUY_CALL",
                "tier": "0DTE",
                "direction": "bullish",
                "confidence": 0.7,
            }
        )
    )

    assert len(captured) == 1
    sql, args = captured[0]
    norm = " ".join(sql.split())
    assert "INSERT INTO signal_action_cards" in norm
    assert "WHERE NOT EXISTS" in norm
    # asyncpg positional reuse: dedup keys reuse $1 (underlying), $3
    # (pattern), $2 (timestamp) — so the arg count is UNCHANGED at 8.
    assert "WHERE underlying = $1 AND pattern = $3 AND timestamp = $2" in norm
    assert "VALUES ($1," not in norm
    assert len(args) == 8
    assert args[0] == "SPY" and args[2] == "gamma_flip_bounce" and args[1] == ts


# ===========================================================================
# #5a — signal_events idempotent insert (real _persist_advanced_signals path)
# ===========================================================================
class _CtxCursor:
    def __init__(self, sink):
        self._sink = sink

    def execute(self, sql, params=None):
        self._sink.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _CtxConn:
    def __init__(self, sink):
        self._sink = sink

    def cursor(self):
        return _CtxCursor(self._sink)

    def commit(self):
        pass


def _fake_result(name, score, signal, triggered):
    class _R:
        pass

    r = _R()
    r.name = name
    r.score = score
    r.context = {"signal": signal, "triggered": triggered}
    return r


def test_signal_events_insert_is_idempotent_guarded(monkeypatch):
    from src.signals.unified_signal_engine import UnifiedSignalEngine
    from src.signals.components.base import MarketContext

    sink: list = []

    @contextlib.contextmanager
    def _conn():
        yield _CtxConn(sink)

    monkeypatch.setattr("src.signals.unified_signal_engine.db_connection", _conn)

    eng = UnifiedSignalEngine.__new__(UnifiedSignalEngine)
    eng._advanced_state = {}
    eng._HYSTERESIS_CYCLES = 1
    eng._SCORE_DEDUPE_EPSILON = 0.01
    eng._heartbeat_due = lambda *_a, **_k: False
    eng.advanced_signal_engine = type(
        "Adv",
        (),
        {"evaluate": lambda self, ctx: [_fake_result("eod_pressure", 0.9, "bullish", True)]},
    )()

    ctx = MarketContext(
        timestamp=datetime(2026, 5, 15, 18, 30, tzinfo=timezone.utc),
        underlying="SPY",
        close=678.4,
        net_gex=7.1e9,
        gamma_flip=676.5,
        put_call_ratio=0.36,
        max_pain=675.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[],
        iv_rank=None,
    )

    eng._persist_advanced_signals(ctx)

    ev = [(s, p) for (s, p) in sink if isinstance(s, str) and "INSERT INTO signal_events" in s]
    assert len(ev) == 1
    sql, params = ev[0]
    norm = " ".join(sql.split())
    assert "SELECT %s, %s, %s, %s, %s, %s::jsonb, %s" in norm
    assert "WHERE NOT EXISTS (" in norm
    assert "WHERE underlying = %s AND signal_name = %s AND timestamp = %s" in norm
    assert "VALUES (" not in norm  # old unguarded form gone
    # Parity: 7 select + 3 guard placeholders == 10 params.
    assert norm.count("%s") == len(params) == 10
    # Trailing three params are the dedup keys (underlying, name, ts).
    assert params[-3:] == (
        "SPY",
        "eod_pressure",
        datetime(2026, 5, 15, 18, 30, tzinfo=timezone.utc),
    )
