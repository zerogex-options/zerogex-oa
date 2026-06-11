import asyncio
import os
from datetime import datetime, timedelta, timezone

from src.api.database import DatabaseManager


class _FakeFlowRefreshConn:
    def __init__(self, last_fact_ts=None):
        self.execute_calls = []
        self._last_fact_ts = last_fact_ts

    async def fetchval(self, query, *_args):
        if "FROM option_chains" in query:
            return datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
        if "FROM underlying_quotes" in query:
            return 5200.0
        if "FROM flow_contract_facts" in query:
            return self._last_fact_ts
        return None

    async def execute(self, query, *args, **kwargs):
        self.execute_calls.append((query, args, kwargs))
        return "INSERT 0 1"


def test_refresh_flow_cache_seeds_prior_rows_for_lag():
    db = DatabaseManager()
    conn = _FakeFlowRefreshConn()
    os.environ["FLOW_CANONICAL_ONLY"] = "true"
    os.environ["FLOW_CANONICAL_BACKFILL_MINUTES"] = "240"

    asyncio.run(db._do_refresh_flow_cache(conn, "SPX"))

    assert conn.execute_calls
    canonical_call = next(
        (
            (q, args, kwargs)
            for (q, args, kwargs) in conn.execute_calls
            if "INSERT INTO flow_contract_facts" in q
        ),
        ("", (), {}),
    )
    canonical_query, canonical_args, _ = canonical_call
    assert canonical_query
    assert "WITH window_rows AS (" in canonical_query
    assert "seed_rows AS (" in canonical_query
    assert "active_symbols AS (" in canonical_query
    assert "JOIN LATERAL (" in canonical_query
    assert "source_rows AS (" in canonical_query
    assert "FROM source_rows s" in canonical_query
    assert "WHERE timestamp >= $2" in canonical_query
    # backfill_start should honor FLOW_CANONICAL_BACKFILL_MINUTES
    assert canonical_args[1] == datetime(2026, 4, 10, 10, 0, tzinfo=timezone.utc)


def test_refresh_flow_cache_slides_floor_back_for_late_arrivals():
    # The vendor upserts option_chains.volume monotonically as late snapshots
    # arrive within a minute bucket; the first LAG-delta we computed for that
    # minute is sometimes partial.  Refresh must reprocess the last
    # FLOW_REPROCESS_MINUTES minutes to overwrite stale partial rows -- not
    # just append rows beyond last_fact_ts.
    db = DatabaseManager()
    last_fact_ts = datetime(2026, 4, 10, 13, 55, tzinfo=timezone.utc)
    conn = _FakeFlowRefreshConn(last_fact_ts=last_fact_ts)
    os.environ["FLOW_CANONICAL_BACKFILL_MINUTES"] = "240"
    os.environ["FLOW_REPROCESS_MINUTES"] = "5"

    asyncio.run(db._do_refresh_flow_cache(conn, "SPX"))

    canonical_call = next(
        (
            (q, args, kwargs)
            for (q, args, kwargs) in conn.execute_calls
            if "INSERT INTO flow_contract_facts" in q
        ),
        ("", (), {}),
    )
    canonical_query, canonical_args, _ = canonical_call
    assert canonical_query

    # $2 (backfill_start) reaches reprocess_minutes + 1 minutes before
    # last_fact_ts so the LAG window has a seed row before the reprocess floor.
    assert canonical_args[1] == last_fact_ts - timedelta(minutes=6)
    # $5 (reprocess_floor) is reprocess_minutes before last_fact_ts; the
    # INSERT's `timestamp > $5` clause now lets ON CONFLICT DO UPDATE rewrite
    # the last 5 minutes' worth of facts when their option_chains.volume grew.
    assert canonical_args[4] == last_fact_ts - timedelta(minutes=5)


def test_refresh_flow_cache_overrides_both_server_and_client_timeouts():
    # The default pool command_timeout (30s, hardcoded) silently kills the
    # multi-hour cold-start backfill, leaving 09:30+ rows missing forever (the
    # backfill rolls back but advances the throttle, so subsequent refreshes
    # only tail-poll).  Refresh must lift BOTH ceilings:
    #   - server-side: SET LOCAL statement_timeout (per-transaction)
    #   - client-side: pass timeout= to conn.execute (per-call override)
    # SET LOCAL alone is insufficient -- asyncpg cancels the call client-side
    # at command_timeout regardless of the server-side limit.
    db = DatabaseManager()
    conn = _FakeFlowRefreshConn()
    os.environ["FLOW_REFRESH_STATEMENT_TIMEOUT_MS"] = "180000"

    asyncio.run(db._do_refresh_flow_cache(conn, "SPX"))

    # Server-side: SET LOCAL statement_timeout fires before the INSERT.
    timeout_calls = [q for q, _a, _k in conn.execute_calls if "SET LOCAL statement_timeout" in q]
    assert timeout_calls, "refresh must SET LOCAL statement_timeout before heavy upsert"
    assert "180000" in timeout_calls[0]

    set_idx = next(
        i for i, (q, _a, _k) in enumerate(conn.execute_calls) if "SET LOCAL statement_timeout" in q
    )
    insert_idx = next(
        i
        for i, (q, _a, _k) in enumerate(conn.execute_calls)
        if "INSERT INTO flow_contract_facts" in q
    )
    assert set_idx < insert_idx

    # Client-side: the heavy INSERT call passes timeout= matching the env var.
    insert_call = conn.execute_calls[insert_idx]
    _q, _args, kwargs = insert_call
    assert kwargs.get("timeout") == 180.0


def test_refresh_flow_cache_reprocess_minutes_zero_preserves_legacy_floor():
    # Operators who want the old append-only behaviour can opt out by setting
    # FLOW_REPROCESS_MINUTES=0; the floor then collapses back to last_fact_ts
    # and backfill_start backs off only the one minute needed for LAG context.
    db = DatabaseManager()
    last_fact_ts = datetime(2026, 4, 10, 13, 55, tzinfo=timezone.utc)
    conn = _FakeFlowRefreshConn(last_fact_ts=last_fact_ts)
    os.environ["FLOW_CANONICAL_BACKFILL_MINUTES"] = "240"
    os.environ["FLOW_REPROCESS_MINUTES"] = "0"

    asyncio.run(db._do_refresh_flow_cache(conn, "SPX"))

    canonical_call = next(
        (
            (q, args, kwargs)
            for (q, args, kwargs) in conn.execute_calls
            if "INSERT INTO flow_contract_facts" in q
        ),
        ("", (), {}),
    )
    _, canonical_args, _ = canonical_call
    assert canonical_args[1] == last_fact_ts - timedelta(minutes=1)
    assert canonical_args[4] == last_fact_ts
