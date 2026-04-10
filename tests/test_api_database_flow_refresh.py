import asyncio
import os
from datetime import datetime, timezone

from src.api.database import DatabaseManager


class _FakeFlowRefreshConn:
    def __init__(self):
        self.execute_queries = []

    async def fetchval(self, query, *_args):
        if "FROM option_chains" in query:
            return datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
        if "FROM underlying_quotes" in query:
            return 5200.0
        if "FROM flow_contract_facts" in query:
            return datetime(2026, 4, 10, 13, 55, tzinfo=timezone.utc)
        return None

    async def execute(self, query, *_args):
        self.execute_queries.append(query)
        return "INSERT 0 1"


def test_refresh_flow_cache_seeds_prior_rows_for_lag():
    db = DatabaseManager()
    conn = _FakeFlowRefreshConn()
    os.environ["FLOW_CANONICAL_ONLY"] = "true"

    asyncio.run(db._do_refresh_flow_cache(conn, "SPX"))

    assert conn.execute_queries
    canonical_query = next((q for q in conn.execute_queries if "INSERT INTO flow_contract_facts" in q), "")
    assert canonical_query
    assert "WITH window_rows AS (" in canonical_query
    assert "seed_rows AS (" in canonical_query
    assert "active_symbols AS (" in canonical_query
    assert "JOIN LATERAL (" in canonical_query
    assert "source_rows AS (" in canonical_query
    assert "FROM source_rows s" in canonical_query
    assert "WHERE timestamp >= $2" in canonical_query
