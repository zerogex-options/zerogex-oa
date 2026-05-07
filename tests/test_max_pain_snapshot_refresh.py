import asyncio

from src.api.database import DatabaseManager


class _FakeMaxPainConn:
    def __init__(self):
        self.execute_calls = []

    async def execute(self, query, *args):
        self.execute_calls.append((query, args))
        return "INSERT 0 1"


def test_refresh_max_pain_snapshot_uses_latest_oi_per_contract():
    """Off-hours fix: snapshot must source OI from the latest row per
    contract within a bounded window, not from a single minute bucket."""
    db = DatabaseManager()
    conn = _FakeMaxPainConn()

    asyncio.run(db._refresh_max_pain_snapshot(conn, "SPY", 200))

    assert conn.execute_calls
    snapshot_query = next(
        (q for (q, _args) in conn.execute_calls if "INSERT INTO max_pain_oi_snapshot " in q),
        "",
    )
    assert snapshot_query, "snapshot upsert query was not executed"

    # Latest-per-contract pattern instead of `oc.timestamp = r.max_ts`.
    assert "DISTINCT ON (oc.option_symbol)" in snapshot_query
    assert "oc.timestamp = r.max_ts" not in snapshot_query

    # Bounded backward scan so the query still uses the per-symbol index.
    assert "INTERVAL '7 days'" in snapshot_query
    assert "oc.timestamp >= r.max_ts - INTERVAL '7 days'" in snapshot_query
    assert "oc.timestamp <= r.max_ts" in snapshot_query

    # Already-expired contracts must not leak into the snapshot now that we
    # look back across multiple sessions.
    assert "oc.expiration >= (r.max_ts AT TIME ZONE 'America/New_York')::date" in snapshot_query

    # Candidate settlements should be ranked by proximity to spot, not by the
    # bottom-N strikes (otherwise a fully-populated chain would truncate to
    # deep OTM puts under the existing strike_limit).
    assert "ABS(s.strike - u.underlying_price)" in snapshot_query
