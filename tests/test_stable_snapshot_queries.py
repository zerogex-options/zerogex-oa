"""Regression tests for the stable-snapshot CTE used by /api/market/open-interest
and /api/gex/vol_surface.

Context: option_chains is UPSERTed in 60-second buckets, so the current minute's
bucket is partially populated at any point in time. Using `MAX(timestamp)` in a
read query can return a partial snapshot — that's the sparse-data bug this CTE
fixes. These tests pin the query shape so a well-meaning refactor can't silently
revert to the naive `MAX(timestamp)`.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from src.api import database as database_module
from src.api.database import DatabaseManager


class _RecordingConn:
    """Captures queries and returns canned results keyed by query-substring match."""

    def __init__(self, fetchrow_row=None, fetch_rows=None):
        self._fetchrow_row = fetchrow_row
        self._fetch_rows = fetch_rows or []
        self.queries = []

    async def fetchrow(self, query, *_args):
        self.queries.append(query)
        return self._fetchrow_row

    async def fetch(self, query, *_args):
        self.queries.append(query)
        return list(self._fetch_rows)


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


def test_stable_snapshot_cte_is_exported_and_well_formed():
    cte = database_module._STABLE_SNAPSHOT_CTE
    # Every consumer of the CTE joins against `latest_ts.ts` — any rename of
    # these would silently break both endpoints, so pin them here.
    assert "recent_ts AS" in cte
    assert "snapshot_stats AS" in cte
    assert "latest_ts AS" in cte
    assert "MAX(oc.updated_at)" in cte
    # The whole point is to *avoid* a bare MAX(timestamp) read.
    assert "MAX(timestamp)" not in cte


def test_get_open_interest_uses_stable_snapshot_cte():
    db = DatabaseManager()
    ts = datetime(2026, 4, 24, 14, 30, tzinfo=timezone.utc)
    row = {
        "timestamp": ts,
        "underlying": "SPY",
        "strike": 500.0,
        "expiration": ts.date(),
        "option_type": "C",
        "open_interest": 100,
        "exposure": 0,
        "updated_at": ts,
    }
    conn = _RecordingConn(
        fetchrow_row={"spot_price": 500.0},
        fetch_rows=[row],
    )
    _install_conn(db, conn)

    result = asyncio.run(db.get_open_interest("SPY"))

    assert result is not None
    assert result["underlying"] == "SPY"
    assert len(result["contracts"]) == 1
    # The second query (fetch) should be the open-interest query with the CTE.
    oi_query = conn.queries[1]
    assert "recent_ts AS" in oi_query
    assert "snapshot_stats AS" in oi_query
    assert "latest_ts AS" in oi_query
    # Must not fall back to the sparse-prone `MAX(timestamp)` pattern.
    assert "MAX(timestamp)" not in oi_query
    # Weekend/after-hours fallback: exposure must be anchored on the most
    # recent snapshot whose Greeks are populated, not the terminal bucket
    # whose gamma is NULL (which would zero every contract's exposure).
    assert "exposure_ts AS" in oi_query
    assert "oc.gamma IS NOT NULL" in oi_query
    assert "JOIN exposure_ts et ON oc.timestamp = et.ts" in oi_query


def test_get_vol_surface_data_uses_stable_snapshot_cte():
    db = DatabaseManager()
    ts = datetime(2026, 4, 24, 14, 30, tzinfo=timezone.utc)
    row = {
        "strike": 500.0,
        "expiration": ts.date(),
        "option_type": "C",
        "implied_volatility": 0.2,
        "delta": 0.5,
        "open_interest": 100,
    }
    conn = _RecordingConn(
        fetchrow_row={"close": 500.0, "timestamp": ts},
        fetch_rows=[row],
    )
    _install_conn(db, conn)

    result = asyncio.run(db.get_vol_surface_data("SPY", dte_max=60, strike_count=30))

    assert result is not None
    assert result["spot_price"] == 500.0
    assert len(result["rows"]) == 1
    # The chain query is the second call; the spot query is the first.
    chain_query = conn.queries[1]
    assert "recent_ts AS" in chain_query
    assert "snapshot_stats AS" in chain_query
    assert "latest_ts AS" in chain_query
    # Older naive pattern picked the bare topmost timestamp; regression-guard it.
    assert "ORDER BY timestamp DESC\n                LIMIT 1" not in chain_query
    # Weekend/after-hours fallback: the surface must be anchored on the most
    # recent snapshot whose IV is populated, not the terminal bucket whose
    # implied_volatility is all NULL ("API returned strikes, but all IV
    # values are null for the selected tenors").
    assert "iv_ts AS" in chain_query
    assert "oc.implied_volatility IS NOT NULL" in chain_query
    assert "timestamp = iv_ts.ts" in chain_query


def test_stable_snapshot_quiescence_env_override(monkeypatch):
    """The quiescence threshold is tunable via env var; confirm it's plumbed."""
    # Re-import to pick up a new env value; the constant is evaluated at import.
    import importlib

    # Other test modules pop `src.api.*` from sys.modules during their fixtures,
    # so importlib.reload(database_module) would fail if we relied on the
    # module-level alias. Resolve it freshly here so it's in sys.modules.
    import src.api.database as fresh_database_module

    monkeypatch.setenv("STABLE_SNAPSHOT_QUIESCENCE_SECONDS", "42")
    reloaded = importlib.reload(fresh_database_module)
    try:
        assert reloaded._STABLE_SNAPSHOT_QUIESCENCE_SECONDS == 42.0
        assert "make_interval(secs => 42" in reloaded._STABLE_SNAPSHOT_CTE
    finally:
        # Restore default so later tests see the stable value.
        monkeypatch.delenv("STABLE_SNAPSHOT_QUIESCENCE_SECONDS", raising=False)
        importlib.reload(fresh_database_module)
