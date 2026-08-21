"""Parity: a chunked window assembly equals the single-query one, exactly.

``STRIKE_PROFILE_TIMESERIES_CHUNK_UNITS`` splits the rewind-chart read into
several bucket-aligned queries so that no single query has to fit inside the
15 s guard.  That is only a safe thing to do if the assembled window is
*identical* to what one query would have produced — this is the endpoint
behind a live trading chart, and a chunk seam that drops a bucket, duplicates
one, or recomputes a bucket's call/put walls from a partial strike set would
be wrong rather than merely slow.

Chunk edges are cut on real bucket timestamps from
``_strike_profile_bucket_list`` precisely so a bucket is always read whole.
These tests pin that end to end against a real Postgres, for chunk sizes that
divide the window evenly, leave a remainder, and leave a remainder of one —
and for the cash-index path, where the session filter shifts every SQL
parameter index the chunk predicate has to bind after.

Value-level SQL behaviour is only meaningful against a real server, so this is
``integration``-marked and skips unless a DSN is exported::

    STRIKE_PROFILE_CHUNK_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_strike_profile_timeseries_chunked_parity.py -q -m integration
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.integration

_DSN = os.getenv("STRIKE_PROFILE_CHUNK_TEST_DSN") or os.getenv("STRIKE_PROFILE_SUM_TEST_DSN")

_SKIP = (
    "STRIKE_PROFILE_CHUNK_TEST_DSN not set — chunked-parity integration test "
    "skipped (needs a scratch Postgres with the project schema)."
)

# Throwaway symbol: the teardown deletes it, so it must never be a real one.
_SYMBOL = "ZZCHNK"
_CLOSE = 640.0
_E0 = date(2026, 8, 12)
_E1 = date(2026, 8, 13)
_STRIKES = [630.0, 635.0, 640.0, 645.0, 650.0]

# 12 buckets of 5 min, all inside 2026-08-12's cash session (Wednesday;
# 17:02Z = 13:02 ET) so the cash-index variant's session filter keeps them.
_N_BUCKETS = 12
_REP = [
    datetime(2026, 8, 12, 17, 2, tzinfo=timezone.utc) + timedelta(minutes=5 * i)
    for i in range(_N_BUCKETS)
]


async def _seed(conn) -> None:
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
    await conn.execute(
        "INSERT INTO symbols(symbol, name, asset_type, is_active) "
        "VALUES ($1, 'strike-profile chunk parity test', 'ETF', true)",
        _SYMBOL,
    )
    for i, ts in enumerate(_REP):
        await conn.execute(
            "INSERT INTO gex_summary(underlying, timestamp, gamma_flip_point) "
            "VALUES ($1, $2, $3)",
            _SYMBOL,
            ts,
            638.0 + i,
        )
        await conn.execute(
            "INSERT INTO underlying_quotes(symbol, timestamp, open, high, low, close) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            _SYMBOL,
            ts,
            _CLOSE + i,
            _CLOSE + i + 1,
            _CLOSE + i - 1,
            _CLOSE + i,
        )
        for exp in (_E0, _E1):
            for j, strike in enumerate(_STRIKES):
                # Vary per bucket AND per strike so a seam that mixed buckets
                # up would change values, not just ordering.
                cg = 100.0 * (i + 1) * (j + 1)
                await conn.execute(
                    "INSERT INTO gex_by_strike(underlying, timestamp, strike, expiration, "
                    "call_gamma, put_gamma, call_oi, put_oi) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    _SYMBOL,
                    ts,
                    strike,
                    exp,
                    cg,
                    cg * 0.5,
                    5000,
                    4000,
                )


async def _teardown(conn) -> None:
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)


async def _fetch(conn, chunk_units, window_units, expirations=None):
    """Run the endpoint against ``conn`` with a given chunk size.

    Returns ``(result, chunk_subsets)`` — the second is the bucket_subset each
    underlying read was asked for, so a test can prove chunking really engaged
    instead of silently falling through to the single-query path.
    """
    from src.api.database import DatabaseManager

    db = DatabaseManager()
    db._strike_profile_timeseries_chunk_units = chunk_units

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    subsets = []
    inner = db._get_strike_profile_timeseries_uncached

    async def _spy(**kwargs):
        subsets.append(kwargs.get("bucket_subset"))
        return await inner(**kwargs)

    db._get_strike_profile_timeseries_uncached = _spy  # type: ignore[method-assign]
    result = await db.get_strike_profile_timeseries(
        symbol=_SYMBOL,
        timeframe="5min",
        window_units=window_units,
        expirations=expirations,
    )
    return result, subsets


@pytest.mark.skipif(_DSN is None, reason=_SKIP)
@pytest.mark.parametrize("chunk_units", [3, 5, 11])
def test_chunked_assembly_equals_single_query(chunk_units):
    import asyncpg

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await _seed(conn)
            mono, mono_subsets = await _fetch(conn, 0, _N_BUCKETS)
            assert mono_subsets == [None], "baseline must be the single-query path"
            assert len(mono) == _N_BUCKETS

            chunked, subsets = await _fetch(conn, chunk_units, _N_BUCKETS)

            # chunking actually happened, on the expected boundaries
            expected_chunks = -(-_N_BUCKETS // chunk_units)
            assert len(subsets) == expected_chunks
            assert all(s is not None for s in subsets)
            assert sum(len(s) for s in subsets) == _N_BUCKETS
            # every bucket read exactly once, none split across a seam
            flat = [b for s in subsets for b in s]
            assert len(set(flat)) == _N_BUCKETS

            assert chunked == mono
        finally:
            await _teardown(conn)
            await conn.close()

    asyncio.run(_run())


@pytest.mark.skipif(_DSN is None, reason=_SKIP)
def test_chunked_parity_holds_with_an_expiration_filter():
    """The expiration array binds at $3, before the chunk array — a filtered
    request is the case where the chunk predicate's parameter index is most
    easily got wrong."""
    import asyncpg

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await _seed(conn)
            mono, _ = await _fetch(conn, 0, _N_BUCKETS, expirations=[_E0])
            chunked, subsets = await _fetch(conn, 5, _N_BUCKETS, expirations=[_E0])
            assert len(subsets) == 3
            assert chunked == mono
            assert len(chunked) == _N_BUCKETS
        finally:
            await _teardown(conn)
            await conn.close()

    asyncio.run(_run())


@pytest.mark.skipif(_DSN is None, reason=_SKIP)
def test_chunked_parity_holds_for_a_cash_index():
    """A cash index appends the holidays array, pushing the chunk array one
    parameter further along in BOTH the full read and the bucket list.

    ``is_cash_index`` is patched rather than seeding a real index symbol: the
    teardown deletes the symbol, and this must stay safe to run against a
    database that has real SPX/NDX data in it.
    """
    import asyncpg

    from src.api import database as database_module

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await _seed(conn)
            with patch.object(database_module, "is_cash_index", lambda s: s == _SYMBOL):
                mono, _ = await _fetch(conn, 0, _N_BUCKETS)
                chunked, subsets = await _fetch(conn, 5, _N_BUCKETS)
            assert len(mono) == _N_BUCKETS, "session filter dropped the seeded buckets"
            assert len(subsets) == 3
            assert chunked == mono
        finally:
            await _teardown(conn)
            await conn.close()

    asyncio.run(_run())
