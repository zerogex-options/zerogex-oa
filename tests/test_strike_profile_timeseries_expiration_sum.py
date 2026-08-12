"""Regression: per-strike gamma is SUMMED across the selected expirations.

Runs the REAL ``get_strike_profile_timeseries`` SQL against a live Postgres
and pins the monotonic-aggregation invariant that a "Strike Profile" user
relies on:

    Adding an expiration to the ``expirations`` filter can never REDUCE the
    per-(rep_ts, strike) aggregate.  For two live expirations E0, E1 the
    aggregate for {E0, E1} must equal the per-strike sum E0 + E1 at every
    strike — hence it is >= the aggregate for {E0} alone.

This is the anti-regression for the class of bug where the multi-expiration
path collapses to a *single* expiration per strike (e.g. a
``DISTINCT ON (strike) ... ORDER BY expiration DESC`` "latest row per group",
an averaging, or a JOIN-then-dedup).  Such a collapse makes the chart's
per-strike max SHRINK when a second expiration is added — the exact reported
symptom (isolated 0DTE ~3.6B; add the next day and it drops to the next
day's value ALONE ~350.7M).  ``call_gamma``/``put_gamma`` are stored as
non-negative ``sum(γ × OI)`` (see ``_calculate_gex_by_strike``), so a true
SUM is monotone and this test would fail loudly the moment the aggregation
stops summing.

Value-level SQL behaviour is only meaningful against a real Postgres (the
SUM/GROUP BY runs server-side; the rest of the strike-profile suite
deliberately mocks the connection), so this is ``integration``-marked and
skips unless ``STRIKE_PROFILE_SUM_TEST_DSN`` points at a database whose
schema matches ``setup/database/schema.sql``.  A throwaway symbol is seeded
and torn down, so it never touches real market data.

Run it against a scratch database, e.g.::

    STRIKE_PROFILE_SUM_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_strike_profile_timeseries_expiration_sum.py -q
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone

import pytest

pytestmark = pytest.mark.integration

_DSN = os.getenv("STRIKE_PROFILE_SUM_TEST_DSN")

pytest_skip_reason = (
    "STRIKE_PROFILE_SUM_TEST_DSN not set — strike-profile aggregation "
    "integration test skipped (needs a scratch Postgres with the project schema)."
)

# Throwaway symbol so the seed/teardown never collides with real data.
_SYMBOL = "ZZSUMT"
_CLOSE = 640.0
_SCALE = 100 * _CLOSE * _CLOSE * 0.01  # γ -> dollar-GEX per 1% move

_E0 = date(2026, 8, 12)  # "today's" 0DTE
_E1 = date(2026, 8, 13)  # the next listed expiration

# Two 15-minute buckets, each anchored on its own gex_summary rep timestamp.
_REP = [
    datetime(2026, 8, 12, 19, 29, 0, tzinfo=timezone.utc),  # bucket 19:15
    datetime(2026, 8, 12, 19, 44, 0, tzinfo=timezone.utc),  # bucket 19:30 (latest)
]
_STRIKES = [630.0, 635.0, 640.0, 645.0, 650.0]

# Per-(expiration, strike) raw call/put gamma.  E0 carries a deliberately
# huge ATM (640) value — the near-expiry 0DTE gamma concentration — so a
# collapse-to-latest-expiration bug would make the {E0,E1} max SHRINK to
# E1's ATM value instead of growing.
_GAMMA = {
    _E0: {630.0: 300.0, 635.0: 800.0, 640.0: 8789.0625, 645.0: 1200.0, 650.0: 1500.0},
    _E1: {630.0: 400.0, 635.0: 600.0, 640.0: 856.0, 645.0: 700.0, 650.0: 500.0},
}


async def _seed(conn) -> None:
    # Idempotent: a prior run's rows cascade-delete with the symbol.
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
    await conn.execute(
        "INSERT INTO symbols(symbol, name, asset_type, is_active) "
        "VALUES ($1, 'strike-profile sum test', 'ETF', true)",
        _SYMBOL,
    )
    for ts in _REP:
        await conn.execute(
            "INSERT INTO gex_summary(underlying, timestamp, gamma_flip_point) "
            "VALUES ($1, $2, $3)",
            _SYMBOL, ts, 638.0,
        )
        await conn.execute(
            "INSERT INTO underlying_quotes(symbol, timestamp, open, high, low, close) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            _SYMBOL, ts, _CLOSE, _CLOSE + 1, _CLOSE - 1, _CLOSE,
        )
        for exp in (_E0, _E1):
            for strike in _STRIKES:
                cg = _GAMMA[exp][strike]
                await conn.execute(
                    "INSERT INTO gex_by_strike(underlying, timestamp, strike, expiration, "
                    "call_gamma, put_gamma, call_oi, put_oi) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    _SYMBOL, ts, strike, exp, cg, cg * 0.5, 5000, 4000,
                )


async def _teardown(conn) -> None:
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)


def _latest_bucket_by_strike(result):
    """Map strike -> (|call_gamma|, |put_gamma|) for the most-recent bucket."""
    assert result, "endpoint returned no buckets"
    bucket = result[-1]  # response is ASCENDING by bucket time; latest is last
    return {
        float(s["strike"]): (abs(float(s["call_gamma"] or 0.0)), abs(float(s["put_gamma"] or 0.0)))
        for s in bucket["strikes"]
    }


@pytest.mark.skipif(_DSN is None, reason=pytest_skip_reason)
def test_multi_expiration_is_per_strike_sum_not_a_collapse():
    import asyncpg

    from src.api.database import DatabaseManager

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await _seed(conn)

            db = DatabaseManager()

            @asynccontextmanager
            async def _acquire():
                yield conn

            db._acquire_connection = _acquire  # type: ignore[method-assign]

            # Distinct expiration filters -> distinct cache keys, so these
            # three calls each hit the DB rather than a stale cache entry.
            e0 = _latest_bucket_by_strike(
                await db.get_strike_profile_timeseries(_SYMBOL, "15min", 40, [_E0])
            )
            e1 = _latest_bucket_by_strike(
                await db.get_strike_profile_timeseries(_SYMBOL, "15min", 40, [_E1])
            )
            both = _latest_bucket_by_strike(
                await db.get_strike_profile_timeseries(_SYMBOL, "15min", 40, [_E0, _E1])
            )
            return e0, e1, both
        finally:
            await _teardown(conn)
            await conn.close()

    e0, e1, both = asyncio.run(_run())

    strikes = set(e0) | set(e1) | set(both)
    assert strikes, "no strikes returned for the seeded buckets"

    for k in strikes:
        c0, p0 = e0.get(k, (0.0, 0.0))
        c1, p1 = e1.get(k, (0.0, 0.0))
        cb, pb = both.get(k, (0.0, 0.0))
        # Exact per-strike sum: {E0,E1} == E0 + E1 (dollar-GEX scales linearly
        # in the summed gamma at a shared bucket close).
        assert cb == pytest.approx(c0 + c1, rel=1e-9, abs=1.0), f"call_gamma@{k}: {cb} != {c0}+{c1}"
        assert pb == pytest.approx(p0 + p1, rel=1e-9, abs=1.0), f"put_gamma@{k}: {pb} != {p0}+{p1}"
        # Monotonic: adding E1 never reduces the per-strike aggregate.
        assert cb + 1e-6 >= c0, f"call_gamma@{k} shrank when adding an expiration"
        assert pb + 1e-6 >= p0, f"put_gamma@{k} shrank when adding an expiration"

    # The ATM strike carries E0's near-expiry concentration.  A collapse to
    # the latest expiration would make {E0,E1}@640 equal E1-alone (the
    # reported ~350.7M shrink); the true SUM is strictly larger.
    atm = 640.0
    assert both[atm][0] > e1[atm][0], "multi-expiration ATM collapsed to the latest expiration alone"
    assert both[atm][0] == pytest.approx(e0[atm][0] + e1[atm][0], rel=1e-9, abs=1.0)

    # And the per-strike MAX (what the chart's value axis reads) grows rather
    # than shrinks when the second expiration is added.
    assert max(c for c, _ in both.values()) >= max(c for c, _ in e0.values())
