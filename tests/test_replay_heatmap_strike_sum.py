"""Regression: replay, heatmap and diff SUM each strike across its expirations.

``gex_by_strike`` is keyed per (strike, expiration).  The live ladder and the
Strike Profile (``get_strike_profile_timeseries``) sum a strike's rows; three
reads used to collapse them another way:

* ``get_gex_frames_for_session`` (``/api/replay/range``) took AVG,
* ``get_gex_heatmap`` (``/api/gex/heatmap``) took AVG,
* ``get_replay_diff`` (``/api/replay/diff``) kept one arbitrary expiration's
  row per strike, and its limit counted rows, not strikes.

AVG divides each strike by its own expiration count, so it bent the shape of
the profile as well as its scale: on ES 2026-11-06 13:20 ET the replay showed
exactly the sum / 12 at the five strikes that had 12 expirations, and less
than that at strikes with fewer.  The seed below gives the at-the-money
strike three expirations, its neighbors two and the wings one, so any
collapse other than a sum fails here, and checks the replay against the live
ladder at the same minute.

Value-level SQL behavior is only meaningful against a real Postgres, so this
is ``integration``-marked like ``test_strike_profile_timeseries_expiration_sum``
and skips unless ``REPLAY_SUM_TEST_DSN`` (or ``STRIKE_PROFILE_SUM_TEST_DSN``)
points at a scratch database built from ``setup/database/schema.sql``.  A
throwaway symbol is seeded and torn down, so it never touches market data::

    REPLAY_SUM_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_replay_heatmap_strike_sum.py -q
"""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone

import pytest

from src.analytics.walls import compute_call_put_walls

pytestmark = pytest.mark.integration

_DSN = os.getenv("REPLAY_SUM_TEST_DSN") or os.getenv("STRIKE_PROFILE_SUM_TEST_DSN")

_SYMBOL = "ZZRSUM"  # throwaway; cascade-deleted with its symbols row
_SESSION = date(2026, 8, 12)  # a Wednesday
_E0 = date(2026, 8, 12)  # the session's 0DTE
_E1 = date(2026, 8, 14)  # Friday
_E2 = date(2026, 8, 21)  # next Friday
_T0 = datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc)  # 10:00 ET
_T1 = datetime(2026, 8, 12, 14, 1, tzinfo=timezone.utc)  # 10:01 ET
_SPOT = 640.0
_SCALE = 100 * _SPOT * _SPOT * 0.01  # raw gamma -> dollars per 1% move

# (expiration, strike) -> (call_gamma, put_gamma) at _T0.  Three expirations
# at the money, two beside it, one on each wing.  Under the {E0, E1} scope the
# call and put walls ranked on per-strike SUMS (640 / 640) differ from the ones
# ranked on per-expiration AVERAGES (650 / 630), so the wall fold is pinned too.
_T0_GAMMA = {
    (_E1, 630.0): (100.0, 800.0),
    (_E0, 635.0): (150.0, 350.0),
    (_E1, 635.0): (120.0, 350.0),
    (_E0, 640.0): (500.0, 450.0),
    (_E1, 640.0): (400.0, 350.0),
    (_E2, 640.0): (300.0, 250.0),
    (_E0, 645.0): (300.0, 90.0),
    (_E1, 645.0): (300.0, 60.0),
    (_E1, 650.0): (700.0, 40.0),
}
_STRIKES = sorted({k for _, k in _T0_GAMMA})


def _gamma_at(ts: datetime) -> dict:
    """The book at ``ts``: at _T1 the at-the-money strike has grown by half."""
    if ts == _T0:
        return dict(_T0_GAMMA)
    return {
        key: (c * 1.5, p * 1.5) if key[1] == 640.0 else (c, p) for key, (c, p) in _T0_GAMMA.items()
    }


def _expected(ts: datetime, scope=None) -> dict:
    """strike -> (net, call, put) dollar totals, summed over the scope."""
    out: dict = {}
    for (exp, strike), (c, p) in _gamma_at(ts).items():
        if scope is not None and exp not in scope:
            continue
        net, call, put = out.get(strike, (0.0, 0.0, 0.0))
        out[strike] = (net + (c - p) * _SCALE, call + c * _SCALE, put - p * _SCALE)
    return out


def _raw_inputs(ts: datetime, scope) -> list:
    """Per-strike summed raw gamma, the input the wall helpers rank on."""
    agg: dict = {}
    for (exp, strike), (c, p) in _gamma_at(ts).items():
        if exp in scope:
            cg, pg = agg.get(strike, (0.0, 0.0))
            agg[strike] = (cg + c, pg + p)
    return [{"strike": k, "call_gamma": c, "put_gamma": p} for k, (c, p) in sorted(agg.items())]


async def _seed(conn) -> None:
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
    await conn.execute(
        "INSERT INTO symbols(symbol, name, asset_type, is_active) "
        "VALUES ($1, 'replay sum test', 'ETF', true)",
        _SYMBOL,
    )
    for ts in (_T0, _T1):
        await conn.execute(
            "INSERT INTO gex_summary(underlying, timestamp, gamma_flip_point) VALUES ($1, $2, $3)",
            _SYMBOL,
            ts,
            641.0,
        )
        await conn.execute(
            "INSERT INTO underlying_quotes(symbol, timestamp, open, high, low, close) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            _SYMBOL,
            ts,
            _SPOT,
            _SPOT + 1,
            _SPOT - 1,
            _SPOT,
        )
        for (exp, strike), (c, p) in _gamma_at(ts).items():
            # net_gex is stored dollar-scaled at the analytics spot, as the
            # engine writes it.
            await conn.execute(
                "INSERT INTO gex_by_strike(underlying, timestamp, strike, expiration, "
                "call_gamma, put_gamma, net_gex, call_oi, put_oi) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                _SYMBOL,
                ts,
                strike,
                exp,
                c,
                p,
                (c - p) * _SCALE,
                1000,
                1000,
            )


async def _teardown(conn) -> None:
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)


def _run(body):
    """Seed, run ``body(db)`` on the scratch connection, always tear down."""
    import asyncpg

    # Fresh API modules, as tests/test_replay_endpoint.py's _build_app does:
    # endpoint tests replace DatabaseManager methods with mocks on the class,
    # and those would answer here instead of the real SQL.
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api.database import DatabaseManager

    async def _go():
        conn = await asyncpg.connect(_DSN)
        try:
            await _seed(conn)
            db = DatabaseManager()

            @asynccontextmanager
            async def _acquire():
                yield conn

            db._acquire_connection = _acquire  # type: ignore[method-assign]
            return await body(db)
        finally:
            await _teardown(conn)
            await conn.close()

    return asyncio.run(_go())


def _frame_at(frames: list, ts: datetime) -> dict:
    hits = [f for f in frames if f["timestamp"] == ts]
    assert len(hits) == 1, f"expected one frame at {ts}, got {len(hits)}"
    return hits[0]


def _ladder_bucket_at(buckets: list, ts: datetime) -> dict:
    hits = [b for b in buckets if b["timestamp"] == ts]
    assert len(hits) == 1, f"expected one ladder bucket at {ts}, got {len(hits)}"
    return hits[0]


skip_without_db = pytest.mark.skipif(
    _DSN is None,
    reason="REPLAY_SUM_TEST_DSN not set: needs a scratch Postgres with the project schema.",
)


@skip_without_db
def test_replay_range_sums_each_strike_and_matches_the_live_ladder():
    async def body(db):
        frames = await db.get_gex_frames_for_session(_SYMBOL, _SESSION)
        ladder = await db.get_strike_profile_timeseries(_SYMBOL, "1min", 5)
        return frames, ladder

    frames, ladder = _run(body)
    frame = _frame_at(frames, _T0)
    got = {float(s["strike"]): s for s in frame["strikes"]}
    want = _expected(_T0)
    assert sorted(got) == _STRIKES

    for k, (net, call, put) in want.items():
        assert float(got[k]["net_gex"]) == pytest.approx(net, rel=1e-9), k
        assert float(got[k]["call_gex"]) == pytest.approx(call, rel=1e-9), k
        assert float(got[k]["put_gex"]) == pytest.approx(put, rel=1e-9), k

    # Three expirations at 640: the total, not a third of it.
    assert float(got[640.0]["net_gex"]) == pytest.approx(
        sum((c - p) * _SCALE for (e, k), (c, p) in _T0_GAMMA.items() if k == 640.0)
    )

    # Same minute, same strikes, same figure as the live ladder.
    live = {float(s["strike"]): s for s in _ladder_bucket_at(ladder, _T0)["strikes"]}
    assert sorted(live) == _STRIKES
    for k in _STRIKES:
        assert float(got[k]["net_gex"]) == pytest.approx(float(live[k]["net_gamma"]), rel=1e-9)
        assert float(got[k]["call_gex"]) == pytest.approx(float(live[k]["call_gamma"]), rel=1e-9)
        assert float(got[k]["put_gex"]) == pytest.approx(float(live[k]["put_gamma"]), rel=1e-9)


@skip_without_db
def test_replay_range_scoped_to_two_expirations_sums_them_and_walls_match_the_ladder():
    scope = [_E0, _E1]

    async def body(db):
        frames = await db.get_gex_frames_for_session(_SYMBOL, _SESSION, expirations=scope)
        ladder = await db.get_strike_profile_timeseries(_SYMBOL, "1min", 5, scope)
        return frames, ladder

    frames, ladder = _run(body)
    frame = _frame_at(frames, _T0)
    got = {float(s["strike"]): s for s in frame["strikes"]}
    for k, (net, call, put) in _expected(_T0, scope).items():
        assert float(got[k]["net_gex"]) == pytest.approx(net, rel=1e-9), k
        assert float(got[k]["call_gex"]) == pytest.approx(call, rel=1e-9), k
        assert float(got[k]["put_gex"]) == pytest.approx(put, rel=1e-9), k

    # The walls are re-ranked from the scoped ladder.  Ranked on sums they are
    # 640 / 640; ranked on per-expiration averages they would be 650 / 630.
    assert compute_call_put_walls(_raw_inputs(_T0, scope), _SPOT) == (640.0, 640.0)
    assert (frame["call_wall"], frame["put_wall"]) == (640.0, 640.0)
    bucket = _ladder_bucket_at(ladder, _T0)
    assert (float(bucket["call_wall"]), float(bucket["put_wall"])) == (640.0, 640.0)
    # And the scoped flip is computed from the same sums on both surfaces.
    assert frame["gamma_flip"] == bucket["gamma_flip"]


@skip_without_db
def test_replay_range_one_date_scope_is_the_single_row():
    """A one-date scope has one row per strike (the primary key is
    (underlying, timestamp, strike, expiration)), so it reads the same whether
    the ladder sums or averages.  Pinned so that stays true."""

    async def body(db):
        return await db.get_gex_frames_for_session(_SYMBOL, _SESSION, expirations=[_E0])

    frame = _frame_at(_run(body), _T0)
    got = {float(s["strike"]): float(s["net_gex"]) for s in frame["strikes"]}
    want = {k: (c - p) * _SCALE for (e, k), (c, p) in _T0_GAMMA.items() if e == _E0}
    assert got == pytest.approx(want, rel=1e-9)


@skip_without_db
def test_heatmap_cells_sum_each_strike():
    async def body(db):
        return await db.get_gex_heatmap(_SYMBOL, "1min", 5)

    buckets = {b["timestamp"]: b for b in _run(body)}
    assert set(buckets) == {_T0, _T1}
    for ts in (_T0, _T1):
        got = {float(c["strike"]): float(c["net_gex"]) for c in buckets[ts]["heatmap"]}
        want = {k: net for k, (net, _, _) in _expected(ts).items()}
        assert got == pytest.approx(want, rel=1e-9), ts


@skip_without_db
def test_strike_totals_limit_counts_whole_strikes():
    """The diff's read sums before it limits: two strikes, each complete."""

    async def body(db):
        return await db.get_gex_strike_totals_at_ts(_SYMBOL, _T0, strike_limit=2)

    rows = _run(body)
    # 640 at distance 0, then 635 and 645 tie at 5: the lower strike goes first.
    assert [float(r["strike"]) for r in rows] == [640.0, 635.0]
    want = _expected(_T0)
    for r in rows:
        k = float(r["strike"])
        assert int(r["expirations"]) == sum(1 for _, s in _T0_GAMMA if s == k)
        assert float(r["net_gex"]) == pytest.approx(want[k][0], rel=1e-9)
        assert float(r["call_gex"]) == pytest.approx(want[k][1], rel=1e-9)
        assert float(r["put_gex"]) == pytest.approx(want[k][2], rel=1e-9)


@skip_without_db
def test_replay_diff_subtracts_strike_totals():
    async def body(db):
        # Imported here, after _run has loaded fresh API modules.
        from src.api.routers.replay import get_replay_diff

        return await get_replay_diff(
            symbol=_SYMBOL,
            ts_a=_T0.isoformat(),
            ts_b=_T1.isoformat(),
            strike_limit=10,
            db=db,
        )

    out = _run(body)
    rows = {r["strike"]: r for r in out["deltas"]}
    assert sorted(rows) == _STRIKES
    before, after = _expected(_T0), _expected(_T1)
    for k in _STRIKES:
        assert rows[k]["net_gex_a"] == pytest.approx(before[k][0], rel=1e-9)
        assert rows[k]["net_gex_b"] == pytest.approx(after[k][0], rel=1e-9)
        assert rows[k]["delta"] == pytest.approx(after[k][0] - before[k][0], rel=1e-9, abs=1e-6)
    # Only the at-the-money book changed, and by half of its three-expiration total.
    assert rows[640.0]["delta"] == pytest.approx(0.5 * before[640.0][0], rel=1e-9)
    assert all(rows[k]["delta"] == pytest.approx(0.0, abs=1e-6) for k in _STRIKES if k != 640.0)
