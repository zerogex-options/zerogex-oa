"""``get_historical_gex`` must read gex_by_strike through correlated LATERALs.

The last unfenced reads of the highest-cardinality table. /api/gex/historical
takes THREE of them -- the strike aggregate and both walls -- and all three
keyed on ``base``, an optimisation-fence CTE (multiply referenced, so PG
materialises it). The [start_ts, end_ts] bound therefore sat behind the fence
where the planner could not see it, and the intended ~window_units index probes
were only an intention.

Same gap as get_strike_profile_timeseries (28a2c33), get_gex_heatmap (15cb6c3)
and the replay frames read -- but the worst-exposed of the four, for two
reasons. First it is three reads, not one. Second the rep-set ratio is at its
maximum: ``base`` is rn=1, ONE timestamp per bucket, and this endpoint accepts
``timeframe=1day&window_units=90``, where the window spans ~90 sessions while
``base`` holds 90 rows -- so a merely-redundant window bound would be capped at
"the window" and the window is ~the whole retention.

Measured at 1day/window_units=90 on a seeded 9.5M-row gex_by_strike (NDX at
production density -- 144 strikes x 6 expirations x 391 minutes) with the
nested loop forced off, i.e. the production fallback:

    before   Index Scan Backward, 8,783,425 rows   (strike_agg)
             Seq Scan,            9,459,072 rows   (call_walls)
             Seq Scan,            9,459,072 rows   (put_walls)
             27.7M rows read                       16,564 ms

    after    Index Scan,  864 rows x  26 loops     (strike_agg)
             Index Scan,  402 rows x  26 loops     (call_walls)
             Index Scan,  468 rows x  26 loops     (put_walls)
                                                    1,530 ms

Output is byte-for-byte identical: verified by running the real method against
Postgres across every timeframe x window_units combination the endpoint accepts
(1min/5min/15min/1hr/1day x 10/90 = 436 rows), once with the pre-fix SQL spliced
back in. NDX is a cash index, so the session-filter branch was the one exercised.
"""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager

from src.api.database import DatabaseManager


def _historical_sql(symbol: str = "NDX", timeframe: str = "1day") -> str:
    """The live SQL, comments stripped.

    Stripped for the reason the sibling guards strip theirs: the query's own
    comment names the join shape it forbids, so a structural assertion against
    the raw string would pass on the prose alone.
    """
    db = DatabaseManager()
    captured: list = []

    class _Conn:
        async def fetch(self, query, *args):
            captured.append(query)
            return []

    @asynccontextmanager
    async def _acquire():
        yield _Conn()

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    asyncio.run(db.get_historical_gex(symbol, None, None, 90, timeframe))
    return "\n".join(
        line.split("--", 1)[0] if "--" in line else line for line in captured[0].splitlines()
    )


def _cte(sql: str, name: str) -> str:
    """The body of one CTE, from its header to the start of the next."""
    start = sql.index(f"{name} AS MATERIALIZED (")
    rest = sql[start:]
    # Next CTE header, or the final SELECT.
    m = re.search(r"\n            [a-z_]+ AS |\n            SELECT\n", rest[10:])
    return rest[: 10 + m.start()] if m else rest


def test_all_three_gex_by_strike_reads_are_correlated_laterals():
    """The core anti-regression, for each of the three reads."""
    sql = _historical_sql()
    for name, alias in (
        ("strike_agg", "gbs"),
        ("call_walls", "gbs"),
        ("put_walls", "gbs"),
    ):
        body = _cte(sql, name)
        assert "JOIN LATERAL (" in body, f"{name}: gex_by_strike read is not a LATERAL"
        assert f"FROM gex_by_strike {alias}" in body, f"{name}: lost the gex_by_strike read"
        # Keyed on the bucket representative -- the point-lookup path.
        assert f"{alias}.timestamp = b.timestamp" in body, f"{name}: not keyed on base"
        # Window bound rides along as defence in depth.
        assert "start_ts FROM bounds" in body, f"{name}: lost the window bound"
        assert "end_ts FROM bounds" in body, f"{name}: lost the window bound"


def test_no_read_falls_back_to_a_plain_join_or_in_on_base():
    """The two pre-fix shapes, both of which left the bound behind the fence."""
    sql = _historical_sql()
    assert "IN (SELECT timestamp FROM base)" not in sql, (
        "strike_agg is back to an IN over the fence CTE: the planner cannot "
        "estimate it and periodically scans the whole table instead."
    )
    assert (
        "JOIN base b ON b.timestamp = gbs.timestamp" not in sql
    ), "the walls are back to a plain join on the fence CTE."


def test_the_three_ctes_stay_materialized():
    """Referenced once each, so PG inlines them -- and an inlined lateral is
    re-evaluated per OUTER row.

    Measured at 1day/window_units=90: 676 probes (26 buckets x 26) where 26 are
    needed. Correct either way, but it multiplies the read by the bucket count,
    and a probe count equal to the rep count is the entire point of the lateral.
    """
    sql = _historical_sql()
    for name in ("strike_agg", "call_walls", "put_walls"):
        assert f"{name} AS MATERIALIZED (" in sql, f"{name} must stay MATERIALIZED"


def test_wall_tiebreakers_survive_the_rewrite():
    """DISTINCT ON + ORDER BY became ORDER BY + LIMIT 1 -- same pick.

    The walls must keep agreeing byte-for-byte with
    /api/gex/strike-profile-timeseries and src/analytics/walls.py, and the
    tie-breaker is the half of that contract a rewrite is most likely to drop:
    call wall breaks ties to the LOWEST strike, put wall to the HIGHEST.
    """
    sql = _historical_sql()
    call = _cte(sql, "call_walls")
    put = _cte(sql, "put_walls")

    assert "gbs.strike >= (" in call or "gbs.strike >= bc.bucket_close" in call
    assert "ORDER BY SUM(COALESCE(gbs.call_gamma, 0)) DESC, gbs.strike ASC" in call
    assert "LIMIT 1" in call

    assert "gbs.strike <= (" in put or "gbs.strike <= bc.bucket_close" in put
    assert "ORDER BY SUM(COALESCE(gbs.put_gamma, 0)) DESC, gbs.strike DESC" in put
    assert "LIMIT 1" in put

    # ``WHERE call_gamma > 0`` on the grouped rows is now HAVING on the sum.
    assert "HAVING SUM(COALESCE(gbs.call_gamma, 0)) > 0" in call
    assert "HAVING SUM(COALESCE(gbs.put_gamma, 0)) > 0" in put


def test_a_bucket_with_no_qualifying_strike_still_yields_no_wall_row():
    """CROSS, not LEFT, is correct for the walls -- and that is deliberate.

    The pre-fix CTE simply had no row for such a bucket, and the outer query
    LEFT JOINs it, so the wall reads NULL. CROSS JOIN LATERAL with LIMIT 1
    reproduces that exactly. (The replay frames ladder needed LEFT ... ON TRUE
    for the opposite reason: there the row itself carried the level lines and
    had to survive.)
    """
    sql = _historical_sql()
    for name in ("call_walls", "put_walls"):
        assert "CROSS JOIN LATERAL (" in _cte(sql, name)
    assert "LEFT JOIN call_walls cw ON cw.bucket_ts = b.bucket_ts" in sql
    assert "LEFT JOIN put_walls  pw ON pw.bucket_ts = b.bucket_ts" in sql


def test_strike_agg_still_zero_fills_rather_than_dropping_a_bucket():
    """The aggregate has no GROUP BY, so the lateral always returns one row.

    Pre-fix, a timestamp with no gex_by_strike rows produced no strike_agg row
    and the outer COALESCE turned the NULL into 0. Post-fix it produces a row
    of zeros. Same output either way -- but only because the aggregate keeps its
    COALESCE, so pin that.
    """
    sql = _historical_sql()
    body = _cte(sql, "strike_agg")
    assert "CROSS JOIN LATERAL (" in body
    assert "GROUP BY" not in body, "a GROUP BY here would let a bucket vanish"
    assert "COALESCE(SUM(gbs.call_gamma" in body
    assert "COALESCE(SUM(-1 * gbs.put_gamma" in body


def test_fence_holds_for_every_timeframe_the_endpoint_accepts():
    """1day is the worst case, but the shape must not vary by timeframe."""
    for timeframe in ("1min", "5min", "15min", "1hr", "1day"):
        sql = _historical_sql(timeframe=timeframe)
        assert sql.count("JOIN LATERAL (") >= 3, timeframe
        assert "IN (SELECT timestamp FROM base)" not in sql, timeframe


def test_fence_holds_for_etfs_too_not_just_cash_indices():
    """SPY takes the no-session-filter branch -- a separate SQL template."""
    sql = _historical_sql(symbol="SPY")
    assert sql.count("JOIN LATERAL (") >= 3
    assert "IN (SELECT timestamp FROM base)" not in sql
    assert "JOIN base b ON b.timestamp = gbs.timestamp" not in sql
