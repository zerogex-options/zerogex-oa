"""Single source of truth for the /api/flow/series 5-minute aggregation.

Three call sites execute the *same* pipeline so that ``flow_series_5min``
rows are byte-identical to the live CTE **by construction**, not by a
re-implementation that can silently drift:

  1. ``src/api/database.py::get_flow_series`` — the live read path and the
     CTE fallback (asyncpg, ``$1..$5`` positional placeholders).
  2. ``src/analytics/main_engine.py::_refresh_flow_series_snapshot`` — the
     per-cycle snapshot write (psycopg2, ``%(name)s`` named placeholders).
  3. ``src/tools/flow_series_5min_backfill.py`` — the one-shot backfill
     (psycopg2).

The canonical text is stored once with neutral ``:name`` tokens and
rendered per driver. The leading colon disambiguates a token from the
identically-named column (``:symbol`` vs the ``symbol`` column), so a
plain string substitution is safe and order-independent. A unit test
(``tests/test_flow_series_snapshot.py``) asserts the two rendered forms
are equivalent modulo the placeholder dialect, which is the anti-drift
guarantee.

The pipeline's outer window is ``ROWS UNBOUNDED PRECEDING ORDER BY
bar_start``, so a *closed* bar's cumulative values are window-invariant:
extending ``:session_end`` never changes an already-closed bar. That is
why a snapshot populated by running this exact query is final and
byte-identical for every closed bar regardless of when it was computed.
"""

from __future__ import annotations

# Canonical CTE. Transcribed verbatim from the original inlined query in
# get_flow_series; the only change is $N -> :name tokenisation. Comments
# are preserved deliberately — this text is the contract.
_FLOW_SERIES_CTE_TEMPLATE = """
                    WITH filtered AS (
                        SELECT
                            timestamp AS bar_start,
                            option_type,
                            strike,
                            expiration,
                            raw_volume,
                            net_volume,
                            net_premium
                        FROM flow_by_contract
                        WHERE symbol = :symbol
                          AND timestamp >= :session_start
                          AND timestamp <= :session_end
                          AND (:strikes::numeric[] IS NULL OR strike = ANY(:strikes::numeric[]))
                          AND (:expirations::date[]    IS NULL OR expiration = ANY(:expirations::date[]))
                    ),
                    contract_deltas AS (
                        SELECT
                            bar_start,
                            option_type,
                            strike,
                            expiration,
                            (raw_volume  - COALESCE(LAG(raw_volume)  OVER w, 0))::bigint  AS raw_volume_delta,
                            (net_volume  - COALESCE(LAG(net_volume)  OVER w, 0))::bigint  AS net_volume_delta,
                            (net_premium - COALESCE(LAG(net_premium) OVER w, 0))::numeric AS net_premium_delta
                        FROM filtered
                        WINDOW w AS (PARTITION BY option_type, strike, expiration ORDER BY bar_start)
                    ),
                    per_bar AS (
                        SELECT
                            bar_start,
                            SUM(CASE WHEN option_type='C' THEN net_premium_delta ELSE 0 END)::numeric AS call_premium_delta,
                            SUM(CASE WHEN option_type='P' THEN net_premium_delta ELSE 0 END)::numeric AS put_premium_delta,
                            SUM(CASE WHEN option_type='C' THEN raw_volume_delta  ELSE 0 END)::bigint  AS call_volume_delta,
                            SUM(CASE WHEN option_type='P' THEN raw_volume_delta  ELSE 0 END)::bigint  AS put_volume_delta,
                            SUM(net_volume_delta)::bigint                                             AS net_volume_delta,
                            SUM(raw_volume_delta)::bigint                                             AS raw_volume_delta,
                            SUM(CASE WHEN option_type='C' THEN net_volume_delta  ELSE 0 END)::bigint  AS call_position_delta,
                            SUM(CASE WHEN option_type='P' THEN net_volume_delta  ELSE 0 END)::bigint  AS put_position_delta,
                            COUNT(*)::int AS contract_count
                        FROM contract_deltas
                        GROUP BY bar_start
                    ),
                    -- Underlying price comes from the tape (underlying_quotes
                    -- OHLC), NOT from flow_by_contract.underlying_price. The
                    -- per-contract column captures each contract's last-trade
                    -- price, which is stale for contracts that didn't trade
                    -- in a given bar — aggregating it produces the stair-step
                    -- artifact where the price sticks for 20–30 minutes and
                    -- then jumps. Critically, this subquery does NOT see the
                    -- strike/expiration filters, so underlying_price stays
                    -- invariant across different filter combinations for the
                    -- same (symbol, bar_start).
                    underlying_by_bar AS (
                        SELECT
                            (date_trunc('hour', timestamp)
                             + FLOOR(EXTRACT(MINUTE FROM timestamp)::int / 5)
                               * INTERVAL '5 minutes') AS bar_start,
                            (ARRAY_AGG(close ORDER BY timestamp DESC))[1] AS underlying_price
                        FROM underlying_quotes
                        WHERE symbol = :symbol
                          AND timestamp >= :session_start
                          AND timestamp <  :session_end::timestamptz + INTERVAL '5 minutes'
                        GROUP BY 1
                    ),
                    timeline AS (
                        -- Gate the timeline on filtered having rows. An empty
                        -- filter match (T5) returns zero rows rather than 81
                        -- synthetic zero-cumulative bars.
                        SELECT g.bar_start
                        FROM generate_series(:session_start::timestamptz, :session_end::timestamptz, INTERVAL '5 minutes') AS g(bar_start)
                        WHERE EXISTS (SELECT 1 FROM filtered)
                    ),
                    joined AS (
                        SELECT
                            t.bar_start,
                            COALESCE(pb.call_premium_delta, 0) AS call_premium_delta,
                            COALESCE(pb.put_premium_delta, 0)  AS put_premium_delta,
                            COALESCE(pb.call_volume_delta, 0)  AS call_volume_delta,
                            COALESCE(pb.put_volume_delta, 0)   AS put_volume_delta,
                            COALESCE(pb.net_volume_delta, 0)   AS net_volume_delta,
                            COALESCE(pb.raw_volume_delta, 0)   AS raw_volume_delta,
                            COALESCE(pb.call_position_delta, 0) AS call_position_delta,
                            COALESCE(pb.put_position_delta, 0)  AS put_position_delta,
                            ub.underlying_price,
                            COALESCE(pb.contract_count, 0) AS contract_count,
                            (pb.bar_start IS NULL) AS is_synthetic
                        FROM timeline t
                        LEFT JOIN per_bar           pb USING (bar_start)
                        LEFT JOIN underlying_by_bar ub USING (bar_start)
                    ),
                    carry AS (
                        -- FIRST_VALUE + partition-by-running-count emulates
                        -- LAST_VALUE(... IGNORE NULLS) portably (Postgres < 16
                        -- doesn't support IGNORE NULLS in LAST_VALUE).
                        SELECT
                            j.*,
                            COUNT(underlying_price) OVER (ORDER BY bar_start ROWS UNBOUNDED PRECEDING) AS up_grp
                        FROM joined j
                    )
                    SELECT
                        bar_start,
                        SUM(call_premium_delta)  OVER w_cum AS call_premium_cum,
                        SUM(put_premium_delta)   OVER w_cum AS put_premium_cum,
                        SUM(call_volume_delta)   OVER w_cum AS call_volume_cum,
                        SUM(put_volume_delta)    OVER w_cum AS put_volume_cum,
                        SUM(net_volume_delta)    OVER w_cum AS net_volume_cum,
                        SUM(raw_volume_delta)    OVER w_cum AS raw_volume_cum,
                        SUM(call_position_delta) OVER w_cum AS call_position_cum,
                        SUM(put_position_delta)  OVER w_cum AS put_position_cum,
                        (SUM(call_premium_delta) OVER w_cum
                         + SUM(put_premium_delta) OVER w_cum) AS net_premium_cum,
                        CASE
                            WHEN SUM(call_volume_delta) OVER w_cum > 0
                            THEN (SUM(put_volume_delta) OVER w_cum)::float8
                               / (SUM(call_volume_delta) OVER w_cum)::float8
                            ELSE NULL
                        END AS put_call_ratio,
                        FIRST_VALUE(underlying_price) OVER (
                            PARTITION BY up_grp ORDER BY bar_start
                        ) AS underlying_price,
                        contract_count,
                        is_synthetic
                    FROM carry
                    WINDOW w_cum AS (ORDER BY bar_start ROWS UNBOUNDED PRECEDING)
                    ORDER BY bar_start DESC
"""

# Ordered (token, asyncpg-positional) mapping. asyncpg has no named
# parameters, so the canonical query is rendered to $1..$5.
_PARAM_ORDER = (
    ("symbol", "$1"),
    ("session_start", "$2"),
    ("session_end", "$3"),
    ("strikes", "$4"),
    ("expirations", "$5"),
)

# Outer SELECT columns, in exact emission order. The snapshot SELECT
# projects these and ONLY these (no ``symbol``) so ``dict(row)`` keys are
# identical to the CTE path's. The snapshot table also stores ``symbol``
# as a PK component, but it is never returned to the API.
FLOW_SERIES_COLUMNS = (
    "bar_start",
    "call_premium_cum",
    "put_premium_cum",
    "call_volume_cum",
    "put_volume_cum",
    "net_volume_cum",
    "raw_volume_cum",
    "call_position_cum",
    "put_position_cum",
    "net_premium_cum",
    "put_call_ratio",
    "underlying_price",
    "contract_count",
    "is_synthetic",
)


def _render_asyncpg(template: str) -> str:
    sql = template
    for name, positional in _PARAM_ORDER:
        sql = sql.replace(":" + name, positional)
    return sql


def _render_psycopg2(template: str) -> str:
    sql = template
    for name, _ in _PARAM_ORDER:
        sql = sql.replace(":" + name, "%(" + name + ")s")
    return sql


# asyncpg form: get_flow_series live CTE path / fallback.
FLOW_SERIES_CTE_ASYNCPG = _render_asyncpg(_FLOW_SERIES_CTE_TEMPLATE)

# psycopg2 form: Analytics Engine snapshot write + backfill.
FLOW_SERIES_CTE_PSYCOPG2 = _render_psycopg2(_FLOW_SERIES_CTE_TEMPLATE)

_COLS_CSV = ",\n    ".join(FLOW_SERIES_COLUMNS)

# Snapshot read (asyncpg). Columns in canonical order, symbol excluded.
# Window resolution (symbol/session_start/session_end) is identical to the
# CTE path's _resolve_flow_series_session output.
SNAPSHOT_SELECT_ASYNCPG = f"""
    SELECT
    {_COLS_CSV}
    FROM flow_series_5min
    WHERE symbol = $1
      AND bar_start >= $2
      AND bar_start <= $3
    ORDER BY bar_start DESC
"""

_UPSERT_SET = ",\n        ".join(
    f"{c} = EXCLUDED.{c}" for c in FLOW_SERIES_COLUMNS if c != "bar_start"
)
_UPSERT_DISTINCT = "\n        OR ".join(
    f"EXCLUDED.{c} IS DISTINCT FROM flow_series_5min.{c}"
    for c in FLOW_SERIES_COLUMNS
    if c != "bar_start"
)

# Snapshot UPSERT (psycopg2). Runs the canonical CTE as a subquery and
# prefixes the literal symbol so the 15 inserted columns line up. The
# IS DISTINCT FROM guard suppresses no-op writes (gex_summary pattern):
# closed bars are window-invariant so they never rewrite once final.
SNAPSHOT_UPSERT_PSYCOPG2 = f"""
INSERT INTO flow_series_5min (
    symbol,
    {_COLS_CSV}
)
SELECT %(symbol)s, s.*
FROM (
{FLOW_SERIES_CTE_PSYCOPG2}
) s
ON CONFLICT (symbol, bar_start) DO UPDATE SET
        {_UPSERT_SET}
WHERE
        {_UPSERT_DISTINCT}
"""
