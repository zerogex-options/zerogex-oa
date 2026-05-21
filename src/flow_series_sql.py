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
#
# This full-window form is reserved for cold-start / gap-fill cases
# (first cycle of a fresh session, recovery from missed cycles).
# Steady-state cycles use SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2 below,
# which refreshes only the prev + curr bar -- 30x cheaper.
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


# Incremental UPSERT: refresh ONLY the prev + curr 5-min bars (the open
# bar and the one immediately before it).
#
# Why this exists
# ---------------
# Closed bars in flow_series_5min are window-invariant: the canonical
# CTE's outer SUM(...) OVER w_cum (ROWS UNBOUNDED PRECEDING ORDER BY
# bar_start) means once a bar's 5-min boundary passes, its cumulative
# values are mathematically fixed -- subsequent cycles compute the
# identical values and the IS DISTINCT FROM guard suppresses the
# write. But the CTE itself still RUNS over the full session window
# every cycle, walking ~78 bars worth of flow_by_contract data even
# though 76 of them are guaranteed no-ops. Measured cost: ~30s per
# cycle on db.t3.small (the new bottleneck after the snapshot
# lookback was shrunk).
#
# Direct cumulative computation
# -----------------------------
# ``flow_by_contract.raw_volume / raw_premium / net_volume / net_premium``
# are session-cumulative per-contract values (see
# ``AnalyticsEngine._refresh_flow_caches`` -- the engine writes each
# bucket as SUM over flow_contract_facts from session_open through
# bucket_end). Summing these across contracts at a given bar gives the
# total-cumulative-through-bar directly, without the LAG-delta-then-
# recum dance the canonical CTE uses. The two formulations are
# algebraically equivalent (verified):
#
#   call_premium_cum(T) = SUM over bars b<=T of (SUM_c (net_premium_C(c,b) - net_premium_C(c,b-1)))
#                       = SUM_c net_premium_C(c,T)   -- telescopes to direct sum
#
# So this incremental form computes the cumulatives directly from
# flow_by_contract for the two target bars only. No window functions,
# no 8-level CTE, no session-wide scan.
#
# Correctness model
# -----------------
# * Closed bars older than prev_bar: never touched. Their rows in
#   flow_series_5min stay as computed by an earlier cycle (when they
#   were the open bar). Window-invariant means they're final.
# * prev_bar: refreshed every cycle. On the cycle that crosses the
#   5-min boundary (prev_bar transitions from "the open bar last
#   cycle" to "closed bar this cycle"), this refresh finalises its
#   value to reflect any flow_by_contract writes that landed after
#   the prior cycle's refresh.
# * curr_bar: refreshed every cycle. Open bar, content changes within
#   the 5-min window as new flow_by_contract.curr_bucket writes arrive.
#
# Backfill / gap handling
# -----------------------
# This form does NOT populate bars between session_open and prev_bar.
# A fresh session (no flow_series_5min rows yet) or a recovery from a
# multi-cycle gap should run the full SNAPSHOT_UPSERT_PSYCOPG2 once
# first to seed the closed bars. The engine detects this and dispatches
# accordingly -- see AnalyticsEngine._refresh_flow_series_snapshot.
#
# Parameters: %(symbol)s, %(prev_bar)s, %(curr_bar)s
SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2 = """
INSERT INTO flow_series_5min (
    symbol,
    bar_start,
    call_premium_cum,
    put_premium_cum,
    call_volume_cum,
    put_volume_cum,
    net_volume_cum,
    raw_volume_cum,
    call_position_cum,
    put_position_cum,
    net_premium_cum,
    put_call_ratio,
    underlying_price,
    contract_count,
    is_synthetic
)
WITH target_bars AS (
    SELECT bar_start FROM (
        VALUES (%(prev_bar)s::timestamptz), (%(curr_bar)s::timestamptz)
    ) AS t(bar_start)
),
per_bar_aggregates AS (
    SELECT
        t.bar_start,
        COALESCE(SUM(CASE WHEN fbc.option_type = 'C' THEN fbc.net_premium ELSE 0 END), 0)::numeric
            AS call_premium_cum,
        COALESCE(SUM(CASE WHEN fbc.option_type = 'P' THEN fbc.net_premium ELSE 0 END), 0)::numeric
            AS put_premium_cum,
        COALESCE(SUM(CASE WHEN fbc.option_type = 'C' THEN fbc.raw_volume ELSE 0 END), 0)::bigint
            AS call_volume_cum,
        COALESCE(SUM(CASE WHEN fbc.option_type = 'P' THEN fbc.raw_volume ELSE 0 END), 0)::bigint
            AS put_volume_cum,
        COALESCE(SUM(fbc.net_volume), 0)::bigint AS net_volume_cum,
        COALESCE(SUM(fbc.raw_volume), 0)::bigint AS raw_volume_cum,
        COALESCE(SUM(CASE WHEN fbc.option_type = 'C' THEN fbc.net_volume ELSE 0 END), 0)::bigint
            AS call_position_cum,
        COALESCE(SUM(CASE WHEN fbc.option_type = 'P' THEN fbc.net_volume ELSE 0 END), 0)::bigint
            AS put_position_cum,
        COUNT(fbc.option_symbol)::int AS contract_count
    FROM target_bars t
    LEFT JOIN flow_by_contract fbc
        ON fbc.symbol = %(symbol)s AND fbc.timestamp = t.bar_start
    GROUP BY t.bar_start
),
underlying_price_per_bar AS (
    SELECT
        t.bar_start,
        (
            SELECT close
            FROM underlying_quotes uq
            WHERE uq.symbol = %(symbol)s
              AND uq.timestamp >= t.bar_start
              AND uq.timestamp <  t.bar_start + INTERVAL '5 minutes'
            ORDER BY uq.timestamp DESC
            LIMIT 1
        ) AS bar_price,
        -- Carry-forward: most recent prior price if no trade in this bar.
        -- Matches the canonical CTE's carry semantic (FIRST_VALUE PARTITION
        -- BY up_grp) for the no-trade case.
        (
            SELECT close
            FROM underlying_quotes uq
            WHERE uq.symbol = %(symbol)s
              AND uq.timestamp < t.bar_start + INTERVAL '5 minutes'
            ORDER BY uq.timestamp DESC
            LIMIT 1
        ) AS carry_price
    FROM target_bars t
)
SELECT
    %(symbol)s AS symbol,
    a.bar_start,
    a.call_premium_cum,
    a.put_premium_cum,
    a.call_volume_cum,
    a.put_volume_cum,
    a.net_volume_cum,
    a.raw_volume_cum,
    a.call_position_cum,
    a.put_position_cum,
    (a.call_premium_cum + a.put_premium_cum) AS net_premium_cum,
    CASE
        WHEN a.call_volume_cum > 0
        THEN a.put_volume_cum::float8 / a.call_volume_cum::float8
        ELSE NULL
    END AS put_call_ratio,
    COALESCE(u.bar_price, u.carry_price) AS underlying_price,
    a.contract_count,
    (a.contract_count = 0) AS is_synthetic
FROM per_bar_aggregates a
JOIN underlying_price_per_bar u USING (bar_start)
ON CONFLICT (symbol, bar_start) DO UPDATE SET
        {_UPSERT_SET}
WHERE
        {_UPSERT_DISTINCT}
""".format(_UPSERT_SET=_UPSERT_SET, _UPSERT_DISTINCT=_UPSERT_DISTINCT)
