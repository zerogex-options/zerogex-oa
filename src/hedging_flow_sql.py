"""Single source of truth for the /api/flow/hedging 5-minute aggregation.

Companion to :mod:`src.flow_series_sql`, and deliberately built to the same
shape: one canonical query text with neutral ``:name`` tokens, rendered per
driver, so any future second call site (a snapshot writer, a backfill) runs
the *same* pipeline rather than a re-implementation that can silently drift.

What it computes
----------------
Estimated dealer hedging pressure per 5-minute bar -- the USD of stock a
delta-flat hedge implies against the session's aggressor-classified option
trades. The primitive and its sign convention are documented in
:mod:`src.analytics.hedging_flow`; in SQL it is::

    (buy_volume - sell_volume) * delta * 100 * underlying_price

summed over contracts, per bar, then accumulated across the session. Positive
means the hedge BUYS stock, matching
:attr:`src.analytics.forced_flow.ForcedFlow.total_usd` so the modeled and
estimated sources are directly additive.

Why ``flow_contract_facts`` and not ``flow_by_contract``
-------------------------------------------------------
Two reasons, and the first is decisive: ``flow_by_contract`` does not carry
``delta`` -- the column was dropped from it (see the schema's
``DROP COLUMN IF EXISTS avg_delta``) -- so the notional simply cannot be
formed there. ``flow_contract_facts`` carries ``delta``, the aggressor split,
and the underlying quote at the fact's own minute. Second, its value columns
are already PER-BUCKET DELTAS, so this pipeline needs no LAG-then-recumulate
dance: it groups, sums, and accumulates once.

The table is sparse (rows exist only where ``volume_delta > 0``), which is why
the timeline is generated and left-joined rather than read off the facts.

Spot: two different prices, on purpose
--------------------------------------
* Each trade's notional is valued at ``flow_contract_facts.underlying_price``
  -- the underlying quote at that fact's own minute, i.e. spot at the time of
  the trade. Valuing an 09:45 trade at the 15:55 print would be wrong.
* The ``underlying_price`` COLUMN RETURNED to the chart comes from
  ``underlying_quotes`` on the 5-minute grid, using the identical expression
  as :mod:`src.flow_series_sql`. That is what makes the Hedging Flow panel
  overlay the Options Flow chart exactly rather than approximately, and --
  same as there -- the subquery does NOT see the strike/expiration filters,
  so price stays invariant across filter combinations for a given bar.

Window invariance
-----------------
The outer window is ``ROWS UNBOUNDED PRECEDING ORDER BY bar_start``, so a
closed bar's cumulative values never change when ``:session_end`` extends --
the same property that lets the flow-series snapshot be written once and
trusted. It is what would let this query be materialised later without
inventing a new correctness argument.
"""

from __future__ import annotations

# Canonical CTE. Tokenised with ``:name`` so a psycopg2 render (for a future
# snapshot writer) is a one-line addition to _PARAM_ORDER's consumers rather
# than a second copy of the SQL.
_HEDGING_FLOW_CTE_TEMPLATE = """
                    WITH filtered AS (
                        SELECT
                            (date_trunc('hour', timestamp)
                             + FLOOR(EXTRACT(MINUTE FROM timestamp)::int / 5)
                               * INTERVAL '5 minutes') AS bar_start,
                            option_type,
                            -- Net customer contracts under the aggressor
                            -- classification. buy_volume/sell_volume are
                            -- extrapolated upstream to cover volume_delta,
                            -- so a fully mid-classified bucket has both at 0
                            -- and contributes no flow rather than a guess.
                            (buy_volume - sell_volume)::numeric AS net_contracts,
                            (buy_volume + sell_volume)::numeric AS classified_volume,
                            volume_delta::numeric AS total_volume,
                            delta,
                            underlying_price
                        FROM flow_contract_facts
                        WHERE symbol = :symbol
                          AND timestamp >= :session_start
                          AND timestamp <  :session_end::timestamptz + INTERVAL '5 minutes'
                          AND delta IS NOT NULL
                          AND underlying_price IS NOT NULL
                          AND (:strikes::numeric[] IS NULL OR strike = ANY(:strikes::numeric[]))
                          AND (:expirations::date[] IS NULL OR expiration = ANY(:expirations::date[]))
                    ),
                    priced AS (
                        SELECT
                            bar_start,
                            option_type,
                            classified_volume,
                            total_volume,
                            -- The primitive. Signed delta (calls +, puts -)
                            -- makes all four customer actions come out with
                            -- the right hedge direction without a CASE.
                            (net_contracts * delta * 100 * underlying_price)::numeric AS hedge_usd
                        FROM filtered
                    ),
                    per_bar AS (
                        SELECT
                            bar_start,
                            SUM(CASE WHEN option_type='C' THEN hedge_usd ELSE 0 END)::numeric AS call_flow_usd,
                            SUM(CASE WHEN option_type='P' THEN hedge_usd ELSE 0 END)::numeric AS put_flow_usd,
                            SUM(hedge_usd)::numeric                                            AS net_flow_usd,
                            SUM(classified_volume)::numeric                                    AS classified_volume,
                            SUM(total_volume)::numeric                                         AS total_volume,
                            COUNT(*)::int                                                      AS contract_count
                        FROM priced
                        GROUP BY bar_start
                    ),
                    -- Identical expression to src/flow_series_sql.py's
                    -- underlying_by_bar, and identically unfiltered, so the
                    -- Hedging Flow panel and the Options Flow chart land on
                    -- the same price at the same bar.
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
                        -- Gated on filtered having rows, so a filter that
                        -- matches nothing returns zero rows rather than a
                        -- full session of synthetic zero bars.
                        SELECT g.bar_start
                        FROM generate_series(:session_start::timestamptz, :session_end::timestamptz, INTERVAL '5 minutes') AS g(bar_start)
                        WHERE EXISTS (SELECT 1 FROM filtered)
                    ),
                    joined AS (
                        SELECT
                            t.bar_start,
                            COALESCE(pb.call_flow_usd, 0)     AS call_flow_usd,
                            COALESCE(pb.put_flow_usd, 0)      AS put_flow_usd,
                            COALESCE(pb.net_flow_usd, 0)      AS net_flow_usd,
                            COALESCE(pb.classified_volume, 0) AS classified_volume,
                            COALESCE(pb.total_volume, 0)      AS total_volume,
                            COALESCE(pb.contract_count, 0)    AS contract_count,
                            ub.underlying_price,
                            (pb.bar_start IS NULL) AS is_synthetic
                        FROM timeline t
                        LEFT JOIN per_bar           pb USING (bar_start)
                        LEFT JOIN underlying_by_bar ub USING (bar_start)
                    ),
                    carry AS (
                        -- FIRST_VALUE + partition-by-running-count emulates
                        -- LAST_VALUE(... IGNORE NULLS) portably, same as the
                        -- flow-series pipeline.
                        SELECT
                            j.*,
                            COUNT(underlying_price) OVER (ORDER BY bar_start ROWS UNBOUNDED PRECEDING) AS up_grp
                        FROM joined j
                    )
                    SELECT
                        bar_start,
                        call_flow_usd,
                        put_flow_usd,
                        net_flow_usd,
                        SUM(call_flow_usd) OVER w_cum AS cum_call_usd,
                        SUM(put_flow_usd)  OVER w_cum AS cum_put_usd,
                        SUM(net_flow_usd)  OVER w_cum AS cum_net_usd,
                        CASE
                            WHEN total_volume > 0
                            THEN (classified_volume / total_volume)::float8
                            ELSE NULL
                        END AS classified_ratio,
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
# parameters, so the canonical query is rendered to $1..$5. Order and meaning
# match src/flow_series_sql.py's _PARAM_ORDER exactly, so the two read paths
# can share session-resolution code.
_PARAM_ORDER = (
    ("symbol", "$1"),
    ("session_start", "$2"),
    ("session_end", "$3"),
    ("strikes", "$4"),
    ("expirations", "$5"),
)

#: Outer SELECT columns, in exact emission order.
HEDGING_FLOW_COLUMNS = (
    "bar_start",
    "call_flow_usd",
    "put_flow_usd",
    "net_flow_usd",
    "cum_call_usd",
    "cum_put_usd",
    "cum_net_usd",
    "classified_ratio",
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


# asyncpg form: the live read path in get_hedging_flow_series.
HEDGING_FLOW_CTE_ASYNCPG = _render_asyncpg(_HEDGING_FLOW_CTE_TEMPLATE)

# psycopg2 form: the Analytics Engine snapshot write and the backfill tool.
# Rendered from the same template as the asyncpg form rather than transcribed,
# which is the drift the flow-series module exists to prevent.
HEDGING_FLOW_CTE_PSYCOPG2 = _render_psycopg2(_HEDGING_FLOW_CTE_TEMPLATE)


# ---------------------------------------------------------------------------
# hedging_flow_5min snapshot
# ---------------------------------------------------------------------------
#
# Why a snapshot at all, when the live CTE is cheap
# -------------------------------------------------
# Not for speed. ``flow_contract_facts`` is in ``DB_MAINTAIN_TABLES``, so
# ``make db-prune`` deletes it at ``DATA_RETENTION_DAYS`` (90). Recomputing a
# past session from it therefore answers for a quarter and then returns an
# empty session -- indistinguishable, to a reader, from a genuinely quiet day.
# The snapshot exists so a session survives its source data, which is the same
# reason ``gex_summary`` and ``underlying_quotes`` were made retention-exempt
# in 2026-08 for the TradeWorkz screen.
#
# It is also tiny: 78 bars per symbol per session, two scopes, which is
# smaller than either of those two tables at ~1 row/min/symbol.
#
# Why the rows can be written once and trusted
# --------------------------------------------
# The correctness argument is the one already made at the top of this module
# and it is not re-derived here: the outer window is ROWS UNBOUNDED PRECEDING
# ORDER BY bar_start, so a closed bar's cumulative values do not move when
# ``:session_end`` extends. That is what makes an UPSERT-per-cycle converge
# rather than churn, and it is why the IS DISTINCT FROM guard below suppresses
# essentially every write after a bar closes.
#
# Scope, and why it is a column rather than a filter
# --------------------------------------------------
# The live CTE takes arbitrary ``strikes``/``expirations`` arrays, and a
# snapshot cannot pre-compute an arbitrary filter -- which is why
# ``flow_series_5min`` supersedes only the UNFILTERED read and leaves filtered
# reads on the CTE. The Hedging Flow page, though, offers exactly one filter:
# a 0DTE toggle that resolves to the session's own date. That is a closed set
# of two, so both are materialised and the toggle picks a scope instead of
# re-running a pipeline whose inputs have been pruned. Any OTHER filter still
# falls through to the CTE and is still bounded by the prune window, exactly
# as on the flow series.
#
# A session that was not an expiry simply has no ``0dte`` rows, which is the
# same honest "no 0DTE contracts traded this session" the live page reports
# rather than a fabricated flat line.

#: The two materialised expiration scopes. ``all`` is the unfiltered series;
#: ``0dte`` is the session's own date passed as the expirations filter.
SCOPE_ALL = "all"
SCOPE_0DTE = "0dte"
HEDGING_FLOW_SCOPES = (SCOPE_ALL, SCOPE_0DTE)

_COLS_CSV = ",\n    ".join(HEDGING_FLOW_COLUMNS)

#: Snapshot read (asyncpg). Same columns, same order, same window resolution
#: as the CTE path, so a dated read and a live read decode identically.
HEDGING_FLOW_SNAPSHOT_SELECT_ASYNCPG = f"""
    SELECT
    {_COLS_CSV}
    FROM hedging_flow_5min
    WHERE symbol = $1
      AND scope = $2
      AND bar_start >= $3
      AND bar_start <= $4
    ORDER BY bar_start DESC
"""

_UPSERT_SET = ",\n        ".join(
    f"{c} = EXCLUDED.{c}" for c in HEDGING_FLOW_COLUMNS if c != "bar_start"
)
_UPSERT_DISTINCT = "\n        OR ".join(
    f"EXCLUDED.{c} IS DISTINCT FROM hedging_flow_5min.{c}"
    for c in HEDGING_FLOW_COLUMNS
    if c != "bar_start"
)

#: Snapshot UPSERT (psycopg2). Runs the canonical CTE as a subquery and
#: prefixes the symbol and scope so the inserted columns line up. The
#: IS DISTINCT FROM guard suppresses no-op writes, so re-running a cycle over
#: closed bars costs a read and no write at all.
#:
#: Unlike the flow series this has no separate incremental form. That one
#: exists because its CTE walks ``flow_by_contract`` with LAG-and-recumulate
#: over the whole session (~30s/cycle measured); this pipeline reads
#: ``flow_contract_facts``, whose values are already per-bucket deltas, so the
#: full-session form is the cheap one and a second query shape would be
#: maintenance for no gain.
HEDGING_FLOW_SNAPSHOT_UPSERT_PSYCOPG2 = f"""
INSERT INTO hedging_flow_5min (
    symbol,
    scope,
    {_COLS_CSV}
)
SELECT %(symbol)s, %(scope)s, s.*
FROM (
{HEDGING_FLOW_CTE_PSYCOPG2}
) s
ON CONFLICT (symbol, scope, bar_start) DO UPDATE SET
        {_UPSERT_SET},
        updated_at = NOW()
WHERE
        {_UPSERT_DISTINCT}
"""

#: Trading days that have snapshot rows, newest first, with enough of a
#: summary for a session card to say something about the day rather than only
#: name it. ``had_0dte`` is read off the presence of ``0dte`` rows, which is
#: exactly what the toggle needs to know before it is offered.
HEDGING_FLOW_SESSIONS_ASYNCPG = """
    WITH days AS (
        SELECT
            (bar_start AT TIME ZONE 'America/New_York')::date AS session_date,
            scope,
            bar_start,
            cum_net_usd,
            is_synthetic
        FROM hedging_flow_5min
        WHERE symbol = $1
    )
    SELECT
        session_date,
        COUNT(*) FILTER (WHERE scope = 'all')::int AS bar_count,
        COUNT(*) FILTER (WHERE scope = 'all' AND NOT is_synthetic)::int AS real_bar_count,
        BOOL_OR(scope = '0dte') AS had_0dte,
        -- The session's closing lean: the last 'all' bar's running total.
        (ARRAY_AGG(cum_net_usd ORDER BY bar_start DESC)
            FILTER (WHERE scope = 'all'))[1] AS cum_net_usd,
        MIN(bar_start) FILTER (WHERE scope = 'all') AS first_bar,
        MAX(bar_start) FILTER (WHERE scope = 'all') AS last_bar
    FROM days
    GROUP BY session_date
    HAVING COUNT(*) FILTER (WHERE scope = 'all') > 0
    ORDER BY session_date DESC
    LIMIT $2
"""
