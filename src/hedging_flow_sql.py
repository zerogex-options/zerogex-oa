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

# psycopg2 form: no call site yet. Rendered here rather than at the point a
# snapshot writer is added, so that writer inherits this text instead of
# transcribing it -- the drift the flow-series module exists to prevent.
HEDGING_FLOW_CTE_PSYCOPG2 = _render_psycopg2(_HEDGING_FLOW_CTE_TEMPLATE)
