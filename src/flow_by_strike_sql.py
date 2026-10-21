"""The /api/flow/by-strike aggregation: who crossed the spread, strike by strike.

One row per (bar, strike) with the contracts bought and sold on each side of
the chain at that strike, read straight from ``flow_contract_facts``, in
5-minute bars (``FLOW_BY_STRIKE_ASYNCPG``) or 1-minute bars
(``FLOW_BY_STRIKE_1MIN_ASYNCPG``). Facts are per-bucket deltas whose buy/sell
split already sums to the bucket's volume (mid prints are shared out pro rata;
a print with no side to go on counts half each way), so a bar's per-strike
figures add up to the bought and sold totals the flow series carries for the
same contracts.

Each bucket is formed exactly as the series of the same bar size forms it: the
5-minute one as the hour plus whole 5-minute steps, the 1-minute one as the
minute. So a strike's bar lines up with the series bar of the same
``bar_start``, and the two queries differ in that expression alone.

Strikes are limited to a band around the price: from the lower of the
session's low and its reference price (the last quote before the open) to the
higher of its high and that reference, widened by ``$6`` (a fraction of price)
on each side. A chart can only draw the strikes inside its price window, and
an index chain lists thousands of strikes that never come near it. The band is
computed here rather than passed in so that it lives in the chain's own price
space: the futures middleware projects the strikes that come back onto the
ES / NQ axis, which a caller-supplied band in futures prices could not survive.

Parameters (asyncpg):
  $1 symbol
  $2 window start (inclusive): the first bar returned
  $3 window end (exclusive): the end of the last bar returned
  $4 session start, $5 session end (exclusive): the range the band is built on
  $6 band, a fraction of price (0.02 = 2%)
  $7 strikes numeric[] or NULL, $8 expirations date[] or NULL
"""

from __future__ import annotations

_FLOW_BY_STRIKE_TEMPLATE = """
    WITH session_range AS (
        SELECT MIN(low)::float8 AS lo, MAX(high)::float8 AS hi
        FROM underlying_quotes
        WHERE symbol = $1
          AND timestamp >= $4
          AND timestamp <  $5
    ),
    reference AS (
        SELECT close::float8 AS price
        FROM underlying_quotes
        WHERE symbol = $1
          AND timestamp < $4
        ORDER BY timestamp DESC
        LIMIT 1
    ),
    band AS (
        SELECT
            LEAST(r.lo, (SELECT price FROM reference)) * (1 - $6::float8) AS lo,
            GREATEST(r.hi, (SELECT price FROM reference)) * (1 + $6::float8) AS hi
        FROM session_range r
    )
    SELECT
        {bar_start} AS bar_start,
        f.strike,
        COALESCE(SUM(f.buy_volume)  FILTER (WHERE f.option_type = 'C'), 0)::bigint AS call_bought,
        COALESCE(SUM(f.sell_volume) FILTER (WHERE f.option_type = 'C'), 0)::bigint AS call_sold,
        COALESCE(SUM(f.buy_volume)  FILTER (WHERE f.option_type = 'P'), 0)::bigint AS put_bought,
        COALESCE(SUM(f.sell_volume) FILTER (WHERE f.option_type = 'P'), 0)::bigint AS put_sold
    FROM flow_contract_facts f
    CROSS JOIN band b
    WHERE f.symbol = $1
      AND f.timestamp >= $2
      AND f.timestamp <  $3
      AND (b.lo IS NULL OR f.strike >= b.lo)
      AND (b.hi IS NULL OR f.strike <= b.hi)
      AND ($7::numeric[] IS NULL OR f.strike = ANY($7::numeric[]))
      AND ($8::date[]    IS NULL OR f.expiration = ANY($8::date[]))
    GROUP BY 1, 2
    HAVING SUM(f.volume_delta) > 0
    ORDER BY 1 DESC, 2 ASC
"""

_BAR_START_5MIN = """(date_trunc('hour', f.timestamp)
         + FLOOR(EXTRACT(MINUTE FROM f.timestamp)::int / 5)
           * INTERVAL '5 minutes')"""
_BAR_START_1MIN = "date_trunc('minute', f.timestamp)"

FLOW_BY_STRIKE_ASYNCPG = _FLOW_BY_STRIKE_TEMPLATE.format(bar_start=_BAR_START_5MIN)
FLOW_BY_STRIKE_1MIN_ASYNCPG = _FLOW_BY_STRIKE_TEMPLATE.format(bar_start=_BAR_START_1MIN)
