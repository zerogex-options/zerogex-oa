"""The /api/flow/centroids aggregation: contracts traded and strike x contracts,
per 5-minute bar and side of the chain.

One row per (5-minute bar, option type) read straight from
``flow_contract_facts``: how many contracts traded and the sum of strike times
contracts. Dividing the two gives the volume-weighted average strike, and
because both are plain sums, any run of bars adds up to the centroid of that
run. :mod:`src.analytics.flow_centroids` does that adding.

The 5-minute bucket is formed exactly as ``/api/flow/series`` and
``/api/flow/by-strike`` form it (hour plus whole 5-minute steps), so a
centroid bar lines up with the flow bar of the same ``bar_start``.

There is no strike band, unlike ``/api/flow/by-strike``: a centroid is an
average over everything that traded, and cutting the far strikes would pull
it toward the price.

Parameters (asyncpg):
  $1 symbol
  $2 window start (inclusive), $3 window end (exclusive)
  $4 expirations date[] or NULL
"""

from __future__ import annotations

FLOW_CENTROIDS_ASYNCPG = """
    SELECT
        (date_trunc('hour', f.timestamp)
         + FLOOR(EXTRACT(MINUTE FROM f.timestamp)::int / 5)
           * INTERVAL '5 minutes') AS bar_start,
        f.option_type,
        SUM(f.volume_delta)::bigint AS volume,
        SUM(f.strike * f.volume_delta)::float8 AS strike_volume
    FROM flow_contract_facts f
    WHERE f.symbol = $1
      AND f.timestamp >= $2
      AND f.timestamp <  $3
      AND ($4::date[] IS NULL OR f.expiration = ANY($4::date[]))
    GROUP BY 1, 2
    HAVING SUM(f.volume_delta) > 0
    ORDER BY 1 ASC, 2 ASC
"""
